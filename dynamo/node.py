"""A Dynamo storage node: coordinator, replica and gossip participant.

Request flow
------------
Any node can receive a client ``Put``/``Get`` and acts as its *coordinator*:

1. Build the key's **replica plan** from the consistent-hash ring: the first
   N distinct nodes clockwise from the key. Replicas the failure detector
   considers dead are swapped for the next healthy nodes on the ring
   (*sloppy quorum*); those stand-ins store the write as a **hint** tagged
   with the intended replica.
2. Fan the operation out to the plan in parallel and return as soon as W
   (writes) or R (reads) replicas have answered. Slower replicas finish in
   the background.
3. Reads reconcile the returned versions with vector clocks; concurrent
   versions are returned as siblings. Once *all* replicas have answered,
   stale replicas are fixed in the background (**read repair**).

Background threads gossip membership/heartbeats and hand hints back to
their intended replicas once those recover (**hinted handoff**).
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn, TypeVar

import grpc

from dynamo.config import NodeConfig
from dynamo.membership import Event, Membership
from dynamo.proto import dynamo_pb2 as pb
from dynamo.proto import dynamo_pb2_grpc
from dynamo.ring import HashRing
from dynamo.rpc import ChannelPool
from dynamo.storage import DurableCounter, HintStore, Storage
from dynamo.vector_clock import VectorClock
from dynamo.versioning import merged_clock, reconcile, same_versions, version_key

log = logging.getLogger(__name__)

MAX_KEY_BYTES = 1024
T = TypeVar("T")


class NodeLogger(logging.LoggerAdapter):
    """Prefixes every message with the node id."""

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        return f"[{self.extra['node']}] {msg}", kwargs


@dataclass(frozen=True)
class Target:
    """One slot of a key's replica plan.

    ``intended`` is the replica from the top-N preference list. ``node_id`` is
    who actually receives the operation: the replica itself, a healthy
    stand-in (sloppy quorum), or ``None`` if no healthy node is available.
    """

    node_id: str | None
    intended: str

    @property
    def hinted(self) -> bool:
        return self.node_id is not None and self.node_id != self.intended


class QuorumError(Exception):
    """Raised when fewer than R/W replicas responded."""


class DynamoNode(dynamo_pb2_grpc.DynamoServiceServicer):
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self.node_id = config.node_id
        self.log = NodeLogger(log, {"node": self.node_id})

        root = Path(config.data_dir) / self.node_id if config.data_dir else None
        self.storage = Storage(root / "store" if root else None, fsync=config.fsync)
        self.hints = HintStore(root / "hints" if root else None, fsync=config.fsync)
        self._write_counter = DurableCounter(
            root / "write_counter" if root else None,
            floor=max(self.storage.max_counter(self.node_id), self.hints.max_counter(self.node_id)),
        )

        self.membership = Membership(
            self.node_id,
            config.address,
            fail_timeout=config.fail_timeout,
            remove_timeout=config.remove_timeout,
            logger=self.log,
        )
        self.ring = HashRing(config.vnodes)
        self.ring.add_node(self.node_id, config.address)
        self._ring_lock = threading.Lock()

        self.pool = ChannelPool()
        self._fanout = ThreadPoolExecutor(config.fanout_workers, thread_name_prefix=f"{self.node_id}-rpc")
        self._background = ThreadPoolExecutor(8, thread_name_prefix=f"{self.node_id}-bg")
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._delivering: set[str] = set()
        self._delivering_lock = threading.Lock()

        self._stats: Counter[str] = Counter()
        self._stats_lock = threading.Lock()

        self.log.info(
            "initialised at %s (N=%d R=%d W=%d, %d keys, %d hinted keys on disk)",
            config.address, config.n, config.r, config.w, len(self.storage), self.hints.count(),
        )

    # ================================================================ lifecycle
    def start(self) -> None:
        for name, target in (("gossip", self._gossip_loop), ("hints", self._hint_loop)):
            thread = threading.Thread(target=target, name=f"{self.node_id}-{name}", daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._fanout.shutdown(wait=False, cancel_futures=True)
        self._background.shutdown(wait=False, cancel_futures=True)
        self.pool.close()

    def leave(self) -> None:
        """Gracefully announce departure; peers drop us from the ring."""
        self.membership.leave()
        self._sync_ring()

    # ================================================================== metrics
    def _count(self, name: str, amount: int = 1) -> None:
        with self._stats_lock:
            self._stats[name] += amount

    def stats(self) -> dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    # ============================================================ replica plan
    def replica_plan(self, key: str) -> list[Target]:
        order = self.ring.preference_list(key)
        top, rest = order[: self.config.n], order[self.config.n:]
        standins = [nid for nid in rest if self.membership.is_healthy(nid)]
        plan: list[Target] = []
        for nid in top:
            if self.membership.is_healthy(nid):
                plan.append(Target(nid, nid))
            elif standins:
                plan.append(Target(standins.pop(0), nid))
            else:
                plan.append(Target(None, nid))
        return plan

    def _address(self, node_id: str) -> str | None:
        return self.ring.address_of(node_id) or self.membership.address_of(node_id)

    def _rpc_failed(self, node_id: str, op: str, err: grpc.RpcError) -> None:
        code = err.code() if hasattr(err, "code") else None
        self.log.debug("%s -> %s failed: %s", op, node_id, code)
        if code == grpc.StatusCode.UNAVAILABLE:
            self.membership.mark_dead(node_id)

    # =============================================================== fan-out
    def _fan_out(self, targets: Iterable[Target], fn: Callable[..., T], *args: Any) -> list[Future[T]]:
        """Run ``fn(target, *args)`` for every target.

        Remote calls are dispatched to the RPC pool first; work that stays on
        this node (our own replica, or a hint we keep ourselves) then runs
        inline, overlapping with the remote calls and avoiding a thread
        hand-off.
        """
        futures: list[Future[T]] = []
        local: list[Target] = []
        for target in targets:
            if target.node_id in (None, self.node_id):
                local.append(target)
            else:
                futures.append(self._fanout.submit(fn, target, *args))
        for target in local:
            fut: Future[T] = Future()
            try:
                fut.set_result(fn(target, *args))
            except Exception as exc:  # noqa: BLE001 - surfaced through the future
                fut.set_exception(exc)
            futures.append(fut)
        return futures

    def _gather(
        self,
        futures: Iterable[Future[T]],
        needed: int,
        accept: Callable[[T], bool],
    ) -> tuple[list[T], set[Future[T]]]:
        """Wait until ``needed`` futures produced an accepted result, all
        futures finished, or the coordinator deadline passed."""
        pending: set[Future[T]] = set(futures)
        accepted: list[T] = []
        deadline = time.monotonic() + self.config.rpc_timeout + 0.25
        while pending and len(accepted) < needed:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            done, pending = wait(pending, timeout=remaining, return_when=FIRST_COMPLETED)
            for fut in done:
                result = fut.result()
                if accept(result):
                    accepted.append(result)
        return accepted, pending

    # ------------------------------------------------------------------ write
    def _write_one(self, target: Target, key: str, version: pb.ValueWithContext) -> bool:
        """Send one version to one plan slot. True means it counts toward W."""
        if target.node_id is None:
            self._store_hint(target.intended, key, version)
            return False
        if target.node_id == self.node_id:
            if target.hinted:
                self._store_hint(target.intended, key, version)
            else:
                self.storage.put(key, version)
            return True

        address = self._address(target.node_id)
        if address is None:
            self._store_hint(target.intended, key, version)
            return False
        try:
            stub = self.pool.stub(address)
            if target.hinted:
                hint = pb.Hint(key=key, data=version, target_node_id=target.intended,
                               timestamp_ms=int(time.time() * 1000))
                reply = stub.PutHint(hint, timeout=self.config.rpc_timeout)
            else:
                reply = stub.ForwardPut(pb.PutRequest(key=key, data=version),
                                        timeout=self.config.rpc_timeout)
            if reply.success:
                return True
        except grpc.RpcError as err:
            self._rpc_failed(target.node_id, "write", err)
        # The chosen node did not take the write: keep it ourselves so it is
        # eventually handed to the intended replica.
        self._store_hint(target.intended, key, version)
        return False

    def _store_hint(self, intended: str, key: str, version: pb.ValueWithContext) -> None:
        if intended == self.node_id:
            self.storage.put(key, version)
            return
        if not self.hints.count(intended):
            self.log.info("holding hinted writes for unreachable replica %s", intended)
        self.hints.add(intended, key, version)
        self._count("hints_stored")

    # ------------------------------------------------------------------- read
    def _read_one(self, target: Target, key: str) -> tuple[Target, list[pb.ValueWithContext]] | None:
        assert target.node_id is not None
        if target.node_id == self.node_id:
            return target, self._local_versions(key)
        address = self._address(target.node_id)
        if address is None:
            return None
        try:
            reply = self.pool.stub(address).ForwardGet(pb.GetRequest(key=key),
                                                       timeout=self.config.rpc_timeout)
            return target, list(reply.data)
        except grpc.RpcError as err:
            self._rpc_failed(target.node_id, "read", err)
            return None

    def _local_versions(self, key: str) -> list[pb.ValueWithContext]:
        # A node serving as a stand-in also answers from the hints it holds.
        return reconcile([*self.storage.get(key), *self.hints.versions_for_key(key)])

    def _quorum_read(
        self, key: str, needed: int, *, minimal: bool = False
    ) -> tuple[list[tuple[Target, list[pb.ValueWithContext]]], set[Future]]:
        """Read ``key`` from its replicas until ``needed`` have answered.

        Normally every replica is asked (late answers feed read repair).
        ``minimal`` asks only ``needed`` replicas, preferring ourselves, and
        falls back to the others only if some of those fail.
        """
        plan = [t for t in self.replica_plan(key) if t.node_id is not None]
        if not minimal:
            futures = self._fan_out(plan, self._read_one, key)
            return self._gather(futures, needed, lambda r: r is not None)
        plan.sort(key=lambda t: t.node_id != self.node_id)
        first, spare = plan[:needed], plan[needed:]
        responses, pending = self._gather(self._fan_out(first, self._read_one, key),
                                          needed, lambda r: r is not None)
        if len(responses) < needed and spare:
            more, pending = self._gather([*pending, *self._fan_out(spare, self._read_one, key)],
                                         needed - len(responses), lambda r: r is not None)
            responses += more
        return responses, pending

    def _read_repair(
        self,
        key: str,
        responses: list[tuple[Target, list[pb.ValueWithContext]]],
        pending: set[Future],
    ) -> None:
        """Bring every replica that answered up to the reconciled state."""
        if pending:
            done, _ = wait(pending, timeout=self.config.rpc_timeout + 0.25)
            responses = responses + [f.result() for f in done if f.result() is not None]
        latest = reconcile(v for _, versions in responses for v in versions)
        if not latest:
            return
        for target, versions in responses:
            # Stand-ins are not real replicas; their data arrives via handoff.
            if target.hinted or same_versions(reconcile(versions), latest):
                continue
            have = {version_key(v) for v in versions}
            missing = [v for v in latest if version_key(v) not in have]
            self._count("read_repairs")
            for version in missing:
                self._write_one(Target(target.node_id, target.node_id), key, version)
            self.log.debug("read repair: pushed %d version(s) of %r to %s",
                           len(missing), key, target.node_id)

    # ========================================================= client-facing
    @staticmethod
    def _abort(context: grpc.ServicerContext | None, code: grpc.StatusCode, msg: str) -> NoReturn:
        if context is None:
            raise QuorumError(msg)
        context.abort(code, msg)
        raise AssertionError("unreachable")  # context.abort always raises

    def _validate_key(self, key: str, context: grpc.ServicerContext | None) -> None:
        if not key:
            self._abort(context, grpc.StatusCode.INVALID_ARGUMENT, "key must not be empty")
        if len(key.encode("utf-8")) > MAX_KEY_BYTES:
            self._abort(context, grpc.StatusCode.INVALID_ARGUMENT,
                        f"key exceeds {MAX_KEY_BYTES} bytes")

    def Put(self, request: pb.PutRequest, context: grpc.ServicerContext | None) -> pb.PutReply:
        key = request.key
        self._validate_key(key, context)
        self._count("puts")

        causal = VectorClock.from_proto(request.data.context)
        if not causal:
            # Blind write: order it after everything the replicas currently
            # hold so it supersedes those versions instead of becoming a
            # sibling of them.
            responses, _ = self._quorum_read(key, self.config.r, minimal=True)
            causal = merged_clock(v for _, versions in responses for v in versions)

        # A fresh, never-reused dot identifies this write event. It must be
        # above causal[self] so the write is not "already seen" by itself.
        counter = self._write_counter.next_after(causal.get(self.node_id))
        version = pb.ValueWithContext(
            value=request.data.value,
            context=causal.to_proto(),
            dot=pb.VectorClockItem(node_id=self.node_id, counter=counter),
        )

        plan = self.replica_plan(key)
        acks, _ = self._gather(self._fan_out(plan, self._write_one, key, version),
                               self.config.w, bool)
        if len(acks) < self.config.w:
            self._count("write_quorum_failures")
            self._abort(context, grpc.StatusCode.UNAVAILABLE,
                        f"write quorum not met: {len(acks)}/{self.config.w} acks "
                        "(the value may be stored on some replicas)")
        return pb.PutReply(success=True, message=f"stored ({len(acks)}/{self.config.w} acks)")

    def Get(self, request: pb.GetRequest, context: grpc.ServicerContext | None) -> pb.GetReply:
        key = request.key
        self._validate_key(key, context)
        self._count("gets")

        responses, pending = self._quorum_read(key, self.config.r)
        if len(responses) < self.config.r:
            self._count("read_quorum_failures")
            self._abort(context, grpc.StatusCode.UNAVAILABLE,
                        f"read quorum not met: {len(responses)}/{self.config.r} replies")

        versions = reconcile(v for _, vs in responses for v in vs)
        try:
            self._background.submit(self._read_repair, key, responses, pending)
        except RuntimeError:
            pass  # shutting down
        return pb.GetReply(found=bool(versions), data=versions,
                           message="OK" if versions else "not found")

    # ============================================================ replica RPCs
    def ForwardPut(self, request: pb.PutRequest, context: grpc.ServicerContext | None) -> pb.PutReply:
        if not request.key:
            return pb.PutReply(success=False, message="empty key")
        self.storage.put(request.key, request.data)
        return pb.PutReply(success=True, message="stored")

    def ForwardGet(self, request: pb.GetRequest, context: grpc.ServicerContext | None) -> pb.GetReply:
        versions = self._local_versions(request.key)
        return pb.GetReply(found=bool(versions), data=versions, message="OK")

    def PutHint(self, request: pb.Hint, context: grpc.ServicerContext | None) -> pb.PutReply:
        if not request.key or not request.target_node_id:
            return pb.PutReply(success=False, message="hint needs key and target")
        self._store_hint(request.target_node_id, request.key, request.data)
        return pb.PutReply(success=True, message="hint stored")

    def DeliverHints(self, request: pb.DeliverHintsRequest,
                     context: grpc.ServicerContext | None) -> pb.DeliverHintsReply:
        if request.target_node_id != self.node_id:
            return pb.DeliverHintsReply(success=False,
                                        message=f"hints are for {request.target_node_id}")
        for hint in request.hints:
            self.storage.put(hint.key, hint.data)
        self._count("hints_received", len(request.hints))
        return pb.DeliverHintsReply(success=True, message=f"applied {len(request.hints)}")

    # ================================================================ gossip
    def Gossip(self, request: pb.MembershipList,
               context: grpc.ServicerContext | None) -> pb.MembershipList:
        self._apply_membership(request.members)
        return self.membership.snapshot()

    def _apply_membership(self, states: Iterable[pb.NodeState]) -> None:
        events = self.membership.merge(states)
        if not events:
            return
        self._sync_ring()
        # We may have learned about this peer from *its* gossip while our own
        # channel to it is still backing off from an earlier failed attempt
        # (e.g. we gossiped to it as a seed before it was listening). Start
        # with a fresh connection so the first requests do not fail fast.
        fresh = [nid for e, nid in events if e in (Event.JOINED, Event.RECOVERED, Event.REJOINED)]
        for node_id in fresh:
            address = self.membership.address_of(node_id)
            if address:
                self.pool.reset(address)
        if any(e in (Event.RECOVERED, Event.REJOINED) for e, _ in events):
            self._wake_hints()

    def _sync_ring(self) -> None:
        """Make the ring match the membership table."""
        with self._ring_lock:
            desired = self.membership.ring_members()
            current = self.ring.nodes()
            for node_id, address in desired.items():
                if current.get(node_id) != address:
                    self.ring.add_node(node_id, address)
            for node_id in current.keys() - desired.keys():
                self.ring.remove_node(node_id)
                self.log.info("ring: removed %s", node_id)

    def _gossip_once(self) -> None:
        self.membership.heartbeat()
        alive = self.membership.peers(alive=True)
        dead = self.membership.peers(alive=False)
        targets: list[tuple[str | None, str]] = []
        if alive:
            peer = random.choice(alive)
            targets.append((peer.node_id, peer.address))
        else:
            # Not (or no longer) connected to anyone: (re)join via the seeds.
            targets.extend((None, s) for s in self.config.seeds if s != self.config.address)
        # Peers we have not heard from for half the failure timeout are
        # contacted directly, so a slow gossip path does not get a live node
        # declared dead (and a dead one is detected sooner).
        chosen = {node_id for node_id, _ in targets}
        for peer in self.membership.suspects(self.config.fail_timeout / 2)[:2]:
            if peer.node_id not in chosen:
                targets.append((peer.node_id, peer.address))
        if dead and random.random() < self.config.probe_dead_probability:
            # Occasionally probe a dead node so recovery is noticed even if
            # every node had marked every other node dead (e.g. a partition).
            peer = random.choice(dead)
            targets.append((peer.node_id, peer.address))

        snapshot = self.membership.snapshot()
        for node_id, address in targets:
            try:
                reply = self.pool.stub(address).Gossip(snapshot, timeout=self.config.rpc_timeout)
            except grpc.RpcError as err:
                if node_id is not None:
                    self._rpc_failed(node_id, "gossip", err)
                continue
            self._apply_membership(reply.members)

        # Judge liveness only *after* this round's exchanges: if this loop
        # was itself delayed (CPU starvation, GC), the suspects it just
        # contacted get a chance to prove they are alive first.
        if self.membership.check_failures():
            self._sync_ring()

    def _gossip_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._gossip_once()
            except Exception:  # noqa: BLE001 - the loop must survive anything
                self.log.exception("gossip round failed")
            # Jitter avoids every node gossiping in lock-step.
            self._stop.wait(self.config.gossip_interval * random.uniform(0.8, 1.2))

    # ======================================================== hinted handoff
    def _wake_hints(self) -> None:
        try:
            self._background.submit(self._deliver_all_hints)
        except RuntimeError:
            pass  # shutting down

    def _hint_loop(self) -> None:
        while not self._stop.wait(self.config.hint_interval):
            try:
                self._deliver_all_hints()
            except Exception:  # noqa: BLE001
                self.log.exception("hint delivery round failed")

    def _deliver_all_hints(self) -> None:
        for target in self.hints.targets():
            member = self.membership.get(target)
            if member is None:
                continue
            if member.left:
                self._rehome_hints(target)
            elif member.healthy:
                self._deliver_hints(target)

    def _deliver_hints(self, target: str) -> int:
        """Hand pending hints to ``target``. Returns the number of keys delivered."""
        with self._delivering_lock:
            if target in self._delivering:
                return 0
            self._delivering.add(target)
        delivered = 0
        try:
            address = self._address(target)
            if address is None:
                return 0
            pending = self.hints.pending(target)
            batch_size = self.config.hint_batch_size
            for start in range(0, len(pending), batch_size):
                batch = pending[start:start + batch_size]
                request = pb.DeliverHintsRequest(
                    target_node_id=target,
                    hints=[pb.Hint(key=k, data=v, target_node_id=target)
                           for k, versions in batch for v in versions],
                )
                try:
                    reply = self.pool.stub(address).DeliverHints(
                        request, timeout=max(self.config.rpc_timeout, 5.0))
                except grpc.RpcError as err:
                    self._rpc_failed(target, "deliver-hints", err)
                    break
                if not reply.success:
                    self.log.warning("hint delivery to %s rejected: %s", target, reply.message)
                    break
                for key, versions in batch:
                    self.hints.discard(target, key, versions)
                delivered += len(batch)
            if delivered:
                self._count("hints_delivered", delivered)
                self.log.info("hinted handoff: delivered %d key(s) to %s", delivered, target)
            return delivered
        finally:
            with self._delivering_lock:
                self._delivering.discard(target)

    def _rehome_hints(self, target: str) -> None:
        """``target`` left for good: give its hints to the keys' new owners."""
        for key, versions in self.hints.pending(target):
            owners = [t for t in self.replica_plan(key) if t.node_id and not t.hinted]
            stored = 0
            for owner in owners:
                if all(self._write_one(owner, key, v) for v in versions):
                    stored += 1
            if stored:
                self.hints.discard(target, key, versions)
                self._count("hints_rehomed")
