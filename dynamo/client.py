"""Client library and command-line interface.

Library::

    from dynamo.client import DynamoClient

    with DynamoClient(["localhost:50051"]) as db:
        db.put("cart:42", "apples")
        result = db.get("cart:42")
        if result.has_conflict:
            merged = ",".join(v.text for v in result.versions)
            db.put("cart:42", merged, context=result.context)   # resolves siblings

CLI::

    dynamo-cli --seeds localhost:50051 put cart:42 apples
    dynamo-cli --seeds localhost:50051 get cart:42
    dynamo-cli --seeds localhost:50051 members
    dynamo-cli --seeds localhost:50051          # interactive shell
"""

from __future__ import annotations

import argparse
import logging
import random
import shlex
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TypeVar

import grpc

from dynamo.proto import dynamo_pb2 as pb
from dynamo.proto import dynamo_pb2_grpc
from dynamo.rpc import CHANNEL_OPTIONS
from dynamo.versioning import full_clock, merged_clock

T = TypeVar("T")
log = logging.getLogger(__name__)

EXIT_OK, EXIT_NOT_FOUND, EXIT_USAGE, EXIT_UNAVAILABLE = 0, 1, 2, 3
AVOID_SECONDS = 5.0   # recently failed nodes are tried last for this long


class DynamoError(Exception):
    """Base class for client errors."""


class NoReachableNode(DynamoError):
    """None of the known nodes could be contacted."""


class QuorumUnavailable(DynamoError):
    """The coordinator could not reach enough replicas (R or W)."""


@dataclass(frozen=True)
class Version:
    value: bytes
    clock: dict[str, int]

    @property
    def text(self) -> str:
        return self.value.decode("utf-8", errors="replace")


@dataclass(frozen=True)
class GetResult:
    key: str
    versions: list[Version]
    context: pb.Context        # pass to put() to supersede every version read
    coordinator: str

    @property
    def found(self) -> bool:
        return bool(self.versions)

    @property
    def has_conflict(self) -> bool:
        return len(self.versions) > 1


class DynamoClient:
    """Talks to any node of the cluster, failing over between nodes."""

    def __init__(self, seeds: Sequence[str], *, timeout: float = 5.0, discover: bool = True) -> None:
        if not seeds:
            raise ValueError("at least one seed address is required")
        self.timeout = timeout
        self._seeds = list(dict.fromkeys(seeds))
        self._known: list[str] = list(self._seeds)
        self._channels: dict[str, tuple[grpc.Channel, dynamo_pb2_grpc.DynamoServiceStub]] = {}
        self._discover = discover
        self._discovered = False
        self._failed_at: dict[str, float] = {}

    # ------------------------------------------------------------- plumbing
    def _stub(self, address: str) -> dynamo_pb2_grpc.DynamoServiceStub:
        entry = self._channels.get(address)
        if entry is None:
            channel = grpc.insecure_channel(address, options=CHANNEL_OPTIONS)
            entry = (channel, dynamo_pb2_grpc.DynamoServiceStub(channel))
            self._channels[address] = entry
        return entry[1]

    def _call(self, fn: Callable[[dynamo_pb2_grpc.DynamoServiceStub], T]) -> tuple[str, T]:
        if self._discover and not self._discovered:
            self._discovered = True
            try:
                self.refresh_members()
            except NoReachableNode:
                pass
        candidates = list(self._known)
        random.shuffle(candidates)
        # Nodes that failed recently are tried last rather than first.
        now = time.monotonic()
        candidates.sort(key=lambda a: self._failed_at.get(a, -1e9) > now - AVOID_SECONDS)
        last: grpc.RpcError | None = None
        for address in candidates:
            try:
                return address, fn(self._stub(address))
            except grpc.RpcError as err:
                details = err.details() or ""
                if "quorum" in details:
                    raise QuorumUnavailable(details) from err
                if err.code() == grpc.StatusCode.INVALID_ARGUMENT:
                    raise DynamoError(details) from err
                self._failed_at[address] = time.monotonic()
                last = err
        raise NoReachableNode(f"no reachable node among {candidates}: {last}")

    # ------------------------------------------------------------- data API
    def put(self, key: str, value: bytes | str, context: pb.Context | None = None) -> str:
        """Write ``value``. Returns the coordinator address.

        Without ``context`` the write supersedes every version the
        coordinator can see ("last writer wins" from the client's view).
        With the ``context`` of a previous :meth:`get` it supersedes exactly
        the versions that were read; concurrent writes become siblings.
        """
        data = value.encode("utf-8") if isinstance(value, str) else value
        request = pb.PutRequest(key=key, data=pb.ValueWithContext(value=data, context=context))
        address, _ = self._call(lambda s: s.Put(request, timeout=self.timeout))
        return address

    def get(self, key: str) -> GetResult:
        request = pb.GetRequest(key=key)
        address, reply = self._call(lambda s: s.Get(request, timeout=self.timeout))
        versions = [Version(bytes(v.value), full_clock(v).as_dict()) for v in reply.data]
        context = merged_clock(reply.data).to_proto()
        return GetResult(key=key, versions=versions, context=context, coordinator=address)

    # ----------------------------------------------------------- cluster API
    def members(self) -> list[pb.NodeState]:
        _, reply = self._call(lambda s: s.Gossip(pb.MembershipList(), timeout=self.timeout))
        return sorted(reply.members, key=lambda m: m.node_id)

    def refresh_members(self) -> None:
        """Learn the addresses of all live nodes from any reachable node."""
        self._discover = False
        try:
            members = self.members()
        finally:
            self._discover = True
        addresses = [m.address for m in members if m.up and m.address]
        self._known = list(dict.fromkeys([*addresses, *self._seeds]))

    def leave(self, node_id: str) -> None:
        """Ask ``node_id`` to leave the cluster (it is removed from the ring)."""
        target = next((m for m in self.members() if m.node_id == node_id), None)
        if target is None:
            raise DynamoError(f"unknown node {node_id!r}")
        notice = pb.MembershipList(members=[pb.NodeState(
            node_id=target.node_id, address=target.address, generation=target.generation,
            last_updated_time=target.last_updated_time + 1, up=False)])
        try:
            # The node itself is the authority on its own state.
            # wait_for_ready: if the connection is still being (re)established,
            # wait for it instead of failing fast and taking the fallback.
            self._stub(target.address).Gossip(notice, timeout=self.timeout, wait_for_ready=True)
        except grpc.RpcError as err:
            log.warning("leave: %s unreachable at %s (%s: %s); notifying the cluster instead",
                        node_id, target.address, err.code(), err.details())
            # Unreachable: tell the rest of the cluster directly.
            self._call(lambda s: s.Gossip(notice, timeout=self.timeout))

    def close(self) -> None:
        for channel, _ in self._channels.values():
            channel.close()
        self._channels.clear()

    def __enter__(self) -> DynamoClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ============================================================================
# CLI
# ============================================================================
def _print_get(result: GetResult) -> int:
    if not result.found:
        print(f"{result.key}: not found")
        return EXIT_NOT_FOUND
    label = "versions (concurrent siblings)" if result.has_conflict else "version"
    print(f"{result.key}: {len(result.versions)} {label}  [coordinator {result.coordinator}]")
    for i, v in enumerate(result.versions):
        clock = ", ".join(f"{n}:{c}" for n, c in sorted(v.clock.items()))
        print(f"  [{i}] {v.text!r}  clock={{{clock}}}")
    return EXIT_OK


def _print_members(members: list[pb.NodeState]) -> int:
    print(f"{'NODE':<12} {'ADDRESS':<22} {'GENERATION':<12} STATUS")
    for m in members:
        print(f"{m.node_id:<12} {m.address:<22} {m.generation:<12} {'NORMAL' if m.up else 'LEFT'}")
    return EXIT_OK


def run_command(client: DynamoClient, argv: list[str]) -> int:
    if not argv:
        return EXIT_OK
    cmd, args = argv[0].lower(), argv[1:]
    try:
        if cmd == "put" and len(args) >= 2:
            address = client.put(args[0], " ".join(args[1:]))
            print(f"OK  [coordinator {address}]")
            return EXIT_OK
        if cmd == "get" and len(args) == 1:
            return _print_get(client.get(args[0]))
        if cmd == "resolve" and len(args) >= 2:
            current = client.get(args[0])
            address = client.put(args[0], " ".join(args[1:]), context=current.context)
            print(f"OK  superseded {len(current.versions)} version(s)  [coordinator {address}]")
            return EXIT_OK
        if cmd == "members" and not args:
            return _print_members(client.members())
        if cmd == "leave" and len(args) == 1:
            client.leave(args[0])
            print(f"OK  {args[0]} asked to leave")
            return EXIT_OK
    except (QuorumUnavailable, NoReachableNode) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_UNAVAILABLE
    except DynamoError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_USAGE
    print("usage: put KEY VALUE | get KEY | resolve KEY VALUE | members | leave NODE_ID",
          file=sys.stderr)
    return EXIT_USAGE


def repl(client: DynamoClient) -> None:
    print("Dynamo shell. Commands: put KEY VALUE | get KEY | resolve KEY VALUE | "
          "members | leave NODE_ID | exit")
    while True:
        try:
            line = input("dynamo> ")
        except (EOFError, KeyboardInterrupt):
            print()
            return
        try:
            argv = shlex.split(line)
        except ValueError as exc:
            print(f"parse error: {exc}")
            continue
        if argv and argv[0] in ("exit", "quit"):
            return
        run_command(client, argv)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="dynamo-cli", description="Dynamo key-value store client")
    parser.add_argument("--seeds", default="localhost:50051",
                        help="comma-separated host:port of cluster nodes")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("command", nargs=argparse.REMAINDER,
                        help="put KEY VALUE | get KEY | resolve KEY VALUE | members | leave NODE_ID")
    args = parser.parse_args(argv)
    seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]
    with DynamoClient(seeds, timeout=args.timeout) as client:
        if not args.command:
            repl(client)
            return EXIT_OK
        return run_command(client, args.command)


if __name__ == "__main__":
    sys.exit(main())
