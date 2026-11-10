"""Gossip-based membership and failure detection.

Every node keeps a table of ``(generation, version)`` per member:

* ``generation`` identifies a process incarnation (its start time in
  seconds), so a restarted node always supersedes what peers remember about
  its previous life.
* ``version`` is a heartbeat counter the node advances every gossip round.

Peers exchange their whole table (push-pull) and keep the entry with the
larger ``(generation, version)``. A node is considered **alive** while its
heartbeat keeps advancing; if we have not seen it advance for
``fail_timeout`` seconds - measured on *our* monotonic clock, so wall-clock
skew between machines does not matter - it is marked **dead**. Dead nodes
stay on the ring (their writes go to stand-ins via hinted handoff) until
they have been dead for ``remove_timeout``.

``left`` is an administrative state (the ``up=False`` field on the wire): a
node that has been told to leave announces it, and peers drop it from the
ring immediately.
"""

from __future__ import annotations

import enum
import logging
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace

from dynamo.proto.dynamo_pb2 import MembershipList, NodeState

log = logging.getLogger(__name__)


class Event(enum.Enum):
    JOINED = "joined"
    RECOVERED = "recovered"
    DIED = "died"
    LEFT = "left"
    REJOINED = "rejoined"


@dataclass
class Member:
    node_id: str
    address: str
    generation: int
    version: int
    left: bool = False
    alive: bool = True
    last_seen: float = 0.0          # local monotonic time of last advance
    dead_since: float | None = None

    @property
    def healthy(self) -> bool:
        return self.alive and not self.left

    def to_proto(self) -> NodeState:
        return NodeState(
            node_id=self.node_id,
            address=self.address,
            generation=self.generation,
            last_updated_time=self.version,
            up=not self.left,
        )


class Membership:
    """Thread-safe membership table for one node."""

    def __init__(
        self,
        node_id: str,
        address: str,
        *,
        fail_timeout: float,
        remove_timeout: float,
        monotonic: Callable[[], float] = time.monotonic,
        wall: Callable[[], float] = time.time,
        logger: logging.Logger | logging.LoggerAdapter | None = None,
    ) -> None:
        self.node_id = node_id
        self.log = logger or log
        self.fail_timeout = fail_timeout
        self.remove_timeout = remove_timeout
        self._monotonic = monotonic
        self._wall = wall
        self._lock = threading.Lock()
        now = monotonic()
        self._members: dict[str, Member] = {
            node_id: Member(
                node_id=node_id,
                address=address,
                generation=int(wall()),
                version=self._wall_ms(),
                last_seen=now,
            )
        }

    # ---------------------------------------------------------------- helpers
    def _wall_ms(self) -> int:
        return int(self._wall() * 1000)

    @property
    def me(self) -> Member:
        with self._lock:
            return replace(self._members[self.node_id])

    # ------------------------------------------------------------- local ops
    def heartbeat(self) -> None:
        """Advance our own heartbeat version (called once per gossip round)."""
        with self._lock:
            me = self._members[self.node_id]
            me.version = max(me.version + 1, self._wall_ms())
            me.last_seen = self._monotonic()

    def leave(self) -> None:
        """Announce that this node is leaving the cluster."""
        with self._lock:
            me = self._members[self.node_id]
            me.left = True
            me.version = max(me.version + 1, self._wall_ms())
        self.log.info("membership: %s is leaving the cluster", self.node_id)

    def mark_dead(self, node_id: str) -> bool:
        """Mark a peer dead after a failed RPC. Returns True if it was alive."""
        if node_id == self.node_id:
            return False
        with self._lock:
            member = self._members.get(node_id)
            if member is None or not member.alive:
                return False
            member.alive = False
            member.dead_since = self._monotonic()
        self.log.info("membership: %s marked DEAD (unreachable)", node_id)
        return True

    # ---------------------------------------------------------------- gossip
    def merge(self, states: Iterable[NodeState]) -> list[tuple[Event, str]]:
        """Merge a peer's membership table into ours."""
        events: list[tuple[Event, str]] = []
        now = self._monotonic()
        with self._lock:
            for state in states:
                if not state.node_id:
                    continue
                if state.node_id == self.node_id:
                    self._merge_self(state)
                    continue
                incoming = (state.generation, state.last_updated_time)
                current = self._members.get(state.node_id)
                if current is None:
                    member = Member(
                        node_id=state.node_id,
                        address=state.address,
                        generation=state.generation,
                        version=state.last_updated_time,
                        left=not state.up,
                        last_seen=now,
                    )
                    self._members[state.node_id] = member
                    events.append((Event.LEFT if member.left else Event.JOINED, member.node_id))
                    continue
                if incoming <= (current.generation, current.version):
                    continue
                was_left, was_alive = current.left, current.alive
                new_generation = state.generation > current.generation
                current.generation, current.version = incoming
                if state.address:
                    current.address = state.address
                current.left = not state.up
                current.last_seen = now
                if not current.left:
                    current.alive = True
                    current.dead_since = None
                if current.left and not was_left:
                    events.append((Event.LEFT, current.node_id))
                elif was_left and not current.left:
                    events.append((Event.REJOINED, current.node_id))
                elif not current.left and (not was_alive or new_generation):
                    # A restart we never noticed (new generation) also counts:
                    # the node may have lost in-flight writes and needs hints.
                    events.append((Event.RECOVERED, current.node_id))
        for event, node_id in events:
            self.log.info("membership: %s %s", node_id, event.value.upper())
        return events

    def _merge_self(self, state: NodeState) -> None:
        """Handle what a peer believes about *us* (lock held)."""
        me = self._members[self.node_id]
        if state.generation > me.generation:
            # A peer remembers a newer incarnation than ours: either the wall
            # clock went backwards across a restart, or another live process
            # is using our node id. Jump ahead so our state wins.
            self.log.warning(
                "membership: peers know a newer incarnation of %s (generation %d > %d); "
                "is another process running with the same node id?",
                self.node_id, state.generation, me.generation)
            me.generation = state.generation + 1
            me.version = self._wall_ms()
        elif state.generation == me.generation and not state.up and not me.left:
            # An operator asked this incarnation to leave.
            me.left = True
            me.version = max(me.version, state.last_updated_time) + 1
            self.log.info("membership: %s received leave request", self.node_id)

    def check_failures(self) -> list[str]:
        """Mark peers whose heartbeat stalled as dead. Returns newly dead ids."""
        now = self._monotonic()
        died: list[str] = []
        with self._lock:
            for member in self._members.values():
                if member.node_id == self.node_id or not member.alive or member.left:
                    continue
                if now - member.last_seen > self.fail_timeout:
                    member.alive = False
                    member.dead_since = now
                    died.append(member.node_id)
        for node_id in died:
            self.log.info("membership: %s marked DEAD (heartbeat timeout)", node_id)
        return died

    # --------------------------------------------------------------- queries
    def snapshot(self) -> MembershipList:
        with self._lock:
            return MembershipList(members=[m.to_proto() for m in self._members.values()])

    def get(self, node_id: str) -> Member | None:
        with self._lock:
            member = self._members.get(node_id)
            return replace(member) if member is not None else None

    def members(self) -> list[Member]:
        with self._lock:
            return [replace(m) for m in self._members.values()]

    def is_healthy(self, node_id: str) -> bool:
        if node_id == self.node_id:
            return True
        with self._lock:
            member = self._members.get(node_id)
            return member is not None and member.healthy

    def address_of(self, node_id: str) -> str | None:
        with self._lock:
            member = self._members.get(node_id)
            return member.address if member is not None else None

    def peers(self, *, alive: bool) -> list[Member]:
        """Other non-left members that are (or are not) alive."""
        with self._lock:
            return [
                replace(m)
                for m in self._members.values()
                if m.node_id != self.node_id and not m.left and m.alive == alive
            ]

    def suspects(self, after: float) -> list[Member]:
        """Live peers whose heartbeat we have not seen advance for ``after`` s.

        The gossip loop contacts these directly before the failure detector
        gives up on them (as in SWIM's "suspect" state): a live peer answers
        and refreshes its heartbeat, a dead one fails the RPC and is marked
        dead immediately instead of at the timeout.
        """
        now = self._monotonic()
        with self._lock:
            return [
                replace(m)
                for m in self._members.values()
                if m.node_id != self.node_id and m.alive and not m.left
                and now - m.last_seen > after
            ]

    def ring_members(self) -> dict[str, str]:
        """Nodes that should own tokens: not left, not dead for too long."""
        now = self._monotonic()
        with self._lock:
            return {
                m.node_id: m.address
                for m in self._members.values()
                if not m.left
                and m.address
                and (m.alive or m.dead_since is None or now - m.dead_since < self.remove_timeout)
            }
