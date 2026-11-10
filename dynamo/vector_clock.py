"""Vector clocks for tracking causality between versions of a value.

A vector clock maps ``node_id -> counter``. Each time a node coordinates a
write it increments its own entry, so two clocks can be compared to decide
whether one version causally descends from the other or whether they were
written concurrently (and must both be kept as *siblings*).
"""

from __future__ import annotations

import enum
from collections.abc import Iterable, Mapping

from dynamo.proto.dynamo_pb2 import Context, VectorClockItem


class Ordering(enum.Enum):
    """Result of comparing two vector clocks ``a.compare(b)``."""

    BEFORE = "before"          # a happened-before b (b descends from a)
    AFTER = "after"            # b happened-before a (a descends from b)
    EQUAL = "equal"            # identical causal histories
    CONCURRENT = "concurrent"  # neither descends from the other


class VectorClock:
    """An immutable-by-convention mapping of node id to write counter.

    ``increment`` and ``merge`` return new clocks rather than mutating in
    place, which makes it safe to share instances between threads.
    """

    __slots__ = ("_clock",)

    def __init__(self, clock: Mapping[str, int] | None = None) -> None:
        self._clock: dict[str, int] = {}
        for node, counter in (clock or {}).items():
            if counter < 0:
                raise ValueError(f"vector clock counter for {node!r} is negative")
            if counter:
                # Zero entries carry no information; dropping them keeps the
                # canonical form unique.
                self._clock[node] = int(counter)

    # ------------------------------------------------------------------ access
    def get(self, node_id: str) -> int:
        return self._clock.get(node_id, 0)

    def as_dict(self) -> dict[str, int]:
        return dict(self._clock)

    def __len__(self) -> int:
        return len(self._clock)

    def __bool__(self) -> bool:
        return bool(self._clock)

    # -------------------------------------------------------------- operations
    def increment(self, node_id: str) -> VectorClock:
        """Return a copy with ``node_id``'s counter advanced by one."""
        updated = dict(self._clock)
        updated[node_id] = updated.get(node_id, 0) + 1
        return VectorClock(updated)

    def merge(self, other: VectorClock) -> VectorClock:
        """Return the element-wise maximum (least upper bound) of two clocks."""
        merged = dict(self._clock)
        for node, counter in other._clock.items():
            if counter > merged.get(node, 0):
                merged[node] = counter
        return VectorClock(merged)

    @classmethod
    def merge_all(cls, clocks: Iterable[VectorClock]) -> VectorClock:
        result = cls()
        for clock in clocks:
            result = result.merge(clock)
        return result

    def compare(self, other: VectorClock) -> Ordering:
        self_greater = False
        other_greater = False
        for node in self._clock.keys() | other._clock.keys():
            a, b = self.get(node), other.get(node)
            if a > b:
                self_greater = True
            elif b > a:
                other_greater = True
            if self_greater and other_greater:
                return Ordering.CONCURRENT
        if self_greater:
            return Ordering.AFTER
        if other_greater:
            return Ordering.BEFORE
        return Ordering.EQUAL

    def descends_from(self, other: VectorClock) -> bool:
        """True if ``self`` has seen everything ``other`` has (>=)."""
        return self.compare(other) in (Ordering.AFTER, Ordering.EQUAL)

    # ------------------------------------------------------------ conversions
    def canonical(self) -> tuple[tuple[str, int], ...]:
        """Hashable, order-independent representation of the clock."""
        return tuple(sorted(self._clock.items()))

    def to_proto(self) -> Context:
        # Entries are emitted in sorted order so that equal clocks always
        # serialize to identical bytes.
        return Context(
            clock=[VectorClockItem(node_id=n, counter=c) for n, c in self.canonical()]
        )

    @classmethod
    def from_proto(cls, ctx: Context) -> VectorClock:
        clock: dict[str, int] = {}
        for item in ctx.clock:
            # Tolerate duplicate entries from older peers by keeping the max.
            clock[item.node_id] = max(clock.get(item.node_id, 0), item.counter)
        return cls(clock)

    # ----------------------------------------------------------------- dunder
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, VectorClock):
            return NotImplemented
        return self._clock == other._clock

    def __hash__(self) -> int:
        return hash(self.canonical())

    def __repr__(self) -> str:
        inner = ", ".join(f"{n}:{c}" for n, c in self.canonical())
        return f"VectorClock({{{inner}}})"
