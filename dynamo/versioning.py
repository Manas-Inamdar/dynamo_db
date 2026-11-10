"""Version ordering and syntactic reconciliation (dotted version vectors).

Why not plain vector clocks?
----------------------------
The Dynamo paper has the coordinator increment its own entry of the
client's clock. When one coordinator handles two writes that started from
the same context, plain vector clocks can only

* give both writes the same clock (two values, one clock - a collision), or
* give the second write a larger counter, which then *dominates* the first
  even though the writers never saw each other's value (a lost update).

Dotted version vectors (Preguiça et al., 2012; used by Riak 2.0) fix this by
storing, for every version,

* ``dot``: the unique write event ``(coordinator, counter)``, and
* ``context``: the causal history the writer had *read*.

Version A supersedes version B iff A's context contains B's dot
(``A.context[B.dot.node] >= B.dot.counter``). Two writes from the same stale
context never contain each other's dot, so they correctly become siblings.

Versions without a dot (built by hand in tests, or written by pre-DVV nodes)
fall back to plain vector-clock comparison of their context.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from dynamo.proto.dynamo_pb2 import ValueWithContext
from dynamo.vector_clock import Ordering, VectorClock

Dot = tuple[str, int]
VersionKey = tuple[bytes, tuple[tuple[str, int], ...], Dot | None]


def dot_of(version: ValueWithContext) -> Dot | None:
    if version.HasField("dot") and version.dot.node_id:
        return version.dot.node_id, version.dot.counter
    return None


def context_of(version: ValueWithContext) -> VectorClock:
    """What the writer had seen when it wrote ``version``."""
    return VectorClock.from_proto(version.context)


def full_clock(version: ValueWithContext) -> VectorClock:
    """Everything ``version`` reflects: its context plus its own dot."""
    clock = context_of(version)
    dot = dot_of(version)
    if dot is not None:
        clock = clock.merge(VectorClock({dot[0]: dot[1]}))
    return clock


def version_key(version: ValueWithContext) -> VersionKey:
    """Identity of a version (used for de-duplication and set comparison)."""
    return bytes(version.value), context_of(version).canonical(), dot_of(version)


def supersedes(a: ValueWithContext, b: ValueWithContext) -> bool:
    """True if ``b`` is in ``a``'s causal past, i.e. ``a`` replaces ``b``."""
    b_dot = dot_of(b)
    if b_dot is not None:
        if dot_of(a) == b_dot:
            return False
        return context_of(a).get(b_dot[0]) >= b_dot[1]
    return full_clock(a).compare(full_clock(b)) is Ordering.AFTER


def reconcile(versions: Iterable[ValueWithContext]) -> list[ValueWithContext]:
    """Return the de-duplicated subset of ``versions`` no other version supersedes.

    Whatever remains is mutually concurrent and is returned to clients as
    siblings. The output order is deterministic so replicas holding the same
    versions produce identical replies.
    """
    unique: dict[VersionKey, ValueWithContext] = {}
    for v in versions:
        unique.setdefault(version_key(v), v)
    if len(unique) <= 1:
        return list(unique.values())

    items = [v for _, v in sorted(unique.items(), key=lambda kv: repr(kv[0]))]
    survivors = []
    for i, candidate in enumerate(items):
        # Require strict dominance so that corrupt, mutually-"superseding"
        # versions are both kept rather than both dropped.
        if not any(
            j != i and supersedes(other, candidate) and not supersedes(candidate, other)
            for j, other in enumerate(items)
        ):
            survivors.append(candidate)
    return survivors


def same_versions(a: Sequence[ValueWithContext], b: Sequence[ValueWithContext]) -> bool:
    """True if two version lists contain exactly the same versions."""
    return {version_key(v) for v in a} == {version_key(v) for v in b}


def merged_clock(versions: Iterable[ValueWithContext]) -> VectorClock:
    """Context that supersedes every version in ``versions``."""
    return VectorClock.merge_all(full_clock(v) for v in versions)
