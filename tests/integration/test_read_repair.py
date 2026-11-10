"""Read repair brings stale replicas up to date."""

import pytest

from dynamo.proto.dynamo_pb2 import ValueWithContext
from dynamo.vector_clock import VectorClock

from .conftest import wait_until

pytestmark = pytest.mark.integration


def version(value: str, **clock: int) -> ValueWithContext:
    return ValueWithContext(value=value.encode(), context=VectorClock(clock).to_proto())


def test_stale_replica_is_repaired_on_read(cluster):
    old, new = version("old", x=1), version("new", x=2)
    cluster.node("node-1").storage.put("k", new)
    cluster.node("node-2").storage.put("k", new)
    cluster.node("node-3").storage.put("k", old)

    assert [v.text for v in cluster.client("node-1").get("k").versions] == ["new"]
    wait_until(lambda: cluster.values_on("node-3", "k") == ["new"], message="read repair")
    assert cluster.node("node-1").stats()["read_repairs"] >= 1


def test_missing_replica_is_repaired_on_read(cluster):
    cluster.node("node-1").storage.put("k", version("only-here", x=1))
    cluster.node("node-2").storage.put("k", version("only-here", x=1))
    cluster.client("node-1").get("k")
    wait_until(lambda: cluster.values_on("node-3", "k") == ["only-here"])


def test_siblings_are_repaired_everywhere(cluster):
    a, b = version("a", x=1), version("b", y=1)
    cluster.node("node-1").storage.put("k", a)
    cluster.node("node-2").storage.put("k", b)
    cluster.node("node-3").storage.put("k", a)
    cluster.client("node-1").get("k")
    wait_until(lambda: all(cluster.values_on(n, "k") == ["a", "b"] for n in cluster.ids),
               message="all replicas hold both siblings")


def test_node_that_missed_writes_converges(cluster):
    # Hinted handoff and read repair both target this case; either must fix it.
    db = cluster.client("node-1")
    cluster.stop("node-2")
    cluster.wait_dead("node-2")
    db.put("k", "while-down")
    cluster.start("node-2")
    cluster.wait_converged()
    db.get("k")
    wait_until(lambda: cluster.values_on("node-2", "k") == ["while-down"])
