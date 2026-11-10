"""Durability across restarts."""

import pytest

from dynamo.vector_clock import VectorClock

from .conftest import wait_until

pytestmark = pytest.mark.integration


def test_data_survives_full_cluster_restart(cluster):
    db = cluster.client()
    for i in range(20):
        db.put(f"k{i}", f"v{i}")
    wait_until(lambda: all(len(cluster.holders(f"k{i}")) == 3 for i in range(20)))
    for node_id in list(cluster.ids):
        cluster.restart(node_id)
    cluster.wait_converged()
    db = cluster.client()
    for i in range(20):
        assert db.get(f"k{i}").versions[0].text == f"v{i}"


def test_restarted_coordinator_never_reuses_a_clock(cluster):
    db = cluster.client("node-1")
    db.put("k", "base")
    base = db.get("k")
    db.put("k", "before-restart", context=base.context)
    clock_before = VectorClock(db.get("k").versions[0].clock)

    cluster.restart("node-1")
    cluster.wait_converged()
    db = cluster.client("node-1")
    # Same (stale) context as before the restart: the new version must get a
    # different clock, so it shows up as a sibling instead of colliding.
    db.put("k", "after-restart", context=base.context)
    versions = db.get("k").versions
    assert sorted(v.text for v in versions) == ["after-restart", "before-restart"]
    clocks = {VectorClock(v.clock) for v in versions}
    assert clock_before in clocks and len(clocks) == 2
