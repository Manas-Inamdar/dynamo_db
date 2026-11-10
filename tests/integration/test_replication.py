"""Placement, quorum and storage-growth guarantees."""

import pytest

from dynamo.client import QuorumUnavailable

from .conftest import wait_until

pytestmark = pytest.mark.integration


def test_value_stored_on_exactly_the_preference_list(make_cluster):
    cluster = make_cluster(5)
    db = cluster.client()
    for i in range(30):
        key = f"key-{i}"
        db.put(key, "x")
        wait_until(lambda key=key: cluster.holders(key) == set(cluster.preference(key)),
                   message=f"{key} on its preference list")


def test_storage_does_not_grow_with_updates(cluster):
    # Regression: every write used to append a new version and a new file.
    db = cluster.client()
    for i in range(50):
        db.put("hot-key", f"v{i}")
    wait_until(lambda: all(cluster.values_on(n, "hot-key") == ["v49"] for n in cluster.ids))
    for node_id in cluster.ids:
        files = list((cluster.data_dir / node_id / "store").glob("*.rec"))
        assert len(files) == 1


def test_one_replica_down_is_tolerated(cluster):
    db = cluster.client("node-1")
    db.put("k", "before")
    cluster.stop("node-3")
    db.put("k", "during")
    assert [v.text for v in db.get("k").versions] == ["during"]


def test_quorum_failure_reported_when_majority_down(cluster):
    db = cluster.client("node-1")
    db.put("k", "v")
    cluster.stop("node-2")
    cluster.stop("node-3")
    with pytest.raises(QuorumUnavailable):
        db.put("k", "v2")
    with pytest.raises(QuorumUnavailable):
        db.get("k")
    assert cluster.node("node-1").stats()["write_quorum_failures"] == 1


def test_r1_w1_stays_available_with_one_node(make_cluster):
    cluster = make_cluster(3, r=1, w=1)
    db = cluster.client("node-1")
    cluster.stop("node-2")
    cluster.stop("node-3")
    db.put("k", "solo")
    assert [v.text for v in db.get("k").versions] == ["solo"]


def test_single_node_cluster_with_n1(make_cluster):
    cluster = make_cluster(1, n=1, r=1, w=1)
    db = cluster.client()
    db.put("k", "v")
    assert db.get("k").versions[0].text == "v"


def test_quorum_read_sees_latest_acknowledged_write(cluster):
    # R + W > N: any read quorum overlaps the last write quorum.
    db = cluster.client()
    for i in range(20):
        db.put("k", f"v{i}")
        assert [v.text for v in db.get("k").versions] == [f"v{i}"]
