"""Gossip, failure detection, join and leave."""

import pytest

from dynamo.client import DynamoClient

from .conftest import free_port, wait_until

pytestmark = pytest.mark.integration


def test_all_nodes_discover_each_other_via_seeds(make_cluster):
    cluster = make_cluster(5)
    for node_id in cluster.ids:
        assert set(cluster.node(node_id).ring.nodes()) == set(cluster.ids)


def test_client_discovers_cluster_from_one_seed(cluster):
    with DynamoClient([cluster.address("node-1")]) as db:
        assert [m.node_id for m in db.members()] == cluster.ids
        db.refresh_members()
        db.put("k", "v")


def test_crash_is_detected_and_recovery_noticed(cluster):
    cluster.stop("node-2")
    cluster.wait_dead("node-2")
    assert "node-2" in cluster.node("node-1").ring, "dead nodes stay on the ring (sloppy quorum)"
    cluster.start("node-2")
    cluster.wait_converged()


def test_client_fails_over_to_live_node(cluster):
    db = cluster.client("node-1", "node-2")
    cluster.stop("node-1")
    for i in range(5):
        db.put(f"k{i}", "v")
        assert db.get(f"k{i}").found


def test_new_node_joins_ring(make_cluster):
    cluster = make_cluster(3)
    cluster.ids.append("node-4")
    cluster.ports["node-4"] = free_port()
    cluster.start("node-4")
    cluster.wait_converged()
    db = cluster.client()
    for i in range(30):
        db.put(f"after-join-{i}", "v")
    assert any(cluster.node("node-4").storage.get(f"after-join-{i}") for i in range(30))


def test_leave_removes_node_from_ring_and_restart_rejoins(cluster):
    db = cluster.client("node-1")
    db.leave("node-3")
    wait_until(lambda: all("node-3" not in cluster.node(n).ring for n in ("node-1", "node-2")),
               message="node-3 removed from every ring")
    members = {m.node_id: m.up for m in db.members()}
    assert members["node-3"] is False

    cluster.restart("node-3")
    wait_until(lambda: all("node-3" in cluster.node(n).ring for n in cluster.ids),
               message="restarted node rejoins")


def test_leave_of_crashed_node(cluster):
    cluster.stop("node-3")
    cluster.wait_dead("node-3")
    cluster.client("node-1").leave("node-3")
    wait_until(lambda: all("node-3" not in cluster.node(n).ring for n in cluster.running))


def test_partitioned_nodes_heal(cluster):
    # Each node wrongly believes every other node is dead (e.g. after a
    # network blip). Probing dead nodes must reconnect them.
    for a in cluster.ids:
        for b in cluster.ids:
            cluster.node(a).membership.mark_dead(b)
    cluster.wait_converged()


def test_second_process_cannot_bind_a_used_port(cluster):
    # Regression: gRPC's default SO_REUSEPORT let a duplicate node bind the
    # same port, after which both processes fought over the node id.
    from dynamo.server import NodeServer

    with pytest.raises(OSError):
        NodeServer(cluster.config("node-1"))
