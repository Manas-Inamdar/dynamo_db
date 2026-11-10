"""Sloppy quorum and hinted handoff."""

import pytest

from .conftest import wait_until

pytestmark = pytest.mark.integration


def key_with_replicas(cluster, wanted_in, not_in=()):
    """A key whose preference list contains ``wanted_in`` and not ``not_in``."""
    for i in range(10_000):
        key = f"key-{i}"
        pref = cluster.preference(key)
        if all(n in pref for n in wanted_in) and not any(n in pref for n in not_in):
            return key
    raise AssertionError("no suitable key")


def test_standin_holds_hint_and_hands_it_back(make_cluster):
    cluster = make_cluster(4)
    key = key_with_replicas(cluster, wanted_in=["node-4"], not_in=["node-1"])
    cluster.stop("node-4")
    cluster.wait_dead("node-4")

    cluster.client("node-1").put(key, "written-while-down")
    # The 4th node on the ring (node-1, outside the top 3) is the stand-in.
    # Put returns after W acks, so the third (hinted) write may still be in flight.
    wait_until(lambda: cluster.node("node-1").hints.count("node-4") == 1,
               message="stand-in stored the hint")
    assert cluster.client("node-1").get(key).versions[0].text == "written-while-down"

    cluster.start("node-4")
    wait_until(lambda: cluster.values_on("node-4", key) == ["written-while-down"],
               message="hint delivered to recovered node")
    wait_until(lambda: cluster.node("node-1").hints.count() == 0, message="hint cleared")
    assert cluster.node("node-4").stats().get("hints_received", 0) >= 1


def test_coordinator_keeps_hint_when_no_standin(cluster):
    # 3 nodes, N=3: there is no 4th node to stand in.
    cluster.stop("node-3")
    cluster.wait_dead("node-3")
    db = cluster.client("node-1")
    for i in range(10):
        db.put(f"k{i}", f"v{i}")
    wait_until(lambda: cluster.node("node-1").hints.count("node-3") == 10)

    cluster.start("node-3")
    wait_until(lambda: all(cluster.values_on("node-3", f"k{i}") == [f"v{i}"] for i in range(10)),
               message="all hints delivered")
    assert cluster.node("node-1").hints.count() == 0


def test_hint_detected_on_rpc_failure_before_failure_detector(make_cluster):
    # Gossip is slowed down so the coordinator still believes node-3 is alive
    # when it sends the write: the failed RPC must fall back to a hint.
    cluster = make_cluster(3, gossip_interval=5.0, fail_timeout=30.0, probe_dead_probability=0.0)
    cluster.stop("node-3")
    cluster.client("node-1").put("k", "v")
    wait_until(lambda: cluster.node("node-1").hints.count("node-3") == 1)


def test_hints_survive_standin_restart(make_cluster):
    cluster = make_cluster(3)
    cluster.stop("node-3")
    cluster.wait_dead("node-3")
    cluster.client("node-1").put("k", "durable-hint")
    wait_until(lambda: cluster.node("node-1").hints.count("node-3") == 1)
    cluster.restart("node-1")
    assert cluster.node("node-1").hints.count("node-3") == 1
    cluster.start("node-3")
    wait_until(lambda: cluster.values_on("node-3", "k") == ["durable-hint"],
               message="hint delivered after the stand-in restarted")


def test_newer_write_while_down_wins_after_handoff(cluster):
    db = cluster.client("node-1")
    db.put("k", "v1")
    wait_until(lambda: cluster.values_on("node-3", "k") == ["v1"])
    cluster.stop("node-3")
    cluster.wait_dead("node-3")
    db.put("k", "v2")
    db.put("k", "v3")
    cluster.start("node-3")
    wait_until(lambda: cluster.values_on("node-3", "k") == ["v3"],
               message="stale replica superseded by handed-off write")
