import statistics
import threading

import pytest

from dynamo.ring import HashRing, hash_key


@pytest.fixture
def ring():
    r = HashRing(vnodes=32)
    for i in range(1, 6):
        r.add_node(f"node-{i}", f"localhost:5005{i}")
    return r


def test_empty_ring():
    assert HashRing().preference_list("k", 3) == []


def test_invalid_vnodes():
    with pytest.raises(ValueError):
        HashRing(vnodes=0)


def test_preference_list_has_distinct_nodes(ring):
    for i in range(200):
        pref = ring.preference_list(f"key-{i}", 3)
        assert len(pref) == 3 and len(set(pref)) == 3


def test_preference_list_is_deterministic(ring):
    assert ring.preference_list("apple", 3) == ring.preference_list("apple", 3)


def test_full_walk_returns_every_node(ring):
    full = ring.preference_list("apple")
    assert sorted(full) == sorted(ring.nodes())
    assert full[:3] == ring.preference_list("apple", 3)


def test_n_larger_than_cluster(ring):
    assert len(ring.preference_list("k", 10)) == 5


def test_add_existing_node_updates_address(ring):
    assert ring.add_node("node-1", "otherhost:1") is False
    assert ring.address_of("node-1") == "otherhost:1"
    assert len(ring) == 5


def test_remove_node(ring):
    assert ring.remove_node("node-3") is True
    assert ring.remove_node("node-3") is False
    assert "node-3" not in ring
    assert all("node-3" not in ring.preference_list(f"k{i}", 3) for i in range(100))


def test_removal_only_moves_keys_of_removed_node(ring):
    keys = [f"key-{i}" for i in range(2000)]
    before = {k: ring.primary(k) for k in keys}
    ring.remove_node("node-2")
    for k in keys:
        if before[k] != "node-2":
            assert ring.primary(k) == before[k], "consistent hashing moved an unaffected key"


def test_join_moves_about_one_over_n_keys(ring):
    keys = [f"key-{i}" for i in range(5000)]
    before = {k: ring.primary(k) for k in keys}
    ring.add_node("node-6", "localhost:50056")
    moved = sum(before[k] != ring.primary(k) for k in keys)
    # Ideal is 1/6 of the keys; allow for vnode variance.
    assert 0.08 < moved / len(keys) < 0.28
    assert all(ring.primary(k) == "node-6" for k in keys if before[k] != ring.primary(k))


def test_vnodes_balance_load():
    loads = {}
    for vnodes in (1, 128):
        r = HashRing(vnodes=vnodes)
        for i in range(5):
            r.add_node(f"n{i}", "x")
        counts = {f"n{i}": 0 for i in range(5)}
        for k in range(10000):
            counts[r.primary(f"key-{k}")] += 1
        loads[vnodes] = statistics.pstdev(counts.values()) / statistics.mean(counts.values())
    assert loads[128] < loads[1]
    assert loads[128] < 0.15


def test_hash_key_range():
    assert 0 <= hash_key("anything") < 2**160


def test_concurrent_modification_and_lookup(ring):
    errors = []

    def churn():
        for i in range(200):
            ring.add_node(f"tmp-{i % 3}", "x")
            ring.remove_node(f"tmp-{i % 3}")

    def lookup():
        try:
            for i in range(2000):
                pref = ring.preference_list(f"k{i}", 3)
                assert len(set(pref)) == len(pref) == 3
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=churn)] + [threading.Thread(target=lookup) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
