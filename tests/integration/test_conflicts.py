"""Vector-clock conflict detection and resolution."""

import threading

import pytest

from dynamo.versioning import dot_of, supersedes

from .conftest import wait_until

pytestmark = pytest.mark.integration


def test_concurrent_writes_with_same_context_become_siblings(cluster):
    db1, db2 = cluster.client("node-1"), cluster.client("node-2")
    db1.put("cart", "empty")
    base = db1.get("cart")

    # Two clients both update the version they read, unaware of each other.
    db1.put("cart", "apples", context=base.context)
    db2.put("cart", "bananas", context=base.context)

    result = cluster.client("node-3").get("cart")
    assert result.has_conflict
    assert sorted(v.text for v in result.versions) == ["apples", "bananas"]


def test_write_with_merged_context_resolves_siblings(cluster):
    db1, db2 = cluster.client("node-1"), cluster.client("node-2")
    db1.put("cart", "empty")
    base = db1.get("cart")
    db1.put("cart", "apples", context=base.context)
    db2.put("cart", "bananas", context=base.context)

    siblings = db1.get("cart")
    merged = ",".join(sorted(v.text for v in siblings.versions))
    db1.put("cart", merged, context=siblings.context)

    result = db2.get("cart")
    assert [v.text for v in result.versions] == ["apples,bananas"]
    wait_until(lambda: all(cluster.values_on(n, "cart") == ["apples,bananas"] for n in cluster.ids))


def test_stale_context_does_not_overwrite_newer_value(cluster):
    db = cluster.client("node-1")
    db.put("k", "v1")
    stale = db.get("k")
    db.put("k", "v2")
    db.put("k", "from-stale-reader", context=stale.context)
    assert sorted(v.text for v in db.get("k").versions) == ["from-stale-reader", "v2"]


def test_blind_write_supersedes_existing_siblings(cluster):
    db = cluster.client("node-1")
    db.put("k", "v")
    base = db.get("k")
    db.put("k", "a", context=base.context)
    cluster.client("node-2").put("k", "b", context=base.context)
    assert db.get("k").has_conflict
    db.put("k", "blind")
    assert [v.text for v in db.get("k").versions] == ["blind"]


def test_concurrent_writes_on_one_coordinator_are_never_lost_or_colliding(cluster):
    # Regression: two concurrent PUTs on the same coordinator used to read the
    # same clock and both produce {node: k+1} with different values.
    db_clients = [cluster.client("node-1") for _ in range(8)]
    barrier = threading.Barrier(len(db_clients))

    def writer(i, db):
        barrier.wait()
        for j in range(10):
            db.put("contended", f"w{i}-{j}")

    threads = [threading.Thread(target=writer, args=(i, db)) for i, db in enumerate(db_clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    raw = cluster.node("node-1").storage.get("contended")
    dots = [dot_of(v) for v in raw]
    assert None not in dots
    assert len(set(dots)) == len(dots), "two different values share one write identity"
    for a in raw:
        for b in raw:
            assert a is b or not supersedes(a, b), "a surviving version supersedes another"
    # At most one survivor per concurrent writer.
    assert 1 <= len(raw) <= 8


def test_writes_from_stale_context_on_same_coordinator_are_kept(cluster):
    db = cluster.client("node-1")
    db.put("k", "v1")
    stale = db.get("k")
    for i in range(3):
        db.put("k", f"stale-{i}", context=stale.context)
    texts = sorted(v.text for v in db.get("k").versions)
    assert texts == ["stale-0", "stale-1", "stale-2"]
