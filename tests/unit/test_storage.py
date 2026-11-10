import threading

import pytest

from dynamo.proto.dynamo_pb2 import ValueWithContext
from dynamo.storage import DurableCounter, HintStore, Storage
from dynamo.vector_clock import VectorClock


def v(value: str, **clock: int) -> ValueWithContext:
    return ValueWithContext(value=value.encode(), context=VectorClock(clock).to_proto())


def values(versions):
    return sorted(x.value.decode() for x in versions)


@pytest.fixture(params=["memory", "disk"])
def store(request, tmp_path):
    return Storage(None if request.param == "memory" else tmp_path / "store")


class TestStorage:
    def test_get_missing(self, store):
        assert store.get("nope") == []

    def test_put_and_get(self, store):
        assert store.put("k", v("a", n1=1)) is True
        assert values(store.get("k")) == ["a"]

    def test_newer_version_replaces_older(self, store):
        store.put("k", v("old", n1=1))
        store.put("k", v("new", n1=2))
        assert values(store.get("k")) == ["new"]

    def test_older_version_is_ignored(self, store):
        store.put("k", v("new", n1=2))
        assert store.put("k", v("old", n1=1)) is False
        assert values(store.get("k")) == ["new"]

    def test_duplicate_put_reports_no_change(self, store):
        store.put("k", v("a", n1=1))
        assert store.put("k", v("a", n1=1)) is False

    def test_concurrent_versions_kept(self, store):
        store.put("k", v("x", n1=1))
        store.put("k", v("y", n2=1))
        assert values(store.get("k")) == ["x", "y"]

    def test_versions_do_not_accumulate(self, store):
        # Regression: the original stored every version forever.
        for i in range(1, 101):
            store.put("k", v(f"v{i}", n1=i))
        assert values(store.get("k")) == ["v100"]

    def test_discard_removes_only_given_versions(self, store):
        store.put("k", v("x", n1=1))
        store.put("k", v("y", n2=1))
        store.discard("k", [v("x", n1=1)])
        assert values(store.get("k")) == ["y"]
        store.discard("k", [v("y", n2=1)])
        assert store.get("k") == [] and "k" not in store

    def test_get_returns_copy(self, store):
        store.put("k", v("a", n1=1))
        store.get("k").clear()
        assert len(store.get("k")) == 1

    def test_max_counter(self, store):
        store.put("a", v("x", n1=3, n2=7))
        store.put("b", v("y", n1=5))
        assert store.max_counter("n1") == 5 and store.max_counter("n2") == 7
        assert store.max_counter("n9") == 0

    def test_parallel_writes_to_one_key(self, store):
        def writer(node):
            for i in range(1, 51):
                store.put("k", v(f"{node}-{i}", **{node: i}))

        threads = [threading.Thread(target=writer, args=(f"n{i}",)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert values(store.get("k")) == ["n0-50", "n1-50", "n2-50", "n3-50"]


class TestDurability:
    def test_reload_from_disk(self, tmp_path):
        s1 = Storage(tmp_path)
        s1.put("user:1", v("alice", n1=1))
        s1.put("user:1", v("bob", n1=2))
        s1.put("üñíçødé / key\nwith newline", v("ok", n2=1))
        s2 = Storage(tmp_path)
        assert values(s2.get("user:1")) == ["bob"]
        assert values(s2.get("üñíçødé / key\nwith newline")) == ["ok"]

    def test_one_file_per_key(self, tmp_path):
        s = Storage(tmp_path)
        for i in range(1, 21):
            s.put("k", v(f"v{i}", n1=i))
        s.put("other", v("x", n1=1))
        assert len(list(tmp_path.glob("*.rec"))) == 2

    def test_discard_last_version_deletes_file(self, tmp_path):
        s = Storage(tmp_path)
        s.put("k", v("x", n1=1))
        s.discard("k", [v("x", n1=1)])
        assert list(tmp_path.glob("*.rec")) == []

    def test_corrupt_and_temp_files_are_skipped(self, tmp_path):
        Storage(tmp_path).put("good", v("x", n1=1))
        (tmp_path / "garbage.rec").write_bytes(b"\xff\x00not a protobuf")
        (tmp_path / "half-written.rec.123.tmp").write_bytes(b"partial")
        s = Storage(tmp_path)
        assert values(s.get("good")) == ["x"]
        assert not list(tmp_path.glob("*.tmp"))

    def test_fsync_mode(self, tmp_path):
        s = Storage(tmp_path, fsync=True)
        s.put("k", v("x", n1=1))
        assert values(Storage(tmp_path).get("k")) == ["x"]


class TestHintStore:
    def test_add_and_pending(self, tmp_path):
        h = HintStore(tmp_path)
        h.add("node-2", "k1", v("a", n1=1))
        h.add("node-2", "k2", v("b", n1=2))
        h.add("node-3", "k1", v("a", n1=1))
        assert sorted(h.targets()) == ["node-2", "node-3"]
        assert sorted(k for k, _ in h.pending("node-2")) == ["k1", "k2"]
        assert h.count() == 3 and h.count("node-2") == 2 and h.count("nobody") == 0

    def test_newer_hint_supersedes_older(self):
        h = HintStore(None)
        h.add("node-2", "k", v("old", n1=1))
        h.add("node-2", "k", v("new", n1=2))
        assert [values(vs) for _, vs in h.pending("node-2")] == [["new"]]

    def test_discard_and_targets(self):
        h = HintStore(None)
        h.add("node-2", "k", v("a", n1=1))
        h.discard("node-2", "k", [v("a", n1=1)])
        assert h.targets() == []

    def test_versions_for_key_across_targets(self):
        h = HintStore(None)
        h.add("node-2", "k", v("a", n1=1))
        h.add("node-3", "k", v("b", n2=1))
        assert values(h.versions_for_key("k")) == ["a", "b"]

    def test_hints_survive_restart(self, tmp_path):
        HintStore(tmp_path).add("node/with:odd chars", "k", v("a", n1=1))
        h = HintStore(tmp_path)
        assert h.targets() == ["node/with:odd chars"]
        assert h.max_counter("n1") == 1


class TestDurableCounter:
    def test_strictly_increasing(self):
        c = DurableCounter(None)
        values_ = [c.next_after(0) for _ in range(5)]
        assert values_ == [1, 2, 3, 4, 5]

    def test_respects_observed(self):
        c = DurableCounter(None)
        assert c.next_after(41) == 42
        assert c.next_after(10) == 43

    def test_never_repeats_after_restart(self, tmp_path):
        path = tmp_path / "counter"
        c1 = DurableCounter(path, block=10)
        used = [c1.next_after(0) for _ in range(25)]
        c2 = DurableCounter(path, block=10)
        assert c2.next_after(0) > max(used)

    def test_floor(self):
        assert DurableCounter(None, floor=100).next_after(0) == 101

    def test_thread_safety(self):
        c = DurableCounter(None)
        out, lock = [], threading.Lock()

        def worker():
            local = [c.next_after(0) for _ in range(500)]
            with lock:
                out.extend(local)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(out)) == 4000
