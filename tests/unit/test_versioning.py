from dynamo.proto.dynamo_pb2 import ValueWithContext
from dynamo.vector_clock import VectorClock
from dynamo.versioning import merged_clock, reconcile, same_versions, version_key


def v(value: str, **clock: int) -> ValueWithContext:
    return ValueWithContext(value=value.encode(), context=VectorClock(clock).to_proto())


def values(versions):
    return sorted(x.value.decode() for x in versions)


def test_empty():
    assert reconcile([]) == []


def test_single_version_kept():
    assert values(reconcile([v("a", n1=1)])) == ["a"]


def test_duplicates_collapsed():
    assert values(reconcile([v("a", n1=1), v("a", n1=1)])) == ["a"]


def test_dominated_version_dropped():
    assert values(reconcile([v("old", n1=1), v("new", n1=2)])) == ["new"]


def test_concurrent_versions_kept_as_siblings():
    assert values(reconcile([v("x", n1=1), v("y", n2=1)])) == ["x", "y"]


def test_merge_resolves_siblings():
    resolved = v("xy", n1=1, n2=1, n3=1)
    assert values(reconcile([v("x", n1=1), v("y", n2=1), resolved])) == ["xy"]


def test_same_clock_different_values_both_kept():
    # Should never be produced by a coordinator, but must not lose data.
    assert values(reconcile([v("a", n1=1), v("b", n1=1)])) == ["a", "b"]


def test_output_is_deterministic():
    a = reconcile([v("x", n1=1), v("y", n2=1), v("z", n3=1)])
    b = reconcile([v("z", n3=1), v("x", n1=1), v("y", n2=1)])
    assert [x.SerializeToString() for x in a] == [x.SerializeToString() for x in b]


def test_chain_of_many_versions():
    versions = [v(f"v{i}", n1=i) for i in range(1, 50)]
    assert values(reconcile(versions)) == ["v49"]


def test_same_versions_ignores_order():
    assert same_versions([v("a", n1=1), v("b", n2=1)], [v("b", n2=1), v("a", n1=1)])
    assert not same_versions([v("a", n1=1)], [v("a", n1=2)])


def test_version_key_uses_canonical_clock():
    a = ValueWithContext(value=b"x", context=VectorClock({"b": 1, "a": 1}).to_proto())
    b = ValueWithContext(value=b"x", context=VectorClock({"a": 1, "b": 1}).to_proto())
    assert version_key(a) == version_key(b)


def test_merged_clock():
    assert merged_clock([v("x", n1=2), v("y", n2=3)]).as_dict() == {"n1": 2, "n2": 3}


# ---------------------------------------------------------------------------
# Dotted version vectors
# ---------------------------------------------------------------------------
from dynamo.proto.dynamo_pb2 import VectorClockItem  # noqa: E402
from dynamo.versioning import dot_of, full_clock, supersedes  # noqa: E402


def dv(value: str, dot: tuple[str, int], **context: int) -> ValueWithContext:
    return ValueWithContext(value=value.encode(), context=VectorClock(context).to_proto(),
                            dot=VectorClockItem(node_id=dot[0], counter=dot[1]))


class TestDottedVersionVectors:
    def test_dot_and_full_clock(self):
        x = dv("x", ("n1", 3), n1=1, n2=4)
        assert dot_of(x) == ("n1", 3)
        assert full_clock(x).as_dict() == {"n1": 3, "n2": 4}
        assert dot_of(v("legacy", n1=1)) is None

    def test_write_that_read_previous_supersedes_it(self):
        v1 = dv("v1", ("n1", 1))
        v2 = dv("v2", ("n1", 2), n1=1)
        assert supersedes(v2, v1) and not supersedes(v1, v2)
        assert values(reconcile([v1, v2])) == ["v2"]

    def test_stale_context_on_same_coordinator_creates_sibling(self):
        # Regression for the lost update a plain vector clock causes:
        # v2 was written after v1; a client still holding v1's context writes
        # again through the same coordinator. Neither has seen the other.
        v1 = dv("v1", ("n1", 1))
        v2 = dv("v2", ("n1", 2), n1=1)
        stale = dv("stale", ("n1", 3), n1=1)
        assert not supersedes(stale, v2) and not supersedes(v2, stale)
        assert values(reconcile([v1, v2, stale])) == ["stale", "v2"]

    def test_merged_context_resolves_dvv_siblings(self):
        a = dv("a", ("n1", 2), n1=1)
        b = dv("b", ("n1", 3), n1=1)
        resolved = dv("ab", ("n2", 1), **merged_clock([a, b]).as_dict())
        assert values(reconcile([a, b, resolved])) == ["ab"]

    def test_merged_clock_uses_full_clocks(self):
        assert merged_clock([dv("a", ("n1", 5), n2=1)]).as_dict() == {"n1": 5, "n2": 1}

    def test_same_dot_is_never_superseded_by_itself(self):
        x = dv("x", ("n1", 1))
        assert not supersedes(x, x)

    def test_mutually_superseding_corrupt_versions_both_kept(self):
        a = dv("a", ("n1", 1), n2=1)
        b = dv("b", ("n2", 1), n1=1)
        assert values(reconcile([a, b])) == ["a", "b"]

    def test_dotted_version_supersedes_legacy_version(self):
        legacy = v("legacy", n1=1)
        new = dv("new", ("n1", 2), n1=1)
        assert values(reconcile([legacy, new])) == ["new"]
