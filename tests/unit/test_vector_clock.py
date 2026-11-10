import pytest

from dynamo.proto.dynamo_pb2 import Context, VectorClockItem
from dynamo.vector_clock import Ordering, VectorClock


def vc(**entries: int) -> VectorClock:
    return VectorClock({k.replace("_", "-"): v for k, v in entries.items()})


class TestCompare:
    def test_empty_clocks_are_equal(self):
        assert VectorClock().compare(VectorClock()) is Ordering.EQUAL

    def test_increment_happens_after(self):
        a = vc(n1=1)
        b = a.increment("n1")
        assert a.compare(b) is Ordering.BEFORE
        assert b.compare(a) is Ordering.AFTER

    def test_missing_entry_counts_as_zero(self):
        assert vc(n1=1).compare(vc(n1=1, n2=1)) is Ordering.BEFORE

    def test_concurrent(self):
        assert vc(n1=2, n2=1).compare(vc(n1=1, n2=2)) is Ordering.CONCURRENT

    def test_descends_from_includes_equal(self):
        assert vc(n1=1).descends_from(vc(n1=1))
        assert vc(n1=2).descends_from(vc(n1=1))
        assert not vc(n1=1).descends_from(vc(n2=1))


class TestOperations:
    def test_increment_returns_new_clock(self):
        a = vc(n1=1)
        b = a.increment("n1")
        assert a.get("n1") == 1 and b.get("n1") == 2

    def test_merge_is_elementwise_max(self):
        merged = vc(n1=3, n2=1).merge(vc(n1=1, n2=4, n3=2))
        assert merged.as_dict() == {"n1": 3, "n2": 4, "n3": 2}

    def test_merge_dominates_both_inputs(self):
        a, b = vc(n1=2), vc(n2=2)
        merged = a.merge(b)
        assert merged.descends_from(a) and merged.descends_from(b)

    def test_merge_all(self):
        assert VectorClock.merge_all([vc(a=1), vc(b=2), vc(a=3)]).as_dict() == {"a": 3, "b": 2}

    def test_zero_entries_dropped(self):
        assert VectorClock({"n1": 0}) == VectorClock()

    def test_negative_counter_rejected(self):
        with pytest.raises(ValueError):
            VectorClock({"n1": -1})


class TestProto:
    def test_roundtrip(self):
        clock = vc(n2=5, n1=3)
        assert VectorClock.from_proto(clock.to_proto()) == clock

    def test_serialization_is_canonical(self):
        # Regression: equal clocks built in different insertion orders used to
        # serialize differently, so replicas treated them as different versions.
        a = VectorClock({"b": 1, "a": 2}).to_proto().SerializeToString()
        b = VectorClock({"a": 2, "b": 1}).to_proto().SerializeToString()
        assert a == b

    def test_from_proto_tolerates_duplicates(self):
        ctx = Context(clock=[VectorClockItem(node_id="n1", counter=1),
                             VectorClockItem(node_id="n1", counter=3)])
        assert VectorClock.from_proto(ctx).get("n1") == 3

    def test_hash_and_eq(self):
        assert len({vc(a=1, b=2), VectorClock({"b": 2, "a": 1})}) == 1
