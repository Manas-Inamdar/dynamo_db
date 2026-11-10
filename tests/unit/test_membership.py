import pytest

from dynamo.membership import Event, Membership
from dynamo.proto.dynamo_pb2 import NodeState


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clocks():
    return FakeClock(), FakeClock(1_700_000_000.0)


def make(node_id, clocks, **kw):
    mono, wall = clocks
    return Membership(node_id, f"{node_id}:1", fail_timeout=kw.get("fail", 5.0),
                      remove_timeout=kw.get("remove", 60.0), monotonic=mono, wall=wall)


def state(node_id, gen, ver, up=True, address=None):
    return NodeState(node_id=node_id, address=address or f"{node_id}:1", generation=gen,
                     last_updated_time=ver, up=up)


def test_starts_with_only_self(clocks):
    m = make("a", clocks)
    assert [x.node_id for x in m.members()] == ["a"]
    assert m.is_healthy("a")
    assert m.ring_members() == {"a": "a:1"}


def test_merge_discovers_new_node(clocks):
    m = make("a", clocks)
    assert m.merge([state("b", 1, 10)]) == [(Event.JOINED, "b")]
    assert m.is_healthy("b")
    assert m.ring_members() == {"a": "a:1", "b": "b:1"}


def test_stale_state_ignored(clocks):
    m = make("a", clocks)
    m.merge([state("b", 1, 10, address="new:1")])
    assert m.merge([state("b", 1, 5, address="stale:1")]) == []
    assert m.address_of("b") == "new:1"


def test_heartbeat_timeout_marks_dead(clocks):
    mono, _ = clocks
    m = make("a", clocks, fail=5.0)
    m.merge([state("b", 1, 10)])
    mono.advance(4.0)
    assert m.check_failures() == []
    mono.advance(2.0)
    assert m.check_failures() == ["b"]
    assert not m.is_healthy("b")
    assert "b" in m.ring_members(), "dead nodes stay on the ring until remove_timeout"


def test_advancing_heartbeat_keeps_node_alive(clocks):
    mono, _ = clocks
    m = make("a", clocks, fail=5.0)
    for ver in range(10, 20):
        m.merge([state("b", 1, ver)])
        mono.advance(3.0)
        assert m.check_failures() == []


def test_recovery_after_new_heartbeat(clocks):
    mono, _ = clocks
    m = make("a", clocks)
    m.merge([state("b", 1, 10)])
    mono.advance(10)
    m.check_failures()
    assert m.merge([state("b", 1, 11)]) == [(Event.RECOVERED, "b")]
    assert m.is_healthy("b")


def test_mark_dead_then_recover(clocks):
    m = make("a", clocks)
    m.merge([state("b", 1, 10)])
    assert m.mark_dead("b") is True
    assert m.mark_dead("b") is False
    assert m.mark_dead("a") is False, "a node never marks itself dead"
    assert m.merge([state("b", 1, 10)]) == [], "same heartbeat is not proof of life"
    assert m.merge([state("b", 1, 11)]) == [(Event.RECOVERED, "b")]


def test_removed_from_ring_after_remove_timeout(clocks):
    mono, _ = clocks
    m = make("a", clocks, fail=5.0, remove=60.0)
    m.merge([state("b", 1, 10)])
    mono.advance(6)
    m.check_failures()
    mono.advance(61)
    assert "b" not in m.ring_members()


def test_restart_with_new_generation_supersedes(clocks):
    m = make("a", clocks)
    m.merge([state("b", 5, 999_999)])
    m.mark_dead("b")
    events = m.merge([state("b", 6, 1)])
    assert (Event.RECOVERED, "b") in events
    assert m.get("b").generation == 6


def test_leave_propagates(clocks):
    m = make("a", clocks)
    m.merge([state("b", 1, 10)])
    assert m.merge([state("b", 1, 11, up=False)]) == [(Event.LEFT, "b")]
    assert not m.is_healthy("b")
    assert "b" not in m.ring_members()
    assert m.peers(alive=True) == []


def test_rejoin_after_leave_with_new_generation(clocks):
    m = make("a", clocks)
    m.merge([state("b", 1, 10)])
    m.merge([state("b", 1, 11, up=False)])
    assert m.merge([state("b", 2, 1)]) == [(Event.REJOINED, "b")]
    assert "b" in m.ring_members()


def test_self_accepts_leave_request_for_current_generation(clocks):
    m = make("a", clocks)
    me = m.me
    m.merge([state("a", me.generation, 0, up=False)])
    assert m.me.left
    assert "a" not in m.ring_members()
    assert m.me.version > me.version


def test_self_ignores_stale_or_bogus_claims(clocks):
    m = make("a", clocks)
    me = m.me
    m.merge([state("a", me.generation - 1, 10**15, up=False)])
    assert not m.me.left
    m.merge([state("a", me.generation, 10**15, up=True, address="evil:1")])
    assert m.me.address == "a:1"


def test_self_jumps_ahead_of_newer_remembered_generation(clocks):
    m = make("a", clocks)
    future = m.me.generation + 100
    m.merge([state("a", future, 1)])
    assert m.me.generation > future


def test_heartbeat_is_monotonic_even_if_wall_clock_goes_back(clocks):
    _, wall = clocks
    m = make("a", clocks)
    before = m.me.version
    wall.now -= 3600
    m.heartbeat()
    assert m.me.version == before + 1


def test_snapshot_roundtrip(clocks):
    a, b = make("a", clocks), make("b", clocks)
    b.merge(a.snapshot().members)
    a.merge(b.snapshot().members)
    assert {x.node_id for x in a.members()} == {x.node_id for x in b.members()} == {"a", "b"}


def test_peers_split_by_liveness(clocks):
    m = make("a", clocks)
    m.merge([state("b", 1, 1), state("c", 1, 1)])
    m.mark_dead("c")
    assert [p.node_id for p in m.peers(alive=True)] == ["b"]
    assert [p.node_id for p in m.peers(alive=False)] == ["c"]


def test_suspects_are_live_peers_with_stale_heartbeats(clocks):
    mono, _ = clocks
    m = make("a", clocks, fail=4.0)
    m.merge([state("b", 1, 1), state("c", 1, 1)])
    mono.advance(1.0)
    m.merge([state("c", 1, 2)])          # c heartbeats, b goes quiet
    mono.advance(1.5)
    assert [p.node_id for p in m.suspects(2.0)] == ["b"]
    m.mark_dead("b")
    assert m.suspects(2.0) == [], "dead peers are probed separately, not as suspects"
