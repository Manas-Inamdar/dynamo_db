import pytest

from dynamo.config import NodeConfig


def test_defaults_are_valid():
    cfg = NodeConfig(node_id="n1", port=50051)
    assert cfg.address == "localhost:50051"
    assert cfg.strongly_consistent_quorums


@pytest.mark.parametrize("kwargs", [
    {"node_id": ""},
    {"port": 0},
    {"port": 70000},
    {"n": 0},
    {"r": 4},
    {"w": 0},
    {"vnodes": 0},
    {"rpc_timeout": 0},
    {"gossip_interval": 2.0, "fail_timeout": 1.0},
    {"probe_dead_probability": 1.5},
])
def test_invalid_configs_rejected(kwargs):
    base = {"node_id": "n1", "port": 50051}
    with pytest.raises(ValueError):
        NodeConfig(**{**base, **kwargs})


def test_weak_quorum_detected():
    assert not NodeConfig(node_id="n1", port=1, n=3, r=1, w=1).strongly_consistent_quorums
