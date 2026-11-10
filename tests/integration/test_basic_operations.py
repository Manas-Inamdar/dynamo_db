"""PUT/GET semantics through every coordinator."""

import grpc
import pytest

from dynamo.client import DynamoError
from dynamo.proto import dynamo_pb2 as pb
from dynamo.proto import dynamo_pb2_grpc

pytestmark = pytest.mark.integration


def test_put_then_get_from_every_coordinator(cluster):
    cluster.client("node-1").put("user:1", "alice")
    for node_id in cluster.ids:
        result = cluster.client(node_id).get("user:1")
        assert [v.text for v in result.versions] == ["alice"]


def test_get_missing_key(cluster):
    result = cluster.client().get("does-not-exist")
    assert not result.found and result.versions == []


def test_overwrite_supersedes_previous_value(cluster):
    db = cluster.client()
    for value in ("v1", "v2", "v3"):
        db.put("k", value)
    result = db.get("k")
    assert [v.text for v in result.versions] == ["v3"]


def test_overwrite_through_different_coordinators(cluster):
    for i, node_id in enumerate(cluster.ids * 2):
        cluster.client(node_id).put("k", f"v{i}")
    assert [v.text for v in cluster.client().get("k").versions] == ["v5"]


def test_binary_and_unicode(cluster):
    db = cluster.client()
    payload = bytes(range(256)) * 100
    db.put("blob", payload)
    db.put("ключ/🔑", "значение")
    assert db.get("blob").versions[0].value == payload
    assert db.get("ключ/🔑").versions[0].text == "значение"


def test_many_keys(cluster):
    db = cluster.client()
    for i in range(100):
        db.put(f"key-{i}", f"value-{i}")
    for i in range(100):
        assert db.get(f"key-{i}").versions[0].text == f"value-{i}"


def test_empty_key_rejected(cluster):
    with pytest.raises(DynamoError):
        cluster.client().put("", "x")
    with pytest.raises(DynamoError):
        cluster.client().get("")


def test_oversized_key_rejected(cluster):
    with pytest.raises(DynamoError):
        cluster.client().put("k" * 2000, "x")


def test_raw_grpc_status_codes(cluster):
    channel = grpc.insecure_channel(cluster.address("node-1"))
    stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
    with pytest.raises(grpc.RpcError) as err:
        stub.Get(pb.GetRequest(key=""), timeout=2)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    channel.close()


def test_vector_clock_tracks_coordinators(cluster):
    cluster.client("node-1").put("k", "a")
    cluster.client("node-2").put("k", "b")
    (version,) = cluster.client().get("k").versions
    assert version.text == "b"
    assert version.clock.get("node-1") == 1 and version.clock.get("node-2") == 1
