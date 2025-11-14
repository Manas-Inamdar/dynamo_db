# test_put_get.py
import grpc, time, dynamo_pb2, dynamo_pb2_grpc, sys

# Usage: ensure nodes node-1..node-3 are running on 50051..50053
SEED = "localhost:50051"

def put(node_addr, key, value):
    ch = grpc.insecure_channel(node_addr)
    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
    # empty context for new key
    req = dynamo_pb2.PutRequest(key=key, data=dynamo_pb2.ValueWithContext(
        value=value.encode('utf-8'), context=dynamo_pb2.Context()))
    r = stub.Put(req, timeout=3)
    return r

def get(node_addr, key):
    ch = grpc.insecure_channel(node_addr)
    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
    r = stub.Get(dynamo_pb2.GetRequest(key=key), timeout=3)
    return r

def main():
    print("PUT via node-1")
    r = put("localhost:50051", "test:putget", "value1")
    print("PUT reply:", r.success, r.message)
    time.sleep(0.5)
    print("GET via node-2")
    g = get("localhost:50052", "test:putget")
    print("GET found:", g.found, "versions:", len(g.data))
    assert g.found
    print("OK: put/get test passed")

if __name__ == "__main__":
    main()
