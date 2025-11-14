# join_node.py
import sys
import time
import grpc
import dynamo_pb2
import dynamo_pb2_grpc

if len(sys.argv) != 4:
    print("Usage: python join_node.py <seed_addr> <new_node_id> <new_node_addr>")
    print("Example: python join_node.py localhost:50051 node-4 localhost:50054")
    sys.exit(1)

seed_addr = sys.argv[1]
new_node_id = sys.argv[2]
new_node_addr = sys.argv[3]

# build MembershipList containing the new node
ml = dynamo_pb2.MembershipList()
ns = dynamo_pb2.NodeState()
ns.node_id = new_node_id
ns.address = new_node_addr
ns.generation = 1
ns.last_updated_time = int(time.time() * 1000)
ns.up = True
ml.members.append(ns)

try:
    ch = grpc.insecure_channel(seed_addr)
    stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
    resp = stub.Gossip(ml, timeout=3.0)
    print("Gossip reply received from seed (merged snapshot).")
except Exception as e:
    print("Failed to contact seed:", e)
