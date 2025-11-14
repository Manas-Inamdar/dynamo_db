# leave_node.py
import grpc
import time
import dynamo_pb2
import dynamo_pb2_grpc
import sys

if len(sys.argv) != 3:
    print("Usage: python leave_node.py <seed_addr> <node_id>")
    exit(1)

seed_addr = sys.argv[1]        # e.g. localhost:50051
node_id = sys.argv[2]

channel = grpc.insecure_channel(seed_addr)
stub = dynamo_pb2_grpc.DynamoServiceStub(channel)

# Build a membership update marking node as DOWN.
ns = dynamo_pb2.NodeState()
ns.node_id = node_id
ns.address = ""                 # leaving node is not reachable
ns.generation = 1               # bump to a LOW, safe value
ns.last_updated_time = int(time.time() * 1000)
ns.up = False

req = dynamo_pb2.MembershipList()
req.members.append(ns)

resp = stub.Gossip(req)

print("Leave request sent. Updated membership:")
for m in resp.members:
    print(f"{m.node_id}: up={m.up}, gen={m.generation}")
