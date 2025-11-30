#!/usr/bin/env python3
import sys, os, time
import grpc

# Add parent folder so imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dynamo_pb2
import dynamo_pb2_grpc

def force_down(target):
    print(f"[FORCE] Marking {target} DOWN and forcing ring removal...")

    # Trick: set last_ts far in the past so every node thinks target timed out
    old_ts = int(time.time()*1000) - 60000   # 60 seconds ago
    generation = int(time.time())           # bump generation

    ml = dynamo_pb2.MembershipList()
    ns = dynamo_pb2.NodeState(
        node_id=target,
        address="",
        generation=generation,
        last_updated_time=old_ts,
        up=False
    )
    ml.members.append(ns)

    for port in [50051, 50052]:
        try:
            ch = grpc.insecure_channel(f"localhost:{port}")
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
            stub.Gossip(ml, timeout=1)
            print(f"[OK] Forced DOWN injected via node on port {port}")
        except:
            print(f"[WARN] Node on port {port} unreachable")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: force_down.py node-id")
        sys.exit(1)
    force_down(sys.argv[1])
