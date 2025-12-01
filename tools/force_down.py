#!/usr/bin/env python3
import sys, os, time
import grpc

# Add parent folder so imports work
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import dynamo_pb2
import dynamo_pb2_grpc

def force_down(target):
    print(f"[FORCE] Marking {target} DOWN and forcing ring removal...")

    # Make last_ts FAR enough in the past so removal condition triggers ( > REMOVE_TIMEOUT_MS)
    old_ts = int(time.time()*1000) - (5 * 60 * 1000)   # 5 minutes ago
    generation = int(time.time())

    ml = dynamo_pb2.MembershipList()
    ns = dynamo_pb2.NodeState(
        node_id=target,
        address="",
        generation=generation,
        last_updated_time=old_ts,
        up=False
    )
    ml.members.append(ns)

    # send gossip to live nodes so they update membership + ring
    for port in [50051, 50052]:
        try:
            ch = grpc.insecure_channel(f"localhost:{port}")
            stub = dynamo_pb2_grpc.DynamoServiceStub(ch)
            stub.Gossip(ml, timeout=1)
            print(f"[OK] Forced DOWN injected via node on port {port}")
        except Exception as e:
            print(f"[WARN] Node on port {port} unreachable: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: force_down.py node-id")
        sys.exit(1)
    force_down(sys.argv[1])
