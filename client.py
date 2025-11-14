#!/usr/bin/env python3
import grpc
import sys
import random
import argparse
import dynamo_pb2
import dynamo_pb2_grpc
from pprint import pprint

NODES = {
    "node-1": "localhost:50051",
    "node-2": "localhost:50052",
    "node-3": "localhost:50053",
    # node-4,5,... will auto-join via gossip
}

def get_stub(addr):
    channel = grpc.insecure_channel(addr)
    return dynamo_pb2_grpc.DynamoServiceStub(channel)

# -------------------------------------------------------------------
# Utility: pretty print vector clock
# -------------------------------------------------------------------
def format_clock(clock_proto):
    return {entry.node_id: entry.counter for entry in clock_proto.clock}

def choose_random_node():
    """Pick a random live node from the local list."""
    return random.choice(list(NODES.values()))

# -------------------------------------------------------------------
# PUT
# -------------------------------------------------------------------
def do_put(key, value):
    node = choose_random_node()
    stub = get_stub(node)

    req = dynamo_pb2.PutRequest(
        key=key,
        data=dynamo_pb2.ValueWithContext(
            value=value.encode("utf-8"),
            context=dynamo_pb2.Context()   # empty context for new writes
        )
    )

    print(f"[CLIENT] PUT → {node} : {key} = {value}")

    try:
        reply = stub.Put(req, timeout=4)
        print("Success:", reply.success)
        print("Message:", reply.message)
    except Exception as e:
        print("[CLIENT] PUT failed:", e)

# -------------------------------------------------------------------
# GET
# -------------------------------------------------------------------
def do_get(key):
    node = choose_random_node()
    stub = get_stub(node)
    print(f"[CLIENT] GET → {node} : {key}")

    try:
        reply = stub.Get(dynamo_pb2.GetRequest(key=key), timeout=4)
    except Exception as e:
        print("[CLIENT] GET failed:", e)
        return

    if not reply.found:
        print("Key NOT FOUND:", reply.message)
        return

    print(f"\nFound {len(reply.data)} version(s):\n")

    for i, version in enumerate(reply.data):
        val = version.value.decode("utf-8", errors="ignore")
        clock = format_clock(version.context)
        print(f"--- Version {i} ---")
        print("Value: ", val)
        print("Vector Clock:", clock)
        print()

# -------------------------------------------------------------------
# MEMBERSHIP VIEW via Gossip RPC
# -------------------------------------------------------------------
def view_membership():
    node = choose_random_node()
    stub = get_stub(node)
    print(f"[CLIENT] VIEW MEMBERSHIP → {node}")

    try:
        empty = dynamo_pb2.MembershipList()
        resp = stub.Gossip(empty, timeout=4)
    except Exception as e:
        print("Error:", e)
        return

    print("\n--- Cluster Membership ---")
    for m in resp.members:
        status = "UP" if m.up else "DOWN"
        print(f"{m.node_id:<10}  {m.address:<20}  gen={m.generation:<5}  {status}")
    print()

# -------------------------------------------------------------------
# Interactive CLI
# -------------------------------------------------------------------
def repl():
    print("DynamoDB-like Client CLI")
    print("Commands:")
    print("  put <key> <value>")
    print("  get <key>")
    print("  members")
    print("  exit")
    print()

    while True:
        try:
            cmd = input("dynamo> ").strip().split()
        except EOFError:
            break

        if not cmd:
            continue

        op = cmd[0].lower()

        if op == "put":
            if len(cmd) < 3:
                print("Usage: put <key> <value>")
                continue
            key = cmd[1]
            value = " ".join(cmd[2:])
            do_put(key, value)

        elif op == "get":
            if len(cmd) != 2:
                print("Usage: get <key>")
                continue
            do_get(cmd[1])

        elif op == "members":
            view_membership()

        elif op == "exit":
            print("Bye!")
            break

        else:
            print("Unknown command:", op)

# -------------------------------------------------------------------
# Batch mode
# -------------------------------------------------------------------
def batch_mode(args):
    if args.op == "put":
        do_put(args.key, args.value)
    elif args.op == "get":
        do_get(args.key)
    elif args.op == "members":
        view_membership()

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("op", nargs="?", help="put/get/members")
    parser.add_argument("key", nargs="?")
    parser.add_argument("value", nargs="?")
    args = parser.parse_args()

    if args.op is None:
        repl()
    else:
        batch_mode(args)
