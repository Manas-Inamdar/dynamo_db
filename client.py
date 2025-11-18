#!/usr/bin/env python3
import grpc
import random
import argparse
import dynamo_pb2
import dynamo_pb2_grpc

# Local cache of known nodes (updated via gossip)
NODES = {}

# -----------------------------
# RPC with failover
# -----------------------------
def _call_rpc_with_failover(method_name, request, timeout=2.0):
    """
    Try calling an RPC on each known node.
    Returns (nid, addr, reply) or (None, None, None).
    """
    if not NODES:
        return None, None, None

    items = list(NODES.items())
    random.shuffle(items)

    for nid, addr in items:
        try:
            channel = grpc.insecure_channel(addr)
            stub = dynamo_pb2_grpc.DynamoServiceStub(channel)
            rpc = getattr(stub, method_name)

            reply = rpc(request, timeout=timeout)

            # Update node cache from gossip reply
            if method_name == "Gossip" and reply is not None:
                for m in reply.members:
                    if m.address:
                        NODES[m.node_id] = m.address

            return nid, addr, reply

        except Exception:
            continue

    return None, None, None

# -----------------------------
# Pretty print vector clock
# -----------------------------
def format_clock(clock_proto):
    return {entry.node_id: entry.counter for entry in clock_proto.clock}

# -----------------------------
# PUT
# -----------------------------
def do_put(key, value):
    req = dynamo_pb2.PutRequest(
        key=key,
        data=dynamo_pb2.ValueWithContext(
            value=value.encode("utf-8"),
            context=dynamo_pb2.Context()
        )
    )

    nid, addr, reply = _call_rpc_with_failover("Put", req, timeout=3)
    if reply is None:
        print("[CLIENT] PUT failed: no reachable nodes")
        return

    print(f"[CLIENT] PUT → {addr} : {key} = {value}")
    print("Success:", reply.success)
    print("Message:", reply.message)

# -----------------------------
# GET
# -----------------------------
def do_get(key):
    req = dynamo_pb2.GetRequest(key=key)
    nid, addr, reply = _call_rpc_with_failover("Get", req, timeout=3)

    if reply is None:
        print("[CLIENT] GET failed: no reachable nodes")
        return

    print(f"[CLIENT] GET → {addr} : {key}")

    if not reply.found:
        print("Key NOT FOUND:", reply.message)
        return

    print(f"\nFound {len(reply.data)} version(s):\n")
    for i, version in enumerate(reply.data):
        val = version.value.decode("utf-8", errors="ignore")
        clock = format_clock(version.context)

        print(f"--- Version {i} ---")
        print("Value:", val)
        print("Vector Clock:", clock)
        print()

# -----------------------------
# View membership
# -----------------------------
def view_membership():
    req = dynamo_pb2.MembershipList()
    nid, addr, reply = _call_rpc_with_failover("Gossip", req, timeout=3)

    if reply is None:
        print("[CLIENT] VIEW MEMBERSHIP failed: no reachable nodes")
        return

    print(f"[CLIENT] VIEW MEMBERSHIP → {addr}\n")
    print("--- Cluster Membership ---")
    for m in reply.members:
        status = "UP" if m.up else "DOWN"
        print(f"{m.node_id:<10}  {m.address:<20}  gen={m.generation:<5}  {status}")

# -----------------------------
# Interactive CLI
# -----------------------------
def repl():
    print("Dynamo-like Client CLI")
    print("Commands:")
    print("  put <key> <value>")
    print("  get <key>")
    print("  members")
    print("  seeds           (show current seeds)")
    print("  addseed <addr>  (add a new seed manually)")
    print("  exit")
    print()

    while True:
        try:
            parts = input("dynamo> ").strip().split()
        except (EOFError, KeyboardInterrupt):
            break

        if not parts:
            continue

        cmd = parts[0].lower()

        if cmd == "put":
            if len(parts) < 3:
                print("Usage: put <key> <value>")
                continue
            do_put(parts[1], " ".join(parts[2:]))

        elif cmd == "get":
            if len(parts) != 2:
                print("Usage: get <key>")
                continue
            do_get(parts[1])

        elif cmd == "members":
            view_membership()

        elif cmd == "seeds":
            print("\nKnown nodes:")
            for nid, addr in NODES.items():
                print(f"{nid}: {addr}")
            print()

        elif cmd == "addseed":
            if len(parts) != 2:
                print("Usage: addseed <host:port>")
                continue

            seed = parts[1]
            fake_id = f"seed-{random.randint(1000,9999)}"
            NODES[fake_id] = seed
            print(f"Added seed {seed}")

        elif cmd == "exit":
            break

        else:
            print("Unknown command:", cmd)

# -----------------------------
# Batch mode
# -----------------------------
def batch_mode(args):
    if args.op == "put":
        do_put(args.key, args.value)
    elif args.op == "get":
        do_get(args.key)
    elif args.op == "members":
        view_membership()

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamo-like client CLI")
    parser.add_argument("--seed", help="Initial seed node address (host:port)")
    parser.add_argument("op", nargs="?", help="put/get/members")
    parser.add_argument("key", nargs="?")
    parser.add_argument("value", nargs="?")
    args = parser.parse_args()

    # Add initial seed if provided
    if args.seed:
        NODES["seed"] = args.seed
        print(f"[CLIENT] Added seed: {args.seed}")
    else:
        print("[CLIENT] WARNING: No seed provided. Use: --seed <addr>")
        print("         or use 'addseed <addr>' inside CLI.")

    if args.op is None:
        repl()
    else:
        batch_mode(args)
