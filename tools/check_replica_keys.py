import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import grpc
import argparse
import dynamo_pb2
import dynamo_pb2_grpc

def check_keys(port, keyspace, outpath):
    channel = grpc.insecure_channel(f"localhost:{port}")
    stub = dynamo_pb2_grpc.DynamoServiceStub(channel)

    missing = []

    for i in range(keyspace):
        key = f"bench-key-{i}"
        try:
            resp = stub.Get(dynamo_pb2.GetRequest(key=key), timeout=1.0)
            if not resp.success:
                missing.append(key)
        except Exception:
            missing.append(key)

    with open(outpath, "w") as f:
        f.write(f"Checked node {port}\n")
        f.write(f"Total keys: {keyspace}\n")
        f.write(f"Missing: {len(missing)}\n")
        for k in missing:
            f.write(f"{k}\n")

    print(f"[HH] Missing keys: {len(missing)} (details in {outpath})")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--keyspace", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    check_keys(args.port, args.keyspace, args.out)
