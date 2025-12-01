#!/usr/bin/env python3
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# tools/bench_throughput.py
# Usage:
#   python3 tools/bench_throughput.py --seed localhost:50051 --clients 10 --ops 1000 --ratio 0.5 --keyspace 1000 --out results.csv
#
# ratio = fraction of PUTs (0..1). e.g., 0.5 => 50% PUT, 50% GET.

import argparse
import time
import threading
import random
import csv
import socket

import grpc
import dynamo_pb2
import dynamo_pb2_grpc

def make_stub(seed_addr):
    ch = grpc.insecure_channel(seed_addr)
    return dynamo_pb2_grpc.DynamoServiceStub(ch)

def worker_thread(tid, seed_addr, ops, ratio_put, keyspace, out_list, start_barrier):
    stub = make_stub(seed_addr)
    random.seed(tid + int(time.time()))
    start_barrier.wait()
    for i in range(ops):
        key = f"bench-key-{random.randrange(0, keyspace)}"
        do_put = random.random() < ratio_put
        ts_start = time.time()
        success = False
        msg = ""
        try:
            if do_put:
                vwc = dynamo_pb2.ValueWithContext()
                vwc.value = f"value-{tid}-{i}".encode('utf-8')
                # leave context empty — coordinator will merge existing contexts
                req = dynamo_pb2.PutRequest(key=key, data=vwc)
                resp = stub.Put(req, timeout=1.0)
                success = resp.success
                msg = resp.message
                op_type = "PUT"
            else:
                req = dynamo_pb2.GetRequest(key=key)
                resp = stub.Get(req,timeout=1.0)
                success = resp.found
                msg = resp.message
                op_type = "GET"
        except Exception as e:
            success = False
            msg = str(e)
            op_type = "PUT" if do_put else "GET"
        ts_end = time.time()
        latency_ms = (ts_end - ts_start) * 1000.0
        out_list.append({
            "thread": tid,
            "op_idx": i,
            "op": op_type,
            "key": key,
            "latency_ms": f"{latency_ms:.3f}",
            "success": int(bool(success)),
            "message": msg,
            "ts": int(ts_end * 1000),
            "host": socket.gethostname()
        })

def run_benchmark(seed_addr, num_clients, total_ops, ratio_put, keyspace, out_csv):
    ops_per_client = total_ops // num_clients
    threads = []
    results = []
    start_barrier = threading.Barrier(num_clients + 1)
    for t in range(num_clients):
        th = threading.Thread(target=worker_thread, args=(t, seed_addr, ops_per_client, ratio_put, keyspace, results, start_barrier), daemon=True)
        threads.append(th)
        th.start()
    t0 = time.time()
    start_barrier.wait()  # start all workers at the same time
    for th in threads:
        th.join()
    t1 = time.time()

    # Write CSV
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["thread","op_idx","op","key","latency_ms","success","message","ts","host"])
        writer.writeheader()
        for r in results:
            writer.writerow(r)

    total_done = len(results)
    elapsed = t1 - t0
    ops_per_sec = total_done / elapsed if elapsed > 0 else 0.0
    latencies = [float(r["latency_ms"]) for r in results]
    avg = sum(latencies)/len(latencies) if latencies else 0.0
    p95 = sorted(latencies)[int(0.95*len(latencies))-1] if latencies else 0.0
    p99 = sorted(latencies)[int(0.99*len(latencies))-1] if latencies else 0.0

    print(f"RESULTS: total_ops={total_done} elapsed_s={elapsed:.3f} ops/sec={ops_per_sec:.2f} avg_latency_ms={avg:.3f} p95={p95:.3f} p99={p99:.3f}")
    return {
        "total_ops": total_done,
        "elapsed_s": elapsed,
        "ops_per_sec": ops_per_sec,
        "avg_latency_ms": avg,
        "p95_latency_ms": p95,
        "p99_latency_ms": p99,
        "csv": out_csv
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="seed address host:port")
    parser.add_argument("--clients", type=int, default=5, help="number of parallel worker threads")
    parser.add_argument("--ops", type=int, default=1000, help="total ops performed across all clients")
    parser.add_argument("--ratio", type=float, default=0.5, help="fraction of PUT ops (0..1)")
    parser.add_argument("--keyspace", type=int, default=1000, help="number of distinct keys")
    parser.add_argument("--out", default="results.csv", help="output CSV file")
    args = parser.parse_args()

    print("Starting benchmark:", args)
    stats = run_benchmark(args.seed, args.clients, args.ops, args.ratio, args.keyspace, args.out)
    # print a short JSON-like summary
    print(stats)
