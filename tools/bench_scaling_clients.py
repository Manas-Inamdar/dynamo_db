#!/usr/bin/env python3
import subprocess
import csv
import time
import json
import os

LOAD_LEVELS = [1, 5, 10, 20, 50, 100, 200, 300, 400, 500]
OPS_PER_LEVEL = 2000
SEED = "localhost:50051"
OUT_CSV = "tests/bench_output/throughput_vs_clients.csv"

def run_load(clients):
    print(f"\n=== Running load: {clients} logical clients ===")
    
    # Run benchmark
    result = subprocess.run([
        "python3", "tools/bench_throughput.py",
        "--seed", SEED,
        "--clients", "20",  # always use 20 worker threads
        "--ops", str((OPS_PER_LEVEL * clients) // 20),
        "--ratio", "0.5",
        "--keyspace", "2000",
        "--out", "tmp.csv"
    ], capture_output=True, text=True)

    print(result.stdout)

    # Parse JSON statistics printed at end
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
        # Try JSON first
                stats = json.loads(line)
            except:
        # Fallback: convert Python dict → JSON by replacing single quotes
                fixed = line.replace("'", "\"")
                stats = json.loads(fixed)
            return stats["ops_per_sec"]


    return 0.0

def main():
    print("[CLUSTER] Starting 3-node cluster...")
    subprocess.run(["./scripts/start_cluster_3.sh"])
    time.sleep(3)

    print("[OK] Cluster started.\n")

    # Write CSV header
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["clients", "ops_per_sec"])

        for c in LOAD_LEVELS:
            tput = run_load(c)
            writer.writerow([c, tput])
            print(f"[RESULT] {c} clients → {tput:.2f} ops/sec")

    print("\n[CLUSTER] Stopping cluster...")
    subprocess.run(["./scripts/stop_cluster.sh"])

    print(f"\n[DONE] Results saved to: {OUT_CSV}")

if __name__ == "__main__":
    main()
