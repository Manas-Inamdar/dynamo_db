#!/usr/bin/env bash
set -euo pipefail

OUTDIR="tests/bench_output"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/throughput_results.csv"

echo "[TEST] Starting cluster"
./scripts/start_cluster_3.sh
sleep 2

echo "[TEST] Running throughput benchmark: clients=10 ops=2000 ratio=0.5"
python3 tools/bench_throughput.py \
    --seed localhost:50051 \
    --clients 10 \
    --ops 2000 \
    --ratio 0.5 \
    --keyspace 1000 \
    --out "$CSV"

echo "[TEST] Benchmark done. Results at $CSV"

echo "[TEST] Stopping cluster"
./scripts/stop_cluster.sh

echo "[TEST] Done"
