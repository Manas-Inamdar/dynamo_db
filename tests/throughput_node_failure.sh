#!/usr/bin/env bash
set -euo pipefail

OUTDIR="tests/bench_output"
mkdir -p "$OUTDIR"
CSV="$OUTDIR/throughput_with_node_failure.csv"

echo "[TEST] Starting 3-node cluster..."
./scripts/start_cluster_3.sh
sleep 2

echo "[TEST] Killing node-3 to simulate failure"
kill $(cat pids/node-3.pid)
sleep 1
echo "[TEST] Forcing membership DOWN..."
python3 tools/force_down.py node-3
sleep 1

echo "[TEST] Running throughput benchmark (cluster degraded)..."
python3 tools/bench_throughput.py \
    --seed localhost:50051 \
    --clients 20 \
    --ops 500 \
    --ratio 0.5 \
    --out "$CSV"

echo "[TEST] Throughput results (node failure) saved to $CSV"

echo "[TEST] Restarting cluster cleanly..."
./scripts/stop_cluster.sh

echo "[TEST] DONE"
