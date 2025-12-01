#!/usr/bin/env bash
set -euo pipefail

echo "[HH] Cleaning old processes..."
./scripts/stop_cluster.sh || true

echo "[HH] Starting 3-node cluster..."
./scripts/start_cluster_3.sh
sleep 2

echo "[HH] Killing node-3 to simulate failure..."
NODE3_PID=$(cat pids/node-3.pid)
kill -9 $NODE3_PID
rm -f pids/node-3.pid
sleep 1

echo "[HH] Running workload while node-3 is DOWN..."

python3 tools/bench_throughput.py \
    --seed localhost:50051 \
    --clients 10 \
    --ops 500 \
    --ratio 0.5 \
    --keyspace 200 \
    --out tests/bench_output/hh_down.csv

echo "[HH] Restarting node-3..."
nohup python3 server.py node-3 50053 --seed localhost:50051 > logs/node3.log 2>&1 &
echo $! > pids/node-3.pid

echo "[HH] Waiting for hinted handoff repair..."
sleep 5

echo "[HH] Checking if node-3 received inconsistent keys..."
python3 tools/check_replica_keys.py \
    --port 50053 \
    --keyspace 200 \
    --out tests/bench_output/hh_repair_results.txt

echo "== HH Test Complete =="
echo "  - Workload while node was down: tests/bench_output/hh_down.csv"
echo "  - Repair check: tests/bench_output/hh_repair_results.txt"

echo "[HH] Stopping cluster..."
./scripts/stop_cluster.sh

echo "[HH] DONE."
