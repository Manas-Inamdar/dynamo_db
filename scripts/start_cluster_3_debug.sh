#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"   # repo dynamo_db root
mkdir -p "$ROOT/logs" "$ROOT/pids" "$ROOT/data"

echo "[CLUSTER_DEBUG] Starting node-1 (seed: localhost:50051)"
( cd "$ROOT" && python3 -u server.py node-1 50051 2>&1 | tee "$ROOT/logs/node1.log" ) &
echo $! > "$ROOT/pids/node-1.pid"
sleep 1.2

echo "[CLUSTER_DEBUG] Starting node-2 (join seed)"
( cd "$ROOT" && python3 -u server.py node-2 50052 --seed localhost:50051 2>&1 | tee "$ROOT/logs/node2.log" ) &
echo $! > "$ROOT/pids/node-2.pid"
sleep 1.0

echo "[CLUSTER_DEBUG] Starting node-3 (join seed)"
( cd "$ROOT" && python3 -u server.py node-3 50053 --seed localhost:50051 2>&1 | tee "$ROOT/logs/node3.log" ) &
echo $! > "$ROOT/pids/node-3.pid"
sleep 1.5

echo "[CLUSTER_DEBUG] Started 3-node cluster"
