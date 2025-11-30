#!/usr/bin/env bash
# scripts/start_cluster_3.sh
# Start a 3-node local cluster: node-1 (seed), node-2, node-3
set -euo pipefail

mkdir -p logs pids data

echo "[CLUSTER] Starting node-1 (seed: localhost:50051)"
nohup python3 server.py node-1 50051 > logs/node1.log 2>&1 &
echo $! > pids/node-1.pid
sleep 1.2

echo "[CLUSTER] Starting node-2 (join seed)"
nohup python3 server.py node-2 50052 --seed localhost:50051 > logs/node2.log 2>&1 &
echo $! > pids/node-2.pid
sleep 1.0

echo "[CLUSTER] Starting node-3 (join seed)"
nohup python3 server.py node-3 50053 --seed localhost:50051 > logs/node3.log 2>&1 &
echo $! > pids/node-3.pid
sleep 1.5

echo "[CLUSTER] Started 3-node cluster"
