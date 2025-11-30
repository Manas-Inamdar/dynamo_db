#!/usr/bin/env bash
# Test 3: Gossip membership check with two nodes

set -e

echo "=== Test 3: Gossip Membership ==="

rm -rf data
mkdir -p data

echo "[TEST] Starting node-1"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
PID1=$!
sleep 2

echo "[TEST] Starting node-2 with seed=node-1"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2.log 2>&1 &
PID2=$!
sleep 3

echo "[TEST] Checking membership via node-1"
python3 client.py --seed localhost:50051 members > members1.txt 2>&1

echo
echo "=== MEMBERSHIP ==="
cat members1.txt

echo "[TEST] Stopping nodes"
kill $PID1 || true
kill $PID2 || true

echo "=== Test 3 Completed ==="
