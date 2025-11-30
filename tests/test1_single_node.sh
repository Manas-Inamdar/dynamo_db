#!/usr/bin/env bash
# Test 1: Single node basic PUT/GET

set -e

echo "=== Test 1: Single-node basic PUT/GET ==="

# cleanup old data
rm -rf data
mkdir -p data

# Start node-1
echo "[TEST] Starting node-1 at port 50051"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
NODE1_PID=$!
sleep 2

echo "[TEST] PUT key=hello"
python3 client.py --seed localhost:50051 put hello "world" > put_output.txt 2>&1

echo "[TEST] GET key=hello"
python3 client.py --seed localhost:50051 get hello > get_output.txt 2>&1

echo
echo "=== PUT OUTPUT ==="
cat put_output.txt

echo
echo "=== GET OUTPUT ==="
cat get_output.txt

# stop node
echo "[TEST] Stopping node-1"
kill $NODE1_PID
sleep 1

echo "=== Test 1 Completed ==="
