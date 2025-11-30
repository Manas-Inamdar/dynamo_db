#!/usr/bin/env bash
# Test 2: Simple key-value PUT and GET with different values

set -e

echo "=== Test 2: PUT/GET Different Keys ==="

rm -rf data
mkdir -p data

echo "[TEST] Starting node-1 (port 50051)"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
NODE1_PID=$!
sleep 2

echo "[TEST] PUT key=a value=apple"
python3 client.py --seed localhost:50051 put a apple > out1.txt 2>&1

echo "[TEST] PUT key=b value=banana"
python3 client.py --seed localhost:50051 put b banana > out2.txt 2>&1

echo "[TEST] GET key=a"
python3 client.py --seed localhost:50051 get a > get_a.txt 2>&1

echo "[TEST] GET key=b"
python3 client.py --seed localhost:50051 get b > get_b.txt 2>&1

echo
echo "=== GET a ==="
cat get_a.txt

echo
echo "=== GET b ==="
cat get_b.txt

kill $NODE1_PID
sleep 1

echo "=== Test 2 Completed ==="
