#!/usr/bin/env bash
# Test 4: Replication + quorum + hinted handoff

set -e

echo "=== Test 4: Replication & Hinted Handoff ==="

rm -rf data
mkdir -p data

echo "[TEST] Starting node-1 (seed)"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
PID1=$!
sleep 2

echo "[TEST] Starting node-2 (joins seed)"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2.log 2>&1 &
PID2=$!
sleep 2

echo "[TEST] Starting node-3 (joins seed)"
nohup python3 server.py node-3 50053 --seed localhost:50051 > node3.log 2>&1 &
PID3=$!
sleep 3

echo "[TEST] Performing PUT"
python3 client.py --seed localhost:50051 put xkey "xvalue" > put_output.txt 2>&1
cat put_output.txt
sleep 1

echo "[TEST] Stopping node-2 (simulate failure)"
kill $PID2
sleep 1

echo "[TEST] GET during failure (should still succeed due to R=2)"
python3 client.py --seed localhost:50051 get xkey > get_output1.txt 2>&1
cat get_output1.txt

echo "[TEST] Restarting node-2"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2_restart.log 2>&1 &
PID2_NEW=$!
sleep 6   # allow gossip + hinted handoff

echo "[TEST] GET after recovery"
python3 client.py --seed localhost:50051 get xkey > get_output2.txt 2>&1
cat get_output2.txt

echo "=== Test 4 Complete ==="

# Cleanup
kill $PID1 || true
kill $PID2_NEW || true
kill $PID3 || true
