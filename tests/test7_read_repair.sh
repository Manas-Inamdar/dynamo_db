#!/usr/bin/env bash
# Test 7: Read Repair Test

set -euo pipefail

echo "=== Test 7: Read Repair ==="

rm -rf data
mkdir -p data
rm -f node1.log node2.log node3.log

########################################
# 1) Start 3 nodes
########################################
echo "[TEST] Starting node-1 (seed)"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
PID1=$!
sleep 1.5

echo "[TEST] Starting node-2"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2.log 2>&1 &
PID2=$!
sleep 1.5

echo "[TEST] Starting node-3"
nohup python3 server.py node-3 50053 --seed localhost:50051 > node3.log 2>&1 &
PID3=$!
sleep 2.0

########################################
# 2) PUT initial value (all replicas have this)
########################################
KEY="rr-key"
echo "[TEST] PUT initial value: value-v1"
python3 client.py --seed localhost:50051 put "$KEY" "value-v1" > put_v1.txt 2>&1
cat put_v1.txt
sleep 1

########################################
# 3) Stop node-3 to cause divergence
########################################
echo "[TEST] Stopping node-3 (force stale replica)"
kill $PID3 || true
sleep 1

########################################
# 4) PUT newer value (node-3 will miss this)
########################################
echo "[TEST] PUT new value: value-v2"
python3 client.py --seed localhost:50051 put "$KEY" "value-v2" > put_v2.txt 2>&1
cat put_v2.txt
sleep 1

########################################
# 5) Restart node-3 (it has stale value-v1)
########################################
echo "[TEST] Restarting node-3 (stale replica)"
nohup python3 server.py node-3 50053 --seed localhost:50051 > node3_restarted.log 2>&1 &
PID3_NEW=$!
sleep 5   # allow gossip + hinted handoff + startup

########################################
# 6) GET to trigger READ REPAIR
########################################
echo "[TEST] GET before read repair:"
echo "[TEST] Waiting 3s to allow gossip to stabilize..."
sleep 3
python3 client.py --seed localhost:50051 get "$KEY" > get_before_rr.txt 2>&1
cat get_before_rr.txt

echo "[TEST] Waiting 3s to allow read repair propagation..."
sleep 3

########################################
# 7) GET again to verify replicas fixed
########################################
echo "[TEST] GET after read repair:"
python3 client.py --seed localhost:50051 get "$KEY" > get_after_rr.txt 2>&1
cat get_after_rr.txt

echo "=== Test 7 Completed ==="

# Cleanup
kill $PID1 || true
kill $PID2 || true
kill $PID3_NEW || true

echo "Logs saved: node1.log node2.log node3.log"
echo "Outputs: put_v1.txt put_v2.txt get_before_rr.txt get_after_rr.txt"
