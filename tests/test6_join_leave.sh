#!/usr/bin/env bash
# Test 6: Join + Leave + Gossip Membership Update

set -euo pipefail

echo "=== Test 6: Join/Leave Membership Test ==="

# Clean state
rm -rf data
mkdir -p data
rm -f node1.log node2.log node3.log

########################################
# 1) START NODE-1 (SEED)
########################################
echo "[TEST] Starting node-1 (seed)"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
PID1=$!
sleep 2

########################################
# 2) START NODE-2
########################################
echo "[TEST] Starting node-2"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2.log 2>&1 &
PID2=$!
sleep 2

########################################
# 3) JOIN NODE-3 VIA GOSSIP (but don't start server yet)
########################################
echo "[TEST] Sending join gossip for node-3"
python3 join_node.py localhost:50051 node-3 localhost:50053 > join_node3.txt 2>&1 || true
echo "Join output:"
cat join_node3.txt
sleep 2

########################################
# 4) START ACTUAL NODE-3 PROCESS
########################################
echo "[TEST] Starting node-3 server"
nohup python3 server.py node-3 50053 --seed localhost:50051 > node3.log 2>&1 &
PID3=$!
sleep 3

########################################
# 5) CHECK MEMBERSHIP SHOULD SHOW 3 NODES
########################################
echo "[TEST] Membership after join:"
python3 client.py --seed localhost:50051 members > mem_after_join.txt 2>&1
cat mem_after_join.txt

########################################
# 6) SEND LEAVE REQUEST FOR NODE-2
########################################
echo "[TEST] Sending leave request for node-2"
python3 leave_node.py localhost:50051 node-2 > leave_out.txt 2>&1 || true
echo "Leave output:"
cat leave_out.txt

sleep 4

########################################
# 7) CHECK MEMBERSHIP AFTER LEAVE
########################################
echo "[TEST] Membership after node-2 leave:"
python3 client.py --seed localhost:50051 members > mem_after_leave.txt 2>&1
cat mem_after_leave.txt

echo "=== Test 6 Completed ==="

# Cleanup
kill $PID1 || true
kill $PID2 || true
kill $PID3 || true

echo "Logs saved as node1.log node2.log node3.log"
echo "Membership logs saved as mem_after_join.txt mem_after_leave.txt"
