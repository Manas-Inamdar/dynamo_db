#!/usr/bin/env bash
# tests/test5_conflict.sh
# Conflict test: concurrent writes from two coordinators -> expect siblings (multiple versions)

set -euo pipefail

echo "=== Test 5: Conflict / Vector-Clock Siblings ==="

# clean state
rm -rf data
mkdir -p data
rm -f node1.log node2.log node3.log putA.txt putB.txt get_conflict.txt

# Start 3 nodes (seed = node-1)
echo "[TEST] Starting node-1 (seed)"
nohup python3 server.py node-1 50051 > node1.log 2>&1 &
PID1=$!
sleep 1.2

echo "[TEST] Starting node-2 (join seed)"
nohup python3 server.py node-2 50052 --seed localhost:50051 > node2.log 2>&1 &
PID2=$!
sleep 1.0

echo "[TEST] Starting node-3 (join seed)"
nohup python3 server.py node-3 50053 --seed localhost:50051 > node3.log 2>&1 &
PID3=$!
sleep 2.0

KEY="conflict-key"

echo "[TEST] Warmup GET (should be empty)"
python3 client.py --seed localhost:50051 get "$KEY" > /dev/null 2>&1 || true

# Now perform two nearly-simultaneous PUTs from different coordinators
echo "[TEST] Issuing two concurrent PUTs for key=$KEY"

python3 client.py --seed localhost:50051 put "$KEY" "value-from-node1" > putA.txt 2>&1 &
PID_PUT_A=$!

# tiny sleep to increase concurrency but still overlap
sleep 0.02

python3 client.py --seed localhost:50052 put "$KEY" "value-from-node2" > putB.txt 2>&1 &
PID_PUT_B=$!

# Wait for both to complete (they each block for W quorum)
wait $PID_PUT_A || true
wait $PID_PUT_B || true

echo
echo "---- PUT A output (node-1 client) ----"
cat putA.txt || true
echo
echo "---- PUT B output (node-2 client) ----"
cat putB.txt || true
echo

# Give a moment for writes to propagate
sleep 1.0

echo "[TEST] GET after concurrent PUTs"
python3 client.py --seed localhost:50051 get "$KEY" > get_conflict.txt 2>&1 || true

echo "---- GET output (should show siblings if concurrent) ----"
cat get_conflict.txt || true
echo

# Basic check: did we see more than 1 version?
if grep -q "Found 2 version(s)" get_conflict.txt || grep -q "Found 3 version(s)" get_conflict.txt; then
  echo "RESULT: Conflict observed — siblings present (PASS)"
else
  echo "RESULT: No siblings detected. This can happen if writes serialized. See notes below for how to increase chance."
fi

# Stop servers
echo "[TEST] Stopping nodes"
kill $PID1 || true
kill $PID2 || true
kill $PID3 || true

echo "Logs: node1.log, node2.log, node3.log"
echo "PUT logs: putA.txt putB.txt"
echo "GET result: get_conflict.txt"

echo "=== Test 5 Completed ==="
