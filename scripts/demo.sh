#!/usr/bin/env bash
# Guided walkthrough of every Dynamo technique on a live 4-node cluster.
# Usage: ./scripts/demo.sh            (press Enter between steps)
#        AUTO=1 ./scripts/demo.sh     (no pauses)
set -euo pipefail
source "$(dirname "$0")/_common.sh"
export DATA_DIR="$ROOT/data/demo"
trap '"$ROOT/scripts/stop_cluster.sh" >/dev/null' EXIT

step() { echo; echo "=== $* ==="; [[ -n "${AUTO:-}" ]] || read -rp "(press Enter) " _; }
run()  { echo "\$ dynamo-cli $*"; cli "$@" || true; }

"$ROOT/scripts/stop_cluster.sh" >/dev/null; rm -rf "$DATA_DIR"
FAST=(--gossip-interval 0.5 --fail-timeout 3 --hint-interval 1)

step "1. Start a 4-node cluster (N=3, R=2, W=2); nodes join through node-1"
for i in 1 2 3 4; do start_node "$i" "${FAST[@]}"; done
sleep 2; run members

step "2. Basic PUT/GET: any node can coordinate"
run put user:42 alice
run get user:42

step "3. Updates supersede old versions (vector clock grows)"
run put user:42 alice-v2
run get user:42

step "4. Crash node-2 (SIGKILL) and keep writing: sloppy quorum + hinted handoff"
stop_node 2 KILL; sleep 4
for i in $(seq 1 5); do cli put "order:$i" "item-$i" >/dev/null; done
echo "wrote order:1..5 while node-2 was down"
grep -h "DEAD\|holding hinted" "$LOG_DIR"/node-{1,3,4}.log | tail -6 || true

step "5. Restart node-2: hints are handed back automatically"
start_node 2 "${FAST[@]}"; sleep 4
grep -h "hinted handoff: delivered" "$LOG_DIR"/node-{1,3,4}.log | tail -5 || true
run get order:3

step "6. Concurrent writes from the same context -> siblings"
"$PYTHON" - <<PY
from dynamo.client import DynamoClient
a = DynamoClient(["localhost:$BASE_PORT"]); b = DynamoClient(["localhost:$((BASE_PORT+1))"])
a.put("cart", "empty"); base = a.get("cart")
a.put("cart", "apples", context=base.context)
b.put("cart", "bananas", context=base.context)
print("two clients updated the same version without seeing each other")
PY
run get cart

step "7. Resolve the conflict: write with the merged context"
run resolve cart apples+bananas
run get cart

step "8. Decommission node-4"
run leave node-4; sleep 2
run members

echo; echo "Demo finished. Logs are in logs/. The cluster will now be stopped."
