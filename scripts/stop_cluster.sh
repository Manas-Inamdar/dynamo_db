#!/usr/bin/env bash
# Stop every node started by start_cluster.sh (graceful SIGTERM, then SIGKILL).
set -euo pipefail
source "$(dirname "$0")/_common.sh"
shopt -s nullglob
for pidfile in "$PID_DIR"/node-*.pid; do
  i=${pidfile##*/node-}; i=${i%.pid}
  stop_node "$i" && echo "stopped node-$i"
done
