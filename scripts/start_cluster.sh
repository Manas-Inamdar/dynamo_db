#!/usr/bin/env bash
# Start a local cluster: ./scripts/start_cluster.sh [NODES=3] [extra dynamo-server args...]
# Environment: BASE_PORT (50051), DATA_DIR (./data)
set -euo pipefail
source "$(dirname "$0")/_common.sh"
nodes=${1:-3}; shift || true
for i in $(seq 1 "$nodes"); do start_node "$i" "$@"; done
sleep 1.5   # let gossip converge
cli members
