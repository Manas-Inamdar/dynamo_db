#!/usr/bin/env bash
set -euo pipefail

echo "[CLUSTER] Stopping cluster processes (pids/*)"

# Kill Python servers silently
pkill -f "python3 server.py" >/dev/null 2>&1 || true
pkill -f "server.py" >/dev/null 2>&1 || true
pkill -f "python3 join_node.py" >/dev/null 2>&1 || true
pkill -f "join_node.py" >/dev/null 2>&1 || true
pkill -f "python3 leave_node.py" >/dev/null 2>&1 || true
pkill -f "leave_node.py" >/dev/null 2>&1 || true

sleep 1
