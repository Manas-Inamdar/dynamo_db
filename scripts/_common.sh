# Shared helpers for the cluster scripts. Source, do not execute.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ -x "$ROOT/.venv/bin/python" ]]; then PYTHON="$ROOT/.venv/bin/python"; else PYTHON="${PYTHON:-python3}"; fi
BASE_PORT="${BASE_PORT:-50051}"
DATA_DIR="${DATA_DIR:-$ROOT/data}"
LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/pids"

port_open() {  # port_open PORT -> success if something listens on localhost:PORT
  "$PYTHON" -c "import socket; socket.create_connection(('localhost', $1), 0.2)" 2>/dev/null
}

wait_for_port() {  # wait_for_port PORT [TIMEOUT_S]
  local port=$1 deadline=$((SECONDS + ${2:-10}))
  until port_open "$port"; do
    (( SECONDS >= deadline )) && { echo "node on port $port did not come up" >&2; return 1; }
    sleep 0.1
  done
}

start_node() {  # start_node INDEX [extra server args...]
  local i=$1; shift
  local port=$((BASE_PORT + i - 1)) seeds=""
  (( i > 1 )) && seeds="--seeds localhost:$BASE_PORT"
  mkdir -p "$LOG_DIR" "$PID_DIR" "$DATA_DIR"
  if port_open "$port"; then
    echo "port $port is already in use (stale node? run scripts/stop_cluster.sh)" >&2
    return 1
  fi
  # `exec` makes the background job *be* the server, so $! is its real PID.
  (cd "$ROOT" && exec "$PYTHON" -m dynamo.server --id "node-$i" --port "$port" $seeds \
      --data-dir "$DATA_DIR" "$@") >>"$LOG_DIR/node-$i.log" 2>&1 </dev/null &
  local pid=$!
  disown "$pid" 2>/dev/null || true   # no "Killed" job notices when we stop it
  echo "$pid" >"$PID_DIR/node-$i.pid"
  wait_for_port "$port"
  kill -0 "$pid" 2>/dev/null || { echo "node-$i exited during startup; see logs/node-$i.log" >&2; return 1; }
  echo "started node-$i on localhost:$port (pid $(cat "$PID_DIR/node-$i.pid"), log logs/node-$i.log)"
}

stop_node() {  # stop_node INDEX [SIGNAL]
  local pidfile="$PID_DIR/node-$1.pid" sig=${2:-TERM}
  [[ -f $pidfile ]] || return 0
  local pid; pid=$(cat "$pidfile")
  if kill -0 "$pid" 2>/dev/null; then
    kill "-$sig" "$pid"
    for _ in $(seq 50); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
    kill -0 "$pid" 2>/dev/null && kill -KILL "$pid"
  fi
  rm -f "$pidfile"
}

cli() { (cd "$ROOT" && "$PYTHON" -m dynamo.client --seeds "localhost:$BASE_PORT" "$@"); }
