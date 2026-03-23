#!/usr/bin/env bash
# scripts/start.sh  —  launch all storage nodes and api nodes
# Run from the build/ directory:
#   cd build && ../scripts/start.sh [start|stop|status]

set -euo pipefail
BUILD_DIR="$(cd "$(dirname "$0")/.." && pwd)/build"
LOG_DIR="${BUILD_DIR}/../logs"
mkdir -p "$LOG_DIR"

N_STORAGE=8
N_API=8
FIRST_API_PORT=50063

start_all() {
    echo "── Stopping any existing processes ──────────────────────────────"
    pkill -f storage_node 2>/dev/null || true
    pkill -f api_node     2>/dev/null || true
    sleep 1   # wait for DB LOCK files to be released

    echo "── Starting $N_STORAGE storage shards ──────────────────────────"
    for i in $(seq 0 $((N_STORAGE - 1))); do
        rm -f "/tmp/kv_${i}.sock"
        "${BUILD_DIR}/storage_node" "$i" \
            > "${LOG_DIR}/storage_${i}.log" 2>&1 &
        echo "  storage_node shard=$i  pid=$!  log=logs/storage_${i}.log"
    done

    # Give RocksDB a moment to open its DB files before api_nodes connect
    sleep 1

    echo "── Starting $N_API api nodes ────────────────────────────────────"
    for i in $(seq 0 $((N_API - 1))); do
        PORT=$((FIRST_API_PORT + i))
        "${BUILD_DIR}/api_node" "$PORT" \
            > "${LOG_DIR}/api_${PORT}.log" 2>&1 &
        echo "  api_node port=$PORT  pid=$!  log=logs/api_${PORT}.log"
    done

    echo
    echo "All processes started.  Run benchmark with:"
    echo "  ${BUILD_DIR}/load_client <THREADS> <SECONDS> [SERVER_IP]"
    echo "  e.g.  ${BUILD_DIR}/load_client 400 60 127.0.0.1"
}

stop_all() {
    echo "Stopping all kv_store processes..."
    pkill -f storage_node || true
    pkill -f api_node     || true
    echo "Done."
}

status_all() {
    echo "Storage nodes:"
    pgrep -a storage_node || echo "  (none running)"
    echo "API nodes:"
    pgrep -a api_node     || echo "  (none running)"
}

case "${1:-start}" in
    start)  start_all  ;;
    stop)   stop_all   ;;
    status) status_all ;;
    *)      echo "Usage: $0 [start|stop|status]" ;;
esac
