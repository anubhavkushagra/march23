#!/usr/bin/env bash
# scripts/build.sh  —  configure and build the project
# Run from the repo root:  ./scripts/build.sh

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="${ROOT}/build"

echo "── Configuring ──────────────────────────────────────────────────"
cmake -S "$ROOT" -B "$BUILD" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

echo "── Building ─────────────────────────────────────────────────────"
cmake --build "$BUILD" --parallel "$(nproc)"

echo
echo "Binaries:"
ls -lh "${BUILD}/storage_node" "${BUILD}/api_node" "${BUILD}/load_client"
