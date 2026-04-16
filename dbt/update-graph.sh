#!/usr/bin/env bash
set -euo pipefail

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SOURCE="${1:-s3}"

resp=$(curl -sf -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")

echo "Graph update triggered: $resp"
