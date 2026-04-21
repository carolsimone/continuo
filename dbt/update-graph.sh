#!/usr/bin/env bash
set -euo pipefail

# UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
UI_BASE="${UI_BASE_URL:-http://168.119.224.110:8090}"
SOURCE="${1:-s3}"

http_code=$(curl -s -o /tmp/graph_update_resp.json -w "%{http_code}" -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")
resp=$(cat /tmp/graph_update_resp.json)

echo "HTTP $http_code — $resp"
[ "$http_code" = "200" ] || exit 1
