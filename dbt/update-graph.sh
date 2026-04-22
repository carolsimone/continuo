#!/usr/bin/env bash
# Upload compiled dbt manifests to S3 (versioned), then trigger graph reload in the UI.
#
# Usage:
#   ./dbt/update-graph.sh [s3|local]
#
# Env vars:
#   UI_BASE_URL  — UI endpoint (default: http://localhost:8090)
#   TARGET       — dbt_upload target profile from targets.yaml (default: localstack)
set -euo pipefail

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SOURCE="${1:-s3}"
TARGET="${TARGET:-localstack}"

# 'upload' skips compile — assumes manifests are already compiled. Use 'load' if they may be stale.
echo "==> Uploading manifests to S3 (target: $TARGET)"
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload upload \
  --services-dir /app/services \
  --target "$TARGET"

echo "==> Triggering graph reload (source: $SOURCE)"
http_code=$(curl -s -o /tmp/graph_update_resp.json -w "%{http_code}" -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")
resp=$(cat /tmp/graph_update_resp.json)

echo "HTTP $http_code — $resp"
[ "$http_code" = "200" ] || exit 1
