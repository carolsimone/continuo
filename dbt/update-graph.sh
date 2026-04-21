#!/usr/bin/env bash
# Upload compiled dbt manifests to S3 (versioned), then trigger graph reload in the UI.
#
# Usage:
#   ./dbt/update-graph.sh [s3|local]
#
# Env vars:
#   UI_BASE_URL  — UI endpoint (default: http://localhost:8090)
#   TARGET       — dbt_upload target profile from targets.yaml (default: localstack)
#   SERVICES_DIR — path to services directory (default: dbt/services relative to repo root)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SOURCE="${1:-s3}"
TARGET="${TARGET:-localstack}"
SERVICES_DIR="${SERVICES_DIR:-$REPO_ROOT/dbt/services}"

echo "==> Uploading manifests to S3 (target: $TARGET)"
(cd "$REPO_ROOT/dbt" && uv run python -m dbt_upload upload \
  --services-dir "$SERVICES_DIR" \
  --target "$TARGET")

echo "==> Triggering graph reload (source: $SOURCE)"
http_code=$(curl -s -o /tmp/graph_update_resp.json -w "%{http_code}" -X POST "$UI_BASE/api/graph/update" \
  -H "Content-Type: application/json" \
  -d "{\"source\":\"$SOURCE\"}")
resp=$(cat /tmp/graph_update_resp.json)

echo "HTTP $http_code — $resp"
[ "$http_code" = "200" ] || exit 1
