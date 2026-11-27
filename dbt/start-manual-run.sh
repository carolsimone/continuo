#!/usr/bin/env bash
# Starts a manual run of a schedule through the ui's HTTP API.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <schedule-name>" >&2
  exit 1
fi

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SCHEDULE="$1"
REQUEST_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"

resp=$(curl -sf -X POST "$UI_BASE/api/schedules/$SCHEDULE/manual-run" \
  -H "Content-Type: application/json" \
  -d "{\"request_id\":\"$REQUEST_ID\"}")

echo "Manual run started: $resp"
