#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <schedule-name>" >&2
  exit 1
fi

UI_BASE="${UI_BASE_URL:-http://localhost:8090}"
SCHEDULE="$1"

resp=$(curl -sf -X POST "$UI_BASE/api/schedules/$SCHEDULE/trigger" \
  -H "Content-Type: application/json")

echo "DAG triggered: $resp"
