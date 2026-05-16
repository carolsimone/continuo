#!/usr/bin/env bash
#
# Destroys deprecated Redis consumer groups left behind after the rename to
# kebab-case + per-stream split. Idempotent: NOGROUP is treated as success.
#
# Usage:
#   REDIS_URL=redis://:continuo@localhost:6379 ./scripts/migrate-consumer-groups.sh
#
# Pre-production cleanup tool. Do not auto-run on service boot.

set -euo pipefail

: "${REDIS_URL:?REDIS_URL must be set, e.g. redis://:continuo@localhost:6379}"

migrations=(
  "node.updated:v1|orchestrator_node_updated"
  "manifest.loaded:v1|orchestrator_manifest_loaded"
  "initialize.run:v1|orchestrator_initialize_run"
  "scheduler.started:v1|orchestrator_scheduler_started"
  "scheduler.started:v1|startup_controller_consumers"
  "trigger.rerun:v1|orchestrator_rerun"
  "trigger.rebase:v1|orchestrator_rebase"
  "trigger.single_node_run:v1|orchestrator_single_node_run"
  "query.model:v1|executor_controller_consumers"
  "retry.task:v1|executor_controller_consumers"
  "node.deployed:v1|k8s_controller_consumers"
  "check.k8s:v1|k8s_controller_consumers"
  "update.graph:v1|manifest-controller"
)

destroy() {
  local stream="$1" group="$2"
  echo "XGROUP DESTROY $stream $group"
  redis-cli -u "$REDIS_URL" XGROUP DESTROY "$stream" "$group" || true
}

for entry in "${migrations[@]}"; do
  destroy "${entry%%|*}" "${entry##*|}"
done

echo "Migration complete. New groups are created on first read by each service."
