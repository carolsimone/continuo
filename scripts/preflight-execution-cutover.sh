#!/usr/bin/env bash
# Go/no-go check before upgrading to the chart version that replaces
# executor-controller and k8s-controller with execution-controller.
#
# The new service starts on an empty database. Anything mid-flight in the two
# old databases or in the delay queue is not carried over, so the upgrade must
# happen while nothing is in flight. This script reads the OLD state and prints
# GO when every counter is zero, NO-GO otherwise (exit 1).
#
# Env: POSTGRES_HOST, POSTGRES_PORT (5432), POSTGRES_USER, PGPASSWORD,
#      REDIS_HOST, REDIS_PORT (6379), REDIS_PASSWORD (optional),
#      K8S_NAMESPACE (default).
set -euo pipefail

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"
: "${REDIS_HOST:?REDIS_HOST is required}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
REDIS_PORT="${REDIS_PORT:-6379}"
K8S_NAMESPACE="${K8S_NAMESPACE:-default}"

psql_count() { # $1 db, $2 sql
  psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$1" -tAc "$2" | tr -d '[:space:]'
}
redis_count() { # $1 command...
  if [ -n "${REDIS_PASSWORD:-}" ]; then
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --no-auth-warning "$@" | tr -d '[:space:]'
  else
    redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@" | tr -d '[:space:]'
  fi
}

pending_deployments="$(psql_count continuo_executor "SELECT count(*) FROM executor_deployments WHERE status IN ('pending','blocked')")"
executor_outbox="$(psql_count continuo_executor "SELECT count(*) FROM executor_outbox WHERE status IN ('pending','scheduled')")"
k8s_outbox="$(psql_count continuo_k8s "SELECT count(*) FROM k8s_outbox WHERE status IN ('pending','scheduled')")"
tickets="$(redis_count HLEN checkk8s:tickets)"
active_jobs="$(kubectl get jobs -n "$K8S_NAMESPACE" -l app=dbt-job --field-selector status.successful!=1 -o name 2>/dev/null | grep -c . || true)"

printf '%-40s %s\n' "pending/blocked executor_deployments" "$pending_deployments"
printf '%-40s %s\n' "unpublished executor_outbox rows" "$executor_outbox"
printf '%-40s %s\n' "unpublished k8s_outbox rows" "$k8s_outbox"
printf '%-40s %s\n' "delay-queue tickets (checkk8s:tickets)" "$tickets"
printf '%-40s %s\n' "active app=dbt-job Jobs" "$active_jobs"

total=$((pending_deployments + executor_outbox + k8s_outbox + tickets + active_jobs))
if [ "$total" -eq 0 ]; then
  echo "GO: nothing in flight; safe to upgrade."
  exit 0
fi
echo "NO-GO: work is in flight. Wait for it to finish (or cancel it) and re-run." >&2
exit 1
