#!/usr/bin/env bash
# Go/no-go check before upgrading to the chart version that replaces
# executor-controller and k8s-controller with execution-controller.
#
# The new service starts on an empty database. Anything mid-flight in the two
# old databases, the delay queue, the intra-service self-loop streams, or the
# Kubernetes Jobs is not carried over, so the upgrade must happen while nothing
# is in flight. Stop the old services first, then run this against the OLD state.
# It prints each counter and returns GO (exit 0) only when every counter is zero;
# otherwise NO-GO (exit 1). A backing-store query that cannot be answered
# (Postgres/Redis unreachable, kubectl forbidden) is treated as NO-GO, never as
# an empty result — the check fails closed.
#
# Env: POSTGRES_HOST, POSTGRES_PORT (5432), POSTGRES_USER, PGPASSWORD,
#      REDIS_HOST, REDIS_PORT (6379), REDIS_PASSWORD (optional),
#      K8S_NAMESPACE (required).
set -euo pipefail

: "${POSTGRES_HOST:?POSTGRES_HOST is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${PGPASSWORD:?PGPASSWORD is required}"
: "${REDIS_HOST:?REDIS_HOST is required}"
: "${K8S_NAMESPACE:?K8S_NAMESPACE is required (the release namespace; without it active Jobs would be counted in the wrong namespace and read as 0)}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
REDIS_PORT="${REDIS_PORT:-6379}"

# psql_count runs a scalar count. With `set -euo pipefail`, a psql connection or
# query failure aborts the whole script with a non-zero exit (fail-closed).
psql_count() { # $1 db, $2 sql
  psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$1" -tAc "$2" | tr -d '[:space:]'
}

redis_cli() { # $@ command...
  if [ -n "${REDIS_PASSWORD:-}" ]; then
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" -a "$REDIS_PASSWORD" --no-auth-warning "$@"
  else
    command redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" "$@"
  fi
}

# selfLoopUndrained sums, across one intra-service self-loop stream, every
# consumer group's undelivered (lag) + delivered-unacked (pending) messages.
# These streams are produced and consumed by the same merged service, so an
# unconsumed message is lost when the new service starts on an empty database.
# XINFO GROUPS errors with "no such key" when the stream was never created; Redis
# reachability is verified separately (PING) before this runs, so an error here
# means an absent stream (nothing enqueued), counted as 0. The `lag` field needs
# Redis >= 7; on older servers only pending contributes.
selfLoopUndrained() { # $1 stream
  redis_cli XINFO GROUPS "$1" 2>/dev/null | awk '
    flag { v=$NF; gsub(/[^0-9]/,"",v); if (v!="") sum+=v; flag=0 }
    /"pending"$|"lag"$/ { flag=1 }
    END { print sum+0 }'
}

# ---- Postgres: unpublished/unsettled work in the two old databases ----
pending_deployments="$(psql_count continuo_executor "SELECT count(*) FROM executor_deployments WHERE status IN ('pending','blocked')")"
# A candidate Job that has been dispatched (status='deployed') but whose outcome
# is still NULL is unsettled: its node-completed result has not been recorded.
# Its Job may already be terminal (excluded from the active-Job count) and its
# outbox row and delay ticket already cleared, so this is the only counter that
# catches it. Upgrading while it is unsettled loses the deployment record and the
# release never settles.
unsettled_candidates="$(psql_count continuo_executor "SELECT count(*) FROM executor_deployments WHERE status = 'deployed' AND outcome IS NULL AND mode IN ('validation','seed_build','compile')")"
executor_outbox="$(psql_count continuo_executor "SELECT count(*) FROM executor_outbox WHERE status IN ('pending','scheduled')")"
k8s_outbox="$(psql_count continuo_k8s "SELECT count(*) FROM k8s_outbox WHERE status IN ('pending','scheduled')")"

# ---- Redis: delay-queue tickets + undrained self-loop consumer groups ----
redis_cli PING >/dev/null 2>&1 || {
  echo "NO-GO: cannot reach Redis at $REDIS_HOST:$REDIS_PORT (cannot verify the delay queue and self-loop consumers)." >&2
  exit 1
}
tickets="$(redis_cli HLEN checkk8s:tickets | tr -d '[:space:]')"
undrained_selfloop="$(selfLoopUndrained "check.k8s:v1")"

# ---- Kubernetes: non-terminal Jobs ----
# A Job is terminal once it has a Complete or Failed condition; the field selector
# status.successful!=1 would wrongly count terminal FAILED Jobs (successful=0
# forever) as active. Count only non-terminal (active/pending) Jobs. A kubectl
# failure (expired credentials, missing RBAC, API server down) must abort as
# NO-GO — its stderr goes to this script's stderr and the non-zero exit is caught
# below — rather than silently reading zero. The JOB: sentinel keeps a pending
# Job (no conditions) from collapsing to an empty, uncounted line.
if ! jobs_lines="$(kubectl get jobs -n "$K8S_NAMESPACE" -l app=dbt-job \
  -o jsonpath='{range .items[*]}JOB:{.status.conditions[*].type}{"\n"}{end}')"; then
  echo "NO-GO: kubectl could not list Jobs in namespace $K8S_NAMESPACE (see the error above); cannot verify running Jobs." >&2
  exit 1
fi
active_jobs="$(printf '%s\n' "$jobs_lines" | grep '^JOB:' | grep -cvE 'Complete|Failed' || true)"

printf '%-42s %s\n' "pending/blocked executor_deployments" "$pending_deployments"
printf '%-42s %s\n' "unsettled candidate deployments" "$unsettled_candidates"
printf '%-42s %s\n' "unpublished executor_outbox rows" "$executor_outbox"
printf '%-42s %s\n' "unpublished k8s_outbox rows" "$k8s_outbox"
printf '%-42s %s\n' "delay-queue tickets (checkk8s:tickets)" "$tickets"
printf '%-42s %s\n' "undrained self-loop group messages" "$undrained_selfloop"
printf '%-42s %s\n' "non-terminal app=dbt-job Jobs" "$active_jobs"

total=$((pending_deployments + unsettled_candidates + executor_outbox + k8s_outbox + tickets + undrained_selfloop + active_jobs))
if [ "$total" -eq 0 ]; then
  echo "GO: nothing in flight; safe to upgrade."
  exit 0
fi
echo "NO-GO: work is in flight. Wait for it to finish (or cancel it) and re-run." >&2
exit 1
