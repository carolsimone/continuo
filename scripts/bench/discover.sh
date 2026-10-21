#!/usr/bin/env bash
# Prints what the benchmark needs from a k8s install. Secrets are reported only
# as resolved or missing, never printed.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "k8s" ] || { echo "discover.sh: BENCH_TARGET=k8s only" >&2; exit 2; }
# release-controller's database serves the export and the release check;
# state's and execution-controller's serve every rep's hand-off breakdown.
for svc in release-controller state execution-controller; do
  for var in POSTGRES_USER POSTGRES_DB; do
    printf '%s %s=%s\n' "${svc}" "${var}" "$(bench_env_value "${svc}" "${var}" 2>/dev/null || echo MISSING)"
  done
  if bench_env_value "${svc}" POSTGRES_PASSWORD >/dev/null 2>&1; then
    echo "${svc} POSTGRES_PASSWORD: resolved"
  else
    echo "${svc} POSTGRES_PASSWORD: MISSING"
  fi
done
if bench_env_value state REDIS_PASSWORD >/dev/null 2>&1; then
  echo "REDIS_PASSWORD: resolved"
else
  echo "REDIS_PASSWORD: MISSING"
fi
for var in DOCKERHUB_USERNAME MAX_CONCURRENT_JOBS JOB_START_TIMEOUT_SECONDS; do
  printf '%s=%s\n' "${var}" "$(bench_env_value execution-controller "${var}" 2>/dev/null || echo unset)"
done
echo "datastore pods (candidates for BENCH_REDIS_POD and BENCH_PG_POD):"
bench_kubectl get pods -l 'app.kubernetes.io/name in (redis,postgresql)' -o name
bench_kubectl top pods -l '!job-name' --no-headers | head -3
