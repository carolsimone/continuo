#!/usr/bin/env bash
# Prints what the benchmark needs from a k8s install. Secrets are reported only
# as resolved or missing, never printed.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "k8s" ] || { echo "discover.sh: BENCH_TARGET=k8s only" >&2; exit 2; }
for var in POSTGRES_USER POSTGRES_DB; do
  printf '%s=%s\n' "${var}" "$(bench_env_value release-controller "${var}" 2>/dev/null || echo MISSING)"
done
if bench_env_value release-controller POSTGRES_PASSWORD >/dev/null 2>&1; then
  echo "POSTGRES_PASSWORD: resolved"
else
  echo "POSTGRES_PASSWORD: MISSING"
fi
if bench_env_value state REDIS_PASSWORD >/dev/null 2>&1; then
  echo "REDIS_PASSWORD: resolved"
else
  echo "REDIS_PASSWORD: MISSING"
fi
for var in DOCKERHUB_USERNAME MAX_CONCURRENT_JOBS K8S_CHECK_DELAY_SECONDS; do
  printf '%s=%s\n' "${var}" "$(bench_env_value execution-controller "${var}" 2>/dev/null || echo unset)"
done
echo "datastore pods (candidates for BENCH_REDIS_POD and BENCH_PG_POD):"
bench_kubectl get pods -l 'app.kubernetes.io/name in (redis,postgresql)' -o name
bench_kubectl top pods -l '!job-name' --no-headers | head -3
