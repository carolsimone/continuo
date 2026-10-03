#!/usr/bin/env bash
# Lists (default) or deletes (--apply) finished dbt Jobs that carry no TTL: Jobs
# created before the 24 h TTL existed, which Kubernetes never removes.
# Deleting a Job also deletes its pods.
#   cleanup_legacy_jobs.sh [--apply]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
names="$(bench_kubectl get jobs -l app=dbt-job -o json | python3 "${here}/k8s_json.py" legacy-jobs)"
count="$(printf '%s\n' "${names}" | grep -c . || true)"
echo "cleanup_legacy_jobs.sh: ${count} legacy Jobs" >&2
if [ "${1:-}" != "--apply" ]; then
  printf '%s\n' "${names}" | head -5 >&2
  echo "cleanup_legacy_jobs.sh: dry run; pass --apply to delete" >&2
  exit 0
fi
batch=()
while IFS= read -r name; do
  [ -n "${name}" ] || continue
  batch+=("${name}")
  if [ "${#batch[@]}" -ge 100 ]; then
    bench_kubectl delete jobs --cascade=background "${batch[@]}" >/dev/null
    batch=()
  fi
done <<< "${names}"
if [ "${#batch[@]}" -gt 0 ]; then
  bench_kubectl delete jobs --cascade=background "${batch[@]}" >/dev/null
fi
echo "cleanup_legacy_jobs.sh: deleted ${count} legacy Jobs" >&2
