#!/usr/bin/env bash
# Dump cluster state after a failed install-test run. Best-effort output for
# the CI log; never fails itself.
set -uo pipefail
NS="${NS:-continuo}"

echo "=== pods ==="
kubectl -n "$NS" get pods -o wide
echo "=== recent events ==="
kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -50
echo "=== unhealthy pod details ==="
for p in $(kubectl -n "$NS" get pods \
    --field-selector=status.phase!=Running,status.phase!=Succeeded -o name); do
  echo "--- ${p} (describe, tail)"
  kubectl -n "$NS" describe "$p" | tail -40
  echo "--- ${p} (logs)"
  kubectl -n "$NS" logs "$p" --all-containers --tail=100 --prefix || true
done
echo "=== helm status ==="
helm -n "$NS" status continuo || true
exit 0
