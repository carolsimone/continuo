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
# Phase alone misses crashlooping pods: kubelet reports them phase=Running
# (with a not-ready container) between restarts, so a phase-only filter never
# selects them and their logs never make it into this dump. Select on BOTH
# a non-terminal-success phase AND any container that isn't ready.
for p in $(kubectl -n "$NS" get pods -o json | jq -r '
    .items[]
    | select(
        (.status.phase != "Running" and .status.phase != "Succeeded")
        or ([.status.containerStatuses[]?.ready] | any(. == false))
      )
    | .metadata.name'); do
  echo "--- pod/${p} (describe, tail)"
  kubectl -n "$NS" describe "pod/${p}" | tail -40
  echo "--- pod/${p} (logs)"
  kubectl -n "$NS" logs "pod/${p}" --all-containers --tail=100 --prefix || true
  echo "--- pod/${p} (previous logs)"
  kubectl -n "$NS" logs "pod/${p}" --all-containers --tail=100 --prefix --previous || true
done
echo "=== helm status ==="
helm -n "$NS" status continuo || true
exit 0
