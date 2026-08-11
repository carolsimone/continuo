#!/usr/bin/env bash
# Fails if scripts/setup.sh can reach its first kubectl call without having
# exported the kind cluster's kubeconfig.
#
# setup.sh only creates the cluster when one of that name is not already
# present, and `kind create cluster` is the single command that writes the
# cluster's context into the kubeconfig. So an export placed inside that
# create-only branch leaves the reuse path with no context at all: a host that
# holds a running cluster whose context is missing from the kubeconfig — a CI
# runner whose home directory was recycled between jobs is the usual way —
# takes the reuse branch, skips the export, and every later kubectl call
# silently falls back to the localhost:8080 default.
#
# The failure that produces is maximally misleading. It surfaces as
# "connection refused" from `kubectl wait`, roughly a hundred lines and several
# minutes after the branch that actually caused it, pointing at a control plane
# that is running perfectly well.
#
# The invariant this enforces is positional: the export must sit OUTSIDE the
# cluster-creation conditional (so both paths run it) and BEFORE the first
# kubectl call that talks to the cluster. Checking the text is present is not
# enough — the bug this guards against is an export in the wrong place, not a
# missing one.

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
SETUP="${REPO_ROOT}/scripts/setup.sh"

if [ ! -f "$SETUP" ]; then
  echo "check-kubeconfig-export: ${SETUP} not found" >&2
  exit 1
fi

# The conditional that decides whether to create the cluster.
create_guard_line="$(grep -n 'kind get clusters' "$SETUP" | head -1 | cut -d: -f1)"
if [ -z "$create_guard_line" ]; then
  echo "check-kubeconfig-export: no 'kind get clusters' guard found in scripts/setup.sh — this check is stale" >&2
  exit 1
fi

# The end of that if/else block: the first bare `fi` at column 0 after it.
close_line="$(awk -v start="$create_guard_line" 'NR > start && /^fi[[:space:]]*$/ { print NR; exit }' "$SETUP")"
if [ -z "$close_line" ]; then
  echo "check-kubeconfig-export: could not find the end of the cluster-creation block — this check is stale" >&2
  exit 1
fi

export_line="$(grep -n 'kind export kubeconfig' "$SETUP" | head -1 | cut -d: -f1)"
if [ -z "$export_line" ]; then
  echo "check-kubeconfig-export: scripts/setup.sh never runs 'kind export kubeconfig'." >&2
  echo "  The reuse path (cluster already exists) then has no kubeconfig, and every" >&2
  echo "  later kubectl call falls back to localhost:8080 and fails with a misleading" >&2
  echo "  'connection refused' far from the real cause." >&2
  exit 1
fi

if [ "$export_line" -lt "$close_line" ]; then
  echo "check-kubeconfig-export: 'kind export kubeconfig' (line ${export_line}) is INSIDE the" >&2
  echo "  cluster-creation block (lines ${create_guard_line}-${close_line}), so it only runs when this" >&2
  echo "  invocation creates the cluster. Move it after line ${close_line} so the reuse path" >&2
  echo "  exports a kubeconfig too." >&2
  exit 1
fi

# The first kubectl call that actually talks to the cluster.
kubectl_line="$(grep -n '^[[:space:]]*kubectl ' "$SETUP" | head -1 | cut -d: -f1)"
if [ -n "$kubectl_line" ] && [ "$export_line" -gt "$kubectl_line" ]; then
  echo "check-kubeconfig-export: 'kind export kubeconfig' (line ${export_line}) runs AFTER the first" >&2
  echo "  kubectl call (line ${kubectl_line}), which will therefore use whatever context the" >&2
  echo "  kubeconfig happened to hold." >&2
  exit 1
fi

echo "Kubeconfig export OK: scripts/setup.sh exports on both the create and reuse paths (line ${export_line})."
exit 0
