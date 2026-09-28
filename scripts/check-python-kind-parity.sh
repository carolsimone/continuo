#!/usr/bin/env bash
# Fails if the pinned continuo-python-runtime-postgres image's set of python
# node kinds has drifted from pkg/streams/contract.yaml's node_type
# vocabulary.
#
# The node_type vocabulary in contract.yaml is this repo's side of the python
# node-kind contract: every value whose `runtime` is `python` (today
# `python-node` and `python-csv`) is a kind continuo's own services (parser,
# executor, remediation, UI) know how to route. continuo-python-runtime is
# the OTHER side: it ships from its own repository
# (github.com/carolsimone/continuo-python-runtime) on its own release train,
# and its installed package (continuo_python_runtime.contract.model.KINDS)
# is the set of kinds the pinned runner actually validates and executes.
#
# Nothing arbitrates between the two lists at build time — they can only be
# compared by actually running the pinned image. If contract.yaml adds a
# python kind the runner doesn't know yet, or the runner ships a kind this
# repo hasn't wired into the vocabulary, a release could parse and promote a
# node that the validation runner silently mishandles (or the runner could
# gain a kind that never reaches an operator because nothing here emits it).
# This check pulls the exact image pinned in docker-compose.yml, asks it for
# its own KINDS, and fails unless the two sets are identical.

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPOSE_FILE="${REPO_ROOT}/docker-compose.yml"
CONTRACT_FILE="${REPO_ROOT}/pkg/streams/contract.yaml"

# Matches the full ref: ghcr.io/carolsimone/continuo-python-runtime-postgres:vX.Y.Z
REF_RE='ghcr\.io/carolsimone/continuo-python-runtime-postgres:[A-Za-z0-9._-]+'

if [ ! -f "$COMPOSE_FILE" ]; then
  echo "check-python-kind-parity: ${COMPOSE_FILE} not found" >&2
  exit 1
fi
if [ ! -f "$CONTRACT_FILE" ]; then
  echo "check-python-kind-parity: ${CONTRACT_FILE} not found" >&2
  exit 1
fi

image_ref="$(grep -oE "$REF_RE" "$COMPOSE_FILE" | head -1)"
if [ -z "$image_ref" ]; then
  echo "check-python-kind-parity: no continuo-python-runtime-postgres ref found in ${COMPOSE_FILE}" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "check-python-kind-parity: docker not found on PATH" >&2
  exit 1
fi

if ! docker pull "$image_ref" >/dev/null 2>&1; then
  echo "check-python-kind-parity: docker pull ${image_ref} failed" >&2
  exit 1
fi

# The contract vocabulary's own python kinds: every node_type entry whose
# `runtime` is `python`. Scoped to the `node_type` vocabulary block only, so a
# `value:`/`runtime:` pair belonging to a different vocabulary (e.g. a future
# one that happens to also use those keys) can never leak in.
extract_contract_kinds() {
  awk '
    /^  - name: node_type$/ { in_block = 1; next }
    in_block && /^  - name:/ { in_block = 0 }
    in_block && /^      - value: / {
      val = $0
      sub(/^      - value: /, "", val)
      pending = val
      next
    }
    in_block && /^        runtime: python$/ && pending != "" {
      print pending
      pending = ""
    }
  ' "$1" | sort -u
}

contract_kinds="$(extract_contract_kinds "$CONTRACT_FILE")"
if [ -z "$contract_kinds" ]; then
  echo "check-python-kind-parity: found no python node_type values in ${CONTRACT_FILE} — the extraction pattern may be stale" >&2
  exit 1
fi

# The runner's own KINDS, asked of the image directly rather than assumed —
# a stale local guess would defeat the point of a live-image check.
runtime_kinds_raw="$(docker run --rm --entrypoint python "$image_ref" \
  -c "from continuo_python_runtime.contract.model import KINDS
for k in sorted(KINDS):
    print(k)")"
run_rc=$?
if [ "$run_rc" -ne 0 ]; then
  echo "check-python-kind-parity: docker run against ${image_ref} failed to report KINDS" >&2
  exit 1
fi
runtime_kinds="$(printf '%s\n' "$runtime_kinds_raw" | sort -u)"

if [ -z "$runtime_kinds" ]; then
  echo "check-python-kind-parity: ${image_ref} reported no kinds at all" >&2
  exit 1
fi

if [ "$contract_kinds" != "$runtime_kinds" ]; then
  echo "PYTHON NODE KIND MISMATCH between ${image_ref} and ${CONTRACT_FILE}:" >&2
  echo "  contract.yaml (runtime: python):" >&2
  printf '    %s\n' $contract_kinds >&2
  echo "  ${image_ref} KINDS:" >&2
  printf '    %s\n' $runtime_kinds >&2
  echo "Add the missing kind to whichever side is behind — pkg/streams/contract.yaml's node_type vocabulary (then go generate ./pkg/streams/...) or the pinned continuo-python-runtime image — so both sides agree on every kind." >&2
  exit 1
fi

kinds_list="$(printf '%s, ' $contract_kinds)"
kinds_list="${kinds_list%, }"
echo "python node kinds match (${kinds_list}) between contract.yaml and ${image_ref}"
exit 0
