#!/usr/bin/env bash
# Fails unless the agent-remediation Dockerfiles pin one continuo-python-runtime
# PyPI version, at or above the runtime version continuo targets everywhere else.
#
# agent-remediation packages a proposed fix by shelling out to the
# `continuo-runtime` CLI (ContractPackager). That CLI comes from the
# continuo-python-runtime PyPI package, pinned by version in both
# agent-remediation/Dockerfile.dev (uv tool install ...==X.Y.Z) and
# agent-remediation/Dockerfile.prod (pip install ...==X.Y.Z). A pin that lags
# the runtime continuo actually targets rejects node kinds the current runtime
# accepts (a stale pin predating the python-node rename rejects
# `kind: python-node`), silently killing otherwise-valid repairs before they
# reach verification.
#
# The runtime version continuo targets is the X.Y.Z of the
# continuo-python-runtime-postgres image docker-compose.yml pins for validation
# — the same string check-validation-image-pin.sh already holds equal across
# every image-ref location, and check-python-kind-parity.sh reads to compare
# the runtime's accepted kinds against the contract. This check ties the PyPI
# pin (a plain `==X.Y.Z`, not a ghcr image ref, so the image-pin guard's
# extraction never sees it) to that X.Y.Z: the PyPI pin may be newer than the
# validation image, never older. The two Dockerfiles pin one version.
#
# The PyPI pin may run ahead of the image: a runtime release can change what
# `pack` writes, which agent-remediation needs at once, together with the
# validation image's op names, which the image pin can take only once
# execution-controller sends them.
set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
COMPOSE="${REPO_ROOT}/docker-compose.yml"
DEV="${REPO_ROOT}/agent-remediation/Dockerfile.dev"
PROD="${REPO_ROOT}/agent-remediation/Dockerfile.prod"

fail=0

# The X.Y.Z of the validation runtime image docker-compose.yml pins.
target="$(grep -oE 'ghcr\.io/carolsimone/continuo-python-runtime-postgres:v[0-9]+\.[0-9]+\.[0-9]+' "$COMPOSE" 2>/dev/null \
  | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+$')"
if [ -z "$target" ]; then
  echo "check-agent-remediation-runtime-pin: no continuo-python-runtime-postgres:vX.Y.Z pin found in docker-compose.yml" >&2
  exit 1
fi

# The PyPI pin in one Dockerfile: continuo-python-runtime==X.Y.Z.
extract_pip_pin() {
  grep -oE 'continuo-python-runtime==[0-9]+\.[0-9]+\.[0-9]+' "$1" 2>/dev/null \
    | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+$'
}

# older_than A B: true when version A is older than version B.
older_than() {
  [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -1)" = "$1" ]
}

check_pin() {
  local name="$1" file="$2"
  local pin
  pin="$(extract_pip_pin "$file")"
  if [ -z "$pin" ]; then
    echo "check-agent-remediation-runtime-pin: no continuo-python-runtime==X.Y.Z pin found in ${name}" >&2
    fail=1
    return
  fi
  if older_than "$pin" "$target"; then
    echo "check-agent-remediation-runtime-pin: ${name} pins continuo-python-runtime==${pin}, older than the ${target} continuo targets (docker-compose.yml validation image)" >&2
    fail=1
  fi
}

check_pin "agent-remediation/Dockerfile.dev" "$DEV"
check_pin "agent-remediation/Dockerfile.prod" "$PROD"

dev_pin="$(extract_pip_pin "$DEV")"
prod_pin="$(extract_pip_pin "$PROD")"
if [ -n "$dev_pin" ] && [ -n "$prod_pin" ] && [ "$dev_pin" != "$prod_pin" ]; then
  echo "check-agent-remediation-runtime-pin: agent-remediation/Dockerfile.dev pins continuo-python-runtime==${dev_pin} but agent-remediation/Dockerfile.prod pins ${prod_pin}" >&2
  fail=1
fi

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "agent-remediation runtime pin OK: continuo-python-runtime==${dev_pin}, validation image ${target}"
exit 0
