#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-agent-remediation-runtime-pin.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# Builds a fixture repo root with a docker-compose.yml pinning the given
# validation image tag and the two agent-remediation Dockerfiles pinning the
# given PyPI versions.
mkfix(){ # $1 dir  $2 image-tag  $3 dev-pin  $4 prod-pin
  mkdir -p "$1/agent-remediation"
  printf 'services:\n  x:\n    environment:\n      - VALIDATION_IMAGE=ghcr.io/carolsimone/continuo-python-runtime-postgres:v%s\n' "$2" > "$1/docker-compose.yml"
  printf 'FROM base\nRUN uv tool install --python 3.14 continuo-python-runtime==%s\n' "$3" > "$1/agent-remediation/Dockerfile.dev"
  printf 'FROM py\nRUN pip install --no-cache-dir continuo-python-runtime==%s\n' "$4" > "$1/agent-remediation/Dockerfile.prod"
}

mkfix "$tmp/match" 0.6.0 0.6.0 0.6.0
bash "$G" "$tmp/match" >/dev/null 2>&1; assert "pins equal to the target pass" "[ $? -eq 0 ]"

mkfix "$tmp/ahead" 0.6.0 0.7.0 0.7.0
bash "$G" "$tmp/ahead" >/dev/null 2>&1; assert "pins newer than the target pass" "[ $? -eq 0 ]"

mkfix "$tmp/ahead-minor" 0.9.0 0.10.0 0.10.0
bash "$G" "$tmp/ahead-minor" >/dev/null 2>&1; assert "pins compare as versions, not text (0.10.0 is newer than 0.9.0)" "[ $? -eq 0 ]"

mkfix "$tmp/split" 0.6.0 0.7.0 0.8.0
bash "$G" "$tmp/split" >/dev/null 2>&1; assert "dev and prod pins that differ fail" "[ $? -ne 0 ]"

mkfix "$tmp/devdrift" 0.6.0 0.4.0 0.6.0
bash "$G" "$tmp/devdrift" >/dev/null 2>&1; assert "dev pin lagging the target fails" "[ $? -ne 0 ]"

mkfix "$tmp/proddrift" 0.6.0 0.6.0 0.4.0
bash "$G" "$tmp/proddrift" >/dev/null 2>&1; assert "prod pin lagging the target fails" "[ $? -ne 0 ]"

mkfix "$tmp/nopin" 0.6.0 0.6.0 0.6.0
printf 'FROM py\nRUN pip install --no-cache-dir something-else\n' > "$tmp/nopin/agent-remediation/Dockerfile.prod"
bash "$G" "$tmp/nopin" >/dev/null 2>&1; assert "a Dockerfile that stopped pinning the runtime fails" "[ $? -ne 0 ]"

# The real repo must be clean.
bash "$G" "${HERE}/.." >/dev/null 2>&1; assert "repo pins pass" "[ $? -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
