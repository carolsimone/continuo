#!/usr/bin/env bash
# Fails if the CI workflow re-implements build/run logic inline instead of calling
# Make targets / scripts. Keeps local and CI on the same primitives.
set -uo pipefail
CI_FILE="${1:-.github/workflows/ci.yml}"
patterns=(
  'docker exec .*go build|inline Go build (use a Make target / start-services.sh)'
  'go run main\.go|inline service start (use start_go_service)'
  'for i in \$\(seq|inline retry/wait loop (use wait_for_* from scripts/lib/common.sh)'
  '/dev/tcp/|inline TCP probe (use wait_for_tcp_port)'
  'scripts/check-[a-z0-9_-]+\.sh|inline guard script (run make guards instead — a gate added to the target must reach CI)'
  'diff .*\.proto|inline proto sync check (covered by make guards)'
)
rc=0
if ! grep -qE '^\s*(- )?run: make guards$' "$CI_FILE"; then
  echo "ALIGNMENT VIOLATION in ${CI_FILE}: no 'run: make guards' step — gates added to the Make target would never run in CI" >&2
  rc=1
fi
for e in "${patterns[@]}"; do re="${e%%|*}"; why="${e#*|}"
  if grep -nE "$re" "$CI_FILE" >/dev/null 2>&1; then
    echo "ALIGNMENT VIOLATION in ${CI_FILE}: ${why}" >&2; grep -nE "$re" "$CI_FILE" >&2; rc=1; fi
done
[ "$rc" -eq 0 ] && echo "CI alignment OK: ${CI_FILE}"; exit "$rc"
