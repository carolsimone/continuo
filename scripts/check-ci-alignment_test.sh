#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-ci-alignment.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }
printf 'steps:\n  - run: make guards\n  - run: make stack-up\n' > "$tmp/clean.yml"
bash "$G" "$tmp/clean.yml"; assert "clean passes" "[ $? -eq 0 ]"
printf 'steps:\n  - run: docker exec state go build -o bin/state .\n' > "$tmp/d1.yml"
bash "$G" "$tmp/d1.yml"; assert "inline go build fails" "[ $? -ne 0 ]"
printf 'steps:\n  - run: |\n      for i in $(seq 1 30); do :; done\n' > "$tmp/d2.yml"
bash "$G" "$tmp/d2.yml"; assert "inline wait loop fails" "[ $? -ne 0 ]"
printf 'steps:\n  - run: make stack-up\n' > "$tmp/d3.yml"
bash "$G" "$tmp/d3.yml"; assert "missing make guards step fails" "[ $? -ne 0 ]"
printf 'steps:\n  - run: make guards\n  - run: bash scripts/check-release-tag-trigger.sh\n' > "$tmp/d4.yml"
bash "$G" "$tmp/d4.yml"; assert "inline guard script fails" "[ $? -ne 0 ]"
printf 'steps:\n  - run: make guards\n  - run: diff a/state.proto b/state.proto\n' > "$tmp/d5.yml"
bash "$G" "$tmp/d5.yml"; assert "inline proto diff fails" "[ $? -ne 0 ]"
if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
