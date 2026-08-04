#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-release-tag-trigger.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# Each case gets its own workflow dir: release.yml plus the tag families owned
# by other workflows.
mkdir -p "$tmp/good" "$tmp/overlap" "$tmp/narrow" "$tmp/inline"

printf 'on:\n  push:\n    tags:\n      - %s\n\njobs:\n  a:\n    steps:\n      - run: docker build -t x .\n' "'v[0-9]*'" > "$tmp/good/release.yml"
printf 'on:\n  push:\n    tags: ["validation-contract-v*"]\n' > "$tmp/good/publish-pypi.yml"
bash "$G" "$tmp/good" >/dev/null 2>&1; assert "disjoint families pass" "[ $? -eq 0 ]"

# A bare v* overlaps any other tag family that also starts with a v.
printf 'on:\n  push:\n    tags:\n      - %s\n' "'v*'" > "$tmp/overlap/release.yml"
printf 'on:\n  push:\n    tags: ["validation-contract-v*"]\n' > "$tmp/overlap/publish-pypi.yml"
bash "$G" "$tmp/overlap" >/dev/null 2>&1; assert "overlapping tag families fail" "[ $? -ne 0 ]"

# A glob narrow enough to dodge every other family but too narrow to release.
printf 'on:\n  push:\n    tags:\n      - %s\n' "'v9[0-9]*'" > "$tmp/narrow/release.yml"
bash "$G" "$tmp/narrow" >/dev/null 2>&1; assert "trigger that drops real releases fails" "[ $? -ne 0 ]"

# Same collision, with release.yml using the inline-array form.
printf 'on:\n  push:\n    tags: ["v*"]\n' > "$tmp/inline/release.yml"
printf 'on:\n  push:\n    tags:\n      - %s\n' "'validation-contract-v*'" > "$tmp/inline/publish-pypi.yml"
bash "$G" "$tmp/inline" >/dev/null 2>&1; assert "inline-array form is parsed too" "[ $? -ne 0 ]"

# Image tags live under jobs:, not on:, and must not be read as triggers.
printf 'on:\n  push:\n    tags:\n      - %s\n\njobs:\n  build:\n    steps:\n      - uses: docker/metadata-action@v5\n        with:\n          tags: ["validation-contract-v*"]\n' "'v[0-9]*'" > "$tmp/good/deploy.yml"
bash "$G" "$tmp/good" >/dev/null 2>&1; assert "image tags outside on: are ignored" "[ $? -eq 0 ]"

# The real workflows must be clean.
bash "$G" "${HERE}/../.github/workflows" >/dev/null 2>&1; assert "repo workflows pass" "[ $? -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
