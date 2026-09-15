#!/usr/bin/env bash
# Exercises scripts/check-standalone-modules.sh against throwaway repos.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# make_repo <dir> <pkg-import-line> <go.sum-content> <extra-context>
make_repo() {
  local dir="$1" import_line="$2" gosum="$3" extra_ctx="$4"
  mkdir -p "$dir/pkg/cmd/x" "$dir/.github/workflows"
  printf 'module example.com/pkg\n\ngo 1.22\n\n%s\n' "${5:-}" > "$dir/pkg/go.mod"
  printf '%s' "$gosum" > "$dir/pkg/go.sum"
  printf 'package main\n\n%s\n\nfunc main() {}\n' "$import_line" > "$dir/pkg/cmd/x/main.go"
  printf 'x:\n  matrix:\n    include:\n      - {service: "a", dockerfile: "a/Dockerfile", context: "."}\n      - {service: "b", dockerfile: "pkg/cmd/x/Dockerfile", context: "pkg"}\n%s' "$extra_ctx" > "$dir/.github/workflows/deploy.yml"
}

# 1. stdlib-only module with a complete (empty) go.sum: pass
make_repo "$tmp/ok" 'import _ "fmt"' '' ''
bash "$here/check-standalone-modules.sh" "$tmp/ok" >/dev/null || { echo "FAIL: clean module must pass"; exit 1; }

# 2. a requirement whose hashes are missing from go.sum: fail
make_repo "$tmp/gap" 'import _ "github.com/google/uuid"' '' '' 'require github.com/google/uuid v1.6.0'
if bash "$here/check-standalone-modules.sh" "$tmp/gap" >/dev/null 2>&1; then echo "FAIL: missing go.sum entry must fail"; exit 1; fi

# 3. deploy.yml builds a Go module standalone that MODULES does not list: fail
make_repo "$tmp/unlisted" 'import _ "fmt"' '' '      - {service: "c", dockerfile: "other/Dockerfile", context: "other"}'
mkdir -p "$tmp/unlisted/other" && printf 'module example.com/other\n\ngo 1.22\n' > "$tmp/unlisted/other/go.mod"
if bash "$here/check-standalone-modules.sh" "$tmp/unlisted" >/dev/null 2>&1; then echo "FAIL: unlisted standalone module must fail"; exit 1; fi

echo "check-standalone-modules_test: ok"
