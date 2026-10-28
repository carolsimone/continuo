#!/usr/bin/env bash
# Fails when a Go function or method is unreachable from every service main.
#
# golangci-lint's `unused` cannot see this: it treats exported names as used and
# counts callers in test files. `deadcode` starts from the main packages and
# follows calls, so an exported function that only a test calls is reported.
#
# It must run ONCE with every main package of the workspace listed. A run per
# module reports pkg/ symbols as dead even when another service calls them. It
# reports functions and methods only, never types, constants or fields. The
# cli module sits outside go.work, so it is checked on its own.
#
# scripts/deadcode.allow lists findings that are intentional (test doubles that
# live in non-test files, test-only seams, methods that satisfy an interface).
# One extended regex per line, matched against "path: symbol".
#
# Usage: bash scripts/check-dead-go.sh        (run from anywhere)
set -euo pipefail

# Single source of truth for the pinned version. Bump here only.
DEADCODE_VERSION="v0.51.0"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

bin="${DEADCODE:-$(go env GOPATH)/bin/deadcode}"
if [ ! -x "$bin" ] || ! go version -m "$bin" 2>/dev/null | grep -q "golang.org/x/tools[[:space:]]*${DEADCODE_VERSION}"; then
  echo "Installing deadcode ${DEADCODE_VERSION}..."
  go install "golang.org/x/tools/cmd/deadcode@${DEADCODE_VERSION}"
  bin="$(go env GOPATH)/bin/deadcode"
fi

mains=()
for dir in $(go list -m -f '{{.Dir}}' | sed "s#^${repo_root}/##"); do
  while IFS= read -r p; do mains+=("$p"); done < <(
    cd "$dir" && go list -f '{{if eq .Name "main"}}{{.Dir}}{{end}}' ./... | grep . | sed "s#^${repo_root}#.#"
  )
done

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

"$bin" "${mains[@]}" | sed -E 's#^([^:]+):[0-9]+:[0-9]+: unreachable func: #\1: #' | sort >"$tmp/raw"
(cd cli && GOWORK=off "$bin" ./... | sed -E 's#^([^:]+):[0-9]+:[0-9]+: unreachable func: #cli/\1: #' | sort) >>"$tmp/raw"

grep -vE '^\s*(#|$)' scripts/deadcode.allow >"$tmp/allow"
grep -vEf "$tmp/allow" "$tmp/raw" >"$tmp/left" || true

if [ -s "$tmp/left" ]; then
  echo "deadcode: functions no service main reaches (delete them, or add a justified line to scripts/deadcode.allow):" >&2
  cat "$tmp/left" >&2
  exit 1
fi
echo "deadcode: no unreachable functions"
