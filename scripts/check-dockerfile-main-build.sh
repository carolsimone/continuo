#!/usr/bin/env bash
# A Go service whose `package main` spans more than one non-test .go file must
# build the whole package in its Dockerfile.dev (`go build ... .`), never a
# single `go build ... main.go`. The single-file form compiles main.go alone
# and silently omits its sibling files, so a symbol defined in a sibling (e.g.
# release-controller/startup.go's runStartupStep) is undefined and the image
# build fails — a break that `go build ./...` and `go test` never surface
# because they build the package.
set -euo pipefail

cd "$(dirname "$0")/.."

fail=0
for df in */Dockerfile.dev; do
  [ -f "$df" ] || continue
  svc=$(dirname "$df")
  n=$(find "$svc" -maxdepth 1 -name '*.go' ! -name '*_test.go' | wc -l | tr -d ' ')
  [ "$n" -gt 1 ] || continue
  if grep -qE 'go build .* main\.go([[:space:]]|$)' "$df"; then
    echo "ERROR: $df builds 'main.go' but $svc/ has $n non-test files in package main;" >&2
    echo "       build the whole package instead: 'go build -o <out> .'" >&2
    fail=1
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "Dockerfile.dev package builds OK (no single-file main.go build in a multi-file main package)"
fi
exit "$fail"
