#!/usr/bin/env bash
# A Go service whose `package main` spans more than one non-test .go file must
# build the whole package in its Dockerfile.dev (`go build ... .`), never a
# single `go build ... main.go`. The single-file form compiles main.go alone
# and silently omits its sibling files, so a symbol defined in a sibling (e.g.
# release-controller/startup.go's runStartupStep) is undefined and the image
# build fails — a break that `go build ./...` and `go test` never surface
# because they build the package. The shared e2e launcher
# (scripts/lib/common.sh) starts those same services with `go run`, so it is held
# to the same rule: `go run .`, never `go run main.go`.
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

# The shared e2e launcher starts services whose package main spans several
# files, so it must run the whole package (`go run .`), never `main.go` alone.
launcher=scripts/lib/common.sh
if grep -qE 'go (run|build)( [^"]*)? main\.go([[:space:]"]|$)' "$launcher"; then
  echo "ERROR: $launcher runs/builds 'main.go' alone; the services it launches may have a" >&2
  echo "       multi-file package main. Use the whole package: 'go run .'" >&2
  fail=1
fi

if [ "$fail" -eq 0 ]; then
  echo "package builds OK (no single-file main.go build in Dockerfile.dev or the e2e launcher)"
fi
exit "$fail"
