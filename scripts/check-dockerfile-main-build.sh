#!/usr/bin/env bash
# A Go service whose `package main` spans more than one non-test .go file must
# build the whole package in its Dockerfile.dev (`go build ... .`), never a
# single `go build ... main.go`. The single-file form compiles main.go alone
# and silently omits its sibling files, so a symbol defined in a sibling (e.g.
# release-controller/startup.go's runStartupStep) is undefined and the image
# build fails — a break that `go build ./...` and `go test` never surface
# because they build the package. The shared e2e launcher
# (scripts/lib/common.sh) starts those same services with `go run`, so it is held
# to the same rule: `go run .`, never `go run main.go`. So are the other places
# that relaunch a service with `go run`: the e2e harness (tests/e2e/*.go) and the
# bench launcher (scripts/bench/start_local_services.sh).
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

# Every place that launches a service with `go run`/`go build` starts services
# whose package main may span several files, so it must run the whole package
# (`go run .`), never `main.go` alone: the shared e2e launcher, the e2e harness
# (which relaunches services in their containers) and the bench launcher.
single_file_main='go (run|build)( [^"]*)? main\.go([[:space:]"]|$)'
launchers=(scripts/lib/common.sh scripts/bench/start_local_services.sh)
for f in tests/e2e/*.go; do
  [ -f "$f" ] && launchers+=("$f")
done
for launcher in "${launchers[@]}"; do
  [ -f "$launcher" ] || continue
  if hits=$(grep -nE "$single_file_main" "$launcher"); then
    while IFS= read -r hit; do
      echo "ERROR: $launcher:${hit%%:*} runs/builds 'main.go' alone; the services it launches may have a" >&2
      echo "       multi-file package main. Use the whole package: 'go run .'" >&2
    done <<<"$hits"
    fail=1
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "package builds OK (no single-file main.go build in Dockerfile.dev, the e2e launcher, the e2e harness or the bench launcher)"
fi
exit "$fail"
