#!/usr/bin/env bash
# Fails if a Go module that a production image builds OUTSIDE go.work cannot
# build in module mode with a read-only go.sum.
#
# Inside the workspace, `go build` resolves every dependency hash through
# go.work.sum, so a module's own go.sum can silently fall behind after a
# dependency bump. An image whose Dockerfile copies only that module (build
# context is the module directory, no go.work in sight) runs `go build` in
# module mode, where a missing go.sum entry is a hard error:
#
#   golang.org/x/sys@v0.48.0: missing go.sum entry for go.mod file
#
# Nothing in CI builds those images, so the first place the gap surfaces is
# the deploy workflow on main, where the failed image build skips the deploy
# job and every merge behind it stays out of production. This guard runs the
# same module-mode build with -mod=readonly so the gap fails the PR instead.
#
# MODULES lists the Go modules built that way. The loop over deploy.yml keeps
# the list honest: any build-matrix context that is a Go module directory
# (other than the repo root, which builds under go.work) must be listed here.
set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
DEPLOY_WORKFLOW="${REPO_ROOT}/.github/workflows/deploy.yml"
MODULES="pkg"
fail=0

if [ -f "$DEPLOY_WORKFLOW" ]; then
  for ctx in $(grep -oE 'context: "[^"]+"' "$DEPLOY_WORKFLOW" | sed 's/context: "//; s/"$//' | sort -u); do
    [ "$ctx" = "." ] && continue
    [ -f "${REPO_ROOT}/${ctx}/go.mod" ] || continue
    case " ${MODULES} " in
      *" ${ctx} "*) ;;
      *)
        echo "check-standalone-modules: deploy.yml builds Go module '${ctx}' outside go.work but MODULES in $(basename "$0") does not list it" >&2
        fail=1
        ;;
    esac
  done
fi

for m in ${MODULES}; do
  if [ ! -f "${REPO_ROOT}/${m}/go.mod" ]; then
    echo "check-standalone-modules: ${m}/go.mod not found" >&2
    fail=1
    continue
  fi
  if ! out="$(cd "${REPO_ROOT}/${m}" && GOWORK=off GOFLAGS=-mod=readonly GOPROXY=off go build ./... 2>&1)"; then
    echo "check-standalone-modules: '${m}' does not build in module mode; its go.sum is incomplete for a standalone image build:" >&2
    echo "$out" | head -5 >&2
    echo "  fix: (cd ${m} && GOWORK=off go mod download <module>@<version>) for each module named above, then commit ${m}/go.sum" >&2
    fail=1
  fi
done

if [ "$fail" -eq 0 ]; then
  echo "check-standalone-modules: ok (${MODULES})"
fi
exit "$fail"
