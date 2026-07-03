#!/usr/bin/env bash
# Runs golangci-lint across the Go modules, or one named module.
#
# This is the single primitive shared by `make lint-go` and CI, so local and CI
# run byte-identical rules. The module list is derived from go.work (the
# authoritative workspace registry) plus the cli module, which lives outside the
# workspace by design (see CLAUDE.md). We never scan for go.mod files, because
# .claude/worktrees/ holds full stale copies of every module.
#
# Usage:
#   scripts/lint-go.sh            # lint every module
#   scripts/lint-go.sh state      # lint one module
set -uo pipefail

# Single source of truth for the pinned version. Bump here only.
GOLANGCI_VERSION="v2.12.2"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required (used to read go.work)" >&2
  exit 1
fi

gopath_bin="$(go env GOPATH)/bin"

install_golangci() {
  if command -v golangci-lint >/dev/null 2>&1 && \
     golangci-lint version 2>/dev/null | grep -q "${GOLANGCI_VERSION#v}"; then
    return 0
  fi
  echo "Installing golangci-lint ${GOLANGCI_VERSION}..."
  curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/HEAD/install.sh \
    | sh -s -- -b "${gopath_bin}" "${GOLANGCI_VERSION}"
}

# Authoritative workspace members + cli (outside go.work by design).
modules() {
  go work edit -json | jq -r '.Use[].DiskPath'
  echo "./cli"
}

install_golangci
export PATH="${gopath_bin}:${PATH}"

if [ "$#" -ge 1 ]; then
  targets="./$1"
else
  targets="$(modules)"
fi

rc=0
for m in ${targets}; do
  echo "==> linting ${m}"
  ( cd "${m}" && golangci-lint run ./... ) || rc=1
done

exit "${rc}"
