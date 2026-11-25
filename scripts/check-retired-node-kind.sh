#!/usr/bin/env bash
# The python script node kind is "python-node". The retired value
# "python-model" may appear only where stored data written under it is read or
# rewritten, in immutable history, and in the tests that pin those paths.
set -euo pipefail
cd "$(dirname "$0")/.."
if git grep -n "python-model" -- \
    ':!db/migration' ':!deploy/continuo/CHANGELOG.md' ':!docs/release-notes.md' ':!docs/superpowers' \
    ':!scripts/check-retired-node-kind.sh' \
    ':!pkg/domain/model/nodetype_guard_test.go' \
    ':!agent-remediation/adapters/repofs/contract_locator.go' \
    ':!agent-remediation/adapters/repofs/contract_locator_test.go' \
    ':!ui/tests/client/node-type-icon-family.test.ts'; then
  echo "ERROR: retired node kind 'python-model' found (see matches above)" >&2
  exit 1
fi
