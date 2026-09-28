#!/usr/bin/env bash
# The python script node kind is "python-node". The retired value
# "python-model" may appear only where stored data written under it is read or
# rewritten, in immutable history, and in the tests that pin those paths.
set -euo pipefail
cd "$(dirname "$0")/.."
if git grep -n "python-model" -- \
    ':!db/migration' ':!deploy/continuo/CHANGELOG.md' ':!docs/release-notes.md' ':!docs/superpowers' \
    ':!scripts/check-retired-node-kind.sh' \
    ':!topology-controller/service/python_kind_rules.py' \
    ':!topology-controller/tests/test_python_kind_rules.py' \
    ':!topology-controller/tests/test_python_contract_parser.py' \
    ':!topology-controller/tests/test_candidate_manifest_handler.py' \
    ':!orchestrator/adapters/neo4j/schema.go' ':!orchestrator/adapters/neo4j/schema_test.go' \
    ':!release-controller/adapters/postgres/node_type_migration_test.go' \
    ':!pkg/domain/model/nodetype_guard_test.go' \
    ':!ui/tests/client/node-type-icon-family.test.ts'; then
  echo "ERROR: retired node kind 'python-model' found (see matches above)" >&2
  exit 1
fi
