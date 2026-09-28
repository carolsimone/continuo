#!/usr/bin/env bash
# Fails if the retired node_type value "python-model" appears anywhere
# outside the small set of places that legitimately still name it.
#
# continuo-python-runtime < 0.6.0 called what the contract vocabulary now
# calls "python-node" by the name "python-model". Every production release
# and every doc, comment, or test written against the current vocabulary
# should say "python-node" — a live reference to "python-model" describes a
# node kind that no longer exists and misleads a reader with no prior
# history of the rename.
#
# The retired string legitimately survives in a bounded set of places, all
# excluded below:
#   - db/migration/**: the historical Flyway migration that rewrites stored
#     JSONB rows from the old value to the new one; the old value is data it
#     operates on, not a kind it still supports.
#   - deploy/continuo/CHANGELOG.md: a historical record (see its own header).
#   - .superpowers/**: this branch's own gitignored scratch/review notes,
#     never part of a checked-out repo in CI.
#   - topology-controller/service/python_kind_rules.py: the runtime-compat
#     alias (STORED_KIND_ALIASES) that resolves a contract stored by an
#     older runtime at parse time — the old string is a real input it must
#     keep recognizing, not stale terminology.
#   - the tests that exercise that alias, the DB/graph migrations, or the
#     retired value's rejection, with the retired string as deliberate
#     input: orchestrator/adapters/neo4j/schema_test.go,
#     pkg/domain/model/nodetype_guard_test.go,
#     release-controller/adapters/postgres/node_type_migration_test.go,
#     topology-controller/tests/test_candidate_manifest_handler.py,
#     topology-controller/tests/test_python_contract_parser.py,
#     topology-controller/tests/test_python_kind_rules.py,
#     ui/tests/client/node-type-icon-family.test.ts.
#   - orchestrator/adapters/neo4j/schema.go: the Cypher data migration that
#     names the old value as the FROM side of a rewrite.
#
# Everywhere else — including a Go test's own name (e.g.
# TestDeploy_PythonModel_...), a doc's prose, or a script's comment — a hit
# is stale terminology left over from before the vocabulary split "python"
# into "python-node" and "python-csv", and must be renamed to "python-node".

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Case-insensitive so it also catches PythonModel/python_model spellings
# (Go test names, Python identifiers), not just the hyphenated contract form.
PATTERN='python[_-]?model'

# This guard's own path (its comment above necessarily spells out the
# retired string it looks for) and every location that legitimately still
# names it — a bounded, hand-reviewed list, not a directory-wide carve-out,
# so a NEW file introducing stale terminology next to one of these is still
# caught.
EXCLUDE_FILES=(
  "scripts/check-retired-node-kind.sh"
  "deploy/continuo/CHANGELOG.md"
  "topology-controller/service/python_kind_rules.py"
  "orchestrator/adapters/neo4j/schema.go"
  "orchestrator/adapters/neo4j/schema_test.go"
  "pkg/domain/model/nodetype_guard_test.go"
  "release-controller/adapters/postgres/node_type_migration_test.go"
  "topology-controller/tests/test_candidate_manifest_handler.py"
  "topology-controller/tests/test_python_contract_parser.py"
  "topology-controller/tests/test_python_kind_rules.py"
  "ui/tests/client/node-type-icon-family.test.ts"
)

is_excluded() {
  local rel="$1" f
  for f in "${EXCLUDE_FILES[@]}"; do
    [ "$rel" = "$f" ] && return 0
  done
  return 1
}

raw_hits="$(grep -rliE \
  --exclude-dir=.git --exclude-dir=worktrees --exclude-dir=.superpowers \
  --exclude-dir=node_modules --exclude-dir=.venv --exclude-dir=migration \
  --exclude-dir=dist --exclude-dir=build \
  --exclude-dir=__pycache__ --exclude-dir=.pytest_cache \
  --exclude='*.pyc' \
  "$PATTERN" "$REPO_ROOT" 2>/dev/null)"

hits=()
while IFS= read -r hit; do
  [ -z "$hit" ] && continue
  rel="${hit#"$REPO_ROOT"/}"
  is_excluded "$rel" || hits+=("$rel")
done <<<"$raw_hits"

if [ "${#hits[@]}" -gt 0 ]; then
  echo "RETIRED NODE KIND \"python-model\" found outside its allowed locations:" >&2
  for rel in "${hits[@]}"; do
    echo "  ${rel}" >&2
  done
  echo "Rename each hit's \"python-model\" (in any casing) to \"python-node\" and re-read the sentence/identifier for sense; the node_type vocabulary no longer has a \"python-model\" kind." >&2
  exit 1
fi

echo "check-retired-node-kind: OK — no live \"python-model\" references outside db/migration, the CHANGELOG, .superpowers, the runtime-compat alias, its tests, and the neo4j schema migration"
exit 0
