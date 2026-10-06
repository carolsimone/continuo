#!/usr/bin/env bash
# Fails when a file gen-streams writes from pkg/streams/contract.yaml is not
# what the generator produces from the contract as it stands: a contract edit
# without its regenerated bindings, or a hand edit to a generated file.
# TestGeneratedGoMatchesContract compares constant values only, so a changed
# description, producer list or vocabulary would otherwise pass unnoticed.
#
# It regenerates into the tree, compares each file with the copy that was
# there before, and puts every original back, so the tree is the same
# afterwards whatever the result. It compares against the working tree, not
# the index, so a local run with regenerated but unstaged files passes; CI
# checks out the commit, where the two are the same.
set -euo pipefail
cd "$(dirname "$0")/.."

generated=(
  pkg/streams/streams.gen.go
  pkg/streams/streams_test_access.gen.go
  pkg/domain/model/vocabulary.gen.go
  pkg/domain/model/vocabulary_test_access.gen.go
  topology-controller/streams_contract.py
  topology-controller/domain/contract_vocabulary.py
  ui/src/server/generated/vocabulary.gen.ts
)

snap="$(mktemp -d)"
restore() {
  for f in "${generated[@]}"; do
    if [ -e "${snap}/${f}" ]; then cp -p "${snap}/${f}" "$f"; fi
  done
  rm -rf "$snap"
}
for f in "${generated[@]}"; do
  mkdir -p "${snap}/$(dirname "$f")"
  cp -p "$f" "${snap}/${f}"
done
trap restore EXIT

(cd pkg && go generate ./streams/...)

stale=0
for f in "${generated[@]}"; do
  if ! git diff --no-index --exit-code -- "${snap}/${f}" "$f"; then
    stale=1
  fi
done
if [ "$stale" -ne 0 ]; then
  echo "::error::generated stream bindings differ from pkg/streams/contract.yaml: run 'cd pkg && go generate ./streams/...' and commit the result" >&2
  exit 1
fi
echo "Generated stream bindings match pkg/streams/contract.yaml"
