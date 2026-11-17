#!/usr/bin/env bash
# Fails a PR that changes the continuo chart's user-facing surface (values.yaml,
# values.schema.json, Chart.yaml, templates/**) without also updating
# CHANGELOG.md, a PR whose CHANGELOG.md change adds a line under an
# already released version (changelog-placement-check.sh), a PR that
# changes the generated Redis rules (deploy/redis-acl-rules.acl) without a
# CHANGELOG.md line that says "Redis rule", and a PR that moves
# topology-controller's continuo-contracts pin (topology-controller/pyproject.toml)
# without a CHANGELOG.md line that names the new version. Needs BASE_SHA (the
# PR's base commit) in the environment; install-test.yml sets it from
# github.event.pull_request.base.sha and fetches it before calling this script.
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${BASE_SHA:?BASE_SHA must be set (the PR base commit)}"

# The Redis rules are applied by the operator's own tooling, which reads the
# CHANGELOG for what to re-apply: a change to them is named there with the
# words "Redis rule".
if [ -n "$(git diff --name-only "${BASE_SHA}" HEAD -- deploy/redis-acl-rules.acl)" ]; then
  if ! git diff -U0 "${BASE_SHA}" HEAD -- deploy/continuo/CHANGELOG.md | grep -Eq '^\+[^+].*Redis rule'; then
    echo "::error::deploy/redis-acl-rules.acl changed without a CHANGELOG.md line naming the Redis rule change."
    echo
    echo "Add a line containing the words 'Redis rule' under '## [Unreleased]'" \
         "in deploy/continuo/CHANGELOG.md, saying which login's rule changed" \
         "so operators re-apply the rules before they upgrade."
    exit 1
  fi
  echo "deploy/redis-acl-rules.acl changed and CHANGELOG.md names the Redis rule change. OK."
fi

# topology-controller reads every Python contract with the continuo-contracts
# version it pins, and a service's CI pins the same version, which it learns
# from the CHANGELOG. A pin change is therefore named in the CHANGELOG as
# "continuo-contracts <version>", with the new version. A comment line that
# names the library is not a pin.
pin_file=topology-controller/pyproject.toml
pin_diff="$(git diff -U0 "${BASE_SHA}" HEAD -- "$pin_file")"
pin_version(){ sed -n -E 's/^'"$1"'[^#]*continuo-contracts[^0-9"]*([0-9][0-9.]*[0-9]).*$/\1/p' <<<"$pin_diff" | head -n 1; }
if grep -Eq '^\+[^#+][^#]*continuo-contracts' <<<"$pin_diff"; then
  new_pin="$(pin_version '\+')"
  old_pin="$(pin_version '-')"
  if [ -z "$new_pin" ]; then
    echo "::error::$pin_file pins continuo-contracts without a version number."
    echo
    echo "Pin it to one exact version (continuo-contracts==X.Y.Z), so the CHANGELOG.md" \
         "can name the version topology-controller reads contracts with."
    exit 1
  fi
  if [ "$new_pin" != "$old_pin" ]; then
    named="continuo-contracts\`?[ =]+v?${new_pin//./\\.}([^0-9]|$)"
    if ! git diff -U0 "${BASE_SHA}" HEAD -- deploy/continuo/CHANGELOG.md | grep -Eq "^\+[^+].*${named}"; then
      echo "::error::$pin_file moves the continuo-contracts pin to ${new_pin} without a CHANGELOG.md line naming 'continuo-contracts ${new_pin}'."
      echo
      echo "Add a line containing the words 'continuo-contracts ${new_pin}' under" \
           "'## [Unreleased]' in deploy/continuo/CHANGELOG.md, saying what the new" \
           "version reads so service teams pin the same version in their CI."
      exit 1
    fi
    echo "$pin_file moves the continuo-contracts pin to ${new_pin} and CHANGELOG.md names it. OK."
  fi
fi

# Two-commit diff (not three-dot): compares tree contents directly, so it
# works even when BASE_SHA was fetched as a disconnected shallow object with
# no shared history graph to compute a merge-base from.
changed="$(git diff --name-only "${BASE_SHA}" HEAD -- deploy/continuo)"

if [ -z "$changed" ]; then
  echo "No deploy/continuo changes in this PR; nothing to check."
  exit 0
fi

echo "deploy/continuo files changed in this PR:"
echo "$changed"

if grep -q '^deploy/continuo/CHANGELOG\.md$' <<<"$changed"; then
  if ! bash scripts/install-test/changelog-placement-check.sh; then
    exit 1
  fi
  echo "CHANGELOG.md updated alongside chart changes. OK."
  exit 0
fi

surface="$(grep -Ev '^deploy/continuo/(CHANGELOG\.md|README\.md|\.kube-linter\.yaml)$' <<<"$changed" || true)"
if [ -z "$surface" ]; then
  echo "Only meta files (README/kube-linter config) changed; no changelog entry required."
  exit 0
fi

echo "::error::deploy/continuo changed without a CHANGELOG.md entry:"
echo "$surface"
echo
echo "Add an entry under '## [Unreleased]' in deploy/continuo/CHANGELOG.md" \
     "describing the change (Added/Changed/Removed/Breaking), then decide" \
     "the chart's semver bump per AGENTS.md's Helm chart versioning section."
exit 1
