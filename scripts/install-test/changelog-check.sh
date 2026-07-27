#!/usr/bin/env bash
# Fails a PR that changes the continuo chart's user-facing surface (values.yaml,
# values.schema.json, Chart.yaml, templates/**) without also updating
# CHANGELOG.md. Needs BASE_SHA (the PR's base commit) in the environment;
# install-test.yml sets it from github.event.pull_request.base.sha and fetches
# it before calling this script.
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${BASE_SHA:?BASE_SHA must be set (the PR base commit)}"

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
     "the chart's semver bump per CLAUDE.md's Helm chart versioning section."
exit 1
