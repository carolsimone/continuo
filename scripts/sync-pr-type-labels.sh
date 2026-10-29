#!/usr/bin/env bash
# Makes a pull request's type labels match its title.
#
#   PR_TITLE='fix(run-controller): ...' GH_REPO=owner/repo scripts/sync-pr-type-labels.sh 123
#
# The labels pr-title-type.sh prints for the title are added when missing. Every
# other type label (bug, enhancement, refactoring, documentation, performance,
# ci/cd, security, test, chore) and breaking-change is removed, so a title that
# changes from `fix:` to `feat:` swaps the label instead of stacking a second
# one. Labels outside that set (area/*, p1-p3, benchmark, ...) are never touched.
# A title that is not a Conventional Commit title leaves every label alone: the
# `pr-title` check reports that on the pull request.
set -euo pipefail

pr="${1:?usage: sync-pr-type-labels.sh <pull-request-number>}"
: "${PR_TITLE?PR_TITLE must be set}" "${GH_REPO:?GH_REPO must be set}"

managed=(bug enhancement refactoring documentation performance ci/cd security test chore breaking-change)

if ! desired="$(bash "$(dirname "${BASH_SOURCE[0]}")/pr-title-type.sh" "$PR_TITLE" 2>/dev/null)"; then
  echo "The title of #${pr} is not a Conventional Commit title; its labels are left alone."
  exit 0
fi

current="$(gh api "repos/${GH_REPO}/issues/${pr}/labels" --paginate --jq '.[].name')"

for label in "${managed[@]}"; do
  have=false; want=false
  grep -qxF -- "$label" <<<"$current" && have=true
  grep -qxF -- "$label" <<<"$desired" && want=true
  if $want && ! $have; then
    gh api -X POST "repos/${GH_REPO}/issues/${pr}/labels" -f "labels[]=${label}" >/dev/null
    echo "Added ${label}."
  elif $have && ! $want; then
    # A label name is a URL path segment: the slash in ci/cd must be escaped.
    gh api -X DELETE "repos/${GH_REPO}/issues/${pr}/labels/${label//\//%2F}" >/dev/null
    echo "Removed ${label}."
  fi
done
