#!/usr/bin/env bash
# Fails unless the workflow that cancels a closed pull request's `ci` run
# (.github/workflows/cancel-closed-pr-ci.yml) keeps the properties it exists
# for:
#
#   * it is triggered when a pull request closes (`pull_request` with the
#     `closed` type), merged or not;
#   * its token may cancel runs (`actions: write`);
#   * every job runs on a GitHub-hosted ubuntu runner, so it never waits for
#     the self-hosted runners whose runs it cancels;
#   * it lists ci.yml's runs only, and its selection of the runs to cancel
#     takes the closed pull request's own queued and running runs and nothing
#     else: never a push run (main's ci), never another pull request's run.
#
# The last property is checked by running the workflow's own `--jq` program
# against fixture runs, so a selection edited into one that would cancel a
# push run fails here rather than on main. Needs jq.
#
# Usage: check-closed-pr-ci-cancel.sh [workflow-file]
set -uo pipefail

WF="${1:-.github/workflows/cancel-closed-pr-ci.yml}"
rc=0
fail() { echo "CLOSED-PR CI CANCEL VIOLATION in ${WF}: $*" >&2; rc=1; }

if [ ! -f "$WF" ]; then
  echo "check-closed-pr-ci-cancel: ${WF} not found" >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo "check-closed-pr-ci-cancel: jq is required" >&2
  exit 1
fi

# Prints the activity types of the `pull_request` trigger in the `on:` block,
# one per line, in the inline-array form (`types: [closed]`) or the list form.
pull_request_types() {
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    /^[[:space:]]*(#.*)?$/ { next }
    /^[^[:space:]]/ { in_on = ($0 ~ /^on:[[:space:]]*(#.*)?$/); in_pr = 0; in_types = 0; next }
    !in_on { next }
    {
      ind = indent($0)
      if (in_types) {
        if (ind >= types_ind && $0 ~ /^[[:space:]]*-/) {
          item = $0
          sub(/^[[:space:]]*-[[:space:]]*/, "", item)
          sub(/[[:space:]]*(#.*)?$/, "", item)
          gsub(/["'"'"']/, "", item)
          print item
          next
        }
        in_types = 0
      }
      if (in_pr && ind <= pr_ind) in_pr = 0
      if (!in_pr && $0 ~ /^[[:space:]]+pull_request:[[:space:]]*(#.*)?$/) { in_pr = 1; pr_ind = ind; next }
      if (in_pr && $0 ~ /^[[:space:]]+types:/) {
        v = $0
        sub(/^[[:space:]]+types:[[:space:]]*/, "", v)
        sub(/[[:space:]]*#.*$/, "", v)
        if (v == "") { in_types = 1; types_ind = ind; next }
        gsub(/[][ "'"'"']/, "", v)
        n = split(v, items, ",")
        for (i = 1; i <= n; i++) if (items[i] != "") print items[i]
      }
    }
  ' "$1"
}

if ! pull_request_types "$WF" | grep -qx 'closed'; then
  fail "no pull_request trigger with the 'closed' type; a closed pull request would keep its ci run"
fi

if ! grep -Eq '^[[:space:]]+actions:[[:space:]]*write[[:space:]]*(#.*)?$' "$WF"; then
  fail "no 'actions: write' permission; the token could not cancel a run"
fi

runs_on="$(grep -E '^[[:space:]]+runs-on:' "$WF" \
  | sed -E 's/^[[:space:]]+runs-on:[[:space:]]*//; s/[[:space:]]*(#.*)?$//; s/^["'"'"']//; s/["'"'"']$//')"
if [ -z "$runs_on" ]; then
  fail "no runs-on"
fi
while IFS= read -r runner; do
  [ -z "$runner" ] && continue
  if ! [[ "$runner" =~ ^ubuntu-[a-z0-9.]+$ ]]; then
    fail "runs-on '${runner}' is not a GitHub-hosted ubuntu runner; it could wait behind the runs it cancels"
  fi
done <<< "$runs_on"

list_lines="$(grep -c 'gh run list' "$WF")"
if [ "$list_lines" -ne 1 ]; then
  fail "expected one 'gh run list', found ${list_lines}"
fi
if ! grep -Eq 'gh run list .*--workflow ci\.yml( |$)' "$WF"; then
  fail "'gh run list' is not scoped to --workflow ci.yml"
fi
# shellcheck disable=SC2016  # matches the workflow's literal "$id"
if grep -F 'gh run cancel' "$WF" | grep -vqF 'gh run cancel "$id"'; then
  fail "a 'gh run cancel' cancels something other than a selected \$id"
fi

selectors="$(grep -c -- "--jq '" "$WF")"
selector="$(sed -n "s/.*--jq '\([^']*\)'.*/\1/p" "$WF" | head -n 1)"
if [ "$selectors" -ne 1 ] || [ -z "$selector" ]; then
  fail "expected one single-quoted --jq selection of the runs to cancel, found ${selectors}"
else
  head_sha=1111111111111111111111111111111111111111
  other_sha=2222222222222222222222222222222222222222
  runs="[
    {\"databaseId\": 1, \"event\": \"pull_request\", \"headSha\": \"${head_sha}\", \"status\": \"in_progress\"},
    {\"databaseId\": 2, \"event\": \"pull_request\", \"headSha\": \"${head_sha}\", \"status\": \"queued\"},
    {\"databaseId\": 3, \"event\": \"pull_request\", \"headSha\": \"${head_sha}\", \"status\": \"waiting\"},
    {\"databaseId\": 4, \"event\": \"push\", \"headSha\": \"${head_sha}\", \"status\": \"in_progress\"},
    {\"databaseId\": 5, \"event\": \"push\", \"headSha\": \"${other_sha}\", \"status\": \"queued\"},
    {\"databaseId\": 6, \"event\": \"pull_request\", \"headSha\": \"${other_sha}\", \"status\": \"in_progress\"},
    {\"databaseId\": 7, \"event\": \"pull_request\", \"headSha\": \"${head_sha}\", \"status\": \"completed\"}
  ]"
  if ! selected="$(HEAD_SHA="$head_sha" jq -r "$selector" <<< "$runs" 2>&1)"; then
    fail "the --jq selection does not run: ${selected}"
  else
    selected="$(tr '\n' ' ' <<< "$selected" | sed 's/ *$//')"
    for id in $selected; do
      case "$id" in
        1 | 2 | 3) ;;
        4 | 5) fail "the selection takes push run ${id}; it would cancel main's ci" ;;
        6) fail "the selection takes run ${id} of another head commit; it would cancel another pull request's ci" ;;
        7) fail "the selection takes completed run ${id}, which cannot be cancelled" ;;
        *) fail "the selection printed '${id}', not a run id" ;;
      esac
    done
    for id in 1 2 3; do
      if ! grep -qw "$id" <<< "$selected"; then
        fail "the selection leaves the closed pull request's unfinished run ${id} running (selected: '${selected}')"
      fi
    done
  fi
fi

[ "$rc" -eq 0 ] && echo "Closed-PR ci cancel OK: ${WF}"
exit "$rc"
