#!/usr/bin/env bash
# Fails unless the workflow that cancels a closed pull request's `ci` run
# (.github/workflows/cancel-closed-pr-ci.yml) keeps the properties it exists
# for:
#
#   * it is triggered by `pull_request_target` with the `closed` type, and by
#     no `pull_request` trigger: GitHub starts no pull_request workflow for a
#     pull request with merge conflicts, so closing one would leave its ci
#     running;
#   * its token may cancel runs (`actions: write`);
#   * every job runs on a GitHub-hosted ubuntu runner, so it never waits for
#     the self-hosted runners whose runs it cancels;
#   * pull_request_target holds the base repository's token, so no pull
#     request code and no pull-request-controlled text reaches a shell: no
#     `actions/checkout` step, no `${{ }}` expression inside a `run:` block
#     (the step reads the pull request's values from env:), and fork pull
#     requests are skipped;
#   * it lists ci.yml's runs only and cancels nothing but a run it selected.
#
# It then runs the workflow's own cancel step (its `run:` block) with a stub
# gh against fixture runs, so the selection and the re-check the workflow
# actually ships are what is tested. With the pull request still closed the
# step must cancel exactly the runs a pull_request event started at its head
# commit before the closure that triggered the job and that have not
# finished: never a push run (main's ci), a run of another head commit or of
# another workflow, a finished run, or a run started after the closure (the
# pull request was reopened). With the pull request open again it must
# cancel nothing. Needs jq.
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

# trigger_types EVENT FILE: the activity types of EVENT's trigger in the `on:`
# block, one per line, in the inline-array form (`types: [closed]`) or the
# list form.
trigger_types() {
  awk -v event="$1" '
    function indent(s) { match(s, /^ */); return RLENGTH }
    /^[[:space:]]*(#.*)?$/ { next }
    /^[^[:space:]]/ { in_on = ($0 ~ /^on:[[:space:]]*(#.*)?$/); in_ev = 0; in_types = 0; next }
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
      if (in_ev && ind <= ev_ind) in_ev = 0
      if (!in_ev && $0 ~ ("^[[:space:]]+" event ":[[:space:]]*(#.*)?$")) { in_ev = 1; ev_ind = ind; next }
      if (in_ev && $0 ~ /^[[:space:]]+types:/) {
        v = $0
        sub(/^[[:space:]]+types:[[:space:]]*/, "", v)
        sub(/[[:space:]]*#.*$/, "", v)
        if (v == "") { in_types = 1; types_ind = ind; next }
        gsub(/[][ "'"'"']/, "", v)
        n = split(v, items, ",")
        for (i = 1; i <= n; i++) if (items[i] != "") print items[i]
      }
    }
  ' "$2"
}

# run_blocks FILE: the text of every `run:` step, single-line or block.
run_blocks() {
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    in_block {
      if ($0 ~ /^[[:space:]]*$/) next
      if (indent($0) > run_ind) { print; next }
      in_block = 0
    }
    /^[[:space:]]*(- )?run:[[:space:]]*[|>][-+]?[[:space:]]*$/ { in_block = 1; run_ind = indent($0); next }
    /^[[:space:]]*(- )?run:[[:space:]]*[^[:space:]]/ { v = $0; sub(/^[[:space:]]*(- )?run:[[:space:]]*/, "", v); print v }
  ' "$1"
}

# cancel_step FILE: the `run: |` block that lists the runs, dedented.
cancel_step() {
  awk '
    function indent(s) { match(s, /^ */); return RLENGTH }
    function flush() { if (buf ~ /gh run list/ && !done) { printf "%s", buf; done = 1 } buf = "" }
    in_block {
      if ($0 ~ /^[[:space:]]*$/) { buf = buf "\n"; next }
      if (indent($0) > run_ind) {
        if (body_ind < 0) body_ind = indent($0)
        buf = buf substr($0, body_ind + 1) "\n"
        next
      }
      in_block = 0; flush()
    }
    /^[[:space:]]*(- )?run:[[:space:]]*\|[-+]?[[:space:]]*$/ { in_block = 1; run_ind = indent($0); body_ind = -1; buf = ""; next }
    END { if (in_block) flush() }
  ' "$1"
}

# --- trigger -----------------------------------------------------------------
if ! trigger_types pull_request_target "$WF" | grep -qx 'closed'; then
  fail "no pull_request_target trigger with the 'closed' type; closing a pull request with merge conflicts starts no pull_request workflow, so its ci would keep running"
fi
if awk '/^[^[:space:]]/ { in_on = ($0 ~ /^on:/) } in_on' "$WF" \
    | grep -Eq '(^[[:space:]]+pull_request:|^on:.*pull_request([^_]|$))'; then
  fail "a pull_request trigger; the workflow is triggered by pull_request_target alone"
fi

# --- token and runner ----------------------------------------------------------
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

# --- no pull request code, no pull request text in a shell ---------------------
if grep -Eq 'uses:[[:space:]]*["'"'"']?actions/checkout' "$WF"; then
  fail "an actions/checkout step; under pull_request_target the job must not fetch or run the pull request's code"
fi
# shellcheck disable=SC2016  # the literal ${{ is what must not appear
if run_blocks "$WF" | grep -q '\${{'; then
  fail "a \${{ }} expression inside a run: block; pass pull request values through env: and quote them"
fi
if ! grep -qF 'github.event.pull_request.head.repo.full_name == github.repository' "$WF"; then
  fail "no skip of fork pull requests (head.repo.full_name == github.repository)"
fi
# shellcheck disable=SC2016  # literal workflow expressions
for mapping in \
  'GH_REPO: ${{ github.repository }}' \
  'PR_NUMBER: ${{ github.event.pull_request.number }}' \
  'HEAD_REF: ${{ github.event.pull_request.head.ref }}' \
  'HEAD_SHA: ${{ github.event.pull_request.head.sha }}' \
  'CLOSED_AT: ${{ github.event.pull_request.closed_at }}'; do
  if ! grep -qF "$mapping" "$WF"; then
    fail "the cancel step's env: lacks '${mapping}'"
  fi
done

# --- what it lists and cancels ---------------------------------------------------
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

# --- the cancel step against fixture runs ----------------------------------------
stub="$(mktemp -d)"
trap 'rm -rf "$stub"' EXIT
cancel_step "$WF" > "$stub/step.sh"
if ! grep -q 'gh run list' "$stub/step.sh"; then
  fail "no 'run: |' step that lists the runs"
else
  mkdir -p "$stub/bin"
  cat > "$stub/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "$1 ${2:-}" in
  "run list")
    prog=""
    while [ $# -gt 0 ]; do
      if [ "$1" = "--jq" ]; then prog="$2"; break; fi
      shift
    done
    [ -n "$prog" ] || { echo "stub gh: run list without --jq" >&2; exit 2; }
    jq -r "$prog" <<< "$STUB_RUNS" ;;
  "run cancel") echo "$3" >> "$STUB_DIR/cancelled" ;;
  "run view") echo in_progress ;;
  "api repos/${GH_REPO}/pulls/${PR_NUMBER}") echo "$STUB_PR_STATE" ;;
  *) echo "stub gh: unexpected call: gh $*" >&2; exit 2 ;;
esac
STUB
  chmod +x "$stub/bin/gh"

  head_sha=1111111111111111111111111111111111111111
  other_sha=2222222222222222222222222222222222222222
  closed_at=2026-10-27T12:00:00Z
  before=2026-10-27T11:00:00Z
  after=2026-10-27T12:05:00Z
  run_json() { printf '{"databaseId": %s, "workflowName": "%s", "event": "%s", "headSha": "%s", "status": "%s", "createdAt": "%s"}' "$@"; }
  runs="[
    $(run_json 1 ci pull_request "$head_sha" in_progress "$before"),
    $(run_json 2 ci pull_request "$head_sha" queued "$before"),
    $(run_json 3 ci pull_request "$head_sha" waiting "$closed_at"),
    $(run_json 4 ci push "$head_sha" in_progress "$before"),
    $(run_json 5 ci push "$other_sha" queued "$before"),
    $(run_json 6 ci pull_request "$other_sha" in_progress "$before"),
    $(run_json 7 ci pull_request "$head_sha" completed "$before"),
    $(run_json 8 ci pull_request "$head_sha" in_progress "$after"),
    $(run_json 9 install-test pull_request "$head_sha" in_progress "$before")
  ]"

  # scenario PR-STATE: runs the step; its exit code lands in $step_rc and the
  # ids it cancelled, space-separated, in $cancelled.
  scenario() {
    : > "$stub/cancelled"
    (cd "$stub" && PATH="$stub/bin:$PATH" STUB_DIR="$stub" STUB_RUNS="$runs" STUB_PR_STATE="$1" \
      GH_TOKEN=stub GH_REPO=owner/repo PR_NUMBER=7 HEAD_REF=feature HEAD_SHA="$head_sha" CLOSED_AT="$closed_at" \
      bash -eo pipefail "$stub/step.sh") > "$stub/out" 2>&1
    step_rc=$?
    cancelled="$(tr '\n' ' ' < "$stub/cancelled" | sed 's/ *$//')"
  }

  scenario closed
  if [ "$step_rc" -ne 0 ]; then
    fail "the cancel step fails on fixture runs: $(tr '\n' ' ' < "$stub/out")"
  else
    for id in $cancelled; do
      case "$id" in
        1 | 2 | 3) ;;
        4 | 5) fail "the step cancels push run ${id}; it would cancel main's ci" ;;
        6) fail "the step cancels run ${id} of another head commit; it would cancel another pull request's ci" ;;
        7) fail "the step cancels completed run ${id}, which cannot be cancelled" ;;
        8) fail "the step cancels run ${id}, started after the pull request closed; a reopened pull request's ci would be cancelled" ;;
        9) fail "the step cancels run ${id} of another workflow" ;;
        *) fail "the step cancelled '${id}', not a run id" ;;
      esac
    done
    for id in 1 2 3; do
      if ! grep -qw "$id" <<< "$cancelled"; then
        fail "the step leaves the closed pull request's unfinished run ${id} running (cancelled: '${cancelled}')"
      fi
    done
  fi

  scenario open
  if [ "$step_rc" -ne 0 ]; then
    fail "the cancel step fails when the pull request is open again: $(tr '\n' ' ' < "$stub/out")"
  elif [ -n "$cancelled" ]; then
    fail "the step cancels runs ${cancelled} of a pull request that is open again"
  fi
fi

[ "$rc" -eq 0 ] && echo "Closed-PR ci cancel OK: ${WF}"
exit "$rc"
