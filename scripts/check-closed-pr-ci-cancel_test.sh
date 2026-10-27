#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-closed-pr-ci-cancel.sh"
REAL="${HERE}/../.github/workflows/cancel-closed-pr-ci.yml"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# variant NAME SED-SCRIPT: the real workflow with one edit, checked by the guard.
# The guard's exit code lands in $rc and its stderr in $tmp/NAME.err.
variant(){ sed -e "$2" "$REAL" > "$tmp/$1.yml"; bash "$G" "$tmp/$1.yml" >/dev/null 2>"$tmp/$1.err"; rc=$?; }

bash "$G" "$REAL" >/dev/null 2>&1; assert "the repo workflow passes" "[ $? -eq 0 ]"

variant hosted-pinned 's/runs-on: ubuntu-latest/runs-on: ubuntu-24.04/'
assert "a pinned ubuntu image passes" "[ $rc -eq 0 ]"

# The runners it frees must never be the ones it runs on.
variant self-hosted 's/runs-on: ubuntu-latest/runs-on: [self-hosted, kind]/'
assert "a self-hosted label list fails" "[ $rc -ne 0 ] && grep -q 'not a GitHub-hosted' \"$tmp/self-hosted.err\""
variant self-hosted-bare 's/runs-on: ubuntu-latest/runs-on: self-hosted/'
assert "a bare self-hosted runner fails" "[ $rc -ne 0 ]"
variant expression "s/runs-on: ubuntu-latest/runs-on: \${{ github.event.pull_request.head.repo.fork \&\& 'ubuntu-latest' || 'self-hosted' }}/"
assert "a runner chosen by an expression fails" "[ $rc -ne 0 ]"

# The trigger must include a pull request closing, in either YAML form.
variant no-closed 's/types: \[closed\]/types: [opened, synchronize]/'
assert "a trigger without 'closed' fails" "[ $rc -ne 0 ] && grep -q \"'closed' type\" \"$tmp/no-closed.err\""
variant target 's/^  pull_request:/  pull_request_target:/'
assert "a pull_request_target trigger is not the pull_request trigger" "[ $rc -ne 0 ]"
variant list-form 's/    types: \[closed\]/    types:\n      - opened\n      - closed/'
assert "the list form of types is read" "[ $rc -eq 0 ]"

variant read-only 's/actions: write/actions: read/'
assert "a token without actions: write fails" "[ $rc -ne 0 ] && grep -q 'actions: write' \"$tmp/read-only.err\""

# The selection must never take main's push runs or another pull request's runs.
variant any-event 's/\.event == "pull_request" and //'
assert "a selection that takes push runs fails" "[ $rc -ne 0 ] && grep -q \"push run\" \"$tmp/any-event.err\""
variant any-sha 's/ and \.headSha == env\.HEAD_SHA//'
assert "a selection that takes other head commits fails" "[ $rc -ne 0 ] && grep -q 'another pull request' \"$tmp/any-sha.err\""
variant finished 's/ and \.status != "completed"//'
assert "a selection that takes completed runs fails" "[ $rc -ne 0 ] && grep -q 'completed run' \"$tmp/finished.err\""
variant too-narrow 's/\.status != "completed"/.status == "in_progress"/'
assert "a selection that leaves queued runs fails" "[ $rc -ne 0 ] && grep -q 'leaves the closed pull request' \"$tmp/too-narrow.err\""
variant broken-jq 's/select(/select((/'
assert "a selection jq cannot run fails" "[ $rc -ne 0 ] && grep -q 'does not run' \"$tmp/broken-jq.err\""
variant any-workflow 's/--workflow ci\.yml //'
assert "a run list not scoped to ci.yml fails" "[ $rc -ne 0 ] && grep -q 'ci.yml' \"$tmp/any-workflow.err\""
# shellcheck disable=SC2016  # the workflow text holds these variables unexpanded
variant cancel-other 's/gh run cancel "\$id"/gh run cancel "$GITHUB_RUN_ID"/'
assert "a cancel of something other than a selected run fails" "[ $rc -ne 0 ]"

bash "$G" "$tmp/does-not-exist.yml" >/dev/null 2>&1; assert "a missing workflow fails" "[ $? -ne 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
