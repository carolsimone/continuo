#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-closed-pr-ci-cancel.sh"
REAL="${HERE}/../.github/workflows/cancel-closed-pr-ci.yml"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# variant NAME SED-ARGS...: the real workflow with one edit, checked by the
# guard. The guard's exit code lands in $rc and its stderr in $tmp/NAME.err.
variant(){ local name="$1"; shift; sed "$@" "$REAL" > "$tmp/$name.yml"; bash "$G" "$tmp/$name.yml" >/dev/null 2>"$tmp/$name.err"; rc=$?; }
said(){ grep -q -- "$2" "$tmp/$1.err"; }

bash "$G" "$REAL" >/dev/null 2>&1; assert "the repo workflow passes" "[ $? -eq 0 ]"

variant hosted-pinned -e 's/runs-on: ubuntu-latest/runs-on: ubuntu-24.04/'
assert "a pinned ubuntu image passes" "[ $rc -eq 0 ]"

# The runners it frees must never be the ones it runs on.
variant self-hosted -e 's/runs-on: ubuntu-latest/runs-on: [self-hosted, kind]/'
assert "a self-hosted label list fails" "[ $rc -ne 0 ] && said self-hosted 'not a GitHub-hosted'"
variant self-hosted-bare -e 's/runs-on: ubuntu-latest/runs-on: self-hosted/'
assert "a bare self-hosted runner fails" "[ $rc -ne 0 ]"
variant expression -e "s/runs-on: ubuntu-latest/runs-on: \${{ github.event.pull_request.head.repo.fork \&\& 'ubuntu-latest' || 'self-hosted' }}/"
assert "a runner chosen by an expression fails" "[ $rc -ne 0 ]"

# The trigger is pull_request_target with the closed type: GitHub starts no
# pull_request workflow for a pull request with merge conflicts.
variant pull-request-only -e 's/^  pull_request_target:/  pull_request:/'
assert "a pull_request-only trigger fails" "[ $rc -ne 0 ] && said pull-request-only 'pull_request_target'"
variant pull-request-beside -e 's/^on:$/on:\n  pull_request:\n    types: [closed]/'
assert "a pull_request trigger beside it fails" "[ $rc -ne 0 ] && said pull-request-beside 'a pull_request trigger'"
variant no-closed -e 's/types: \[closed\]/types: [opened, synchronize]/'
assert "a trigger without 'closed' fails" "[ $rc -ne 0 ] && said no-closed \"'closed' type\""
variant list-form -e 's/    types: \[closed\]/    types:\n      - opened\n      - closed/'
assert "the list form of types is read" "[ $rc -eq 0 ]"

variant read-only -e 's/actions: write/actions: read/'
assert "a token without actions: write fails" "[ $rc -ne 0 ] && said read-only 'actions: write'"

# pull_request_target holds the base repository's token: no pull request
# code and no pull-request-controlled text may reach a shell.
variant checkout -e 's/^    steps:$/    steps:\n      - uses: actions\/checkout@v7/'
assert "a checkout step fails" "[ $rc -ne 0 ] && said checkout 'actions/checkout'"
# shellcheck disable=SC2016  # the workflow expression is meant literally
variant interpolated -e 's/--branch "\$HEAD_REF"/--branch "${{ github.event.pull_request.head.ref }}"/'
assert "a pull request expression inside run: fails" "[ $rc -ne 0 ] && said interpolated 'inside a run: block'"
variant forks -e '/head\.repo\.full_name == github\.repository/d'
assert "dropping the fork skip fails" "[ $rc -ne 0 ] && said forks 'fork'"

# The selection must never take main's push runs or another pull request's runs.
variant any-event -e 's/ and \.event == "pull_request"//'
assert "a selection that takes push runs fails" "[ $rc -ne 0 ] && said any-event 'push run'"
variant any-sha -e 's/ and \.headSha == env\.HEAD_SHA//'
assert "a selection that takes other head commits fails" "[ $rc -ne 0 ] && said any-sha 'another pull request'"
variant any-workflow -e 's/\.workflowName == "ci" and //'
assert "a selection that takes other workflows' runs fails" "[ $rc -ne 0 ] && said any-workflow 'another workflow'"
variant finished -e 's/ and \.status != "completed"//'
assert "a selection that takes completed runs fails" "[ $rc -ne 0 ] && said finished 'completed run'"
variant too-narrow -e 's/\.status != "completed"/.status == "in_progress"/'
assert "a selection that leaves queued runs fails" "[ $rc -ne 0 ] && said too-narrow 'leaves the closed pull request'"
variant broken-jq -e 's/select(/select((/'
assert "a step whose selection jq cannot run fails" "[ $rc -ne 0 ] && said broken-jq 'fails on'"
variant unscoped -e 's/--workflow ci\.yml //'
assert "a run list not scoped to ci.yml fails" "[ $rc -ne 0 ] && said unscoped 'ci.yml'"
# shellcheck disable=SC2016  # the workflow text holds these variables unexpanded
variant cancel-other -e 's/gh run cancel "\$id"/gh run cancel "$GITHUB_RUN_ID"/'
assert "a cancel of something other than a selected run fails" "[ $rc -ne 0 ]"

# Close, reopen: only runs the pull request started before the closure that
# triggered the job are taken, and nothing once it is open again.
variant unbound -e 's/ and (\.createdAt | fromdateiso8601) <= (env\.CLOSED_AT | fromdateiso8601)//'
assert "a selection that takes a run started after the closure fails" "[ $rc -ne 0 ] && said unbound 'after the pull request closed'"
variant reversed -e 's/) <= (env\.CLOSED_AT/) > (env.CLOSED_AT/'
assert "a selection that leaves the runs started before the closure fails" "[ $rc -ne 0 ] && said reversed 'leaves the closed pull request'"
# shellcheck disable=SC2016  # the workflow text holds these variables unexpanded
variant no-recheck -e 's/\[ "\$state" != "closed" \]/false/'
assert "a step that cancels after the pull request reopened fails" "[ $rc -ne 0 ] && said no-recheck 'open again'"

bash "$G" "$tmp/does-not-exist.yml" >/dev/null 2>&1; assert "a missing workflow fails" "[ $? -ne 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
