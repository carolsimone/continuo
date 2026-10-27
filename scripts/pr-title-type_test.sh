#!/usr/bin/env bash
# Runs scripts/pr-title-type.sh against titles it must accept and reject: the
# labels printed for each accepted title, and the failure for everything else.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/pr-title-type.sh"
fail=0
pass(){ echo "ok - $1"; }
miss(){ echo "NOT OK - $1"; fail=1; }

# expect <title> <expected stdout, newline-separated>
expect(){
  local got rc
  got="$(bash "$G" "$1" 2>/dev/null)"; rc=$?
  if [ "$rc" -eq 0 ] && [ "$got" = "$2" ]; then pass "accepts: $1"; else miss "accepts: $1 (exit $rc, printed '$got')"; fi
}
# reject <title>
reject(){
  local out rc
  out="$(bash "$G" "$1" 2>/dev/null)"; rc=$?
  if [ "$rc" -eq 1 ] && [ -z "$out" ]; then pass "rejects: ${1:-<empty>}"; else miss "rejects: ${1:-<empty>} (exit $rc, printed '$out')"; fi
}

expect 'feat: add a node list command' 'enhancement'
expect 'fix(state): stop double-dispatching a retried task' 'bug'
expect 'refactor(execution-controller): settle outcomes in-process' 'refactoring'
expect 'docs: explain the lease wait' 'documentation'
expect 'perf(execution-controller): raise the Kubernetes client rate limit' 'performance'
expect 'ci: cancel a closed pull request'"'"'s run' 'ci/cd'
expect 'test(e2e): cover the cancel path' 'test'
expect 'chore(deps): bump the go-minor-patch group across 1 directory' 'chore'
expect 'build: drop network: host from the image builds' 'chore'
expect 'security(ui): refuse an unsigned session cookie' 'security'
expect 'feat(ui,cli): show attempts' 'enhancement'
expect 'fix(ui/cli): align the run card' 'bug'
expect 'feat(cli)!: rename node trigger to node run' $'enhancement\nbreaking-change'
expect 'refactor!: rename ui-service to ui' $'refactoring\nbreaking-change'
expect 'Revert "feat: add a node list command"' 'chore'

reject ''
reject 'Add a node list command'
reject 'feat add a node list command'
reject 'feat:add a node list command'
reject 'feat: '
reject 'Feat: add a node list command'
reject 'feature: add a node list command'
reject 'wip: add a node list command'
reject 'feat(): add a node list command'
reject 'feat(State): add a node list command'
reject 'ui: draw dependency graphs in execution order'
reject 'TEMPORARY: two-box concurrency test (do not merge)'

# The rejection message names every accepted type, so a contributor can fix the title.
msg="$(bash "$G" 'nope' 2>&1 >/dev/null)"
for t in feat fix refactor docs perf ci test chore build security; do
  if [[ "$msg" == *" $t"* ]]; then pass "rejection message lists '$t'"; else miss "rejection message lists '$t'"; fi
done

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
