#!/usr/bin/env bash
# Tests scripts/check-service-names.sh against throwaway fixture repos: every
# retired form must fail the guard, history and ordinary prose must pass, and
# the repository itself must pass.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-service-names.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
n=0
assert(){ if eval "$2"; then echo "PASS - $1"; else echo "FAIL - $1"; fail=1; fi; }

# mkfix DIR PATH CONTENT builds a fixture repo whose single tracked file PATH
# holds CONTENT.
mkfix(){
  mkdir -p "$1/$(dirname "$2")"
  printf '%s\n' "$3" > "$1/$2"
  git -C "$1" init -q
  git -C "$1" add -A
}

# expect_fail NAME SAMPLE: a tracked file holding SAMPLE makes the guard fail.
expect_fail(){
  n=$((n+1)); local d="$tmp/fix$n"
  mkfix "$d" "src/sample.txt" "$2"
  bash "$G" "$d" >/dev/null 2>&1; local rc=$?
  assert "$1 fails the guard" "[ $rc -ne 0 ]"
}

# expect_pass NAME PATH SAMPLE: a tracked file PATH holding SAMPLE passes.
expect_pass(){
  n=$((n+1)); local d="$tmp/fix$n"
  mkfix "$d" "$2" "$3"
  bash "$G" "$d" >/dev/null 2>&1; local rc=$?
  assert "$1 passes the guard" "[ $rc -eq 0 ]"
}

# The retired state service, in the forms that cannot be ordinary words.
expect_fail "database continuo_state"            'POSTGRES_DB: continuo_state'
expect_fail "outbox table state_outbox"          'SELECT * FROM state_outbox'
expect_fail "STATE_GRPC_ADDR"                    'STATE_GRPC_ADDR=state:1'
expect_fail "CONTINUO_STATE_ADDR"                'export CONTINUO_STATE_ADDR=x:1'
expect_fail "STATE_HOST"                         'STATE_HOST=localhost'
expect_fail "values key stateGrpcAddr"           'stateGrpcAddr: x'
expect_fail "address state:50051"                'addr: state:50051'
expect_fail "module path continuo/state"         'import "github.com/carolsimone/continuo/state/domain"'
expect_fail "image continuo-state"               'image: ghcr.io/x/continuo-state:v1'
expect_fail "compose service flyway-state"       'depends_on: [flyway-state]'
expect_fail "label area/state"                   'area/state:'
expect_fail "group state-task-status-updated"    'group: state-task-status-updated'
expect_fail "group state-release-promoted"       'group: state-release-promoted'
expect_fail "identifier stateDB"                 'db := stateDB(t)'
expect_fail "identifier stateDSN"                'dsn := stateDSN'
expect_fail "identifier StateEndpoint"           'cfg.StateEndpoint = x'
expect_fail "identifier EnvStateAddr"            'const EnvStateAddr = "X"'
expect_fail "script assert-state-recreate"       'python3 assert-state-recreate.py'

# The retired executor group prefix, every group of the old set.
expect_fail "contract group: Executor"           '  group: ExecutorTaskDispatch'
expect_fail "constant const: Executor"           '  const: ExecutorRunCancelled'
for g in task-dispatch run-cancelled validation-requested validation-result-teardown \
         seed-build-requested compile-requested release-promoted release-rejected \
         pipeline-run-finished; do
  expect_fail "group executor-$g"                "group: executor-$g"
done

# Other retired names.
expect_fail "directory k8s-controllers"          'bash tests/e2e/deploy-k8s-controllers.sh'
expect_fail "aggregate remediation_agent"        'AggregateType: "remediation_agent"'

# The August names, matched case-insensitively.
expect_fail "ui-service"                         'cd ui-service'
expect_fail "UI-Service (any case)"              'UI-Service is gone'
expect_fail "agent-runner"                       'image: agent-runner'
expect_fail "remediation-agent"                  'name: remediation-agent'
expect_fail "manifest-controller"                'docker exec manifest-controller sh'

# History stays: a migration, the released changelog, the release notes, the
# superpowers docs and the NOTES.txt upgrade warnings may name old services.
expect_pass "a migration naming continuo_state"  'db/migration/run_controller/V1__x.sql' 'ALTER DATABASE continuo_state RENAME TO x;'
expect_pass "a migration naming state_outbox"    'db/migration/run_controller/V2__x.sql' 'ALTER TABLE state_outbox RENAME TO run_controller_outbox;'
expect_pass "the chart changelog"                'deploy/continuo/CHANGELOG.md' 'The service state is now run-controller; group state-task-status-updated.'
expect_pass "the release notes"                  'docs/release-notes.md' 'continuo-state image, STATE_GRPC_ADDR'
expect_pass "the superpowers docs"               'docs/superpowers/plan.md' 'rename continuo_state'
expect_pass "the NOTES.txt upgrade warnings"     'deploy/continuo/templates/NOTES.txt' 'set CONTINUO_STATE_ADDR'

# Ordinary words and identifiers must not trip the guard.
expect_pass "the word state in prose"            'docs/guide.md' 'the run state machine moves a task to the next state'
expect_pass "a Go variable named state"          'pkg/x/x.go' 'state := 1'
expect_pass "React state"                        'ui/src/a.tsx' 'const [state, setState] = useState(0)'
expect_pass "an Executor type"                   'agent-chat/adapters/cliexec/executor.go' 'type Executor struct{}'
expect_pass "the executor word in prose"         'docs/guide.md' 'the executor runs each attempt as one Job'
expect_pass "a run-controller reference"         'deploy/values.yaml' 'addr: run-controller:50051 db: continuo_run_controller'
expect_pass "the retired schedule-catalog group fixture" 'pkg/streams/streams_test.go' '"state-schedule-catalog"'

# The real repo must be clean.
bash "$G" "${HERE}/.." >/dev/null 2>&1; rc=$?
assert "the repository itself" "[ $rc -eq 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
