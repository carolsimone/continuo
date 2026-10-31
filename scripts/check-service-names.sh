#!/usr/bin/env bash
# Retired service names. The services below no longer exist; their names may
# appear only in immutable history: Flyway migrations, the chart changelog, the
# product release notes, superpowers docs, and the NOTES.txt upgrade-warning
# block (which intentionally names old->new for operators).
#
# Three lists are checked:
#   - the August names (ui-service, agent-runner, remediation-agent,
#     manifest-controller), matched case-insensitively. The match is
#     deliberately hyphen-only: it does not catch camelCase (uiService) or
#     snake_case (manifest_controller) spellings, which were verified absent
#     when the rename landed.
#   - the retired `orchestrator` service name (now `catalog`), matched
#     case-insensitively as a substring, so it also catches every compound
#     form: ORCHESTRATOR_GRPC_ADDR, flyway-orchestrator, continuo_orchestrator,
#     OrchestratorQuery, orchestrator/. The words "orchestration" and
#     "orchestrates" are ordinary prose and do not match.
#   - the retired `state` and `executor` service names, matched
#     case-sensitively and only in forms that cannot be ordinary words (the
#     word `state` is everywhere: task state, React state, OAuth state). Each
#     alternative is one form of the name: the database, the outbox table, the
#     environment variables and values keys, the DNS address, the module path,
#     the image, the compose service, the label, the identifiers of the Go
#     tests and the CLI, the consumer groups and their contract entries, and
#     the retired agent-remediation outbox aggregate.
#
# Usage: check-service-names.sh [repo-root]   (default: this repository)
set -euo pipefail
cd "${1:-$(dirname "$0")/..}"

# History, and the files that spell the names on purpose.
excludes=(
  ':!db/migration' ':!deploy/continuo/CHANGELOG.md' ':!docs/release-notes.md'
  ':!docs/superpowers'
  ':!deploy/continuo/templates/NOTES.txt'
  ':!scripts/check-service-names.sh' ':!scripts/check-service-names_test.sh'
  ':!pkg/deploycheck/compose_no_proxy_test.go'
)

status=0

if git grep -inE "ui-service|agent-runner|remediation-agent|manifest-controller" -- \
    "${excludes[@]}"; then
  echo "ERROR: retired service name found (see matches above)" >&2
  status=1
fi

if git grep -inE "orchestrator" -- "${excludes[@]}"; then
  echo "ERROR: retired orchestrator name found (see matches above); the service is catalog" >&2
  status=1
fi

retired_state='continuo_state|state_outbox|STATE_GRPC_ADDR|CONTINUO_STATE_ADDR|STATE_HOST|stateGrpcAddr|state:50051|continuo/state|continuo-state|flyway-state|area/state|stateDB|stateDSN|StateEndpoint|EnvStateAddr|assert-state-recreate'
retired_state_groups='state-(task-status-updated|release-promoted)'
retired_executor_groups='(group|const): Executor|executor-(task-dispatch|run-cancelled|validation-requested|validation-result-teardown|seed-build-requested|compile-requested|release-promoted|release-rejected|pipeline-run-finished)'
retired_other='k8s-controllers|remediation_agent'

if git grep -nE "${retired_state}|${retired_state_groups}|${retired_executor_groups}|${retired_other}" -- \
    "${excludes[@]}"; then
  echo "ERROR: retired state, executor or k8s-controller name found (see matches above)" >&2
  status=1
fi

exit "$status"
