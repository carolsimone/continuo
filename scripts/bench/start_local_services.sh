#!/usr/bin/env bash
# Starts the run-lifecycle services of the local compose stack for a benchmark:
# state, orchestrator and execution-controller, each as a fresh `go run` inside
# its container (any running instance is stopped first), logging to
# /tmp/<service>.log in that container, which outage.sh reads. execution-controller
# runs in compose and reaches MinIO through the Docker bridge, an address that
# both its own uploads and the task pods in kind resolve; compose DNS names such
# as `minio` do not resolve inside kind, which would leave every task pod's
# parse-cache fetch waiting on retries.
#   start_local_services.sh
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "compose" ] || { echo "start_local_services.sh: BENCH_TARGET=compose only" >&2; exit 2; }
# shellcheck source=scripts/lib/common.sh
. "$(bench_root)/scripts/lib/common.sh"

# start SERVICE [DOCKER_EXEC_ARGS...]: go run in /app/SERVICE, output to /tmp/SERVICE.log.
start() {
  local svc="$1"
  shift
  log_info "Starting ${svc} (go run, logs in ${svc}:/tmp/${svc}.log)..."
  docker exec -d "$@" "${svc}" bash -c "cd /app/${svc} && exec go run . > /tmp/${svc}.log 2>&1"
  sleep 25
}

bridge="$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')"
[ -n "${bridge}" ] || { echo "start_local_services.sh: no Docker bridge gateway" >&2; exit 1; }

for svc in state orchestrator execution-controller; do
  docker exec "${svc}" pkill -f 'go run \.' || true
  docker exec "${svc}" pkill -f "go-build.*/exe/${svc}\$" || true
done
sleep 3

start state
check_container_health state 8082
start orchestrator
check_container_health orchestrator 8087
log_info "execution-controller reaches S3 at http://${bridge}:9000"
start execution-controller -e "S3_ENDPOINT_URL=http://${bridge}:9000"
check_container_health execution-controller 8084
