#!/usr/bin/env bash
# Shared helpers for the benchmark scripts. Source it; do not execute it.
# BENCH_TARGET selects the environment: compose (local compose stack + kind,
# the default) or k8s (an install reached through BENCH_KUBECONFIG whose
# services run in BENCH_K8S_NAMESPACE). On k8s, BENCH_REDIS_POD and BENCH_PG_POD
# name the install's Redis and Postgres pods (discover.sh lists candidates), and
# build_image.sh imports the images over ssh to BENCH_SSH_HOST, a k3s node.

# Both run in a subshell so the caller's working directory never changes.
bench_here() (
  cd "$(dirname "${BASH_SOURCE[0]}")" && pwd
)
bench_root() (
  cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd
)

bench_target() {
  echo "${BENCH_TARGET:-compose}"
}

# kubectl against the cluster that runs the task Jobs (and, on k8s, the services).
bench_kubectl() {
  if [ "$(bench_target)" = "k8s" ]; then
    kubectl --kubeconfig "${BENCH_KUBECONFIG:?BENCH_KUBECONFIG is required for BENCH_TARGET=k8s}" \
      -n "${BENCH_K8S_NAMESPACE:-continuo}" "$@"
  else
    kubectl --context "${BENCH_KUBE_CONTEXT:-kind-continuo}" -n "${BENCH_K8S_NAMESPACE:-default}" "$@"
  fi
}

# The continuo CLI: the local binary on compose, the copy inside agent-chat on k8s.
bench_cli_run() {
  if [ "$(bench_target)" = "k8s" ]; then
    bench_kubectl exec -i deploy/agent-chat -- continuo "$@"
  else
    "${CONTINUO_CLI:-$(bench_root)/cli/bin/continuo}" "$@"
  fi
}

# Compose only: REDIS_PASSWORD from the repo's .env when not already set.
bench_load_env() {
  local root
  root="$(bench_root)"
  if [ -z "${REDIS_PASSWORD:-}" ] && [ -f "${root}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    . "${root}/.env"
    set +a
  fi
  : "${REDIS_PASSWORD:?REDIS_PASSWORD is not set and not in .env}"
}

# Compose only: project of the running stack, read from the state container's
# label, so the harness works from a worktree whose directory name differs.
bench_project() {
  docker inspect state --format '{{ index .Config.Labels "com.docker.compose.project" }}'
}

# Compose only: container id of a compose service in the running stack.
bench_container() {
  local service="$1" id
  id="$(docker ps -q \
    --filter "label=com.docker.compose.project=$(bench_project)" \
    --filter "label=com.docker.compose.service=${service}")"
  [ -n "${id}" ] || { echo "bench: no running container for service ${service}" >&2; return 1; }
  echo "${id}"
}

# k8s only: value of env VAR as Deployment DEPLOY receives it (literal value,
# secretKeyRef, configMapKeyRef or envFrom). Callers keep it in a variable.
bench_env_value() {
  local deploy="$1" var="$2" kind a b value here
  here="$(bench_here)"
  while IFS=$'\t' read -r kind a b; do
    value=""
    case "${kind}" in
      value) value="${a}" ;;
      secret) value="$(bench_kubectl get secret "${a}" -o json | python3 "${here}/k8s_json.py" secret-key "${b}" || true)" ;;
      configmap) value="$(bench_kubectl get configmap "${a}" -o json | python3 "${here}/k8s_json.py" configmap-key "${b}" || true)" ;;
      envfrom-secret) value="$(bench_kubectl get secret "${a}" -o json | python3 "${here}/k8s_json.py" secret-key "${var}" || true)" ;;
      envfrom-configmap) value="$(bench_kubectl get configmap "${a}" -o json | python3 "${here}/k8s_json.py" configmap-key "${var}" || true)" ;;
    esac
    if [ -n "${value}" ]; then
      printf '%s' "${value}"
      return 0
    fi
  done < <(bench_kubectl get deploy "${deploy}" -o json | python3 "${here}/k8s_json.py" env-source "${var}")
  echo "bench: cannot resolve ${var} for deployment ${deploy}" >&2
  return 1
}

# Exports BENCH_REDIS_CMD: a JSON command prefix that runs an authenticated
# redis-cli against the target's Redis (read by redis_streams.py and bench_redis_cli).
bench_setup_redis() {
  local pw
  if [ "$(bench_target)" = "k8s" ]; then
    pw="$(bench_env_value state REDIS_PASSWORD)"
    BENCH_REDIS_CMD="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' \
      kubectl --kubeconfig "${BENCH_KUBECONFIG}" -n "${BENCH_K8S_NAMESPACE:-continuo}" exec -i \
      "${BENCH_REDIS_POD:?BENCH_REDIS_POD is required on k8s (discover.sh lists candidates)}" -c "${BENCH_REDIS_CONTAINER_NAME:-redis}" -- \
      env "REDISCLI_AUTH=${pw}" redis-cli)"
  else
    bench_load_env
    export REDISCLI_AUTH="${REDIS_PASSWORD}"
    BENCH_REDIS_CMD="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' \
      docker exec -i -e REDISCLI_AUTH "$(bench_container redis)" redis-cli)"
  fi
  export BENCH_REDIS_CMD
}

# redis-cli through BENCH_REDIS_CMD; stdin passes through (for `-x`).
bench_redis_cli() {
  python3 -c 'import json, os, subprocess, sys; sys.exit(subprocess.call(json.loads(os.environ["BENCH_REDIS_CMD"]) + sys.argv[1:]))' "$@"
}

# k8s only: waits while the UTC clock is inside 22:15-23:45, so no rep overlaps
# the install's 23:00 UTC daily run.
bench_wait_outside_window() {
  local now
  [ "$(bench_target)" = "k8s" ] || return 0
  while true; do
    now="$(date -u +%H%M)"
    if [ "${now}" -lt 2215 ] || [ "${now}" -ge 2345 ]; then
      return 0
    fi
    echo "bench: inside the daily-run window (22:15-23:45 UTC); waiting" >&2
    sleep 300
  done
}
