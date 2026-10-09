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
# kubectl exec runs without -i: no CLI command reads stdin, and -i would swallow
# the stdin of a caller's `while read` loop.
bench_cli_run() {
  if [ "$(bench_target)" = "k8s" ]; then
    bench_kubectl exec deploy/agent-chat -- continuo "$@"
  else
    "${CONTINUO_CLI:-$(bench_root)/cli/bin/continuo}" "$@"
  fi
}

# Compose only: REDIS_PASSWORD as the running state container received it, when
# not already set. The stack's .env is compose syntax, not shell, so it is
# never sourced.
bench_load_env() {
  if [ -z "${REDIS_PASSWORD:-}" ]; then
    REDIS_PASSWORD="$(docker exec state printenv REDIS_PASSWORD 2>/dev/null || true)"
  fi
  : "${REDIS_PASSWORD:?REDIS_PASSWORD is not set and the state container does not carry it}"
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

# The command that runs inside the Redis container: it reads the password from
# the first line of stdin, so the password never appears in a command line.
# shellcheck disable=SC2016 # expanded by the shell inside the container
BENCH_REDIS_SH='read -r REDISCLI_AUTH && export REDISCLI_AUTH && exec redis-cli "$@"'

# Exports BENCH_REDIS_CMD, a JSON command prefix that runs redis-cli in the
# target's Redis container, and BENCH_REDIS_PASSWORD, which redis_streams.py
# writes to that command's stdin.
bench_setup_redis() {
  local pw
  if [ "$(bench_target)" = "k8s" ]; then
    pw="$(bench_env_value state REDIS_PASSWORD)"
    BENCH_REDIS_CMD="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' \
      kubectl --kubeconfig "${BENCH_KUBECONFIG}" -n "${BENCH_K8S_NAMESPACE:-continuo}" exec -i \
      "${BENCH_REDIS_POD:?BENCH_REDIS_POD is required on k8s (discover.sh lists candidates)}" -c "${BENCH_REDIS_CONTAINER_NAME:-redis}" -- \
      sh -c "${BENCH_REDIS_SH}" redis-cli)"
  else
    bench_load_env
    pw="${REDIS_PASSWORD}"
    BENCH_REDIS_CMD="$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' \
      docker exec -i "$(bench_container redis)" sh -c "${BENCH_REDIS_SH}" redis-cli)"
  fi
  BENCH_REDIS_PASSWORD="${pw}"
  export BENCH_REDIS_CMD BENCH_REDIS_PASSWORD
}

# redis-cli through BENCH_REDIS_CMD. Its stdin follows the password (for `-x`),
# so callers redirect stdin, from /dev/null when they have no input.
bench_redis_cli() {
  python3 "$(bench_here)/redis_streams.py" cli "$@"
}

# k8s only: runs SQL with psql in POD as USER on DB and prints the unaligned
# result. The password goes to the pod on stdin, never into a command line.
#   bench_psql POD USER DB PASSWORD SQL
bench_psql() {
  local pod="$1" user="$2" db="$3" pw="$4" sql="$5"
  # shellcheck disable=SC2016 # expanded by the shell inside the pod
  printf '%s\n' "${pw}" | bench_kubectl exec -i "${pod}" -- \
    sh -c 'read -r PGPASSWORD && export PGPASSWORD && exec psql -X -q -h 127.0.0.1 -U "$1" -d "$2" -At -c "$3"' \
    psql "${user}" "${db}" "${sql}"
}

# release-controller's announce-topology command with ARGS: inside
# deploy/release-controller on k8s (the production image carries it on PATH),
# inside the release-controller container on compose (the dev image builds it
# next to the service binary). It reads the caller's stdin, so callers redirect
# it, from /dev/null when they have no input; its JSON result goes to stdout and
# its exit code is the command's own (2: usage or a release id already taken,
# 3: no current_prod).
bench_announce() {
  if [ "$(bench_target)" = "k8s" ]; then
    bench_kubectl exec -i deploy/release-controller -- announce-topology "$@"
  else
    docker exec -i "$(bench_container release-controller)" /app/release-controller/bin/announce-topology "$@"
  fi
}

# Prints the final status once SCHEDULE's latest run is RUN_ID and is no
# longer running, or "timeout" after LIMIT_S seconds. Polls every second.
bench_wait_run() {
  local schedule="$1" run_id="$2" limit_s="$3" deadline rid running status
  deadline=$(( $(date +%s) + limit_s ))
  while true; do
    if read -r rid running status < <(bench_cli_run schedule status "${schedule}" 2>/dev/null \
         | python3 "$(bench_here)/cli_json.py" run-state) \
       && [ "${rid}" = "${run_id}" ] && [ "${running}" = "false" ]; then
      echo "${status}"
      return 0
    fi
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "timeout"
      return 0
    fi
    sleep 1
  done
}

# Prints the run's final status, or "timeout". A run still live after LIMIT_S
# seconds is cancelled with REASON and waited for (up to 300 s), so nothing that
# follows starts beside it or deletes the Jobs of a live run.
bench_finish_run() {
  local schedule="$1" run_id="$2" limit_s="$3" reason="$4" status
  status="$(bench_wait_run "${schedule}" "${run_id}" "${limit_s}")"
  if [ "${status}" = "timeout" ]; then
    bench_cli_run schedule cancel "${schedule}" "${reason}" >/dev/null || true
    bench_wait_run "${schedule}" "${run_id}" 300 >/dev/null
  fi
  echo "${status}"
}

# Stops a scenario that is still running: ends its runner CHILD (a process this
# shell started; empty when none), then cancels SCHEDULE's run if it is live and
# waits up to 300 s for it to stop, so nothing the benchmark started keeps running.
bench_stop_scenario() {
  local child="$1" schedule="$2" run_id running
  if [ -n "${child}" ]; then
    kill "${child}" 2>/dev/null || true
    wait "${child}" 2>/dev/null || true
  fi
  [ -n "${schedule}" ] || return 0
  if read -r run_id running _ < <(bench_cli_run schedule status "${schedule}" 2>/dev/null \
       | python3 "$(bench_here)/cli_json.py" run-state) && [ "${running}" = "true" ]; then
    bench_cli_run schedule cancel "${schedule}" "benchmark interrupted" >/dev/null || true
    bench_wait_run "${schedule}" "${run_id}" 300 >/dev/null
  fi
}

# k8s only: release-controller's current production release id.
bench_current_release() {
  bench_psql "${BENCH_PG_POD:?BENCH_PG_POD is required (discover.sh lists candidates)}" \
    "$(bench_env_value release-controller POSTGRES_USER)" "$(bench_env_value release-controller POSTGRES_DB)" \
    "$(bench_env_value release-controller POSTGRES_PASSWORD)" "SELECT release_id FROM current_prod WHERE id = 1"
}

# k8s only: returns 0 when current_prod still names the release exported to
# OUT_DIR/restore.json; 1 when a release was promoted since, which makes the
# export stale (re-announcing it would revert the live topology); and 2 when
# current_prod cannot be read, so nothing can be decided.
bench_check_release() {
  local out="$1" exported live
  exported="$(python3 "$(bench_here)/cli_json.py" field release_id < "${out}/restore.json")"
  if ! live="$(bench_current_release)" || [ -z "${live}" ]; then
    echo "bench: cannot read current_prod; the install may be unreachable" >&2
    return 2
  fi
  if [ "${live}" != "${exported}" ]; then
    echo "bench: current_prod is ${live}, but ${out}/restore.json holds ${exported}" >&2
    return 1
  fi
}

# True when scenario NAME is selected: BENCH_SCENARIOS lists the scenarios to
# run, comma- or space-separated; empty or unset selects every scenario.
bench_wanted() {
  [ -z "${BENCH_SCENARIOS:-}" ] && return 0
  case " $(printf '%s' "${BENCH_SCENARIOS}" | tr ',' ' ') " in
    *" $1 "*) return 0 ;;
    *) return 1 ;;
  esac
}

# True when HHMM (UTC) falls inside one of the quiet windows: the install's own
# scheduled runs. BENCH_QUIET_WINDOWS_UTC lists them as comma-separated
# HHMM-HHMM ranges, end exclusive; a range may cross midnight. The default,
# 2215-2345, surrounds a 23:00 UTC daily run.
bench_in_quiet_window() {
  local now="$1" window start end
  for window in $(printf '%s' "${BENCH_QUIET_WINDOWS_UTC:-2215-2345}" | tr ',' ' '); do
    start="${window%-*}"
    end="${window#*-}"
    if [ "${start}" -le "${end}" ]; then
      if [ "${now}" -ge "${start}" ] && [ "${now}" -lt "${end}" ]; then
        return 0
      fi
    elif [ "${now}" -ge "${start}" ] || [ "${now}" -lt "${end}" ]; then
      return 0
    fi
  done
  return 1
}

# k8s only: waits while the UTC clock is inside a quiet window, so no rep
# starts during the install's own scheduled runs.
bench_wait_outside_window() {
  [ "$(bench_target)" = "k8s" ] || return 0
  while bench_in_quiet_window "$(date -u +%H%M)"; do
    echo "bench: inside a quiet window (${BENCH_QUIET_WINDOWS_UTC:-2215-2345} UTC); waiting" >&2
    sleep 300
  done
}
