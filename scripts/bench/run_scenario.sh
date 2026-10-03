#!/usr/bin/env bash
# Runs one benchmark scenario REPS times and writes OUT_DIR/rep<N>.json per rep.
#   run_scenario.sh NAME PAYLOAD SCHEDULE OPERATION REPS OUT_DIR [CANCEL_AFTER_S]
# OPERATION is run or test. With CANCEL_AFTER_S the run is cancelled after that
# many seconds, a second run is triggered at once and cancelled after
# BENCH_SECOND_RUN_S, and the result records whether the two runs' Jobs overlapped.
# Env: BENCH_IDLE_S (300, idle window before rep 1), BENCH_SETTLE_S (15),
#      BENCH_RUN_TIMEOUT_S (3600), BENCH_SECOND_RUN_S (90), plus lib.sh's knobs.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
name="${1:?}"; payload="${2:?}"; schedule="${3:?}"; op="${4:?}"; reps="${5:?}"; out="${6:?}"
cancel_after="${7:-}"
case "${op}" in run) sub="trigger" ;; test) sub="test" ;; *) echo "OPERATION must be run or test" >&2; exit 2 ;; esac
idle_s="${BENCH_IDLE_S:-300}"
settle_s="${BENCH_SETTLE_S:-15}"
timeout_s="${BENCH_RUN_TIMEOUT_S:-3600}"
second_run_s="${BENCH_SECOND_RUN_S:-90}"
bench_setup_redis
mkdir -p "${out}"

now() { date -u +%FT%TZ; }
field() { python3 "${here}/cli_json.py" field "$1"; }

start_sampler() {
  if [ "$(bench_target)" = "k8s" ]; then
    # stdout is detached so the command substitution that captures the PID returns at once.
    "${here}/sample.sh" k8s "$1" 5 "${BENCH_K8S_NAMESPACE:-continuo}" --kubeconfig "${BENCH_KUBECONFIG}" >/dev/null 2>&1 &
  else
    "${here}/sample.sh" docker "$1" 5 state orchestrator execution-controller \
      "$(bench_container postgres)" "$(bench_container redis)" "$(bench_container neo4j)" >/dev/null 2>&1 &
  fi
  echo $!
}

# Prints the final status once the schedule's latest run is RUN_ID and is no longer running.
wait_done() {
  local run_id="$1" deadline rid running status
  deadline=$(( $(date +%s) + timeout_s ))
  while true; do
    if read -r rid running status < <(bench_cli_run schedule status "${schedule}" 2>/dev/null \
         | python3 "${here}/cli_json.py" run-state) \
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

for rep in $(seq 1 "${reps}"); do
  bench_wait_outside_window
  bench_kubectl delete jobs -l "schedule=${schedule}" --ignore-not-found --wait=true >/dev/null
  samples="${out}/samples-rep${rep}.tsv"
  : > "${samples}"
  sampler="$(start_sampler "${samples}")"
  idle_start=""
  idle_end=""
  if [ "${rep}" -eq 1 ]; then
    idle_start="$(now)"
    sleep "${idle_s}"
    idle_end="$(now)"
  else
    sleep "${settle_s}"
  fi
  python3 "${here}/redis_streams.py" snapshot > "${out}/streams-before-rep${rep}.json"
  trig="$(bench_cli_run schedule "${sub}" "${schedule}")"
  run_id="$(printf '%s' "${trig}" | field schedule_id)"
  trigger_ts="$(printf '%s' "${trig}" | field triggered_at)"
  extra=(--extra "final_status=pending")
  cancel_args=()
  if [ -n "${cancel_after}" ]; then
    sleep "${cancel_after}"
    cancel_ts="$(bench_cli_run schedule cancel "${schedule}" "benchmark cancel" | field cancelled_at)"
    first_status="$(wait_done "${run_id}")"
    next_id="$(bench_cli_run schedule "${sub}" "${schedule}" | field schedule_id)"
    sleep "${second_run_s}"
    bench_cli_run schedule cancel "${schedule}" "benchmark cancel (second run)" >/dev/null || true
    second_status="$(wait_done "${next_id}")"
    extra=(--extra "final_status=${first_status}" --extra "second_status=${second_status}")
    cancel_args=(--cancel-ts "${cancel_ts}" --next-run-id "${next_id}")
  else
    extra=(--extra "final_status=$(wait_done "${run_id}")")
  fi
  done_ts="$(now)"
  sleep "${settle_s}"
  python3 "${here}/redis_streams.py" snapshot > "${out}/streams-after-rep${rep}.json"
  kill "${sampler}" 2>/dev/null || true
  wait "${sampler}" 2>/dev/null || true
  bench_kubectl get jobs -l "schedule=${schedule}" -o json > "${out}/jobs-rep${rep}.json"
  python3 "${here}/collect.py" --scenario "${name}" --rep "${rep}" --operation "${op}" --run-id "${run_id}" \
    --payload "${payload}" --jobs "${out}/jobs-rep${rep}.json" --trigger-ts "${trigger_ts}" --done-ts "${done_ts}" \
    --samples "${samples}" --idle-start "${idle_start}" --idle-end "${idle_end}" \
    --streams-before "${out}/streams-before-rep${rep}.json" --streams-after "${out}/streams-after-rep${rep}.json" \
    "${extra[@]}" ${cancel_args[@]+"${cancel_args[@]}"} --out "${out}/rep${rep}.json"
done
