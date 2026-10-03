#!/usr/bin/env bash
# Dependency-outage scenario on the local compose stack only: triggers a run,
# cuts SERVICE (postgres or neo4j) off the network START_AFTER_S seconds later
# for OUTAGE_S seconds, waits for the run, and records its final status and how
# many messages consumers dropped. The drop count reads the service logs that
# start_local_services.sh writes to /tmp/<service>.log in each container.
#   outage.sh SERVICE PAYLOAD SCHEDULE OUT_DIR [START_AFTER_S=30] [OUTAGE_S=600]
# Env: BENCH_RUN_TIMEOUT_S (2700), after which the run is cancelled.
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "compose" ] || { echo "outage.sh: local compose stack only" >&2; exit 2; }
service="${1:?}"; payload="${2:?}"; schedule="${3:?}"; out="${4:?}"
start_after="${5:-30}"; outage_s="${6:-600}"
bench_setup_redis
target="$(bench_container "${service}")"
consumers=(state orchestrator execution-controller)
for c in "${consumers[@]}"; do
  docker exec "${c}" test -f "/tmp/${c}.log" \
    || { echo "outage.sh: no /tmp/${c}.log in ${c}; start the services with start_local_services.sh" >&2; exit 2; }
done
mkdir -p "${out}"

# The outage disconnects the datastore container from every network it is on and
# reconnects it with the same aliases: its clients see an unreachable host, and
# its data survives (the stack keeps Postgres data on tmpfs, which stopping the
# container would wipe). Any exit during the outage reconnects it.
links="$(docker inspect "${target}" | python3 "${here}/docker_json.py" network-aliases)"
disconnected=0
reconnect() {
  local net alias_list alias args
  [ "${disconnected}" = "1" ] || return 0
  while IFS=$'\t' read -r net alias_list; do
    args=()
    # shellcheck disable=SC2086 # alias_list is a tab-separated list to split
    for alias in ${alias_list}; do
      args+=(--alias "${alias}")
    done
    docker network connect ${args[@]+"${args[@]}"} "${net}" "${target}" >/dev/null
  done <<< "${links}"
  disconnected=0
}
trap reconnect EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP
bench_kubectl delete jobs -l "schedule=${schedule}" --ignore-not-found --wait=true >/dev/null
python3 "${here}/redis_streams.py" snapshot > "${out}/streams-before-rep1.json"
# Log length per consumer before the trigger: only lines written after it count.
offsets=()
for c in "${consumers[@]}"; do
  offsets+=("$(docker exec "${c}" sh -c "wc -l < /tmp/${c}.log" | tr -d ' ')")
done
trig="$(bench_cli_run schedule trigger "${schedule}")"
run_id="$(printf '%s' "${trig}" | python3 "${here}/cli_json.py" field schedule_id)"
trigger_ts="$(printf '%s' "${trig}" | python3 "${here}/cli_json.py" field triggered_at)"
sleep "${start_after}"
disconnected=1
while IFS=$'\t' read -r net _; do
  docker network disconnect "${net}" "${target}" >/dev/null
done <<< "${links}"
# A background sleep and wait, so a signal ends the outage at once.
sleep "${outage_s}" &
wait $!
reconnect
status="$(bench_finish_run "${schedule}" "${run_id}" "${BENCH_RUN_TIMEOUT_S:-2700}" "benchmark outage cleanup")"
done_ts="$(date -u +%FT%TZ)"
# Every consumer, Go (pkg/redis) and Python (topology-controller), logs one
# "Message dead-lettered — ACKing to drop from PEL" line per dead letter,
# whether the message was permanently failing or past its delivery limit.
dropped=0
i=0
for c in "${consumers[@]}"; do
  n="$(docker exec "${c}" sh -c "tail -n +$(( offsets[i] + 1 )) /tmp/${c}.log" \
    | grep -cE 'ACK-dropping|ACKing to drop from PEL' || true)"
  dropped=$(( dropped + n ))
  i=$(( i + 1 ))
done
python3 "${here}/redis_streams.py" snapshot > "${out}/streams-after-rep1.json"
bench_kubectl get jobs -l "schedule=${schedule}" -o json | python3 "${here}/k8s_json.py" slim-jobs > "${out}/jobs-rep1.json"
python3 "${here}/collect.py" --scenario "outage-${service}" --rep 1 --operation run --run-id "${run_id}" \
  --payload "${payload}" --jobs "${out}/jobs-rep1.json" --trigger-ts "${trigger_ts}" --done-ts "${done_ts}" \
  --streams-before "${out}/streams-before-rep1.json" --streams-after "${out}/streams-after-rep1.json" \
  --extra "final_status=${status}" --extra "messages_dropped=${dropped}" \
  --extra "outage_service=${service}" --extra "outage_s=${outage_s}" --out "${out}/rep1.json"
