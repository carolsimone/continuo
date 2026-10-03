#!/usr/bin/env bash
# Dependency-outage scenario on the local compose stack only: triggers a run,
# stops SERVICE (postgres or neo4j) START_AFTER_S seconds later for OUTAGE_S
# seconds, waits for the run, and records its final status and how many
# messages consumers dropped. The drop count reads the service logs that
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
docker stop "${target}" >/dev/null
sleep "${outage_s}"
docker start "${target}" >/dev/null
status="$(bench_finish_run "${schedule}" "${run_id}" "${BENCH_RUN_TIMEOUT_S:-2700}" "benchmark outage cleanup")"
done_ts="$(date -u +%FT%TZ)"
# pkg/redis logs one of these two lines for every message a consumer drops:
# a poison message past its delivery limit, or a permanent handler error.
dropped=0
i=0
for c in "${consumers[@]}"; do
  n="$(docker exec "${c}" sh -c "tail -n +$(( offsets[i] + 1 )) /tmp/${c}.log" \
    | grep -cE 'ACK-dropping|ACKing to drop from PEL' || true)"
  dropped=$(( dropped + n ))
  i=$(( i + 1 ))
done
python3 "${here}/redis_streams.py" snapshot > "${out}/streams-after-rep1.json"
bench_kubectl get jobs -l "schedule=${schedule}" -o json > "${out}/jobs-rep1.json"
python3 "${here}/collect.py" --scenario "outage-${service}" --rep 1 --operation run --run-id "${run_id}" \
  --payload "${payload}" --jobs "${out}/jobs-rep1.json" --trigger-ts "${trigger_ts}" --done-ts "${done_ts}" \
  --streams-before "${out}/streams-before-rep1.json" --streams-after "${out}/streams-after-rep1.json" \
  --extra "final_status=${status}" --extra "messages_dropped=${dropped}" \
  --extra "outage_service=${service}" --extra "outage_s=${outage_s}" --out "${out}/rep1.json"
