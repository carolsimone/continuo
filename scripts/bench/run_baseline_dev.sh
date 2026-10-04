#!/usr/bin/env bash
# Baseline on a k8s install. Exports the live production topology, checks it
# against the live graphs, runs the synthetic scenarios on the union of the live
# topology and a bench DAG, and re-announces the live topology on every exit
# after the first injection. BENCH_SCENARIOS selects a subset by name.
# The whole-DAG test fan-out (test-2000) runs on the local stack instead
# (run_baseline_kind.sh): 2,000 near-simultaneous Jobs can saturate a small
# cluster's API server, which the harness must not do to a shared install.
#   BENCH_TARGET=k8s BENCH_KUBECONFIG=... [BENCH_SCENARIOS=...] run_baseline_dev.sh [OUT_DIR]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "k8s" ] || { echo "run_baseline_dev.sh: BENCH_TARGET=k8s only" >&2; exit 2; }
root="$(bench_root)"
out="${1:-${root}/.bench/baseline-dev-$(date -u +%Y%m%dT%H%M)}"
p="${out}/payloads"
mkdir -p "${p}"

"${here}/export_topology.sh" "${out}"
"${here}/preflight.sh" "${out}"

# Every exit after the first injection, including a signal or a failed step,
# stops the running scenario and its live bench run, then re-announces the live
# topology.
injected=0
child=""
current_schedule=""
restore_on_exit() {
  local rc=$?
  trap - INT TERM HUP
  bench_stop_scenario "${child}" "${current_schedule}"
  if [ "${injected}" = "1" ]; then
    if ! "${here}/restore.sh" "${out}"; then
      echo "run_baseline_dev.sh: RESTORE FAILED — run: BENCH_TARGET=k8s BENCH_KUBECONFIG=${BENCH_KUBECONFIG} ${here}/restore.sh ${out}" >&2
      rc=1
    fi
  fi
  exit "${rc}"
}
trap restore_on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM HUP

"${here}/build_image.sh"

# bench NAME SCHEDULE GEN_ARGS... -> payloads/NAME.json, the union of the live topology and the bench DAG
bench() {
  local name="$1" schedule="$2"
  shift 2
  python3 "${here}/gen_topology.py" --schedule "${schedule}" "$@" > "${p}/${name}-bench.json"
  python3 "${here}/topology_io.py" union --base "${out}/restore.json" --bench "${p}/${name}-bench.json" \
    --release-id "bench-${schedule}-$(date +%s)" > "${p}/${name}.json"
}
bench smoke bench-smoke --nodes 3 --levels 3 --fan-in 1
bench chain-500 bench-chain-500 --nodes 500 --levels 50 --fan-in 1
bench dag-500 bench-dag-500 --nodes 500 --levels 10 --fan-in 2
bench dag-2000 bench-dag-2000 --nodes 2000 --levels 20 --fan-in 2
bench cascade-2000 bench-cascade-2000 --nodes 2000 --levels 20 --fan-in 2 --fail-root
bench cancel-500 bench-cancel-500 --nodes 500 --levels 10 --fan-in 2 --image-tag slow30

# scenario NAME FILE SCHEDULE ARGS... : refuses to publish when a release was
# promoted since the export, then publishes the union and runs the scenario as a
# child process that the exit handler can stop.
scenario() {
  local name="$1" file="$2" schedule="$3"
  shift 3
  bench_check_release "${out}"
  injected=1
  current_schedule="${schedule}"
  "${here}/inject.sh" "${p}/${file}" "${schedule}"
  "${here}/run_scenario.sh" "${name}" "${p}/${file}" "${schedule}" "$@" &
  child=$!
  wait "${child}"
  child=""
}

if bench_wanted smoke; then
  BENCH_IDLE_S=900 scenario smoke smoke.json bench-smoke run 1 "${out}/smoke"
  if ! python3 -c 'import json, sys; r = json.load(open(sys.argv[1])); sys.exit(0 if r["final_status"] == "succeeded" and r["nodes_executed"] == 3 else 1)' "${out}/smoke/rep1.json"; then
    echo "run_baseline_dev.sh: smoke run did not succeed on all 3 nodes; see ${out}/smoke" >&2
    exit 1
  fi
fi
export BENCH_IDLE_S=30
if bench_wanted chain-500; then
  scenario chain-500 chain-500.json bench-chain-500 run 3 "${out}/chain-500"
fi
if bench_wanted dag-500; then
  scenario dag-500 dag-500.json bench-dag-500 run 3 "${out}/dag-500"
fi
if bench_wanted dag-2000; then
  scenario dag-2000 dag-2000.json bench-dag-2000 run 3 "${out}/dag-2000"
fi
if bench_wanted cascade-2000; then
  scenario cascade-2000 cascade-2000.json bench-cascade-2000 run 3 "${out}/cascade-2000"
fi
if bench_wanted cancel-500; then
  scenario cancel-500 cancel-500.json bench-cancel-500 run 3 "${out}/cancel-500" 60
fi
python3 "${here}/report.py" "${out}" > "${out}/report.md"
echo "run_baseline_dev.sh: report at ${out}/report.md (restore follows)" >&2
