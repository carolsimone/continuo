#!/usr/bin/env bash
# Local-stack baseline: starts the run-lifecycle services, builds the benchmark
# images, checks the harness on a 3-node DAG, runs dag-500, the two
# dependency-outage scenarios and the whole-DAG test fan-out of 2,000 nodes
# (test-2000), and writes OUT_DIR/report.md. BENCH_SCENARIOS selects a subset
# by name. The other scale scenarios run on a k8s install
# (run_baseline_dev.sh). Replaces the stack's topology; the e2e suite re-seeds.
#   [BENCH_SCENARIOS=...] run_baseline_kind.sh [OUT_DIR]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "compose" ] || { echo "run_baseline_kind.sh: BENCH_TARGET=compose only" >&2; exit 2; }
root="$(bench_root)"
out="${1:-${root}/.bench/baseline-kind-$(date -u +%Y%m%dT%H%M)}"
p="${out}/payloads"
mkdir -p "${p}"
"${here}/start_local_services.sh"
"${here}/build_image.sh"
python3 "${here}/gen_topology.py" --nodes 3 --levels 3 --fan-in 1 --schedule bench-smoke > "${p}/smoke.json"
python3 "${here}/gen_topology.py" --nodes 500 --levels 10 --fan-in 2 --schedule bench-dag-500 > "${p}/dag-500.json"
python3 "${here}/gen_topology.py" --nodes 2000 --levels 20 --fan-in 2 --schedule bench-dag-2000 > "${p}/dag-2000.json"

if bench_wanted smoke; then
  "${here}/inject.sh" "${p}/smoke.json" bench-smoke
  BENCH_IDLE_S=300 "${here}/run_scenario.sh" smoke "${p}/smoke.json" bench-smoke run 1 "${out}/smoke"
  if ! python3 -c 'import json, sys; r = json.load(open(sys.argv[1])); sys.exit(0 if r["final_status"] == "succeeded" and r["nodes_executed"] == 3 else 1)' "${out}/smoke/rep1.json"; then
    echo "run_baseline_kind.sh: smoke run did not succeed on all 3 nodes; see ${out}/smoke" >&2
    exit 1
  fi
fi
# The outage scenarios run on the dag-500 topology.
if bench_wanted dag-500 || bench_wanted outage-postgres || bench_wanted outage-neo4j; then
  "${here}/inject.sh" "${p}/dag-500.json" bench-dag-500
fi
if bench_wanted dag-500; then
  BENCH_IDLE_S=30 "${here}/run_scenario.sh" dag-500 "${p}/dag-500.json" bench-dag-500 run 3 "${out}/dag-500"
fi
if bench_wanted outage-postgres; then
  "${here}/outage.sh" postgres "${p}/dag-500.json" bench-dag-500 "${out}/outage-postgres"
fi
if bench_wanted outage-neo4j; then
  "${here}/outage.sh" neo4j "${p}/dag-500.json" bench-dag-500 "${out}/outage-neo4j"
fi
if bench_wanted test-2000; then
  "${here}/inject.sh" "${p}/dag-2000.json" bench-dag-2000
  BENCH_IDLE_S=30 "${here}/run_scenario.sh" test-2000 "${p}/dag-2000.json" bench-dag-2000 test 3 "${out}/test-2000"
fi
python3 "${here}/report.py" "${out}" > "${out}/report.md"
echo "run_baseline_kind.sh: report at ${out}/report.md" >&2
