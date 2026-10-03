#!/usr/bin/env bash
# Exports the install's production topology (release-controller's current_prod)
# and the payload that re-announces it unchanged.
#   export_topology.sh OUT_DIR   ->  OUT_DIR/current.json, OUT_DIR/restore.json
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "k8s" ] || { echo "export_topology.sh: BENCH_TARGET=k8s only" >&2; exit 2; }
out="${1:?usage: export_topology.sh OUT_DIR}"
mkdir -p "${out}"
user="$(bench_env_value release-controller POSTGRES_USER)"
db="$(bench_env_value release-controller POSTGRES_DB)"
pw="$(bench_env_value release-controller POSTGRES_PASSWORD)"
bench_psql "${BENCH_PG_POD:?BENCH_PG_POD is required (discover.sh lists candidates)}" "${user}" "${db}" "${pw}" \
  "SELECT json_build_object('release_id', release_id, 'topology', topology_snapshot)::text FROM current_prod WHERE id = 1" \
  > "${out}/current.json"
python3 "${here}/topology_io.py" restore-payload < "${out}/current.json" > "${out}/restore.json"
