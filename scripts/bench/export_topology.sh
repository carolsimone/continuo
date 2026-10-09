#!/usr/bin/env bash
# Exports the install's production topology: current_prod's release id and the
# nodes of the artifact it points at (announce-topology --print-current), and
# the payload topology_io.py builds from them. The release id is read before and
# after the nodes, so a promotion that lands in between fails the export rather
# than pairing one release's id with another's nodes.
#   export_topology.sh OUT_DIR   ->  OUT_DIR/current.json, OUT_DIR/restore.json
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
[ "$(bench_target)" = "k8s" ] || { echo "export_topology.sh: BENCH_TARGET=k8s only" >&2; exit 2; }
out="${1:?usage: export_topology.sh OUT_DIR}"
mkdir -p "${out}"
release_id="$(bench_current_release)"
[ -n "${release_id}" ] || { echo "export_topology.sh: current_prod names no release" >&2; exit 1; }
bench_announce --print-current < /dev/null > "${out}/current-nodes.json"
if [ "$(bench_current_release)" != "${release_id}" ]; then
  echo "export_topology.sh: current_prod moved during the export; run it again" >&2
  exit 1
fi
python3 "${here}/topology_io.py" current --release-id "${release_id}" < "${out}/current-nodes.json" > "${out}/current.json"
python3 "${here}/topology_io.py" restore-payload < "${out}/current.json" > "${out}/restore.json"
