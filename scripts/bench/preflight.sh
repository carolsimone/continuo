#!/usr/bin/env bash
# Refuses to continue unless the exported topology matches the live schedule
# graphs, and records the schedule list that restore.sh must return to.
#   preflight.sh OUT_DIR   (needs OUT_DIR/restore.json)
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
out="${1:?usage: preflight.sh OUT_DIR}"
graphs="${2:-${out}/graphs}"
schedules_file="${3:-${out}/schedules-before.json}"
mkdir -p "${graphs}"
bench_cli_run schedule list > "${schedules_file}"
while IFS= read -r schedule; do
  [ -n "${schedule}" ] || continue
  bench_cli_run schedule graph "${schedule}" > "${graphs}/${schedule}.json"
done < <(python3 "${here}/cli_json.py" schedule-names < "${schedules_file}")
python3 "${here}/topology_io.py" compare --payload "${out}/restore.json" --graphs-dir "${graphs}"
