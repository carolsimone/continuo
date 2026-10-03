#!/usr/bin/env bash
# Re-announces the exported production topology under its original release id,
# then checks the install is back: the schedule list equals the one recorded
# before the benchmark and the live graphs match the export again.
#   restore.sh OUT_DIR
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
out="${1:?usage: restore.sh OUT_DIR}"
if [ ! -f "${out}/restore.json" ] || [ ! -f "${out}/schedules-before.json" ]; then
  echo "restore.sh: ${out} lacks restore.json or schedules-before.json" >&2
  exit 2
fi
"${here}/inject.sh" "${out}/restore.json"
deadline=$(( $(date +%s) + 300 ))
until bench_cli_run schedule list 2>/dev/null \
    | python3 "${here}/cli_json.py" same-schedules "${out}/schedules-before.json"; do
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    echo "restore.sh: schedule list still differs from ${out}/schedules-before.json after 300 s" >&2
    exit 1
  fi
  sleep 3
done
"${here}/preflight.sh" "${out}" "${out}/graphs-after" "${out}/schedules-after.json"
echo "restore.sh: production topology restored and verified" >&2
