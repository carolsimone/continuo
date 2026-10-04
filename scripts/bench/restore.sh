#!/usr/bin/env bash
# Re-announces the exported production topology under its original release id,
# then checks the install is back: the schedule list equals the one recorded
# before the benchmark and the live graphs match the export again. When a
# release was promoted during the benchmark, it re-exports and re-announces
# that release instead and checks the graphs only.
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
# A release promoted during the benchmark makes the export stale; the live
# topology to return to is then the new current_prod, whose schedules may differ
# from the ones recorded before the benchmark.
moved=0
release_state=0
bench_check_release "${out}" || release_state=$?
case "${release_state}" in
  0) ;;
  1)
    echo "restore.sh: re-exporting the release promoted during the benchmark" >&2
    "${here}/export_topology.sh" "${out}"
    moved=1
    ;;
  *)
    echo "restore.sh: cannot read current_prod; nothing was published. Run restore.sh again once the install answers." >&2
    exit 1
    ;;
esac
"${here}/inject.sh" "${out}/restore.json" < /dev/null
deadline=$(( $(date +%s) + 300 ))
if [ "${moved}" = "0" ]; then
  until bench_cli_run schedule list 2>/dev/null \
      | python3 "${here}/cli_json.py" same-schedules "${out}/schedules-before.json"; do
    if [ "$(date +%s)" -ge "${deadline}" ]; then
      echo "restore.sh: schedule list still differs from ${out}/schedules-before.json after 300 s" >&2
      exit 1
    fi
    sleep 3
  done
fi
until "${here}/preflight.sh" "${out}" "${out}/graphs-after" "${out}/schedules-after.json"; do
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    echo "restore.sh: the live graphs still differ from ${out}/restore.json after 300 s" >&2
    exit 1
  fi
  sleep 5
done
echo "restore.sh: production topology restored and verified" >&2
