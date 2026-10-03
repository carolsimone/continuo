#!/usr/bin/env bash
# Publishes a release-promotion payload and, when SCHEDULE is given, waits until
# the schedule catalog lists it. On a shared install the payload must be a
# topology_io.py union that contains every live node (see README).
#   inject.sh PAYLOAD [SCHEDULE]
set -euo pipefail
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/bench/lib.sh
. "${here}/lib.sh"
payload="${1:?usage: inject.sh PAYLOAD [SCHEDULE]}"
schedule="${2:-}"
bench_setup_redis
stream="$(python3 "${here}/contract.py" ReleasePromotedV1)"
bench_redis_cli -x XADD "${stream}" '*' payload < "${payload}" >/dev/null
[ -n "${schedule}" ] || exit 0
deadline=$(( $(date +%s) + 180 ))
until bench_cli_run schedule list 2>/dev/null | python3 "${here}/cli_json.py" has-schedule "${schedule}"; do
  if [ "$(date +%s)" -ge "${deadline}" ]; then
    echo "inject.sh: ${schedule} not in the schedule catalog after 180 s" >&2
    exit 1
  fi
  sleep 2
done
echo "inject.sh: ${schedule} ready" >&2
