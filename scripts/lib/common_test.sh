#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/common.sh"
fail=0; assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }
for fn in log_info log_warn log_error wait_for_tcp_port wait_for_http_host \
          wait_for_container_running check_container_health start_go_service; do
  assert "$fn is a function" "declare -F $fn >/dev/null"
done
wait_for_tcp_port "no-such-xyz" 50051 1 0; assert "tcp fails fast for missing container" "[ $? -ne 0 ]"
wait_for_http_host "http://127.0.0.1:1/nope" 1 0; assert "http fails fast for closed port" "[ $? -ne 0 ]"
if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
