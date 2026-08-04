#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${HERE}/common.sh"
fail=0; assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }
for fn in log_info log_warn log_error wait_for_tcp_port wait_for_http_host \
          wait_for_container_running check_container_health start_go_service \
          kind_load_pulled_image; do
  assert "$fn is a function" "declare -F $fn >/dev/null"
done
wait_for_tcp_port "no-such-xyz" 50051 1 0; assert "tcp fails fast for missing container" "[ $? -ne 0 ]"
wait_for_http_host "http://127.0.0.1:1/nope" 1 0; assert "http fails fast for closed port" "[ $? -ne 0 ]"

# kind_load_pulled_image: platform derivation, loud failure, and archive
# cleanup — stub docker/kind so this needs neither installed nor a real
# cluster. $DOCKER_SAVE_RC / $KIND_LOAD_RC drive the stubs' exit codes.
tmp_archives(){ ls "${TMPDIR:-/tmp}"/continuo-validation-image.* 2>/dev/null; }
docker(){ if [ "$1" = "save" ]; then [ "$DOCKER_SAVE_RC" = "0" ] && touch "$6"; return "$DOCKER_SAVE_RC"; fi; }
kind(){ if [ "$1" = "load" ]; then return "$KIND_LOAD_RC"; fi; }

uname(){ echo "sparc64"; }
out="$(kind_load_pulled_image "ghcr.io/x/y:v1" "c" 2>&1)"; rc=$?
assert "unsupported host arch fails" "[ $rc -ne 0 ]"
assert "unsupported host arch names the arch" "[[ \"\$out\" == *sparc64* ]]"
unset -f uname

uname(){ echo "arm64"; }

DOCKER_SAVE_RC=1 KIND_LOAD_RC=0
out="$(kind_load_pulled_image "ghcr.io/x/y:v1" "c" 2>&1)"; rc=$?
assert "docker save failure fails loudly" "[ $rc -ne 0 ]"
assert "docker save failure names docker save --platform" "[[ \"\$out\" == *'docker save --platform'* ]]"
assert "docker save failure leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

DOCKER_SAVE_RC=0 KIND_LOAD_RC=1
out="$(kind_load_pulled_image "ghcr.io/x/y:v1" "c" 2>&1)"; rc=$?
assert "kind load failure fails loudly" "[ $rc -ne 0 ]"
assert "kind load failure names kind load image-archive" "[[ \"\$out\" == *'kind load image-archive'* ]]"
assert "kind load failure leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

DOCKER_SAVE_RC=0 KIND_LOAD_RC=0
out="$(kind_load_pulled_image "ghcr.io/x/y:v1" "c" 2>&1)"; rc=$?
assert "success path succeeds" "[ $rc -eq 0 ]"
assert "success path leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"
unset -f uname docker kind

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
