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

# kind_load_pulled_image: platform derivation, loud failure, archive
# cleanup, and the platform-scoped-with-fallback behaviour — stub
# docker/kind so this needs neither installed nor a real cluster.
#
# $DOCKER_SAVE_RC / $DOCKER_SAVE_EMPTY drive the `docker save` stub;
# $KIND_LOAD_ARCHIVE_RC / $KIND_LOAD_DOCKERIMAGE_RC drive the two `kind
# load` subcommands independently, so tests can force save to "succeed but
# make an empty archive" or "succeed but the archive load still fails" and
# assert the fallback kicks in for both. $DOCKER_SAVE_ARGS and
# $KIND_LOAD_DOCKERIMAGE_CALLED capture what was actually invoked.
tmp_archives(){ ls "${TMPDIR:-/tmp}"/continuo-pulled-image.* 2>/dev/null; }
# Runs a command with its output captured into $out, without the
# command-substitution subshell `out="$(cmd)"` would create — a subshell
# would run the docker/kind stubs below in a forked shell, so their global
# variable mutations (e.g. $KIND_LOAD_DOCKERIMAGE_CALLED) would vanish the
# moment it exits, and every "was the fallback actually invoked" assertion
# would silently see stale state instead of a real failure.
run_capture(){
  local f; f="$(mktemp)"
  "$@" >"$f" 2>&1
  rc=$?
  out="$(cat "$f")"
  rm -f "$f"
  return "$rc"
}
reset_stub_state(){
  DOCKER_SAVE_RC=0 DOCKER_SAVE_EMPTY=0 DOCKER_SAVE_ARGS=""
  KIND_LOAD_ARCHIVE_RC=0 KIND_LOAD_DOCKERIMAGE_RC=0 KIND_LOAD_DOCKERIMAGE_CALLED=0
}
docker(){
  if [ "$1" = "save" ]; then
    DOCKER_SAVE_ARGS="$*"
    # $6 is the archive path (save --platform <p> <ref> -o <archive>).
    [ "$DOCKER_SAVE_RC" = "0" ] && [ "$DOCKER_SAVE_EMPTY" != "1" ] && printf x > "$6"
    return "$DOCKER_SAVE_RC"
  fi
}
kind(){
  if [ "$1" = "load" ] && [ "$2" = "image-archive" ]; then return "$KIND_LOAD_ARCHIVE_RC"; fi
  if [ "$1" = "load" ] && [ "$2" = "docker-image" ]; then
    KIND_LOAD_DOCKERIMAGE_CALLED=1
    return "$KIND_LOAD_DOCKERIMAGE_RC"
  fi
}

uname(){ echo "sparc64"; }
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "unsupported host arch fails" "[ $rc -ne 0 ]"
assert "unsupported host arch names the arch" "[[ \"\$out\" == *sparc64* ]]"
unset -f uname

# --- arch -> platform mapping (M3): capture docker save's actual args -----
for pair in "x86_64:linux/amd64" "amd64:linux/amd64" "aarch64:linux/arm64" "arm64:linux/arm64"; do
  arch="${pair%%:*}"; want="${pair##*:}"
  reset_stub_state
  uname(){ echo "$arch"; }
  kind_load_pulled_image "ghcr.io/x/y:v1" "c" >/dev/null 2>&1
  assert "${arch} maps to ${want}" "[[ \"\$DOCKER_SAVE_ARGS\" == *'--platform ${want}'* ]]"
  unset -f uname
done

uname(){ echo "arm64"; }

# --- platform-scoped route succeeds outright: no fallback needed ----------
reset_stub_state
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "success path succeeds" "[ $rc -eq 0 ]"
assert "success path leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"
assert "success path never falls back" "[ \"\$KIND_LOAD_DOCKERIMAGE_CALLED\" = 0 ]"

# --- docker save fails: falls back to kind load docker-image, and that succeeds
reset_stub_state; DOCKER_SAVE_RC=1
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "docker save failure falls back instead of failing outright" "[ $rc -eq 0 ]"
assert "docker save failure actually invokes the fallback" "[ \"\$KIND_LOAD_DOCKERIMAGE_CALLED\" = 1 ]"
assert "docker save failure leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

# --- docker save exits 0 but writes nothing (M6): falls back too ----------
reset_stub_state; DOCKER_SAVE_EMPTY=1
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "empty archive falls back instead of failing outright" "[ $rc -eq 0 ]"
assert "empty archive actually invokes the fallback" "[ \"\$KIND_LOAD_DOCKERIMAGE_CALLED\" = 1 ]"
assert "empty archive leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

# --- archive save is fine but the image-archive load fails: falls back too
reset_stub_state; KIND_LOAD_ARCHIVE_RC=1
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "image-archive load failure falls back instead of failing outright" "[ $rc -eq 0 ]"
assert "image-archive load failure actually invokes the fallback" "[ \"\$KIND_LOAD_DOCKERIMAGE_CALLED\" = 1 ]"
assert "image-archive load failure leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

# --- both the platform-scoped route and the fallback fail: loud, names both
# and the requirement/remedy — this is the only path a containerd store on
# Docker <28.0 (the one combination with no working route) ever reaches, so
# it is what stands in for that case here: both attempts genuinely run and
# genuinely fail, and the message is what an operator on that combination
# actually sees.
reset_stub_state; DOCKER_SAVE_RC=1; KIND_LOAD_DOCKERIMAGE_RC=1
run_capture kind_load_pulled_image "ghcr.io/x/y:v1" "c"
assert "total failure (both routes) fails loudly" "[ $rc -ne 0 ]"
assert "total failure names docker save --platform" "[[ \"\$out\" == *'docker save --platform'* ]]"
assert "total failure names kind load docker-image" "[[ \"\$out\" == *'kind load docker-image'* ]]"
assert "total failure names the API 1.48 requirement" "[[ \"\$out\" == *'API 1.48'* ]]"
assert "total failure names the containerd image store" "[[ \"\$out\" == *'containerd'* ]]"
assert "total failure names the classic-graphdriver remedy" "[[ \"\$out\" == *'switch the Docker Engine image store back to the classic graphdriver'* ]]"
assert "total failure leaves no archive behind" "[ -z \"\$(tmp_archives)\" ]"

unset -f uname docker kind

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
