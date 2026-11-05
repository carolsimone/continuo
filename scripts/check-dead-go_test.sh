#!/usr/bin/env bash
# Test for check-dead-go.sh. A stub go on PATH reports the Go version the case
# chooses and "installs" a stub deadcode, so nothing is built or downloaded. A
# stub deadcode records that it ran, and the one left in GOPATH/bin fails the way
# a binary built by an older Go does.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/check-dead-go.sh"; ROOT="$(cd "${HERE}/.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; sed 's/^/    /' "$tmp/err"; fi; }

mkdir -p "$tmp/bin" "$tmp/gopath/bin"
cat > "$tmp/bin/go" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  env)
    case "$2" in
      GOVERSION) echo "$STUB_GOVERSION" ;;
      GOPATH) echo "$STUB_GOPATH" ;;
      *) echo "unexpected go env $2" >&2; exit 2 ;;
    esac ;;
  list)
    case "$2" in
      -m) echo "${STUB_ROOT}/pkg" ;;
      *) echo "${STUB_ROOT}/pkg/cmd/main" ;;
    esac ;;
  install)
    printf 'install %s GOBIN=%s\n' "$2" "$GOBIN" >> "$STUB_LOG"
    mkdir -p "$GOBIN"
    cat > "$GOBIN/deadcode" <<EOF
#!/usr/bin/env bash
printf 'ran %s\n' "$GOBIN/deadcode" >> "$STUB_LOG"
EOF
    chmod +x "$GOBIN/deadcode" ;;
  *) echo "unexpected go call: $*" >&2; exit 2 ;;
esac
STUB
chmod +x "$tmp/bin/go"

stale="$tmp/gopath/bin/deadcode"
cat > "$stale" <<'STUB'
#!/usr/bin/env bash
printf 'ran stale\n' >> "$STUB_LOG"
echo "file requires newer Go version go1.27 (application built with go1.26)" >&2
echo "deadcode: packages contain errors" >&2
exit 1
STUB
chmod +x "$stale"

# check VERSION: runs the script under that Go version; exit code in $rc and what
# it installed and ran in $tmp/log.
check(){
  : > "$tmp/log"
  PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/log" STUB_GOPATH="$tmp/gopath" STUB_ROOT="$ROOT" STUB_GOVERSION="$1" \
    bash "$S" >"$tmp/out" 2>"$tmp/err"
  rc=$?
}

check go1.27.2
tool="$tmp/gopath/pkg/continuo-tools"
assert "a stale deadcode in GOPATH/bin does not fail the check" "[ $rc -eq 0 ]"
assert "the stale deadcode is never run" "! grep -q 'ran stale' \"$tmp/log\""
assert "it installs into a directory named for the Go version" \
  "grep -q \"^install golang.org/x/tools/cmd/deadcode@v[0-9.]* GOBIN=${tool}/deadcode-v[0-9.]*-go1.27.2\$\" \"$tmp/log\""
assert "it runs what it installed" "grep -q \"^ran ${tool}/deadcode-v[0-9.]*-go1.27.2/deadcode\$\" \"$tmp/log\""

check go1.27.2
assert "the same Go version reuses the install" "[ $rc -eq 0 ] && ! grep -q '^install' \"$tmp/log\" && grep -q '^ran .*go1.27.2/deadcode\$' \"$tmp/log\""

check go1.27.3
assert "another Go version installs its own" "[ $rc -eq 0 ] && grep -q '^install .*-go1.27.3\$' \"$tmp/log\" && grep -q '^ran .*go1.27.3/deadcode\$' \"$tmp/log\""

own="$tmp/own-deadcode"
cat > "$own" <<'STUB'
#!/usr/bin/env bash
printf 'ran own\n' >> "$STUB_LOG"
STUB
chmod +x "$own"
export DEADCODE="$own"
check go1.28.0
unset DEADCODE
assert "DEADCODE is run as it is" "[ $rc -eq 0 ] && grep -q '^ran own\$' \"$tmp/log\""
assert "DEADCODE installs nothing" "! grep -q '^install' \"$tmp/log\""

export DEADCODE="$tmp/no-such-deadcode"
check go1.28.0
unset DEADCODE
assert "a DEADCODE that does not exist fails and names it" "[ $rc -ne 0 ] && grep -q 'no-such-deadcode is not an executable' \"$tmp/err\""

export DEADCODE="$stale"
check go1.28.0
unset DEADCODE
assert "a deadcode that fails fails the check" "[ $rc -ne 0 ] && grep -q 'ran stale' \"$tmp/log\""

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
