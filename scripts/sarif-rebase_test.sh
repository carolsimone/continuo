#!/usr/bin/env bash
# Runs scripts/sarif-rebase.py against a govulncheck-shaped SARIF file: every
# location under %SRCROOT% gains the module directory, locations under any other
# base keep their path, and input it cannot rewrite is refused untouched.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; R="${HERE}/sarif-rebase.py"
fail=0
pass(){ echo "ok - $1"; }
miss(){ echo "NOT OK - $1"; fail=1; }

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT

# One result at the module's go.mod, with a call stack through a file of the
# module and a file of the standard library.
fixture(){
  cat > "$1" <<'JSON'
{"version": "2.1.0", "runs": [{"tool": {"driver": {"name": "govulncheck"}}, "results": [{
  "ruleId": "GO-2026-0001",
  "locations": [{"physicalLocation": {"artifactLocation": {"uri": "go.mod", "uriBaseId": "%SRCROOT%"}}}],
  "codeFlows": [{"threadFlows": [{"locations": [
    {"location": {"physicalLocation": {"artifactLocation": {"uri": "internal/x.go", "uriBaseId": "%SRCROOT%"}}}},
    {"location": {"physicalLocation": {"artifactLocation": {"uri": "src/net/http/server.go", "uriBaseId": "%GOROOT%"}}}}
  ]}]}]
}]}]}
JSON
}

# Prints "<uri> <uriBaseId>" for the result location and each call-stack frame.
locations(){
  python3 -I -c '
import json, sys
res = json.load(open(sys.argv[1]))["runs"][0]["results"][0]
locs = [res["locations"][0]] + [f["location"] for f in res["codeFlows"][0]["threadFlows"][0]["locations"]]
for loc in locs:
    a = loc["physicalLocation"]["artifactLocation"]
    print(a["uri"], a["uriBaseId"])
' "$1"
}

f="${tmp}/nested.sarif"; fixture "$f"
python3 "$R" tests/e2e "$f" 2>/dev/null; rc=$?
got="$(locations "$f" 2>&1)"
want=$'tests/e2e/go.mod %SRCROOT%\ntests/e2e/internal/x.go %SRCROOT%\nsrc/net/http/server.go %GOROOT%'
if [ "$rc" -eq 0 ] && [ "$got" = "$want" ]; then pass "rebases every SRCROOT location"; else miss "rebases every SRCROOT location (exit $rc, got '$got')"; fi

f="${tmp}/dotslash.sarif"; fixture "$f"
python3 "$R" ./agent-chat "$f" 2>/dev/null; rc=$?
got="$(locations "$f" 2>&1 | head -1)"
if [ "$rc" -eq 0 ] && [ "$got" = "agent-chat/go.mod %SRCROOT%" ]; then pass "normalises a ./ module path"; else miss "normalises a ./ module path (exit $rc, got '$got')"; fi

f="${tmp}/empty.sarif"; : > "$f"
python3 "$R" agent-chat "$f" 2>/dev/null; rc=$?
if [ "$rc" -eq 1 ] && [ ! -s "$f" ]; then pass "refuses a file that is not JSON"; else miss "refuses a file that is not JSON (exit $rc, size $(wc -c < "$f"))"; fi

err="$(python3 "$R" agent-chat 2>&1 >/dev/null)"; rc=$?
if [ "$rc" -eq 2 ] && [[ "$err" == *"Usage: sarif-rebase.py MODULE_DIR SARIF_FILE"* ]]; then pass "refuses wrong arguments"; else miss "refuses wrong arguments (exit $rc, stderr '$err')"; fi

exit "$fail"
