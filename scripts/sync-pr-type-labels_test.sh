#!/usr/bin/env bash
# Test for sync-pr-type-labels.sh. A stub gh on PATH answers the label read from
# $STUB_LABELS (one label per line) and records every write in $STUB_LOG, so no
# network is touched.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/sync-pr-type-labels.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; echo "    calls: $(tr '\n' ';' < "$tmp/log")"; fail=1; fi; }

mkdir -p "$tmp/bin"
cat > "$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
# gh api [-X METHOD] PATH [-f field]
method=GET; [ "$2" = "-X" ] && method="$3"
case "$method" in
  GET) printf '%s' "${STUB_LABELS:-}" ;;
  *) printf '%s\n' "gh $*" >> "$STUB_LOG" ;;
esac
STUB
chmod +x "$tmp/bin/gh"

# run TITLE LABELS...: syncs a pull request carrying LABELS; calls land in $tmp/log.
run(){
  local title="$1"; shift
  : > "$tmp/log"
  STUB_LABELS="$(printf '%s\n' "$@")" STUB_LOG="$tmp/log" PATH="$tmp/bin:$PATH" \
    PR_TITLE="$title" GH_REPO=o/r bash "$S" 7 >/dev/null 2>"$tmp/err"
  rc=$?
}
writes(){ grep -c . "$tmp/log" || true; }

run 'fix(run-controller): stop a double dispatch'
assert "an unlabelled fix gains bug" "[ $rc -eq 0 ] && [ \"\$(writes)\" = 1 ] && grep -q 'POST repos/o/r/issues/7/labels -f labels\[\]=bug' $tmp/log"

run 'fix(run-controller): stop a double dispatch' bug area/run-controller
assert "a correct label is not touched, and area/* is ignored" "[ $rc -eq 0 ] && [ \"\$(writes)\" = 0 ]"

run 'feat(run-controller): add attempts' bug area/run-controller
assert "a title edited from fix to feat swaps bug for enhancement" "[ \$(writes) = 2 ] && grep -q 'DELETE repos/o/r/issues/7/labels/bug' $tmp/log && grep -q 'labels\[\]=enhancement' $tmp/log"

run 'feat(cli)!: rename node trigger' enhancement
assert "a ! title adds breaking-change" "[ \$(writes) = 1 ] && grep -q 'labels\[\]=breaking-change' $tmp/log"

run 'feat(cli): rename node trigger' enhancement breaking-change
assert "dropping the ! removes breaking-change" "[ \$(writes) = 1 ] && grep -q 'DELETE repos/o/r/issues/7/labels/breaking-change' $tmp/log"

run 'chore: tidy' ci/cd
assert "the slash of ci/cd is escaped in the delete path" "grep -q 'DELETE repos/o/r/issues/7/labels/ci%2Fcd' $tmp/log"

run 'ci: pin the runner' chore p1 benchmark
assert "labels outside the managed set survive" "[ \$(writes) = 2 ] && ! grep -q 'p1\|benchmark' $tmp/log"

run 'Add a node list command' bug
assert "a non-conventional title leaves every label alone" "[ $rc -eq 0 ] && [ \"\$(writes)\" = 0 ]"

run 'security(ui): refuse an unsigned cookie' enhancement security
assert "a duplicate type is collapsed to the one the title names" "[ \$(writes) = 1 ] && grep -q 'DELETE repos/o/r/issues/7/labels/enhancement' $tmp/log"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
