#!/usr/bin/env bash
# Test for wait-for-image-publish.sh and the workflow wiring it relies on. A stub
# gh answers `run list` from the next line of $STUB_LISTS on each call and `run
# view` from the next line of a per-run file of $STUB_VIEWS, applying the real --jq
# filter with jq, so no network is touched. A line "fail" makes the call fail.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/wait-for-image-publish.sh"; ROOT="$(cd "${HERE}/../.." && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; sed 's/^/    /' "$tmp/out"; fi; }

mkdir -p "$tmp/bin"
cat > "$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "gh $*" >> "$STUB_LOG"
# next FILE: prints FILE's next unread line, repeating the last one once it runs out.
next(){
  local count="$1.count" n
  n="$(cat "$count" 2>/dev/null || echo 0)"; n=$((n + 1)); echo "$n" > "$count"
  sed -n "${n}p" "$1" | grep . || tail -n 1 "$1"
}
filter=""
for ((i = 1; i <= $#; i++)); do
  if [ "${!i}" = "--jq" ]; then j=$((i + 1)); filter="${!j}"; fi
done
case "$1 $2" in
  "run list") line="$(next "$STUB_LISTS")" ;;
  "run view") line="$(next "$STUB_VIEWS/$3")" ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
[ "$line" != fail ] || { echo "HTTP 502" >&2; exit 1; }
printf '%s' "$line" | jq -r "$filter"
STUB
chmod +x "$tmp/bin/gh"

jobs(){ # jobs NAME=CONCLUSION...: the JSON of gh run view --json jobs.
  local out="" pair
  for pair in "$@"; do
    out="${out:+$out,}{\"name\":\"${pair%%=*}\",\"conclusion\":\"${pair#*=}\"}"
  done
  printf '{"jobs":[%s]}\n' "$out"
}
PUBLISHED="$(jobs 'build-publish (ui, false, ui/Dockerfile, ui)=success' 'build-publish (catalog, false, catalog/Dockerfile.prod, .)=success' 'deploy=')"
PUBLISHING="$(jobs 'build-publish (ui, false, ui/Dockerfile, ui)=success' 'build-publish (catalog, false, catalog/Dockerfile.prod, .)=')"
FAILED_LEG="$(jobs 'build-publish (ui, false, ui/Dockerfile, ui)=failure' 'build-publish (catalog, false, catalog/Dockerfile.prod, .)=success')"
NOT_STARTED="$(jobs 'detect-changes=')"
SKIPPED="$(jobs 'detect-changes=success' 'build-publish=skipped' 'deploy=skipped')"

# scenario LIST_LINE...: resets the stub with these `run list` answers.
scenario(){
  rm -rf "$tmp/stub"; mkdir -p "$tmp/stub/views"
  printf '%s\n' "$@" > "$tmp/stub/lists"
}
view(){ local id="$1"; shift; printf '%s\n' "$@" > "$tmp/stub/views/$id"; }
# wait_for: runs the script; exit code in $rc. A script that never ends is killed,
# which fails the scenario's assertions.
wait_for(){
  : > "$tmp/log"
  PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/log" STUB_LISTS="$tmp/stub/lists" STUB_VIEWS="$tmp/stub/views" \
    POLL_SECONDS=0 NO_RUN_GRACE_SECONDS="${GRACE:-0}" bash "$S" abc123 >"$tmp/out" 2>&1 &
  local pid=$!
  ( sleep 20; kill "$pid" ) >/dev/null 2>&1 &
  local watchdog=$!
  wait "$pid"
  rc=$?
  kill "$watchdog" 2>/dev/null
  wait "$watchdog" 2>/dev/null
}
lists(){ grep -c '^gh run list' "$tmp/log"; }

scenario '[{"databaseId":1,"status":"completed"}]'
view 1 "$PUBLISHED"
wait_for
assert "a run that has published ends the wait at once" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 1 ] && grep -q 'run 1 has published' \"$tmp/out\""
assert "it asks for the runs of this commit of deploy.yml" "grep -q -- '--workflow deploy.yml --commit abc123' \"$tmp/log\""

scenario '[{"databaseId":1,"status":"queued"}]' '[{"databaseId":1,"status":"in_progress"}]' '[{"databaseId":1,"status":"in_progress"}]'
view 1 "$NOT_STARTED" "$PUBLISHING" "$PUBLISHED"
wait_for
assert "it waits through a queued run and a publish in progress" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 3 ] && grep -q 'has published' \"$tmp/out\""
assert "it does not wait for the run's deploy job" "grep -c '^Waiting' \"$tmp/out\" | grep -qx 2"

scenario '[{"databaseId":1,"status":"completed"}]'
view 1 "$NOT_STARTED"
wait_for
assert "a run cancelled in the queue ends the wait, leaving the verdict to the retag" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 1 ] && grep -q 'none published' \"$tmp/out\""

scenario '[{"databaseId":1,"status":"completed"}]'
view 1 "$SKIPPED"
wait_for
assert "a commit whose run skipped build-publish ends the wait" "[ $rc -eq 0 ] && grep -q 'none published' \"$tmp/out\""

scenario '[{"databaseId":1,"status":"in_progress"}]' '[{"databaseId":1,"status":"completed"}]'
view 1 "$FAILED_LEG"
wait_for
assert "a failed build waits for its run to end, then ends the wait unpublished" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 2 ] && grep -q 'none published' \"$tmp/out\""

scenario '[{"databaseId":2,"status":"queued"},{"databaseId":1,"status":"completed"}]'
view 1 "$PUBLISHED"
view 2 "$NOT_STARTED"
wait_for
assert "a run that published is enough while a force_publish run of the commit queues" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 1 ] && grep -q 'run 1 has published' \"$tmp/out\""

scenario 'fail' '[{"databaseId":1,"status":"in_progress"}]'
view 1 fail "$PUBLISHED"
wait_for
assert "a failure to read the runs or a run's jobs is retried" "[ $rc -eq 0 ] && grep -q 'trying again' \"$tmp/out\" && grep -q 'has published' \"$tmp/out\""

scenario '[]'
GRACE=0 wait_for
assert "a commit with no run ends the wait once the grace is over" "[ $rc -eq 0 ] && grep -q 'no deploy.yml run' \"$tmp/out\""

scenario '[]' '[]' '[{"databaseId":1,"status":"completed"}]'
view 1 "$PUBLISHED"
GRACE=60 wait_for
assert "a run that appears within the grace is waited for" "[ $rc -eq 0 ] && [ \"\$(lists)\" -eq 3 ] && grep -q 'has published' \"$tmp/out\""

# The wiring this script relies on.
deploy="${ROOT}/.github/workflows/deploy.yml"
release="${ROOT}/.github/workflows/release.yml"
release_filter="$(awk '/^            release:$/{on=1; next} on && /^            [a-z_]+:$/{on=0} on' "$deploy")"
assert "a CHANGELOG-only release cut matches deploy.yml's release filter" "grep -qF -- \"- 'deploy/**'\" <<< \"\$release_filter\""
assert "deploy.yml's build-publish runs on that filter" "grep -qF \"needs.detect-changes.outputs.release == 'true'\" \"$deploy\""
job(){ awk -v job="  $1:" '$0 == job {on=1; next} on && /^  [A-Za-z0-9_-]+:$/ {on=0} on' "$release"; }
assert "release.yml retags only after the wait" "grep -qE '^    needs: wait-for-images\$' <<< \"\$(job retag-images)\""
assert "the wait holds no self-hosted runner, which the deploy run it waits for needs" "grep -qE '^    runs-on: ubuntu-latest\$' <<< \"\$(job wait-for-images)\""
assert "the wait runs this script" "grep -qF 'scripts/release/wait-for-image-publish.sh' <<< \"\$(job wait-for-images)\""

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
