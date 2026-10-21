#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/last-deployed-commit.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# A fake curl answers from files in $FAKE_API: runs.json for the run list and
# jobs-<run id>.json for a run's jobs; a missing file fails the request.
mkdir -p "$tmp/bin"
cat > "$tmp/bin/curl" <<'EOF'
#!/usr/bin/env bash
url="${*: -1}"
case "$url" in
  */actions/workflows/deploy.yml/runs\?*) f="$FAKE_API/runs.json" ;;
  */actions/runs/*/jobs\?*) id="${url#*/actions/runs/}"; f="$FAKE_API/jobs-${id%%/*}.json" ;;
  *) exit 22 ;;
esac
[ -f "$f" ] || exit 22
cat "$f"
EOF
chmod +x "$tmp/bin/curl"

# main: c1 (root) -> c2 -> c3 -> c4 (HEAD); side: a commit off c2 that main never had.
repo="$tmp/repo"; git init -q -b main "$repo"
commit(){ git -C "$repo" commit -q --allow-empty -m "$1"; git -C "$repo" rev-parse HEAD; }
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
c1="$(commit c1)"; c2="$(commit c2)"
git -C "$repo" checkout -q -b side; side="$(commit side)"; git -C "$repo" checkout -q main
c3="$(commit c3)"; c4="$(commit c4)"

runs(){ local out='{"workflow_runs":['; local sep=""
  while [ $# -gt 0 ]; do out+="${sep}{\"id\":$1,\"head_sha\":\"$2\"}"; sep=","; shift 2; done
  echo "${out}]}" > "$FAKE_API/runs.json"; }
jobs(){ echo "{\"jobs\":[{\"name\":\"detect-changes\",\"conclusion\":\"success\"},{\"name\":\"deploy\",\"conclusion\":\"$2\"}]}" \
  > "$FAKE_API/jobs-$1.json"; }
run(){ (cd "$repo" && PATH="$tmp/bin:$PATH" GITHUB_REPOSITORY=o/r GITHUB_TOKEN=x bash "$S" 2>"$tmp/stderr"); }
case_dir(){ export FAKE_API="$tmp/api-$1"; mkdir -p "$FAKE_API"; }

# HEAD's own run is never the base; a run that deployed nothing is passed over.
case_dir skipped
runs 4 "$c4" 3 "$c3" 2 "$c2" 1 "$c1"; jobs 4 success; jobs 3 skipped; jobs 2 success; jobs 1 success
got="$(run)"; assert "the newest deployed commit before HEAD, past a run that deployed nothing" "[ \"$got\" = \"$c2\" ]"

# A deployed commit HEAD's history does not contain is passed over.
case_dir side
runs 9 "$side" 2 "$c2"; jobs 9 success; jobs 2 success
got="$(run)"; assert "a deploy of a commit off HEAD's history is passed over" "[ \"$got\" = \"$c2\" ]"

# No deployed commit: the root commit, so every service counts as changed.
case_dir none
runs 3 "$c3"; jobs 3 skipped
got="$(run)"; assert "no deploy found falls back to the root commit" "[ \"$got\" = \"$c1\" ]"
assert "the fallback is reported" "grep -q 'every service is rebuilt' \"$tmp/stderr\""

# The API cannot be read: the step fails rather than guess.
case_dir no-runs
run >/dev/null; assert "an unreadable run list fails" "[ $? -ne 0 ]"
case_dir no-jobs
runs 3 "$c3"
run >/dev/null; assert "an unreadable job list fails" "[ $? -ne 0 ]"

# deploy.yml detects changes from this script's commit, with the permission to read runs.
W="${HERE}/../../.github/workflows/deploy.yml"
assert "deploy.yml runs the script" "grep -q 'bash scripts/deploy/last-deployed-commit.sh' \"$W\""
assert "the paths filter uses its commit as base" "grep -qF 'base: \${{ steps.since.outputs.sha }}' \"$W\""
assert "detect-changes may read workflow runs" "grep -q 'actions: read' \"$W\""

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
