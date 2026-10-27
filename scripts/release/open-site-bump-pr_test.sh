#!/usr/bin/env bash
# Test for open-site-bump-pr.sh. A bare repository stands in for the site
# repository and a stub gh on PATH clones it, answers `release view` from
# $STUB_RELEASES and records `pr create`, so no network is touched. Git runs
# without the user's global configuration.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/open-site-bump-pr.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; [ -f "$tmp/err" ] && sed 's/^/    /' "$tmp/err"; fi; }

export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1
export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t

mkdir -p "$tmp/bin"
cat > "$tmp/bin/gh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "gh $*" >> "$STUB_LOG"
case "$1 $2" in
  "repo clone") git clone -q "$STUB_SITE_REMOTE" "$4" ;;
  "release view")
    for r in ${STUB_RELEASES:-}; do [ "$r" = "$3" ] && exit 0; done
    echo "release not found" >&2; exit 1 ;;
  "pr create")
    shift 2
    while [ $# -gt 0 ]; do
      case "$1" in
        --title) printf '%s' "$2" > "$STUB_PR/title"; shift 2 ;;
        --body) printf '%s' "$2" > "$STUB_PR/body"; shift 2 ;;
        --head) printf '%s' "$2" > "$STUB_PR/head"; shift 2 ;;
        --base) printf '%s' "$2" > "$STUB_PR/base"; shift 2 ;;
        --repo) printf '%s' "$2" > "$STUB_PR/repo"; shift 2 ;;
        *) shift ;;
      esac
    done
    echo "https://github.com/example/site/pull/1" ;;
  *) echo "unexpected gh call: $*" >&2; exit 2 ;;
esac
STUB
chmod +x "$tmp/bin/gh"

SITE_CONFIG="export const site = {
  name: 'continuo',
  /** Helm chart version shown in the quickstart. */
  chartVersion: '0.15.0',
  url: 'https://example.com',
} as const;"

# new_site NAME CONFIG: a bare site repository whose main holds CONFIG as
# site.config.ts; sets $remote and $main.
new_site(){
  remote="$tmp/$1.git"; local seed="$tmp/$1-seed"
  git init -q --bare -b main "$remote"
  git init -q -b main "$seed"
  printf '%s\n' "$2" > "$seed/site.config.ts"
  git -C "$seed" add -A && git -C "$seed" commit -q -m init
  git -C "$seed" push -q "$remote" main
  main="$(git -C "$remote" rev-parse main)"
}
# bump VERSION: runs the script against $remote; exit code in $rc.
bump(){
  rm -rf "$tmp/pr"; mkdir -p "$tmp/pr"; : > "$tmp/log"
  PATH="$tmp/bin:$PATH" STUB_LOG="$tmp/log" STUB_PR="$tmp/pr" STUB_SITE_REMOTE="$remote" \
    STUB_RELEASES="${STUB_RELEASES-v0.15.0 v0.16.0 v0.16.0-rc.1}" SITE_REPO=example/site \
    bash "$S" "$@" >"$tmp/out" 2>"$tmp/err"
  rc=$?
}
branch_exists(){ git -C "$remote" rev-parse -q --verify "refs/heads/$1" >/dev/null; }

# A published version: one commit on a new branch off main, then a PR.
new_site ok "$SITE_CONFIG"
bump 0.16.0
b=chore/bump-chart-0.16.0
assert "a published version succeeds" "[ $rc -eq 0 ]"
assert "the branch is pushed" "branch_exists $b"
assert "the branch is one commit off main" "[ \"\$(git -C \"$remote\" rev-parse $b^)\" = \"$main\" ]"
only_pin_changed(){
  local want
  want="$(printf '%s\n' "-  chartVersion: '0.15.0'," "+  chartVersion: '0.16.0',")"
  [ "$(git -C "$remote" diff main "$b" -- site.config.ts | grep -E '^[-+] ')" = "$want" ]
}
assert "only the chartVersion line changes, indentation kept" only_pin_changed
msg="$(git -C "$remote" log -1 --format=%B "$b" 2>/dev/null)"
has_line(){ grep -qxF "$1" <<< "$msg"; }
assert "the commit subject names the version" "[ \"\$(head -n 1 <<< \"\$msg\")\" = 'chore: bump quickstart chart version to 0.16.0' ]"
assert "the commit is signed off" "has_line 'Signed-off-by: t <t@t>'"
assert "the commit carries the Co-Authored-By trailer" "has_line 'Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>'"
assert "the commit carries the Claude-Session trailer" "has_line 'Claude-Session: https://claude.ai/code/session_01PSskScdqXuRyxr36mAaXUi'"
assert "git reads the trailers as trailers" \
  "[ \"\$(git -C \"$remote\" log -1 --format='%(trailers:key=Co-Authored-By,valueonly)' $b | head -n 1)\" = 'Claude Sonnet 5.5 <noreply@anthropic.com>' ]"
assert "the PR is titled for the version" "[ \"\$(cat \"$tmp/pr/title\")\" = 'chore: bump quickstart chart version to 0.16.0' ]"
assert "the PR goes from the branch to main of the site repository" \
  "[ \"\$(cat \"$tmp/pr/head\")\" = $b ] && [ \"\$(cat \"$tmp/pr/base\")\" = main ] && [ \"\$(cat \"$tmp/pr/repo\")\" = example/site ]"
assert "the PR body names the published chart" "grep -qF 'oci://ghcr.io/carolsimone/charts/continuo' \"$tmp/pr/body\""
assert "the PR body links the release" "grep -qF 'https://github.com/carolsimone/continuo/releases/tag/v0.16.0' \"$tmp/pr/body\""
assert "the PR URL is printed" "grep -qF 'https://github.com/example/site/pull/1' \"$tmp/out\""

# A second run for the same version finds its branch and stops.
bump 0.16.0
assert "an existing branch fails before anything is pushed" "[ $rc -ne 0 ] && grep -q 'already has a branch' \"$tmp/err\" && [ ! -f \"$tmp/pr/title\" ]"

export COMMIT_TRAILERS=''
bump 0.16.0-rc.1
unset COMMIT_TRAILERS
msg="$(git -C "$remote" log -1 --format=%B chore/bump-chart-0.16.0-rc.1 2>/dev/null)"
assert "a pre-release version with an empty COMMIT_TRAILERS succeeds" "[ $rc -eq 0 ]"
assert "an empty COMMIT_TRAILERS adds no trailer but keeps the sign-off" \
  "! grep -q 'Co-Authored-By' <<< \"\$msg\" && has_line 'Signed-off-by: t <t@t>'"

# The failure modes push nothing and open no PR.
new_site same "$SITE_CONFIG"
bump 0.15.0
assert "a version the site already pins fails" "[ $rc -ne 0 ] && grep -q \"already pins chartVersion '0.15.0'\" \"$tmp/err\""
assert "it pushes no branch" "! branch_exists chore/bump-chart-0.15.0 && [ ! -f \"$tmp/pr/title\" ]"

new_site missing "$(grep -v chartVersion <<< "$SITE_CONFIG")"
bump 0.16.0
assert "a site.config.ts without the line fails" "[ $rc -ne 0 ] && grep -q 'no \"chartVersion' \"$tmp/err\""
assert "it pushes no branch" "! branch_exists chore/bump-chart-0.16.0 && [ ! -f \"$tmp/pr/title\" ]"

new_site twice "$SITE_CONFIG
export const other = { chartVersion: '0.1.0' };
  chartVersion: '0.2.0',"
bump 0.16.0
assert "more than one chartVersion line fails" "[ $rc -ne 0 ] && grep -q 'expected one' \"$tmp/err\""

new_site unpublished "$SITE_CONFIG"
bump 0.17.0
assert "a version with no release fails" "[ $rc -ne 0 ] && grep -q 'no release v0.17.0' \"$tmp/err\""
assert "it does not clone or push" "! grep -q 'repo clone' \"$tmp/log\" && ! branch_exists chore/bump-chart-0.17.0"

bump v0.16.0
assert "a version with a leading v fails" "[ $rc -ne 0 ] && grep -q 'not a chart version' \"$tmp/err\""
bump
assert "a missing version fails" "[ $rc -ne 0 ] && grep -q 'usage' \"$tmp/err\""

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
