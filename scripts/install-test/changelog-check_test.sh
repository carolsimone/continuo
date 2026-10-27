#!/usr/bin/env bash
# Test for changelog-check.sh and changelog-placement-check.sh. Each case
# builds a throwaway git repository holding copies of both scripts and a
# fixture chart, commits a base CHANGELOG.md and the PR's version of it, and
# runs changelog-check.sh with BASE_SHA set to the base commit.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; [ -f "$out" ] && sed 's/^/    /' "$out"; fi; }

export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@t GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@t
g(){ git -C "$repo" -c commit.gpgsign=false -c core.hooksPath=/dev/null "$@"; }
commit(){ g add -A && g commit -q -m "$1" && g rev-parse HEAD; }

BASE_CHANGELOG='# Changelog

Notes on how this file is kept.

## [Unreleased]

### Added
- pending value.

## [0.2.0] - 2026-10-26

Intro of 0.2.0. **MINOR.**

### Added
- foo value.

## [0.1.0] - 2026-10-20

First release.'

# new_repo NAME: a repository whose base commit holds the chart and
# BASE_CHANGELOG; sets $repo, $base and $out.
new_repo(){
  repo="$tmp/$1"; out="$tmp/$1.out"
  git init -q -b main "$repo"
  mkdir -p "$repo/scripts/install-test" "$repo/deploy/continuo/templates"
  cp "$HERE/changelog-check.sh" "$HERE/changelog-placement-check.sh" "$repo/scripts/install-test/"
  printf 'a: 1\n' > "$repo/deploy/continuo/values.yaml"
  printf '%s\n' "$BASE_CHANGELOG" > "$repo/deploy/continuo/CHANGELOG.md"
  base="$(commit base)"
}
# edit_changelog SED-ARGS...: edits the repository's CHANGELOG.md with sed.
edit_changelog(){
  local f="$repo/deploy/continuo/CHANGELOG.md"
  sed "$@" "$f" > "$f.new" && mv "$f.new" "$f"
}
touch_chart(){ printf 'a: 2\n' > "$repo/deploy/continuo/values.yaml"; }
check(){ BASE_SHA="$1" bash "$repo/scripts/install-test/changelog-check.sh" >"$out" 2>&1; rc=$?; }

ADD_UNRELEASED='s/^- pending value\.$/&\n- bar value./'
ADD_RELEASED='s/^- foo value\.$/&\n- bar value./'
CUT='s/^## \[Unreleased\]$/&\n\n## [0.3.0] - 2026-10-27\n\nIntro of 0.3.0. **MINOR.**/'

# A new bullet under [Unreleased] is where entries belong.
new_repo unreleased
touch_chart; edit_changelog "$ADD_UNRELEASED"
commit pr >/dev/null; check "$base"
assert "a bullet under [Unreleased] passes" "[ $rc -eq 0 ]"

# A bullet under a released version fails, naming the line and the section.
new_repo released
touch_chart; edit_changelog "$ADD_RELEASED"
commit pr >/dev/null; check "$base"
assert "a bullet under a released section fails" "[ $rc -ne 0 ]"
assert "the failure names the line" "grep -qF 'line=16::deploy/continuo/CHANGELOG.md line 16 adds \"- bar value.\"' \"$out\""
assert "the failure names the section" "grep -qF 'under the released section \"## [0.2.0] - 2026-10-26\"' \"$out\""

new_repo oldest
printf -- '- a late note.\n' >> "$repo/deploy/continuo/CHANGELOG.md"
commit pr >/dev/null; check "$base"
assert "a line under the oldest released section fails" "[ $rc -ne 0 ] && grep -qF '\"## [0.1.0] - 2026-10-20\"' \"$out\""

# A release cut adds its version heading and an intro paragraph above the
# entries Unreleased held, which now sit under the new version.
new_repo cut
edit_changelog "$CUT"
commit pr >/dev/null; check "$base"
assert "a release cut passes" "[ $rc -eq 0 ]"

# The cut opens only its own section: a line under an older one still fails.
new_repo cut-and-older
edit_changelog -e "$CUT" -e "$ADD_RELEASED"
commit pr >/dev/null; check "$base"
assert "a release cut that also edits an older release fails" "[ $rc -ne 0 ] && grep -qF '\"## [0.2.0] - 2026-10-26\"' \"$out\""

# Lines before the first section and blank lines anywhere are not entries.
new_repo docs
edit_changelog -e 's/^Notes on how this file is kept\.$/&\nEach PR adds its entry under Unreleased./' \
               -e 's/^First release\.$/&\n/'
commit pr >/dev/null; check "$base"
assert "lines before the first section and blank lines pass" "[ $rc -eq 0 ]"

# The way such a line usually arrives: the branch adds a bullet under
# [Unreleased], main cuts a release, and merging main into the branch moves
# the bullet under the new version's heading without a conflict.
new_repo merged-cut
g checkout -q -b feature
touch_chart; edit_changelog "$ADD_UNRELEASED"
commit feature >/dev/null
g checkout -q main
edit_changelog "$CUT"
main_cut="$(commit cut)"
g checkout -q feature
g merge -q --no-edit main >/dev/null 2>&1
assert "the merge placed the branch's bullet under the cut version" \
  "awk '/^## \\[0.3.0\\]/{s=1} /^## \\[0.2.0\\]/{s=0} s && /bar value/{f=1} END{exit !f}' \"$repo/deploy/continuo/CHANGELOG.md\""
check "$main_cut"
assert "a bullet a merge of main moved under a released section fails" "[ $rc -ne 0 ] && grep -qF '\"## [0.3.0] - 2026-10-27\"' \"$out\""

# The surface check this script started with still holds.
new_repo no-entry
touch_chart
commit pr >/dev/null; check "$base"
assert "a chart change without a CHANGELOG.md entry fails" "[ $rc -ne 0 ] && grep -q 'without a CHANGELOG.md entry' \"$out\""

new_repo untouched
printf 'x\n' > "$repo/README.md"
commit pr >/dev/null; check "$base"
assert "a PR that does not touch the chart passes" "[ $rc -eq 0 ]"

new_repo no-base
(cd "$repo" && env -u BASE_SHA bash scripts/install-test/changelog-check.sh >"$out" 2>&1); rc=$?
assert "an unset BASE_SHA fails" "[ $rc -ne 0 ]"

if [ "$fail" -eq 0 ]; then echo "ALL PASS"; else echo "FAILURES"; exit 1; fi
