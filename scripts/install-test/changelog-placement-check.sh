#!/usr/bin/env bash
# Fails when the PR's diff of deploy/continuo/CHANGELOG.md adds a non-blank
# line inside a released version section (`## [x.y.z] - date`). A new entry
# belongs under `## [Unreleased]`; one that lands under a released heading
# describes a change that version never shipped. Merging main into a branch
# after a release cut produces exactly that: the cut moved the Unreleased
# entries under the new version's heading, and git places the branch's own
# bullet next to them, under the released heading, without a conflict.
#
# A release cut adds its version's heading, so a section whose heading line
# the diff adds takes added lines (the heading, its intro paragraph and the
# entries moved under it). Lines under `## [Unreleased]`, under any `## `
# heading that names no version, and before the first `## ` heading are
# always allowed. Blank lines are never counted.
#
# Needs BASE_SHA (the PR's base commit) and compares it with HEAD, as
# changelog-check.sh (which calls this) does. CHANGELOG overrides the file.
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${BASE_SHA:?BASE_SHA must be set (the PR base commit)}"
changelog="${CHANGELOG:-deploy/continuo/CHANGELOG.md}"

# One hunk holding the whole new file: its context (' ') and added ('+')
# lines, in order, give each added line's line number and the heading above it.
diff="$(git diff --no-color --unified=1000000 "${BASE_SHA}" HEAD -- "$changelog")"
if [ -z "$diff" ]; then
  exit 0
fi

# Prints "<line>\t<text>\t<section heading>" for each offending line.
violations="$(awk '
  { lines[NR] = $0 }
  END {
    # The version headings this diff adds: the sections a release cut opens.
    in_hunk = 0
    for (i = 1; i <= NR; i++) {
      l = lines[i]
      if (l ~ /^@@ /) { in_hunk = 1; continue }
      if (in_hunk && l ~ /^\+## \[/) opened[substr(l, 2)] = 1
    }
    in_hunk = 0; line = 0; section = ""; released = 0
    for (i = 1; i <= NR; i++) {
      l = lines[i]
      if (l ~ /^@@ /) {
        # @@ -a,b +c,d @@: the hunk starts at line c of the new file.
        start = l
        sub(/^@@ -[0-9]+(,[0-9]+)? \+/, "", start)
        sub(/[^0-9].*$/, "", start)
        line = start - 1
        in_hunk = 1
        continue
      }
      if (!in_hunk) continue
      kind = substr(l, 1, 1)
      if (kind != " " && kind != "+") continue
      text = substr(l, 2)
      line++
      if (text ~ /^## /) {
        section = text
        released = (text ~ /^## \[[0-9]+\.[0-9]+\.[0-9]+[^]]*\]/) && !(text in opened)
      }
      if (kind == "+" && released && text !~ /^[[:space:]]*$/) {
        printf "%d\t%s\t%s\n", line, text, section
      }
    }
  }
' <<< "$diff")"

if [ -z "$violations" ]; then
  echo "CHANGELOG.md adds no line to a released section. OK."
  exit 0
fi

while IFS=$'\t' read -r line text section; do
  echo "::error file=${changelog},line=${line}::${changelog} line ${line} adds \"${text}\" under the released section \"${section}\""
done <<< "$violations"
echo
echo "A released section changes only in the release cut that adds its heading." \
     "Move these lines under '## [Unreleased]' in ${changelog} (after merging" \
     "main, git can place a branch's new bullet under the version a release" \
     "cut just created)."
exit 1
