#!/usr/bin/env bash
# Fails when the PR's diff of deploy/continuo/CHANGELOG.md adds a non-blank
# line inside a released version section (`## [x.y.z] - date`). A new entry
# belongs under `## [Unreleased]`; one that lands under a released heading
# describes a change that version never shipped. Merging main into a branch
# after a release cut produces exactly that: the cut moved the Unreleased
# entries under the new version's heading, and git places the branch's own
# bullet next to them, under the released heading, without a conflict.
#
# A released section is one whose version the base CHANGELOG already has a
# heading for. A release cut adds the section of a version the base does not
# have, so that section takes added lines (the heading, its intro paragraph
# and the entries moved under it); editing an existing version's heading, its
# date say, opens nothing. Lines under `## [Unreleased]`, under any `## `
# heading that names no version, and before the first `## ` heading are always
# allowed. Blank lines are never counted.
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

# The versions the base CHANGELOG already has a `## [x.y.z]` section for. A
# version heading whose version is not among them is a release cut; editing
# the heading of a version the base already has (its date, say) is not.
base_versions="$(git show "${BASE_SHA}:${changelog}" 2>/dev/null \
  | sed -nE 's/^## \[([0-9]+\.[0-9]+\.[0-9]+[^]]*)\].*/\1/p' | tr '\n' ' ' || true)"

# Prints "<line>\t<text>\t<section heading>" for each offending line.
violations="$(awk -v base_versions="$base_versions" '
  # The version a `## [x.y.z] ...` heading names; empty for any other line.
  function version_of(text,    v) {
    if (text !~ /^## \[[0-9]+\.[0-9]+\.[0-9]+[^]]*\]/) return ""
    v = text
    sub(/^## \[/, "", v)
    sub(/\].*$/, "", v)
    return v
  }
  { lines[NR] = $0 }
  END {
    n = split(base_versions, known, " ")
    for (i = 1; i <= n; i++) at_base[known[i]] = 1
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
        v = version_of(text)
        released = (v != "" && (v in at_base))
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
