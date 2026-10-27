#!/usr/bin/env bash
# Maps a pull request title to the labels its Conventional Commit prefix names.
#
#   scripts/pr-title-type.sh "fix(state)!: stop double-dispatching a retried task"
#
# A valid title is `<type>[(<scope>)][!]: <summary>` with <type> one of the
# prefixes in label_for below. Stdout is machine-read by
# .github/workflows/pr-labels.yml: one label per line, the type label first,
# then `breaking-change` when the title carries `!`. A title that does not
# follow the format prints nothing on stdout, explains why on stderr and exits
# 1, which is what the `pr-title` check in .github/workflows/pr-title.yml
# reports on the pull request.
#
# GitHub's own `Revert "<title>"` titles are accepted and labelled `chore`.
set -uo pipefail

title="${1-}"

label_for() {
  case "$1" in
    feat) echo enhancement ;;
    fix) echo bug ;;
    refactor) echo refactoring ;;
    docs) echo documentation ;;
    perf) echo performance ;;
    ci) echo ci/cd ;;
    test) echo test ;;
    chore | build) echo chore ;;
    security) echo security ;;
    *) return 1 ;;
  esac
}

if [[ "$title" == Revert\ \"* ]]; then
  echo chore
  exit 0
fi

re='^([a-z]+)(\([a-z0-9][a-z0-9,/+._ -]*\))?(!)?: [^ ].*$'
if [[ "$title" =~ $re ]] && label="$(label_for "${BASH_REMATCH[1]}")"; then
  echo "$label"
  [[ -n "${BASH_REMATCH[3]}" ]] && echo breaking-change
  exit 0
fi

{
  echo "The pull request title must be '<type>[(<scope>)][!]: <summary>'."
  echo "  <type> is one of: feat fix refactor docs perf ci test chore build security"
  echo "  <scope> is optional and lowercase, e.g. (state) or (ui,cli)"
  echo "  '!' before the colon marks a breaking change"
  echo "Examples: 'fix(state): stop double-dispatching a retried task'"
  echo "          'feat(cli)!: rename node trigger to node run'"
  echo "Got: '${title}'"
} >&2
exit 1
