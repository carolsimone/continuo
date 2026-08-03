#!/usr/bin/env bash
# Fails if release.yml's tag trigger would also fire for a tag belonging to some
# other workflow's tag family.
#
# Every workflow decides on its own whether a pushed tag is for it; nothing
# arbitrates between them, so two overlapping globs mean one tag starts two
# workflows. `v*` matched `validation-contract-v0.3.0` — the glob anchors at the
# first character, and `validation` starts with a `v` — which sent a Python
# package tag into the product release pipeline.
#
# The check reads the tag globs out of every workflow's `on:` block, turns each
# non-release family into a sample tag, and asserts release.yml rejects it while
# still accepting a real release tag.
set -uo pipefail

WORKFLOW_DIR="${1:-.github/workflows}"
RELEASE_FILE="${WORKFLOW_DIR}/release.yml"

# Tags this trigger must keep firing for; a glob that excludes junk by also
# excluding real releases is not a fix.
RELEASE_TAGS=(v0.1.1 v1.2.3 v0.1.0-rc.1)

# Prints one tag glob per line from a workflow's `on:` block. Only the `on:`
# block counts: `tags:` also names image tags deeper in a workflow, and those
# are not triggers. Handles both the block-list form and the inline-array form.
extract_tag_globs() {
  awk '
    # A key at column 0 ends whatever top-level block we were in.
    /^[^[:space:]#]/ { in_on = ($0 ~ /^on:/); in_tags = 0 }
    !in_on { next }
    # tags: ["a*", "b*"]
    /^[[:space:]]*tags:[[:space:]]*\[/ {
      line = $0
      while (match(line, /["'"'"'][^"'"'"']+["'"'"']/)) {
        item = substr(line, RSTART + 1, RLENGTH - 2)
        print item
        line = substr(line, RSTART + RLENGTH)
      }
      next
    }
    # tags:
    #   - a*
    /^[[:space:]]*tags:[[:space:]]*$/ { in_tags = 1; next }
    in_tags {
      # Comments and blank lines sit between list entries without ending them.
      if ($0 ~ /^[[:space:]]*(#.*)?$/) next
      if ($0 ~ /^[[:space:]]*-[[:space:]]*/) {
        item = $0
        sub(/^[[:space:]]*-[[:space:]]*/, "", item)
        sub(/[[:space:]]*(#.*)?$/, "", item)
        gsub(/^["'"'"']|["'"'"']$/, "", item)
        if (item != "") print item
        next
      }
      in_tags = 0
    }
  ' "$1"
}

# Turns a glob into a concrete tag another workflow would plausibly receive.
sample_tag_for() {
  printf '%s\n' "${1//\*/0.0.0}"
}

matches_any() {
  local candidate="$1" glob
  shift
  for glob in "$@"; do
    case "$glob" in
      '!'*) continue ;;  # exclusion patterns cannot pull a tag in
    esac
    # shellcheck disable=SC2254  # the glob is meant to stay a glob here
    case "$candidate" in
      $glob) return 0 ;;
    esac
  done
  return 1
}

if [ ! -f "$RELEASE_FILE" ]; then
  echo "check-release-tag-trigger: ${RELEASE_FILE} not found" >&2
  exit 1
fi

# Read into an array the long way: mapfile needs bash 4, and this has to run on
# the macOS system bash too.
release_globs=()
while IFS= read -r glob; do
  [ -n "$glob" ] && release_globs+=("$glob")
done < <(extract_tag_globs "$RELEASE_FILE")


if [ "${#release_globs[@]}" -eq 0 ]; then
  echo "check-release-tag-trigger: no tag trigger found in ${RELEASE_FILE}" >&2
  exit 1
fi

rc=0

for tag in "${RELEASE_TAGS[@]}"; do
  if ! matches_any "$tag" "${release_globs[@]}"; then
    echo "RELEASE TRIGGER TOO NARROW in ${RELEASE_FILE}: ${tag} no longer starts a release (globs: ${release_globs[*]})" >&2
    rc=1
  fi
done

for wf in "$WORKFLOW_DIR"/*.yml "$WORKFLOW_DIR"/*.yaml; do
  [ -f "$wf" ] || continue
  [ "$wf" = "$RELEASE_FILE" ] && continue
  while IFS= read -r glob; do
    case "$glob" in '' | '!'*) continue ;; esac
    sample="$(sample_tag_for "$glob")"
    if matches_any "$sample" "${release_globs[@]}"; then
      echo "RELEASE TRIGGER OVERLAP in ${RELEASE_FILE}: tag ${sample} (family ${glob}, owned by $(basename "$wf")) would also start the release workflow" >&2
      rc=1
    fi
  done < <(extract_tag_globs "$wf")
done

[ "$rc" -eq 0 ] && echo "Release tag trigger OK: ${RELEASE_FILE} (${release_globs[*]})"
exit "$rc"
