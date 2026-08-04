#!/usr/bin/env bash
# Fails if anything in the repo side-loads the pulled continuo-validation
# image with a bare `kind load docker-image` instead of the platform-scoped
# route, and separately fails if either kind-provisioning script stops
# calling that route at all.
#
# `docker pull` on a containerd-backed image store only fetches the blobs for
# the host platform, but keeps the full multi-platform manifest-list
# metadata — including any buildx provenance/SBOM attestation manifests the
# publisher attached. `kind load docker-image` always runs `ctr images
# import --all-platforms`, which then fails looking for blobs of platforms
# that were never pulled: `ctr: content digest sha256:...: not found`. This
# only affects the one image the repo *pulls*; every other image anything
# loads is built locally, and a local build only ever writes manifests it
# actually has blobs for.
#
# The fix is scripts/lib/common.sh's kind_load_pulled_image helper, which
# tries a single-platform archive (`docker save --platform <host arch>` +
# `kind load image-archive`) and falls back to the bare form only when that
# route itself is unavailable (older Docker, a classic graphdriver store).
# Two things regress this:
#   1. Any file — not just the two scripts known about today — side-loading
#      the pulled validation ref straight off `kind load docker-image`. A
#      new provisioner, install-test harness, or docs snippet doing this
#      would pass silently if this guard only checked known filenames, which
#      is exactly the regression the guard exists to prevent. So this half
#      is a repo-wide search, excluding this guard's own *_test.sh fixtures
#      (which deliberately contain the bad form as negative test cases).
#   2. scripts/setup.sh or tests/e2e/provision-k8s-test-env.sh dropping the
#      kind_load_pulled_image call entirely. Nothing else is expected to
#      provision a kind cluster from scratch, so this half stays scoped to
#      those two files.

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Matches the full ref: ghcr.io/carolsimone/continuo-validation-postgres:vX.Y.Z
REF_RE='ghcr\.io/carolsimone/continuo-validation-postgres:[A-Za-z0-9._-]+'

# `.*` between the subcommand and the ref tolerates flags in either position
# — `kind load docker-image <ref> --name x` and `kind load docker-image
# --name x <ref>` both match — since grep's `.` does not cross newlines, this
# only fires within a single line, which is how every real call site in this
# repo is written.
BARE_FORM_RE="kind load docker-image.*${REF_RE}"

fail=0

# --- half 1: nothing anywhere uses the bare form for this ref -------------
# .git and any dir literally named "worktrees" (.claude/worktrees holds full
# stale copies of every module — see CLAUDE.md) aren't source. .superpowers
# is this branch's own gitignored scratch/review notes, which legitimately
# quote the old bare form verbatim when diffing the fix; it is never part of
# a checked-out repo in CI, so scanning it here would only be noise.
bare_hits="$(grep -rlE \
  --exclude-dir=.git --exclude-dir=worktrees --exclude-dir=.superpowers \
  --exclude='*_test.sh' \
  "$BARE_FORM_RE" "$REPO_ROOT" 2>/dev/null)"
if [ -n "$bare_hits" ]; then
  echo "check-validation-image-sideload: bare 'kind load docker-image' on the pulled validation image found in — this fails on a containerd-backed image store (see scripts/lib/common.sh:kind_load_pulled_image):" >&2
  while IFS= read -r hit; do
    echo "  ${hit#"$REPO_ROOT"/}" >&2
  done <<<"$bare_hits"
  fail=1
fi

# --- half 2: the two known provisioning scripts still call the helper -----
check_calls_helper() {
  local file="$1" rel="$2"

  if [ ! -f "$file" ]; then
    echo "check-validation-image-sideload: ${rel} not found" >&2
    fail=1
    return
  fi

  if ! grep -qE "kind_load_pulled_image[[:space:]]+\"?${REF_RE}" "$file"; then
    echo "check-validation-image-sideload: ${rel} never calls kind_load_pulled_image for the validation image" >&2
    fail=1
  fi
}

check_calls_helper "${REPO_ROOT}/scripts/setup.sh" "scripts/setup.sh"
check_calls_helper "${REPO_ROOT}/tests/e2e/provision-k8s-test-env.sh" "tests/e2e/provision-k8s-test-env.sh"

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Validation image side-load OK: no bare kind load docker-image, both scripts call the platform-scoped route"
exit 0
