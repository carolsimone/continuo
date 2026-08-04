#!/usr/bin/env bash
# Fails if either kind-provisioning script side-loads the pulled
# continuo-validation image with a bare `kind load docker-image` instead of
# the platform-scoped route.
#
# `docker pull` on a containerd-backed image store only fetches the blobs for
# the host platform, but keeps the full multi-platform manifest-list
# metadata — including any buildx provenance/SBOM attestation manifests the
# publisher attached. `kind load docker-image` always runs `ctr images
# import --all-platforms`, which then fails looking for blobs of platforms
# that were never pulled: `ctr: content digest sha256:...: not found`. This
# only affects the one image the scripts *pull*; every other image the
# scripts load is built locally, and a local build only ever writes
# manifests it actually has blobs for.
#
# The fix is scripts/lib/common.sh's kind_load_pulled_image helper, which
# saves a single-platform archive (`docker save --platform <host arch>`) and
# loads that archive directly. This guard fails if either script regresses
# to loading the pulled validation ref straight off `kind load
# docker-image`.

set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Matches the full ref: ghcr.io/carolsimone/continuo-validation-postgres:vX.Y.Z
REF_RE='ghcr\.io/carolsimone/continuo-validation-postgres:[A-Za-z0-9._-]+'

fail=0

check_file() {
  local file="$1" rel="$2"

  if [ ! -f "$file" ]; then
    echo "check-validation-image-sideload: ${rel} not found" >&2
    fail=1
    return
  fi

  if grep -qE "kind load docker-image[[:space:]]+${REF_RE}" "$file"; then
    echo "check-validation-image-sideload: ${rel} side-loads the pulled validation image with a bare 'kind load docker-image' — this fails on a containerd-backed image store (see scripts/lib/common.sh:kind_load_pulled_image)" >&2
    fail=1
  fi

  if ! grep -qE "kind_load_pulled_image[[:space:]]+\"?${REF_RE}" "$file"; then
    echo "check-validation-image-sideload: ${rel} never calls kind_load_pulled_image for the validation image" >&2
    fail=1
  fi
}

check_file "${REPO_ROOT}/scripts/setup.sh" "scripts/setup.sh"
check_file "${REPO_ROOT}/tests/e2e/provision-k8s-test-env.sh" "tests/e2e/provision-k8s-test-env.sh"

if [ "$fail" -ne 0 ]; then
  exit 1
fi

echo "Validation image side-load OK: both scripts use the platform-scoped route"
exit 0
