#!/usr/bin/env bash
# Retag every published continuo image from :<commit-sha> to :<release-tag>
# on ghcr, so a release installs fully immutable image references. Requires a
# prior `docker login ghcr.io` with packages:write (the release workflow's
# GITHUB_TOKEN) and buildx.
#
# Usage: retag-images.sh <commit-sha> <release-tag> <registry-owner>
set -euo pipefail

SHA="${1:?usage: retag-images.sh <commit-sha> <release-tag> <registry-owner>}"
TAG="${2:?usage: retag-images.sh <commit-sha> <release-tag> <registry-owner>}"
OWNER="${3:?usage: retag-images.sh <commit-sha> <release-tag> <registry-owner>}"

# Keep in sync with the build-publish matrix in .github/workflows/deploy.yml —
# the continuo-owned images every main push publishes as :<git-sha>.
services=(
  state orchestrator executor-controller k8s-controller ui
  manifest-controller release-controller agent-runner remediation
  remediation-agent migrations s3-sidecar stream-reaper
)

# Verify-all-then-retag: never leave a half-tagged release on a missing image.
missing=()
for svc in "${services[@]}"; do
  repo="ghcr.io/${OWNER}/continuo-${svc}"
  if ! docker buildx imagetools inspect "${repo}:${SHA}" >/dev/null 2>&1; then
    missing+=("${repo}:${SHA}")
  fi
done
if [ "${#missing[@]}" -gt 0 ]; then
  {
    echo "ERROR: no published image for the tagged commit:"
    printf '  %s\n' "${missing[@]}"
    echo
    echo "A release tag must point at a main commit whose push ran deploy.yml's"
    echo "build-publish job (a docs/CI-only commit does not). Fix: run"
    echo "  gh workflow run deploy.yml --ref main -f force_publish=true"
    echo "A plain dispatch re-runs the paths filter, finds no changes, and skips"
    echo "build-publish; the force flag skips that filter so unchanged services"
    echo "get their :latest retagged to :<sha> for the current main head. Wait"
    echo "for it to finish, then tag that main head."
  } >&2
  exit 1
fi

for svc in "${services[@]}"; do
  repo="ghcr.io/${OWNER}/continuo-${svc}"
  docker buildx imagetools create -t "${repo}:${TAG}" "${repo}:${SHA}"
  echo "tagged ${repo}:${TAG}"
done
echo "All ${#services[@]} images tagged :${TAG}."
