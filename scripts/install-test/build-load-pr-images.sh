#!/usr/bin/env bash
# Build the images a PR CHANGED and side-load them into the install-test kind
# cluster, so the chart install exercises THIS PR's images for anything that
# changed and pulls the published :latest from ghcr for everything else.
#
# Why this exists: install-test installs the chart with global.imageTag=latest
# on PR runs, which by itself pulls the images published from main. A PR that
# renames a service or a database (so the chart references an image name or a
# migration that main's published :latest does not yet contain) would fail the
# install against those stale images — even though the PR is internally correct.
# ci.yml's e2e suite already builds locally and never hit this; the chart-install
# gate did. Building only the changed images here closes that gap without
# rebuilding all ~13 every run.
#
# Only meaningful on pull_request runs. The release path (workflow_call, a real
# vX.Y.Z image_tag) MUST keep pulling the published release images and never
# calls this script.
#
# The chart's global.imagePullPolicy defaults to IfNotPresent, so an image
# already present in the node's containerd store (what `kind load` puts there)
# is used as-is and never pulled — that is what makes a loaded image win over
# the published :latest of the same ref.
#
# Env:
#   BASE_SHA  (required) the PR base commit to diff against. Fetch it first
#             (`git fetch --depth=1 origin "$BASE_SHA"`); a two-commit diff is
#             used so a disconnected shallow base with no merge-base still works.
#   CLUSTER   kind cluster name to load into (default: install-test).
#   TAG       tag to build+load under (default: latest) — must match the tag the
#             chart resolves, i.e. the IMAGE_TAG install-test passes to run.sh.
set -euo pipefail
cd "$(dirname "$0")/../.."

: "${BASE_SHA:?BASE_SHA must be set (the PR base commit)}"
CLUSTER="${CLUSTER:-install-test}"
TAG="${TAG:-latest}"
VALUES=deploy/continuo/values.yaml

# The loaded ref MUST equal what "continuo.image" resolves at install time:
#   <imageRegistry>/<imageRepositoryPrefix>/continuo-<name>:<tag>  (registry set)
#   <imageRepositoryPrefix>/continuo-<name>:<tag>                  (registry "")
# install-test's run.sh overrides only global.imageTag, so registry and prefix
# come from the chart defaults below — read them from values.yaml so this stays
# in lockstep if those defaults ever change.
registry="$(awk '/^global:/{f=1} f&&/^  imageRegistry:/{gsub(/"/,"",$2); print $2; exit}' "$VALUES")"
prefix="$(awk '/^global:/{f=1} f&&/^  imageRepositoryPrefix:/{gsub(/"/,"",$2); print $2; exit}' "$VALUES")"
: "${prefix:?could not read global.imageRepositoryPrefix from $VALUES}"
if [ -n "$registry" ]; then
  image_base="${registry}/${prefix}/continuo"
else
  image_base="${prefix}/continuo"
fi

# Source of truth: deploy.yml's build-publish matrix. Columns:
#   <image-name> <trigger-dir> <dockerfile> <context> <needs-base>
# <trigger-dir> is the source path whose change means this image must rebuild;
# <image-name> is what the chart references as continuo-<image-name>. Only the
# 8 Go-service Deployments, ui, topology-controller and the migrations Job/init
# container are gated by the install's --wait/--wait-for-jobs, but every image
# the chart can reference is listed so a rename of any of them is handled.
# needs-base=1 images build FROM continuo-base (Dockerfile.base); it is built
# once up front when any such image is selected.
IMAGES="\
state|state/|state/Dockerfile.prod|.|1
orchestrator|orchestrator/|orchestrator/Dockerfile.prod|.|1
executor-controller|executor-controller/|executor-controller/Dockerfile.prod|.|1
k8s-controller|k8s-controller/|k8s-controller/Dockerfile.prod|.|1
ui|ui/|ui/Dockerfile|ui|0
topology-controller|topology-controller/|topology-controller/Dockerfile.prod|topology-controller|0
release-controller|release-controller/|release-controller/Dockerfile.prod|.|1
agent-chat|agent-chat/|agent-chat/Dockerfile.prod|.|1
remediation|remediation/|remediation/Dockerfile.prod|.|1
agent-remediation|agent-remediation/|agent-remediation/Dockerfile.prod|.|1
migrations|db/|db/Dockerfile.migrate|db|0
s3-sidecar|s3-sidecar/|s3-sidecar/Dockerfile|s3-sidecar|0
stream-reaper|pkg/cmd/stream-reaper/|pkg/cmd/stream-reaper/Dockerfile|pkg|1"

# Two-commit diff (not three-dot): compares tree contents directly, so it works
# even when BASE_SHA was fetched as a disconnected shallow object with no shared
# history graph to compute a merge-base from (mirrors changelog-check.sh).
changed="$(git diff --name-only "${BASE_SHA}" HEAD)"
if [ -z "$changed" ]; then
  echo "No files changed against ${BASE_SHA}; nothing to build. Install pulls published :${TAG}."
  exit 0
fi
echo "Changed files in this PR:"
echo "$changed"

selected=""
need_base=0
while IFS='|' read -r name trigger dockerfile context needs_base; do
  [ -n "$name" ] || continue
  if grep -q "^${trigger}" <<<"$changed"; then
    selected="${selected}${name}|${dockerfile}|${context}|${needs_base}"$'\n'
    [ "$needs_base" = "1" ] && need_base=1
  fi
done <<<"$IMAGES"

if [ -z "$selected" ]; then
  echo "No changed path maps to a buildable image; nothing to load. Install pulls published :${TAG}."
  exit 0
fi

echo "Images to build+load for this PR:"
printf '%s' "$selected" | awk -F'|' 'NF{print "  - continuo-"$1}'

if [ "$need_base" = "1" ]; then
  echo "Building continuo-base:latest (a selected Go image builds FROM it)..."
  DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .
fi

while IFS='|' read -r name dockerfile context needs_base; do
  [ -n "$name" ] || continue
  ref="${image_base}-${name}:${TAG}"
  echo "Building ${ref} (dockerfile=${dockerfile} context=${context})..."
  if [ "$needs_base" = "1" ]; then
    DOCKER_BUILDKIT=1 docker build -t "$ref" -f "$dockerfile" \
      --build-arg "BASE_IMAGE=continuo-base:latest" "$context"
  else
    DOCKER_BUILDKIT=1 docker build -t "$ref" -f "$dockerfile" "$context"
  fi
  echo "Loading ${ref} into kind cluster '${CLUSTER}'..."
  kind load docker-image "$ref" --name "$CLUSTER"
done <<<"$(printf '%s' "$selected")"

echo "Done: PR-changed images built and loaded; the chart install will use them (imagePullPolicy=IfNotPresent) and pull published :${TAG} for the rest."
