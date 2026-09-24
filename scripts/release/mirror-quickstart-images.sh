#!/usr/bin/env bash
# Mirror the bundled-MinIO quickstart images (server + mc client) into this
# project's own ghcr namespace, so the chart and the local stack never pull
# them from a vendor registry at install time. MinIO gated its quay images and
# Docker Hub dropped them; a mirror we own is the only pull path that survives
# the next move, and it needs no credentials from users.
#
# Copies preserve the full multi-arch manifest list (imagetools create), so the
# result runs on amd64 and on the arm64 clusters operators install on.
#
# Requires: docker with buildx and a ghcr login with packages:write. The source
# is anonymous. Run on a MinIO version bump, not per release.
#
# Usage: MINIO_SRC=<ref> MC_SRC=<ref> mirror-quickstart-images.sh <owner>
set -euo pipefail

OWNER="${1:?usage: MINIO_SRC=<ref> MC_SRC=<ref> mirror-quickstart-images.sh <owner>}"
MINIO_SRC="${MINIO_SRC:?set MINIO_SRC to the source minio SERVER image ref}"
MC_SRC="${MC_SRC:?set MC_SRC to the source mc CLIENT image ref}"

# Source ref and its destination repo, paired positionally (no associative
# arrays — this must also run on the macOS default bash 3.2).
SRCS="$MINIO_SRC $MC_SRC"
DEST_MINIO="ghcr.io/${OWNER}/continuo-minio"
DEST_MC="ghcr.io/${OWNER}/continuo-mc"

dest_for() {
  case "$1" in
    "$MINIO_SRC") echo "$DEST_MINIO" ;;
    "$MC_SRC")    echo "$DEST_MC" ;;
  esac
}

# Verify-all-then-copy: never leave one image mirrored and the other missing.
missing=""
for src in $SRCS; do
  docker buildx imagetools inspect "$src" >/dev/null 2>&1 || missing="${missing} ${src}"
done
if [ -n "$missing" ]; then
  { echo "ERROR: source image(s) not pullable:"; for m in $missing; do echo "  $m"; done; } >&2
  exit 1
fi

for src in $SRCS; do
  tag="${src##*:}"
  dest="$(dest_for "$src"):${tag}"
  docker buildx imagetools create -t "$dest" "$src"
  raw="$(docker buildx imagetools inspect "$dest" --raw)"
  for arch in amd64 arm64; do
    grep -Eq "\"architecture\": *\"${arch}\"" <<<"$raw" \
      || { echo "ERROR: ${dest} missing linux/${arch} — refusing a single-arch mirror" >&2; exit 1; }
  done
  echo "mirrored ${src} -> ${dest} (amd64+arm64)"
done
echo "All quickstart images mirrored into ghcr.io/${OWNER}."
