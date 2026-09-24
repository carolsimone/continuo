#!/usr/bin/env bash
# Fail if any datastore image the platform pulls without credentials is not
# anonymously pullable — from EITHER deployment path (the bundled chart and
# docker-compose). MinIO gated its registry under a static pin once; this makes
# it impossible for that class to reach a release silently again. Runs with NO
# docker login on purpose.
set -euo pipefail
cd "$(dirname "$0")/../.."

# Chart-rendered images + the compose minio/mc pins. Restrict to the datastore
# repos (not our own continuo service images, which are gated to the release
# tag and reachable via the ghcr packages the release publishes).
chart="$(helm template t deploy/continuo | grep -oE 'image: "[^"]+"' | sed 's/image: "//;s/"//')"
compose="$(grep -oE 'image: *ghcr\.io/[^ ]*continuo-(minio|mc):[^ ]+' docker-compose.yml | sed 's/image: *//')"
images="$(printf '%s\n%s\n' "$chart" "$compose" | sort -u \
  | grep -E 'continuo-minio|continuo-mc|postgres|redis|neo4j|dexidp/dex' || true)"

bad=""
while IFS= read -r ref; do
  [ -z "$ref" ] && continue
  docker buildx imagetools inspect "$ref" >/dev/null 2>&1 || bad="${bad} ${ref}"
done <<EOF
$images
EOF

if [ -n "$bad" ]; then
  { echo "ERROR: datastore image(s) not anonymously pullable:"; for b in $bad; do echo "  $b"; done; } >&2
  exit 1
fi
echo "All datastore images anonymously pullable."
