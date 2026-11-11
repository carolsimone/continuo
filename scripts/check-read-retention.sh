#!/usr/bin/env bash
# Fails if a compose file installs an object-store expiry rule on `code-bundles/`
# or `tenants/`. Those prefixes hold the code bundle and the topology artifact
# that external consumers read for the release GET /api/v1/current-prod names
# (docs/read-contract.md). An age rule cannot tell the live release from an old
# one, so a release that stays live past the rule's age would lose its objects.
# release-controller's retention deletes them, and never for a release that is
# still current_prod or service_prod.
#
# Usage: check-read-retention.sh [compose-file...]   (default: the repo's compose files)
set -uo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -eq 0 ]; then set -- docker-compose.yml docker-compose.ci.yml; fi
rc=0
for f in "$@"; do
  [ -f "$f" ] || continue
  if grep -nE '"Prefix": *"(code-bundles|tenants)/".*"Expiration"' "$f" >&2; then
    echo "RETENTION VIOLATION in ${f}: an expiry rule on code-bundles/ or tenants/ can delete the objects of the live release (release-controller's retention owns their deletion)" >&2
    rc=1
  fi
done
[ "$rc" -eq 0 ] && echo "read-retention OK: no expiry rule on code-bundles/ or tenants/"
exit "$rc"
