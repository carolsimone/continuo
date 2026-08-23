#!/usr/bin/env bash
# Renamed services (2026-08): ui-service, agent-runner, remediation-agent,
# manifest-controller no longer exist. Old names may appear only in immutable
# history: Flyway migrations, the chart changelog, superpowers docs, and the
# NOTES.txt upgrade-warning block (which intentionally names old->new for operators).
set -euo pipefail
cd "$(dirname "$0")/.."
if git grep -inE "ui-service|agent-runner|remediation-agent|manifest-controller" -- \
    ':!db/migration' ':!deploy/continuo/CHANGELOG.md' ':!docs/superpowers' \
    ':!deploy/continuo/templates/NOTES.txt' ':!scripts/check-service-names.sh'; then
  echo "ERROR: retired service name found (see matches above)" >&2
  exit 1
fi
