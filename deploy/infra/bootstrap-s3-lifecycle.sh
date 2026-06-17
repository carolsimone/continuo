#!/usr/bin/env bash
# bootstrap-s3-lifecycle.sh
#
# Applies the 30-day expiry lifecycle rules (deploy/infra/s3-lifecycle.json)
# to the continuo S3 bucket.
#
# Run this once per environment when the bucket is first provisioned.
# The bucket is NOT managed by the Helm chart in deploy/infra/ (which only
# provisions Postgres, Redis, and Neo4j), so this script is the authoritative
# one-time bootstrap step.
#
# For local dev (LocalStack) the same rules are applied automatically on every
# startup via localstack/init/init-s3.sh — this script is not needed there.
#
# Usage:
#   BUCKET=continuo S3_ENDPOINT_URL=https://s3.hetzner.example.com \
#     ./deploy/infra/bootstrap-s3-lifecycle.sh
#
# Environment variables:
#   BUCKET             — bucket name (default: continuo)
#   S3_ENDPOINT_URL    — custom endpoint URL for non-AWS S3 (optional)
#   AWS_*              — standard AWS credential env vars consumed by the CLI

set -euo pipefail

BUCKET="${BUCKET:-continuo}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LIFECYCLE_FILE="${SCRIPT_DIR}/s3-lifecycle.json"

if [[ ! -f "${LIFECYCLE_FILE}" ]]; then
  echo "ERROR: lifecycle config not found at ${LIFECYCLE_FILE}" >&2
  exit 1
fi

ENDPOINT_ARGS=()
if [[ -n "${S3_ENDPOINT_URL:-}" ]]; then
  ENDPOINT_ARGS=(--endpoint-url "${S3_ENDPOINT_URL}")
fi

echo "Applying S3 lifecycle rules from ${LIFECYCLE_FILE} to bucket '${BUCKET}'..."
aws s3api put-bucket-lifecycle-configuration \
  "${ENDPOINT_ARGS[@]}" \
  --bucket "${BUCKET}" \
  --lifecycle-configuration "file://${LIFECYCLE_FILE}"

echo "Done. Verifying..."
aws s3api get-bucket-lifecycle-configuration \
  "${ENDPOINT_ARGS[@]}" \
  --bucket "${BUCKET}"
