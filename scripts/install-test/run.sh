#!/usr/bin/env bash
# Install the deploy/continuo chart into the CURRENT kubectl context (a
# throwaway kind cluster in CI) and verify the release comes up healthy.
#
# Usage: run.sh <bundled|byo-inline|byo-secret> <image-tag>
#   bundled     quickstart defaults: bundled Postgres/Redis/Neo4j/MinIO/Dex
#   byo-inline  external datastores, credentials inline in values
#   byo-secret  external datastores, credentials from a pre-created Secret
#
# The image tag names published ghcr images (linux/amd64): PR runs pass
# "latest", release runs pass the vX.Y.Z tag being published.
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/lib/common.sh

MODE="${1:?usage: run.sh <bundled|byo-inline|byo-secret> <image-tag>}"
IMAGE_TAG="${2:?usage: run.sh <bundled|byo-inline|byo-secret> <image-tag>}"
NS=continuo
RELEASE=continuo
CHART=deploy/continuo
FIXTURES=scripts/install-test

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

helm_args=(--set "global.imageTag=${IMAGE_TAG}")
case "$MODE" in
  bundled) ;;
  byo-inline|byo-secret)
    # Stand up the "existing infrastructure" the BYO install points at. It
    # must be READY before helm install: with postgresql.enabled=false the
    # db-init-migrate Job is a pre-install hook and connects immediately.
    kubectl -n "$NS" apply -f "${FIXTURES}/byo-datastores.yaml"
    kubectl -n "$NS" rollout status deployment/byo-postgres --timeout=180s
    kubectl -n "$NS" rollout status deployment/byo-redis --timeout=180s
    kubectl -n "$NS" rollout status deployment/byo-neo4j --timeout=300s
    if [ "$MODE" = byo-secret ]; then
      kubectl -n "$NS" create secret generic continuo-datastores \
        --from-literal=postgres-password=ci-postgres-pw \
        --from-literal=redis-password=ci-redis-pw \
        --from-literal=neo4j-password=ci-neo4j-pw \
        --dry-run=client -o yaml | kubectl apply -f -
      helm_args+=(-f "${FIXTURES}/values-byo-secret.yaml")
    else
      helm_args+=(-f "${FIXTURES}/values-byo-inline.yaml")
    fi
    ;;
  *)
    log_error "unknown mode: ${MODE}"
    exit 2
    ;;
esac

log_info "helm install (${MODE}, images :${IMAGE_TAG})..."
# --wait-for-jobs: the bundled-mode db-init-migrate Job and the MinIO
# bucket-init Job are regular resources (not hooks), so plain --wait would
# report success before they finish.
helm install "$RELEASE" "$CHART" -n "$NS" "${helm_args[@]}" \
  --wait --wait-for-jobs --timeout 20m

kubectl -n "$NS" get pods

# Every pod must be Running or Succeeded (completed Jobs). Anything Pending/
# Failed/CrashLoopBackOff at this point is an install failure --wait let slip
# through (e.g. an unschedulable extra replica).
not_healthy="$(kubectl -n "$NS" get pods \
  --field-selector=status.phase!=Running,status.phase!=Succeeded \
  --no-headers 2>/dev/null || true)"
if [ -n "$not_healthy" ]; then
  log_error "pods not Running/Succeeded after install:"
  echo "$not_healthy" >&2
  exit 1
fi

if [ "$MODE" = bundled ]; then
  # Bundled mode runs db-init-migrate as a regular revision-suffixed Job.
  # (In BYO mode it is a pre-install hook: helm already blocked on it and
  # its hook-succeeded delete policy removed it, so there is nothing left
  # to check — helm install succeeding IS the migration check there.)
  kubectl -n "$NS" wait --for=condition=complete \
    job -l app.kubernetes.io/name=db-init-migrate --timeout=60s
fi

# End-to-end health endpoints through port-forwards from this host: the same
# unauthenticated /healthz the ui-service readiness probe hits, plus the
# bundled Dex's OIDC discovery document (proves the IdP answers at the issuer
# ui-service is configured to trust). High local ports avoid colima/dev
# listeners when running outside CI.
kubectl -n "$NS" port-forward svc/ui-service 18090:8090 >/dev/null 2>&1 &
pf_ui=$!
kubectl -n "$NS" port-forward "svc/${RELEASE}-dex" 15556:5556 >/dev/null 2>&1 &
pf_dex=$!
trap 'kill "$pf_ui" "$pf_dex" 2>/dev/null || true' EXIT

wait_for_http_host http://127.0.0.1:18090/healthz 15 2
wait_for_http_host http://127.0.0.1:15556/dex/.well-known/openid-configuration 15 2
curl -sf http://127.0.0.1:15556/dex/.well-known/openid-configuration \
  | grep -q "\"issuer\": *\"http://${RELEASE}-dex:5556/dex\""

log_info "install-test (${MODE}) OK"
