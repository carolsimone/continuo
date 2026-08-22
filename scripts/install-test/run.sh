#!/usr/bin/env bash
# Install the continuo chart into the CURRENT kubectl context (a throwaway
# kind cluster in CI) and verify the release comes up healthy.
#
# Usage: run.sh <bundled|byo-inline|byo-secret> <image-tag|chart-default>
#   bundled     quickstart defaults: bundled Postgres/Redis/Neo4j/MinIO/Dex
#   byo-inline  external datastores, credentials inline in values
#   byo-secret  external datastores, credentials from a pre-created Secret
#
# The image tag names published ghcr images (linux/amd64): PR runs pass
# "latest", release runs pass the vX.Y.Z tag being published. The literal
# "chart-default" passes no global.imageTag at all, so the chart resolves
# every image from its own Chart.AppVersion instead — the path a user gets
# when they install the published chart without overriding anything.
#
# Environment:
#   CHART          chart to install; defaults to the in-repo directory. Set to
#                  an oci:// reference to install a published package instead.
#   CHART_VERSION  chart version to install; required for an oci:// CHART,
#                  meaningless for the directory (which carries a placeholder).
set -euo pipefail
cd "$(dirname "$0")/../.."
source scripts/lib/common.sh

MODE="${1:?usage: run.sh <bundled|byo-inline|byo-secret> <image-tag|chart-default>}"
IMAGE_TAG="${2:?usage: run.sh <bundled|byo-inline|byo-secret> <image-tag|chart-default>}"
NS=continuo
RELEASE=continuo
CHART="${CHART:-deploy/continuo}"
FIXTURES=scripts/install-test

kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

# values-ci-resources.yaml only shrinks requests/limits so the release fits a
# CI node; it does not touch image selection, so the AppVersion path below is
# still exercised exactly as a user would get it.
helm_args=(-f "${FIXTURES}/values-ci-resources.yaml")
if [ "$IMAGE_TAG" = "chart-default" ]; then
  log_info "no global.imageTag override: images resolve from the chart's AppVersion"
else
  helm_args+=(--set "global.imageTag=${IMAGE_TAG}")
fi
if [ -n "${CHART_VERSION:-}" ]; then
  helm_args+=(--version "${CHART_VERSION}")
fi
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
      # values-byo-secret sets validation.createWarehouseSecret=false, so the chart
      # does not create this — the operator (here, the fixture) provides it with the
      # postgres runner's POSTGRES_* keys, or the first validation pod would fail with
      # CreateContainerConfigError.
      kubectl -n "$NS" create secret generic continuo-warehouse-validation \
        --from-literal=POSTGRES_HOST=byo-postgres \
        --from-literal=POSTGRES_PORT=5432 \
        --from-literal=POSTGRES_DB=continuo_dbt \
        --from-literal=POSTGRES_USER=continuo \
        --from-literal=POSTGRES_PASSWORD=ci-postgres-pw \
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

# Every pod must be Running or Succeeded (completed Jobs). Anything Pending
# or Failed at this point slipped through --wait; crashlooping pods keep
# phase=Running and are caught earlier by --wait's readiness gate.
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
# unauthenticated /healthz the ui readiness probe hits, plus the
# bundled Dex's OIDC discovery document (proves the IdP answers at the issuer
# ui is configured to trust). High local ports avoid colima/dev
# listeners when running outside CI.
kubectl -n "$NS" port-forward svc/ui 18090:8090 >/dev/null 2>&1 &
pf_ui=$!
kubectl -n "$NS" port-forward "svc/${RELEASE}-dex" 15556:5556 >/dev/null 2>&1 &
pf_dex=$!
trap 'kill "$pf_ui" "$pf_dex" 2>/dev/null || true' EXIT

wait_for_http_host http://127.0.0.1:18090/healthz 15 2
wait_for_http_host http://127.0.0.1:15556/dex/.well-known/openid-configuration 15 2
grep -q "\"issuer\": *\"http://${RELEASE}-dex:5556/dex\"" \
  <<<"$(curl -sf http://127.0.0.1:15556/dex/.well-known/openid-configuration)"

log_info "install-test (${MODE}) OK"
