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

# The agents reach the outside world only through egress-proxy, and the kind
# cluster enforces NetworkPolicy, so check it from the agent pods: the proxy
# admits an allowlisted host and refuses another, and a direct connection to a
# public address never completes. The check is skipped only when the
# install's own values set egress.enabled=false or networkPolicy.enabled=false.
# Otherwise the release's rendered manifest must hold the agents' egress policy
# and the cluster must hold every egress object the manifest renders; the proxy
# checks run when the manifest renders egress-proxy (it renders none when its
# allowlist would be empty, which no mode of this script produces).
#
# Every probe prints a sentinel and the check requires it, so a probe that
# itself fails (a missing tool, a failed exec) is reported as such and never
# counts as a blocked connection. agent-chat's alpine image carries busybox nc;
# agent-remediation's python image has no nc, so it probes with python3 sockets.
probe_failed() {
  log_error "egress probe failed: $1 (got '${2}'); the probe itself did not run"
  exit 1
}

# proxy_status <host:port>: the status code egress-proxy answers a CONNECT from
# agent-chat. The request is followed by a sleep that keeps stdin open: when it
# closes, nc half-closes the socket at once, egress-proxy sees the client go
# and abandons the dial before it can answer 200 for an admitted host. The
# sleep (8 s) leaves the proxy most of its 10 s dial timeout.
proxy_status() {
  local out
  out="$(kubectl -n "$NS" exec deploy/agent-chat -- sh -c \
    "(printf 'CONNECT $1 HTTP/1.1\r\nHost: $1\r\n\r\n'; sleep 8) | nc -w 10 egress-proxy 3128 | head -n1")" \
    || probe_failed "kubectl exec into agent-chat for CONNECT $1" ""
  awk '{print $2}' <<<"$out"
}

# agent_chat_dial <host> <port>: connected|blocked, from agent-chat.
agent_chat_dial() {
  kubectl -n "$NS" exec deploy/agent-chat -- sh -c \
    "nc -w 5 $1 $2 </dev/null >/dev/null 2>&1 && echo connected || echo blocked"
}

# agent_remediation_dial <host> <port>: connected|blocked, from agent-remediation.
agent_remediation_dial() {
  kubectl -n "$NS" exec deploy/agent-remediation -- python3 -c '
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), 5).close()
    print("connected")
except OSError:
    print("blocked")' "$1" "$2"
}

# egress_values_off: prints "off" when the install's own values (helm get
# values: the -f files and --set flags this script passed) set
# egress.enabled=false or networkPolicy.enabled=false, else "on".
egress_values_off() {
  helm get values "$RELEASE" -n "$NS" -o json | python3 -c '
import json, sys
values = json.load(sys.stdin) or {}
off = (values.get("egress") or {}).get("enabled") is False \
    or (values.get("networkPolicy") or {}).get("enabled") is False
print("off" if off else "on")'
}

# rendered <kind> <name>: whether the release's rendered manifest ($manifest)
# holds that object.
rendered() {
  awk -v kind="$1" -v name="$2" '
    /^---/ { k = 0; n = 0; next }
    $0 == "kind: " kind { k = 1 }
    $0 == "  name: " name { n = 1 }
    k && n { found = 1 }
    END { exit found ? 0 : 1 }' "$manifest"
}

# require_object <kind> <name>: the object exists in the namespace. A read that
# fails for another reason than NotFound is retried; NotFound, or a read that
# keeps failing, fails the run.
require_object() {
  local err attempt
  for attempt in 1 2 3 4 5; do
    if err="$(kubectl -n "$NS" get "$1" "$2" -o name 2>&1 >/dev/null)"; then
      return 0
    fi
    if grep -q NotFound <<<"$err"; then
      log_error "the release renders $1/$2 but the cluster holds none: ${err}"
      exit 1
    fi
    log_info "reading $1/$2 failed (try ${attempt}/5): ${err}"
    sleep 3
  done
  log_error "could not read $1/$2 after 5 tries: ${err}"
  exit 1
}

if [ "$(egress_values_off)" = off ]; then
  log_info "this install's values set egress.enabled=false or networkPolicy.enabled=false: skipping the egress enforcement check"
else
  manifest="$(mktemp)"
  helm get manifest "$RELEASE" -n "$NS" > "$manifest"
  rendered NetworkPolicy "${RELEASE}-agents-egress" \
    || { log_error "egress and networkPolicy are on, but the release renders no ${RELEASE}-agents-egress NetworkPolicy"; exit 1; }
  require_object networkpolicy "${RELEASE}-agents-egress"
  log_info "checking agent egress enforcement..."
  # Positive control: the python probe reaches an address inside the release,
  # so a "blocked" below means a drop and not a broken python3 or exec.
  control_host=release-controller control_port=8088
  if rendered Service egress-proxy; then
    require_object service egress-proxy
    require_object networkpolicy "${RELEASE}-egress-proxy-egress"
    require_object networkpolicy "${RELEASE}-allow-egress-proxy"
    status="$(proxy_status api.github.com:443)" || exit 1
    [ "$status" = 200 ] \
      || { log_error "egress-proxy answered '${status}' for api.github.com:443, which is on the default allowlist (want 200)"; exit 1; }
    status="$(proxy_status example.com:443)" || exit 1
    [ "$status" = 403 ] \
      || { log_error "egress-proxy answered '${status}' for example.com:443, which is not on the allowlist (want 403)"; exit 1; }
    control_host=egress-proxy control_port=3128
  else
    log_info "the release renders no egress-proxy (its allowlist is empty): checking only that direct connections are dropped"
  fi
  got="$(agent_remediation_dial "$control_host" "$control_port")" || probe_failed "kubectl exec into agent-remediation" ""
  [ "$got" = connected ] || probe_failed "agent-remediation could not reach ${control_host}:${control_port} in the release" "$got"

  got="$(agent_chat_dial 1.1.1.1 443)" || probe_failed "kubectl exec into agent-chat for a direct dial" ""
  case "$got" in
    blocked) ;;
    connected) log_error "agent-chat opened a direct connection to a public address (1.1.1.1:443)"; exit 1 ;;
    *) probe_failed "agent-chat direct dial" "$got" ;;
  esac
  got="$(agent_remediation_dial 1.1.1.1 443)" || probe_failed "kubectl exec into agent-remediation for a direct dial" ""
  case "$got" in
    blocked) ;;
    connected) log_error "agent-remediation opened a direct connection to a public address (1.1.1.1:443)"; exit 1 ;;
    *) probe_failed "agent-remediation direct dial" "$got" ;;
  esac
  rm -f "$manifest"
fi

log_info "install-test (${MODE}) OK"
