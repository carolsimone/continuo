#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../scripts/lib/common.sh"

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
K8S_DIR="${SCRIPT_DIR}/k8s"

log_info "Starting K8s controller setup for E2E tests..."

# Step 1: Detect docker bridge IP
log_info "Detecting docker bridge IP..."
DOCKER_BRIDGE_IP=$(docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || echo "")

if [ -z "$DOCKER_BRIDGE_IP" ]; then
    log_error "Failed to detect docker bridge IP"
    exit 1
fi

log_info "Docker bridge IP: ${DOCKER_BRIDGE_IP}"

# Export for envsubst
export DOCKER_BRIDGE_IP

log_info "Building controller image..."

cd "${REPO_ROOT}"

log_info "Building execution-controller image"
docker build -f execution-controller/Dockerfile.dev -t continuo-execution-controller:latest . || {
  log_error "failed to build execution-controller image"
  exit 1
}

log_info "Building dbt base image..."
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest dbt/base/ || { log_error "failed to build dbt-base"; exit 1; }
log_info "Building s3-sidecar image..."
DOCKER_BUILDKIT=1 docker build -t s3-sidecar:latest s3-sidecar/ || { log_error "failed to build s3-sidecar"; exit 1; }
log_info "Pulling continuo-python-runtime (postgres) image..."
docker pull ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.5.0 \
  || { log_error "failed to pull continuo-python-runtime-postgres"; exit 1; }

log_info "Loading controller image into kind..."
kind load docker-image continuo-execution-controller:latest --name continuo || {
  log_error "Failed to load execution-controller image into kind"
  exit 1
}
kind load docker-image dbt-base:latest --name continuo || { log_error "failed to load dbt-base into kind"; exit 1; }
kind load docker-image s3-sidecar:latest --name continuo || { log_error "failed to load s3-sidecar into kind"; exit 1; }
# The validation image is pulled, not built locally, so a plain `kind load
# docker-image` fails on a containerd-backed image store — see
# scripts/lib/common.sh:kind_load_pulled_image.
kind_load_pulled_image ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.5.0 continuo || exit 1

# Build each dbt service image, tag it by its own content digest (never :latest),
# and load that tag into kind. The controller composes the dbt job image as
# service-<n>:<image_tag>, so the kind image must be loaded under exactly the tag
# the e2e seeds into service_prod. The per-service tags are written to the
# bind-mounted tests/e2e/.image-tags file (immutable setup metadata); the e2e
# rebuilds the service_prod baseline from it before each consumer.
log_info "Building + loading dbt service images (content-hash tags)..."
DBT_PER_SERVICE=""
for svc in service-1 service-2 service-3; do
  DOCKER_BUILDKIT=1 docker build -f "dbt/services/${svc}/Dockerfile.local" -t "${svc}:build-tmp" "dbt/services/${svc}/" \
    || { log_error "failed to build ${svc}"; exit 1; }
  img_id="$(docker image inspect --format '{{.Id}}' "${svc}:build-tmp")"
  tag="${img_id#sha256:}"; tag="${tag:0:12}"
  docker tag "${svc}:build-tmp" "${svc}:${tag}"
  log_info "  ${svc} -> ${svc}:${tag}"
  kind load docker-image "${svc}:${tag}" --name continuo \
    || { log_error "Failed to load ${svc} into kind"; exit 1; }
  DBT_PER_SERVICE="${DBT_PER_SERVICE:+${DBT_PER_SERVICE},}${svc}=${tag}"
done
printf '%s' "$DBT_PER_SERVICE" > "${REPO_ROOT}/tests/e2e/.image-tags"
log_info "Wrote per-service image tags to tests/e2e/.image-tags: ${DBT_PER_SERVICE}"

log_info "Images built and loaded successfully"

# Regenerate kubeconfig with the current kind API server address so that
# service containers can reach the cluster (bind-mounted from host).
log_info "Regenerating kubeconfig for service containers..."
CLUSTER_NAME="continuo"
KUBE_IP=$(docker exec ${CLUSTER_NAME}-control-plane kubectl get endpoints kubernetes -n default -o jsonpath='{.subsets[0].addresses[0].ip}')
KUBE_PORT=$(docker exec ${CLUSTER_NAME}-control-plane kubectl get endpoints kubernetes -n default -o jsonpath='{.subsets[0].ports[0].port}')

if [ -z "$KUBE_IP" ]; then
    log_error "Could not determine Kubernetes API server IP"
    exit 1
fi

log_info "Kubernetes API server: ${KUBE_IP}:${KUBE_PORT}"

mkdir -p "${REPO_ROOT}/kubeconfig"
kubectl config view --raw \
    | sed "s|server: https://[^:]*:[0-9]*|server: https://${KUBE_IP}:${KUBE_PORT}|g" \
    > "${REPO_ROOT}/kubeconfig/kubeconfig.yaml"

for svc in execution-controller; do
    mkdir -p "${REPO_ROOT}/${svc}/kubeconfig"
    cp "${REPO_ROOT}/kubeconfig/kubeconfig.yaml" "${REPO_ROOT}/${svc}/kubeconfig/kubeconfig.yaml"
    log_info "Kubeconfig copied to ${svc}/"
done

# Step 4: Apply K8s manifests with environment variable substitution
log_info "Applying K8s manifests..."

cd "${K8S_DIR}"

# Apply the validation warehouse Secret before the controller so the validation
# pods it launches can resolve their warehouse credentials via envFrom.
log_info "Creating validation warehouse Secret..."
envsubst < validation-warehouse-secret.yaml | kubectl apply -f - || {
    log_error "Failed to apply validation warehouse Secret"
    exit 1
}

# Remove any obsolete executor-controller / k8s-controller resources left over
# from a kind cluster provisioned before the merge. They share this service's
# consumer groups, so a leftover controller could consume a candidate completion,
# find no matching deployment, and ACK it without settling the new release.
# --wait blocks until the old pods terminate; --ignore-not-found makes this a
# no-op on a fresh cluster.
log_info "Removing any obsolete executor-controller/k8s-controller resources..."
for old in executor-controller k8s-controller; do
    kubectl delete deployment,service,serviceaccount,role,rolebinding "${old}" \
        -n default --ignore-not-found --wait --timeout=60s || true
done

# Apply execution-controller
log_info "Deploying execution-controller..."
envsubst < execution-controller-deployment.yaml | kubectl apply -f - || {
    log_error "Failed to apply execution-controller manifest"
    exit 1
}

# Step 4.5: Force rollout restart to use the new image
log_info "Restarting deployment to use new image..."
kubectl rollout restart deployment/execution-controller -n default || {
    log_error "Failed to restart deployment"
    exit 1
}

# Step 5: Wait for deployment to be ready
log_info "Waiting for deployment to be ready (timeout: 120s)..."

kubectl wait --for=condition=available --timeout=120s deployment/execution-controller -n default || {
    log_error "execution-controller deployment did not become ready"
    log_info "Pod logs:"
    kubectl logs -l app=execution-controller -n default --tail=50 || true
    exit 1
}

log_info "Deployment is ready"

log_info "Verifying controller health endpoint..."

# Function to check health for a k8s-deployed service via kubectl port-forward
check_k8s_health() {
  local deployment=$1
  local port=$2
  local service_name=$3

  log_info "Checking ${service_name} health..."

  kubectl port-forward deployment/${deployment} ${port}:${port} -n default >/dev/null 2>&1 &
  local PF_PID=$!

  # Wait for port-forward to be ready
  sleep 2

  # Check health endpoint
  local http_code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:${port}/health || echo "000")

  # kill port-forward after checking
  kill ${PF_PID} 2>/dev/null || true
  wait ${PF_PID} 2>/dev/null || true

  if [ "${http_code}" = "200" ]; then
    log_info "${service_name} health check: OK (${http_code})"
    return 0
  else
    log_error "${service_name} health check: FAILED (${http_code})"
  fi
}

check_k8s_health "execution-controller" "8084" "execution-controller" || {
    log_error "execution-controller health check failed"
    kubectl logs -l app=execution-controller -n default --tail=50
    exit 1
}

log_info "All health checks passed!"
