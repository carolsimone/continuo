#!/usr/bin/env bash
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S_DIR="${SCRIPT_DIR}/k8s"

log_info "Starting K8s controllers setup for E2E tests..."

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

log_info "Building controller images..."

cd "${SCRIPT_DIR}/.."

log_info "Building executor-controller image"
docker build -f executor-controller/Dockerfile.dev -t continuo-executor-controller:latest . || {
  log_error "failed to build executor-controller image"
  exit 1
}

log_info "Building k8s-controller image"
docker build -f k8s-controller/Dockerfile.dev -t continuo-k8s-controller:latest . || {
  log_error "failed to build k8s-controller image"
  exit 1
}

log_info "Building dbt service images..."
DOCKER_BUILDKIT=1 docker build -t service-1:latest dbt/services/service-1/ || { log_error "failed to build service-1"; exit 1; }
DOCKER_BUILDKIT=1 docker build -t service-2:latest dbt/services/service-2/ || { log_error "failed to build service-2"; exit 1; }
DOCKER_BUILDKIT=1 docker build -t service-3:latest dbt/services/service-3/ || { log_error "failed to build service-3"; exit 1; }
DOCKER_BUILDKIT=1 docker build -t service-3-broken:latest dbt/services/service-3-broken/ || { log_error "failed to build service-3-broken"; exit 1; }

log_info "Load images into k8s/kind..."

kind load docker-image continuo-executor-controller:latest --name continuo || {
  log_error "Failed to load executor-controller image into kind"
  exit 1
}

kind load docker-image continuo-k8s-controller:latest --name continuo || {
  log_error "Failed to load k8s-controller image into kind"
}

log_info "Loading dbt service images into kind..."
kind load docker-image service-1:latest --name continuo || { log_error "Failed to load service-1 into kind"; exit 1; }
kind load docker-image service-2:latest --name continuo || { log_error "Failed to load service-2 into kind"; exit 1; }
kind load docker-image service-3:latest --name continuo || { log_error "Failed to load service-3 into kind"; exit 1; }
kind load docker-image service-3-broken:latest --name continuo || { log_error "Failed to load service-3-broken into kind"; exit 1; }

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

mkdir -p "${SCRIPT_DIR}/../kubeconfig"
kubectl config view --raw \
    | sed "s|server: https://[^:]*:[0-9]*|server: https://${KUBE_IP}:${KUBE_PORT}|g" \
    > "${SCRIPT_DIR}/../kubeconfig/kubeconfig.yaml"

for svc in executor-controller; do
    mkdir -p "${SCRIPT_DIR}/../${svc}/kubeconfig"
    cp "${SCRIPT_DIR}/../kubeconfig/kubeconfig.yaml" "${SCRIPT_DIR}/../${svc}/kubeconfig/kubeconfig.yaml"
    log_info "Kubeconfig copied to ${svc}/"
done

log_info "Applying k8s manifests..."

# Step 4: Apply K8s manifests with environment variable substitution
log_info "Applying K8s manifests..."

cd "${K8S_DIR}"

# Apply executor-controller
log_info "Deploying executor-controller..."
envsubst < executor-controller-deployment.yaml | kubectl apply -f - || {
    log_error "Failed to apply executor-controller manifest"
    exit 1
}

# Apply k8s-controller
log_info "Deploying k8s-controller..."
envsubst < k8s-controller-deployment.yaml | kubectl apply -f - || {
    log_error "Failed to apply k8s-controller manifest"
    exit 1
}

# Step 4.5: Force rollout restart to use new images
log_info "Restarting deployments to use new images..."
kubectl rollout restart deployment/executor-controller deployment/k8s-controller -n default || {
    log_error "Failed to restart deployments"
    exit 1
}

# Step 5: Wait for deployments to be ready
log_info "Waiting for deployments to be ready (timeout: 120s)..."

kubectl wait --for=condition=available --timeout=120s deployment/executor-controller -n default || {
    log_error "executor-controller deployment did not become ready"
    log_info "Pod logs:"
    kubectl logs -l app=executor-controller -n default --tail=50 || true
    exit 1
}

kubectl wait --for=condition=available --timeout=120s deployment/k8s-controller -n default || {
    log_error "k8s-controller deployment did not become ready"
    log_info "Pod logs:"
    kubectl logs -l app=k8s-controller -n default --tail=50 || true
    exit 1
}

log_info "Deployments are ready"

log_info "Verifying controller health endpoints..."

# Function to check health for all services
check_health() {
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

check_health "executor-controller" "8084" "executor-controller" || {
    log_error "executor-controller health check failed"
    kubectl logs -l app=executor-controller -n default --tail=50
    exit 1
}

check_health "k8s-controller" "8085" "k8s-controller" || {
    log_error "k8s-controller health check failed"
    kubectl logs -l app=k8s-controller -n default --tail=50
    exit 1
}

log_info "All health checks passed!"