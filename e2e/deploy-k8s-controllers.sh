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

# Images are pre-built and loaded into kind by scripts/setup.sh
log_info "Applying k8s manifests..."

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

# kubectl wait --for=condition=available already verified the readinessProbe
# (which hits /health) for both deployments above, so the controllers are healthy.
log_info "Controllers verified healthy via readinessProbe (kubectl wait passed)"