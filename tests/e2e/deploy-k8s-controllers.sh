#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../scripts/lib/common.sh"

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

# Step 5: Wait for rollout to fully complete (all old pods terminated, all new
# pods Ready). `rollout status` is stricter than `wait --for=condition=available`,
# which only requires minReplicas Ready and can return while old pods are still
# terminating — leaving a window where `kubectl port-forward deployment/...`
# from the e2e suite can attach to a dying pod, producing a "K8s service
# unhealthy after port-forward" failure on the first test that runs.
log_info "Waiting for rollout to complete (timeout: 120s)..."

kubectl rollout status deployment/executor-controller -n default --timeout=120s || {
    log_error "executor-controller rollout did not complete"
    log_info "Pod logs:"
    kubectl logs -l app=executor-controller -n default --tail=50 || true
    exit 1
}

kubectl rollout status deployment/k8s-controller -n default --timeout=120s || {
    log_error "k8s-controller rollout did not complete"
    log_info "Pod logs:"
    kubectl logs -l app=k8s-controller -n default --tail=50 || true
    exit 1
}

log_info "Rollouts complete (no terminating pods, all new pods Ready)"

# rollout status passing implies the readinessProbe (which hits /health) has
# succeeded for every new pod and no old pods remain.
log_info "Controllers verified healthy via readinessProbe (kubectl rollout status passed)"