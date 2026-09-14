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

log_info "Cleaning up K8s controller..."

# Delete deployment and service
log_info "Deleting execution-controller deployment and service..."
kubectl delete deployment execution-controller -n default --ignore-not-found=true

kubectl delete service execution-controller -n default --ignore-not-found=true

# Delete RBAC resources
log_info "Deleting RBAC resources..."
kubectl delete serviceaccount execution-controller -n default --ignore-not-found=true
kubectl delete role execution-controller -n default --ignore-not-found=true
kubectl delete rolebinding execution-controller -n default --ignore-not-found=true

# Delete any test jobs (cleanup test data)
log_info "Deleting test jobs..."
kubectl delete jobs -n default -l schedule=e2e-schedule --ignore-not-found=true || true

log_info "Cleanup complete!"
