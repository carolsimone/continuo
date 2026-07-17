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

log_info "Cleaning up K8s controllers..."

# Delete deployments and services
log_info "Deleting executor-controller deployment and service..."
kubectl delete deployment executor-controller -n default --ignore-not-found=true

kubectl delete service executor-controller -n default --ignore-not-found=true

log_info "Deleting k8s-controller deployment and service..."
kubectl delete deployment k8s-controller -n default --ignore-not-found=true

kubectl delete service k8s-controller -n default --ignore-not-found=true

# Delete RBAC resources
log_info "Deleting RBAC resources..."
kubectl delete serviceaccount executor-controller -n default --ignore-not-found=true
kubectl delete role executor-controller -n default --ignore-not-found=true
kubectl delete rolebinding executor-controller -n default --ignore-not-found=true

kubectl delete serviceaccount k8s-controller -n default --ignore-not-found=true
kubectl delete role k8s-controller -n default --ignore-not-found=true
kubectl delete rolebinding k8s-controller -n default --ignore-not-found=true

# Delete any test jobs (cleanup test data)
log_info "Deleting test jobs..."
kubectl delete jobs -n default -l schedule=e2e-schedule --ignore-not-found=true || true

# Delete the worker pools the executor created. A pool owns a Deployment, the
# Secret holding its credential, and the pods the Deployment runs; all three
# carry the same app label whichever pool they belong to. They outlive the
# executor that made them, so a run that left pools behind would hand the next
# one pods holding credentials no live pool row can authenticate.
log_info "Deleting dbt worker pools..."
kubectl delete deployment,secret,pod -n default \
    -l app=continuo-dbt-worker --ignore-not-found=true || true

log_info "Cleanup complete!"