#!/bin/bash
set -eo pipefail

CLUSTER_NAME="continuo"
KIND_VERSION="v0.30.0"

# Ensure 'docker compose' plugin is available (macOS/Colima ships only the
# standalone docker-compose binary; CI runners ship only the plugin).
if ! docker compose version &>/dev/null 2>&1; then
    STANDALONE=$(command -v docker-compose 2>/dev/null || true)
    if [ -n "$STANDALONE" ]; then
        mkdir -p ~/.docker/cli-plugins
        ln -sf "$STANDALONE" ~/.docker/cli-plugins/docker-compose
        echo "Linked $STANDALONE as docker compose plugin"
    else
        echo "Error: neither 'docker compose' nor 'docker-compose' found" >&2
        exit 1
    fi
fi

# Install kind if not present
if ! command -v kind &> /dev/null; then
    echo "Installing kind ${KIND_VERSION}..."
    if [[ "$(uname)" == "Darwin" ]]; then
        if command -v brew &> /dev/null; then
            brew install kind
        else
            sudo curl -Lo /usr/local/bin/kind https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-darwin-arm64
            sudo chmod +x /usr/local/bin/kind
        fi
    else
        # Linux (CI runners, etc.)
        sudo curl -Lo /usr/local/bin/kind https://kind.sigs.k8s.io/dl/${KIND_VERSION}/kind-linux-amd64
        sudo chmod +x /usr/local/bin/kind
    fi
fi

# ── Parallelise kind creation and Docker image builds ────────────────────────
# kind cluster creation (~3-4 min) and Docker image builds (~3-5 min) are
# independent, so run them concurrently to halve total wall-clock time.

# Start kind cluster in the background if it doesn't already exist
KIND_PID=""
if ! kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    echo "Creating kind cluster '${CLUSTER_NAME}' in background..."
    kind create cluster --name ${CLUSTER_NAME} --image kindest/node:v1.34.0 -v 3 --wait 5m --retain &
    KIND_PID=$!
else
    echo "Kind cluster '${CLUSTER_NAME}' already exists."
fi

# Build Docker images while the kind cluster creates.
# In CI, continuo-base is pre-built with layer cache by docker/build-push-action
# before this script runs; skip building it again if it's already present.
if ! docker image inspect continuo-base:latest > /dev/null 2>&1; then
    echo "Building continuo-base image..."
    DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .
fi
# dbt-base must exist before docker compose build because dbt-compile-and-load
# depends on it and is now in the default (non-profiled) service set.
echo "Building dbt base image..."
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest dbt/base/
echo "Building service images..."
DOCKER_BUILDKIT=1 docker compose build

# Build dbt service images and load into KIND

echo "Building dbt service images..."
DOCKER_BUILDKIT=1 docker build -f dbt/services/service-1/Dockerfile.local -t service-1:latest dbt/services/service-1/
DOCKER_BUILDKIT=1 docker build -f dbt/services/service-2/Dockerfile.local -t service-2:latest dbt/services/service-2/
DOCKER_BUILDKIT=1 docker build -f dbt/services/service-3/Dockerfile.local -t service-3:latest dbt/services/service-3/

# continuo-executor-controller and continuo-k8s-controller are already built by
# 'docker compose build' above with the correct tags, so no need to rebuild them here.

# Wait for kind cluster to finish (if we started it above)
if [ -n "$KIND_PID" ]; then
    echo "Waiting for kind cluster creation to complete..."
    wait $KIND_PID
    echo "✓ Kind cluster ready"
fi
echo "Loading images into kind (parallel)..."
kind load docker-image service-1:latest --name ${CLUSTER_NAME} &
kind load docker-image service-2:latest --name ${CLUSTER_NAME} &
kind load docker-image service-3:latest --name ${CLUSTER_NAME} &
kind load docker-image continuo-executor-controller:latest --name ${CLUSTER_NAME} &
kind load docker-image continuo-k8s-controller:latest --name ${CLUSTER_NAME} &
wait
echo "✓ All images loaded into kind"
# ─────────────────────────────────────────────────────────────────────────────

# Compile dbt manifests and upload to localstack S3 using the dbt-compile-and-load
# service. This mirrors the production flow: manifests live in S3, and the
# manifest-controller reads them via source=s3 when update.graph:v1 fires.
echo "Starting postgres and localstack for dbt manifest compilation..."
docker compose up -d postgres localstack
echo "Waiting for postgres to be ready..."
for i in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U continuo_svc > /dev/null 2>&1; then
        echo "✓ postgres ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "✗ postgres not ready after 60s"
        exit 1
    fi
    sleep 2
done
echo "Waiting for localstack to be healthy..."
for i in $(seq 1 30); do
    if docker compose exec -T localstack curl -sf http://localhost:4566/_localstack/health > /dev/null 2>&1; then
        echo "✓ localstack ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "✗ localstack not ready after 60s"
        exit 1
    fi
    sleep 2
done
echo "Starting dbt-compile-and-load..."
docker compose up -d dbt-compile-and-load
echo "Compiling and uploading dbt manifests to localstack S3..."
docker exec dbt-compile-and-load uv run python -m dbt_upload load --services-dir /app/services --target localstack
echo "✓ dbt manifests compiled and uploaded to localstack S3"

# Wait for control plane
echo "Waiting for control plane..."
kubectl wait --for=condition=Ready node/${CLUSTER_NAME}-control-plane --timeout=60s

kubectl create serviceaccount ${CLUSTER_NAME}-app-sa || echo "ServiceAccount already exists, continuing..."
kubectl create clusterrolebinding ${CLUSTER_NAME}-app-admin \
  --clusterrole=cluster-admin \
  --serviceaccount=default:${CLUSTER_NAME}-app-sa || echo "ClusterRoleBinding already exists, continuing..."

# Resolve the API server address reachable from inside docker containers
KUBE_IP=$(docker exec ${CLUSTER_NAME}-control-plane kubectl get endpoints kubernetes -n default -o jsonpath='{.subsets[0].addresses[0].ip}')
KUBE_PORT=$(docker exec ${CLUSTER_NAME}-control-plane kubectl get endpoints kubernetes -n default -o jsonpath='{.subsets[0].ports[0].port}')

if [ -z "$KUBE_IP" ]; then
    echo "Error: Could not determine Kubernetes API server IP."
    exit 1
fi

echo "Kubernetes API server: ${KUBE_IP}:${KUBE_PORT}"

# Write patched kubeconfig pointing at the real API server IP
mkdir -p kubeconfig
echo "Creating kubeconfig with correct server endpoint..."
kubectl config view --raw > kubeconfig.yaml.tmp
sed "s|server: https://[^:]*:[0-9]*|server: https://${KUBE_IP}:${KUBE_PORT}|g" \
    kubeconfig.yaml.tmp > kubeconfig/kubeconfig.yaml

mkdir -p executor-controller/kubeconfig
cp kubeconfig/kubeconfig.yaml executor-controller/kubeconfig/kubeconfig.yaml
echo "✓ Copied kubeconfig to executor-controller/"

rm kubeconfig.yaml.tmp
echo "Kubeconfig created at: kubeconfig/kubeconfig.yaml"

echo "Starting docker-compose in background..."
docker compose up -d
