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
echo "Building service images..."
DOCKER_BUILDKIT=1 docker compose build

# Build dbt service images and load into KIND
echo "Building dbt base image..."
DOCKER_BUILDKIT=1 docker build -t dbt-base:latest dbt/base/

echo "Building dbt service images..."
DOCKER_BUILDKIT=1 docker build -t service-1:latest dbt/services/service-1/
DOCKER_BUILDKIT=1 docker build -t service-2:latest dbt/services/service-2/
DOCKER_BUILDKIT=1 docker build -t service-3:latest dbt/services/service-3/
DOCKER_BUILDKIT=1 docker build -t service-3-broken:latest dbt/services/service-3-broken/

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
kind load docker-image service-3-broken:latest --name ${CLUSTER_NAME} &
kind load docker-image continuo-executor-controller:latest --name ${CLUSTER_NAME} &
kind load docker-image continuo-k8s-controller:latest --name ${CLUSTER_NAME} &
wait
echo "✓ All images loaded into kind"
# ─────────────────────────────────────────────────────────────────────────────

# Compile dbt manifests for each service image so the continuo can read the
# full dependency graph at runtime.  dbt compile requires a live Postgres
# connection to resolve source references; run on the continuo_default network
# with DBT_POSTGRES_HOST pointing at the postgres container.
# Start postgres now so the compose network exists and postgres is
# reachable before we try to compile.
echo "Starting postgres for dbt manifest compilation..."
docker compose up -d postgres
echo "Waiting for postgres to be ready..."
for i in $(seq 1 30); do
    if docker compose exec -T postgres pg_isready -U runner > /dev/null 2>&1; then
        echo "✓ postgres ready"
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "✗ postgres not ready after 60s"
        exit 1
    fi
    sleep 2
done
# Detect the compose network name (varies by directory/project name)
COMPOSE_NETWORK=$(docker compose config --format json 2>/dev/null | python3 -c "import sys,json; nets=json.load(sys.stdin).get('networks',{}); print(list(nets.values())[0].get('name','') if nets else '')" 2>/dev/null || true)
if [ -z "$COMPOSE_NETWORK" ]; then
    # Fallback: inspect the postgres container's networks
    COMPOSE_NETWORK=$(docker inspect --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}' "$(docker compose ps -q postgres)")
fi
echo "Using compose network: $COMPOSE_NETWORK"
echo "Compiling dbt manifests..."
for svc in service-1 service-2 service-3; do
    echo "  Compiling ${svc}..."
    CID=$(docker run -d --network "$COMPOSE_NETWORK" \
        -e DBT_POSTGRES_HOST=postgres \
        --entrypoint dbt "${svc}:latest" compile --profiles-dir /project)
    docker wait "$CID"
    docker cp "$CID:/project/target/manifest.json" "dbt/services/${svc}/manifest.json"
    docker rm "$CID"
    echo "  ✓ manifest saved to dbt/services/${svc}/manifest.json"
done
echo "✓ dbt manifests compiled"

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
