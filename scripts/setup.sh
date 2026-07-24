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

# Generate the developer-local .env before any compose command reads it.
bash "$(dirname "${BASH_SOURCE[0]}")/ensure-dev-env.sh"

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
echo "Building s3-sidecar image..."
DOCKER_BUILDKIT=1 docker build -t s3-sidecar:latest s3-sidecar/
echo "Building slim validation-runner (postgres) image..."
# CONTINUO_INDEX_URL resolves continuo-validation-* from TestPyPI until they are on real
# PyPI (the reshape's pre-release phase); harmless once they are.
DOCKER_BUILDKIT=1 docker build -t continuo-validation-runner-postgres:latest \
  -f validation-runner/Dockerfile.postgres \
  --build-arg CONTINUO_INDEX_URL=https://test.pypi.org/simple/ validation-runner/
echo "Building service images (batched)..."
# Build in small batches instead of all ~13 services at once. On a 2-CPU/7.75GB
# CI runner, building everything in parallel thrashes memory and disk I/O so badly
# that layer export stalls (observed: one image export hung ~27 min). Batches of
# BUILD_BATCH (default 2, ≈ the runner's CPU count) keep memory/disk in budget.
# The buildable service list is derived from the merged compose config so it
# tracks COMPOSE_FILE (CI adds docker-compose.ci.yml).
# Portable array build (macOS bash 3.2 has no `mapfile`).
_buildable=()
while IFS= read -r _svc; do [ -n "$_svc" ] && _buildable+=("$_svc"); done < <(docker compose config --format json \
  | python3 -c "import sys,json; print('\n'.join(k for k,v in json.load(sys.stdin)['services'].items() if 'build' in v))")
_batch=()
_build_batch() { [ "${#_batch[@]}" -gt 0 ] && { echo "  building: ${_batch[*]}"; DOCKER_BUILDKIT=1 docker compose build "${_batch[@]}"; }; }
for _svc in "${_buildable[@]}"; do
    _batch+=("$_svc")
    if [ "${#_batch[@]}" -ge "${BUILD_BATCH:-2}" ]; then _build_batch; _batch=(); fi
done
_build_batch

# Build dbt service images and load into KIND

IMAGE_TAG="$(git rev-parse --short HEAD)-$(date +%s)"
echo "Using IMAGE_TAG=${IMAGE_TAG} for dbt service images"

DBT_SERVICES=(service-1 service-2 service-3)
echo "Building dbt service images with content-addressed tag..."
for svc in "${DBT_SERVICES[@]}"; do
    DOCKER_BUILDKIT=1 docker build \
        -f "dbt/services/${svc}/Dockerfile.local" \
        -t "${svc}:${IMAGE_TAG}" \
        "dbt/services/${svc}/"
done

PER_SERVICE=""
for svc in "${DBT_SERVICES[@]}"; do
    [ -n "$PER_SERVICE" ] && PER_SERVICE="${PER_SERVICE},"
    PER_SERVICE="${PER_SERVICE}${svc}=${IMAGE_TAG}"
done
export IMAGE_TAG_PER_SERVICE="$PER_SERVICE"
echo "Exported IMAGE_TAG_PER_SERVICE=${IMAGE_TAG_PER_SERVICE}"

# Write the per-service image tags to the bind-mounted tests/e2e/.image-tags file
# (immutable setup metadata). The e2e rebuilds the release-controller service_prod
# baseline from this before each consumer, so a prior test's mutation of
# service_prod cannot break a later read. Replaces the obsolete S3 image-tag sidecar.
printf '%s' "$PER_SERVICE" > tests/e2e/.image-tags
echo "Wrote per-service image tags to tests/e2e/.image-tags"

# continuo-executor-controller and continuo-k8s-controller are already built by
# 'docker compose build' above with the correct tags, so no need to rebuild them here.

# Wait for kind cluster to finish (if we started it above)
if [ -n "$KIND_PID" ]; then
    echo "Waiting for kind cluster creation to complete..."
    wait $KIND_PID
    echo "✓ Kind cluster ready"
fi
echo "Loading images into kind (sequential)..."
# Load one image at a time. `kind load` imports a tarball into the node's
# containerd, which is disk-write-bound; on a 2-CPU/7.75GB CI runner, loading
# all images at once thrashes disk I/O and can stall the import. Sequential
# loading keeps disk pressure bounded — the same reason the image build above
# is batched.
for svc in "${DBT_SERVICES[@]}"; do
    kind load docker-image "${svc}:${IMAGE_TAG}" --name "${CLUSTER_NAME}"
done
kind load docker-image continuo-executor-controller:latest --name ${CLUSTER_NAME}
kind load docker-image continuo-k8s-controller:latest --name ${CLUSTER_NAME}
kind load docker-image dbt-base:latest --name ${CLUSTER_NAME}
kind load docker-image s3-sidecar:latest --name ${CLUSTER_NAME}
kind load docker-image continuo-validation-runner-postgres:latest --name ${CLUSTER_NAME}
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
docker exec dbt-compile-and-load uv run python -m dbt_upload load --services-dir /app/services --target localstack --release-id e2e-baseline
echo "✓ dbt manifests compiled and uploaded to localstack S3 (key: <service>/e2e-baseline/manifest.json)"

# Per-service image tags are seeded into the release-controller service_prod
# pointer table at the end of this script (after the final compose up runs the
# release-controller migrations). The e2e reads image_tag from service_prod — the
# production source of truth — so no S3 image-tag sidecar is needed.

# Materialize dbt seeds into e2e_schema. The e2e topology is seeded directly
# into Neo4j (no manifest.loaded ingest), and the standing "seed" schedule is
# not run before the seed-dependent models (e.g. table_a = ref('seed_table_1')),
# so their seed inputs must already exist in the shared dbt database. Only
# service-1 ships seeds; dbt-compile-and-load shares continuo_dbt with the kind
# run-jobs, so seeds land where those jobs read them.
echo "Materializing dbt seeds into e2e_schema..."
docker exec dbt-compile-and-load bash -c "cd /app/services/service-1 && dbt seed --profiles-dir ."
echo "✓ dbt seeds materialized"

# Wait for control plane
echo "Waiting for control plane..."
kubectl wait --for=condition=Ready node/${CLUSTER_NAME}-control-plane --timeout=60s

kubectl create serviceaccount ${CLUSTER_NAME}-app-sa || echo "ServiceAccount already exists, continuing..."
kubectl create clusterrolebinding ${CLUSTER_NAME}-app-admin \
  --clusterrole=cluster-admin \
  --serviceaccount=default:${CLUSTER_NAME}-app-sa || echo "ClusterRoleBinding already exists, continuing..."

# Warehouse credentials for the validation pods, which the executor attaches via
# envFrom (VALIDATION_WAREHOUSE_SECRET). The host bridge IP is used because the
# validation Job runs inside kind and reaches Postgres on the host, not via the
# compose service name.
echo "Creating validation warehouse Secret..."
export DOCKER_BRIDGE_IP=$(docker network inspect bridge --format='{{(index .IPAM.Config 0).Gateway}}' 2>/dev/null || echo "")
envsubst < tests/e2e/k8s/validation-warehouse-secret.yaml | kubectl apply -f - || {
  echo "Error: failed to create validation warehouse Secret"; exit 1
}

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
