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

# continuo-executor-controller and continuo-k8s-controller are already built by
# 'docker compose build' above with the correct tags, so no need to rebuild them here.

# Wait for kind cluster to finish (if we started it above)
if [ -n "$KIND_PID" ]; then
    echo "Waiting for kind cluster creation to complete..."
    wait $KIND_PID
    echo "✓ Kind cluster ready"
fi
echo "Loading images into kind (parallel)..."
for svc in "${DBT_SERVICES[@]}"; do
    kind load docker-image "${svc}:${IMAGE_TAG}" --name "${CLUSTER_NAME}" &
done
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
docker exec dbt-compile-and-load uv run python -m dbt_upload load --services-dir /app/services --target localstack --release-id e2e-baseline
echo "✓ dbt manifests compiled and uploaded to localstack S3 (key: <service>/e2e-baseline/manifest.json)"

# Upload per-service image-tag sidecars so e2e tests can resolve image_tag
# from S3 without needing IMAGE_TAG_PER_SERVICE forwarded into the test
# container. Each sidecar lands at <service>/e2e-baseline/service_metadata.json,
# matching the readServiceImageTag helper in release_promote_test.go.
echo "Uploading per-service image-tag sidecars for e2e baseline..."
for svc in "${DBT_SERVICES[@]}"; do
  svc_tag="${IMAGE_TAG}"
  docker exec dbt-compile-and-load \
    uv run python3 -c "
import boto3, json, os
client = boto3.client('s3',
  endpoint_url=os.environ.get('S3_ENDPOINT_URL','http://localstack:4566'),
  aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID','test'),
  aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY','test'),
  region_name=os.environ.get('AWS_DEFAULT_REGION','us-east-1'))
client.put_object(
  Bucket='continuo',
  Key='${svc}/e2e-baseline/service_metadata.json',
  Body=json.dumps({'image_tag':'${svc_tag}','manifest_version':'e2e-baseline'}).encode())
print('uploaded ${svc}/e2e-baseline/service_metadata.json')
"
done
echo "✓ per-service image-tag sidecars uploaded"

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
