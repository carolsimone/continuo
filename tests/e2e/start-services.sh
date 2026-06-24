#!/usr/bin/env bash
set -euo pipefail

RED="\033[0;31m"
GREEN="\033[0;32m"
YELLOW="\033[1;33m"
NC="\033[0m"

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

check_health() {
  local service=$1
  local port=$2
  local path=${3:-/health}
  local max_retries=30

  log_info "Waiting for $service to become healthy (port $port$path)..."

  for i in $(seq 1 $max_retries); do
    if docker exec "$service" curl -sf "http://localhost:${port}${path}" >/dev/null; then
      log_info "$service is healthy!"
      return 0
    else
      log_warn "Attempt $i: ${service} is not healthy yet. Retrying in 2 seconds..."
      sleep 2
    fi
  done

  docker exec "$service" ps aux | grep "go run" || true
  log_error "Recent logs:"
  docker logs --tail 20 "$service" 2>&1 || true
  return 1
}

start_service() {
  local container=$1
  local service=$2
  local service_path=$3

  log_info "Starting $service in container $container..."
  docker exec -d "$container" bash -c "cd /app/$service_path && go run main.go" || {
    log_error "Failed to start $container..."
    return 1
  }
  log_info "Waiting for $service to compile and start (this may take 20-30 seconds)..."
  sleep 20
}

log_info "Starting all services..."

# Start state service first (dependency for others)
start_service "state" "state" "state"
check_health "state" 8082 || exit 1

# Copy kubeconfig so orchestrator can run kubectl during e2e tests
docker exec orchestrator mkdir -p /root/.kube
docker cp kubeconfig/kubeconfig.yaml orchestrator:/root/.kube/config 2>/dev/null || log_warn "No kubeconfig found — orchestrator kubectl will not work"
docker network connect kind orchestrator 2>/dev/null || true

start_service "orchestrator" "orchestrator" "orchestrator"

# Note: executor-controller and k8s-controller will run in kind for E2E tests,
# but we can start them in docker-compose too for development flexibility
# Uncomment if you want to start them:
# start_service "executor-controller" "executor-controller" "executor-controller"
# start_service "k8s-controller" "k8s-controller" "k8s-controller"

# Check health of all started services
check_health "orchestrator" 8087 || exit 1

# Uncomment if you started executor/k8s controllers:
# check_health "executor-controller" 8084 || exit 1
# check_health "k8s-controller" 8085 || exit 1

# release-controller (blue/green release API + stream consumers) runs in
# docker-compose alongside the other stream services. The e2e release-promote
# test reaches its HTTP API at release-controller:8088 over the compose network,
# and its consumers drive the candidate-parse → validation → promotion loop.
start_service "release-controller" "release-controller" "release-controller"
check_health "release-controller" 8088 "/healthz" || exit 1

start_service "remediation" "remediation" "remediation"
check_health "remediation" 8090 "/healthz" || exit 1

start_service "remediation-agent" "remediation-agent" "remediation-agent"
check_health "remediation-agent" 8092 "/healthz" || exit 1

log_info "Starting manifest-controller..."
docker exec -d manifest-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"
sleep 3
log_info "manifest-controller started"

log_info "Starting agent-runner..."
docker exec -d agent-runner bash -c "cd /app/agent-runner && go run . > /tmp/agent-runner.log 2>&1"
log_info "Waiting for agent-runner to compile and start (this may take 20-30 seconds)..."
sleep 20
check_health "agent-runner" 8091 || exit 1
# Also wait for the gRPC listener (50053): the chat e2e dials it through the
# ui-service relay, and /health (always 200) can come up before the port binds.
log_info "Waiting for agent-runner gRPC port 50053..."
for i in $(seq 1 30); do
  if docker exec agent-runner bash -c 'echo > /dev/tcp/localhost/50053' 2>/dev/null; then
    log_info "agent-runner gRPC ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    log_info "agent-runner gRPC not ready after 60s"
    docker exec agent-runner tail -40 /tmp/agent-runner.log || true
    exit 1
  fi
  sleep 2
done

log_info "Compiling and uploading dbt manifests..."
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload load --services-dir /app/services --release-id e2e-baseline
log_info "dbt manifests uploaded to canonical keys (<service>/e2e-baseline/manifest.json)"
log_info "NOTE: per-service image tags are seeded into the release-controller service_prod table by setup.sh / provision-k8s-test-env.sh"

log_info "All services started successfully!"
