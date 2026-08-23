#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../../scripts/lib/common.sh"

check_health(){ check_container_health "$1" "$2" "${3:-/health}"; }
start_service(){ start_go_service "$1" "$3"; }

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

start_service "agent-remediation" "agent-remediation" "agent-remediation"
check_health "agent-remediation" 8092 "/healthz" || exit 1

log_info "Starting manifest-controller..."
docker exec -d manifest-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"
sleep 3
log_info "manifest-controller started"

log_info "Starting agent-chat..."
docker exec -d agent-chat bash -c "cd /app/agent-chat && go run . > /tmp/agent-chat.log 2>&1"
log_info "Waiting for agent-chat to compile and start (this may take 20-30 seconds)..."
sleep 20
check_health "agent-chat" 8091 || exit 1
# Also wait for the gRPC listener (50053): the chat e2e dials it through the
# ui relay, and /health (always 200) can come up before the port binds.
log_info "Waiting for agent-chat gRPC port 50053..."
wait_for_tcp_port agent-chat 50053

log_info "Compiling and uploading dbt manifests..."
docker exec dbt-compile-and-load \
  uv run python -m dbt_upload load --services-dir /app/services --release-id e2e-baseline
log_info "dbt manifests uploaded to canonical keys (<service>/e2e-baseline/manifest.json)"
log_info "NOTE: per-service image tags are seeded into the release-controller service_prod table by setup.sh / provision-k8s-test-env.sh"

log_info "All services started successfully!"
