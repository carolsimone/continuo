#!/usr/bin/env bash
set -euo pipefail

RED="\033[0;31M"
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
  local max_retries=30

  log_info "Waiting for $service to become healthy (port $port)..."

  for i in $(seq 1 $max_retries); do
    if docker exec "$service" curl -sf http://localhost:"$port"/health >/dev/null; then
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

start_service "startup-controller" "startup-controller" "startup-controller"
start_service "orchestrator" "orchestrator" "orchestrator"

# Note: executor-controller and k8s-controller will run in kind for E2E tests,
# but we can start them in docker-compose too for development flexibility
# Uncomment if you want to start them:
# start_service "executor-controller" "executor-controller" "executor-controller"
# start_service "k8s-controller" "k8s-controller" "k8s-controller"

# Check health of all started services
check_health "startup-controller" 8083 || exit 1
check_health "orchestrator" 8087 || exit 1

# Uncomment if you started executor/k8s controllers:
# check_health "executor-controller" 8084 || exit 1
# check_health "k8s-controller" 8085 || exit 1

log_info "Starting manifest-controller..."
docker exec -d manifest-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"
sleep 3
log_info "manifest-controller started"

log_info "All services started successfully!"
