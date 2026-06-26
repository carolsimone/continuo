#!/usr/bin/env bash
# Single source of truth for build/run/wait helpers shared by local dev (via the
# Makefile) and CI jobs. No build/run logic may live inline in ci.yml — the
# alignment guard (scripts/check-ci-alignment.sh) enforces that.
RED="\033[0;31m"; GREEN="\033[0;32m"; YELLOW="\033[1;33m"; NC="\033[0m"
log_info(){ echo -e "${GREEN}[INFO]${NC} $1" >&2; }
log_warn(){ echo -e "${YELLOW}[WARN]${NC} $1" >&2; }
log_error(){ echo -e "${RED}[ERROR]${NC} $1" >&2; }
wait_for_tcp_port(){ local c=$1 p=$2 r=${3:-30} n=${4:-2}; for ((i=1;i<=r;i++)); do
  docker exec "$c" bash -c "echo > /dev/tcp/localhost/${p}" 2>/dev/null && { log_info "${c}:${p} ready"; return 0; }; sleep "$n"; done
  log_error "${c}:${p} not ready after $((r*n))s"; return 1; }
wait_for_http_host(){ local u=$1 r=${2:-30} n=${3:-2}; for ((i=1;i<=r;i++)); do
  curl -sf "$u" >/dev/null 2>&1 && { log_info "${u} ready"; return 0; }; sleep "$n"; done
  log_error "${u} not ready after $((r*n))s"; return 1; }
wait_for_container_running(){ local name=$1 r=${2:-30} n=${3:-2}; for ((i=1;i<=r;i++)); do
  [ "$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)" = "true" ] && { log_info "${name} running"; return 0; }; sleep "$n"; done
  log_error "${name} not running after $((r*n))s"; docker logs "$name" --tail 50 2>/dev/null||true; return 1; }
check_container_health(){ local c=$1 p=$2 path=${3:-/health} r=${4:-30}; for ((i=1;i<=r;i++)); do
  docker exec "$c" curl -sf "http://localhost:${p}${path}" >/dev/null 2>&1 && { log_info "${c} healthy"; return 0; }; sleep 2; done
  log_error "${c} not healthy after $((r*2))s"; docker logs --tail 20 "$c" 2>&1||true; return 1; }
start_go_service(){ local c=$1 path=$2 warm=${3:-20}; log_info "Starting ${c} (go run, cold ~20-30s)..."
  docker exec -d "$c" bash -c "cd /app/${path} && go run main.go" || { log_error "Failed to start ${c}"; return 1; }; sleep "$warm"; }
