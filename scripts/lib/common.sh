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

# Side-loads a pulled (not locally built) image into a kind cluster.
#
# `docker pull` on a containerd-backed image store only fetches the blobs for
# the host platform, but keeps the full multi-platform manifest-list
# metadata — including any buildx provenance/SBOM attestation manifests the
# publisher attached. `kind load docker-image` always runs `ctr images
# import --all-platforms`, which then fails looking for blobs of platforms
# that were never pulled. Locally built images never hit this, since a local
# build only ever writes manifests it actually has blobs for.
#
# `docker save --platform <arch>` collapses the image down to the one
# manifest whose blobs are present, and `kind load image-archive` loads that
# archive directly — one fewer round trip than reloading it into the local
# engine first. The platform is derived from the host, not hardcoded, so this
# works on both amd64 and arm64 hosts pulling the same multi-arch tag.
kind_load_pulled_image(){
  local ref=$1 cluster=$2 host_arch platform archive
  host_arch="$(uname -m)"
  case "$host_arch" in
    x86_64|amd64) platform="linux/amd64" ;;
    aarch64|arm64) platform="linux/arm64" ;;
    *) log_error "kind_load_pulled_image: unsupported host architecture '${host_arch}' for ${ref}"; return 1 ;;
  esac

  # The XXXXXX run must be the template's last characters: BSD/macOS mktemp
  # only randomizes a trailing run, silently leaving a literal "XXXXXX" (a
  # fixed, collidable name) if anything follows it — unlike GNU mktemp,
  # which accepts a suffix. No file extension is needed for docker/kind to
  # read the archive, so there is nothing to put after it.
  archive="$(mktemp "${TMPDIR:-/tmp}/continuo-validation-image.XXXXXX")" || {
    log_error "kind_load_pulled_image: failed to create a temp file for ${ref}"; return 1; }

  if ! docker save --platform "$platform" "$ref" -o "$archive"; then
    log_error "kind_load_pulled_image: 'docker save --platform ${platform}' failed for ${ref}"
    rm -f "$archive"; return 1
  fi
  if ! kind load image-archive "$archive" --name "$cluster"; then
    log_error "kind_load_pulled_image: 'kind load image-archive' failed for ${ref} (platform ${platform})"
    rm -f "$archive"; return 1
  fi
  rm -f "$archive"
}
