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

# True if this Docker's active image store is the containerd snapshotter
# rather than the classic graphdriver. `docker info`'s DriverStatus field is
# graphdriver-specific plumbing (backing filesystem, native overlay diff
# support, etc.) and is always empty under the containerd store — there is
# nothing else to report there. `--format '{{json .DriverStatus}}'` prints
# `[]` for an empty/nil slice either way, which is the signal.
_kind_load_uses_containerd_store(){
  local status
  status="$(docker info --format '{{json .DriverStatus}}' 2>/dev/null)"
  [ "$status" = "null" ] || [ "$status" = "[]" ]
}

# True if the Docker Engine (server, not client) is 28.0 or newer — the
# version `docker save --platform` was added in.
_kind_load_docker_ge_28(){
  local major
  major="$(docker version --format '{{.Server.Version}}' 2>/dev/null | cut -d. -f1)"
  case "$major" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "$major" -ge 28 ]
}

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
#
# `--platform` needs both a containerd-backed image store and a recent-enough
# Docker (API 1.48 / Engine-CLI 28.0+); a classic graphdriver store never has
# the multi-platform-index bug in the first place (a graphdriver pull only
# ever resolves and stores the host platform, no manifest list involved), so
# on those hosts the platform-scoped attempt is simply unavailable, not
# broken — it fails harmlessly and the bare fallback below works fine. This
# tries the platform-scoped route first and falls back to the original bare
# `kind load docker-image` on any failure — save failing, an empty archive,
# or the archive load failing.
#
# One combination has NO working route at all: a containerd-backed image
# store on Docker <28.0. `docker save --platform` is unavailable there (the
# flag itself needs 28.0+), and the bare fallback's `kind load docker-image`
# runs `ctr images import --all-platforms` — the exact same missing-blob
# failure the platform-scoped route exists to avoid, not something the
# fallback happens to dodge. Attempting the fallback in that case does not
# rescue anything; it just trades one confusing failure (an archive that
# quietly never gets built) for another (`ctr`'s "content digest ... not
# found", several layers removed from the real cause). This is detected up
# front and fails immediately with an actionable message instead.
kind_load_pulled_image(){
  local ref=$1 cluster=$2 host_arch platform archive

  host_arch="$(uname -m)"
  case "$host_arch" in
    x86_64|amd64) platform="linux/amd64" ;;
    aarch64|arm64) platform="linux/arm64" ;;
    *) log_error "kind_load_pulled_image: unsupported host architecture '${host_arch}' for ${ref}"; return 1 ;;
  esac

  if _kind_load_uses_containerd_store && ! _kind_load_docker_ge_28; then
    log_error "kind_load_pulled_image: this Docker uses the containerd image store on Docker <28.0 — neither 'docker save --platform' (needs Engine-CLI 28.0+ / API 1.48+) nor the bare 'kind load docker-image' fallback (fails on missing non-host-platform blobs, same root cause) can side-load a pulled multi-platform image (${ref}) on this combination. Upgrade Docker to 28.0+, or switch the Docker Engine image store back to the classic graphdriver."
    return 1
  fi

  # The XXXXXX run must be the template's last characters: BSD/macOS mktemp
  # only randomizes a trailing run, silently leaving a literal "XXXXXX" (a
  # fixed, collidable name) if anything follows it — unlike GNU mktemp,
  # which accepts a suffix. No file extension is needed for docker/kind to
  # read the archive, so there is nothing to put after it.
  archive="$(mktemp "${TMPDIR:-/tmp}/continuo-validation-image.XXXXXX")" || {
    log_error "kind_load_pulled_image: failed to create a temp file for ${ref}"; return 1; }

  if docker save --platform "$platform" "$ref" -o "$archive" && [ -s "$archive" ]; then
    if kind load image-archive "$archive" --name "$cluster"; then
      rm -f "$archive"
      return 0
    fi
    log_warn "kind_load_pulled_image: 'kind load image-archive' failed for ${ref} (platform ${platform}); falling back to 'kind load docker-image'"
  else
    log_warn "kind_load_pulled_image: 'docker save --platform ${platform}' failed or produced an empty archive for ${ref} (Docker <28.0 or a classic graphdriver store never has the --platform flag / behaviour); falling back to 'kind load docker-image'"
  fi
  rm -f "$archive"

  if kind load docker-image "$ref" --name "$cluster"; then
    return 0
  fi

  log_error "kind_load_pulled_image: both the platform-scoped route ('docker save --platform ${platform}' + 'kind load image-archive') and the fallback ('kind load docker-image') failed for ${ref}"
  return 1
}
