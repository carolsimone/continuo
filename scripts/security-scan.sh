#!/usr/bin/env bash
# Runs the vulnerability and secret scanners across the repository.
#
# This is the single primitive shared by `make security-scan` and the security
# workflow, so local and CI run byte-identical checks. Each scanner is a separate
# subcommand so CI can fan them out into parallel jobs while a developer can run
# them all in one go.
#
# Blocking vs advisory:
#   vuln, secrets  -> exit non-zero on any finding. govulncheck only reports
#                     vulnerabilities the code actually reaches, and a committed
#                     secret is never acceptable, so both are high-signal enough
#                     to gate a merge.
#   deps, config   -> report HIGH/CRITICAL and always exit 0. Base-image CVEs are
#                     frequently unfixable upstream, so gating on them would wedge
#                     every pull request behind an ignore-list edit.
#
# Usage:
#   scripts/security-scan.sh              # every scanner
#   scripts/security-scan.sh vuln         # govulncheck over the Go modules
#   scripts/security-scan.sh secrets      # gitleaks over the working tree
#   scripts/security-scan.sh secrets --history   # gitleaks over full git history
#   scripts/security-scan.sh deps         # trivy dependency CVEs (advisory)
#   scripts/security-scan.sh config       # trivy Dockerfile/K8s misconfig (advisory)
set -uo pipefail

# Single source of truth for the pinned versions. Bump here only.
GOVULNCHECK_VERSION="v1.1.4"
GITLEAKS_IMAGE="ghcr.io/gitleaks/gitleaks:v8.24.3"
TRIVY_IMAGE="aquasec/trivy:0.58.2"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# SARIF output is opt-in via SARIF_DIR (a path relative to the repository root).
# CI sets it so findings can be uploaded to code scanning; local runs leave it
# unset and pay nothing. It has to be opt-in because collecting SARIF means
# running some scanners a second time — see scan_vuln for why.
SARIF_DIR="${SARIF_DIR:-}"
sarif_out=""
if [ -n "${SARIF_DIR}" ]; then
  sarif_out="${repo_root}/${SARIF_DIR}"
  mkdir -p "${sarif_out}"
fi

# Turns a module path from the registry into a filename-safe token:
# ./tests/e2e -> tests-e2e. Needed because several modules are nested, and a
# slash in a filename would silently create a directory that never gets uploaded.
sarif_name() {
  local m="${1#./}"
  printf '%s' "${m//\//-}"
}

# Scanners walk the whole tree, and .claude/worktrees/ holds full stale copies of
# every module. Without this every finding is reported once per worktree, and a
# fixed issue keeps firing from a stale copy. Built as an array because trivy
# takes one --skip-dirs flag per directory; a single joined string is silently
# accepted as one literal path and excludes nothing.
TRIVY_SKIP=(
  --skip-dirs .claude
  --skip-dirs docs/superpowers
  --skip-dirs node_modules
  --skip-dirs .git
)

have_docker() {
  if ! command -v docker >/dev/null 2>&1 || ! docker info >/dev/null 2>&1; then
    echo "error: docker is required for this scanner but is not available" >&2
    return 1
  fi
}

# ── govulncheck ──────────────────────────────────────────────────────────────
# Reachability-based: a vulnerability in a dependency is only reported when a
# call path from this module actually reaches the vulnerable symbol. That is why
# this one is allowed to block.
scan_vuln() {
  # Resolve the module list first and fail closed if it is absent or empty, before
  # installing anything. This is a blocking gate, so an empty target list must be
  # an error, not a zero-iteration loop that returns success having scanned
  # nothing. Same registry the lint gate uses, so the two gates never drift.
  local registry="scripts/lint-ci-modules.txt"
  if [ ! -s "${registry}" ]; then
    echo "error: ${registry} is missing or empty; refusing to report a blocking" \
         "vulnerability gate as green having scanned nothing" >&2
    return 1
  fi

  local targets
  targets="$(sed -e 's/#.*//' -e '/^[[:space:]]*$/d' "${registry}" \
    | sed 's#^[[:space:]]*#./#')"

  if [ -z "${targets}" ]; then
    echo "error: ${registry} lists no modules; the vulnerability gate scanned" \
         "nothing and will not pass by default" >&2
    return 1
  fi

  local gopath_bin
  gopath_bin="$(go env GOPATH)/bin"

  if ! command -v govulncheck >/dev/null 2>&1 || \
     ! govulncheck -version 2>/dev/null | grep -q "${GOVULNCHECK_VERSION#v}"; then
    echo "Installing govulncheck ${GOVULNCHECK_VERSION}..."
    go install "golang.org/x/vuln/cmd/govulncheck@${GOVULNCHECK_VERSION}" || return 1
  fi
  export PATH="${gopath_bin}:${PATH}"

  local rc=0
  for m in ${targets}; do
    echo "==> govulncheck ${m}"
    if [ "${m}" = "./cli" ]; then
      # cli is nested under the root go.work but is deliberately not a member
      # (see CLAUDE.md). Go's workspace auto-detection would otherwise walk up
      # and pick up the root go.work; force it off so cli's own go.mod resolves.
      ( cd "${m}" && GOWORK=off govulncheck ./... ) || rc=1
    else
      ( cd "${m}" && govulncheck ./... ) || rc=1
    fi

    # Second pass, for reporting only. `-format sarif` writes the report to
    # stdout and exits 0 EVEN WHEN VULNERABILITIES ARE FOUND (verified against
    # v1.1.4: text mode exits 1 on the same input that SARIF mode exits 0 on).
    # So it cannot replace the gating run above — using it alone would leave
    # this blocking gate permanently green while still appearing to work.
    # `|| true` keeps a reporting failure from turning the gate red on its own.
    if [ -n "${sarif_out}" ]; then
      local safe
      safe="$(sarif_name "${m}")"
      if [ "${m}" = "./cli" ]; then
        ( cd "${m}" && GOWORK=off govulncheck -format sarif ./... ) \
          > "${sarif_out}/govulncheck-${safe}.sarif" || true
      else
        ( cd "${m}" && govulncheck -format sarif ./... ) \
          > "${sarif_out}/govulncheck-${safe}.sarif" || true
      fi
    fi
  done
  return "${rc}"
}

# ── gitleaks ─────────────────────────────────────────────────────────────────
# Default scans the working tree. --history walks every commit, which is what you
# want before making the repository public; it is too slow for a per-PR gate.
scan_secrets() {
  have_docker || return 1

  local mode="dir"
  [ "${1:-}" = "--history" ] && mode="git"

  echo "==> gitleaks (${mode})"

  # The repo is mounted read-only on purpose, so SARIF cannot be written into it.
  # Bind the output directory separately, read-write, at a path of its own.
  local sarif_args=() sarif_mount=()
  if [ -n "${sarif_out}" ]; then
    sarif_mount=(-v "${sarif_out}:/sarif:rw")
    sarif_args=(--report-format sarif --report-path /sarif/gitleaks.sarif)
  fi

  if [ "${mode}" = "dir" ]; then
    docker run --rm \
      -v "${repo_root}:/repo:ro" \
      ${sarif_mount[@]+"${sarif_mount[@]}"} \
      "${GITLEAKS_IMAGE}" \
      dir /repo \
      --config /repo/.gitleaks.toml \
      ${sarif_args[@]+"${sarif_args[@]}"} \
      --redact \
      --no-banner \
      -v
    return
  fi

  # History mode needs the object database, and in a git worktree .git is a file
  # holding an absolute path to the main repository's gitdir. Bind-mounting the
  # checkout alone leaves that path dangling inside the container, and gitleaks
  # then reports "no leaks found in partial scan" — a scan that examined nothing.
  # Mount both at their real host paths so the absolute reference resolves.
  local git_common mounts=()
  git_common="$(cd "$(git rev-parse --git-common-dir)" && pwd)"
  mounts+=(-v "${repo_root}:${repo_root}:ro")
  case "${git_common}" in
    "${repo_root}"/*) ;;                                   # already covered
    *) mounts+=(-v "${git_common}:${git_common}:ro") ;;
  esac

  docker run --rm \
    ${mounts[@]+"${mounts[@]}"} \
    ${sarif_mount[@]+"${sarif_mount[@]}"} \
    -w "${repo_root}" \
    "${GITLEAKS_IMAGE}" \
    git "${repo_root}" \
    --config "${repo_root}/.gitleaks.toml" \
    ${sarif_args[@]+"${sarif_args[@]}"} \
    --redact \
    --no-banner \
    -v
}

# ── trivy: dependency CVEs ───────────────────────────────────────────────────
# Advisory. Reads the lockfiles (go.sum, package-lock.json, uv.lock) rather than
# building anything, so it is fast and needs no images present.
scan_deps() {
  have_docker || return 1

  local sarif_args=() sarif_mount=()
  if [ -n "${sarif_out}" ]; then
    sarif_mount=(-v "${sarif_out}:/sarif:rw")
    sarif_args=(--format sarif --output /sarif/trivy-fs.sarif)
  fi

  echo "==> trivy filesystem (dependency CVEs, advisory)"
  docker run --rm \
    -v "${repo_root}:/repo:ro" \
    ${sarif_mount[@]+"${sarif_mount[@]}"} \
    "${TRIVY_IMAGE}" \
    filesystem /repo \
    ${sarif_args[@]+"${sarif_args[@]}"} \
    --scanners vuln \
    --severity HIGH,CRITICAL \
    --ignore-unfixed \
    "${TRIVY_SKIP[@]}" \
    --exit-code 0 \
    --quiet
}

# ── trivy: misconfiguration ──────────────────────────────────────────────────
# Advisory. Covers Dockerfiles and the Helm/K8s manifests under deploy/.
scan_config() {
  have_docker || return 1

  echo "==> trivy config (Dockerfile/K8s misconfiguration, advisory)"
  local sarif_args=() sarif_mount=()
  if [ -n "${sarif_out}" ]; then
    sarif_mount=(-v "${sarif_out}:/sarif:rw")
    sarif_args=(--format sarif --output /sarif/trivy-config.sarif)
  fi

  docker run --rm \
    -v "${repo_root}:/repo:ro" \
    ${sarif_mount[@]+"${sarif_mount[@]}"} \
    "${TRIVY_IMAGE}" \
    config /repo \
    ${sarif_args[@]+"${sarif_args[@]}"} \
    --severity HIGH,CRITICAL \
    "${TRIVY_SKIP[@]}" \
    --exit-code 0 \
    --quiet
}

case "${1:-all}" in
  vuln)    scan_vuln ;;
  secrets) shift; scan_secrets "$@" ;;
  deps)    scan_deps ;;
  config)  scan_config ;;
  all)
    rc=0
    scan_vuln    || rc=1
    scan_secrets || rc=1
    scan_deps    || true   # advisory
    scan_config  || true   # advisory
    exit "${rc}"
    ;;
  *)
    echo "usage: $0 [vuln|secrets [--history]|deps|config|all]" >&2
    exit 2
    ;;
esac
