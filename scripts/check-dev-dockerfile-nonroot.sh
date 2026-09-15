#!/usr/bin/env bash
# Fails if any dev/base Dockerfile that runs as root is missing from the
# AVD-DS-0002 (non-root USER) exclusion list in .trivyignore.yaml — or if a
# path in that list no longer exists.
#
# Trivy's `config` scan (scripts/security-scan.sh) checks every Dockerfile in
# the repo, including the dev/base images that run as root by necessity (host
# bind-mounts, /var/run/docker.sock, root-owned Go/uv build caches — see the
# reasoning in .trivyignore.yaml). Those images are silenced by pinning each
# one under AVD-DS-0002 in .trivyignore.yaml, path by path.
#
# The failure mode this guard closes: when a service adds a Dockerfile.dev, or
# an existing one is renamed (a service rename moves `foo/Dockerfile.dev` to
# `bar/Dockerfile.dev`), the exclusion list is easy to forget, and DS002
# resurfaces as a fresh HIGH advisory on the uncovered image. So every
# `Dockerfile.base` and `*/Dockerfile.dev` must either declare a non-root USER
# of its own or appear in the exclusion; and every excluded path must still
# exist, so a rename leaves no dead entry behind.
#
# Production images are out of scope here on purpose: the *.prod Dockerfiles
# set a non-root USER and stay fully scanned, so they satisfy the USER branch
# below and never need an exclusion.
set -uo pipefail

REPO_ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
IGNORE_FILE="${REPO_ROOT}/.trivyignore.yaml"
fail=0

if [ ! -f "$IGNORE_FILE" ]; then
  echo "check-dev-dockerfile-nonroot: ${IGNORE_FILE} not found" >&2
  exit 1
fi

# The repo-relative paths currently excluded under AVD-DS-0002. The block runs
# from the `- id: AVD-DS-0002` line to the next `- id:` (or EOF); within it,
# every double-quoted entry is one path.
ignored_paths() {
  awk '
    /-[[:space:]]*id:[[:space:]]*AVD-DS-0002/ { inblk = 1; next }
    inblk && /-[[:space:]]*id:/               { inblk = 0 }
    inblk && match($0, /"[^"]+"/) {
      print substr($0, RSTART + 1, RLENGTH - 2)
    }
  ' "$IGNORE_FILE"
}

# True if $1 declares a non-root USER: a USER instruction whose argument is
# neither `root` nor `0`. A Dockerfile with no USER line (or an explicit
# `USER root`) is treated as root.
has_nonroot_user() {
  grep -E '^[[:space:]]*USER[[:space:]]+' "$1" 2>/dev/null \
    | grep -qvE '^[[:space:]]*USER[[:space:]]+(root|0)([[:space:]:]|$)'
}

IGN="$(ignored_paths)"

is_ignored() {
  printf '%s\n' "$IGN" | grep -qxF -- "$1"
}

# Forward direction: every root-running dev/base image must be excluded (or
# carry its own non-root USER).
while IFS= read -r df; do
  [ -n "$df" ] || continue
  rel="${df#"$REPO_ROOT"/}"
  if has_nonroot_user "$df"; then
    continue
  fi
  if ! is_ignored "$rel"; then
    echo "check-dev-dockerfile-nonroot: ${rel} runs as root and is not excluded under AVD-DS-0002 in .trivyignore.yaml" >&2
    echo "    -> give it a non-root USER, or add \"${rel}\" to the AVD-DS-0002 paths in .trivyignore.yaml" >&2
    fail=1
  fi
done < <(
  {
    find "$REPO_ROOT" \
      \( -name .worktrees -o -name node_modules -o -name .git \) -prune -o \
      -type f -name 'Dockerfile.dev' -print
    [ -f "${REPO_ROOT}/Dockerfile.base" ] && printf '%s\n' "${REPO_ROOT}/Dockerfile.base"
  } | sort
)

# Reverse direction: every excluded path must still exist.
while IFS= read -r rel; do
  [ -n "$rel" ] || continue
  if [ ! -f "${REPO_ROOT}/${rel}" ]; then
    echo "check-dev-dockerfile-nonroot: .trivyignore.yaml excludes \"${rel}\" under AVD-DS-0002, but that file does not exist (stale entry — remove it or fix the path)" >&2
    fail=1
  fi
done < <(printf '%s\n' "$IGN")

if [ "$fail" -ne 0 ]; then
  exit 1
fi

n_ignored="$(printf '%s\n' "$IGN" | grep -c . || true)"
echo "Dev/base Dockerfile non-root coverage OK (${n_ignored} AVD-DS-0002 exclusions, all present)"
exit 0
