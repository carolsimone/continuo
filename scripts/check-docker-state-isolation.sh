#!/usr/bin/env bash
# Fails if a workflow job shares Docker client state with jobs running
# concurrently on the same machine.
#
# Both self-hosted runner agents execute as the same OS user, so unless a job
# points DOCKER_CONFIG at a private directory they all read and write one
# ~/.docker. Two things live there that concurrent jobs then clobber:
#
#   * buildx/current — the selected builder. A sibling job's
#     `docker buildx create --use` (every setup-buildx-action does this)
#     redirects an in-flight build onto a docker-container builder, which is
#     isolated from the daemon's image store; a locally-built base tag such as
#     `FROM continuo-base:latest` then resolves against docker.io and fails
#     with "pull access denied".
#   * config.json — registry credentials. docker/login-action's post step logs
#     out, dropping the auth entry a concurrent push still needs, which fails
#     with "failed to fetch anonymous token".
#
# Selecting the builder with a `docker buildx use` step does not fix the first
# case: it writes the same shared pointer and loses the next race. Only the
# per-process BUILDX_BUILDER environment variable is immune.
set -uo pipefail

WORKFLOW_DIR="${1:-.github/workflows}"
rc=0

# Jobs touching Docker must isolate DOCKER_CONFIG.
while IFS= read -r violation; do
  echo "DOCKER STATE VIOLATION: ${violation}" >&2
  rc=1
done < <(
  awk '
    FILENAME != prev_file {
      if (job != "") flush()
      prev_file = FILENAME; in_jobs = 0; job = ""
    }
    /^jobs:[[:space:]]*$/ { in_jobs = 1; next }
    !in_jobs { next }
    # A top-level job key: exactly two spaces of indent, then "<id>:".
    /^  [A-Za-z0-9_-]+:[[:space:]]*$/ {
      if (job != "") flush()
      job = $0; sub(/^  /, "", job); sub(/:.*$/, "", job)
      job_file = FILENAME; body = ""; next
    }
    job != "" { body = body "\n" $0 }
    END { if (job != "") flush() }

    function flush() {
      uses_docker = (body ~ /docker\/(login|setup-buildx|build-push)-action/) \
                 || (body ~ /(^|[^-a-z])docker (build|buildx|compose|image|pull|push|run|tag)/)
      if (uses_docker && body !~ /DOCKER_CONFIG:/)
        printf "%s job \"%s\" uses Docker without a private DOCKER_CONFIG (add DOCKER_CONFIG: ${{ runner.temp }}/.docker to the job env)\n", job_file, job
    }
  ' "$WORKFLOW_DIR"/*.yml
)

# `docker buildx use` mutates the shared pointer instead of pinning per process.
if grep -rnE '^[^#]*docker buildx use' "$WORKFLOW_DIR"/*.yml >/dev/null 2>&1; then
  echo "DOCKER STATE VIOLATION: 'docker buildx use' writes the shared builder pointer; pin BUILDX_BUILDER in the job env instead" >&2
  grep -rnE '^[^#]*docker buildx use' "$WORKFLOW_DIR"/*.yml >&2
  rc=1
fi

# The CI job builds images FROM local-only tags (continuo-base, dbt-base,
# s3-sidecar), which only the docker driver can resolve.
if ! grep -qE '^\s*BUILDX_BUILDER:\s*default\s*$' "$WORKFLOW_DIR/ci.yml"; then
  echo "DOCKER STATE VIOLATION: ${WORKFLOW_DIR}/ci.yml must pin BUILDX_BUILDER: default — its builds resolve local-only base tags from the daemon image store" >&2
  rc=1
fi

[ "$rc" -eq 0 ] && echo "Docker state isolation OK: ${WORKFLOW_DIR}"
exit "$rc"
