#!/usr/bin/env bash
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; G="${HERE}/check-dev-dockerfile-nonroot.sh"
fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

# Writes a fresh fixture repo under $1: an .trivyignore.yaml excluding the
# dev/base images, plus a matching set of Dockerfiles. Callers then mutate the
# tree to exercise one branch of the guard.
write_fixture() {
  local dir="$1"
  rm -rf "$dir"; mkdir -p "$dir/svc-a" "$dir/svc-b"

  cat > "$dir/.trivyignore.yaml" <<'YAML'
misconfigurations:
  - id: AVD-DS-0002 # DS002: image USER should not be 'root'
    paths:
      - "Dockerfile.base"
      - "svc-a/Dockerfile.dev"
      - "svc-b/Dockerfile.dev"
YAML

  printf 'FROM golang:1\nWORKDIR /app\n'            > "$dir/Dockerfile.base"
  printf 'FROM base\nCOPY . .\n'                     > "$dir/svc-a/Dockerfile.dev"
  printf 'FROM base\nCOPY . .\n'                     > "$dir/svc-b/Dockerfile.dev"
  # A production image with its own non-root USER — must never need an entry.
  printf 'FROM base\nUSER 65532:65532\n'            > "$dir/svc-a/Dockerfile.prod"
}

tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

# 1. Fully-covered tree passes.
write_fixture "$tmp/ok"
bash "$G" "$tmp/ok" >/dev/null 2>&1
assert "complete coverage passes" "[ $? -eq 0 ]"

# 2. A new root dev image with no exclusion fails.
write_fixture "$tmp/uncovered"; mkdir -p "$tmp/uncovered/svc-c"
printf 'FROM base\nCOPY . .\n' > "$tmp/uncovered/svc-c/Dockerfile.dev"
out="$(bash "$G" "$tmp/uncovered" 2>&1)"; rc=$?
assert "unlisted root dev image fails" "[ $rc -ne 0 ]"
assert "the message names the offending file" "printf '%s' \"\$out\" | grep -q 'svc-c/Dockerfile.dev'"

# 3. A dev image that sets its own non-root USER needs no exclusion.
write_fixture "$tmp/user"; mkdir -p "$tmp/user/svc-c"
printf 'FROM base\nUSER 65532:65532\n' > "$tmp/user/svc-c/Dockerfile.dev"
bash "$G" "$tmp/user" >/dev/null 2>&1
assert "unlisted dev image with a non-root USER passes" "[ $? -eq 0 ]"

# 4. An explicit `USER root` counts as root and must be excluded.
write_fixture "$tmp/rootuser"; mkdir -p "$tmp/rootuser/svc-c"
printf 'FROM base\nUSER root\n' > "$tmp/rootuser/svc-c/Dockerfile.dev"
bash "$G" "$tmp/rootuser" >/dev/null 2>&1
assert "unlisted 'USER root' dev image fails" "[ $? -ne 0 ]"

# 5. A stale exclusion (path no longer on disk) fails.
write_fixture "$tmp/stale"; rm -f "$tmp/stale/svc-b/Dockerfile.dev"
out="$(bash "$G" "$tmp/stale" 2>&1)"; rc=$?
assert "stale exclusion path fails" "[ $rc -ne 0 ]"
assert "the message names the stale entry" "printf '%s' \"\$out\" | grep -q 'svc-b/Dockerfile.dev'"

[ "$fail" -eq 0 ] && echo "PASS" || echo "FAIL"
exit "$fail"
