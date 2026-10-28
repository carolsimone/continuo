#!/usr/bin/env bash
# Runs scripts/ci-free-disk.sh against a stub `docker` that records its calls
# and a stub `df`, and checks what it removes and what it leaves alone.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/ci-free-disk.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

mkdir "$tmp/bin"
# docker appends its arguments to STUB_DOCKER_LOG; a call whose arguments start
# with STUB_DOCKER_FAIL exits 1.
cat > "$tmp/bin/docker" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$STUB_DOCKER_LOG"
case "$*" in "${STUB_DOCKER_FAIL:-__none__}"*) exit 1;; esac
exit 0
EOF
cat > "$tmp/bin/df" <<'EOF'
#!/usr/bin/env bash
printf 'Filesystem Size Used Avail Use%% Mounted on\n/dev/sda1 75G 40G 31G 57%% /\n'
EOF
chmod +x "$tmp/bin/docker" "$tmp/bin/df"

# run [FAILING-DOCKER-ARGS]: the script's exit code lands in $rc, its output in
# $tmp/out and the docker calls it made in $tmp/docker.log.
run(){
  : > "$tmp/docker.log"
  PATH="$tmp/bin:$PATH" STUB_DOCKER_LOG="$tmp/docker.log" STUB_DOCKER_FAIL="${1:-}" bash "$S" >"$tmp/out" 2>&1
  rc=$?
}

run
assert "it succeeds when docker does" "[ $rc -eq 0 ]"
assert "it removes every unused image" "grep -qx 'image prune -af' '$tmp/docker.log'"
assert "removing images is its only prune" "[ \"\$(grep -c prune '$tmp/docker.log')\" -eq 1 ]"
assert "it passes no age filter, so the previous job's images go too" "! grep -q -- '--filter' '$tmp/docker.log'"
assert "it reports the disk after the removal" "grep -q 'Disk usage — after removing unused images: 40G used of 75G' '$tmp/out'"

run 'image prune'
assert "a failed removal fails the script" "[ $rc -ne 0 ]"

exit "$fail"
