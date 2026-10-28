#!/usr/bin/env bash
# Runs scripts/ci-disk-usage.sh against stub `df` and `docker` commands and
# checks when its CI_MIN_FREE_GIB gate stops the job.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; S="${HERE}/ci-disk-usage.sh"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT; fail=0
assert(){ if eval "$2"; then echo "ok - $1"; else echo "NOT OK - $1"; fail=1; fi; }

GIB_KIB=$((1024 * 1024))
mkdir "$tmp/bin"
# `df -Pk /` reports STUB_AVAIL_KIB available; `df -h /` is a fixed table.
cat > "$tmp/bin/df" <<'EOF'
#!/usr/bin/env bash
case "$*" in
  *-Pk*) printf 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/sda1 78643200 74000000 %s 99%% /\n' "$STUB_AVAIL_KIB";;
  *)     printf 'Filesystem Size Used Avail Use%% Mounted on\n/dev/sda1 75G 71G 831M 99%% /\n';;
esac
EOF
printf '#!/usr/bin/env bash\nexit 0\n' > "$tmp/bin/docker"
chmod +x "$tmp/bin/df" "$tmp/bin/docker"

# run AVAIL_KIB [MIN_GIB]: the script's exit code lands in $rc, its output in $tmp/out.
# Without MIN_GIB the variable is unset, as on a runner that has no gate.
run(){
  if [ $# -ge 2 ]; then
    PATH="$tmp/bin:$PATH" STUB_AVAIL_KIB="$1" CI_MIN_FREE_GIB="$2" bash "$S" "test stage" >"$tmp/out" 2>&1
  else
    PATH="$tmp/bin:$PATH" STUB_AVAIL_KIB="$1" bash "$S" "test stage" >"$tmp/out" 2>&1
  fi
  rc=$?
}
# shellcheck disable=SC2329  # called through assert's eval
said(){ grep -q -- "$1" "$tmp/out"; }

run 1000
assert "no gate: a nearly full disk passes when CI_MIN_FREE_GIB is unset" "[ $rc -eq 0 ]"
assert "no gate: the usage summary is still printed" "said 'Disk usage — test stage: 71G used of 75G, 831M free (99%)'"

run 1000 ""
assert "an empty CI_MIN_FREE_GIB is no gate" "[ $rc -eq 0 ]"

run $((10 * GIB_KIB)) 6
assert "free space above the minimum passes" "[ $rc -eq 0 ]"

run $((6 * GIB_KIB)) 6
assert "free space equal to the minimum passes" "[ $rc -eq 0 ]"

run $((6 * GIB_KIB - 1)) 6
assert "one KiB under the minimum stops the job" "[ $rc -eq 1 ]"
assert "the stop is an error annotation naming the stage" "said '::error::Disk usage — test stage'"
assert "the stop names the minimum" "said '6 GiB'"
assert "the usage summary is printed before the stop" "said 'Disk usage — test stage: 71G used'"

run $((10 * GIB_KIB)) six
assert "a minimum that is not a whole number fails closed" "[ $rc -eq 2 ]"
assert "the failure names the variable" "said 'CI_MIN_FREE_GIB'"

run $((10 * GIB_KIB)) 6.5
assert "a fractional minimum fails closed" "[ $rc -eq 2 ]"

run $((9 * GIB_KIB)) 08
assert "a minimum with a leading zero is read as decimal" "[ $rc -eq 0 ]"

exit "$fail"
