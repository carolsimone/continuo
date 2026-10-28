#!/usr/bin/env bash
# Prints how full the runner's root disk is, and how Docker splits it, under a
# label naming the CI stage. The job log keeps one block per stage, so a run
# that dies of a full disk shows where the space went. On GitHub Actions the
# one-line summary also lands on the run page.
#
# CI_MIN_FREE_GIB, when set, is the free space this stage needs, in whole GiB.
# Below it the script prints an error annotation and exits 1, so the job stops
# here with the numbers in its log instead of failing later on a write the disk
# can no longer take. A value that is not a whole number exits 2.
#
# Usage: scripts/ci-disk-usage.sh <stage label>
set -u

label="${1:-disk}"
min_free_gib="${CI_MIN_FREE_GIB:-}"

case "$min_free_gib" in
  *[!0-9]*)
    echo "::error::CI_MIN_FREE_GIB must be a whole number of GiB, got '${min_free_gib}'"
    exit 2
    ;;
esac

echo "::group::Disk usage — ${label}"
df -h /
docker system df 2>/dev/null || true
echo "::endgroup::"

# df's last line is "<fs> <size> <used> <avail> <use%> <mount>".
read -r size used avail pct _ < <(df -h / | tail -1 | awk '{print $2, $3, $4, $5}')
echo "Disk usage — ${label}: ${used} used of ${size}, ${avail} free (${pct})"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  echo "- **${label}**: ${used} used of ${size}, **${avail} free** (${pct})" >> "${GITHUB_STEP_SUMMARY}"
fi

if [ -n "$min_free_gib" ]; then
  # -P keeps one line per filesystem and counts in KiB, so the comparison does
  # not depend on df -h's rounding. 10# reads a leading zero as decimal.
  avail_kib="$(df -Pk / | awk 'NR==2 {print $4}')"
  if [ "$avail_kib" -lt $((10#$min_free_gib * 1024 * 1024)) ]; then
    echo "::error::Disk usage — ${label}: ${avail} free, below the ${min_free_gib} GiB this stage needs"
    exit 1
  fi
fi
