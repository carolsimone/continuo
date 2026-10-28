#!/usr/bin/env bash
# Prints how full the runner's root disk is, and how Docker splits it, under a
# label naming the CI stage. The job log keeps one block per stage, so a run
# that dies of a full disk shows where the space went. On GitHub Actions the
# one-line summary also lands on the run page.
#
# Usage: scripts/ci-disk-usage.sh <stage label>
set -u

label="${1:-disk}"

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
