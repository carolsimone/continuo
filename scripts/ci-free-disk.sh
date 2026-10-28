#!/usr/bin/env bash
# Removes every Docker image no container uses, then prints how full the disk
# is. Run at the start of a job on a self-hosted runner, where the images the
# previous jobs built stay behind between runs. The build cache is kept, so the
# job's builds still hit it, and scripts/setup.sh builds or pulls every image it
# needs.
#
# Usage: scripts/ci-free-disk.sh
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

docker image prune -af
bash "${HERE}/ci-disk-usage.sh" "after removing unused images"
