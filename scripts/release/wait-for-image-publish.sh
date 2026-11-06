#!/usr/bin/env bash
# Waits until deploy.yml has published the images of a release tag's commit, so a
# tag can be pushed as soon as its commit is on main.
#
# Every push to main that changes a path in deploy.yml's release filter publishes
# a :<commit-sha> of every image. deploy/continuo/CHANGELOG.md is such a path, so a
# release cut that changes only the changelog publishes like any other commit.
# That run queues behind others on the shared self-hosted runners, often for an
# hour or more, and retag-images.sh fails on a commit whose images are not there
# yet.
#
# It exits 0 as soon as one of two things holds:
#   - a deploy.yml run of the commit has finished every build-publish job with
#     success, even if the run's deploy job is still going;
#   - no deploy.yml run of the commit is still going. A commit with no run at all
#     gets NO_RUN_GRACE_SECONDS for its run to appear first. This script decides
#     nothing about such a commit: retag-images.sh, which runs next, refuses it
#     when an image is missing and says what to do.
# A failure to read the runs is retried, and the workflow job's timeout bounds the
# whole wait.
#
# Usage: wait-for-image-publish.sh <commit-sha>   (needs gh signed in, in a clone
# of the repository, with actions: read)
#
# Environment (defaults in brackets; the tests set them):
#   GH                    the gh executable [gh]
#   POLL_SECONDS          the pause between checks [60]
#   NO_RUN_GRACE_SECONDS  how long a commit with no run is waited for [300]
set -uo pipefail

SHA="${1:?usage: wait-for-image-publish.sh <commit-sha>}"
GH="${GH:-gh}"
POLL_SECONDS="${POLL_SECONDS:-60}"
NO_RUN_GRACE_SECONDS="${NO_RUN_GRACE_SECONDS:-300}"

# published RUN_ID: succeeds when the run has build-publish jobs and every one has
# succeeded. The matrix lists all its jobs at once, so a run cannot pass on some.
published() {
  [ "$("$GH" run view "$1" --json jobs \
    --jq '[.jobs[] | select(.name | startswith("build-publish"))] | length > 0 and all(.conclusion == "success")' \
    2>/dev/null)" = "true" ]
}

started="$SECONDS"
while :; do
  if runs="$("$GH" run list --workflow deploy.yml --commit "$SHA" --limit 20 --json databaseId,status \
    --jq '.[] | "\(.databaseId) \(.status)"')"; then
    seen=0
    going=0
    while read -r run_id status; do
      [ -n "$run_id" ] || continue
      seen=1
      if published "$run_id"; then
        echo "deploy.yml run ${run_id} has published the images of ${SHA}."
        exit 0
      fi
      if [ "$status" != completed ]; then
        going=1
      fi
    done <<< "$runs"

    if [ "$going" -eq 0 ]; then
      if [ "$seen" -eq 1 ]; then
        echo "No deploy.yml run of ${SHA} is still going, and none published its images."
        exit 0
      fi
      if [ $((SECONDS - started)) -ge "$NO_RUN_GRACE_SECONDS" ]; then
        echo "${SHA} has no deploy.yml run after ${NO_RUN_GRACE_SECONDS} s."
        exit 0
      fi
    fi
    echo "Waiting for deploy.yml to publish the images of ${SHA}..."
  else
    echo "Could not read the deploy.yml runs of ${SHA}; trying again."
  fi
  sleep "$POLL_SECONDS"
done
