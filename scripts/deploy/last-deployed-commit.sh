#!/usr/bin/env bash
# Prints the commit deploy.yml detects changes from: the newest commit before
# HEAD, on HEAD's history, that a push-triggered deploy.yml run deployed (its
# `deploy` job succeeded). Every change since that commit counts as not yet
# deployed, those of a deploy run that was cancelled while it waited behind
# another included. A run that deployed nothing (no release path changed, or a
# force_publish dispatch) does not count, and neither does HEAD itself, so a
# re-run detects the same changes as the run it repeats.
#
# When none of the newest 50 successful runs deployed such a commit, it prints
# the repository's root commit, so every service counts as changed, and says so
# on stderr. It fails when the GitHub API cannot be read.
#
# Usage: last-deployed-commit.sh   (in a full clone; needs GITHUB_REPOSITORY and
# GITHUB_TOKEN; GITHUB_API_URL defaults to https://api.github.com)
set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must name the repository (owner/name)}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN must be set}"
api="${GITHUB_API_URL:-https://api.github.com}"
head="$(git rev-parse HEAD)"

# get PATH: the GitHub API's answer for repos/<repository>/PATH.
get() {
  curl -fsSL --retry 3 \
    -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "${api}/repos/${GITHUB_REPOSITORY}/$1"
}

runs="$(get "actions/workflows/deploy.yml/runs?branch=main&event=push&status=success&per_page=50" \
  | jq -r '.workflow_runs[] | "\(.id) \(.head_sha)"')"
while read -r run_id sha; do
  if [ -z "${run_id}" ] || [ "${sha}" = "${head}" ]; then
    continue
  fi
  git merge-base --is-ancestor "${sha}" "${head}" 2>/dev/null || continue
  deployed="$(get "actions/runs/${run_id}/jobs?per_page=100" \
    | jq '[.jobs[] | select(.name == "deploy" and .conclusion == "success")] | length')"
  if [ "${deployed}" -gt 0 ]; then
    echo "${sha}"
    exit 0
  fi
done <<< "${runs}"

echo "last-deployed-commit.sh: no deploy of a commit before ${head} among the newest successful deploy runs;" \
  "detecting changes from the root commit, so every service is rebuilt" >&2
git rev-list --max-parents=0 HEAD | tail -n 1
