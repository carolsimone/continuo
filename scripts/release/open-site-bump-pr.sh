#!/usr/bin/env bash
# Opens the pull request in the continuo-site repository that moves the
# site's quickstart chart pin (the `chartVersion: '...'` line of
# site.config.ts) to a published chart version. Nothing else bumps that pin,
# so the quickstart keeps installing an older chart until this runs.
#
# Run it after release.yml has published vX.Y.Z and the pull request that pins
# this repository's install guides to X.Y.Z has merged. It checks that the
# GitHub release vX.Y.Z exists, clones the site repository into a temporary
# directory with `gh repo clone`, branches chore/bump-chart-X.Y.Z off
# origin/main, rewrites the chartVersion line, commits with a sign-off, pushes
# the branch and opens the pull request with `gh pr create`. It fails, before
# pushing anything, when the release does not exist, the branch already exists
# on the site repository, or site.config.ts holds no chartVersion line, more
# than one, or one that already names X.Y.Z. It needs gh signed in with push
# access to the site repository; it creates no repository secret.
#
# Usage: open-site-bump-pr.sh X.Y.Z
#
# Environment (defaults in brackets; the tests point them at a local fixture):
#   SITE_REPO        the site repository [carolsimone/continuo-site]
#   CHART_REPO       the repository whose release the pull request links [carolsimone/continuo]
#   GH               the gh executable [gh]
#   COMMIT_TRAILERS  trailer lines that end the commit message, above the
#                    sign-off [the Co-Authored-By and Claude-Session lines
#                    below]; empty adds none
set -euo pipefail

SITE_REPO="${SITE_REPO:-carolsimone/continuo-site}"
CHART_REPO="${CHART_REPO:-carolsimone/continuo}"
GH="${GH:-gh}"
DEFAULT_TRAILERS='Co-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PSskScdqXuRyxr36mAaXUi'
COMMIT_TRAILERS="${COMMIT_TRAILERS-$DEFAULT_TRAILERS}"
CONFIG=site.config.ts
CHART_REF="oci://ghcr.io/carolsimone/charts/continuo"

die() { echo "open-site-bump-pr: $*" >&2; exit 1; }

[ "$#" -eq 1 ] || die "usage: open-site-bump-pr.sh X.Y.Z"
version="$1"
if ! [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  die "'${version}' is not a chart version (X.Y.Z, without a leading v)"
fi
branch="chore/bump-chart-${version}"
release_url="https://github.com/${CHART_REPO}/releases/tag/v${version}"

if ! "$GH" release view "v${version}" --repo "$CHART_REPO" >/dev/null 2>&1; then
  die "${CHART_REPO} has no release v${version}; run this once release.yml has published it"
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
site="${work}/site"
"$GH" repo clone "$SITE_REPO" "$site" >/dev/null
git -C "$site" fetch -q origin main

if git -C "$site" ls-remote --exit-code --heads origin "$branch" >/dev/null 2>&1; then
  die "${SITE_REPO} already has a branch ${branch}; its pull request may already be open"
fi

config="${site}/${CONFIG}"
[ -f "$config" ] || die "${SITE_REPO} has no ${CONFIG}"
pin_re="^[[:space:]]*chartVersion:[[:space:]]*'[^']*'"
matches="$(grep -cE "$pin_re" "$config" || true)"
if [ "$matches" -eq 0 ]; then
  die "no \"chartVersion: '...'\" line in ${SITE_REPO}'s ${CONFIG}"
elif [ "$matches" -gt 1 ]; then
  die "${matches} \"chartVersion: '...'\" lines in ${SITE_REPO}'s ${CONFIG}; expected one"
fi
current="$(grep -E "$pin_re" "$config" | sed -E "s/^[^']*'([^']*)'.*/\1/")"
if [ "$current" = "$version" ]; then
  die "${SITE_REPO}'s ${CONFIG} already pins chartVersion '${version}'"
fi

git -C "$site" checkout -q -b "$branch" origin/main
sed -E "s/^([[:space:]]*chartVersion:[[:space:]]*)'[^']*'/\1'${version}'/" "$config" > "${config}.new"
mv "${config}.new" "$config"

title="chore: bump quickstart chart version to ${version}"
message="${title}

The continuo chart ${version} is published (${CHART_REF}, ${release_url}),
so the quickstart installs it instead of ${current}."
if [ -n "$COMMIT_TRAILERS" ]; then
  message="${message}

${COMMIT_TRAILERS}"
fi
git -C "$site" add "$CONFIG"
git -C "$site" commit -q -s -F - <<< "$message"
git -C "$site" push -q -u origin "$branch"

body="The continuo Helm chart ${version} is published:

- chart: \`${CHART_REF}\`, version \`${version}\`
- release: ${release_url}

This moves the quickstart's \`chartVersion\` in \`${CONFIG}\` from \`${current}\` to \`${version}\`."
"$GH" pr create --repo "$SITE_REPO" --base main --head "$branch" --title "$title" --body "$body"
