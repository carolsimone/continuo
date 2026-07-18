#!/usr/bin/env bash
# Chart lint gate for deploy/continuo: helm lint + helm template across every
# supported values topology, then kube-linter over each rendered manifest,
# plus a bash -n syntax check on the release-flow scripts (they have no
# other CI gate). Runs identically in CI (install-test.yml) and locally;
# needs helm and Go.
set -euo pipefail
cd "$(dirname "$0")/../.."

CHART=deploy/continuo
# The chart's kubeVersion gate (>=1.27.0-0) rejects helm's older client-default
# capability set, so every offline render must state a real cluster version.
KUBE_VERSION="${KUBE_VERSION:-1.29.0}"
# Version-pinned `go run` needs no PATH setup and works the same on CI runners
# and dev machines; the module build is cached after the first invocation.
KUBE_LINTER=(go run golang.stackrox.io/kube-linter/cmd/kube-linter@v0.7.1)

# name:values-file — one entry per supported topology ('' = chart defaults).
# values-byo.yaml.example is the operator-facing example and must keep
# rendering; the two fixture files are what the install jobs actually use.
renders=(
  "defaults:"
  "byo-example:${CHART}/values-byo.yaml.example"
  "byo-inline:scripts/install-test/values-byo-inline.yaml"
  "byo-secret:scripts/install-test/values-byo-secret.yaml"
)

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

for entry in "${renders[@]}"; do
  name="${entry%%:*}"
  values="${entry#*:}"
  args=(--kube-version "$KUBE_VERSION")
  if [ -n "$values" ]; then
    args+=(-f "$values")
  fi
  echo "--- helm lint (${name})"
  helm lint "$CHART" "${args[@]}"
  echo "--- helm template (${name})"
  helm template continuo "$CHART" "${args[@]}" > "${tmp}/${name}.yaml"
  echo "--- kube-linter (${name})"
  "${KUBE_LINTER[@]}" lint --config "${CHART}/.kube-linter.yaml" "${tmp}/${name}.yaml"
done

echo "--- bash -n (release scripts)"
bash -n scripts/release/retag-images.sh

echo "Chart lint OK across ${#renders[@]} topologies."
