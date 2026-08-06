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

# Services read the shared ConfigMap through envFrom, and Kubernetes never
# refreshes environment variables in a running pod when that ConfigMap changes.
# The pod template therefore carries a checksum of it, so `helm upgrade` rolls
# the pods whose configuration actually changed. Two properties have to hold,
# and they pull in opposite directions:
#   - it must CHANGE when shared config changes, or the fix is inert and a pod
#     keeps serving stale config forever (for validation.engine that means
#     manifest-controller uploading one engine's SQL to another's validator);
#   - it must be STABLE when nothing changes, or every upgrade needlessly
#     restarts the whole platform.
echo "--- configmap checksum rolls pods on config change (and only then)"
# Renders to a file rather than piping into awk: an awk that exits early would
# SIGPIPE helm, and `set -o pipefail` would abort this script on what is
# actually a success.
# Renders to a file rather than piping into awk: an awk that exits early would
# SIGPIPE helm, and `set -o pipefail` would abort this script on what is
# actually a success. Prints nothing (exit 0) when the annotation is absent, so
# the explicit check below reports it instead of `set -e` killing the run
# without a diagnostic.
mc_checksum() {
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$@" > "${tmp}/checksum-probe.yaml"
  awk '/^kind: Deployment$/{d=1; f=0}
       d && /manifest-controller/{f=1}
       f && /checksum\/config:/ && !printed {print $2; printed=1}' \
    "${tmp}/checksum-probe.yaml"
}
base="$(mc_checksum)"
[ -n "$base" ] || { echo "FAIL: no checksum/config on the manifest-controller pod template"; exit 1; }
[ "$(mc_checksum)" = "$base" ] || { echo "FAIL: checksum/config is unstable across identical renders — every upgrade would roll every pod"; exit 1; }
engine_changed="$(mc_checksum --set validation.engine=trino --set validation.createWarehouseSecret=false --set validation.warehouseSecret=byo-warehouse)"
[ "$engine_changed" != "$base" ] || { echo "FAIL: changing validation.engine leaves the manifest-controller pod template identical — an engine change would not roll it, so it would keep the dialect it resolved at boot"; exit 1; }
loglevel_changed="$(mc_checksum --set global.logLevel=DEBUG)"
[ "$loglevel_changed" != "$base" ] || { echo "FAIL: changing a shared ConfigMap value leaves the pod template identical"; exit 1; }

echo "--- bash -n (release scripts)"
bash -n scripts/release/retag-images.sh

echo "Chart lint OK across ${#renders[@]} topologies."
