#!/usr/bin/env bash
# Chart lint gate for deploy/continuo: helm lint + helm template across every
# supported values topology, then kube-linter over each rendered manifest, a
# NetworkPolicy reachability check on the default render, plus a bash -n syntax
# check on the release-flow scripts (they have no other CI gate). Runs
# identically in CI (install-test.yml) and locally; needs helm, Go, and
# Python 3 with PyYAML (both preinstalled on the CI runner).
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

# The chart default-denies ingress and re-opens exactly the edges the service
# graph needs. A service that starts calling another without its allow rule
# ships a platform that is "up" but silently unreachable on that path. Assert
# the required cross-service edges are present in the default render (which has
# networkPolicy.enabled=true).
echo "--- network policy reachability (defaults)"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/defaults.yaml" continuo

# A service reads its HTTP port under one specific env name. The chart sets a
# differently named variable that works only while the default happens to equal
# the Deployment's containerPort, and changing httpPort then moves the probes
# and Service but not the listener. Assert each service gets its port under
# the name its own code reads.
echo "--- http port env names match what each service reads (defaults)"
python3 scripts/install-test/assert-port-env.py "${tmp}/defaults.yaml" .

# A bundled Redis is a StatefulSet with no ordering guarantee against the
# Deployments, so every service whose code connects to Redis must wait for it
# in bundled mode, and none may wait in BYO mode, where Redis is already up.
# Which services connect to Redis is read from their source.
echo "--- wait-for-redis gates every Redis-using service (defaults) and none (byo)"
python3 scripts/install-test/assert-redis-gate.py . "${tmp}/defaults.yaml" \
  "${tmp}/byo-example.yaml" "${tmp}/byo-inline.yaml" "${tmp}/byo-secret.yaml"

# Services read the shared ConfigMap through envFrom, and Kubernetes never
# refreshes environment variables in a running pod when that ConfigMap changes.
# The pod template therefore carries a checksum of it, so `helm upgrade` rolls
# the pods whose configuration actually changed. Two properties have to hold,
# and they pull in opposite directions:
#   - it must CHANGE when shared config changes, or the fix is inert and a pod
#     keeps serving stale config forever (for validation.engine that means
#     topology-controller uploading one engine's SQL to another's validator);
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
       d && /topology-controller/{f=1}
       f && /checksum\/config:/ && !printed {print $2; printed=1}' \
    "${tmp}/checksum-probe.yaml"
}
base="$(mc_checksum)"
[ -n "$base" ] || { echo "FAIL: no checksum/config on the topology-controller pod template"; exit 1; }
[ "$(mc_checksum)" = "$base" ] || { echo "FAIL: checksum/config is unstable across identical renders — every upgrade would roll every pod"; exit 1; }
engine_changed="$(mc_checksum --set validation.engine=trino --set validation.createWarehouseSecret=false --set validation.warehouseSecret=byo-warehouse)"
[ "$engine_changed" != "$base" ] || { echo "FAIL: changing validation.engine leaves the topology-controller pod template identical — an engine change would not roll it, so it would keep the dialect it resolved at boot"; exit 1; }
loglevel_changed="$(mc_checksum --set global.logLevel=DEBUG)"
[ "$loglevel_changed" != "$base" ] || { echo "FAIL: changing a shared ConfigMap value leaves the pod template identical"; exit 1; }

# state, execution-controller and agent-remediation read their mounted chart
# ConfigMap (schedules, dbt commands, service repos) only at startup, so each
# pod template carries a digest of it. For every chart-managed ConfigMap, a
# change must roll exactly the workloads that read it at startup and no others;
# with the volume pointed at an operator-owned ConfigMap it must roll nothing;
# and every chart ConfigMap a workload consumes must have a case in the script.
echo "--- chart ConfigMap changes roll exactly the pods that read them at startup"
python3 scripts/install-test/assert-config-rollout.py "$CHART" "$KUBE_VERSION"

# ciAuth.bindings.*.repositoryId must be a quoted digit string: GitHub's
# repository_id claim is a string, so an unquoted YAML number would render a
# binding the ui can never match. The schema refuses it at render time. A valid
# binding must reach the ui's ci-auth.json and roll the ui pod, and an empty
# audience must resolve to the origin of auth.publicUrl.
echo "--- ciAuth: unquoted repositoryId is refused by the schema"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set 'ciAuth.bindings.core[0].repositoryId=812345678' > /dev/null 2> "${tmp}/ciauth.err"; then
  echo "FAIL: an unquoted (numeric) repositoryId rendered; the ui would never match it"; exit 1
fi
grep -q 'repositoryId' "${tmp}/ciauth.err" || { echo "FAIL: unexpected error:"; cat "${tmp}/ciauth.err"; exit 1; }

echo "--- ciAuth: bindings render into ci-auth.json and roll the ui"
ui_ciauth_checksum() {
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$@" > "${tmp}/ciauth-probe.yaml"
  awk '/^kind: Deployment$/{d=1; f=0}
       d && /app.kubernetes.io\/name: ui$/{f=1}
       f && /checksum\/ci-auth:/ && !printed {print $2; printed=1}' "${tmp}/ciauth-probe.yaml"
}
ci_base="$(ui_ciauth_checksum)"
[ -n "$ci_base" ] || { echo "FAIL: no checksum/ci-auth on the ui pod template"; exit 1; }
ci_bound="$(ui_ciauth_checksum --set-string 'ciAuth.bindings.core[0].repositoryId=812345678')"
[ "$ci_bound" != "$ci_base" ] || { echo "FAIL: changing ciAuth.bindings leaves the ui pod template identical"; exit 1; }
# The probe now holds the bound render; ci-auth.json is a quoted JSON string, so
# its inner quotes appear escaped.
grep -q '\\"repositoryId\\":\\"812345678\\"' "${tmp}/ciauth-probe.yaml" || { echo "FAIL: binding missing from ci-auth.json"; exit 1; }
grep -q '\\"audience\\":\\"http://localhost:8090\\"' "${tmp}/ciauth-probe.yaml" || { echo "FAIL: audience did not default to the origin of auth.publicUrl"; exit 1; }

# The ui's CI-auth env, mount and volume come from the template, not from its
# `services` entry: Helm replaces lists wholesale, so an operator's own
# `services` list must not silently disable CI auth. ciAuth itself is optional,
# so `helm upgrade --reuse-values` from a chart without it still renders, with
# the GitHub issuer and no bindings (no CI access).
echo "--- ciAuth: the ui gets CI auth from the template"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/defaults.yaml" continuo
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  -f scripts/install-test/values-ui-minimal-services.yaml > "${tmp}/ui-minimal-services.yaml"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/ui-minimal-services.yaml" continuo
echo "--- ciAuth: a release without ciAuth values renders the defaults"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set ciAuth=null > "${tmp}/ciauth-null.yaml" \
  || { echo "FAIL: rendering with ciAuth=null failed; helm upgrade --reuse-values from a chart without ciAuth would fail"; exit 1; }
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/ciauth-null.yaml" continuo --default-ci-auth

# The "continuo-api-" Secret-name prefix is reserved for the operator-created
# Secrets a python-api node contract may name. A chart fullname inside it would
# name every chart-created Secret inside it, and so would a user-supplied
# Secret name; either would let a contract receive platform credentials. The
# render must fail with the reserved-prefix message, and a name that merely
# shares the letters ("continuo-apis") must still render.
echo "--- reserved continuo-api- Secret prefix is refused"
refuse_reserved() {
  local release="$1"; shift
  if helm template "$release" "$CHART" --kube-version "$KUBE_VERSION" "$@" > /dev/null 2> "${tmp}/reserved.err"; then
    echo "FAIL: rendering release ${release} $* succeeded; a name inside the reserved continuo-api- prefix must fail the render"; exit 1
  fi
  grep -qF 'reserved "continuo-api-" prefix' "${tmp}/reserved.err" \
    || { echo "FAIL: release ${release} $* failed for another reason:"; cat "${tmp}/reserved.err"; exit 1; }
}
refuse_reserved continuo-api
refuse_reserved continuo-api-prod
refuse_reserved continuo --set fullnameOverride=continuo-api
refuse_reserved continuo --set s3.existingSecret=continuo-api-s3
refuse_reserved continuo --set validation.createWarehouseSecret=false --set validation.warehouseSecret=continuo-api-warehouse
helm template continuo-apis "$CHART" --kube-version "$KUBE_VERSION" > /dev/null \
  || { echo "FAIL: release continuo-apis is outside the reserved prefix and must render"; exit 1; }

# database.pool reaches every Go service through the shared ConfigMap only when
# set (0 keeps each service's default). An idle limit above the open limit, or
# an idle limit with no open limit, fails the render instead of producing a
# release a service would refuse to boot with. A release without a database
# block (helm upgrade --reuse-values from an older chart) renders the defaults.
echo "--- database.pool reaches the ConfigMap only when set; idle above open and idle alone are refused"
if grep -q 'DB_MAX_OPEN_CONNS\|DB_MAX_IDLE_CONNS\|DB_POOL_SIZE\|DB_MAX_OVERFLOW' "${tmp}/defaults.yaml"; then
  echo "FAIL: the default render sets a pool limit; every service must keep its own default"; exit 1
fi
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set database.pool.maxOpenConns=40 --set database.pool.maxIdleConns=8 > "${tmp}/pool-set.yaml"
grep -q 'DB_MAX_OPEN_CONNS: "40"' "${tmp}/pool-set.yaml" && grep -q 'DB_MAX_IDLE_CONNS: "8"' "${tmp}/pool-set.yaml" \
  || { echo "FAIL: database.pool does not reach the shared ConfigMap"; exit 1; }
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set database.pool.maxOpenConns=3 --set database.pool.maxIdleConns=5 > /dev/null 2>&1; then
  echo "FAIL: maxIdleConns above maxOpenConns must fail the render"; exit 1
fi
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set database.pool.maxIdleConns=5 > /dev/null 2>&1; then
  echo "FAIL: maxIdleConns without maxOpenConns must fail the render"; exit 1
fi
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set database=null > "${tmp}/database-null.yaml" \
  || { echo "FAIL: rendering with database=null failed; helm upgrade --reuse-values from a chart without database.pool would fail"; exit 1; }
if grep -q 'DB_MAX_OPEN_CONNS\|DB_MAX_IDLE_CONNS' "${tmp}/database-null.yaml"; then
  echo "FAIL: a release without database values must leave every service on its default pool"; exit 1
fi

echo "--- bash -n (release scripts)"
bash -n scripts/release/retag-images.sh

echo "Chart lint OK across ${#renders[@]} topologies."
