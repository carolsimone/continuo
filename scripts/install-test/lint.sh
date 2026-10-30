#!/usr/bin/env bash
# Chart lint gate for deploy/continuo: helm lint + helm template across every
# supported values topology, then kube-linter over each rendered manifest, a
# NetworkPolicy reachability check on each topology and on the egress
# variants, plus a bash -n syntax
# check on the release-flow scripts (they have no other CI gate). Runs
# identically in CI (install-test.yml) and locally; needs Helm 3.16 or newer
# (the egress checks render with --skip-schema-validation), Go, and Python 3
# with PyYAML (Helm and Python are preinstalled on the CI runner). It needs no
# Kubernetes cluster and no kubeconfig: every check renders offline with
# `helm template` or lints with `helm lint`, and nothing contacts an API server.
set -euo pipefail
cd "$(dirname "$0")/../.."

helm_version="$(helm version --template '{{.Version}}')"
helm_major="$(sed -E 's/^v([0-9]+)\..*/\1/' <<<"$helm_version")"
helm_minor="$(sed -E 's/^v[0-9]+\.([0-9]+).*/\1/' <<<"$helm_version")"
if ! [[ "$helm_major" =~ ^[0-9]+$ && "$helm_minor" =~ ^[0-9]+$ ]] \
  || [ "$helm_major" -lt 3 ] || { [ "$helm_major" -eq 3 ] && [ "$helm_minor" -lt 16 ]; }; then
  echo "FAIL: lint.sh needs Helm 3.16 or newer (it renders with --skip-schema-validation); found '${helm_version}'"
  exit 1
fi

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

echo "--- service enablement, replica counts and independent application ports"
python3 scripts/install-test/test-service-controls.py -v

# The chart default-denies ingress and re-opens exactly the edges the service
# graph needs. A service that starts calling another without its allow rule
# ships a platform that is "up" but silently unreachable on that path. With
# egress on, the agents reach the outside only through egress-proxy, and the
# proxy only what its allowlist names. Assert both, in each topology (each has
# networkPolicy.enabled=true); the checker reads what to expect from the
# render's allowlist and datastores. The egress variants are checked with the
# egress renders below.
for f in defaults byo-example byo-inline byo-secret; do
  echo "--- network policy reachability (${f})"
  python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/${f}.yaml" continuo
done

# A service reads its HTTP port under one specific env name. The chart sets a
# differently named variable that works only while the default happens to equal
# the Deployment's containerPort, and changing httpPort then moves the probes
# and Service but not the listener. Assert each service gets its port under
# the name its own code reads.
echo "--- http port env names match what each service reads (defaults)"
python3 scripts/install-test/assert-port-env.py "${tmp}/defaults.yaml" .

# run-controller's pods all consume one status consumer group and dispatch what
# a success unblocks, so the previous release's pod must be gone before the new
# one consumes anything: run-controller rolls with Recreate.
echo "--- run-controller rolls with Recreate (defaults)"
python3 scripts/install-test/assert-run-controller-recreate.py "${tmp}/defaults.yaml"

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

# execution-controller and agent-remediation read their mounted chart
# ConfigMap (dbt commands, service repos) only at startup, so each pod template
# carries a digest of it. For every chart-managed ConfigMap, a
# change must roll exactly the workloads that read it at startup and no others;
# with the volume pointed at an operator-owned ConfigMap it must roll nothing;
# and every chart ConfigMap a workload consumes must have a case in the script.
echo "--- chart ConfigMap changes roll exactly the pods that read them at startup"
python3 scripts/install-test/assert-config-rollout.py "$CHART" "$KUBE_VERSION"

# A services[].volumes entry with an empty configMap.name resolves to the chart's
# own ConfigMap only on a service the chart mounts one into (execution-controller,
# agent-remediation, ui). On any other service it would render a volume the API
# server refuses at apply time, so the render fails and names the entry instead.
echo "--- an empty configMap.name on a service with no chart ConfigMap fails the render"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     -f scripts/install-test/values-empty-configmap-volume.yaml > /dev/null 2> "${tmp}/emptyvolume.err"; then
  echo "FAIL: a run-controller volume with an empty configMap.name rendered; the API server would refuse the Deployment"; exit 1
fi
grep -qF 'services[run-controller].volumes[extra-config]: configMap.name is empty' "${tmp}/emptyvolume.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/emptyvolume.err"; exit 1; }

# NOTES.txt tells an upgrading operator that schedules are no longer read from
# the chart, and an installing one nothing about it. `helm template` prints no
# notes and `helm install|upgrade --dry-run=client` asks the API server for its
# version, so the check renders NOTES.txt as an ordinary template: in a copy of
# the chart, the file's text becomes a named template (`probe.notes`) that a
# ConfigMap includes, and `helm template --is-upgrade` or without it sets
# .Release.IsUpgrade to true or false for it.
echo "--- NOTES.txt: the schedules note prints on upgrade and not on install"
schedules_note='Schedules are no longer read from the chart: every cron stops until it is set again in the ui (Edit cron) or with `continuo schedule set-cron`.'
notes_chart="${tmp}/notes-chart"
cp -R "$CHART" "$notes_chart"
{
  printf '{{- define "probe.notes" -}}\n'
  cat "${notes_chart}/templates/NOTES.txt"
  printf '\n{{- end -}}\n'
} > "${notes_chart}/templates/_probe.tpl"
rm "${notes_chart}/templates/NOTES.txt"
cat > "${notes_chart}/templates/probe.yaml" <<'PROBE'
apiVersion: v1
kind: ConfigMap
metadata:
  name: notes-probe
data:
  notes.txt: |
{{ include "probe.notes" . | indent 4 }}
PROBE
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --is-upgrade --show-only templates/probe.yaml \
  > "${tmp}/notes-upgrade.txt" || { echo "FAIL: rendering NOTES.txt as an upgrade failed"; exit 1; }
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --show-only templates/probe.yaml \
  > "${tmp}/notes-install.txt" || { echo "FAIL: rendering NOTES.txt as an install failed"; exit 1; }
for notes in install upgrade; do
  grep -q '^ *ACCESS$' "${tmp}/notes-${notes}.txt" || { echo "FAIL: the ${notes} render printed no NOTES.txt"; exit 1; }
done
[ "$(grep -cF "$schedules_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once that schedules are no longer read from the chart"; exit 1; }
if grep -qF "$schedules_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the schedules upgrade note"; exit 1
fi
# An upgrading operator with catalog's backup on is told once what S3 access
# the exports need and how to turn them off; an installing one, and one who
# turned the backup off, are told nothing.
backup_note='To run no exports, set catalog.backup.enabled=false.'
[ "$(grep -cF "$backup_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once what S3 access catalog's backup needs"; exit 1; }
grep -qF 's3:AbortMultipartUpload' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the upgrade notes do not name s3:AbortMultipartUpload"; exit 1; }
if grep -qF "$backup_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the catalog backup upgrade note"; exit 1
fi
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --is-upgrade --set catalog.backup.enabled=false \
  --show-only templates/probe.yaml > "${tmp}/notes-upgrade-backup-off.txt" \
  || { echo "FAIL: rendering NOTES.txt as an upgrade with the backup off failed"; exit 1; }
if grep -qF "$backup_note" "${tmp}/notes-upgrade-backup-off.txt"; then
  echo "FAIL: the upgrade notes carry the catalog backup note although the backup is off"; exit 1
fi

# global.logLevel reaches every service as LOG_LEVEL, and a service refuses to
# start on a level it does not know, so the chart (continuo.logLevel in
# _helpers.tpl) refuses such a level at render time instead. The services
# match the level in any case, so a mixed-case level must still render.
echo "--- global.logLevel: a level the services do not accept fails the render"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set global.logLevel=verbose > /dev/null 2> "${tmp}/loglevel.err"; then
  echo "FAIL: global.logLevel=verbose rendered; every service would refuse to start"; exit 1
fi
grep -q 'logLevel' "${tmp}/loglevel.err" || { echo "FAIL: unexpected error:"; cat "${tmp}/loglevel.err"; exit 1; }
echo "--- global.logLevel: a mixed-case level renders as given"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.logLevel=Info > "${tmp}/loglevel-mixed.yaml" \
  || { echo "FAIL: global.logLevel=Info did not render; the services accept it"; exit 1; }
grep -q 'LOG_LEVEL: "Info"' "${tmp}/loglevel-mixed.yaml" \
  || { echo "FAIL: LOG_LEVEL did not render as Info"; exit 1; }

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
# `services` list must not silently disable CI auth. ciAuth itself is optional:
# values without it (as `helm upgrade --reuse-values` can produce) render with
# the GitHub issuer and no bindings (no CI access).
echo "--- ciAuth: the ui gets CI auth from the template"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/defaults.yaml" continuo
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  -f scripts/install-test/values-ui-minimal-services.yaml > "${tmp}/ui-minimal-services.yaml"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/ui-minimal-services.yaml" continuo
echo "--- ciAuth: a release without ciAuth values renders the defaults"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set ciAuth=null > "${tmp}/ciauth-null.yaml" \
  || { echo "FAIL: rendering with ciAuth=null failed; values without a ciAuth block must render"; exit 1; }
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
# release a service would refuse to boot with. Values without a database block
# (as `helm upgrade --reuse-values` can produce) render the defaults.
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
  || { echo "FAIL: rendering with database=null failed; values without a database block must render"; exit 1; }
if grep -q 'DB_MAX_OPEN_CONNS\|DB_MAX_IDLE_CONNS' "${tmp}/database-null.yaml"; then
  echo "FAIL: a release without database values must leave every service on its default pool"; exit 1
fi

# execution.maxConcurrentJobs reaches execution-controller through the shared
# ConfigMap; a value below 1 fails the render, and values without an execution
# block (as `helm upgrade --reuse-values` can produce) render the default.
echo "--- execution.maxConcurrentJobs reaches the ConfigMap; values below 1 are refused"
grep -q 'MAX_CONCURRENT_JOBS: "50"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set MAX_CONCURRENT_JOBS to 50"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.maxConcurrentJobs=7 > "${tmp}/cap-set.yaml"
grep -q 'MAX_CONCURRENT_JOBS: "7"' "${tmp}/cap-set.yaml" \
  || { echo "FAIL: execution.maxConcurrentJobs does not reach the shared ConfigMap"; exit 1; }
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.maxConcurrentJobs=0 > /dev/null 2>&1; then
  echo "FAIL: execution.maxConcurrentJobs=0 must fail the render"; exit 1
fi
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution=null > "${tmp}/execution-null.yaml" \
  || { echo "FAIL: rendering with execution=null failed; values without an execution block must render"; exit 1; }
grep -q 'MAX_CONCURRENT_JOBS: "50"' "${tmp}/execution-null.yaml" \
  || { echo "FAIL: a release without execution values must get the default cap"; exit 1; }
if grep -A40 'name: execution-controller' "${tmp}/defaults.yaml" | grep -q 'name: MAX_CONCURRENT_JOBS'; then
  echo "FAIL: execution-controller's own env must not shadow the ConfigMap's MAX_CONCURRENT_JOBS"; exit 1
fi

# execution.startTimeoutMinutes reaches execution-controller through the shared
# ConfigMap in seconds; a value outside 1-120 fails the render, and values
# without the key (as `helm upgrade --reuse-values` can produce) render the
# 15-minute default.
echo "--- execution.startTimeoutMinutes reaches the ConfigMap in seconds; values outside 1-120 are refused"
grep -q 'JOB_START_TIMEOUT_SECONDS: "900"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set JOB_START_TIMEOUT_SECONDS to 900"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.startTimeoutMinutes=3 > "${tmp}/start-set.yaml"
grep -q 'JOB_START_TIMEOUT_SECONDS: "180"' "${tmp}/start-set.yaml" \
  || { echo "FAIL: execution.startTimeoutMinutes does not reach the shared ConfigMap in seconds"; exit 1; }
for bad in 0 121; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.startTimeoutMinutes=${bad} > /dev/null 2>&1; then
    echo "FAIL: execution.startTimeoutMinutes=${bad} must fail the render"; exit 1
  fi
done
grep -q 'JOB_START_TIMEOUT_SECONDS: "900"' "${tmp}/execution-null.yaml" \
  || { echo "FAIL: a release without execution values must get the default start timeout"; exit 1; }

# execution.kubeClientQPS and execution.kubeClientBurst reach
# execution-controller through the shared ConfigMap; a QPS outside 1-1000, a
# burst outside 1-2000 or a burst below the QPS fails the render, and a release
# without the keys (helm upgrade --reuse-values from an older chart) renders
# the defaults.
echo "--- execution.kubeClientQPS and execution.kubeClientBurst reach the ConfigMap; values outside their ranges are refused"
grep -q 'K8S_CLIENT_QPS: "50"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set K8S_CLIENT_QPS to 50"; exit 1; }
grep -q 'K8S_CLIENT_BURST: "100"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set K8S_CLIENT_BURST to 100"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set execution.kubeClientQPS=200 --set execution.kubeClientBurst=400 > "${tmp}/rate-set.yaml"
grep -q 'K8S_CLIENT_QPS: "200"' "${tmp}/rate-set.yaml" && grep -q 'K8S_CLIENT_BURST: "400"' "${tmp}/rate-set.yaml" \
  || { echo "FAIL: execution.kubeClientQPS and execution.kubeClientBurst do not reach the shared ConfigMap"; exit 1; }
for bad in "kubeClientQPS=0" "kubeClientQPS=1001" "kubeClientBurst=0" "kubeClientBurst=2001" "kubeClientBurst=49" "kubeClientQPS=200"; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "execution.${bad}" > /dev/null 2>&1; then
    echo "FAIL: execution.${bad} must fail the render"; exit 1
  fi
done
grep -q 'K8S_CLIENT_QPS: "50"' "${tmp}/execution-null.yaml" && grep -q 'K8S_CLIENT_BURST: "100"' "${tmp}/execution-null.yaml" \
  || { echo "FAIL: a release without execution values must get the default rate limit"; exit 1; }

# runs.taskMaxRetries and runs.taskTimeoutHours reach run-controller and
# execution-controller through the shared ConfigMap, the timeout in seconds; a
# value outside its range fails the render, and values without a runs block
# (as `helm upgrade --reuse-values` can produce) render the defaults.
echo "--- runs.taskMaxRetries and runs.taskTimeoutHours reach the ConfigMap; values outside their ranges are refused"
grep -q 'TASK_MAX_RETRIES: "2"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set TASK_MAX_RETRIES to 2"; exit 1; }
grep -q 'TASK_TIMEOUT_SECONDS: "86400"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set TASK_TIMEOUT_SECONDS to 86400"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set runs.taskMaxRetries=0 --set runs.taskTimeoutHours=3 > "${tmp}/runs-set.yaml"
grep -q 'TASK_MAX_RETRIES: "0"' "${tmp}/runs-set.yaml" \
  || { echo "FAIL: runs.taskMaxRetries=0 does not reach the shared ConfigMap"; exit 1; }
grep -q 'TASK_TIMEOUT_SECONDS: "10800"' "${tmp}/runs-set.yaml" \
  || { echo "FAIL: runs.taskTimeoutHours does not reach the shared ConfigMap in seconds"; exit 1; }
for bad in "runs.taskMaxRetries=-1" "runs.taskMaxRetries=11" "runs.taskTimeoutHours=0" "runs.taskTimeoutHours=169"; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${bad}" > /dev/null 2>&1; then
    echo "FAIL: ${bad} must fail the render"; exit 1
  fi
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set runs=null > "${tmp}/runs-null.yaml" \
  || { echo "FAIL: rendering with runs=null failed; values without a runs block must render"; exit 1; }
grep -q 'TASK_MAX_RETRIES: "2"' "${tmp}/runs-null.yaml" && grep -q 'TASK_TIMEOUT_SECONDS: "86400"' "${tmp}/runs-null.yaml" \
  || { echo "FAIL: a release without runs values must get the default retry budget and task timeout"; exit 1; }
# deployment_env_entries prints each env entry named like the ERE $3 in the
# whole Deployment document named $1 of the rendered manifest $2, one
# "- name: ..." line each. It exits 3 when the manifest has no such Deployment,
# so a renamed Deployment cannot make a caller's check pass on nothing.
deployment_env_entries() {
  awk -v dep="$1" -v pattern="$3" '
    function flush() {
      if (isDeployment && named) { found = 1; printf "%s", hits }
      isDeployment = 0; named = 0; hits = ""
    }
    /^---$/ { flush(); next }
    /^kind: Deployment$/ { isDeployment = 1 }
    ($0 == "  name: " dep) { named = 1 }
    $0 ~ ("^ +- name: (" pattern ")$") { hits = hits $0 "\n" }
    END { flush(); if (!found) exit 3 }' "$2"
}
for svc in run-controller execution-controller; do
  shadows="$(deployment_env_entries "$svc" "${tmp}/defaults.yaml" 'TASK_MAX_RETRIES|TASK_TIMEOUT_SECONDS')" \
    || { echo "FAIL: the default render has no Deployment named ${svc}"; exit 1; }
  if [ -n "$shadows" ]; then
    echo "FAIL: ${svc}'s own env must not shadow the ConfigMap's TASK_MAX_RETRIES or TASK_TIMEOUT_SECONDS:"; echo "$shadows"; exit 1
  fi
done

# catalog.backup.enabled, catalog.backup.schedule and catalog.backup.keep reach
# catalog through the shared ConfigMap as CATALOG_BACKUP_ENABLED,
# CATALOG_BACKUP_SCHEDULE and CATALOG_BACKUP_KEEP. A keep below 1, a schedule
# that is neither five fields nor an @ descriptor and a non-boolean enabled
# fail the render, in the helpers as well as in the schema (the helpers refuse
# them with the schema check skipped), a key the schema does not name is
# refused, and values without a catalog block (as `helm upgrade
# --reuse-values` can produce) render the defaults.
echo "--- catalog.backup reaches the ConfigMap; values catalog cannot honour are refused"
grep -q 'CATALOG_BACKUP_ENABLED: "true"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CATALOG_BACKUP_ENABLED to true"; exit 1; }
grep -qF 'CATALOG_BACKUP_SCHEDULE: "0 2 * * *"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CATALOG_BACKUP_SCHEDULE to 0 2 * * *"; exit 1; }
grep -q 'CATALOG_BACKUP_KEEP: "7"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CATALOG_BACKUP_KEEP to 7"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set catalog.backup.enabled=false --set catalog.backup.keep=3 \
  --set-string 'catalog.backup.schedule=30 3 * * 1' > "${tmp}/backup-set.yaml"
grep -q 'CATALOG_BACKUP_ENABLED: "false"' "${tmp}/backup-set.yaml" \
  || { echo "FAIL: catalog.backup.enabled=false does not reach the shared ConfigMap"; exit 1; }
grep -q 'CATALOG_BACKUP_KEEP: "3"' "${tmp}/backup-set.yaml" \
  || { echo "FAIL: catalog.backup.keep does not reach the shared ConfigMap"; exit 1; }
grep -qF 'CATALOG_BACKUP_SCHEDULE: "30 3 * * 1"' "${tmp}/backup-set.yaml" \
  || { echo "FAIL: catalog.backup.schedule does not reach the shared ConfigMap"; exit 1; }
for descriptor in '@daily' '@every 6h'; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-string "catalog.backup.schedule=${descriptor}" > "${tmp}/backup-descriptor.yaml" \
    || { echo "FAIL: catalog.backup.schedule=${descriptor} must render"; exit 1; }
  grep -qF "CATALOG_BACKUP_SCHEDULE: \"${descriptor}\"" "${tmp}/backup-descriptor.yaml" \
    || { echo "FAIL: catalog.backup.schedule=${descriptor} does not reach the shared ConfigMap"; exit 1; }
done
# expect_backup_refused FLAG SETTING: helm template with FLAG SETTING fails,
# with the schema check and again without it.
expect_backup_refused() {
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$1" "$2" > /dev/null 2>&1; then
    echo "FAIL: ${2} must fail the render"; exit 1
  fi
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --skip-schema-validation "$1" "$2" > /dev/null 2>&1; then
    echo "FAIL: ${2} must fail the render even with the schema check skipped"; exit 1
  fi
}
expect_backup_refused --set catalog.backup.keep=0
expect_backup_refused --set catalog.backup.keep=-1
expect_backup_refused --set-string catalog.backup.schedule=nightly
expect_backup_refused --set-string catalog.backup.schedule=
expect_backup_refused --set-string 'catalog.backup.schedule=* * * *'
expect_backup_refused --set-string 'catalog.backup.schedule=* * * * * *'
expect_backup_refused --set catalog.backup.schedule=5
expect_backup_refused --set-string catalog.backup.enabled=yes
expect_backup_refused --set-string catalog.backup.enabled=true
for unknown in catalog.backup.bogus=1 catalog.bogus=1; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${unknown}" > /dev/null 2>&1; then
    echo "FAIL: ${unknown} must fail the render; the schema names no such key"; exit 1
  fi
done
for absent in catalog=null catalog.backup=null; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${absent}" > "${tmp}/backup-absent.yaml" \
    || { echo "FAIL: rendering with ${absent} failed; values without a catalog.backup block must render"; exit 1; }
  grep -q 'CATALOG_BACKUP_ENABLED: "true"' "${tmp}/backup-absent.yaml" \
    && grep -qF 'CATALOG_BACKUP_SCHEDULE: "0 2 * * *"' "${tmp}/backup-absent.yaml" \
    && grep -q 'CATALOG_BACKUP_KEEP: "7"' "${tmp}/backup-absent.yaml" \
    || { echo "FAIL: a release with ${absent} must get the default backup settings"; exit 1; }
done
shadows="$(deployment_env_entries catalog "${tmp}/defaults.yaml" 'CATALOG_BACKUP_ENABLED|CATALOG_BACKUP_SCHEDULE|CATALOG_BACKUP_KEEP')" \
  || { echo "FAIL: the default render has no Deployment named catalog"; exit 1; }
if [ -n "$shadows" ]; then
  echo "FAIL: catalog's own env must not shadow the ConfigMap's CATALOG_BACKUP_* keys:"; echo "$shadows"; exit 1
fi

# maintenance.enabled reaches every service through the shared ConfigMap as
# exactly "true" or "false"; a non-boolean value fails the render, and values
# without a maintenance block (as `helm upgrade --reuse-values` can produce)
# render "false".
echo "--- maintenance.enabled reaches the ConfigMap; non-booleans are refused"
grep -q 'MAINTENANCE_ENABLED: "false"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set MAINTENANCE_ENABLED to false"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set maintenance.enabled=true > "${tmp}/maintenance-on.yaml"
grep -q 'MAINTENANCE_ENABLED: "true"' "${tmp}/maintenance-on.yaml" \
  || { echo "FAIL: maintenance.enabled=true does not reach the shared ConfigMap"; exit 1; }
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-string maintenance.enabled=yes > /dev/null 2>&1; then
  echo "FAIL: a non-boolean maintenance.enabled must fail the render"; exit 1
fi
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set maintenance=null > "${tmp}/maintenance-null.yaml" \
  || { echo "FAIL: rendering with maintenance=null failed; values without a maintenance block must render"; exit 1; }
grep -q 'MAINTENANCE_ENABLED: "false"' "${tmp}/maintenance-null.yaml" \
  || { echo "FAIL: a release without maintenance values must render MAINTENANCE_ENABLED false"; exit 1; }

# Every Go service serves /metrics on the one port the ConfigMap names. The
# chart must expose and annotate exactly those services, admit scrapers only
# when asked and only to that port, and refuse a port another listener uses.
echo "--- metrics port, annotations and scrape rule cover exactly the services that serve /metrics"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'metrics.scrapeNamespaceSelector={"matchLabels":{"kubernetes.io/metadata.name":"monitoring"}}' \
  > "${tmp}/metrics-scrape.yaml"
python3 scripts/install-test/assert-metrics.py . "${tmp}/defaults.yaml" "${tmp}/metrics-scrape.yaml"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set metrics.port=8082 > /dev/null 2>&1; then
  echo "FAIL: metrics.port equal to a service's httpPort must fail the render"; exit 1
fi

# llm.baseUrl reaches both agents as LLM_BASE_URL from the template, so an
# operator's own `services` list cannot drop it. openai-compatible has no
# public address and needs it; a root ending in /v1 would make every request
# /v1/v1/...; a provider outside the three the agents accept fails the schema;
# and a services entry setting LLM_BASE_URL itself, through env or secretEnv, is
# refused. Values without llm.baseUrl (as `helm upgrade --reuse-values` can
# produce) render.
echo "--- llm: baseUrl reaches both agents; invalid LLM settings fail the render"
if grep -q 'name: LLM_BASE_URL' "${tmp}/defaults.yaml"; then
  echo "FAIL: the default render sets LLM_BASE_URL; anthropic must use its public address"; exit 1
fi
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set llm.provider=openai-compatible --set llm.baseUrl=http://vllm.ai.svc:8000/ > "${tmp}/llm-base.yaml"
[ "$(grep -A1 'name: LLM_BASE_URL' "${tmp}/llm-base.yaml" | grep -c 'value: "http://vllm.ai.svc:8000"')" = 2 ] \
  || { echo "FAIL: llm.baseUrl must reach agent-chat and agent-remediation as LLM_BASE_URL, without the trailing slash"; exit 1; }
# refuse_render WANT HELM_ARGS...: the render must fail, and the error must
# contain WANT, as a fixed string, so the failure is the one under test.
refuse_render() {
  local want="$1"; shift
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$@" > /dev/null 2> "${tmp}/refused.err"; then
    echo "FAIL: rendering with $* succeeded; it must fail"; exit 1
  fi
  grep -qF -- "$want" "${tmp}/refused.err" || { echo "FAIL: rendering with $* failed for another reason (want: ${want}):"; cat "${tmp}/refused.err"; exit 1; }
}
refuse_render 'llm.baseUrl' --set llm.provider=openai-compatible
refuse_render 'ends in /v1' --set llm.provider=openai-compatible --set llm.baseUrl=https://api.mistral.ai/v1
refuse_render 'baseUrl' --set llm.baseUrl=api.mistral.ai
refuse_render 'with a host' --set llm.baseUrl=https://
refuse_render 'bare server root' --set 'llm.baseUrl=https://h/api?x=1'
refuse_render 'bare server root' --set 'llm.baseUrl=https://u:p@h'
refuse_render 'provider' --set llm.provider=grok
refuse_render 'LLM_BASE_URL' --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"LLM_BASE_URL":"http://x:1"}}]'
refuse_render 'secretEnv sets LLM_BASE_URL' --set-json 'services=[{"name":"agent-chat","enabled":true,"secretEnv":[{"name":"LLM_BASE_URL","ref":"llm"}]}]'
if ! helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set llm.baseUrl=null > /dev/null; then
  echo "FAIL: a release without llm.baseUrl must render"; exit 1
fi

# The egress-proxy allowlist follows the LLM, GitHub and S3 settings, so a
# company that switches provider never edits it by hand. Every egress.* entry
# is validated as written; a bad one fails the render naming the key and the
# entry, and a release without an egress block renders the proxy with defaults.
echo "--- egress-proxy: the allowlist follows the LLM, GitHub and S3 settings; bad entries fail the render"
python3 scripts/install-test/assert-egress-allowlist.py "$CHART" "$KUBE_VERSION"

# The agents reach the outside only through egress-proxy. While it renders they
# carry HTTP_PROXY, HTTPS_PROXY and a NO_PROXY that covers every in-cluster
# address they are given (or that call goes to the proxy and is refused) and no
# host on the allowlist (or that call skips the proxy). Without the proxy
# (egress.enabled=false, or an allowlist with nothing in it) they carry no
# proxy variable. A release without an egress block runs with the proxy on.
echo "--- egress: agents use the proxy; NO_PROXY covers every in-cluster address and no allowlisted host"
for f in defaults byo-example byo-inline byo-secret; do
  python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/${f}.yaml"
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set egress=null > "${tmp}/egress-null.yaml" \
  || { echo "FAIL: a release without egress values must render"; exit 1; }
grep -q 'name: HTTPS_PROXY$' "${tmp}/egress-null.yaml" \
  || { echo "FAIL: a release without egress values must run the agents through the proxy"; exit 1; }
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-null.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-null.yaml" continuo
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set egress.enabled=false > "${tmp}/egress-off.yaml"
if grep -qE 'name: (HTTP_PROXY|HTTPS_PROXY|NO_PROXY)$|-agents-egress$|-egress-proxy-egress$|-allow-egress-proxy$' "${tmp}/egress-off.yaml"; then
  echo "FAIL: egress.enabled=false must render no proxy variables and no egress policy"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-off.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-off.yaml" continuo
# Every URL the agents call is at an IP address and no domain is listed: the
# allowlist is empty, no proxy renders, and the agents get no proxy variables.
# Their egress policy still renders, admitting those addresses directly, and
# neither egress-proxy policy does: with no proxy pod they would select nothing.
egress_all_ip=(--set llm.provider=openai-compatible --set llm.baseUrl=http://10.0.0.7:8000
  --set-json 'services=[{"name":"agent-chat","enabled":true},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://10.0.0.8:8080"}}]')
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "${egress_all_ip[@]}" > "${tmp}/egress-no-proxy.yaml"
if grep -q '^  name: continuo-egress-proxy$' "${tmp}/egress-no-proxy.yaml"; then
  echo "FAIL: an install whose every destination is an IP address must render no egress-proxy"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-no-proxy.yaml"
grep -q '^  name: continuo-agents-egress$' "${tmp}/egress-no-proxy.yaml" \
  || { echo "FAIL: with egress on and no proxy, the agents' egress policy must still render"; exit 1; }
if grep -qE -- '-egress-proxy-egress$|-allow-egress-proxy$' "${tmp}/egress-no-proxy.yaml"; then
  echo "FAIL: with no proxy, neither egress-proxy NetworkPolicy may render"; exit 1
fi
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-no-proxy.yaml" continuo \
  --direct 10.0.0.7/32=8000 --direct 10.0.0.8/32=8080
# networkPolicy.enabled=false renders no NetworkPolicy, the egress ones
# included, while the agents still use the proxy.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set networkPolicy.enabled=false > "${tmp}/netpol-off.yaml"
if grep -q '^kind: NetworkPolicy$' "${tmp}/netpol-off.yaml"; then
  echo "FAIL: networkPolicy.enabled=false must render no NetworkPolicy"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/netpol-off.yaml"
# The agents reach an external Postgres or Redis on its configured port, which
# the checker reads from the shared ConfigMap; egress-proxy reaches private
# addresses only on the ports of allowPrivate entries, here 8443 while public
# addresses take 443, 8080 and 8443. A datastore port that is not one fails the
# render while the agents' egress policy renders, and only then.
echo "--- egress: the agents reach an external datastore on its port; the proxy reaches private addresses on allowPrivate ports"
egress_byo=(--set postgresql.enabled=false --set externalDatabase.host=pg.example.internal
  --set externalDatabase.password=probe --set redis.enabled=false
  --set externalRedis.host=redis.example.internal --set externalRedis.password=probe)
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "${egress_byo[@]}" \
  --set externalDatabase.port=6543 --set-json 'externalRedis.port=6380' \
  --set-json 'egress.privateDomains=["git.corp.example:8443"]' --set-json 'egress.allowedDomains=["api.example.com:8080"]' \
  > "${tmp}/egress-ports.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-ports.yaml" continuo
# An external Postgres running as a pod of another namespace: the agents reach
# the pods of every namespace on its port, which a network plugin that matches
# no pod by ipBlock still admits.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set postgresql.enabled=false \
  --set externalDatabase.host=pg-rw.databases.svc --set externalDatabase.password=probe > "${tmp}/egress-pg-pod.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-pg-pod.yaml" continuo
# An external Postgres or Redis whose host is an IP address outside the private
# ranges is a direct destination on its configured port: a /32 or /128 rule in
# the agents' egress policy, and the address in NO_PROXY. One at a private
# address is admitted by the private-range rule and gets no rule of its own;
# one at a cloud metadata or link-local address fails the render. The checker
# reads the host and port from the shared ConfigMap.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set postgresql.enabled=false --set externalDatabase.host=203.0.113.5 --set externalDatabase.password=probe \
  --set redis.enabled=false --set 'externalRedis.host=2001:db8::5' --set externalRedis.port=6380 \
  --set externalRedis.password=probe > "${tmp}/egress-ds-public.yaml"
grep -q '^            cidr: 203.0.113.5/32$' "${tmp}/egress-ds-public.yaml" \
  || { echo "FAIL: an external Postgres at a public IP address must be a direct destination"; exit 1; }
grep -q '^            cidr: "2001:db8::5/128"$' "${tmp}/egress-ds-public.yaml" \
  || { echo "FAIL: an external Redis at a public IPv6 address must be a direct destination"; exit 1; }
grep -A1 'name: NO_PROXY' "${tmp}/egress-ds-public.yaml" | grep -q ',203\.0\.113\.5,2001:db8::5"' \
  || { echo "FAIL: an external datastore at a public IP address must be in NO_PROXY"; exit 1; }
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-ds-public.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-ds-public.yaml" continuo
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set postgresql.enabled=false --set externalDatabase.host=10.0.0.5 --set externalDatabase.password=probe \
  > "${tmp}/egress-ds-private.yaml"
if grep -q 'cidr: 10.0.0.5/32' "${tmp}/egress-ds-private.yaml"; then
  echo "FAIL: an external Postgres at a private address is admitted by the private-range rule and needs no rule of its own"; exit 1
fi
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-ds-private.yaml" continuo
refuse_render 'externalDatabase.host="100.100.100.200": its host 100.100.100.200 lies in' \
  --set postgresql.enabled=false --set externalDatabase.host=100.100.100.200 --set externalDatabase.password=probe
refuse_render 'externalRedis.host="169.254.169.254": its host 169.254.169.254 lies in' \
  --set redis.enabled=false --set externalRedis.host=169.254.169.254 --set externalRedis.password=probe
refuse_render 'externalDatabase.port=0 is not a port' "${egress_byo[@]}" --set externalDatabase.port=0
refuse_render 'externalRedis.port=70000 is not a port' "${egress_byo[@]}" --set externalRedis.port=70000
refuse_render 'externalDatabase.port="5432" is not a port' --skip-schema-validation "${egress_byo[@]}" \
  --set-string externalDatabase.port=5432
for off in networkPolicy.enabled=false egress.enabled=false; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "${egress_byo[@]}" --set externalDatabase.port=0 \
    --set "${off}" > /dev/null \
    || { echo "FAIL: with ${off} no egress policy renders and externalDatabase.port=0 must render as before"; exit 1; }
done
# NO_PROXY covers every in-cluster address the agents are given, however it is
# written: a global.*GrpcAddr in another namespace, an agent env URL at a .svc
# name, and the namespace-qualified forms of a release Service. Such an address
# is listed with its port as written (08080 stays 08080, which is what Go
# compares), so the same host on a port the allowlist admits (a GitHub
# Enterprise on 443 here) still goes through the proxy. A name with a trailing
# dot is no in-cluster name and goes through the proxy.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.runControllerGrpcAddr=run-controller.other.svc:50051 > "${tmp}/egress-np-global.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-global.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://foo.tools.svc:80","FOO_ALT_URL":"http://foo.tools.svc:08080","TRAILING_DOT_ADDR":"run-controller.:50051"}},{"name":"foo","enabled":true,"httpPort":80},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://foo.tools.svc/api/v3"}}]' \
  > "${tmp}/egress-np-env.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-env.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller.default:50051","CONTINUO_CATALOG_ADDR":"catalog.default.svc.cluster.local:50052"}},{"name":"run-controller","enabled":true,"httpPort":8082},{"name":"catalog","enabled":true,"httpPort":8087},{"name":"agent-remediation","enabled":true}]' \
  > "${tmp}/egress-np-forms.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-forms.yaml"
# An in-cluster address the allowlist admits on the port the agents use is left
# out of NO_PROXY, so the call goes through the proxy: a GitHub Enterprise at a
# namespace-local name (whose codeload.<name> a bare NO_PROXY name would also
# cover). The release's own Services stay in NO_PROXY in every form, also when
# a wildcard allowlist entry over the cluster domain matches them on another
# port; an llm.baseUrl on localhost needs no allowlist entry.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller:50051"}},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://ghe:8080/api/v3","CONTINUO_CATALOG_ADDR":"catalog:50052"}}]' \
  > "${tmp}/egress-allowlisted-local.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-allowlisted-local.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'egress.privateDomains=["*.svc.cluster.local"]' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller.default.svc.cluster.local:50051"}},{"name":"run-controller","enabled":true,"httpPort":8082},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://ghe.tools.svc/api/v3"}}]' \
  > "${tmp}/egress-allowlisted-wildcard.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-allowlisted-wildcard.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set llm.provider=openai-compatible --set llm.baseUrl=http://localhost:8000 > "${tmp}/egress-llm-localhost.yaml"
if grep -q 'host: "localhost"' "${tmp}/egress-llm-localhost.yaml"; then
  echo "FAIL: an llm.baseUrl on localhost must not reach the allowlist; egress-proxy never connects to loopback"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-llm-localhost.yaml"
# An egress.allowedDomains entry does not allow private addresses, so egress-proxy
# would refuse the private address an in-cluster name resolves to: the name
# stays in NO_PROXY.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'egress.allowedDomains=["foo.tools.svc:80"]' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://foo.tools.svc:80"}},{"name":"agent-remediation","enabled":true}]' \
  > "${tmp}/egress-allowlisted-public.yaml"
[ "$(grep -A1 'name: NO_PROXY' "${tmp}/egress-allowlisted-public.yaml" | grep -cF ',foo.tools.svc:80')" = 2 ] \
  || { echo "FAIL: an in-cluster address on an allowlist entry without allowPrivate must stay in NO_PROXY"; exit 1; }
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-allowlisted-public.yaml"
# An in-cluster address in another namespace that the agents reach without the
# proxy (a global.*GrpcAddr or an agent env URL at <name>.<namespace>.svc) gets
# a rule in their egress policy for that namespace and port; the checker reads
# which from their NO_PROXY and env. A global.*GrpcAddr whose host is no
# in-cluster name goes through the proxy, which logs its refusal, instead of
# NO_PROXY sending it straight into the policy that drops it.
echo "--- egress: the agents reach in-cluster addresses of other namespaces they are given; other hosts go through the proxy"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.deadLetterControllerGrpcAddr=dead-letter-controller.ops.svc:50055 > "${tmp}/egress-np-dlc.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-dlc.yaml"
grep -q 'kubernetes.io/metadata.name: ops$' "${tmp}/egress-np-dlc.yaml" \
  || { echo "FAIL: a global.*GrpcAddr in another namespace must reach the agents' egress policy"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.deadLetterControllerGrpcAddr=dlc.corp.example:50055 \
  --set global.runControllerGrpcAddr=run-controller.corp.example:50051 > "${tmp}/egress-np-fqdn.yaml"
if grep -A1 'name: NO_PROXY' "${tmp}/egress-np-fqdn.yaml" | grep -qE 'dlc\.corp\.example|run-controller\.corp\.example'; then
  echo "FAIL: a global.*GrpcAddr whose host is no in-cluster name must go through the proxy, not NO_PROXY"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-fqdn.yaml"
for f in egress-np-global egress-np-env egress-np-forms egress-np-dlc egress-np-fqdn egress-allowlisted-local \
  egress-allowlisted-wildcard egress-allowlisted-public egress-llm-localhost; do
  python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/${f}.yaml" continuo
done
# An address the agents are given whose host is an IP address (a URL or a
# host:port in their env, IPv4 or IPv6, or a global.*GrpcAddr) is a direct
# destination, because egress-proxy admits host names only: it is in NO_PROXY
# and gets a rule on its port. Loopback and a listen address (0.0.0.0) need
# nothing; an address in link-local or a cloud metadata range, one that is no
# valid address and a port outside 1-65535 fail the render naming the key.
echo "--- egress: an IP address in the agents' env or a global.*GrpcAddr is a direct destination"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set global.runControllerGrpcAddr=10.0.0.5:50051 \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://10.0.0.9:8080/v1","LOCAL_ADDR":"127.0.0.1:9000","LISTEN_ADDR":"0.0.0.0:8081"}},{"name":"agent-remediation","enabled":true,"env":{"BAR_ADDR":"[fd00::9]:50051"}}]' \
  > "${tmp}/egress-np-ip.yaml"
for cidr in 10.0.0.5/32 10.0.0.9/32 '"fd00::9/128"'; do
  grep -qF "cidr: ${cidr}" "${tmp}/egress-np-ip.yaml" \
    || { echo "FAIL: an IP address the agents are given must get a rule in their egress policy (${cidr})"; exit 1; }
done
if grep -qE 'cidr: "?(127\.0\.0\.1|0\.0\.0\.0)/32' "${tmp}/egress-np-ip.yaml"; then
  echo "FAIL: a loopback or listen address the agents are given needs no rule"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-ip.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-np-ip.yaml" continuo \
  --direct 10.0.0.5/32=50051 --direct 10.0.0.9/32=8080 --direct fd00::9/128=50051
refuse_render 'services[name=agent-chat].env.META_URL="http://169.254.169.254/latest": its host 169.254.169.254 lies in' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"META_URL":"http://169.254.169.254/latest"}}]'
refuse_render 'global.runControllerGrpcAddr="[fd00:ec2::254]:50051": its host fd00:ec2::254 lies in' \
  --set 'global.runControllerGrpcAddr=[fd00:ec2::254]:50051'
refuse_render 'services[name=agent-remediation].env.BAD_ADDR="999.0.0.1:80": its host 999.0.0.1 has the octet "999"' \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"env":{"BAD_ADDR":"999.0.0.1:80"}}]'
refuse_render 'services[name=agent-chat].env.FOO_ADDR="10.0.0.9:70000" has a port outside 1-65535' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"FOO_ADDR":"10.0.0.9:70000"}}]'

# While egress is on, an agent's own proxy variable, through env or secretEnv,
# fails the render, as do an S3_ENDPOINT_URL of its own, which would replace the
# s3.endpointUrl the allowlist is built from, and a GITHUB_BASE_URL in
# secretEnv, which the allowlist cannot read; LLM_BASE_URL fails it either way.
# With egress off the proxy variables, S3_ENDPOINT_URL, LLM_PROVIDER and a
# GITHUB_BASE_URL in secretEnv render.
echo "--- egress: an agent's own proxy variables, S3_ENDPOINT_URL, and GITHUB_BASE_URL in secretEnv, fail the render while egress is on"
i=0
for var in HTTP_PROXY HTTPS_PROXY NO_PROXY http_proxy https_proxy no_proxy; do
  agents=(agent-chat agent-remediation)
  a="${agents[$((i % 2))]}"; b="${agents[$(((i + 1) % 2))]}"; i=$((i + 1))
  refuse_render "services[name=${a}].env sets ${var}" \
    --set-json "services=[{\"name\":\"${a}\",\"enabled\":true,\"env\":{\"${var}\":\"http://corp-proxy:8080\"}}]"
  refuse_render "services[name=${b}].secretEnv sets ${var}" \
    --set-json "services=[{\"name\":\"${b}\",\"enabled\":true,\"secretEnv\":[{\"name\":\"${var}\",\"ref\":\"llm\"}]}]"
done
refuse_render 'services[name=agent-chat].env sets HTTPS_PROXY' "${egress_all_ip[@]:0:4}" \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"HTTPS_PROXY":"http://corp-proxy:8080"}},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://10.0.0.8:8080"}}]'
refuse_render 'services[name=agent-remediation].secretEnv sets GITHUB_BASE_URL' \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"GITHUB_BASE_URL","ref":"githubToken"}]}]'
# The allowlist admits the provider of llm.provider, so an agent's LLM_PROVIDER
# other than the __LLM_PROVIDER__ placeholder fails the render, as does one in
# secretEnv; the placeholder renders.
refuse_render 'services[name=agent-chat].env sets LLM_PROVIDER="openai": ' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"LLM_PROVIDER":"openai"}},{"name":"agent-remediation","enabled":true}]'
refuse_render 'services[name=agent-remediation].secretEnv sets LLM_PROVIDER: ' \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"LLM_PROVIDER","ref":"llm"}]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"LLM_PROVIDER":"__LLM_PROVIDER__"}},{"name":"agent-remediation","enabled":true}]' > /dev/null \
  || { echo "FAIL: an agent env LLM_PROVIDER of __LLM_PROVIDER__ must render"; exit 1; }
refuse_render 'services[name=agent-remediation].env sets S3_ENDPOINT_URL: ' \
  --set-json 'services=[{"name":"agent-chat","enabled":true},{"name":"agent-remediation","enabled":true,"env":{"S3_ENDPOINT_URL":"https://my_store.example.com"}}]'
refuse_render 'services[name=agent-chat].secretEnv sets S3_ENDPOINT_URL: ' \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"secretEnv":[{"name":"S3_ENDPOINT_URL","ref":"llm"}]}]'
refuse_render 'services[name=agent-chat].env sets LLM_BASE_URL' --set egress.enabled=false \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"LLM_BASE_URL":"http://x:1"}}]'
refuse_render 'services[name=agent-remediation].secretEnv sets LLM_BASE_URL' --set egress.enabled=false \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"LLM_BASE_URL","ref":"llm"}]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set egress.enabled=false \
  --set-json 'services=[{"name":"agent-chat","enabled":true,"env":{"HTTP_PROXY":"http://corp-proxy:8080","no_proxy":"internal"},"secretEnv":[{"name":"HTTPS_PROXY","ref":"llm"}]},{"name":"agent-remediation","enabled":true,"env":{"NO_PROXY":"internal","https_proxy":"http://corp-proxy:8080","S3_ENDPOINT_URL":"https://store.example.com","LLM_PROVIDER":"openai"},"secretEnv":[{"name":"http_proxy","ref":"llm"},{"name":"GITHUB_BASE_URL","ref":"githubToken"}]}]' > /dev/null \
  || { echo "FAIL: with egress off, an agent's own proxy variables, S3_ENDPOINT_URL and a GITHUB_BASE_URL in secretEnv must render"; exit 1; }

# A URL the agents call (llm.baseUrl, agent-remediation's GITHUB_BASE_URL,
# s3.endpointUrl with MinIO off) whose host is neither a host name the proxy
# admits nor an IP address, or whose port is outside 1-65535, fails the render
# naming the key and the value, rather than leaving the agent pointed at a proxy
# that refuses it. With egress off the same values render.
echo "--- egress: a URL host the proxy cannot admit fails the render"
egress_s3=(--set minio.enabled=false --set s3.bucket=continuo --set s3.region=eu-central-1
  --set s3.accessKeyId=probe --set s3.secretKey=probe)
refuse_render 'llm.baseUrl="http://vllm_x:8000"' --set llm.provider=openai-compatible --set llm.baseUrl=http://vllm_x:8000
refuse_render 'services[name=agent-remediation].env.GITHUB_BASE_URL="https://gh_e.example/api/v3"' \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://gh_e.example/api/v3"}}]'
refuse_render 's3.endpointUrl="http://my_minio:9000"' "${egress_s3[@]}" --set s3.endpointUrl=http://my_minio:9000
refuse_render 'llm.baseUrl="http://gpu:70000" has a port outside 1-65535' --set llm.baseUrl=http://gpu:70000
refuse_render 's3.endpointUrl="http://10.0.0.9:0" has a port outside 1-65535' "${egress_s3[@]}" --set s3.endpointUrl=http://10.0.0.9:0
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set egress.enabled=false \
  --set llm.provider=openai-compatible --set llm.baseUrl=http://vllm_x:8000 > /dev/null \
  || { echo "FAIL: with egress off, the allowlist is not built and llm.baseUrl must render"; exit 1; }

# A URL host that is an IP address is reached directly: it stays out of the
# allowlist, is in NO_PROXY and gets a rule on its port in the agents' egress
# policy, like every egress.directDestinations entry. A loopback one needs
# nothing; one in link-local or a cloud metadata range fails.
echo "--- egress: an IP address the agents reach directly is in NO_PROXY and their egress policy; loopback needs nothing; metadata fails"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set llm.provider=openai-compatible --set llm.baseUrl=http://10.0.0.7:8000 \
  --set-json 'egress.directDestinations=[{"cidr":"203.0.113.10/32","ports":[5432]}]' > "${tmp}/egress-direct.yaml"
grep -q 'cidr: 10.0.0.7/32' "${tmp}/egress-direct.yaml" \
  || { echo "FAIL: an llm.baseUrl at an IP address must be a direct destination"; exit 1; }
grep -q 'cidr: 203.0.113.10/32' "${tmp}/egress-direct.yaml" \
  || { echo "FAIL: egress.directDestinations must reach the agents' egress policy"; exit 1; }
grep -A1 'name: NO_PROXY' "${tmp}/egress-direct.yaml" | grep -q '10\.0\.0\.7' \
  || { echo "FAIL: an IP the agents reach directly must be in NO_PROXY"; exit 1; }
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-direct.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-direct.yaml" continuo \
  --direct 10.0.0.7/32=8000 --direct 203.0.113.10/32=5432
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set llm.provider=openai-compatible --set 'llm.baseUrl=http://[2001:DB8::1]:8000' > "${tmp}/egress-direct6.yaml"
grep -A1 'name: NO_PROXY' "${tmp}/egress-direct6.yaml" | grep -q ',2001:db8::1[,"]' \
  || { echo "FAIL: an IPv6 llm.baseUrl host must be in NO_PROXY, without brackets"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set llm.provider=openai-compatible --set llm.baseUrl=http://127.0.0.1:8000 > /dev/null \
  || { echo "FAIL: an llm.baseUrl on loopback needs no direct destination and must render"; exit 1; }
refuse_render 'llm.baseUrl="http://169.254.169.254"' --set llm.baseUrl=http://169.254.169.254
refuse_render 'llm.baseUrl="http://[fd00:ec2::254]:80"' --set 'llm.baseUrl=http://[fd00:ec2::254]:80'
refuse_render 'llm.baseUrl="http://999.0.0.1:8000"' --set llm.baseUrl=http://999.0.0.1:8000
refuse_render 'llm.baseUrl="http://010.0.0.7:8000"' --set llm.baseUrl=http://010.0.0.7:8000
refuse_render 'services[name=agent-remediation].env.GITHUB_BASE_URL="https://[fe80::1]/api/v3"' \
  --set-json 'services=[{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://[fe80::1]/api/v3"}}]'

# egress.directDestinations is checked as written: an object with exactly cidr
# (an IPv4 or IPv6 CIDR with a prefix length of at least 1, no bit set past it,
# outside loopback, link-local and cloud metadata ranges) and ports (whole
# numbers 1-65535). The templates refuse a bad entry naming it; the schema
# refuses most of them first.
echo "--- egress.directDestinations: accepted entries reach NO_PROXY and the agents' egress policy; bad entries fail the render"
direct_ok='[{"cidr":"203.0.113.10/32","ports":[5432]},{"cidr":"10.0.0.0/8","ports":[5432,6379]},{"cidr":"2001:db8::1/128","ports":[443]},{"cidr":"2001:DB8:0:0:0:0:0:0/32","ports":[1,65535]},{"cidr":"198.51.100.0/24","ports":[443.0]},{"cidr":"64:ff9b::cb00:710a/128","ports":[443]},{"cidr":"1.0.0.0/8","ports":[443]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json "egress.directDestinations=${direct_ok}" > "${tmp}/egress-direct-ok.yaml" \
  || { echo "FAIL: accepted egress.directDestinations entries must render"; exit 1; }
for cidr in 203.0.113.10/32 10.0.0.0/8 2001:db8::1/128 2001:db8:0:0:0:0:0:0/32 198.51.100.0/24 64:ff9b::cb00:710a/128 1.0.0.0/8; do
  [ "$(grep -A1 'name: NO_PROXY' "${tmp}/egress-direct-ok.yaml" | grep -cF ",${cidr}")" = 2 ] \
    || { echo "FAIL: egress.directDestinations entry ${cidr} must reach both agents' NO_PROXY"; exit 1; }
done
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-direct-ok.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-direct-ok.yaml" continuo \
  --direct 203.0.113.10/32=5432 --direct 10.0.0.0/8=5432,6379 --direct 2001:db8::1/128=443 \
  --direct 2001:db8::/32=1,65535 --direct 198.51.100.0/24=443 --direct 64:ff9b::cb00:710a/128=443 \
  --direct 1.0.0.0/8=443
# S: the schema refuses the entry before any template runs, and the templates
# refuse it too when the schema is skipped. T: the entry passes the schema and
# the templates refuse it.
while IFS='|' read -r by entry; do
  [ -n "$entry" ] || continue
  if [ "$by" = S ]; then
    refuse_render "egress.directDestinations entry ${entry}" --skip-schema-validation --set-json "egress.directDestinations=[${entry}]"
    refuse_render 'directDestinations' --set-json "egress.directDestinations=[${entry}]"
    if grep -qF 'egress.directDestinations entry' "${tmp}/refused.err"; then
      echo "FAIL: values.schema.json must refuse egress.directDestinations entry ${entry} before the templates do"; exit 1
    fi
  else
    refuse_render "egress.directDestinations entry ${entry}" --set-json "egress.directDestinations=[${entry}]"
  fi
done <<'EOF'
S|{"cidr":"0.0.0.0/0","ports":[443]}
S|{"cidr":"::/0","ports":[443]}
S|{"cidr":"203.0.113.10","ports":[443]}
S|{"cidr":"203.0.113.10/","ports":[443]}
S|{"cidr":"203.0.113.10/33","ports":[443]}
S|{"cidr":"2001:db8::1/129","ports":[443]}
S|{"cidr":"203.0.113.10/032","ports":[443]}
S|{"cidr":"203.0.113.10/+32","ports":[443]}
S|{"cidr":"203.0.113.010/32","ports":[443]}
S|{"cidr":"256.0.0.1/32","ports":[443]}
S|{"cidr":"10.0.0/24","ports":[443]}
S|{"cidr":"10.0.0.0.0/24","ports":[443]}
T|{"cidr":"10.0.0.1/8","ports":[443]}
T|{"cidr":"2001:db8::1/64","ports":[443]}
T|{"cidr":"127.0.0.1/32","ports":[443]}
T|{"cidr":"126.0.0.0/7","ports":[443]}
T|{"cidr":"0.0.0.0/8","ports":[443]}
T|{"cidr":"169.254.169.254/32","ports":[80]}
T|{"cidr":"169.254.0.0/15","ports":[80]}
T|{"cidr":"100.64.0.0/10","ports":[80]}
T|{"cidr":"168.63.129.16/32","ports":[80]}
T|{"cidr":"::1/128","ports":[443]}
T|{"cidr":"::/128","ports":[443]}
T|{"cidr":"fe80::1/128","ports":[443]}
T|{"cidr":"fe80::/9","ports":[443]}
T|{"cidr":"fd00:ec2::254/128","ports":[443]}
T|{"cidr":"fd00::/8","ports":[443]}
T|{"cidr":"fd20:ce::254/128","ports":[443]}
T|{"cidr":"::ffff:a00:1/128","ports":[443]}
S|{"cidr":"::ffff:10.0.0.1/128","ports":[443]}
T|{"cidr":"64:ff9b::7f00:1/128","ports":[443]}
T|{"cidr":"64:ff9b::a9fe:a9fe/128","ports":[443]}
T|{"cidr":"64:ff9b::/64","ports":[443]}
S|{"cidr":"fe80::1%eth0/128","ports":[443]}
T|{"cidr":"2001:db8::1::2/128","ports":[443]}
T|{"cidr":"2001:db8:0:0:0:0:0:0:1/128","ports":[443]}
T|{"cidr":"2001:db8::12345/128","ports":[443]}
T|{"cidr":":2001:db8::1/128","ports":[443]}
T|{"cidr":"2001:db8::1:/128","ports":[443]}
S|{"cidr":"[2001:db8::1]/128","ports":[443]}
S|{"cidr":"example.com/32","ports":[443]}
S|{"cidr":" 10.0.0.0/8","ports":[443]}
S|{"cidr":"10.0.0.0/8 ","ports":[443]}
S|{"cidr":"","ports":[443]}
S|{"cidr":10,"ports":[443]}
S|{"cidr":["10.0.0.0/8"],"ports":[443]}
S|{"cidr":"10.0.0.0/8"}
S|{"ports":[443]}
S|{"cidr":"10.0.0.0/8","host":"10.0.0.1","ports":[443]}
S|{"cidr":"10.0.0.0/8","ports":[]}
S|{"cidr":"10.0.0.0/8","ports":443}
S|{"cidr":"10.0.0.0/8","ports":null}
S|{"cidr":"10.0.0.0/8","ports":[0]}
S|{"cidr":"10.0.0.0/8","ports":[65536]}
S|{"cidr":"10.0.0.0/8","ports":[-1]}
S|{"cidr":"10.0.0.0/8","ports":[443.5]}
S|{"cidr":"10.0.0.0/8","ports":[1e+21]}
S|{"cidr":"10.0.0.0/8","ports":["443"]}
S|{"cidr":"10.0.0.0/8","ports":["0443"]}
S|{"cidr":"10.0.0.0/8","ports":[true]}
S|{"cidr":"10.0.0.0/8","ports":[null]}
S|{"cidr":"10.0.0.0/8","ports":[[443]]}
S|"10.0.0.0/8"
S|null
EOF
# Each refusal says why: S|ENTRY|REASON or T|ENTRY|REASON, as above.
while IFS='|' read -r by entry why; do
  [ -n "$entry" ] || continue
  if [ "$by" = S ]; then
    refuse_render "$why" --skip-schema-validation --set-json "egress.directDestinations=[${entry}]"
  else
    refuse_render "$why" --set-json "egress.directDestinations=[${entry}]"
  fi
done <<'EOF'
S|{"cidr":"0.0.0.0/0","ports":[443]}|has a prefix length of 0
S|{"cidr":"::/0","ports":[443]}|has a prefix length of 0
T|{"cidr":"10.0.0.1/8","ports":[443]}|has address bits set past its prefix length
S|{"cidr":"203.0.113.010/32","ports":[443]}|has the octet "010"
S|{"cidr":"2001:db8::1/129","ports":[443]}|has a prefix length above 128
T|{"cidr":"126.0.0.0/7","ports":[443]}|overlaps 127.0.0.0/8 (loopback)
T|{"cidr":"169.254.169.254/32","ports":[80]}|overlaps 169.254.0.0/16
T|{"cidr":"fd00::/8","ports":[443]}|overlaps fd00:ec2::/32
T|{"cidr":"::ffff:a00:1/128","ports":[443]}|overlaps ::ffff:0:0/96
T|{"cidr":"64:ff9b::7f00:1/128","ports":[443]}|NAT64, here of 127.0.0.0/8
S|{"cidr":"10.0.0.0/8","host":"10.0.0.1","ports":[443]}|must have exactly the keys cidr and ports
S|{"cidr":10,"ports":[443]}|has a cidr that is not a string
S|{"cidr":"10.0.0.0/8","ports":[]}|has an empty ports list
S|{"cidr":"10.0.0.0/8","ports":443}|has ports that are not a list
S|{"cidr":"10.0.0.0/8","ports":["443"]}|has the port "443", which is not a whole number from 1 to 65535
S|{"cidr":"10.0.0.0/8","ports":[65536]}|has the port 65536, which is not a whole number from 1 to 65535
EOF
refuse_render 'egress.directDestinations must be a list' --skip-schema-validation \
  --set-json 'egress.directDestinations={"cidr":"10.0.0.0/8","ports":[443]}'
refuse_render 'directDestinations' --set-json 'egress.directDestinations={"cidr":"10.0.0.0/8","ports":[443]}'
if grep -qF 'egress.directDestinations must be a list' "${tmp}/refused.err"; then
  echo "FAIL: values.schema.json must refuse a non-list egress.directDestinations before the templates do"; exit 1
fi
# The check runs whenever egress is on, also when no proxy renders (every
# destination an IP address); with egress off it does not run.
refuse_render 'egress.directDestinations entry {"cidr":"0.0.0.0/0","ports":[443]}' --skip-schema-validation \
  "${egress_all_ip[@]}" --set-json 'egress.directDestinations=[{"cidr":"0.0.0.0/0","ports":[443]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --skip-schema-validation --set egress.enabled=false \
  --set-json 'egress.directDestinations=[{"cidr":"0.0.0.0/0","ports":[443]}]' > /dev/null \
  || { echo "FAIL: with egress off, egress.directDestinations is not read and must render"; exit 1; }

echo "--- bash -n (release scripts)"
bash -n scripts/release/retag-images.sh

echo "Chart lint OK across ${#renders[@]} topologies."
