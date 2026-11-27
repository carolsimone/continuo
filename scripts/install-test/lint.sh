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
  "credential-files:scripts/install-test/values-credential-files.yaml"
  "company-files:scripts/install-test/values-company-files.yaml"
  "redis-shared:scripts/install-test/values-redis-shared.yaml"
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

echo "--- component enablement, replica counts and independent application ports"
python3 scripts/install-test/test-component-controls.py -v

# The chart default-denies ingress and re-opens exactly the edges the component
# graph needs. A component that starts calling another without its allow rule
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

# streams-init creates every Redis stream and consumer group before the
# components need them. With a BYO Redis (it exists already) it is a pre-install
# and pre-upgrade hook ordered after the migrate hook, so the streams exist
# before the components roll; with the bundled Redis (no release resource exists
# while hooks run) it is a regular Job that waits for Redis. A hook cannot read
# a Secret or ConfigMap the release creates as a regular resource, so a
# shared-mode BYO render with an inline password must give it a hook Secret.
# redisInit.createStreams reaches it as REDIS_INIT_CREATE_STREAMS, also when a
# release has no redisInit values (helm upgrade --reuse-values), and the Redis
# ACL mode as REDIS_ACL_MODE.
echo "--- streams-init Job: a hook with a BYO Redis, a regular Job with the bundled one, createStreams reaches it"
python3 scripts/install-test/assert-streams-init.py "${tmp}/defaults.yaml" --mode job --create-streams true --acl-mode perComponent
for f in byo-example byo-inline byo-secret; do
  python3 scripts/install-test/assert-streams-init.py "${tmp}/${f}.yaml" --mode hook --create-streams true --acl-mode perComponent
done
python3 scripts/install-test/assert-streams-init.py "${tmp}/redis-shared.yaml" --mode job --create-streams true --acl-mode shared
for f in byo-inline byo-secret; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f "scripts/install-test/values-${f}.yaml" \
    -f scripts/install-test/values-redis-shared.yaml > "${tmp}/streams-init-${f}-shared.yaml"
  python3 scripts/install-test/assert-streams-init.py "${tmp}/streams-init-${f}-shared.yaml" --mode hook \
    --create-streams true --acl-mode shared
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set redisInit.createStreams=false \
  > "${tmp}/streams-init-off.yaml"
python3 scripts/install-test/assert-streams-init.py "${tmp}/streams-init-off.yaml" --mode job --create-streams false --acl-mode perComponent
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-byo-inline.yaml \
  --set redisInit.createStreams=false > "${tmp}/streams-init-byo-off.yaml"
python3 scripts/install-test/assert-streams-init.py "${tmp}/streams-init-byo-off.yaml" --mode hook --create-streams false --acl-mode perComponent
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set redisInit=null > "${tmp}/streams-init-null.yaml" \
  || { echo "FAIL: rendering with redisInit=null failed; values without a redisInit block must render"; exit 1; }
python3 scripts/install-test/assert-streams-init.py "${tmp}/streams-init-null.yaml" --mode job --create-streams true --acl-mode perComponent
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set redisInit.createStreams=maybe > /dev/null 2>&1; then
  echo "FAIL: redisInit.createStreams=maybe must fail the render"; exit 1
fi

# A component reads its HTTP port under one specific env name. The chart sets a
# differently named variable that works only while the default happens to equal
# the Deployment's containerPort, and changing httpPort then moves the probes
# and Service but not the listener. Assert each component gets its port under
# the name its own code reads.
echo "--- http port env names match what each component reads (defaults)"
python3 scripts/install-test/assert-port-env.py "${tmp}/defaults.yaml" .

# run-controller's pods all consume one status consumer group and dispatch what
# a success unblocks, so the previous release's pod must be gone before the new
# one consumes anything. agent-remediation fails, at every start, the attempt
# rows in flight that have no remediation, so a pod of the previous release
# must be gone before it starts or could write such a row after that sweep. Both
# roll with Recreate.
echo "--- run-controller and agent-remediation roll with Recreate (defaults)"
python3 scripts/install-test/assert-recreate.py "${tmp}/defaults.yaml" run-controller agent-remediation

# Recreate leaves run-controller's Service with no endpoint until the new pod is
# Ready, and every read of a run fails meanwhile, so that pod must turn Ready as
# soon as it can serve.
echo "--- run-controller turns Ready as soon as it can serve (defaults)"
python3 scripts/install-test/assert-run-controller-fast-ready.py "${tmp}/defaults.yaml"

# A bundled Redis is a StatefulSet with no ordering guarantee against the
# Deployments, so every component whose code connects to Redis must wait for it
# in bundled mode, and none may wait in BYO mode, where Redis is already up.
# Which components connect to Redis is read from their source.
echo "--- wait-for-redis gates every Redis-using component (defaults) and none (byo)"
python3 scripts/install-test/assert-redis-gate.py . "${tmp}/defaults.yaml" \
  "${tmp}/byo-example.yaml" "${tmp}/byo-inline.yaml" "${tmp}/byo-secret.yaml"

# Credentials reach a pod in one of three ways: as secretKeyRef variables (env
# delivery), as files projected from the same Secrets with <VAR>_FILE pointing
# at them (file delivery), or as files the company provides at paths it names
# (credentials.files), with no Secret behind them. Every component runs under a
# ServiceAccount of its own in each. The file render is compared with the same
# values in env delivery, so a credential cannot get lost between the two.
echo "--- credential delivery: env, file and company-provided files"
for f in defaults byo-example byo-inline; do
  python3 scripts/install-test/assert-credential-delivery.py "${tmp}/${f}.yaml" env
done
python3 scripts/install-test/assert-credential-delivery.py "${tmp}/byo-secret.yaml" file
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  -f scripts/install-test/values-credential-files.yaml --set credentials.delivery=env > "${tmp}/credential-files-env.yaml"
python3 scripts/install-test/assert-credential-delivery.py "${tmp}/credential-files.yaml" file "${tmp}/credential-files-env.yaml"
python3 scripts/install-test/assert-credential-delivery.py "${tmp}/company-files.yaml" company
# deployment_env NAME RENDER: the main container env of Deployment NAME, one
# `VAR=value` (or `VAR<-secret/key`) line per variable.
deployment_env() {
  python3 - "$1" "$2" <<'PY'
import sys, yaml
for doc in yaml.safe_load_all(open(sys.argv[2])):
    if doc and doc.get("kind") == "Deployment" and doc["metadata"]["name"] == sys.argv[1]:
        for e in doc["spec"]["template"]["spec"]["containers"][0].get("env") or []:
            ref = (e.get("valueFrom") or {}).get("secretKeyRef")
            print(f"{e['name']}<-{ref['name']}/{ref['key']}" if ref else f"{e['name']}={e.get('value', '')}")
PY
}
# A company file needs no components entry of its own: execution-controller's S3
# keys, given only in credentials.files, still reach it as files, and its
# other credentials stay secretKeyRef variables under the install's env delivery.
company_executor="$(deployment_env execution-controller "${tmp}/company-files.yaml")"
grep -qx 'AWS_ACCESS_KEY_ID_FILE=/mnt/secrets-store/s3-access-key-id' <<<"$company_executor" \
  && grep -qx 'AWS_SECRET_ACCESS_KEY_FILE=/mnt/secrets-store/s3-secret-access-key' <<<"$company_executor" \
  && grep -q '^POSTGRES_PASSWORD<-' <<<"$company_executor" \
  || { echo "FAIL: execution-controller in the company-files render:"; echo "$company_executor"; exit 1; }
# A key with no value in the chart's own Secret (llm.apiKey and the GitHub keys
# default to empty) is left out under file delivery: an empty file fails the
# component's start, where an empty variable reads as unset.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set credentials.delivery=file > "${tmp}/file-defaults.yaml"
if deployment_env agent-chat "${tmp}/file-defaults.yaml" | grep -q '^LLM_API_KEY'; then
  echo "FAIL: file delivery with an empty llm.apiKey must give agent-chat no LLM_API_KEY variable or file"; exit 1
fi
python3 scripts/install-test/assert-credential-delivery.py "${tmp}/file-defaults.yaml" file
# Each component signs in to Postgres as its own login, read from that login's
# Secret; the migrate Job grants each login its rights, and in the bundled
# quickstart creates them.
echo "--- Postgres: every component signs in as its own login"
for f in defaults credential-files file-defaults; do
  python3 scripts/install-test/assert-postgres-logins.py "${tmp}/${f}.yaml" bundled
done
for f in byo-example byo-inline byo-secret; do
  python3 scripts/install-test/assert-postgres-logins.py "${tmp}/${f}.yaml" external 'continuo-pg-{component}'
done
# An external Postgres values file written before per-component logins (no
# credentials.postgres block) fails the render, naming the login to create.
python3 - scripts/install-test/values-byo-secret.yaml "${tmp}/byo-secret-no-logins.yaml" <<'PY'
import sys, yaml
values = yaml.safe_load(open(sys.argv[1]))
values.get("credentials", {}).pop("postgres", None)
yaml.safe_dump(values, open(sys.argv[2], "w"))
PY
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f "${tmp}/byo-secret-no-logins.yaml" \
     > /dev/null 2> "${tmp}/pglogins.err"; then
  echo "FAIL: an external Postgres with no login Secrets must fail the render"; exit 1
fi
grep -qF 'credentials.postgres.run-controller.existingSecret is required: create the login continuo_run_controller' "${tmp}/pglogins.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/pglogins.err"; exit 1; }
# The chart builds the warehouse Secret only for the bundled Postgres.
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-byo-secret.yaml \
     --set validation.createWarehouseSecret=true > /dev/null 2> "${tmp}/pglogins.err"; then
  echo "FAIL: validation.createWarehouseSecret=true with an external Postgres must fail the render"; exit 1
fi
grep -qF 'validation.createWarehouseSecret needs the bundled Postgres' "${tmp}/pglogins.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/pglogins.err"; exit 1; }
# Each Redis component signs in as its own login, with the rule the streams
# generator wrote for it; the bundled Redis starts from an ACL file of those
# rules with the default user off, and streams-init signs in as every login it
# can reach before it creates the streams. A Redis with no ACL users runs in
# shared mode, with one password.
echo "--- Redis: every component signs in as its own login (perComponent), or with the one password (shared)"
for f in defaults credential-files file-defaults; do
  python3 scripts/install-test/assert-redis-logins.py "${tmp}/${f}.yaml" perComponent bundled
done
for f in byo-example byo-inline byo-secret; do
  python3 scripts/install-test/assert-redis-logins.py "${tmp}/${f}.yaml" perComponent external 'continuo-redis-{component}'
done
python3 scripts/install-test/assert-redis-logins.py "${tmp}/redis-shared.yaml" shared
# A login whose password reaches its component only as a company file is checked
# when that component starts, not by streams-init; with every login given that
# way streams-init mounts no login file and gets no REDIS_LOGINS_DIR, which
# would name a directory that does not exist.
files_args=()
for component in run-controller catalog agent-chat execution-controller ui topology-controller \
           release-controller remediation agent-remediation dead-letter-controller; do
  files_args+=(--set "credentials.files.${component}.REDIS_PASSWORD=/vault/secrets/redis")
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-byo-secret.yaml \
  "${files_args[@]}" > "${tmp}/redis-company-files.yaml"
python3 - "${tmp}/redis-company-files.yaml" <<'PY'
import sys, yaml
for d in yaml.safe_load_all(open(sys.argv[1])):
    if d and d.get("kind") == "Job" and d["metadata"].get("labels", {}).get("app.kubernetes.io/name") == "streams-init":
        spec = d["spec"]["template"]["spec"]
        env = {e["name"] for c in spec["containers"] for e in c.get("env") or []}
        vols = {v["name"] for v in spec.get("volumes") or []}
        if "REDIS_LOGINS_DIR" in env or "redis-logins" in vols:
            sys.exit(f"FAIL: with every Redis password a company file, streams-init sets REDIS_LOGINS_DIR={'REDIS_LOGINS_DIR' in env} and mounts redis-logins={'redis-logins' in vols}; want neither")
        print("streams-init checks no login file when every Redis password is a company file")
        break
else:
    sys.exit("FAIL: no streams-init Job rendered")
PY
# Redis reads its ACL file only when it starts, so any change to the rendered
# file (a rule, a login's name, its password) restarts the bundled Redis.
redis_acl_checksum() {
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$@" --show-only templates/redis/statefulset.yaml \
    | python3 -c 'import sys, yaml; d = yaml.safe_load(sys.stdin); print(d["spec"]["template"]["metadata"]["annotations"]["checksum/redis-acl"])'
}
[ "$(redis_acl_checksum)" = "$(redis_acl_checksum)" ] \
  || { echo "FAIL: the bundled Redis's ACL checksum must be the same on two renders of the same values"; exit 1; }
[ "$(redis_acl_checksum)" != "$(redis_acl_checksum --set credentials.redis.logins.ui.username=continuo_ui_renamed)" ] \
  || { echo "FAIL: renaming a Redis login must change the bundled Redis's ACL checksum"; exit 1; }
python3 - scripts/install-test/values-byo-secret.yaml "${tmp}/byo-secret-no-redis-logins.yaml" <<'PY'
import sys, yaml
values = yaml.safe_load(open(sys.argv[1]))
values.get("credentials", {}).pop("redis", None)
yaml.safe_dump(values, open(sys.argv[2], "w"))
PY
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f "${tmp}/byo-secret-no-redis-logins.yaml" \
     > /dev/null 2> "${tmp}/redislogins.err"; then
  echo "FAIL: an external Redis in perComponent mode with no login Secrets must fail the render"; exit 1
fi
grep -qF 'credentials.redis.logins.run-controller.existingSecret is required: create the login continuo_run_controller' "${tmp}/redislogins.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/redislogins.err"; exit 1; }
# A credentials.files entry names a variable continuo reads as a credential;
# anything else would never be read.
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set credentials.files.agent-chat.LLM_KEY=/vault/secrets/llm > /dev/null 2> "${tmp}/credfiles.err"; then
  echo "FAIL: a credentials.files key that is not a credential must fail the render"; exit 1
fi
grep -qF 'credentials.files.agent-chat.LLM_KEY: not a credential continuo reads' "${tmp}/credfiles.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/credfiles.err"; exit 1; }
# A credentials.files key names an enabled components entry; a typo or a disabled
# component would mount files no pod reads.
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set credentials.files.agent-chatt.LLM_API_KEY=/vault/secrets/llm > /dev/null 2> "${tmp}/credfiles.err"; then
  echo "FAIL: a credentials.files key that names no components entry must fail the render"; exit 1
fi
grep -qF 'credentials.files.agent-chatt: no enabled component named agent-chatt' "${tmp}/credfiles.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/credfiles.err"; exit 1; }
python3 - "$CHART/values.yaml" "${tmp}/agent-chat-disabled.yaml" <<'PY'
import sys, yaml
components = yaml.safe_load(open(sys.argv[1]))["components"]
for component in components:
    if component["name"] == "agent-chat":
        component["enabled"] = False
yaml.safe_dump({"components": components}, open(sys.argv[2], "w"))
PY
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f "${tmp}/agent-chat-disabled.yaml" \
     --set credentials.files.agent-chat.LLM_API_KEY=/vault/secrets/llm > /dev/null 2> "${tmp}/credfiles.err"; then
  echo "FAIL: a credentials.files key that names a disabled component must fail the render"; exit 1
fi
grep -qF 'credentials.files.agent-chat: no enabled component named agent-chat' "${tmp}/credfiles.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/credfiles.err"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set credentials.files.agent-chat.LLM_API_KEY=/vault/secrets/llm > /dev/null \
  || { echo "FAIL: a credentials.files key that names an enabled component must render"; exit 1; }
# Job pods take the S3 key pair only from a Kubernetes Secret, so S3 keys that
# reach execution-controller as company files need execution.jobS3Secret.name.
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-company-files.yaml \
     --set execution.jobS3Secret.name= > /dev/null 2> "${tmp}/jobs3.err"; then
  echo "FAIL: execution-controller S3 keys as company files without execution.jobS3Secret.name must fail the render"; exit 1
fi
grep -qF "execution.jobS3Secret.name is required when execution-controller's S3 keys are files: Job pods can only reference a Kubernetes Secret" "${tmp}/jobs3.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/jobs3.err"; exit 1; }
# The Secret Job pods read the S3 key pair from: the chart's own S3 Secret and
# keys by default, or execution.jobS3Secret with its keys defaulting to the
# variable names.
for want in 'JOB_S3_SECRET_NAME: "continuo-minio"' 'JOB_S3_ACCESS_KEY_ID_KEY: "access-key-id"' \
            'JOB_S3_SECRET_ACCESS_KEY_KEY: "secret-access-key"'; do
  grep -qF "$want" "${tmp}/defaults.yaml" || { echo "FAIL: defaults render lacks ${want}"; exit 1; }
done
for want in 'JOB_S3_SECRET_NAME: "company-job-s3"' 'JOB_S3_ACCESS_KEY_ID_KEY: "AWS_ACCESS_KEY_ID"' \
            'JOB_S3_SECRET_ACCESS_KEY_KEY: "AWS_SECRET_ACCESS_KEY"'; do
  grep -qF "$want" "${tmp}/company-files.yaml" || { echo "FAIL: company-files render lacks ${want}"; exit 1; }
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set credentials=null > "${tmp}/credentials-null.yaml" \
  || { echo "FAIL: rendering with credentials=null failed; values without a credentials block must render"; exit 1; }
python3 scripts/install-test/assert-credential-delivery.py "${tmp}/credentials-null.yaml" env
for bad in 'credentials.delivery=vault' 'credentials.delivery='; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --skip-schema-validation --set "$bad" > /dev/null 2> "${tmp}/delivery.err"; then
    echo "FAIL: ${bad} must fail the render even with the schema check skipped"; exit 1
  fi
  grep -qF 'credentials.delivery' "${tmp}/delivery.err" || { echo "FAIL: unexpected error:"; cat "${tmp}/delivery.err"; exit 1; }
done
# ui and topology-controller read the Redis password as REDIS_PASSWORD, so the
# REDIS_URL the chart gives them carries none.
for component in ui topology-controller; do
  deployment_env "$component" "${tmp}/defaults.yaml" | grep -qx 'REDIS_URL=redis://continuo-redis:6379' \
    || { echo "FAIL: ${component}'s REDIS_URL must be redis://continuo-redis:6379"; deployment_env "$component" "${tmp}/defaults.yaml"; exit 1; }
done
# Each component's pods pull with the image pull secrets of its own
# ServiceAccount: none by default, the top-level imagePullSecrets on every
# ServiceAccount, and a component's serviceAccount.imagePullSecrets in place of
# them for that component.
echo "--- imagePullSecrets on every ServiceAccount, replaced per component"
# pull_secrets RENDER: one `<ServiceAccount> <secret,...>` line per
# ServiceAccount, `-` for none.
pull_secrets() {
  python3 - "$1" <<'PY'
import sys, yaml
for doc in yaml.safe_load_all(open(sys.argv[1])):
    if doc and doc.get("kind") == "ServiceAccount":
        print(doc["metadata"]["name"], ",".join(s["name"] for s in doc.get("imagePullSecrets") or []) or "-")
PY
}
[ -z "$(pull_secrets "${tmp}/defaults.yaml" | grep -v ' -$')" ] \
  || { echo "FAIL: the defaults render puts imagePullSecrets on a ServiceAccount:"; pull_secrets "${tmp}/defaults.yaml"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json 'imagePullSecrets=[{"name":"registry-cred"}]' \
  > "${tmp}/pull-global.yaml"
pull_global="$(pull_secrets "${tmp}/pull-global.yaml")"
[ "$(grep -c ' registry-cred$' <<<"$pull_global")" = 10 ] && [ "$(wc -l <<<"$pull_global" | tr -d ' ')" = 10 ] \
  || { echo "FAIL: imagePullSecrets must reach all ten ServiceAccounts:"; echo "$pull_global"; exit 1; }
# `components` is a list Helm replaces whole, so the override goes into a copy of
# the company fixture's list, whose first entry is agent-chat.
python3 - "${tmp}/company-pull.yaml" <<'PY'
import sys, yaml
values = yaml.safe_load(open("scripts/install-test/values-company-files.yaml"))
values["imagePullSecrets"] = [{"name": "registry-cred"}]
values["components"][0]["serviceAccount"]["imagePullSecrets"] = [{"name": "agent-chat-cred"}]
yaml.safe_dump(values, open(sys.argv[1], "w"))
PY
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f "${tmp}/company-pull.yaml" > "${tmp}/pull-override.yaml"
pull_override="$(pull_secrets "${tmp}/pull-override.yaml")"
grep -qx 'agent-chat agent-chat-cred' <<<"$pull_override" \
  && grep -qx 'execution-controller registry-cred' <<<"$pull_override" \
  || { echo "FAIL: agent-chat's serviceAccount.imagePullSecrets must replace the top-level list for it alone:"; echo "$pull_override"; exit 1; }

# A datastore password every consumer reads from a company file needs no key in
# the chart's credentials Secret, so an external Neo4j with catalog's password
# as a file renders without externalNeo4j.password, and still demands one when
# a component reads it from the Secret.
echo "--- company-file datastore password needs no inline Secret value"
cat > "${tmp}/neo4j-company.yaml" <<'YAML'
neo4j:
  enabled: false
externalNeo4j:
  uri: bolt://neo4j.example:7687
credentials:
  files:
    catalog:
      NEO4J_PASSWORD: /vault/secrets/neo4j
YAML
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-byo-inline.yaml \
  -f "${tmp}/neo4j-company.yaml" --set externalNeo4j.password= > "${tmp}/neo4j-company-render.yaml" \
  || { echo "FAIL: an external Neo4j whose password is a company file must render without externalNeo4j.password"; exit 1; }
! grep -qF 'neo4j-password' "${tmp}/neo4j-company-render.yaml" \
  || { echo "FAIL: the credentials Secret must carry no neo4j-password when every consumer reads a company file"; exit 1; }
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-byo-inline.yaml \
     --set externalNeo4j.password= > /dev/null 2> "${tmp}/neo4j-required.err"; then
  echo "FAIL: an external Neo4j password read from the Secret must still be required"; exit 1
fi
grep -qF 'set externalNeo4j.password or externalNeo4j.existingSecret' "${tmp}/neo4j-required.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/neo4j-required.err"; exit 1; }

# The wait init containers read a password file as the components do: exactly one
# trailing LF or CRLF dropped. Runs the rendered shell on files that end each
# way.
echo "--- wait gates read credential files like the components"
python3 - "${tmp}/credential-files.yaml" <<'PY'
import subprocess, sys, tempfile, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d and d.get("kind") == "Deployment"]
scripts = {}
for doc in docs:
    for init in doc["spec"]["template"]["spec"].get("initContainers") or []:
        scripts[init["name"]] = init["command"][2]
assert {"wait-for-redis", "wait-for-migrations"} <= scripts.keys(), sorted(scripts)
cases = {b"pw": "pw", b"pw\n": "pw", b"pw\r\n": "pw", b"pw\n\n": "pw\n", b"pw\r": "pw\r", b"pw\r\n\r\n": "pw\r\n"}
for name, script in scripts.items():
    if "read_credential" not in script:
        continue
    preamble = script.split("if [ -n", 1)[0]
    for content, want in cases.items():
        with tempfile.NamedTemporaryFile() as f:
            f.write(content); f.flush()
            run = subprocess.run(["sh", "-c", preamble + f'read_credential {f.name}; printf %s "$credential"'],
                                 capture_output=True, check=True)
        if run.stdout.decode() != want:
            sys.exit(f"FAIL: {name} read {content!r} as {run.stdout!r}, want {want!r}")
    gone = subprocess.run(["sh", "-c", preamble + 'read_credential /nonexistent; echo reached'], capture_output=True)
    if gone.returncode == 0 or b"reached" in gone.stdout:
        sys.exit(f"FAIL: {name} must stop when its credential file is missing")
PY

# Components read the shared ConfigMap through envFrom, and Kubernetes never
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
# ConfigMap (dbt commands, project repositories) only at startup, so each pod template
# carries a digest of it. For every chart-managed ConfigMap, a
# change must roll exactly the workloads that read it at startup and no others;
# with the volume pointed at an operator-owned ConfigMap it must roll nothing;
# and every chart ConfigMap a workload consumes must have a case in the script.
echo "--- chart ConfigMap changes roll exactly the pods that read them at startup"
python3 scripts/install-test/assert-config-rollout.py "$CHART" "$KUBE_VERSION"

# A components[].volumes entry with an empty configMap.name resolves to the chart's
# own ConfigMap only on a component the chart mounts one into (execution-controller,
# agent-remediation, ui). On any other component it would render a volume the API
# server refuses at apply time, so the render fails and names the entry instead.
echo "--- an empty configMap.name on a component with no chart ConfigMap fails the render"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     -f scripts/install-test/values-empty-configmap-volume.yaml > /dev/null 2> "${tmp}/emptyvolume.err"; then
  echo "FAIL: a run-controller volume with an empty configMap.name rendered; the API server would refuse the Deployment"; exit 1
fi
grep -qF 'components[run-controller].volumes[extra-config]: configMap.name is empty' "${tmp}/emptyvolume.err" \
  || { echo "FAIL: unexpected error:"; cat "${tmp}/emptyvolume.err"; exit 1; }
# agent-remediation mounts two chart ConfigMaps, each through the volume of its
# own name. An empty configMap.name on a volume of another name mounts the first
# of them, project-repos, with its digest on the pod template, so a components
# list written for a single agent-remediation ConfigMap keeps working.
echo "--- an empty configMap.name on an agent-remediation volume of another name mounts project-repos"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  -f scripts/install-test/values-unknown-configmap-volume.yaml > "${tmp}/unknownvolume.yaml" \
  || { echo "FAIL: an agent-remediation volume of an unknown name with an empty configMap.name failed the render"; exit 1; }
python3 - "${tmp}/unknownvolume.yaml" <<'PY' || { echo "FAIL: the agent-remediation volume extra-config does not mount continuo-project-repos with checksum/project-repos"; exit 1; }
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
pod = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "agent-remediation")["spec"]["template"]
volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
annotations = pod["metadata"]["annotations"]
sys.exit(0 if volumes["extra-config"].get("configMap") == {"name": "continuo-project-repos"}
         and "checksum/project-repos" in annotations
         and "checksum/agent-remediation-workflows" not in annotations else 1)
PY
# A volume named remediation-workflows mounts the ConfigMap of the remediation
# files only while it is mounted at the folder agent-remediation reads them
# from (AGENT_REMEDIATION_WORKFLOWS_DIR, else /etc/continuo-workflows, compared
# without a trailing slash). Mounted anywhere else, or nowhere, it mounts
# project-repos like a volume of any other name, so a components entry whose
# volume of that name holds the project repository map at /etc/continuo keeps
# it.
echo "--- an empty-named remediation-workflows volume mounts the remediation files only at the folder agent-remediation reads"
# single_volume_components <volumeMounts JSON> [<env JSON>] prints a components
# list of one agent-remediation entry whose one volume is an empty-named
# ConfigMap volume called remediation-workflows.
single_volume_components() {
  local env="${2-}"
  [ -n "$env" ] || env='{}'
  printf '[{"name":"agent-remediation","enabled":true,"httpPort":8092,"grpcPort":50054,"env":%s,"volumeMounts":%s,"volumes":[{"name":"remediation-workflows","configMap":{"name":""}}]}]' \
    "$env" "$1"
}
for placement in \
  "at /etc/continuo, the project-repos folder|continuo-project-repos|project-repos|agent-remediation-workflows|$(single_volume_components '[{"name":"remediation-workflows","mountPath":"/etc/continuo","readOnly":true}]')" \
  "with no volumeMounts entry|continuo-project-repos|project-repos|agent-remediation-workflows|$(single_volume_components '[]')" \
  "at the default folder of the remediation files|continuo-agent-remediation-workflows|agent-remediation-workflows|project-repos|$(single_volume_components '[{"name":"remediation-workflows","mountPath":"/etc/continuo-workflows/","readOnly":true}]')" \
  "at the folder AGENT_REMEDIATION_WORKFLOWS_DIR names|continuo-agent-remediation-workflows|agent-remediation-workflows|project-repos|$(single_volume_components '[{"name":"remediation-workflows","mountPath":"/opt/remediation-files","readOnly":true}]' '{"AGENT_REMEDIATION_WORKFLOWS_DIR":"/opt/remediation-files/"}')" \
  "at /etc/continuo-workflows while AGENT_REMEDIATION_WORKFLOWS_DIR names another folder|continuo-project-repos|project-repos|agent-remediation-workflows|$(single_volume_components '[{"name":"remediation-workflows","mountPath":"/etc/continuo-workflows","readOnly":true}]' '{"AGENT_REMEDIATION_WORKFLOWS_DIR":"/opt/remediation-files"}')"; do
  IFS='|' read -r label configmap hashed unhashed components <<< "$placement"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json "components=${components}" > "${tmp}/placement.yaml" 2> "${tmp}/placement.err" \
    || { echo "FAIL: a remediation-workflows volume ${label} failed the render:"; cat "${tmp}/placement.err"; exit 1; }
  python3 - "${tmp}/placement.yaml" "$configmap" "$hashed" "$unhashed" <<'PY' \
    || { echo "FAIL: a remediation-workflows volume ${label} does not mount ${configmap} with checksum/${hashed} alone"; exit 1; }
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
pod = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "agent-remediation")["spec"]["template"]
volumes = {v["name"]: v for v in pod["spec"]["volumes"]}
annotations = pod["metadata"]["annotations"]
sys.exit(0 if volumes["remediation-workflows"].get("configMap") == {"name": sys.argv[2]}
         and "checksum/" + sys.argv[3] in annotations
         and "checksum/" + sys.argv[4] not in annotations else 1)
PY
done

# An install's remediation files (agentRemediation.workflows.files) are the keys
# of the ConfigMap <release>-agent-remediation-workflows, which agent-remediation
# mounts; the defaults render it with no key. The chart checks the kind and the
# version of every document of every file, and a file agent-remediation would
# refuse at startup fails the render, naming the file. A name starting with a
# dot, which the loader skips, fails the schema.
echo "--- agentRemediation.workflows.files: each file is a key of the mounted ConfigMap, a bad one fails the render"
wf=scripts/install-test/fixtures/remediation-workflows
grep -qE '^  name: continuo-agent-remediation-workflows$' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the defaults render has no agent-remediation-workflows ConfigMap"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-file "agentRemediation.workflows.files.valid\.yaml=${wf}/valid.yaml" > "${tmp}/workflows.yaml" \
  || { echo "FAIL: a valid remediation file failed the render"; exit 1; }
python3 - "${tmp}/workflows.yaml" "${wf}/valid.yaml" <<'PY' || { echo "FAIL: the remediation file did not reach the ConfigMap as written"; exit 1; }
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
cm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "continuo-agent-remediation-workflows")
sys.exit(0 if cm["data"] == {"valid.yaml": open(sys.argv[2]).read()} else 1)
PY
# A file's revision is the sha256 of its bytes, so the mounted file must hold
# the bytes of the file in Git, whatever they end with or hold: no newline at
# the end, one, several (a `|+` block keeps them), CRLF line ends, tabs, the
# characters < > &, a letter outside ASCII, a `---` line, a line that starts
# with |. Each is written here with printf, so no editor or Git setting changes
# its bytes, and the ConfigMap the chart renders must give each one back byte
# for byte. A name starting with - or _ stays the key as written.
echo "--- agentRemediation.workflows.files: the mounted files hold the bytes of the files given"
bytes_dir="${tmp}/workflow-bytes"
mkdir -p "$bytes_dir"
printf 'kind: type\nversion: 1\nname: no-newline' > "${bytes_dir}/no-newline.yaml"
printf 'kind: type\nversion: 1\nname: one-newline\n' > "${bytes_dir}/one-newline.yaml"
printf 'kind: type\nversion: 1\nname: three-newlines\n\n\n' > "${bytes_dir}/three-newlines.yaml"
printf 'kind: type\r\nversion: 1\r\nname: "crlf \xc3\xa9t\xc3\xa9\ttab <a> & b"\r\ndescription: "first\r\n|pipe at the start of a line"\r\n---\r\nkind: fixture\r\nversion: 1\r\ntype: crlf\r\n' \
  > "${bytes_dir}/-mixed.yaml"
printf 'kind: fixture\nversion: 1\ntype: keep\nrepo_files:\n  models/a.sql: |+\n    select 1\n    | a line that starts with a pipe\n\n\n' > "${bytes_dir}/_keep-block.yaml"
bytes_args=()
for path in "${bytes_dir}"/*.yaml; do
  file="$(basename "$path")"
  bytes_args+=(--set-file "agentRemediation.workflows.files.${file//./\\.}=${path}")
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "${bytes_args[@]}" > "${tmp}/workflow-bytes.yaml" 2> "${tmp}/workflows.err" \
  || { echo "FAIL: remediation files of every ending failed the render:"; cat "${tmp}/workflows.err"; exit 1; }
python3 - "${tmp}/workflow-bytes.yaml" "$bytes_dir" <<'PY' || { echo "FAIL: a mounted remediation file does not hold the bytes of the file given"; exit 1; }
import os, sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1], encoding="utf-8", newline="")) if d]
cm = next(d for d in docs if d["kind"] == "ConfigMap" and d["metadata"]["name"] == "continuo-agent-remediation-workflows")
want = {name: open(os.path.join(sys.argv[2], name), "rb").read().decode("utf-8") for name in os.listdir(sys.argv[2])}
bad = [name for name in sorted(want) if cm["data"].get(name) != want[name]]
for name in bad:
    print(f"{name}: got {cm['data'].get(name)!r}, want {want[name]!r}")
if sorted(cm["data"]) != sorted(want):
    print(f"keys: got {sorted(cm['data'])}, want {sorted(want)}")
    sys.exit(1)
kept = yaml.safe_load(cm["data"]["_keep-block.yaml"])["repo_files"]["models/a.sql"]
if kept != "select 1\n| a line that starts with a pipe\n\n\n":
    print(f"the |+ block reads {kept!r}")
    sys.exit(1)
sys.exit(1 if bad else 0)
PY
echo "$(find "$bytes_dir" -name '*.yaml' | wc -l | tr -d ' ') remediation files reach the ConfigMap byte for byte"
# A document separator is `---` alone, `---` and a comment, or `---` and the
# start of the document; with CRLF line endings it is `---` and a carriage
# return. The CRLF file is written here, so no editor can turn it into LF.
printf 'kind: type\r\nversion: 1\r\nname: lint-probe\r\n---\r\nkind: flow\r\nversion: 2\r\nname: lint-probe-flow\r\n' \
  > "${tmp}/crlf-separator.yaml"
for bad in \
  'version-2.yaml|: version 2 is not supported (supported: 1)' \
  'version-string.yaml|: version "1" is not a number (supported: 1)' \
  'no-version.yaml|: has no version (supported: 1)' \
  'second-document.yaml| document 2: version 2 is not supported (supported: 1)' \
  'commented-separator.yaml| document 2: version 2 is not supported (supported: 1)' \
  'inline-separator.yaml| document 2: version 2 is not supported (supported: 1)' \
  'crlf-separator.yaml| document 2: version 2 is not supported (supported: 1)' \
  'wrong-kind.yaml|: kind "workflow" is not flow, type or fixture' \
  'not-yaml.yaml|: does not parse as a YAML mapping' \
  'not-a-mapping.yaml|: does not parse as a YAML mapping'; do
  file="${bad%%|*}"
  want="agentRemediation.workflows.files[${file}]${bad#*|}"
  path="${wf}/${file}"
  [ -f "$path" ] || path="${tmp}/${file}"
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
       --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: the remediation file ${file} rendered; agent-remediation would refuse it at startup"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL: want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
done
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set-file 'agentRemediation.workflows.files.\.hidden\.yaml='"${wf}/valid.yaml" > /dev/null 2> "${tmp}/workflows.err"; then
  echo "FAIL: a remediation file named .hidden.yaml rendered; agent-remediation skips it"; exit 1
fi
grep -qF '.hidden.yaml' "${tmp}/workflows.err" || { echo "FAIL: unexpected error:"; cat "${tmp}/workflows.err"; exit 1; }
# The chart reads every placement of `---` markers, blank lines and comments as
# agent-remediation's loader does (TestLoad_EmptyDocuments in
# agent-remediation/adapters/workflowfile pins the loader on the same shapes):
# text before the first marker that holds only blank lines and comments is no
# document, and every marker starts one, so a marker that nothing, blank lines
# and comments alone, or another marker follows is an empty document, which
# fails the render naming the file and the document. A file named accepted-*
# renders and one named refused-* fails; the shapes whose whitespace or line
# ends an editor would change are written here.
echo "--- agentRemediation.workflows.files: an empty YAML document fails the render, as the loader refuses it"
empty_docs="${wf}/empty-documents"
printf '' > "${tmp}/accepted-empty.yaml"
printf '\n\n' > "${tmp}/accepted-blank-lines.yaml"
printf 'kind: type\r\nversion: 1\r\nname: lint-probe\r\n---\r\nkind: flow\r\nversion: 1\r\nname: lint-probe-flow\r\n' \
  > "${tmp}/accepted-crlf-two-documents.yaml"
printf 'kind: type\nversion: 1\nname: lint-probe\n---' > "${tmp}/refused-trailing-marker-no-newline.yaml"
printf 'kind: type\nversion: 1\nname: lint-probe\n---   \n' > "${tmp}/refused-trailing-marker-blanks.yaml"
printf 'kind: type\nversion: 1\nname: lint-probe\n---\n\n\n' > "${tmp}/refused-marker-then-blank-lines.yaml"
printf 'kind: type\r\nversion: 1\r\nname: lint-probe\r\n---\r\n' > "${tmp}/refused-trailing-marker-crlf.yaml"
accepted_shapes=0
for path in "${empty_docs}"/accepted-*.yaml "${tmp}/accepted-empty.yaml" "${tmp}/accepted-blank-lines.yaml" \
            "${tmp}/accepted-crlf-two-documents.yaml"; do
  file="$(basename "$path")"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err" \
    || { echo "FAIL: the remediation file ${file} failed the render; agent-remediation loads it:"; cat "${tmp}/workflows.err"; exit 1; }
  accepted_shapes=$((accepted_shapes + 1))
done
refused_shapes=0
for bad in \
  'refused-trailing-marker.yaml|2' \
  'refused-marker-then-comments.yaml|2' \
  'refused-marker-comment-nothing.yaml|2' \
  'refused-two-leading-markers.yaml|1' \
  'refused-two-markers-between.yaml|2' \
  'refused-one-marker.yaml|1' \
  'refused-end-marker-then-marker.yaml|2' \
  'refused-trailing-marker-no-newline.yaml|2' \
  'refused-trailing-marker-blanks.yaml|2' \
  'refused-marker-then-blank-lines.yaml|2' \
  'refused-trailing-marker-crlf.yaml|2'; do
  file="${bad%%|*}"
  want="agentRemediation.workflows.files[${file}] document ${bad#*|} is empty: no content follows its --- marker"
  path="${empty_docs}/${file}"
  [ -f "$path" ] || path="${tmp}/${file}"
  [ -f "$path" ] || { echo "FAIL: no fixture ${file}"; exit 1; }
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
       --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: the remediation file ${file} rendered; agent-remediation refuses its empty document at startup"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL: want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
  refused_shapes=$((refused_shapes + 1))
done
# Every refused-* fixture file has a case above, so a new one cannot go unchecked.
for path in "${empty_docs}"/refused-*.yaml; do
  grep -qF "'$(basename "$path")|" scripts/install-test/lint.sh || { echo "FAIL: ${path} has no case in lint.sh"; exit 1; }
done
echo "${accepted_shapes} placements of markers render, ${refused_shapes} with an empty document fail the render"
# The chart reads YAML directives (%YAML, %TAG) and `...` end markers as the
# loader does (TestLoad_Directives pins the loader on the same shapes, and
# TestLoad_AgreesWithTheChartsFixtures loads every fixture file here): the
# directive lines before a marker, before the first one, after an end marker
# or after a document, belong to the document that marker starts, and they
# are read with it, so a version other than %YAML 1.1, a second %YAML or an
# unknown directive fails like the loader. A directive no marker follows, text
# between an end marker or a directive and the next marker, and an end marker
# before any document fail the render, naming the file. A file named
# accepted-* renders and one named refused-* fails with the text its case
# names.
echo "--- agentRemediation.workflows.files: directives and end markers are read as the loader reads them"
directive_docs="${wf}/directives"
printf '%%YAML 1.1\r\n---\r\nkind: type\r\nversion: 1\r\nname: lint-probe\r\n' > "${tmp}/accepted-directive-crlf.yaml"
accepted_shapes=0
for path in "${directive_docs}"/accepted-*.yaml "${tmp}/accepted-directive-crlf.yaml"; do
  file="$(basename "$path")"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err" \
    || { echo "FAIL: the remediation file ${file} failed the render; agent-remediation loads it:"; cat "${tmp}/workflows.err"; exit 1; }
  accepted_shapes=$((accepted_shapes + 1))
done
refused_shapes=0
for bad in \
  'refused-yaml-1-2.yaml|: does not parse as a YAML mapping: error converting YAML to JSON: yaml: found incompatible YAML document' \
  'refused-yaml-1-2-second-document.yaml| document 2: does not parse as a YAML mapping: error converting YAML to JSON: yaml: found incompatible YAML document' \
  'refused-two-yaml-directives.yaml|: does not parse as a YAML mapping: error converting YAML to JSON: yaml: line 1: found duplicate %YAML directive' \
  'refused-unknown-directive.yaml|: does not parse as a YAML mapping: error converting YAML to JSON: yaml: found unknown directive name' \
  'refused-directive-then-end-marker.yaml|: does not parse as a YAML mapping:' \
  'refused-directive-then-document.yaml|: "kind: type": only blank lines, comments and directives may follow a ... end marker or a % directive before the next --- marker' \
  'refused-document-after-end-marker.yaml|: "kind: flow": only blank lines, comments and directives may follow a ... end marker or a % directive before the next --- marker' \
  'refused-text-on-the-end-marker-line.yaml|: "... more": only blank lines, comments and directives may follow a ... end marker or a % directive before the next --- marker' \
  'refused-directive-alone.yaml|: the directive %YAML 1.1 has no --- marker after it' \
  'refused-directive-at-the-end.yaml|: the directive %YAML 1.1 has no --- marker after it' \
  'refused-directive-after-end-marker-at-the-end.yaml|: the directive %YAML 1.1 has no --- marker after it' \
  'refused-end-marker-before-any-document.yaml|: a ... end marker comes before any document' \
  'refused-directive-then-marker.yaml| document 1 is empty' \
  'refused-directive-right-after-a-marker.yaml| document 2 is empty'; do
  file="${bad%%|*}"
  want="agentRemediation.workflows.files[${file}]${bad#*|}"
  path="${directive_docs}/${file}"
  [ -f "$path" ] || { echo "FAIL: no fixture ${file}"; exit 1; }
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
       --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: the remediation file ${file} rendered; agent-remediation refuses it at startup"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL: want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
  refused_shapes=$((refused_shapes + 1))
done
for path in "${directive_docs}"/refused-*.yaml; do
  grep -qF "'$(basename "$path")|" scripts/install-test/lint.sh || { echo "FAIL: ${path} has no case in lint.sh"; exit 1; }
done
echo "${accepted_shapes} placements of directives and end markers render, ${refused_shapes} the loader refuses fail the render"
# fromYaml keeps the last of two keys of one mapping, where agent-remediation
# refuses the file, so the chart counts the top-level kind and version lines
# of each document (a line that starts with the key and a colon) and fails the
# render on two, naming the file and the document. A kind or version key
# below the top level, or a line of a block scalar, is indented and does not
# count. Every other duplicate key is left to agent-remediation and
# `remediation-check files`.
echo "--- agentRemediation.workflows.files: a kind or version line defined twice fails the render"
duplicate_docs="${wf}/duplicate-keys"
for path in "${duplicate_docs}"/accepted-*.yaml; do
  file="$(basename "$path")"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err" \
    || { echo "FAIL: the remediation file ${file} failed the render; agent-remediation loads it:"; cat "${tmp}/workflows.err"; exit 1; }
done
for bad in \
  'refused-version-twice.yaml| document 1: version is defined 2 times' \
  'refused-kind-twice.yaml| document 1: kind is defined 2 times' \
  'refused-version-twice-in-the-second-document.yaml| document 2: version is defined 2 times'; do
  file="${bad%%|*}"
  want="agentRemediation.workflows.files[${file}]${bad#*|}"
  path="${duplicate_docs}/${file}"
  [ -f "$path" ] || { echo "FAIL: no fixture ${file}"; exit 1; }
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
       --set-file "agentRemediation.workflows.files.${file//./\\.}=${path}" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: the remediation file ${file} rendered; agent-remediation refuses its duplicate key at startup"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL: want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
done
for path in "${duplicate_docs}"/refused-*.yaml; do
  grep -qF "'$(basename "$path")|" scripts/install-test/lint.sh || { echo "FAIL: ${path} has no case in lint.sh"; exit 1; }
done
# The company-files topology brings its own components list, whose
# agent-remediation entry mounts no workflows volume: files set there would
# reach no pod.
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" -f scripts/install-test/values-company-files.yaml \
     --set-file "agentRemediation.workflows.files.valid\.yaml=${wf}/valid.yaml" > /dev/null 2> "${tmp}/workflows.err"; then
  echo "FAIL: remediation files rendered with no agent-remediation volume to mount them"; exit 1
fi
for want in \
  'agentRemediation.workflows.files is set, but components[agent-remediation] has no volume remediation-workflows, so' \
  "a release upgraded with --reuse-values keeps an older chart's components list, so upgrade once with your values file or --reset-then-reuse-values"; do
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL: want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
done
# With files set, an operator's own agent-remediation entry must mount the
# chart's ConfigMap of the files through a remediation-workflows volume with an
# empty configMap.name, the only form that puts the ConfigMap's digest on the
# pod template (so a changed file rolls the pod), with no subPath, at the
# folder agent-remediation reads: the entry's AGENT_REMEDIATION_WORKFLOWS_DIR,
# else config.go's defaultWorkflowsDir, which the chart repeats (the case with
# no variable set pins the two). Each way an entry can miss that fails the
# render with its own message, the chart's own ConfigMap named explicitly
# among them; an entry that moves the variable and the mount together renders,
# and without files nothing is checked.
echo "--- agentRemediation.workflows.files: the files must be mounted where agent-remediation reads them"
default_workflows_dir="$(sed -nE 's/^const defaultWorkflowsDir = "([^"]+)"$/\1/p' agent-remediation/config/config.go)"
[ -n "$default_workflows_dir" ] || { echo "FAIL: agent-remediation/config/config.go declares no const defaultWorkflowsDir"; exit 1; }
chart_workflows_volume='[{"name":"remediation-workflows","configMap":{"name":""}}]'
default_workflows_mount='[{"name":"remediation-workflows","mountPath":"'"${default_workflows_dir}"'","readOnly":true}]'
# workflows_components <volumeMounts JSON> <volumes JSON> [<env JSON>] prints a
# components list of one agent-remediation entry.
workflows_components() {
  local env="${3-}"
  [ -n "$env" ] || env='{}'
  printf '[{"name":"agent-remediation","enabled":true,"httpPort":8092,"grpcPort":50054,"env":%s,"volumeMounts":%s,"volumes":%s}]' \
    "$env" "$1" "$2"
}
mount_problems=(
  "no volumeMount|components[agent-remediation] has no volumeMounts entry for the volume remediation-workflows, so the files reach no container; mount it at ${default_workflows_dir}|$(workflows_components '[]' "$chart_workflows_volume")"
  "a mount at another path|components[agent-remediation] mounts the volume remediation-workflows at /etc/elsewhere, and AGENT_REMEDIATION_WORKFLOWS_DIR is ${default_workflows_dir}, so|$(workflows_components '[{"name":"remediation-workflows","mountPath":"/etc/elsewhere"}]' "$chart_workflows_volume")"
  "an operator-owned ConfigMap|components[agent-remediation].volumes[remediation-workflows] names the ConfigMap my-own-cm, your own, so the files reach no pod; leave its configMap.name empty|$(workflows_components "$default_workflows_mount" '[{"name":"remediation-workflows","configMap":{"name":"my-own-cm"}}]')"
  "the chart's ConfigMap named explicitly|components[agent-remediation].volumes[remediation-workflows] names the chart's ConfigMap continuo-agent-remediation-workflows, and a named ConfigMap gets no digest on the pod template, so a changed file would not roll agent-remediation and it would keep serving the old files; leave its configMap.name empty|$(workflows_components "$default_workflows_mount" '[{"name":"remediation-workflows","configMap":{"name":"continuo-agent-remediation-workflows"}}]')"
  "an emptyDir volume|components[agent-remediation].volumes[remediation-workflows] is not a ConfigMap volume, so the files reach no pod|$(workflows_components "$default_workflows_mount" '[{"name":"remediation-workflows","emptyDir":{}}]')"
  "a variable naming another folder than the mount|components[agent-remediation] mounts the volume remediation-workflows at ${default_workflows_dir}, and AGENT_REMEDIATION_WORKFLOWS_DIR is /etc/other, so|$(workflows_components "$default_workflows_mount" "$chart_workflows_volume" '{"AGENT_REMEDIATION_WORKFLOWS_DIR":"/etc/other"}')"
)
for problem in "${mount_problems[@]}"; do
  label="${problem%%|*}"
  rest="${problem#*|}"
  want="agentRemediation.workflows.files is set, but ${rest%%|*}"
  components="${rest#*|}"
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json "components=${components}" \
       --set-file "agentRemediation.workflows.files.valid\.yaml=${wf}/valid.yaml" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: remediation files rendered with ${label}; they would reach no agent-remediation pod"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL (${label}): want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json "components=${components}" > /dev/null 2> "${tmp}/workflows.err" \
    || { echo "FAIL: with no remediation files, a components list with ${label} failed the render:"; cat "${tmp}/workflows.err"; exit 1; }
done
# The schema refuses a subPath on any volumeMounts entry. The template refuses
# one on the files' mount as well, for a render that skips the schema: a
# subPath mounts one entry of the ConfigMap, never the folder of files.
subpath_components="$(workflows_components '[{"name":"remediation-workflows","mountPath":"'"${default_workflows_dir}"'","subPath":"valid.yaml"}]' "$chart_workflows_volume")"
for check in \
  "|additional properties 'subPath' not allowed" \
  "--skip-schema-validation|agentRemediation.workflows.files is set, but components[agent-remediation] mounts the volume remediation-workflows at ${default_workflows_dir} with subPath valid.yaml, which mounts one entry"; do
  flags=()
  [ -z "${check%%|*}" ] || flags=("${check%%|*}")
  want="${check#*|}"
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" ${flags[@]+"${flags[@]}"} --set-json "components=${subpath_components}" \
       --set-file "agentRemediation.workflows.files.valid\.yaml=${wf}/valid.yaml" > /dev/null 2> "${tmp}/workflows.err"; then
    echo "FAIL: remediation files rendered with a subPath mount ${flags[*]-}; the files would reach no agent-remediation pod"; exit 1
  fi
  grep -qF "$want" "${tmp}/workflows.err" || { echo "FAIL (a subPath mount ${flags[*]-}): want '${want}', got:"; cat "${tmp}/workflows.err"; exit 1; }
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --skip-schema-validation --set-json "components=${subpath_components}" > /dev/null 2> "${tmp}/workflows.err" \
  || { echo "FAIL: with no remediation files, a components list with a subPath mount failed the render:"; cat "${tmp}/workflows.err"; exit 1; }
# mounted_at <rendered manifest> <folder> checks that agent-remediation mounts
# continuo-agent-remediation-workflows at the folder, with no subPath, reads
# AGENT_REMEDIATION_WORKFLOWS_DIR from there (or from the default), and carries
# the ConfigMap's digest on its pod template.
mounted_at() {
  python3 - "$1" "$2" "$default_workflows_dir" <<'PY'
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
template = next(d for d in docs if d["kind"] == "Deployment" and d["metadata"]["name"] == "agent-remediation")["spec"]["template"]
pod = template["spec"]
volumes = {v["name"]: v for v in pod["volumes"]}
container = pod["containers"][0]
env = {e["name"]: e.get("value") for e in container.get("env") or []}
folder = (env.get("AGENT_REMEDIATION_WORKFLOWS_DIR") or sys.argv[3]).rstrip("/")
mounts = [m for m in container["volumeMounts"] if m["name"] == "remediation-workflows"]
sys.exit(0 if volumes["remediation-workflows"].get("configMap") == {"name": "continuo-agent-remediation-workflows"}
         and "checksum/agent-remediation-workflows" in template["metadata"].get("annotations", {})
         and folder == sys.argv[2].rstrip("/")
         and any(m["mountPath"].rstrip("/") == folder and "subPath" not in m for m in mounts) else 1)
PY
}
for good in \
  "${default_workflows_dir}|$(workflows_components "$default_workflows_mount" "$chart_workflows_volume")" \
  "/opt/remediation-files|$(workflows_components '[{"name":"remediation-workflows","mountPath":"/opt/remediation-files"}]' "$chart_workflows_volume" '{"AGENT_REMEDIATION_WORKFLOWS_DIR":"/opt/remediation-files/"}')"; do
  folder="${good%%|*}"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-json "components=${good#*|}" \
    --set-file "agentRemediation.workflows.files.valid\.yaml=${wf}/valid.yaml" > "${tmp}/workflows-mount.yaml" 2> "${tmp}/workflows.err" \
    || { echo "FAIL: remediation files mounted at ${folder} failed the render:"; cat "${tmp}/workflows.err"; exit 1; }
  mounted_at "${tmp}/workflows-mount.yaml" "$folder" \
    || { echo "FAIL: the render does not mount continuo-agent-remediation-workflows at ${folder}, where agent-remediation reads, with its digest on the pod template"; exit 1; }
done
mounted_at "${tmp}/workflows.yaml" "$default_workflows_dir" \
  || { echo "FAIL: the defaults render does not mount the remediation files at ${default_workflows_dir}"; exit 1; }
# An install starts its own files from copies of the built-in ones, so the
# chart accepts every built-in flow, type and fixture file as an install file.
# A ConfigMap key holds no `/`, so each file is passed under its path with
# every `/` written `-` (flows/rejected-release.yaml as
# flows-rejected-release.yaml).
echo "--- every built-in remediation file renders as an install file"
builtin_files=0
while IFS= read -r path; do
  key="${path#agent-remediation/workflows/}"
  key="${key//\//-}"
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-file "agentRemediation.workflows.files.${key//./\\.}=${path}" > /dev/null 2> "${tmp}/builtin-workflow.err" \
    || { echo "FAIL: the built-in remediation file ${path} failed the render as ${key}:"; cat "${tmp}/builtin-workflow.err"; exit 1; }
  builtin_files=$((builtin_files + 1))
done < <(find agent-remediation/workflows -type f \( -name '*.yaml' -o -name '*.yml' \) | sort)
[ "$builtin_files" -gt 0 ] || { echo "FAIL: found no built-in remediation file under agent-remediation/workflows"; exit 1; }
echo "${builtin_files} built-in remediation files render as install files"

# NOTES.txt tells an upgrading operator that schedules are no longer read from
# the chart, and an installing one nothing about it. `helm template` prints no
# notes and `helm install|upgrade --dry-run=client` asks the API server for its
# version, so the check renders NOTES.txt as an ordinary template: in a copy of
# the chart, the file's text becomes a named template (`probe.notes`) that a
# ConfigMap includes, and `helm template --is-upgrade` or without it sets
# .Release.IsUpgrade to true or false for it.
echo "--- NOTES.txt: the schedules, validation re-pin, catalog backup, contracts S3, catalog Postgres, ServiceAccount, python kind, remediation streams and remediation engine notes print on upgrade and not on install"
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
# An upgrading operator is told once that a validation image below v0.15.0 must
# be re-pinned; an installing one is told nothing.
repin_note='A validation.imageTag below v0.15.0 fails the render'
[ "$(grep -cF "$repin_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once that a validation image below v0.15.0 must be re-pinned"; exit 1; }
if grep -qF "$repin_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the validation re-pin note"; exit 1
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
# An upgrading operator on their own bucket (minio.enabled=false) is told once
# what S3 access topology-controller's contract bundle needs and what catalog's
# live folder needs; an installing one, and one on the bundled MinIO, are told
# nothing.
contracts_s3_note='topology-controller now reads the contract bundle key before it writes it'
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --is-upgrade \
  --set minio.enabled=false --set s3.endpointUrl=https://s3.eu-central-1.amazonaws.com \
  --set s3.bucket=continuo --set s3.region=eu-central-1 --set s3.accessKeyId=probe --set s3.secretKey=probe \
  --show-only templates/probe.yaml > "${tmp}/notes-upgrade-byo.txt" \
  || { echo "FAIL: rendering NOTES.txt as an upgrade on an external bucket failed"; exit 1; }
[ "$(grep -cF "$contracts_s3_note" "${tmp}/notes-upgrade-byo.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes on an external bucket do not say exactly once what S3 access the contract bundle needs"; exit 1; }
grep -qF 'tenants/<tenant>/contracts/live/' "${tmp}/notes-upgrade-byo.txt" \
  || { echo "FAIL: the upgrade notes on an external bucket do not name catalog's live folder prefix"; exit 1; }
if grep -qF "$contracts_s3_note" "${tmp}/notes-upgrade.txt"; then
  echo "FAIL: the upgrade notes on the bundled MinIO carry the contract bundle S3 note"; exit 1
fi
if grep -qF "$contracts_s3_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the contract bundle S3 note"; exit 1
fi
# An upgrading operator is told once that catalog's components entry carries no
# Postgres keys, since a POSTGRES_DB left there renders an init container that
# can keep catalog from starting; an installing one renders the chart's own
# entry and is told nothing.
catalog_postgres_note='entry catalog and delete POSTGRES_DB and POSTGRES_PASSWORD from it.'
[ "$(grep -cF "$catalog_postgres_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once to delete catalog's Postgres keys from a components override"; exit 1; }
if grep -qF "$catalog_postgres_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the catalog Postgres upgrade note"; exit 1
fi
# An operator upgrading with an external Postgres is told once which logins to
# create; one with the bundled Postgres, whose logins the chart creates, and
# an installing one are told nothing.
logins_note='BREAKING: every component signs in to Postgres as its own login.'
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --is-upgrade \
  -f scripts/install-test/values-byo-secret.yaml --show-only templates/probe.yaml > "${tmp}/notes-upgrade-byo.txt" \
  || { echo "FAIL: rendering NOTES.txt as an upgrade with an external Postgres failed"; exit 1; }
[ "$(grep -cF "$logins_note" "${tmp}/notes-upgrade-byo.txt")" = 1 ] \
  && grep -qE '^ +continuo_dead_letter_controller \(dead-letter-controller\)$' "${tmp}/notes-upgrade-byo.txt" \
  || { echo "FAIL: the external-Postgres upgrade notes do not list the logins to create once"; exit 1; }
for notes in upgrade install; do
  if grep -qF "$logins_note" "${tmp}/notes-${notes}.txt"; then
    echo "FAIL: the bundled ${notes} notes carry the Postgres logins note"; exit 1
  fi
done
# The same for an external Redis in perComponent mode, which lists the Redis
# logins; a shared-mode Redis has none to create.
redis_logins_note='BREAKING: every component signs in to Redis as its own login.'
[ "$(grep -cF "$redis_logins_note" "${tmp}/notes-upgrade-byo.txt")" = 1 ] \
  && grep -qE '^ +continuo_redis_migrate \(streams-init\)$' "${tmp}/notes-upgrade-byo.txt" \
  || { echo "FAIL: the external-Redis upgrade notes do not list the Redis logins to create once"; exit 1; }
helm template continuo "$notes_chart" --kube-version "$KUBE_VERSION" --is-upgrade \
  -f scripts/install-test/values-byo-secret.yaml --set credentials.redis.mode=shared \
  --show-only templates/probe.yaml > "${tmp}/notes-upgrade-byo-shared.txt" \
  || { echo "FAIL: rendering NOTES.txt as an upgrade with a shared external Redis failed"; exit 1; }
for notes in upgrade install upgrade-byo-shared; do
  if grep -qF "$redis_logins_note" "${tmp}/notes-${notes}.txt"; then
    echo "FAIL: the ${notes} notes carry the Redis logins note"; exit 1
  fi
done
# An upgrading operator is told once that the components left the namespace's
# default ServiceAccount, and what stops applying; an installing one is not.
service_account_note='Every component now runs under its own ServiceAccount, named after it'
[ "$(grep -cF "$service_account_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once that every component has its own ServiceAccount"; exit 1; }
grep -qF 'set imagePullSecrets' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the ServiceAccount upgrade note does not name imagePullSecrets"; exit 1; }
if grep -qF "$service_account_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the ServiceAccount upgrade note"; exit 1
fi

# An upgrading operator is told once to upgrade with empty databases, which
# five databases to drop, and to release each Python project with kind python
# when it releases its projects again; an installing one is told nothing.
empty_db_note='BREAKING: upgrade with empty databases.'
[ "$(grep -cF "$empty_db_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once to upgrade with empty databases"; exit 1; }
for db in continuo_run_controller continuo_execution continuo_release continuo_remediation continuo_agent_remediation; do
  grep -F "$empty_db_note" "${tmp}/notes-upgrade.txt" | grep -qF "$db" \
    || { echo "FAIL: the empty-database upgrade note does not name ${db}"; exit 1; }
done
grep -qF 'Release each Python project with kind "python"' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the empty-database upgrade note does not tell an operator to release a Python project with kind python"; exit 1; }
if grep -qF "$empty_db_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the empty-database upgrade note"; exit 1
fi
# The re-baseline note agrees with deploy/README.md: a Python project is
# released with kind python, built on runtime 0.13.0 or later, and no older
# runtime is named.
grep -qF 'a Python project is released as "python", built on' "${tmp}/notes-upgrade.txt" \
  && grep -qF 'continuo-python-runtime 0.13.0 or later' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the re-baseline upgrade note does not say a Python project is released with kind python on runtime 0.13.0 or later"; exit 1; }
if grep -qE 'continuo-python-runtime 0\.1[0-2]\.' "${tmp}/notes-upgrade.txt"; then
  echo "FAIL: the upgrade notes name a continuo-python-runtime older than 0.13.0"; exit 1
fi

# An upgrading operator is told once about the `continuo:` Redis prefix, that
# ui users sign in once more, and how to turn stream creation off; an
# installing one is told nothing.
redis_note='set redisInit.createStreams=false.'
[ "$(grep -cF "$redis_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once how to set redisInit.createStreams=false"; exit 1; }
grep -qF 'prefix `continuo:`' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the upgrade notes do not name the continuo: Redis key prefix"; exit 1; }
grep -qF 'every ui user signs in once more' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the upgrade notes do not say that ui users sign in once more"; exit 1; }
if grep -qF "$redis_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the Redis prefix upgrade note"; exit 1
fi

# An upgrading operator is told once that every stream starts again at :v1 and
# to upgrade with an empty Redis; an installing one is told nothing.
streams_v1_note='BREAKING: every Redis stream starts again at :v1.'
[ "$(grep -cF "$streams_v1_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once that every stream starts again at :v1"; exit 1; }
grep -qF 'Upgrade with an empty Redis' "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the streams upgrade note does not say to upgrade with an empty Redis"; exit 1; }
if grep -qF "$streams_v1_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the streams upgrade note"; exit 1
fi

# An upgrading operator is told once that agent-remediation has one engine, that
# agentRemediation.engine is gone, and that the flow file's tries decides how
# many times a fix is tried; an installing one is told nothing.
one_engine_note='agent-remediation has one engine. If your values set agentRemediation.engine, remove it'
[ "$(grep -cF "$one_engine_note" "${tmp}/notes-upgrade.txt")" = 1 ] \
  || { echo "FAIL: the upgrade notes do not say exactly once that agent-remediation has one engine"; exit 1; }
grep -qF "The flow file's tries decides how many times a fix is tried; AGENT_REMEDIATION_MAX_ATTEMPTS is no longer read." "${tmp}/notes-upgrade.txt" \
  || { echo "FAIL: the one-engine upgrade note does not say the flow file's tries decides how many times a fix is tried"; exit 1; }
if grep -qF "$one_engine_note" "${tmp}/notes-install.txt"; then
  echo "FAIL: the install notes carry the one-engine upgrade note"; exit 1
fi

# agentRemediation.engine is no longer a key: a values file that sets it fails
# the render, and the error names the key, so an operator who upgrades without
# reading the notes learns what to remove.
echo "--- agentRemediation.engine: a values file that sets it fails the render, naming the key"
for engine in runner legacy; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
       --set "agentRemediation.engine=${engine}" > /dev/null 2> "${tmp}/engine-key.err"; then
    echo "FAIL: agentRemediation.engine=${engine} rendered; the key is gone and must fail the render"; exit 1
  fi
  grep -qF 'agentRemediation' "${tmp}/engine-key.err" && grep -qF 'engine' "${tmp}/engine-key.err" \
    || { echo "FAIL: agentRemediation.engine=${engine} failed without naming the key:"; cat "${tmp}/engine-key.err"; exit 1; }
done
if grep -qF 'AGENT_REMEDIATION_ENGINE' "${tmp}/defaults.yaml"; then
  echo "FAIL: the default render still publishes AGENT_REMEDIATION_ENGINE"; exit 1
fi

# global.logLevel reaches every component as LOG_LEVEL, and a component refuses to
# start on a level it does not know, so the chart (continuo.logLevel in
# _helpers.tpl) refuses such a level at render time instead. The components
# match the level in any case, so a mixed-case level must still render.
echo "--- global.logLevel: a level the components do not accept fails the render"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
     --set global.logLevel=verbose > /dev/null 2> "${tmp}/loglevel.err"; then
  echo "FAIL: global.logLevel=verbose rendered; every component would refuse to start"; exit 1
fi
grep -q 'logLevel' "${tmp}/loglevel.err" || { echo "FAIL: unexpected error:"; cat "${tmp}/loglevel.err"; exit 1; }
echo "--- global.logLevel: a mixed-case level renders as given"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.logLevel=Info > "${tmp}/loglevel-mixed.yaml" \
  || { echo "FAIL: global.logLevel=Info did not render; the components accept it"; exit 1; }
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
# `components` entry: Helm replaces lists wholesale, so an operator's own
# `components` list must not silently disable CI auth. ciAuth itself is optional:
# values without it (as `helm upgrade --reuse-values` can produce) render with
# the GitHub issuer and no bindings (no CI access).
echo "--- ciAuth: the ui gets CI auth from the template"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/defaults.yaml" continuo
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  -f scripts/install-test/values-ui-minimal-components.yaml > "${tmp}/ui-minimal-components.yaml"
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/ui-minimal-components.yaml" continuo
echo "--- ciAuth: a release without ciAuth values renders the defaults"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set ciAuth=null > "${tmp}/ciauth-null.yaml" \
  || { echo "FAIL: rendering with ciAuth=null failed; values without a ciAuth block must render"; exit 1; }
python3 scripts/install-test/assert-ui-ci-auth.py "${tmp}/ciauth-null.yaml" continuo --default-ci-auth

# The "continuo-api-" Secret-name prefix is reserved for the operator-created
# Secrets a python-api node's data contract may name. A chart fullname inside it would
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

# database.pool reaches every Go component through the shared ConfigMap only when
# set (0 keeps each component's default). An idle limit above the open limit, or
# an idle limit with no open limit, fails the render instead of producing a
# release a component would refuse to boot with. Values without a database block
# (as `helm upgrade --reuse-values` can produce) render the defaults.
echo "--- database.pool reaches the ConfigMap only when set; idle above open and idle alone are refused"
if grep -q 'DB_MAX_OPEN_CONNS\|DB_MAX_IDLE_CONNS\|DB_POOL_SIZE\|DB_MAX_OVERFLOW' "${tmp}/defaults.yaml"; then
  echo "FAIL: the default render sets a pool limit; every component must keep its own default"; exit 1
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
  echo "FAIL: a release without database values must leave every component on its default pool"; exit 1
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

# execution.jobTTLMinutes reaches execution-controller through the shared
# ConfigMap in seconds; a value outside 1-10080 fails the render, and values
# without the key (as `helm upgrade --reuse-values` can produce) render the
# 30-minute default.
echo "--- execution.jobTTLMinutes reaches the ConfigMap in seconds; values outside 1-10080 are refused"
grep -q 'JOB_TTL_SECONDS: "1800"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set JOB_TTL_SECONDS to 1800"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.jobTTLMinutes=45 > "${tmp}/ttl-set.yaml"
grep -q 'JOB_TTL_SECONDS: "2700"' "${tmp}/ttl-set.yaml" \
  || { echo "FAIL: execution.jobTTLMinutes does not reach the shared ConfigMap in seconds"; exit 1; }
for bad in 0 10081 -5 1.5; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set execution.jobTTLMinutes=${bad} > /dev/null 2>&1; then
    echo "FAIL: execution.jobTTLMinutes=${bad} must fail the render"; exit 1
  fi
done
grep -q 'JOB_TTL_SECONDS: "1800"' "${tmp}/execution-null.yaml" \
  || { echo "FAIL: a release without execution values must get the default finished-Job TTL"; exit 1; }

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
for component in run-controller execution-controller; do
  shadows="$(deployment_env_entries "$component" "${tmp}/defaults.yaml" 'TASK_MAX_RETRIES|TASK_TIMEOUT_SECONDS')" \
    || { echo "FAIL: the default render has no Deployment named ${component}"; exit 1; }
  if [ -n "$shadows" ]; then
    echo "FAIL: ${component}'s own env must not shadow the ConfigMap's TASK_MAX_RETRIES or TASK_TIMEOUT_SECONDS:"; echo "$shadows"; exit 1
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

# contracts.publish.server.{host,port,database} and
# catalog.contractHistory.retentionDays reach catalog through the shared
# ConfigMap as CONTRACTS_SERVER_HOST, CONTRACTS_SERVER_PORT,
# CONTRACTS_SERVER_DATABASE and CONTRACT_HISTORY_RETENTION_DAYS. With a host, a
# port outside 1 to 65535 or no database to bind (the database falls back to
# validation.warehouseDatabase) fails the render and names the key, and so does
# a retention that is not a whole number from 0 to 36500, in the helpers as well as
# in the schema (the helpers refuse them with the schema check skipped). A key
# the schema does not name is refused, and values without a contracts or
# catalog.contractHistory block (as `helm upgrade --reuse-values` can produce)
# render the defaults.
echo "--- contracts.publish.server and catalog.contractHistory reach the ConfigMap; values catalog cannot honour are refused"
grep -qF 'CONTRACTS_SERVER_HOST: ""' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CONTRACTS_SERVER_HOST to empty"; exit 1; }
grep -qF 'CONTRACTS_SERVER_PORT: "0"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CONTRACTS_SERVER_PORT to 0"; exit 1; }
grep -qF 'CONTRACTS_SERVER_DATABASE: "continuo_dbt"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not fall back to validation.warehouseDatabase for CONTRACTS_SERVER_DATABASE"; exit 1; }
grep -qF 'CONTRACT_HISTORY_RETENTION_DAYS: "0"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set CONTRACT_HISTORY_RETENTION_DAYS to 0"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set contracts.publish.server.host=warehouse.internal --set contracts.publish.server.port=5432 \
  --set catalog.contractHistory.retentionDays=30 > "${tmp}/contracts-set.yaml"
grep -qF 'CONTRACTS_SERVER_HOST: "warehouse.internal"' "${tmp}/contracts-set.yaml" \
  || { echo "FAIL: contracts.publish.server.host does not reach the shared ConfigMap"; exit 1; }
grep -qF 'CONTRACTS_SERVER_PORT: "5432"' "${tmp}/contracts-set.yaml" \
  || { echo "FAIL: contracts.publish.server.port does not reach the shared ConfigMap"; exit 1; }
grep -qF 'CONTRACTS_SERVER_DATABASE: "continuo_dbt"' "${tmp}/contracts-set.yaml" \
  || { echo "FAIL: a host with no database must use validation.warehouseDatabase"; exit 1; }
grep -qF 'CONTRACT_HISTORY_RETENTION_DAYS: "30"' "${tmp}/contracts-set.yaml" \
  || { echo "FAIL: catalog.contractHistory.retentionDays does not reach the shared ConfigMap"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set contracts.publish.server.host=warehouse.internal --set contracts.publish.server.port=5432 \
  --set validation.warehouseDatabase=other_dbt > "${tmp}/contracts-fallback.yaml"
grep -qF 'CONTRACTS_SERVER_DATABASE: "other_dbt"' "${tmp}/contracts-fallback.yaml" \
  || { echo "FAIL: contracts.publish.server.database must fall back to a changed validation.warehouseDatabase"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set contracts.publish.server.host=warehouse.internal --set contracts.publish.server.port=5432 \
  --set contracts.publish.server.database=analytics > "${tmp}/contracts-database.yaml"
grep -qF 'CONTRACTS_SERVER_DATABASE: "analytics"' "${tmp}/contracts-database.yaml" \
  || { echo "FAIL: contracts.publish.server.database must win over validation.warehouseDatabase"; exit 1; }
# A host that is blank after trimming is no host: no servers entry, whatever
# the port. A host and a database with blanks around them render trimmed.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-string 'contracts.publish.server.host=   ' > "${tmp}/contracts-blank-host.yaml" \
  || { echo "FAIL: a blank contracts.publish.server.host must render as no host"; exit 1; }
grep -qF 'CONTRACTS_SERVER_HOST: ""' "${tmp}/contracts-blank-host.yaml" \
  || { echo "FAIL: a blank contracts.publish.server.host must reach the ConfigMap as an empty host"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-string 'contracts.publish.server.host= warehouse.internal ' --set contracts.publish.server.port=5432 \
  --set-string 'contracts.publish.server.database=  analytics ' > "${tmp}/contracts-padded.yaml" \
  || { echo "FAIL: a padded contracts.publish.server host and database must render"; exit 1; }
grep -qF 'CONTRACTS_SERVER_HOST: "warehouse.internal"' "${tmp}/contracts-padded.yaml" \
  && grep -qF 'CONTRACTS_SERVER_DATABASE: "analytics"' "${tmp}/contracts-padded.yaml" \
  || { echo "FAIL: a padded contracts.publish.server host and database must reach the ConfigMap trimmed"; exit 1; }
# A blank database is no database: the value falls back to
# validation.warehouseDatabase.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set contracts.publish.server.host=warehouse.internal --set contracts.publish.server.port=5432 \
  --set-string 'contracts.publish.server.database=   ' > "${tmp}/contracts-blank-database.yaml" \
  || { echo "FAIL: a blank contracts.publish.server.database must fall back to validation.warehouseDatabase"; exit 1; }
grep -qF 'CONTRACTS_SERVER_DATABASE: "continuo_dbt"' "${tmp}/contracts-blank-database.yaml" \
  || { echo "FAIL: a blank contracts.publish.server.database must reach the ConfigMap as validation.warehouseDatabase"; exit 1; }
# expect_contracts_refused KEY ARG...: helm template with the ARGs fails and
# names KEY, with the schema check and again without it. The schema check names
# a key as a JSON pointer (/a/b/c), the helpers as a dotted path (a.b.c).
expect_contracts_refused() {
  local key="$1" out
  local pointer
  pointer="/$(printf '%s' "$key" | tr . /)"
  shift
  if out="$(helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "$@" 2>&1)"; then
    echo "FAIL: $* must fail the render"; exit 1
  fi
  { grep -qF "$key" <<<"$out" || grep -qF "$pointer" <<<"$out"; } \
    || { echo "FAIL: the render refusing $* does not name ${key}:"; echo "$out"; exit 1; }
  if out="$(helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --skip-schema-validation "$@" 2>&1)"; then
    echo "FAIL: $* must fail the render even with the schema check skipped"; exit 1
  fi
  grep -qF "$key" <<<"$out" \
    || { echo "FAIL: the render refusing $* with the schema check skipped does not name ${key}:"; echo "$out"; exit 1; }
}
server_key=contracts.publish.server
expect_contracts_refused "${server_key}.port" --set "${server_key}.host=warehouse.internal"
expect_contracts_refused "${server_key}.port" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=0"
expect_contracts_refused "${server_key}.port" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=70000"
expect_contracts_refused "${server_key}.port" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=-1"
expect_contracts_refused "${server_key}.port" --set "${server_key}.host=warehouse.internal" --set-string "${server_key}.port=5432"
expect_contracts_refused "${server_key}.database" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=5432" \
  --set-string validation.warehouseDatabase=
expect_contracts_refused "${server_key}.database" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=5432" \
  --set-string validation.warehouseDatabase='   '
expect_contracts_refused "${server_key}.database" --set "${server_key}.host=warehouse.internal" --set "${server_key}.port=5432" \
  --set-string "${server_key}.database=  " --set-string validation.warehouseDatabase='  '
expect_contracts_refused "${server_key}.port" --set-string "${server_key}.host= warehouse.internal "
expect_contracts_refused catalog.contractHistory.retentionDays --set catalog.contractHistory.retentionDays=-1
expect_contracts_refused catalog.contractHistory.retentionDays --set-string catalog.contractHistory.retentionDays=x
expect_contracts_refused catalog.contractHistory.retentionDays --set-string catalog.contractHistory.retentionDays=30
expect_contracts_refused catalog.contractHistory.retentionDays --set catalog.contractHistory.retentionDays=1.5
# 36500 days (100 years) is the largest retention. Above it, catalog's
# time.Duration of the days would overflow into a retention the sweeper reads
# as its 7-day default, so a "keep forever" value such as 999999 is refused.
expect_contracts_refused catalog.contractHistory.retentionDays --set catalog.contractHistory.retentionDays=36501
expect_contracts_refused catalog.contractHistory.retentionDays --set catalog.contractHistory.retentionDays=999999
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set catalog.contractHistory.retentionDays=36500 > "${tmp}/contracts-max-retention.yaml" \
  || { echo "FAIL: catalog.contractHistory.retentionDays=36500 must render"; exit 1; }
grep -qF 'CONTRACT_HISTORY_RETENTION_DAYS: "36500"' "${tmp}/contracts-max-retention.yaml" \
  || { echo "FAIL: catalog.contractHistory.retentionDays=36500 does not reach the shared ConfigMap"; exit 1; }
for unknown in contracts.bogus=1 contracts.publish.bogus=1 contracts.publish.server.bogus=1 catalog.contractHistory.bogus=1; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${unknown}" > /dev/null 2>&1; then
    echo "FAIL: ${unknown} must fail the render; the schema names no such key"; exit 1
  fi
done
for absent in contracts=null contracts.publish=null contracts.publish.server=null catalog.contractHistory=null catalog=null; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${absent}" > "${tmp}/contracts-absent.yaml" \
    || { echo "FAIL: rendering with ${absent} failed; values without a contracts block must render"; exit 1; }
  grep -qF 'CONTRACTS_SERVER_HOST: ""' "${tmp}/contracts-absent.yaml" \
    && grep -qF 'CONTRACTS_SERVER_PORT: "0"' "${tmp}/contracts-absent.yaml" \
    && grep -qF 'CONTRACT_HISTORY_RETENTION_DAYS: "0"' "${tmp}/contracts-absent.yaml" \
    || { echo "FAIL: a release with ${absent} must get the default contracts settings"; exit 1; }
done
shadows="$(deployment_env_entries catalog "${tmp}/defaults.yaml" 'CONTRACTS_SERVER_HOST|CONTRACTS_SERVER_PORT|CONTRACTS_SERVER_DATABASE|CONTRACT_HISTORY_RETENTION_DAYS')" \
  || { echo "FAIL: the default render has no Deployment named catalog"; exit 1; }
if [ -n "$shadows" ]; then
  echo "FAIL: catalog's own env must not shadow the ConfigMap's CONTRACTS_* keys:"; echo "$shadows"; exit 1
fi

# contracts.ontology.enabled and contracts.ontology.baseUrl reach every component
# through the shared ConfigMap as ONTOLOGY_ENABLED, exactly "true" or "false",
# and ONTOLOGY_BASE_URL. The schema refuses a non-boolean switch, a key it does
# not name and a base URL that catalog refuses at startup (a value that is not
# empty and not an absolute http or https URL with a host name, no credentials,
# no query, no fragment and no whitespace), and a few more. Values without a
# contracts.ontology block (as `helm upgrade --reuse-values` can produce) render
# the defaults, and no component's own env shadows the two keys.
echo "--- contracts.ontology reaches the ConfigMap; a switch or a base URL a component cannot honour is refused"
grep -qF 'ONTOLOGY_ENABLED: "false"' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set ONTOLOGY_ENABLED to false"; exit 1; }
grep -qF 'ONTOLOGY_BASE_URL: ""' "${tmp}/defaults.yaml" \
  || { echo "FAIL: the default render does not set ONTOLOGY_BASE_URL to empty"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set contracts.ontology.enabled=true --set contracts.ontology.baseUrl=https://ontology.example/things \
  > "${tmp}/ontology-set.yaml" \
  || { echo "FAIL: contracts.ontology.enabled=true with a base URL must render"; exit 1; }
grep -qF 'ONTOLOGY_ENABLED: "true"' "${tmp}/ontology-set.yaml" \
  || { echo "FAIL: contracts.ontology.enabled=true does not reach the shared ConfigMap"; exit 1; }
grep -qF 'ONTOLOGY_BASE_URL: "https://ontology.example/things"' "${tmp}/ontology-set.yaml" \
  || { echo "FAIL: contracts.ontology.baseUrl does not reach the shared ConfigMap"; exit 1; }
# A value catalog accepts renders as written (catalog trims one trailing slash
# when it starts), and an explicit empty value is the default.
for accepted in https://ontology.example/things/ http://ontology.internal:8080/things https://ontology.example; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
    --set-string "contracts.ontology.baseUrl=${accepted}" > "${tmp}/ontology-url.yaml" \
    || { echo "FAIL: contracts.ontology.baseUrl=${accepted} must render"; exit 1; }
  grep -qF "ONTOLOGY_BASE_URL: \"${accepted}\"" "${tmp}/ontology-url.yaml" \
    || { echo "FAIL: contracts.ontology.baseUrl=${accepted} does not reach the shared ConfigMap as written"; exit 1; }
done
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-string contracts.ontology.baseUrl= > "${tmp}/ontology-empty.yaml" \
  || { echo "FAIL: an empty contracts.ontology.baseUrl must render"; exit 1; }
grep -qF 'ONTOLOGY_BASE_URL: ""' "${tmp}/ontology-empty.yaml" \
  || { echo "FAIL: an empty contracts.ontology.baseUrl must reach the ConfigMap as empty"; exit 1; }
# Each of these is refused by the schema, which names the key, and catalog stops
# at boot on each of them as well.
for refused in 'ftp://x' 'https://' 'x/y' 'https://x?q=1' 'https://x#section' 'https://x/a b' ' https://x' 'https://user:secret@x/things'; do
  if out="$(helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-string "contracts.ontology.baseUrl=${refused}" 2>&1)"; then
    echo "FAIL: contracts.ontology.baseUrl=${refused} must fail the render"; exit 1
  fi
  grep -qF 'contracts' <<<"$out" && grep -qF 'baseUrl' <<<"$out" \
    || { echo "FAIL: the render refusing contracts.ontology.baseUrl=${refused} does not name baseUrl:"; echo "$out"; exit 1; }
done
for refused in 'enabled=yes' 'enabled=TRUE' 'enabled=1' 'bogus=1'; do
  if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set-string "contracts.ontology.${refused}" > /dev/null 2>&1; then
    echo "FAIL: contracts.ontology.${refused} must fail the render"; exit 1
  fi
done
for absent in contracts=null contracts.ontology=null; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "${absent}" > "${tmp}/ontology-absent.yaml" \
    || { echo "FAIL: rendering with ${absent} failed; values without an ontology block must render"; exit 1; }
  grep -qF 'ONTOLOGY_ENABLED: "false"' "${tmp}/ontology-absent.yaml" \
    && grep -qF 'ONTOLOGY_BASE_URL: ""' "${tmp}/ontology-absent.yaml" \
    || { echo "FAIL: a release with ${absent} must get the default ontology settings"; exit 1; }
done
for component in topology-controller release-controller catalog; do
  shadows="$(deployment_env_entries "$component" "${tmp}/defaults.yaml" 'ONTOLOGY_ENABLED|ONTOLOGY_BASE_URL')" \
    || { echo "FAIL: the default render has no Deployment named ${component}"; exit 1; }
  if [ -n "$shadows" ]; then
    echo "FAIL: ${component}'s own env must not shadow the ConfigMap's ONTOLOGY_* keys:"; echo "$shadows"; exit 1
  fi
done

# maintenance.enabled reaches every component through the shared ConfigMap as
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

# Every component in continuo.metrics.components serves /metrics on the one port the
# ConfigMap names. The chart must expose and annotate exactly those components,
# admit scrapers only when asked and only to that port, and refuse a port
# another listener uses.
echo "--- metrics port, annotations and scrape rule cover exactly the components that serve /metrics"
python3 scripts/install-test/test-assert-metrics.py -v
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'metrics.scrapeNamespaceSelector={"matchLabels":{"kubernetes.io/metadata.name":"monitoring"}}' \
  > "${tmp}/metrics-scrape.yaml"
python3 scripts/install-test/assert-metrics.py . "${tmp}/defaults.yaml" "${tmp}/metrics-scrape.yaml"
if helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set metrics.port=8082 > /dev/null 2>&1; then
  echo "FAIL: metrics.port equal to a component's httpPort must fail the render"; exit 1
fi

# llm.baseUrl reaches both agents as LLM_BASE_URL from the template, so an
# operator's own `components` list cannot drop it. openai-compatible has no
# public address and needs it; a root ending in /v1 would make every request
# /v1/v1/...; a provider outside the three the agents accept fails the schema;
# and a components entry setting LLM_BASE_URL itself, through env or secretEnv, is
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
# The list of component entries is `components:` and the Redis mode with one
# login per component is perComponent: the schema refuses any other key or mode.
refuse_render "additional properties 'services' not allowed" --set-json 'services=[{"name":"ui","enabled":true}]'
refuse_render "value must be one of 'perComponent', 'shared'" --set credentials.redis.mode=perLogin
# REDIS_ACL_MODE reaches every component from credentials.redis.mode on the
# shared ConfigMap; an entry's own env or secretEnv would win over it, so the
# render refuses one, on any component and in either mode.
refuse_render 'components[name=run-controller].env sets REDIS_ACL_MODE: the chart sets it from credentials.redis.mode' --set-json 'components=[{"name":"run-controller","enabled":true,"httpPort":8082,"env":{"REDIS_ACL_MODE":"perComponent"}}]'
refuse_render 'components[name=ui].secretEnv sets REDIS_ACL_MODE: the chart sets it from credentials.redis.mode' --set-json 'components=[{"name":"ui","enabled":true,"httpPort":3000,"secretEnv":[{"name":"REDIS_ACL_MODE","ref":"redis"}]}]'
refuse_render 'components[name=topology-controller].env sets REDIS_ACL_MODE' --set credentials.redis.mode=shared --set-json 'components=[{"name":"topology-controller","enabled":true,"env":{"REDIS_ACL_MODE":"shared"}}]'
refuse_render 'ends in /v1' --set llm.provider=openai-compatible --set llm.baseUrl=https://api.mistral.ai/v1
refuse_render 'baseUrl' --set llm.baseUrl=api.mistral.ai
refuse_render 'with a host' --set llm.baseUrl=https://
refuse_render 'bare server root' --set 'llm.baseUrl=https://h/api?x=1'
refuse_render 'bare server root' --set 'llm.baseUrl=https://u:p@h'
refuse_render 'provider' --set llm.provider=grok
refuse_render 'LLM_BASE_URL' --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"LLM_BASE_URL":"http://x:1"}}]'
refuse_render 'secretEnv sets LLM_BASE_URL' --set-json 'components=[{"name":"agent-chat","enabled":true,"secretEnv":[{"name":"LLM_BASE_URL","ref":"llm"}]}]'
if ! helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set llm.baseUrl=null > /dev/null; then
  echo "FAIL: a release without llm.baseUrl must render"; exit 1
fi

# The validation image gate refuses a runner that cannot run what continuo
# sends it: below v0.15.0 the runner knows neither dependents_check, which
# every seed load that drops its table runs first, nor build_from_prod, which
# every release check runs for an unchanged upstream.
echo "--- validation: the image gate refuses a runner below v0.15.0"
for old in v0.5.0 v0.10.0 v0.11.0 v0.12.0 v0.13.1 v0.14.0; do
  refuse_render 'predates the dependents_check and build_from_prod ops' --set "validation.imageTag=${old}"
done
# A v0.0.x tag is below every gate's floor too, so it is refused as well.
refuse_render 'predates python-csv validation support' --set "validation.imageTag=v0.0.9"
# The gate reads validation.engine and validation.imageTag, so a components
# entry that sets VALIDATION_IMAGE itself, to anything but the placeholder the
# chart fills from them, would run a runner the gate never saw: refused, in env
# and in secretEnv.
refuse_render 'components[name=execution-controller].env sets VALIDATION_IMAGE="ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.14.0"' \
  --set-json 'components=[{"name":"execution-controller","enabled":true,"httpPort":8084,"env":{"VALIDATION_IMAGE":"ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.14.0"}}]'
refuse_render 'components[name=execution-controller].secretEnv sets VALIDATION_IMAGE' \
  --set-json 'components=[{"name":"execution-controller","enabled":true,"httpPort":8084,"secretEnv":[{"name":"VALIDATION_IMAGE","ref":"redis"}]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'components=[{"name":"execution-controller","enabled":true,"httpPort":8084,"env":{"VALIDATION_IMAGE":"__VALIDATION_IMAGE__"}}]' > /dev/null \
  || { echo "FAIL: VALIDATION_IMAGE=__VALIDATION_IMAGE__ must render"; exit 1; }
for ok in v0.15.0 v0.15.0@sha256:0000000000000000000000000000000000000000000000000000000000000000 v0.16.2; do
  helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set "validation.imageTag=${ok}" > /dev/null \
    || { echo "FAIL: validation.imageTag=${ok} must render"; exit 1; }
done

# agent-chat's continuo CLI reaches release-controller's gRPC server at the
# address global.releaseControllerGrpcAddr holds, and release-controller's port
# admits agent-chat alone.
echo "--- release-controller's gRPC port: agent-chat gets its address, and only agent-chat reaches it"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" > "${tmp}/rc-grpc-default.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.releaseControllerGrpcAddr=rc.corp.example:50056 > "${tmp}/rc-grpc-moved.yaml"
[ "$(grep -A1 'name: CONTINUO_RELEASE_CONTROLLER_ADDR' "${tmp}/rc-grpc-default.yaml" | grep -cF 'value: "release-controller:50056"')" = 1 ] \
  || { echo "FAIL: agent-chat's CONTINUO_RELEASE_CONTROLLER_ADDR must default to release-controller:50056"; exit 1; }
[ "$(grep -A1 'name: CONTINUO_RELEASE_CONTROLLER_ADDR' "${tmp}/rc-grpc-moved.yaml" | grep -cF 'value: "rc.corp.example:50056"')" = 1 ] \
  || { echo "FAIL: CONTINUO_RELEASE_CONTROLLER_ADDR must render from global.releaseControllerGrpcAddr"; exit 1; }
grep -qF 'name: RELEASE_CONTROLLER_GRPC_PORT' "${tmp}/rc-grpc-default.yaml" \
  || { echo "FAIL: release-controller must get RELEASE_CONTROLLER_GRPC_PORT"; exit 1; }
python3 - "${tmp}/rc-grpc-default.yaml" <<'PY' || { echo "FAIL: release-controller's gRPC port must admit agent-chat alone"; exit 1; }
import sys, yaml
docs = [d for d in yaml.safe_load_all(open(sys.argv[1])) if d]
policies = [d for d in docs if d["kind"] == "NetworkPolicy" and any(
    p.get("port") == 50056 for r in d["spec"].get("ingress", []) for p in r.get("ports", []))]
assert len(policies) == 1, [p["metadata"]["name"] for p in policies]
policy = policies[0]
assert policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/name"] == "release-controller"
sources = [f["podSelector"]["matchLabels"]["app.kubernetes.io/name"]
           for r in policy["spec"]["ingress"] for f in r["from"]]
assert sources == ["agent-chat"], sources
services = [d for d in docs if d["kind"] == "Service" and d["metadata"]["name"] == "release-controller"]
assert [p["port"] for p in services[0]["spec"]["ports"] if p["name"] == "grpc"] == [50056]
PY

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
  --set-json 'components=[{"name":"agent-chat","enabled":true},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://10.0.0.8:8080"}}]')
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
ext_logins=(-f scripts/install-test/values-external-logins.yaml)
egress_byo=("${ext_logins[@]}" --set postgresql.enabled=false --set externalDatabase.host=pg.example.internal
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
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" "${ext_logins[@]}" --set postgresql.enabled=false \
  --set externalDatabase.host=pg-rw.databases.svc --set externalDatabase.password=probe > "${tmp}/egress-pg-pod.yaml"
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-pg-pod.yaml" continuo
# An external Postgres or Redis whose host is an IP address outside the private
# ranges is a direct destination on its configured port: a /32 or /128 rule in
# the agents' egress policy, and the address in NO_PROXY. One at a private
# address is admitted by the private-range rule and gets no rule of its own;
# one at a cloud metadata or link-local address fails the render. The checker
# reads the host and port from the shared ConfigMap.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  "${ext_logins[@]}" --set postgresql.enabled=false --set externalDatabase.host=203.0.113.5 --set externalDatabase.password=probe \
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
  "${ext_logins[@]}" --set postgresql.enabled=false --set externalDatabase.host=10.0.0.5 --set externalDatabase.password=probe \
  > "${tmp}/egress-ds-private.yaml"
if grep -q 'cidr: 10.0.0.5/32' "${tmp}/egress-ds-private.yaml"; then
  echo "FAIL: an external Postgres at a private address is admitted by the private-range rule and needs no rule of its own"; exit 1
fi
python3 scripts/install-test/assert-netpol-reachability.py "${tmp}/egress-ds-private.yaml" continuo
refuse_render 'externalDatabase.host="100.100.100.200": its host 100.100.100.200 lies in' \
  "${ext_logins[@]}" --set postgresql.enabled=false --set externalDatabase.host=100.100.100.200 --set externalDatabase.password=probe
refuse_render 'externalRedis.host="169.254.169.254": its host 169.254.169.254 lies in' \
  "${ext_logins[@]}" --set redis.enabled=false --set externalRedis.host=169.254.169.254 --set externalRedis.password=probe
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
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://foo.tools.svc:80","FOO_ALT_URL":"http://foo.tools.svc:08080","TRAILING_DOT_ADDR":"run-controller.:50051"}},{"name":"foo","enabled":true,"httpPort":80},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://foo.tools.svc/api/v3"}}]' \
  > "${tmp}/egress-np-env.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-env.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller.default:50051","CONTINUO_CATALOG_ADDR":"catalog.default.svc.cluster.local:50052"}},{"name":"run-controller","enabled":true,"httpPort":8082},{"name":"catalog","enabled":true,"httpPort":8087},{"name":"agent-remediation","enabled":true}]' \
  > "${tmp}/egress-np-forms.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-forms.yaml"
# An in-cluster address the allowlist admits on the port the agents use is left
# out of NO_PROXY, so the call goes through the proxy: a GitHub Enterprise at a
# namespace-local name (whose codeload.<name> a bare NO_PROXY name would also
# cover). The release's own Services stay in NO_PROXY in every form, also when
# a wildcard allowlist entry over the cluster domain matches them on another
# port; an llm.baseUrl on localhost needs no allowlist entry.
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller:50051"}},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://ghe:8080/api/v3","CONTINUO_CATALOG_ADDR":"catalog:50052"}}]' \
  > "${tmp}/egress-allowlisted-local.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-allowlisted-local.yaml"
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'egress.privateDomains=["*.svc.cluster.local"]' \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"CONTINUO_RUN_CONTROLLER_ADDR":"run-controller.default.svc.cluster.local:50051"}},{"name":"run-controller","enabled":true,"httpPort":8082},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://ghe.tools.svc/api/v3"}}]' \
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
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://foo.tools.svc:80"}},{"name":"agent-remediation","enabled":true}]' \
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
  --set global.releaseControllerGrpcAddr=release-controller.ops.svc:50056 > "${tmp}/egress-np-rc.yaml"
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-rc.yaml"
grep -q 'kubernetes.io/metadata.name: ops$' "${tmp}/egress-np-rc.yaml" \
  || { echo "FAIL: global.releaseControllerGrpcAddr in another namespace must reach the agents' egress policy"; exit 1; }
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set global.deadLetterControllerGrpcAddr=dlc.corp.example:50055 \
  --set global.releaseControllerGrpcAddr=rc.corp.example:50056 \
  --set global.runControllerGrpcAddr=run-controller.corp.example:50051 > "${tmp}/egress-np-fqdn.yaml"
if grep -A1 'name: NO_PROXY' "${tmp}/egress-np-fqdn.yaml" | grep -qE 'dlc\.corp\.example|rc\.corp\.example|run-controller\.corp\.example'; then
  echo "FAIL: a global.*GrpcAddr whose host is no in-cluster name must go through the proxy, not NO_PROXY"; exit 1
fi
python3 scripts/install-test/assert-agent-egress-env.py "${tmp}/egress-np-fqdn.yaml"
for f in egress-np-global egress-np-env egress-np-forms egress-np-dlc egress-np-rc egress-np-fqdn egress-allowlisted-local \
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
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"FOO_URL":"http://10.0.0.9:8080/v1","LOCAL_ADDR":"127.0.0.1:9000","LISTEN_ADDR":"0.0.0.0:8081"}},{"name":"agent-remediation","enabled":true,"env":{"BAR_ADDR":"[fd00::9]:50051"}}]' \
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
refuse_render 'components[name=agent-chat].env.META_URL="http://169.254.169.254/latest": its host 169.254.169.254 lies in' \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"META_URL":"http://169.254.169.254/latest"}}]'
refuse_render 'global.runControllerGrpcAddr="[fd00:ec2::254]:50051": its host fd00:ec2::254 lies in' \
  --set 'global.runControllerGrpcAddr=[fd00:ec2::254]:50051'
refuse_render 'components[name=agent-remediation].env.BAD_ADDR="999.0.0.1:80": its host 999.0.0.1 has the octet "999"' \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"env":{"BAD_ADDR":"999.0.0.1:80"}}]'
refuse_render 'components[name=agent-chat].env.FOO_ADDR="10.0.0.9:70000" has a port outside 1-65535' \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"FOO_ADDR":"10.0.0.9:70000"}}]'

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
  refuse_render "components[name=${a}].env sets ${var}" \
    --set-json "components=[{\"name\":\"${a}\",\"enabled\":true,\"env\":{\"${var}\":\"http://corp-proxy:8080\"}}]"
  refuse_render "components[name=${b}].secretEnv sets ${var}" \
    --set-json "components=[{\"name\":\"${b}\",\"enabled\":true,\"secretEnv\":[{\"name\":\"${var}\",\"ref\":\"llm\"}]}]"
done
refuse_render 'components[name=agent-chat].env sets HTTPS_PROXY' "${egress_all_ip[@]:0:4}" \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"HTTPS_PROXY":"http://corp-proxy:8080"}},{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"http://10.0.0.8:8080"}}]'
refuse_render 'components[name=agent-remediation].secretEnv sets GITHUB_BASE_URL' \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"GITHUB_BASE_URL","ref":"githubToken"}]}]'
# The allowlist admits the provider of llm.provider, so an agent's LLM_PROVIDER
# other than the __LLM_PROVIDER__ placeholder fails the render, as does one in
# secretEnv; the placeholder renders.
refuse_render 'components[name=agent-chat].env sets LLM_PROVIDER="openai": ' \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"LLM_PROVIDER":"openai"}},{"name":"agent-remediation","enabled":true}]'
refuse_render 'components[name=agent-remediation].secretEnv sets LLM_PROVIDER: ' \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"LLM_PROVIDER","ref":"llm"}]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"LLM_PROVIDER":"__LLM_PROVIDER__"}},{"name":"agent-remediation","enabled":true}]' > /dev/null \
  || { echo "FAIL: an agent env LLM_PROVIDER of __LLM_PROVIDER__ must render"; exit 1; }
refuse_render 'components[name=agent-remediation].env sets S3_ENDPOINT_URL: ' \
  --set-json 'components=[{"name":"agent-chat","enabled":true},{"name":"agent-remediation","enabled":true,"env":{"S3_ENDPOINT_URL":"https://my_store.example.com"}}]'
refuse_render 'components[name=agent-chat].secretEnv sets S3_ENDPOINT_URL: ' \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"secretEnv":[{"name":"S3_ENDPOINT_URL","ref":"llm"}]}]'
refuse_render 'components[name=agent-chat].env sets LLM_BASE_URL' --set egress.enabled=false \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"LLM_BASE_URL":"http://x:1"}}]'
refuse_render 'components[name=agent-remediation].secretEnv sets LLM_BASE_URL' --set egress.enabled=false \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"secretEnv":[{"name":"LLM_BASE_URL","ref":"llm"}]}]'
helm template continuo "$CHART" --kube-version "$KUBE_VERSION" --set egress.enabled=false \
  --set-json 'components=[{"name":"agent-chat","enabled":true,"env":{"HTTP_PROXY":"http://corp-proxy:8080","no_proxy":"internal"},"secretEnv":[{"name":"HTTPS_PROXY","ref":"llm"}]},{"name":"agent-remediation","enabled":true,"env":{"NO_PROXY":"internal","https_proxy":"http://corp-proxy:8080","S3_ENDPOINT_URL":"https://store.example.com","LLM_PROVIDER":"openai"},"secretEnv":[{"name":"http_proxy","ref":"llm"},{"name":"GITHUB_BASE_URL","ref":"githubToken"}]}]' > /dev/null \
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
refuse_render 'components[name=agent-remediation].env.GITHUB_BASE_URL="https://gh_e.example/api/v3"' \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://gh_e.example/api/v3"}}]'
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
refuse_render 'components[name=agent-remediation].env.GITHUB_BASE_URL="https://[fe80::1]/api/v3"' \
  --set-json 'components=[{"name":"agent-remediation","enabled":true,"env":{"GITHUB_BASE_URL":"https://[fe80::1]/api/v3"}}]'

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
