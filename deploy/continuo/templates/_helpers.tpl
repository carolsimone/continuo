{{- define "continuo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* The chart's base name. Every name the chart gives an object, including
     every Secret it creates (<fullname>-credentials, <fullname>-postgresql,
     ...), starts with it, and every template names something, so the
     reserved-prefix checks below run here on every render. */}}
{{- define "continuo.fullname" -}}
{{- $name := "" -}}
{{- if .Values.fullnameOverride -}}
{{- $name = .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains .Chart.Name .Release.Name -}}
{{- $name = .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name = printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- if or (eq $name "continuo-api") (hasPrefix "continuo-api-" $name) -}}
{{- fail (printf "the chart fullname %q (the release name, or fullnameOverride) is inside the reserved \"continuo-api-\" prefix: every Secret the chart creates is named <fullname>-<suffix>, and a python-api node contract may name any Secret starting with \"continuo-api-\", so it could receive the chart's datastore and platform credentials. Choose a release name or fullnameOverride that neither equals \"continuo-api\" nor starts with \"continuo-api-\"." $name) -}}
{{- end -}}
{{- include "continuo.assertSecretValuesOutsideApiPrefix" . -}}
{{- $name -}}
{{- end -}}

{{/* A python-api node contract may name one Secret whose name starts with
     "continuo-api-" (secret_ref); execution-controller attaches it to the
     node's pod as environment variables. The prefix is reserved for those
     operator-created API Secrets, so a Secret the chart is told to use for
     its own credentials must not carry it. */}}
{{- define "continuo.assertSecretValuesOutsideApiPrefix" -}}
{{- $v := .Values -}}
{{- $names := dict
      "postgresql.auth.existingSecret" $v.postgresql.auth.existingSecret
      "externalDatabase.existingSecret" $v.externalDatabase.existingSecret
      "redis.auth.existingSecret" $v.redis.auth.existingSecret
      "externalRedis.existingSecret" $v.externalRedis.existingSecret
      "neo4j.auth.existingSecret" $v.neo4j.auth.existingSecret
      "externalNeo4j.existingSecret" $v.externalNeo4j.existingSecret
      "minio.auth.existingSecret" $v.minio.auth.existingSecret
      "s3.existingSecret" $v.s3.existingSecret
      "auth.existingSecret" $v.auth.existingSecret
      "llm.existingSecret" $v.llm.existingSecret
      "github.existingSecret" $v.github.existingSecret
      "validation.warehouseSecret" $v.validation.warehouseSecret
      "ingress.tls.secretName" $v.ingress.tls.secretName -}}
{{- range $key := keys $names | sortAlpha -}}
{{- $name := get $names $key | default "" | toString -}}
{{- if hasPrefix "continuo-api-" $name -}}
{{- fail (printf "%s=%q is inside the reserved \"continuo-api-\" prefix: a python-api node contract may name any Secret starting with \"continuo-api-\", so it could receive these credentials. Rename the Secret outside that prefix." $key $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "continuo.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.labels" -}}
helm.sh/chart: {{ include "continuo.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: {{ include "continuo.name" . }}
{{- end -}}

{{- define "continuo.selectorLabels" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/name: {{ .service }}
{{- end -}}

{{/* Continuo-owned image ref. Call: include "continuo.image" (dict "root" $ "name" "state") */}}
{{- define "continuo.image" -}}
{{- $tag := default .root.Chart.AppVersion .root.Values.global.imageTag -}}
{{- if .root.Values.global.imageRegistry -}}
{{- printf "%s/%s/continuo-%s:%s" .root.Values.global.imageRegistry .root.Values.global.imageRepositoryPrefix .name $tag -}}
{{- else -}}
{{- printf "%s/continuo-%s:%s" .root.Values.global.imageRepositoryPrefix .name $tag -}}
{{- end -}}
{{- end -}}

{{/* The validation image for the SRE-selected engine. Engine name maps to
     the external continuo-python-runtime-<engine> image, released from
     github.com/carolsimone/continuo-python-runtime; validation.imageTag pins its
     version independently of the chart appVersion — that image releases from
     its own repository. The engine is part of the image NAME so the tag
     position stays free for an @sha256:<digest> immutable pin. The $supported
     list is the set of engines with a published library + image.

     validation.imageTag is optional in the schema (not `required`): an
     existing release upgraded without merging the new chart defaults (e.g.
     `helm upgrade --reuse-values`) has no way to supply it, and a hard
     schema requirement would abort that upgrade before this template — or
     the upgrade NOTES — ever renders. A YAML `imageTag: null` override
     collapses to the same "key absent" state, since Helm drops a key whose
     override value is null before merging, so both cases fall back to
     CONTINUO_VALIDATION_DEFAULT_TAG below.

     A present-but-empty string ("") is different: it survives the merge, so
     it is a real, deliberate misconfiguration rather than an absent key, and
     the schema's `minLength: 1` already rejects it wherever the schema sees
     the merged value. This `fail` is the backstop for the one case the
     schema cannot see: `helm upgrade --reuse-values` re-supplying an
     already-stored empty string.

     scripts/check-validation-image-pin.sh renders this template with
     validation.imageTag unset to assert CONTINUO_VALIDATION_DEFAULT_TAG
     below stays in sync with values.yaml's default — keep them equal.

     An explicit validation.imageTag override also gets a capability gate,
     the same fail-closed treatment as validation.engine above. Two known-bad
     ranges are refused, each because the pinned runner cannot run a node kind
     topology-controller already emits: v0.1.x-v0.3.x predate python-csv
     support (added in v0.4.0) — that runner ignores the "kind"/"csv_source"
     contract fields, so a csv node with a mismatched header would promote
     unvalidated; v0.4.x predate check_binds (dbt-test bind checks, added in
     v0.5.0) — that runner does not run the "kind: check_binds" nodes emitted
     for a changed dbt test, so a release with a changed test would promote its
     bind unvalidated. Together the two gates refuse every tag below v0.5.0. A
     tag that isn't even shaped like "vX.Y.Z" (optionally "@sha256:<digest>")
     can't be compared against those ranges at all, so it fails closed the same
     way rather than rendering a chart that may silently skip validation. */}}
{{- define "continuo.validation.image" -}}
{{- $eng := .Values.validation.engine | default "postgres" -}}
{{- $supported := list "postgres" "trino" -}}
{{- if not (has $eng $supported) -}}
{{- fail (printf "validation.engine=%q is not available: only %s have a published continuo-python-runtime image today. Adding an engine means publishing its continuo-python-runtime-<engine> library + image; it ALSO means you (the operator) must supply that engine's own warehouse connection (set validation.createWarehouseSecret=false and provide a Secret with THAT engine's keys — e.g. the trino library reads TRINO_*, not POSTGRES_*) and configure your dbt team images with that engine's dbt profile. See the validation: block in values.yaml." $eng (join ", " $supported)) -}}
{{- end -}}
{{- $tag := "" -}}
{{- if hasKey .Values.validation "imageTag" -}}
{{- $tag = .Values.validation.imageTag -}}
{{- if not $tag -}}
{{- fail "validation.imageTag is set to an empty string; unset the key entirely to use the chart's default continuo-python-runtime-<engine> image tag, or set a real value (\"vX.Y.Z\" or \"vX.Y.Z@sha256:<digest>\")" -}}
{{- end -}}
{{- $bareTag := regexReplaceAll "@.*$" $tag "" -}}
{{- if not (regexMatch "^v[0-9]+\\.[0-9]+\\.[0-9]+$" $bareTag) -}}
{{- fail (printf "validation.imageTag=%q is not shaped like a released continuo-python-runtime tag (\"vX.Y.Z\" or \"vX.Y.Z@sha256:<digest>\"); the chart cannot tell whether an unparseable tag predates python-csv validation support (added in v0.4.0), so it refuses to render rather than risk silently skipping the csv header check. Re-pin to a real vX.Y.Z tag." $tag) -}}
{{- end -}}
{{- if regexMatch "^v0\\.[1-3]\\." $bareTag -}}
{{- fail (printf "validation.imageTag=%q predates python-csv validation support (added in v0.4.0): topology-controller already accepts \"kind: python-csv\" nodes and emits csv_source, but this runner ignores that field and reports success without checking the file's header, so a csv node with a mismatched header would promote unvalidated. Re-pin validation.imageTag to \"v0.4.0\" or later, or drop the override to track the chart's default." $tag) -}}
{{- end -}}
{{- if regexMatch "^v0\\.4\\." $bareTag -}}
{{- fail (printf "validation.imageTag=%q predates check_binds (dbt-test bind checks, added in v0.5.0): topology-controller already emits \"kind: check_binds\" nodes for a changed dbt test, but this runner does not run them and would report success without checking that the test binds, so a release with a changed test would promote its bind unvalidated. Re-pin validation.imageTag to \"v0.5.0\" or later, or drop the override to track the chart's default." $tag) -}}
{{- end -}}
{{- else -}}
{{- $tag = "v0.10.0" -}}{{/* CONTINUO_VALIDATION_DEFAULT_TAG — must equal values.yaml's validation.imageTag default */}}
{{- end -}}
{{- if .Values.global.imageRegistry -}}
{{- printf "%s/%s/continuo-python-runtime-%s:%s" .Values.global.imageRegistry .Values.global.imageRepositoryPrefix $eng $tag -}}
{{- else -}}
{{- printf "%s/continuo-python-runtime-%s:%s" .Values.global.imageRepositoryPrefix $eng $tag -}}
{{- end -}}
{{- end -}}

{{/* Name of the validation warehouse Secret execution-controller attaches via envFrom.
     - createWarehouseSecret=true, bundled generated Postgres: the keys live on the
       bundled Postgres Secret itself (postgresql/secret.yaml), so the password is
       generated exactly once — name is that Secret.
     - createWarehouseSecret=true, external Postgres with an inline password: the
       chart creates a dedicated Secret (validation-warehouse-secret.yaml).
     - createWarehouseSecret=true but the password is in an existingSecret: the chart
       cannot read it — render fails, directing the operator to opt out.
     - createWarehouseSecret=false: the operator supplies validation.warehouseSecret. */}}
{{- define "continuo.validation.warehouseSecretName" -}}
{{- $v := .Values -}}
{{- if $v.validation.createWarehouseSecret -}}
  {{- $eng := $v.validation.engine | default "postgres" -}}
  {{- if ne $eng "postgres" -}}
    {{- fail (printf "validation.createWarehouseSecret=true only works with validation.engine=postgres: the generated Secret carries POSTGRES_* keys, which the %q engine library does not read. Set validation.createWarehouseSecret=false and supply validation.warehouseSecret with that engine's keys (e.g. trino reads TRINO_HOST/TRINO_CATALOG)." $eng) -}}
  {{- end -}}
  {{- if and $v.postgresql.enabled (not $v.postgresql.auth.existingSecret) -}}
    {{- include "continuo.postgresql.fullname" . -}}
  {{- else if and (not $v.postgresql.enabled) $v.externalDatabase.password (not $v.externalDatabase.existingSecret) -}}
    {{- default (printf "%s-warehouse-validation" (include "continuo.fullname" .)) $v.validation.warehouseSecret -}}
  {{- else -}}
    {{- fail "validation.createWarehouseSecret cannot build the warehouse Secret when the Postgres password lives in an existingSecret; set validation.createWarehouseSecret=false and supply validation.warehouseSecret" -}}
  {{- end -}}
{{- else -}}
  {{- required "set validation.warehouseSecret (the name of your warehouse credentials Secret) when validation.createWarehouseSecret=false" $v.validation.warehouseSecret -}}
{{- end -}}
{{- end -}}

{{- define "continuo.configMapName" -}}
{{- printf "%s-config" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.credentialsSecretName" -}}
{{- printf "%s-credentials" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.dbtCommandsConfigMapName" -}}
{{- printf "%s-dbt-commands" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.cancelConfigMapName" -}}
{{- printf "%s-cancel-config" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.serviceReposConfigMapName" -}}
{{- printf "%s-service-repos" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.ciAuthConfigMapName" -}}
{{- printf "%s-ci-auth" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- /* The CI-auth file the ui reads; also hashed into the ui pod template.
       ciAuth and each of its keys are optional in the schema, so a release
       upgraded with `helm upgrade --reuse-values` from a chart without ciAuth
       (or a `ciAuth: null` override) still renders: issuer defaults to GitHub
       Actions, audience to the origin of auth.publicUrl, bindings to none,
       which the ui treats as no CI access. */ -}}
{{- define "continuo.ciAuth.json" -}}
{{- $ci := .Values.ciAuth | default dict -}}
{{- $audience := $ci.audience -}}
{{- if not $audience -}}
{{-   $u := urlParse .Values.auth.publicUrl -}}
{{-   $audience = printf "%s://%s" $u.scheme $u.host -}}
{{- end -}}
{{- $issuer := default "https://token.actions.githubusercontent.com" $ci.issuer | trimSuffix "/" -}}
{{- dict "issuer" $issuer "audience" $audience "bindings" (default (dict) $ci.bindings) | toJson -}}
{{- end -}}

{{/* Job/Secret name carrying a fixed discriminator (what the object IS, e.g.
     "-db-init-migrate") and, optionally, a "-r<N>" revision suffix. Builds
     the FULL suffix (discriminator, or discriminator+revision) FIRST and
     truncates the base fullname to (63 - len(fullSuffix)) before
     concatenating — reserving room for BOTH parts, not just the revision.
     A plain `printf base+discriminator | trunc 63` (or truncating the
     revision alone against an already-discriminated base) can drop the
     discriminator for a long release name, letting two differently-named
     Jobs (e.g. db-init-migrate and minio-bucket-init) collide on the same
     truncated name. trimSuffix "-" on the truncated base avoids a stray "--".
     Call: include "continuo.jobName" (dict "root" $ "discriminator" "-db-init-migrate" "revisioned" true) */}}
{{- define "continuo.jobName" -}}
{{- $suffix := .discriminator -}}
{{- if .revisioned -}}
{{- $suffix = printf "%s-r%d" .discriminator (int .root.Release.Revision) -}}
{{- end -}}
{{- $base := include "continuo.fullname" .root | trunc (int (sub 63 (len $suffix))) | trimSuffix "-" -}}
{{- printf "%s%s" $base $suffix -}}
{{- end -}}

{{/* Migration Job name: revision-suffixed when the bundled Postgres makes it a
     regular resource (hooks would deadlock); stable when it is a hook (BYO). */}}
{{- define "continuo.migrateJobName" -}}
{{- include "continuo.jobName" (dict "root" . "discriminator" "-db-init-migrate" "revisioned" .Values.postgresql.enabled) -}}
{{- end -}}

{{/* Hook Secret carrying the migrate Job's inline postgres password (see
     templates/db-init-migrate-hook-secret.yaml); always stable-named, since
     it is deleted (hook-succeeded) rather than accumulated across revisions
     like the revision-suffixed bundled-Postgres migrate Job is. */}}
{{- define "continuo.migrateHookSecretName" -}}
{{- include "continuo.jobName" (dict "root" . "discriminator" "-db-init-migrate-creds" "revisioned" false) -}}
{{- end -}}

{{/* ------- datastore names & endpoints ------- */}}

{{- define "continuo.postgresql.fullname" -}}
{{- printf "%s-postgresql" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "continuo.redis.fullname" -}}
{{- printf "%s-redis" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "continuo.neo4j.fullname" -}}
{{- printf "%s-neo4j" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "continuo.minio.fullname" -}}
{{- printf "%s-minio" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "continuo.dex.fullname" -}}
{{- printf "%s-dex" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.postgresql.host" -}}
{{- if .Values.postgresql.enabled -}}
{{- include "continuo.postgresql.fullname" . -}}
{{- else -}}
{{- required "externalDatabase.host is required when postgresql.enabled=false" .Values.externalDatabase.host -}}
{{- end -}}
{{- end -}}

{{- define "continuo.postgresql.port" -}}
{{- if .Values.postgresql.enabled -}}5432{{- else -}}{{ .Values.externalDatabase.port }}{{- end -}}
{{- end -}}

{{- define "continuo.postgresql.username" -}}
{{- if .Values.postgresql.enabled -}}{{ .Values.postgresql.auth.username }}{{- else -}}{{ .Values.externalDatabase.username }}{{- end -}}
{{- end -}}

{{- define "continuo.db.sslMode" -}}
{{- if .Values.postgresql.enabled -}}disable{{- else -}}{{ .Values.externalDatabase.sslMode }}{{- end -}}
{{- end -}}

{{/*
Install-wide Postgres pool limits for the Go services. 0, and an absent
database block (an upgrade with --reuse-values), leave every service on its own
default, so the ConfigMap omits the key. Rendering fails when the idle limit
exceeds the open limit, and when an idle limit is set without an open limit: the
chart cannot know each service's own default open limit, and an idle limit
above it would stop that service from starting.
*/}}
{{- define "continuo.db.pool" -}}
{{- $db := .Values.database | default dict -}}
{{- $pool := $db.pool | default dict -}}
{{- $open := $pool.maxOpenConns | default 0 | int -}}
{{- $idle := $pool.maxIdleConns | default 0 | int -}}
{{- if and (gt $open 0) (gt $idle $open) -}}
{{- fail (printf "database.pool.maxIdleConns (%d) must not exceed database.pool.maxOpenConns (%d)" $idle $open) -}}
{{- end -}}
{{- if and (gt $idle 0) (eq $open 0) -}}
{{- fail (printf "database.pool.maxIdleConns (%d) is set without database.pool.maxOpenConns: set database.pool.maxOpenConns together with maxIdleConns" $idle) -}}
{{- end -}}
{{- dict "open" $open "idle" $idle | toJson -}}
{{- end -}}

{{/* LOG_LEVEL for every service, from global.logLevel, rendered as given. The
     services match the level without regard to case and treat an unset or
     empty one as info; any other name stops every service at startup, so the
     render fails on it first. $accepted is pkg/config.AcceptedLogLevels, and
     pkg/streams' TestChartLogLevelsMatchTheServices pins the two lists to each
     other. The shared ConfigMap includes this helper, so every install runs the
     check. */}}
{{- define "continuo.logLevel" -}}
{{- $level := .Values.global.logLevel | default "info" -}}
{{- $accepted := list "debug" "info" "warn" "warning" "error" -}}
{{- if not (has (lower $level) $accepted) -}}
{{- fail (printf "global.logLevel=%q is not a log level the services accept: use one of %s, in any case (INFO, Info and info all work). A service given another level refuses to start." $level (join ", " $accepted)) -}}
{{- end -}}
{{- $level -}}
{{- end -}}

{{/* llm.baseUrl without a trailing slash, or "" for the provider's public
address. The agents add /v1/... to it, so a root whose path ends in /v1 would
request /v1/v1/..., and openai-compatible has no public address. The render
fails on either, on anything but an http(s) URL with a host, which the agents
refuse to start on, and on a user:password@, ?query or #fragment, which would
send requests to the wrong place or expose a credential in the Deployment.
Tolerates a release whose values hold no llm block or no llm.baseUrl. */}}
{{- define "continuo.llm.baseUrl" -}}
{{- $llm := .Values.llm | default dict -}}
{{- $provider := $llm.provider | default "anthropic" -}}
{{- $raw := $llm.baseUrl | default "" | toString | trim -}}
{{- if and (not $raw) (eq $provider "openai-compatible") -}}
{{- fail "llm.provider=openai-compatible needs llm.baseUrl: the server root of the OpenAI-compatible server, without /v1, e.g. https://api.mistral.ai, https://openrouter.ai/api or http://vllm.ai.svc:8000" -}}
{{- end -}}
{{- if $raw -}}
{{- $root := regexReplaceAll "/+$" $raw "" -}}
{{- $u := urlParse $root -}}
{{- if not (and (has $u.scheme (list "http" "https")) $u.host) -}}
{{- fail (printf "llm.baseUrl=%q must be an http or https URL with a host, e.g. http://vllm.ai.svc:8000" $raw) -}}
{{- end -}}
{{- if or $u.userinfo $u.query $u.fragment (regexMatch "[?#]" $raw) -}}
{{- fail (printf "llm.baseUrl=%q must be a bare server root: no user:password@, ?query or #fragment (put the key in llm.apiKey)" $raw) -}}
{{- end -}}
{{- if regexMatch "/v1$" $u.path -}}
{{- fail (printf "llm.baseUrl=%q ends in /v1: give the server root without it (e.g. %s://%s), because the agents add /v1/... themselves" $raw $u.scheme $u.host) -}}
{{- end -}}
{{- $root -}}
{{- end -}}
{{- end -}}

{{/* Environment the chart sets on agent-chat and agent-remediation from values.
It is injected by deployment.yaml rather than carried by the `services` entries,
so an overridden or reused `services` list cannot drop it: LLM_BASE_URL from
llm.baseUrl and, while egress-proxy renders ("continuo.egress.proxyEnabled"),
HTTP_PROXY and HTTPS_PROXY naming it and the NO_PROXY of
"continuo.egress.noProxy", which send the agents' outbound HTTP and gRPC calls
through it. With egress.enabled=false, or with an allowlist that holds nothing
(no proxy renders), the agents get no proxy variable. Emits YAML list items with
no indentation; empty when nothing is set. */}}
{{- define "continuo.agent.env" -}}
{{- $baseUrl := include "continuo.llm.baseUrl" . -}}
{{- if $baseUrl }}
- name: LLM_BASE_URL
  value: {{ $baseUrl | quote }}
{{- end }}
{{- if eq (include "continuo.egress.proxyEnabled" .) "true" }}
- name: HTTP_PROXY
  value: {{ include "continuo.egress.proxyUrl" . | quote }}
- name: HTTPS_PROXY
  value: {{ include "continuo.egress.proxyUrl" . | quote }}
- name: NO_PROXY
  value: {{ include "continuo.egress.noProxy" . | quote }}
{{- end }}
{{- end -}}

{{/* Fails the render when an agent's `services[].env` or `services[].secretEnv`
names a variable the chart manages for it, which would render a second entry for
the same variable or route the agent around the chart's settings: LLM_BASE_URL
always; while egress.enabled is true the proxy variables HTTP_PROXY,
HTTPS_PROXY and NO_PROXY in upper or lower case, and S3_ENDPOINT_URL, which
would replace the shared ConfigMap's value that the egress-proxy allowlist and
NO_PROXY are built from. While egress.enabled is true a GITHUB_BASE_URL in
secretEnv fails it too: the allowlist is built from the `env` value
("continuo.egress.githubBaseUrl") and cannot read a Secret. So does an
LLM_PROVIDER in secretEnv, or in env with any value but the placeholder
__LLM_PROVIDER__ (which deployment.yaml replaces with llm.provider): the
allowlist admits the provider of llm.provider, so another one would be refused.
Call: include "continuo.agent.assertEnv" (dict "root" $ "svc" $svc) */}}
{{- define "continuo.agent.assertEnv" -}}
{{- $svc := .svc -}}
{{- $reserved := dict "LLM_BASE_URL" "the chart sets it from llm.baseUrl; set llm.baseUrl instead" -}}
{{- $secretReserved := dict -}}
{{- if eq (include "continuo.egress.enabled" .root) "true" -}}
{{- range (list "HTTP_PROXY" "HTTPS_PROXY" "NO_PROXY" "http_proxy" "https_proxy" "no_proxy") -}}
{{- $_ := set $reserved . "while egress.enabled is true the chart manages the agents' proxy variables and their outbound traffic goes through egress-proxy; remove it, or set egress.enabled=false to manage their proxy yourself" -}}
{{- end -}}
{{- $_ := set $reserved "S3_ENDPOINT_URL" "while egress.enabled is true the chart sets it for every service from s3.endpointUrl (or the bundled MinIO), and egress-proxy's allowlist is built from that value; set s3.endpointUrl instead" -}}
{{- $_ := set $secretReserved "GITHUB_BASE_URL" "the egress-proxy allowlist is built from services[name=agent-remediation].env.GITHUB_BASE_URL and cannot read a Secret; set it in env instead, or set egress.enabled=false" -}}
{{- $_ := set $secretReserved "LLM_PROVIDER" "while egress.enabled is true the egress-proxy allowlist admits the provider of llm.provider; set llm.provider instead" -}}
{{- $env := $svc.env | default dict -}}
{{- $llmProvider := get $env "LLM_PROVIDER" | toString -}}
{{- if and (hasKey $env "LLM_PROVIDER") (ne $llmProvider "__LLM_PROVIDER__") -}}
{{- fail (printf "services[name=%s].env sets LLM_PROVIDER=%q: while egress.enabled is true the egress-proxy allowlist admits the provider of llm.provider; set llm.provider instead, and leave LLM_PROVIDER at __LLM_PROVIDER__" $svc.name $llmProvider) -}}
{{- end -}}
{{- end -}}
{{- range $key, $_ := ($svc.env | default dict) -}}
{{- if hasKey $reserved $key -}}
{{- fail (printf "services[name=%s].env sets %s: %s" $svc.name $key (get $reserved $key)) -}}
{{- end -}}
{{- end -}}
{{- range $secret := ($svc.secretEnv | default (list)) -}}
{{- $name := $secret.name | toString -}}
{{- $why := get $reserved $name | default (get $secretReserved $name) -}}
{{- if $why -}}
{{- fail (printf "services[name=%s].secretEnv sets %s: %s" $svc.name $name $why) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* "true" unless egress.enabled is false. A values file without an egress
block (for example after helm upgrade --reuse-values) runs the proxy with its
defaults. */}}
{{- define "continuo.egress.enabled" -}}
{{- $e := .Values.egress | default dict -}}
{{- if and (hasKey $e "enabled") (kindIs "bool" $e.enabled) (not $e.enabled) -}}false{{- else -}}true{{- end -}}
{{- end -}}

{{/* The name of the ConfigMap that holds the egress-proxy allowlist. */}}
{{- define "continuo.egressProxy.configMapName" -}}
{{- printf "%s-egress-proxy" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* "true" when the argument, a host name without a port, is an IP address
literal: dotted IPv4, or IPv6 with or without brackets. Otherwise empty.
Call: include "continuo.egress.isIP" $host */}}
{{- define "continuo.egress.isIP" -}}
{{- $h := . | toString -}}
{{- if or (contains ":" $h) (hasPrefix "[" $h) (regexMatch "^[0-9]+(\\.[0-9]+){3}$" $h) -}}true{{- end -}}
{{- end -}}

{{/* "true" when the last label of the argument, a host name without a port, is
all digits or "0x" followed by hex digits. Resolvers read such a name as part of
a numeric IPv4 address (127.1, 2130706433, 0x7f.1), so it is no host name.
Otherwise empty. Call: include "continuo.egress.numericHost" $host */}}
{{- define "continuo.egress.numericHost" -}}
{{- $last := . | toString | lower | trimSuffix "." | splitList "." | last -}}
{{- if or (regexMatch "^[0-9]+$" $last) (regexMatch "^0x[0-9a-f]*$" $last) -}}true{{- end -}}
{{- end -}}

{{/* The GITHUB_BASE_URL of the agent-remediation entry in `services`, or the
public GitHub API when there is none. It reads the entry's `env` only: while
egress is on, a GITHUB_BASE_URL in `secretEnv` fails the render
("continuo.agent.assertEnv"). */}}
{{- define "continuo.egress.githubBaseUrl" -}}
{{- $url := "https://api.github.com" -}}
{{- range .Values.services -}}
{{- if and (eq .name "agent-remediation") .env .env.GITHUB_BASE_URL -}}
{{- $url = .env.GITHUB_BASE_URL | toString -}}
{{- end -}}
{{- end -}}
{{- $url -}}
{{- end -}}

{{/* Why a host name is no entry the proxy takes, or empty when it is one. The
argument is a dict with "host" (lower- or mixed-case, one trailing dot already
removed, no port) and "wildcard" (whether a "*" first label is allowed). The
reason reads as the rest of a sentence that starts with the entry. It never
fails the render: callers fail with the reason (egress.allowedDomains,
egress.privateDomains, and the URLs of "continuo.egress.assertUrlHost") or leave
out a codeload host derived from a GitHub Enterprise name. The rules are the proxy's: at
most 253 characters in labels of 1-63 letters, digits and inner hyphens; a "*"
first label needs at least two labels after it; no IP address and no name whose
last label is all digits or "0x" and hex digits (127.1, 2130706433).
Call: include "continuo.egress.hostProblem" (dict "host" $h "wildcard" true) */}}
{{- define "continuo.egress.hostProblem" -}}
{{- $host := .host | toString -}}
{{- $reason := "" -}}
{{- if not $host -}}
{{- $reason = "has no host" -}}
{{- else if gt (len $host) 253 -}}
{{- $reason = "is a host name longer than 253 characters" -}}
{{- else if or (include "continuo.egress.isIP" $host) (include "continuo.egress.numericHost" $host) -}}
{{- $reason = "is an IP address or reads as one (its last label is all digits or 0x and hex digits); entries name hosts" -}}
{{- else if eq $host "*" -}}
{{- $reason = "is a bare wildcard, which would admit every host" -}}
{{- else if and (contains "*" $host) (not .wildcard) -}}
{{- $reason = "holds a wildcard, which a host name here cannot" -}}
{{- else if and (contains "*" $host) (not (hasPrefix "*." $host)) -}}
{{- $reason = "uses \"*\" other than as the whole first label" -}}
{{- else -}}
{{- $names := trimPrefix "*." $host -}}
{{- if and (hasPrefix "*." $host) (lt (len (splitList "." $names)) 2) -}}
{{- $reason = "is a wildcard with fewer than two labels after it, which would admit a whole top-level domain" -}}
{{- else -}}
{{- range $label := splitList "." $names -}}
{{- if and (not $reason) (not (regexMatch "^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?$" $label)) -}}
{{- $reason = printf "holds the label %q, which is not 1-63 letters, digits and inner hyphens" $label -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $reason -}}
{{- end -}}

{{/* One allowlist entry, as JSON, from a URL: the host lower-cased and without
its port or one trailing dot (an IPv6 address keeps its brackets); the URL's
port, read in base 10 as written (443 for https and 80 for http when it names
none or an empty one); allowPrivate; "portOk", whether the port is in 1-65535;
and "usable", false for an entry the proxy would refuse: a host that is an IP
address or no host name by the rules of "continuo.egress.hostProblem" (an
underscore, a missing host as in a URL without a scheme, a wildcard), or a port
outside 1-65535. The URL itself is not checked here: "continuo.egress.assertUrlHost"
and "continuo.egress.directDestinations" fail the render on what is not usable.
Call: include "continuo.egress.urlEntry" (dict "url" $u "allowPrivate" true) | fromJson */}}
{{- define "continuo.egress.urlEntry" -}}
{{- $u := urlParse (.url | toString) -}}
{{- $host := $u.host -}}
{{- $port := ternary 443 80 (eq $u.scheme "https") -}}
{{- if regexMatch ":[0-9]+$" $host -}}
{{- $port = regexFind "[0-9]+$" $host | atoi -}}
{{- $host = regexReplaceAll ":[0-9]+$" $host "" -}}
{{- else -}}
{{- $host = trimSuffix ":" $host -}}
{{- end -}}
{{- $host = lower $host | trimSuffix "." -}}
{{- $portOk := and (ge $port 1) (le $port 65535) -}}
{{- $usable := and $portOk (not (include "continuo.egress.hostProblem" (dict "host" $host "wildcard" false))) -}}
{{- dict "host" $host "port" $port "allowPrivate" .allowPrivate "portOk" $portOk "usable" $usable | toJson -}}
{{- end -}}

{{/* The URLs the agents call whose address comes from values, as a JSON list of
{key, url}: llm.baseUrl when set, the GITHUB_BASE_URL of agent-remediation's
`services` entry (the public GitHub API when it has none), and s3.endpointUrl
when minio.enabled=false. "key" names the setting in error messages. The
allowlist admits the host of each one that is a host name; each one whose host
is an IP address is a direct destination. */}}
{{- define "continuo.egress.urlSources" -}}
{{- $out := list -}}
{{- with include "continuo.llm.baseUrl" . }}{{ $out = append $out (dict "key" "llm.baseUrl" "url" .) }}{{ end -}}
{{- $out = append $out (dict "key" "services[name=agent-remediation].env.GITHUB_BASE_URL" "url" (include "continuo.egress.githubBaseUrl" .)) -}}
{{- if not .Values.minio.enabled }}{{ $out = append $out (dict "key" "s3.endpointUrl" "url" (include "continuo.s3.endpointUrl" .)) }}{{ end -}}
{{- $out | toJson -}}
{{- end -}}

{{/* Fails the render when a URL the agents call, one of
"continuo.egress.urlSources", names a host egress-proxy cannot admit and that is
no IP address (an underscore, a wildcard, a name that reads as a number, no
host at all), or a port outside 1-65535: the agent would be sent to a proxy that
refuses every call. The message names the setting, the value and the fix. A host
that is an IP address passes here: the agents reach it directly, and
"continuo.egress.directDestinations" checks it and its port.
Call: include "continuo.egress.assertUrlHost" (dict "key" $key "url" $url "entry" $urlEntry) */}}
{{- define "continuo.egress.assertUrlHost" -}}
{{- $e := .entry -}}
{{- $shown := printf "%s=%q" .key (.url | toString) -}}
{{- if and (not $e.usable) (not (include "continuo.egress.isIP" $e.host)) -}}
{{- $problem := include "continuo.egress.hostProblem" (dict "host" $e.host "wildcard" false) -}}
{{- if not $e.host -}}
{{- fail (printf "%s has no host. Give an http or https URL with a host name or an IP address, e.g. https://host.example.com" $shown) -}}
{{- else if include "continuo.egress.numericHost" $e.host -}}
{{- fail (printf "%s names the host %q, which reads as an IP address written in another form than four decimal octets (its last label is all digits or 0x and hex digits), which egress-proxy cannot admit. Write the IPv4 address as four decimal octets, e.g. 10.0.0.7, which the agents then reach directly, or use a host name" $shown $e.host) -}}
{{- else if $problem -}}
{{- fail (printf "%s names a host egress-proxy cannot admit: %q %s. Use a DNS host name of letters, digits and inner hyphens (no underscore), or an IP address, which the agents then reach directly" $shown $e.host $problem) -}}
{{- else -}}
{{- fail (printf "%s has a port outside 1-65535" $shown) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* One allowlist entry, as JSON, from an egress.allowedDomains or
egress.privateDomains entry: "host" or "host:port" (port 443 when omitted). The
entry is checked as written, and the render fails naming the key and the entry
unless it is a string holding a DNS name (by the rules of
"continuo.egress.hostProblem", with a trailing dot allowed) and an optional
port, with no scheme, path, query, fragment, user:password@ or whitespace. The
port is a whole number from 1 to 65535 without a leading zero: ":0443" is
refused rather than read in a base the writer did not mean. Case is folded after the
check.
Call: include "continuo.egress.domainEntry" (dict "value" . "key" "allowedDomains" "allowPrivate" false) */}}
{{- define "continuo.egress.domainEntry" -}}
{{- $key := printf "egress.%s" .key -}}
{{- $shown := toJson .value -}}
{{- $hint := "Write a host name, optionally with a port: api.example.com, api.example.com:8443 or *.example.com." -}}
{{- if not (kindIs "string" .value) -}}
{{- fail (printf "%s entry %s is not a string. %s" $key $shown $hint) -}}
{{- end -}}
{{- $raw := .value -}}
{{- if not $raw -}}
{{- fail (printf "%s entry %s is empty. %s" $key $shown $hint) -}}
{{- end -}}
{{- if regexMatch "\\s" $raw -}}
{{- fail (printf "%s entry %s holds whitespace. %s" $key $shown $hint) -}}
{{- end -}}
{{- if regexMatch "[/?#@\\\\]" $raw -}}
{{- fail (printf "%s entry %s holds a scheme, path, query, fragment or user:password@, which an entry names none of. %s" $key $shown $hint) -}}
{{- end -}}
{{- if or (hasPrefix "[" $raw) (gt (len (splitList ":" $raw)) 2) -}}
{{- fail (printf "%s entry %s has more than one colon or is an IPv6 address; entries name hosts, not IP addresses. An IP address, which egress-proxy never admits, belongs in egress.directDestinations ({cidr, ports}). %s" $key $shown $hint) -}}
{{- end -}}
{{- $parts := splitList ":" $raw -}}
{{- $host := first $parts -}}
{{- $port := 443 -}}
{{- if eq (len $parts) 2 -}}
{{- $p := last $parts -}}
{{- if not (regexMatch "^[0-9]{1,5}$" $p) -}}
{{- fail (printf "%s entry %s has a port that is not a whole number from 1 to 65535. %s" $key $shown $hint) -}}
{{- end -}}
{{- if and (gt (len $p) 1) (hasPrefix "0" $p) -}}
{{- fail (printf "%s entry %s has a port with a leading zero, which other tools read as octal; write the port without one. %s" $key $shown $hint) -}}
{{- end -}}
{{- $port = atoi $p -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail (printf "%s entry %s has a port outside 1-65535. %s" $key $shown $hint) -}}
{{- end -}}
{{- end -}}
{{- $host = $host | trimSuffix "." -}}
{{- $problem := include "continuo.egress.hostProblem" (dict "host" $host "wildcard" true) -}}
{{- if or (include "continuo.egress.isIP" $host) (include "continuo.egress.numericHost" $host) -}}
{{- fail (printf "%s entry %s %s. An IP address, which egress-proxy never admits, belongs in egress.directDestinations ({cidr, ports}). %s" $key $shown $problem $hint) -}}
{{- else if $problem -}}
{{- fail (printf "%s entry %s %s. %s" $key $shown $problem $hint) -}}
{{- end -}}
{{- dict "host" (lower $host) "port" $port "allowPrivate" .allowPrivate | toJson -}}
{{- end -}}

{{/* The egress-proxy allowlist, a JSON list of {host, port, allowPrivate}
ordered by host and then port (the order of the key "<host>:<port, zero-padded
to five digits>"), so that equal settings render equal lists. It holds what the
agents call: the LLM (llm.baseUrl, else the provider's public API), the
GITHUB_BASE_URL of agent-remediation's `services` entry with the codeload host
archive downloads redirect to, s3.endpointUrl when the bundled MinIO is off, and
the entries of egress.allowedDomains and egress.privateDomains. The hosts an
operator configured (llm.baseUrl, a GitHub Enterprise host, s3.endpointUrl) and
egress.privateDomains may resolve to private addresses. A URL whose host is an
IP address is left out: the agents reach it directly. So is one on localhost,
which is loopback: the agents never send it to a proxy, and egress-proxy never
connects to loopback. A URL whose host the proxy
would refuse as a name, or whose port is outside 1-65535, fails the render
("continuo.egress.assertUrlHost"). A codeload host that is no valid name (the
GitHub Enterprise name is too long to prefix) is left out. Two entries for one
host and port merge, allowing private addresses if either does. The list is
empty when nothing is left. */}}
{{- define "continuo.egress.allowlist" -}}
{{- $e := .Values.egress | default dict -}}
{{- $entries := list -}}
{{- $llm := .Values.llm | default dict -}}
{{- if not (include "continuo.llm.baseUrl" .) -}}
{{- if eq ($llm.provider | default "anthropic") "openai" -}}
{{- $entries = append $entries (dict "host" "api.openai.com" "port" 443 "allowPrivate" false) -}}
{{- else -}}
{{- $entries = append $entries (dict "host" "api.anthropic.com" "port" 443 "allowPrivate" false) -}}
{{- end -}}
{{- end -}}
{{- range $source := include "continuo.egress.urlSources" . | fromJsonArray -}}
{{- $entry := include "continuo.egress.urlEntry" (dict "url" $source.url "allowPrivate" true) | fromJson -}}
{{- $github := hasSuffix ".GITHUB_BASE_URL" $source.key -}}
{{- if and $github (eq $entry.host "api.github.com") (eq (int $entry.port) 443) -}}
{{- $entries = append $entries (dict "host" "api.github.com" "port" 443 "allowPrivate" false) -}}
{{- $entries = append $entries (dict "host" "codeload.github.com" "port" 443 "allowPrivate" false) -}}
{{- else if ne $entry.host "localhost" -}}
{{- include "continuo.egress.assertUrlHost" (dict "key" $source.key "url" $source.url "entry" $entry) -}}
{{- if $entry.usable -}}
{{- $entries = append $entries (dict "host" $entry.host "port" (int $entry.port) "allowPrivate" true) -}}
{{- $codeloadHost := printf "codeload.%s" $entry.host -}}
{{- if and $github (not (include "continuo.egress.hostProblem" (dict "host" $codeloadHost "wildcard" false))) }}{{ $entries = append $entries (dict "host" $codeloadHost "port" (int $entry.port) "allowPrivate" true) }}{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- range ($e.allowedDomains | default list) -}}
{{- $entries = append $entries (include "continuo.egress.domainEntry" (dict "value" . "key" "allowedDomains" "allowPrivate" false) | fromJson) -}}
{{- end -}}
{{- range ($e.privateDomains | default list) -}}
{{- $entries = append $entries (include "continuo.egress.domainEntry" (dict "value" . "key" "privateDomains" "allowPrivate" true) | fromJson) -}}
{{- end -}}
{{- $merged := dict -}}
{{- range $entries -}}
{{- $port := int .port -}}
{{- $key := printf "%s:%05d" .host $port -}}
{{- $allow := .allowPrivate -}}
{{- with get $merged $key }}{{ $allow = or $allow .allowPrivate }}{{ end -}}
{{- $_ := set $merged $key (dict "host" .host "port" $port "allowPrivate" $allow) -}}
{{- end -}}
{{- $out := list -}}
{{- range $key := keys $merged | sortAlpha -}}
{{- $out = append $out (get $merged $key) -}}
{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{/* "true" when the proxy renders: egress.enabled is not false and the
allowlist holds at least one entry, because the proxy refuses to start on an
empty one. The setting alone is "continuo.egress.enabled". */}}
{{- define "continuo.egress.proxyEnabled" -}}
{{- if and (eq (include "continuo.egress.enabled" .) "true") (include "continuo.egress.allowlist" . | fromJsonArray) -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/* The URL the agents name in HTTP_PROXY and HTTPS_PROXY: the egress-proxy
Service and its proxy port. */}}
{{- define "continuo.egress.proxyUrl" -}}http://egress-proxy:3128{{- end -}}

{{/* A CIDR read as written, as JSON: "family" (4 or 6), "prefix" (its prefix
length), "bits" (the address as 32 or 128 characters "0" and "1") and
"problem", empty when the argument is a CIDR this chart takes and otherwise why
not, as the rest of a sentence that starts with the CIDR. It takes an IPv4
address of four decimal octets 0-255 without leading zeros, or an IPv6 address
of eight groups of one to four hex digits in which one "::" may stand for groups
of zeros (no brackets, embedded IPv4 address or zone), then "/" and a prefix
length in decimal without a leading zero, at most 32 or 128, with no bit of the
address set past it. Call: include "continuo.egress.parseCidr" $cidr | fromJson */}}
{{- define "continuo.egress.parseCidr" -}}
{{- $raw := . | toString -}}
{{- $problem := "" -}}
{{- $family := 0 -}}
{{- $bits := "" -}}
{{- $prefix := 0 -}}
{{- $parts := splitList "/" $raw -}}
{{- $addr := first $parts -}}
{{- if not $raw -}}
{{- $problem = "is empty" -}}
{{- else if ne (len $parts) 2 -}}
{{- $problem = "is not one address and one prefix length, as in 203.0.113.10/32" -}}
{{- else if not (regexMatch "^(0|[1-9][0-9]{0,2})$" (last $parts)) -}}
{{- $problem = "has a prefix length that is not a whole number written in decimal without a leading zero" -}}
{{- else if regexMatch "^[0-9.]+$" $addr -}}
{{- $family = 4 -}}
{{- $octets := splitList "." $addr -}}
{{- if ne (len $octets) 4 -}}
{{- $problem = "is not an IPv4 address of four decimal octets" -}}
{{- else -}}
{{- range $octets -}}
{{- if and (not $problem) (or (not (regexMatch "^(0|[1-9][0-9]{0,2})$" .)) (gt (atoi .) 255)) -}}
{{- $problem = printf "has the octet %q, which is not a number from 0 to 255 written without a leading zero" . -}}
{{- end -}}
{{- if not $problem }}{{ $bits = printf "%s%08b" $bits (atoi .) }}{{ end -}}
{{- end -}}
{{- end -}}
{{- else if and (contains ":" $addr) (regexMatch "^[0-9A-Fa-f:]+$" $addr) -}}
{{- $family = 6 -}}
{{- $halves := splitList "::" (lower $addr) -}}
{{- $groups := list -}}
{{- if gt (len $halves) 2 -}}
{{- $problem = "has more than one \"::\"" -}}
{{- else if eq (len $halves) 2 -}}
{{- $left := list -}}
{{- $right := list -}}
{{- if first $halves }}{{ $left = splitList ":" (first $halves) }}{{ end -}}
{{- if last $halves }}{{ $right = splitList ":" (last $halves) }}{{ end -}}
{{- $given := add (len $left) (len $right) -}}
{{- if gt $given 7 -}}
{{- $problem = "has more than eight groups" -}}
{{- else -}}
{{- $groups = $left -}}
{{- range until (int (sub 8 $given)) }}{{ $groups = append $groups "0" }}{{ end -}}
{{- $groups = concat $groups $right -}}
{{- end -}}
{{- else -}}
{{- $groups = splitList ":" (lower $addr) -}}
{{- if ne (len $groups) 8 }}{{ $problem = "does not have eight groups and no \"::\" stands for missing ones" }}{{ end -}}
{{- end -}}
{{- range $groups -}}
{{- if and (not $problem) (not (regexMatch "^[0-9a-f]{1,4}$" .)) -}}
{{- $problem = printf "has the group %q, which is not one to four hex digits" . -}}
{{- end -}}
{{- end -}}
{{- if not $problem -}}
{{- $hex := dict "0" "0000" "1" "0001" "2" "0010" "3" "0011" "4" "0100" "5" "0101" "6" "0110" "7" "0111" "8" "1000" "9" "1001" "a" "1010" "b" "1011" "c" "1100" "d" "1101" "e" "1110" "f" "1111" -}}
{{- range $groups -}}
{{- range (printf "000%s" . | trunc -4 | splitList "") }}{{ $bits = printf "%s%s" $bits (get $hex .) }}{{ end -}}
{{- end -}}
{{- end -}}
{{- else -}}
{{- $problem = "is not an IPv4 or IPv6 address (IPv6 is written in hex groups, without brackets, an embedded IPv4 address or a zone)" -}}
{{- end -}}
{{- if not $problem -}}
{{- $prefix = atoi (last $parts) -}}
{{- $max := ternary 32 128 (eq $family 4) -}}
{{- if gt $prefix $max -}}
{{- $problem = printf "has a prefix length above %d" $max -}}
{{- else if not (regexMatch "^0*$" (substr $prefix $max $bits)) -}}
{{- $problem = "has address bits set past its prefix length; write the first address of the range" -}}
{{- end -}}
{{- end -}}
{{- dict "family" $family "prefix" $prefix "bits" $bits "problem" $problem | toJson -}}
{{- end -}}

{{/* "true" when two CIDRs parsed by "continuo.egress.parseCidr" share an
address: same family, and equal bits up to the shorter prefix length.
Otherwise empty. Call: include "continuo.egress.cidrsOverlap" (dict "a" $a "b" $b) */}}
{{- define "continuo.egress.cidrsOverlap" -}}
{{- if eq (int .a.family) (int .b.family) -}}
{{- $m := min (int .a.prefix) (int .b.prefix) | int -}}
{{- if eq (substr 0 $m .a.bits) (substr 0 $m .b.bits) }}true{{ end -}}
{{- end -}}
{{- end -}}

{{/* The address ranges no direct destination may overlap: the unspecified
network, loopback, link-local (where cloud metadata services answer) and the
other cloud metadata addresses, none of which egress-proxy connects to either;
and IPv4-mapped IPv6 addresses, which stand for IPv4 ones and are written as
those instead. A NAT64 address (64:ff9b::/96) stands for the IPv4 address in its
last 32 bits and is judged by it ("continuo.egress.neverDirectHit"). A JSON list
of {cidr, what}. */}}
{{- define "continuo.egress.neverDirect" -}}
{{- list
    (dict "cidr" "0.0.0.0/8" "what" "the unspecified network")
    (dict "cidr" "127.0.0.0/8" "what" "loopback")
    (dict "cidr" "169.254.0.0/16" "what" "link-local, where cloud metadata services answer")
    (dict "cidr" "100.100.100.200/32" "what" "a cloud metadata address")
    (dict "cidr" "168.63.129.16/32" "what" "a cloud metadata address")
    (dict "cidr" "::/128" "what" "the unspecified address")
    (dict "cidr" "::1/128" "what" "loopback")
    (dict "cidr" "fe80::/10" "what" "link-local")
    (dict "cidr" "fd00:ec2::/32" "what" "a cloud metadata range")
    (dict "cidr" "fd20:ce::254/128" "what" "a cloud metadata address")
    (dict "cidr" "::ffff:0:0/96" "what" "IPv4-mapped IPv6, which stands for IPv4 addresses; write the IPv4 address itself")
  | toJson -}}
{{- end -}}

{{/* The first range of "continuo.egress.neverDirect" a parsed CIDR overlaps,
as JSON {cidr, what}, or {} when there is none. A CIDR that overlaps
64:ff9b::/96 is judged by the IPv4 range its last 32 bits stand for, and one
wider than that /96 always hits it. "never" is the neverDirect list with each
entry's parsed CIDR in "parsed".
Call: include "continuo.egress.neverDirectHit" (dict "c" $parsed "never" $never) | fromJson */}}
{{- define "continuo.egress.neverDirectHit" -}}
{{- $c := .c -}}
{{- $hit := dict -}}
{{- range .never -}}
{{- if and (not $hit) (include "continuo.egress.cidrsOverlap" (dict "a" $c "b" .parsed)) -}}
{{- $hit = dict "cidr" .cidr "what" .what -}}
{{- end -}}
{{- end -}}
{{- $nat64 := include "continuo.egress.parseCidr" "64:ff9b::/96" | fromJson -}}
{{- if and (not $hit) (include "continuo.egress.cidrsOverlap" (dict "a" $c "b" $nat64)) -}}
{{- if lt (int $c.prefix) 96 -}}
{{- $hit = dict "cidr" "64:ff9b::/96" "what" "NAT64, which stands for every IPv4 address, loopback and link-local ones among them" -}}
{{- else -}}
{{- $v4 := dict "family" 4 "prefix" (sub (int $c.prefix) 96) "bits" (substr 96 128 $c.bits) -}}
{{- $inner := include "continuo.egress.neverDirectHit" (dict "c" $v4 "never" .never) | fromJson -}}
{{- if $inner.cidr }}{{ $hit = dict "cidr" "64:ff9b::/96" "what" (printf "NAT64, here of %s: %s" $inner.cidr $inner.what) }}{{ end -}}
{{- end -}}
{{- end -}}
{{- $hit | toJson -}}
{{- end -}}

{{/* Destinations the agents reach without the proxy, as a JSON list of
{cidr, ports, host?}. The chart reads it only while egress.enabled is true
(egress-proxy/configmap.yaml checks it then, whatever else renders). It holds:
- each egress.directDestinations entry. It is checked as written and the render
  fails naming the key and the entry unless it is an object with exactly "cidr"
  and "ports"; cidr a string the rules of "continuo.egress.parseCidr" take,
  with a prefix length of at least 1, overlapping no range of
  "continuo.egress.neverDirect"; ports a non-empty list of whole numbers from 1
  to 65535 (a number, not a string). cidr is lower-cased, ports deduplicated.
- every IP address the agents are given, as a /32 or /128 on its port, with
  "host" the address for NO_PROXY, because egress-proxy admits host names only:
  the host of a URL of "continuo.egress.urlSources"; the host of an address
  of "continuo.egress.givenAddresses" ("continuo.egress.addressIP"); and the
  externalDatabase.host (with postgresql.enabled=false) or externalRedis.host
  (with redis.enabled=false) that is an IP address outside the private ranges
  ("continuo.egress.privateRanges"), on externalDatabase.port or
  externalRedis.port. An external datastore at a private address needs no entry:
  the agents' NetworkPolicy admits the private ranges on its port.
  The render fails, naming the setting, on an address that is not one by those
  rules, that lies in a range of "continuo.egress.neverDirect" other than
  loopback, or whose port is outside 1-65535. A loopback address is left out:
  the agents' own pod answers it and no proxy is ever used for it. So is the
  unspecified address (0.0.0.0, ::) in a givenAddresses value, which names no
  destination but a listen address.
Call: include "continuo.egress.directDestinations" $ | fromJsonArray */}}
{{- define "continuo.egress.directDestinations" -}}
{{- $e := .Values.egress | default dict -}}
{{- $listed := $e.directDestinations | default list -}}
{{- if not (kindIs "slice" $listed) -}}
{{- fail (printf "egress.directDestinations must be a list of {cidr, ports} entries, got %s" (toJson $listed)) -}}
{{- end -}}
{{- $never := list -}}
{{- range include "continuo.egress.neverDirect" . | fromJsonArray -}}
{{- $never = append $never (dict "cidr" .cidr "what" .what "parsed" (include "continuo.egress.parseCidr" .cidr | fromJson)) -}}
{{- end -}}
{{- $hint := "Write each entry as {cidr: <address>/<prefix length>, ports: [<port>, ...]}, e.g. {cidr: 203.0.113.10/32, ports: [5432]}" -}}
{{- $out := list -}}
{{- range $entry := $listed -}}
{{- $shown := printf "egress.directDestinations entry %s" (toJson $entry) -}}
{{- if not (kindIs "map" $entry) -}}
{{- fail (printf "%s is not an object with cidr and ports. %s" $shown $hint) -}}
{{- end -}}
{{- if ne (keys $entry | sortAlpha | join ",") "cidr,ports" -}}
{{- fail (printf "%s must have exactly the keys cidr and ports. %s" $shown $hint) -}}
{{- end -}}
{{- if not (kindIs "string" $entry.cidr) -}}
{{- fail (printf "%s has a cidr that is not a string. %s" $shown $hint) -}}
{{- end -}}
{{- $c := include "continuo.egress.parseCidr" $entry.cidr | fromJson -}}
{{- if $c.problem -}}
{{- fail (printf "%s: its cidr %q %s. %s" $shown $entry.cidr $c.problem $hint) -}}
{{- end -}}
{{- if eq (int $c.prefix) 0 -}}
{{- fail (printf "%s: its cidr %q has a prefix length of 0, which takes in every address; list the ranges the agents need" $shown $entry.cidr) -}}
{{- end -}}
{{- $hit := include "continuo.egress.neverDirectHit" (dict "c" $c "never" $never) | fromJson -}}
{{- if $hit.cidr -}}
{{- fail (printf "%s: its cidr %q overlaps %s (%s), which the agents never reach directly; list narrower ranges that leave it out" $shown $entry.cidr $hit.cidr $hit.what) -}}
{{- end -}}
{{- if not (kindIs "slice" $entry.ports) -}}
{{- fail (printf "%s has ports that are not a list. %s" $shown $hint) -}}
{{- end -}}
{{- if not $entry.ports -}}
{{- fail (printf "%s has an empty ports list; a NetworkPolicy rule without ports admits every port. %s" $shown $hint) -}}
{{- end -}}
{{- $ports := list -}}
{{- range $p := $entry.ports -}}
{{- if not (include "continuo.egress.isPort" $p) -}}
{{- fail (printf "%s has the port %s, which is not a whole number from 1 to 65535. %s" $shown (toJson $p) $hint) -}}
{{- end -}}
{{- $ports = append $ports (atoi (toString $p)) -}}
{{- end -}}
{{- $out = append $out (dict "cidr" (lower $entry.cidr) "ports" (uniq $ports)) -}}
{{- end -}}
{{- $sources := list -}}
{{- range $source := include "continuo.egress.urlSources" . | fromJsonArray -}}
{{- $u := include "continuo.egress.urlEntry" (dict "url" $source.url "allowPrivate" true) | fromJson -}}
{{- if include "continuo.egress.isIP" $u.host -}}
{{- $shown := printf "%s=%q" $source.key $source.url -}}
{{- $sources = append $sources (dict "shown" $shown "ip" (trimAll "[]" $u.host) "port" (int $u.port) "portOk" $u.portOk "portProblem" (printf "%s has a port outside 1-65535" $shown) "listen" false "datastore" false) -}}
{{- end -}}
{{- end -}}
{{- range $given := include "continuo.egress.givenAddresses" . | fromJsonArray -}}
{{- $a := include "continuo.egress.addressIP" $given.value | fromJson -}}
{{- if $a.ip -}}
{{- $shown := printf "%s=%q" $given.key (toString $given.value) -}}
{{- $port := atoi $a.port -}}
{{- $sources = append $sources (dict "shown" $shown "ip" $a.ip "port" $port "portOk" (and (ge $port 1) (le $port 65535)) "portProblem" (printf "%s has a port outside 1-65535" $shown) "listen" true "datastore" false) -}}
{{- end -}}
{{- end -}}
{{- $datastores := list -}}
{{- if not .Values.postgresql.enabled }}{{ $datastores = append $datastores (dict "key" "externalDatabase" "values" (.Values.externalDatabase | default dict)) }}{{ end -}}
{{- if not .Values.redis.enabled }}{{ $datastores = append $datastores (dict "key" "externalRedis" "values" (.Values.externalRedis | default dict)) }}{{ end -}}
{{- range $ds := $datastores -}}
{{- $host := $ds.values.host | default "" | toString -}}
{{- if include "continuo.egress.isIP" $host -}}
{{- $portOk := include "continuo.egress.isPort" $ds.values.port -}}
{{- $sources = append $sources (dict "shown" (printf "%s.host=%q" $ds.key $host) "ip" (trimAll "[]" $host | lower) "port" (atoi (toString $ds.values.port)) "portOk" (not (empty $portOk)) "portProblem" (printf "%s.port=%s is not a port: the agents reach %s.host=%q directly on it, so it must be a whole number from 1 to 65535" $ds.key (toJson $ds.values.port) $ds.key $host) "listen" false "datastore" true) -}}
{{- end -}}
{{- end -}}
{{- $private := list -}}
{{- range include "continuo.egress.privateRanges" . | fromJsonArray -}}
{{- $private = append $private (include "continuo.egress.parseCidr" . | fromJson) -}}
{{- end -}}
{{- range $s := $sources -}}
{{- $c := include "continuo.egress.parseCidr" (printf "%s/%s" $s.ip (ternary "128" "32" (contains ":" $s.ip))) | fromJson -}}
{{- if $c.problem -}}
{{- fail (printf "%s: its host %s %s. Write an IPv4 address as four decimal octets 0-255 without leading zeros, or use a DNS host name, which egress-proxy admits" $s.shown $s.ip $c.problem) -}}
{{- end -}}
{{- $hit := include "continuo.egress.neverDirectHit" (dict "c" $c "never" $never) | fromJson -}}
{{- $listenAddress := and $s.listen (regexMatch "^0+$" $c.bits) -}}
{{- if and $hit.cidr (ne $hit.what "loopback") (not $listenAddress) -}}
{{- fail (printf "%s: its host %s lies in %s (%s), which the agents never reach" $s.shown $s.ip $hit.cidr $hit.what) -}}
{{- end -}}
{{- $privateDatastore := false -}}
{{- if $s.datastore -}}
{{- range $private }}{{ if include "continuo.egress.cidrsOverlap" (dict "a" $c "b" .) }}{{ $privateDatastore = true }}{{ end }}{{ end -}}
{{- end -}}
{{- if not (or $hit.cidr $privateDatastore) -}}
{{- if not $s.portOk -}}
{{- fail $s.portProblem -}}
{{- end -}}
{{- $out = append $out (dict "cidr" (printf "%s/%d" $s.ip (ternary 128 32 (contains ":" $s.ip))) "ports" (list (int $s.port)) "host" $s.ip) -}}
{{- end -}}
{{- end -}}
{{- uniq $out | toJson -}}
{{- end -}}

{{/* The addresses the agents are given through values, as a JSON list of
{key, value}, "key" the setting that gave it: the global.*GrpcAddr values, and
every value in the `services` env of agent-chat and agent-remediation. */}}
{{- define "continuo.egress.givenAddresses" -}}
{{- $given := list -}}
{{- $g := .Values.global | default dict -}}
{{- range $key := list "stateGrpcAddr" "orchestratorGrpcAddr" "agentRemediationGrpcAddr" "agentChatGrpcAddr" "deadLetterControllerGrpcAddr" -}}
{{- $given = append $given (dict "key" (printf "global.%s" $key) "value" (get $g $key)) -}}
{{- end -}}
{{- range $svc := .Values.services -}}
{{- if has $svc.name (list "agent-chat" "agent-remediation") -}}
{{- range $key, $val := ($svc.env | default dict) -}}
{{- $given = append $given (dict "key" (printf "services[name=%s].env.%s" $svc.name $key) "value" $val) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $given | toJson -}}
{{- end -}}

{{/* The IP address and port of an address whose host is an IP address, as JSON
{ip, port}, "port" the digits as written: an http or https URL (port 443 for
https and 80 for http when it names none; a user:password@ is ignored) or a
host:port value, an IPv6 address in brackets in either. An IPv4 address is four
dot-separated runs of digits, checked further by "continuo.egress.parseCidr".
{} for any other value. Call: include "continuo.egress.addressIP" $value | fromJson */}}
{{- define "continuo.egress.addressIP" -}}
{{- $v := . | toString -}}
{{- $hostport := $v -}}
{{- $default := "" -}}
{{- if regexMatch "^https?://" $v -}}
{{- $authority := regexReplaceAll "^https?://([^/?#]*).*$" $v "${1}" -}}
{{- $hostport = regexReplaceAll "^.*@" $authority "" -}}
{{- $default = ternary "443" "80" (hasPrefix "https:" $v) -}}
{{- end -}}
{{- $ip := "" -}}
{{- $port := "" -}}
{{- if regexMatch "^\\[[^\\]]*\\](:[0-9]+)?$" $hostport -}}
{{- $ip = regexReplaceAll "^\\[([^\\]]*)\\].*$" $hostport "${1}" -}}
{{- $port = regexFind "[0-9]+$" (regexFind "\\]:[0-9]+$" $hostport) -}}
{{- else if regexMatch "^[0-9]+(\\.[0-9]+){3}(:[0-9]+)?$" $hostport -}}
{{- $ip = regexReplaceAll ":[0-9]+$" $hostport "" -}}
{{- $port = regexFind "[0-9]+$" (regexFind ":[0-9]+$" $hostport) -}}
{{- end -}}
{{- $port = $port | default $default -}}
{{- if and $ip $port -}}{{ dict "ip" (lower $ip) "port" $port | toJson }}{{- else -}}{}{{- end -}}
{{- end -}}

{{/* The private address ranges, as a JSON list of CIDRs: the ranges
"continuo.egress.privatePeers" admits an external datastore on. */}}
{{- define "continuo.egress.privateRanges" -}}
{{- list "10.0.0.0/8" "100.64.0.0/10" "172.16.0.0/12" "192.168.0.0/16" "fc00::/7" | toJson -}}
{{- end -}}

{{/* The lower-cased host and the port of an address an agent is given, as
"host:port": an http or https URL (without its user:password@; port 443 for
https and 80 for http when it names none) or a host:port value. Empty for any
other value, and for a URL whose host is a bracketed IPv6 address.
Call: include "continuo.egress.addressHostPort" $value */}}
{{- define "continuo.egress.addressHostPort" -}}
{{- $v := . | toString -}}
{{- if regexMatch "^https?://" $v -}}
{{- $authority := regexReplaceAll "^https?://([^/?#]*).*$" $v "${1}" -}}
{{- $hostport := regexReplaceAll "^.*@" $authority "" -}}
{{- $host := regexReplaceAll ":[0-9]*$" $hostport "" | lower -}}
{{- $port := regexFind "[0-9]+$" (regexFind ":[0-9]+$" $hostport) | default (ternary "443" "80" (hasPrefix "https:" $v)) -}}
{{- if and $host (not (hasPrefix "[" $host)) }}{{ printf "%s:%s" $host $port }}{{ end -}}
{{- else if regexMatch "^[A-Za-z0-9.-]+:[0-9]+$" $v -}}
{{- lower $v -}}
{{- end -}}
{{- end -}}

{{/* "true" when the allowlist admits "host" on "port" with private addresses,
as egress-proxy matches it: an entry that allows private addresses and names the
host, or is a "*.d" entry for a host below d, on the same port. Otherwise empty.
An in-cluster name resolves to a private address, which the proxy refuses for
an entry that does not allow private addresses. The port is read in base 10.
Call: include "continuo.egress.allowlistAdmits" (dict "host" $h "port" $p "allowlist" $allowlist) */}}
{{- define "continuo.egress.allowlistAdmits" -}}
{{- $host := lower .host -}}
{{- $port := .port | toString | atoi -}}
{{- $hit := false -}}
{{- range .allowlist -}}
{{- if and .allowPrivate (eq (int .port) $port) (or (eq .host $host) (and (hasPrefix "*." .host) (hasSuffix (trimPrefix "*" .host) $host))) }}{{ $hit = true }}{{ end -}}
{{- end -}}
{{- if $hit }}true{{ end -}}
{{- end -}}

{{/* The names of every Service the chart can render for a workload (each
`services` entry, egress-proxy, and the bundled Postgres, Redis, Neo4j, MinIO
and Dex) in the forms a client may use: <name>, <name>.<namespace>,
<name>.<namespace>.svc and <name>.<namespace>.svc.cluster.local, as a JSON list.
They are pods of the release's namespace. */}}
{{- define "continuo.egress.releaseServiceHosts" -}}
{{- $ns := .Release.Namespace -}}
{{- $services := list "egress-proxy" -}}
{{- range .Values.services }}{{ $services = append $services (.name | toString | lower) }}{{ end -}}
{{- $services = concat $services (list (include "continuo.postgresql.fullname" .) (include "continuo.redis.fullname" .) (include "continuo.neo4j.fullname" .) (include "continuo.minio.fullname" .) (include "continuo.dex.fullname" .)) -}}
{{- $local := list -}}
{{- range uniq $services -}}
{{- $local = concat $local (list . (printf "%s.%s" . $ns) (printf "%s.%s.svc" . $ns) (printf "%s.%s.svc.cluster.local" . $ns)) -}}
{{- end -}}
{{- $local | toJson -}}
{{- end -}}

{{/* The in-cluster addresses the agents are given and reach directly, other
than the release's own Services ("continuo.egress.releaseServiceHosts"), as a
JSON list of {key, host, port}, "port" the digits as written and "key" the
setting that gave it. An address is in-cluster when its host has no dot or ends
in .svc or .svc.cluster.local; it comes from a global.*GrpcAddr value, or from an
http(s) URL or host:port value in the `services` env of agent-chat or
agent-remediation. One the allowlist admits on that port
("continuo.egress.allowlistAdmits") is left out: the proxy carries it. An
address with another host goes through the proxy, which admits it only when the
allowlist does. */}}
{{- define "continuo.egress.inClusterDirect" -}}
{{- $local := include "continuo.egress.releaseServiceHosts" . | fromJsonArray -}}
{{- $allowlist := include "continuo.egress.allowlist" . | fromJsonArray -}}
{{- $out := list -}}
{{- $seen := dict -}}
{{- range include "continuo.egress.givenAddresses" . | fromJsonArray -}}
{{- $hostPort := include "continuo.egress.addressHostPort" .value -}}
{{- $host := regexReplaceAll ":[0-9]+$" $hostPort "" -}}
{{- $port := regexFind "[0-9]+$" $hostPort -}}
{{- if and $hostPort (not (hasKey $seen $hostPort)) (or (not (contains "." $host)) (regexMatch "\\.svc(\\.cluster\\.local)?$" $host)) -}}
{{- $_ := set $seen $hostPort true -}}
{{- if not (or (has $host $local) (include "continuo.egress.allowlistAdmits" (dict "host" $host "port" $port "allowlist" $allowlist))) -}}
{{- $out = append $out (dict "key" .key "host" $host "port" $port) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{/* The namespaces other than the release's that hold an address of
"continuo.egress.inClusterDirect" (a host <name>.<namespace>.svc, optionally
followed by .cluster.local), as a JSON list of {namespace, ports} ordered by
namespace, the ports numbers without duplicates. The agents' NetworkPolicy
admits the pods of each such namespace on those ports. The render fails on a
port that is not from 1 to 65535. */}}
{{- define "continuo.egress.otherNamespaceDestinations" -}}
{{- $byNamespace := dict -}}
{{- range include "continuo.egress.inClusterDirect" . | fromJsonArray -}}
{{- $name := trimSuffix ".cluster.local" .host -}}
{{- $labels := splitList "." (trimSuffix ".svc" $name) -}}
{{- $ns := last $labels -}}
{{- if and (hasSuffix ".svc" $name) (ge (len $labels) 2) (ne $ns $.Release.Namespace) (regexMatch "^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$" $ns) -}}
{{- $port := atoi .port -}}
{{- if or (lt $port 1) (gt $port 65535) -}}
{{- fail (printf "%s gives the agents the in-cluster address %s:%s, whose port is not from 1 to 65535" .key .host .port) -}}
{{- end -}}
{{- $_ := set $byNamespace $ns (append (get $byNamespace $ns | default list) $port | uniq) -}}
{{- end -}}
{{- end -}}
{{- $out := list -}}
{{- range $ns := keys $byNamespace | sortAlpha -}}
{{- $out = append $out (dict "namespace" $ns "ports" (get $byNamespace $ns)) -}}
{{- end -}}
{{- $out | toJson -}}
{{- end -}}

{{/* NO_PROXY for the agents while egress-proxy renders. Their Go HTTP and gRPC
clients send every host and port it does not cover to egress-proxy, which admits
only the hosts and ports on its allowlist, so it lists, comma-separated:
- localhost, 127.0.0.1 and ::1;
- the release's own Services in every form ("continuo.egress.releaseServiceHosts"),
  on every port. These are in the release's namespace and a direct connection
  reaches them on any port, so they stay here whatever the allowlist says;
- every other in-cluster address the agents reach directly
  ("continuo.egress.inClusterDirect"), as "host:port", which Go matches on that
  port only;
- the addresses they reach directly ("continuo.egress.directDestinations"):
  every IP address they are given, and the CIDR of each
  egress.directDestinations entry, which Go matches as a range. */}}
{{- define "continuo.egress.noProxy" -}}
{{- $entries := concat (list "localhost" "127.0.0.1" "::1") (include "continuo.egress.releaseServiceHosts" . | fromJsonArray) -}}
{{- range include "continuo.egress.inClusterDirect" . | fromJsonArray -}}
{{- $entries = append $entries (printf "%s:%s" .host .port) -}}
{{- end -}}
{{- range include "continuo.egress.directDestinations" . | fromJsonArray -}}
{{- $entries = append $entries (.host | default .cidr) -}}
{{- end -}}
{{- uniq $entries | join "," -}}
{{- end -}}

{{/* "true" when the argument is a port as values hold one: a number (not a
string) whose decimal text is a whole number from 1 to 65535 without a leading
zero. Otherwise empty. Call: include "continuo.egress.isPort" $value */}}
{{- define "continuo.egress.isPort" -}}
{{- $text := toString . -}}
{{- if and (has (kindOf .) (list "int" "int64" "float64")) (regexMatch "^[1-9][0-9]{0,4}$" $text) (le (atoi $text) 65535) -}}true{{- end -}}
{{- end -}}

{{/* NetworkPolicy egress rule for name resolution: port 53, UDP and TCP, to
any pod in any namespace (the cluster DNS) and to link-local addresses (a
node-local DNS cache, or a cloud resolver at 169.254.169.254). Emits one YAML
list item with no indentation. */}}
{{- define "continuo.egress.dnsRule" -}}
- to:
    - namespaceSelector: {}
    - ipBlock:
        cidr: 169.254.0.0/16
  ports:
    - protocol: UDP
      port: 53
    - protocol: TCP
      port: 53
{{- end -}}

{{/* The private address ranges as NetworkPolicy peers (10.0.0.0/8,
100.64.0.0/10, 172.16.0.0/12, 192.168.0.0/16 and fc00::/7), less the cloud
metadata addresses inside them (100.100.100.200, fd00:ec2::/32 and
fd20:ce::254), which egress-proxy refuses too. Emits YAML list items with no
indentation. */}}
{{- define "continuo.egress.privatePeers" -}}
- ipBlock:
    cidr: 10.0.0.0/8
- ipBlock:
    cidr: 100.64.0.0/10
    except: [100.100.100.200/32]
- ipBlock:
    cidr: 172.16.0.0/12
- ipBlock:
    cidr: 192.168.0.0/16
- ipBlock:
    cidr: "fc00::/7"
    except: ["fd00:ec2::/32", "fd20:ce::254/128"]
{{- end -}}

{{/* The ports of the external Postgres (externalDatabase.port, with
postgresql.enabled=false) and the external Redis (externalRedis.port, with
redis.enabled=false), as a JSON list of numbers without duplicates; empty while
both are bundled, as pods of the release namespace. The agents' NetworkPolicy
admits them to the private ranges and to the pods of every namespace. The
render fails on a port that is not one by "continuo.egress.isPort". */}}
{{- define "continuo.egress.externalDatastorePorts" -}}
{{- $given := list -}}
{{- if not .Values.postgresql.enabled -}}
{{- $given = append $given (dict "key" "externalDatabase.port" "port" (.Values.externalDatabase | default dict).port) -}}
{{- end -}}
{{- if not .Values.redis.enabled -}}
{{- $given = append $given (dict "key" "externalRedis.port" "port" (.Values.externalRedis | default dict).port) -}}
{{- end -}}
{{- $ports := list -}}
{{- range $given -}}
{{- if not (include "continuo.egress.isPort" .port) -}}
{{- fail (printf "%s=%s is not a port: the agents' NetworkPolicy admits the external datastore on it, so it must be a whole number from 1 to 65535" .key (toJson .port)) -}}
{{- end -}}
{{- $ports = append $ports (atoi (toString .port)) -}}
{{- end -}}
{{- uniq $ports | toJson -}}
{{- end -}}

{{- define "continuo.redis.host" -}}
{{- if .Values.redis.enabled -}}
{{- include "continuo.redis.fullname" . -}}
{{- else -}}
{{- required "externalRedis.host is required when redis.enabled=false" .Values.externalRedis.host -}}
{{- end -}}
{{- end -}}

{{- define "continuo.redis.port" -}}
{{- if .Values.redis.enabled -}}6379{{- else -}}{{ .Values.externalRedis.port }}{{- end -}}
{{- end -}}

{{- define "continuo.neo4j.uri" -}}
{{- if .Values.neo4j.enabled -}}
{{- printf "bolt://%s:7687" (include "continuo.neo4j.fullname" .) -}}
{{- else -}}
{{- required "externalNeo4j.uri is required when neo4j.enabled=false" .Values.externalNeo4j.uri -}}
{{- end -}}
{{- end -}}

{{- define "continuo.neo4j.username" -}}
{{- if .Values.neo4j.enabled -}}neo4j{{- else -}}{{ .Values.externalNeo4j.username }}{{- end -}}
{{- end -}}

{{- define "continuo.s3.endpointUrl" -}}
{{- if .Values.minio.enabled -}}
{{- printf "http://%s:9000" (include "continuo.minio.fullname" .) -}}
{{- else -}}
{{- required "s3.endpointUrl is required when minio.enabled=false" .Values.s3.endpointUrl -}}
{{- end -}}
{{- end -}}

{{- define "continuo.s3.bucket" -}}
{{- if .Values.minio.enabled -}}{{ .Values.minio.bucket }}{{- else -}}{{ required "s3.bucket is required when minio.enabled=false" .Values.s3.bucket }}{{- end -}}
{{- end -}}

{{- define "continuo.s3.region" -}}
{{- if .Values.minio.enabled -}}us-east-1{{- else -}}{{ required "s3.region is required when minio.enabled=false" .Values.s3.region }}{{- end -}}
{{- end -}}

{{- define "continuo.s3.env" -}}{{ .Values.s3.env }}{{- end -}}

{{/*
Services whose process reads or writes object storage and therefore needs the S3
credentials. The deployment template injects AWS_ACCESS_KEY_ID /
AWS_SECRET_ACCESS_KEY for these from here, not from each service's `secretEnv`
entry: `services` is a list, Helm replaces lists wholesale, and an operator
upgrading with their own copy of values.yaml would otherwise carry an older entry
that silently lacks the refs. Endpoint, bucket and region reach every pod through
the shared ConfigMap and need no per-service wiring.
*/}}
{{- /*
Services whose code connects to Redis at boot: the seven Go services read
REDIS_HOST/REDIS_PORT or REDIS_ADDR from the shared ConfigMap, while ui and
topology-controller read the REDIS_URL their `services` entry carries. In
bundled mode each of them is gated on Redis answering PING. Listed here rather
than inferred from a service's `env`, which only names the URL-reading half and
lives in the replaceable `services` list. scripts/install-test/assert-redis-gate.py
pins this list to the services whose source imports a Redis client.
*/ -}}
{{- define "continuo.redis.clientServices" -}}
["state","orchestrator","agent-chat","execution-controller","ui","topology-controller","release-controller","remediation","agent-remediation","dead-letter-controller"]
{{- end -}}

{{- define "continuo.s3.credentialServices" -}}
["state","orchestrator","topology-controller","execution-controller","release-controller","remediation","agent-remediation","ui","agent-chat"]
{{- end -}}

{{- define "continuo.auth.issuerUrl" -}}
{{- if .Values.dex.enabled -}}
{{- printf "http://%s:5556/dex" (include "continuo.dex.fullname" .) -}}
{{- else -}}
{{- required "auth.issuerUrl is required when dex.enabled=false" .Values.auth.issuerUrl -}}
{{- end -}}
{{- end -}}

{{- define "continuo.auth.clientId" -}}
{{- if .Values.dex.enabled -}}continuo-ui{{- else -}}{{ required "auth.clientId is required when dex.enabled=false" .Values.auth.clientId }}{{- end -}}
{{- end -}}

{{- define "continuo.auth.operatorEmails" -}}
{{- if .Values.auth.operatorEmails -}}
{{- .Values.auth.operatorEmails -}}
{{- else if .Values.dex.enabled -}}
{{- .Values.dex.demoUser.email -}}
{{- end -}}
{{- end -}}

{{/* ------- pod/container hardening ------- */}}

{{- define "continuo.podSecurityContext" -}}
runAsNonRoot: true
seccompProfile:
  type: RuntimeDefault
{{- end -}}

{{- define "continuo.containerSecurityContext" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: ["ALL"]
{{- end -}}

{{/* Stable generated password: reuse the value already stored in the live
     Secret (lookup) so upgrades never rotate it; generate on first install.
     `helm template`/`--dry-run` cannot lookup and will show a fresh random —
     harmless, install/upgrade is what matters.
     Call: include "continuo.generatedSecret" (dict "root" $ "secretName" "x" "key" "password") */}}
{{- define "continuo.generatedSecret" -}}
{{- $existing := lookup "v1" "Secret" .root.Release.Namespace .secretName -}}
{{- if and $existing (hasKey $existing.data .key) -}}
{{- index $existing.data .key | b64dec -}}
{{- else -}}
{{- randAlphaNum 32 -}}
{{- end -}}
{{- end -}}

{{/* Which Secret holds a credential group. Resolution order per group:
     bundled datastore secret (generated or its own existingSecret) →
     external existingSecret → chart credentials Secret (inline values). */}}
{{- define "continuo.secretName" -}}
{{- $v := .root.Values -}}
{{- if eq .ref "postgres" -}}
  {{- if $v.postgresql.enabled -}}
    {{- default (include "continuo.postgresql.fullname" .root) $v.postgresql.auth.existingSecret -}}
  {{- else if $v.externalDatabase.existingSecret -}}
    {{- $v.externalDatabase.existingSecret -}}
  {{- else -}}
    {{- include "continuo.credentialsSecretName" .root -}}
  {{- end -}}
{{- else if eq .ref "redis" -}}
  {{- if $v.redis.enabled -}}
    {{- default (include "continuo.redis.fullname" .root) $v.redis.auth.existingSecret -}}
  {{- else if $v.externalRedis.existingSecret -}}
    {{- $v.externalRedis.existingSecret -}}
  {{- else -}}
    {{- include "continuo.credentialsSecretName" .root -}}
  {{- end -}}
{{- else if eq .ref "neo4j" -}}
  {{- if $v.neo4j.enabled -}}
    {{- default (include "continuo.neo4j.fullname" .root) $v.neo4j.auth.existingSecret -}}
  {{- else if $v.externalNeo4j.existingSecret -}}
    {{- $v.externalNeo4j.existingSecret -}}
  {{- else -}}
    {{- include "continuo.credentialsSecretName" .root -}}
  {{- end -}}
{{- else if or (eq .ref "s3AccessKeyId") (eq .ref "s3SecretAccessKey") -}}
  {{- if $v.minio.enabled -}}
    {{- default (include "continuo.minio.fullname" .root) $v.minio.auth.existingSecret -}}
  {{- else if $v.s3.existingSecret -}}
    {{- $v.s3.existingSecret -}}
  {{- else -}}
    {{- include "continuo.credentialsSecretName" .root -}}
  {{- end -}}
{{- else if eq .ref "authClientSecret" -}}
  {{- if $v.dex.enabled -}}
    {{- include "continuo.dex.fullname" .root -}}
  {{- else if $v.auth.existingSecret -}}
    {{- $v.auth.existingSecret -}}
  {{- else -}}
    {{- include "continuo.credentialsSecretName" .root -}}
  {{- end -}}
{{- else if eq .ref "llm" -}}
  {{- default (include "continuo.credentialsSecretName" .root) $v.llm.existingSecret -}}
{{- else if or (eq .ref "githubToken") (eq .ref "githubAppPrivateKey") -}}
  {{- default (include "continuo.credentialsSecretName" .root) $v.github.existingSecret -}}
{{- else -}}
{{- fail (printf "unknown secretEnv ref %q" .ref) -}}
{{- end -}}
{{- end -}}

{{/* Which key inside that Secret. */}}
{{- define "continuo.secretKey" -}}
{{- $v := .root.Values -}}
{{- if eq .ref "postgres" -}}
  {{- if $v.postgresql.enabled -}}
    {{- if $v.postgresql.auth.existingSecret -}}{{ $v.postgresql.auth.existingSecretPasswordKey }}{{- else -}}password{{- end -}}
  {{- else if $v.externalDatabase.existingSecret -}}
    {{- $v.externalDatabase.existingSecretPasswordKey -}}
  {{- else -}}postgres-password{{- end -}}
{{- else if eq .ref "redis" -}}
  {{- if $v.redis.enabled -}}
    {{- if $v.redis.auth.existingSecret -}}{{ $v.redis.auth.existingSecretPasswordKey }}{{- else -}}password{{- end -}}
  {{- else if $v.externalRedis.existingSecret -}}{{ $v.externalRedis.existingSecretPasswordKey }}
  {{- else -}}redis-password{{- end -}}
{{- else if eq .ref "neo4j" -}}
  {{- if $v.neo4j.enabled -}}
    {{- if $v.neo4j.auth.existingSecret -}}{{ $v.neo4j.auth.existingSecretPasswordKey }}{{- else -}}password{{- end -}}
  {{- else if $v.externalNeo4j.existingSecret -}}{{ $v.externalNeo4j.existingSecretPasswordKey }}
  {{- else -}}neo4j-password{{- end -}}
{{- else if eq .ref "s3AccessKeyId" -}}
  {{- if $v.minio.enabled -}}
    {{- if $v.minio.auth.existingSecret -}}{{ $v.minio.auth.existingSecretAccessKeyIdKey }}{{- else -}}access-key-id{{- end -}}
  {{- else if $v.s3.existingSecret -}}{{ $v.s3.existingSecretAccessKeyIdKey }}
  {{- else -}}s3-access-key-id{{- end -}}
{{- else if eq .ref "s3SecretAccessKey" -}}
  {{- if $v.minio.enabled -}}
    {{- if $v.minio.auth.existingSecret -}}{{ $v.minio.auth.existingSecretSecretKeyKey }}{{- else -}}secret-access-key{{- end -}}
  {{- else if $v.s3.existingSecret -}}{{ $v.s3.existingSecretSecretKeyKey }}
  {{- else -}}s3-secret-access-key{{- end -}}
{{- else if eq .ref "authClientSecret" -}}
  {{- if $v.dex.enabled -}}client-secret
  {{- else if $v.auth.existingSecret -}}{{ $v.auth.existingSecretClientSecretKey }}
  {{- else -}}auth-oidc-client-secret{{- end -}}
{{- else if eq .ref "llm" -}}
  {{- if $v.llm.existingSecret -}}{{ $v.llm.existingSecretApiKeyKey }}{{- else -}}llm-api-key{{- end -}}
{{- else if eq .ref "githubToken" -}}
  {{- if $v.github.existingSecret -}}{{ $v.github.existingSecretTokenKey }}{{- else -}}github-token{{- end -}}
{{- else if eq .ref "githubAppPrivateKey" -}}
  {{- if $v.github.existingSecret -}}{{ $v.github.existingSecretAppPrivateKeyKey }}{{- else -}}github-app-private-key{{- end -}}
{{- else -}}
{{- fail (printf "unknown secretEnv ref %q" .ref) -}}
{{- end -}}
{{- end -}}

{{- /* Dex's full config. Takes the client secret as a parameter so the
       deployment can hash the config with a placeholder (the generated
       secret differs between renders). */ -}}
{{- define "continuo.dex.config" -}}
{{- $root := .root -}}
issuer: {{ include "continuo.auth.issuerUrl" $root }}
storage:
  type: memory
web:
  http: 0.0.0.0:5556
oauth2:
  skipApprovalScreen: true
  # Lets a user exchange the demo password for an ID token with one curl
  # (grant_type=password), used as an API bearer token by the tutorial.
  passwordConnector: local
expiry:
  # The ui keeps its own server-side session, so this bounds only how long an
  # ID token used as an API bearer stays valid.
  idTokens: 1h
staticClients:
  - id: continuo-ui
    name: Continuo UI
    redirectURIs:
      - {{ printf "%s/auth/callback" ($root.Values.auth.publicUrl | trimSuffix "/") | quote }}
    secret: {{ .clientSecret | quote }}
enablePasswordDB: true
staticPasswords:
  - email: {{ $root.Values.dex.demoUser.email | quote }}
    hash: {{ $root.Values.dex.demoUser.passwordHash | quote }}
    username: {{ $root.Values.dex.demoUser.username | quote }}
    userID: {{ $root.Values.dex.demoUser.userID | quote }}
{{- end -}}

{{/*
The services that serve /metrics on the metrics port: every Go service.
scripts/install-test/assert-metrics.py pins this list to the services whose
source imports pkg/metrics.
*/ -}}
{{- define "continuo.metrics.services" -}}
["state","orchestrator","execution-controller","release-controller","remediation","agent-remediation","agent-chat","dead-letter-controller","egress-proxy"]
{{- end -}}

{{/*
The port every Go service serves /metrics on (default 9464, also when the
metrics block is absent from stored values). Rendering fails when it equals a
service's httpPort or grpcPort, which that service could then not bind, or, with
the egress proxy rendered, the proxy's listen port (3128) or health port (8093).
*/ -}}
{{- define "continuo.metrics.port" -}}
{{- $m := .Values.metrics | default dict -}}
{{- $port := $m.port | default 9464 | int -}}
{{- range $svc := .Values.services -}}
{{- if or (eq (int (default 0 $svc.httpPort)) $port) (eq (int (default 0 $svc.grpcPort)) $port) -}}
{{- fail (printf "metrics.port %d is service %s's httpPort or grpcPort; choose a free port" $port $svc.name) -}}
{{- end -}}
{{- end -}}
{{- if and (eq (include "continuo.egress.proxyEnabled" .) "true") (has $port (list 3128 8093)) -}}
{{- fail (printf "metrics.port %d is egress-proxy's proxy or health port; choose a free port" $port) -}}
{{- end -}}
{{- $port -}}
{{- end -}}

{{/*
The admission cap execution-controller enforces. A release without an
execution block (for example after helm upgrade --reuse-values) gets the
default; a value below 1 fails the render.
*/ -}}
{{- define "continuo.execution.maxConcurrentJobs" -}}
{{- $e := .Values.execution | default dict -}}
{{- $n := 50 -}}
{{- if hasKey $e "maxConcurrentJobs" -}}{{- $n = int $e.maxConcurrentJobs -}}{{- end -}}
{{- if lt $n 1 -}}
{{- fail (printf "execution.maxConcurrentJobs=%v must be a whole number of at least 1" $e.maxConcurrentJobs) -}}
{{- end -}}
{{- $n -}}
{{- end -}}

{{/*
JOB_START_TIMEOUT_SECONDS for the shared ConfigMap: execution.startTimeoutMinutes
in seconds. A release without an execution block, or without the key (helm
upgrade --reuse-values), gets 15 minutes; a value outside
1-120 fails the render.
*/ -}}
{{- define "continuo.execution.startTimeoutSeconds" -}}
{{- $e := .Values.execution | default dict -}}
{{- $m := 15 -}}
{{- if hasKey $e "startTimeoutMinutes" -}}{{- $m = int $e.startTimeoutMinutes -}}{{- end -}}
{{- if or (lt $m 1) (gt $m 120) -}}
{{- fail (printf "execution.startTimeoutMinutes=%v must be a whole number of minutes from 1 to 120" $e.startTimeoutMinutes) -}}
{{- end -}}
{{- mul $m 60 -}}
{{- end -}}

{{/*
K8S_CLIENT_QPS for the shared ConfigMap: execution.kubeClientQPS. A release
without an execution block, or without the key (helm upgrade --reuse-values
from an older chart), gets 50; a value outside 1-1000 fails the render.
*/ -}}
{{- define "continuo.execution.kubeClientQPS" -}}
{{- $e := .Values.execution | default dict -}}
{{- $q := 50 -}}
{{- if hasKey $e "kubeClientQPS" -}}{{- $q = int $e.kubeClientQPS -}}{{- end -}}
{{- if or (lt $q 1) (gt $q 1000) -}}
{{- fail (printf "execution.kubeClientQPS=%v must be a whole number from 1 to 1000" $e.kubeClientQPS) -}}
{{- end -}}
{{- $q -}}
{{- end -}}

{{/*
K8S_CLIENT_BURST for the shared ConfigMap: execution.kubeClientBurst. A
release without an execution block, or without the key, gets 100; a value
outside 1-2000, or below the QPS, fails the render.
*/ -}}
{{- define "continuo.execution.kubeClientBurst" -}}
{{- $e := .Values.execution | default dict -}}
{{- $b := 100 -}}
{{- if hasKey $e "kubeClientBurst" -}}{{- $b = int $e.kubeClientBurst -}}{{- end -}}
{{- if or (lt $b 1) (gt $b 2000) -}}
{{- fail (printf "execution.kubeClientBurst=%v must be a whole number from 1 to 2000" $e.kubeClientBurst) -}}
{{- end -}}
{{- if lt $b (int (include "continuo.execution.kubeClientQPS" .)) -}}
{{- fail (printf "execution.kubeClientBurst=%v must be at least execution.kubeClientQPS=%v" $b (include "continuo.execution.kubeClientQPS" .)) -}}
{{- end -}}
{{- $b -}}
{{- end -}}

{{/*
TASK_MAX_RETRIES for the shared ConfigMap: runs.taskMaxRetries. A release
without a runs block, or without the key (for example after helm upgrade
--reuse-values), gets 2; a value outside 0-10 fails the render.
*/ -}}
{{- define "continuo.runs.taskMaxRetries" -}}
{{- $r := .Values.runs | default dict -}}
{{- $n := 2 -}}
{{- if hasKey $r "taskMaxRetries" -}}{{- $n = int $r.taskMaxRetries -}}{{- end -}}
{{- if or (lt $n 0) (gt $n 10) -}}
{{- fail (printf "runs.taskMaxRetries=%v must be a whole number from 0 to 10" $r.taskMaxRetries) -}}
{{- end -}}
{{- $n -}}
{{- end -}}

{{/*
TASK_TIMEOUT_SECONDS for the shared ConfigMap: runs.taskTimeoutHours in
seconds. A release without a runs block, or without the key, gets 24 hours; a
value outside 1-168 fails the render.
*/ -}}
{{- define "continuo.runs.taskTimeoutSeconds" -}}
{{- $r := .Values.runs | default dict -}}
{{- $h := 24 -}}
{{- if hasKey $r "taskTimeoutHours" -}}{{- $h = int $r.taskTimeoutHours -}}{{- end -}}
{{- if or (lt $h 1) (gt $h 168) -}}
{{- fail (printf "runs.taskTimeoutHours=%v must be a whole number of hours from 1 to 168" $r.taskTimeoutHours) -}}
{{- end -}}
{{- mul $h 3600 -}}
{{- end -}}

{{/*
MAINTENANCE_ENABLED for the shared ConfigMap: "true" or "false". A release
without a maintenance block (for example after helm upgrade --reuse-values)
renders "false"; the schema refuses a non-boolean value.
*/ -}}
{{- define "continuo.maintenance.enabled" -}}
{{- $m := .Values.maintenance | default dict -}}
{{- if and (hasKey $m "enabled") $m.enabled -}}true{{- else -}}false{{- end -}}
{{- end -}}

{{/*
Whether the metrics pods carry prometheus.io annotations: true unless
metrics.podAnnotations is set to false.
*/ -}}
{{- define "continuo.metrics.podAnnotations" -}}
{{- $m := .Values.metrics | default dict -}}
{{- if hasKey $m "podAnnotations" -}}{{ $m.podAnnotations }}{{- else -}}true{{- end -}}
{{- end -}}
