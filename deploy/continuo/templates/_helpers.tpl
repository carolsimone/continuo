{{- define "continuo.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
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
     the same fail-closed treatment as validation.engine above: v0.1.x-v0.3.x
     are known runner releases that predate python-csv support (added in
     v0.4.0) — that runner ignores the "kind"/"csv_source" contract fields
     manifest-controller already emits for a python-csv node and reports
     success without checking the file's header, so a csv node with a
     mismatched header would promote unvalidated. A tag that isn't even
     shaped like "vX.Y.Z" (optionally "@sha256:<digest>") can't be compared
     against that known-bad range at all, so it fails closed the same way
     rather than rendering a chart that may silently skip csv validation. */}}
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
{{- fail (printf "validation.imageTag=%q predates python-csv validation support (added in v0.4.0): manifest-controller already accepts \"kind: python-csv\" nodes and emits csv_source, but this runner ignores that field and reports success without checking the file's header, so a csv node with a mismatched header would promote unvalidated. Re-pin validation.imageTag to \"v0.4.0\" or later, or drop the override to track the chart's default." $tag) -}}
{{- end -}}
{{- else -}}
{{- $tag = "v0.4.0" -}}{{/* CONTINUO_VALIDATION_DEFAULT_TAG — must equal values.yaml's validation.imageTag default */}}
{{- end -}}
{{- if .Values.global.imageRegistry -}}
{{- printf "%s/%s/continuo-python-runtime-%s:%s" .Values.global.imageRegistry .Values.global.imageRepositoryPrefix $eng $tag -}}
{{- else -}}
{{- printf "%s/continuo-python-runtime-%s:%s" .Values.global.imageRepositoryPrefix $eng $tag -}}
{{- end -}}
{{- end -}}

{{/* Name of the validation warehouse Secret the executor attaches via envFrom.
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

{{- define "continuo.schedulesConfigMapName" -}}
{{- printf "%s-schedules" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
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
{{- define "continuo.s3.credentialServices" -}}
["orchestrator","manifest-controller","k8s-controller","executor-controller","release-controller","remediation","remediation-agent","ui-service","agent-runner"]
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
