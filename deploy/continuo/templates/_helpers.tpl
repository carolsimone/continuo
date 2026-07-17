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
