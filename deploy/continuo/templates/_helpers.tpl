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

{{/* Migration Job name: revision-suffixed when the bundled Postgres makes it a
     regular resource (hooks would deadlock); stable when it is a hook (BYO). */}}
{{- define "continuo.migrateJobName" -}}
{{- if .Values.postgresql.enabled -}}
{{- printf "%s-db-init-migrate-r%d" (include "continuo.fullname" .) (int .Release.Revision) | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-db-init-migrate" (include "continuo.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
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
