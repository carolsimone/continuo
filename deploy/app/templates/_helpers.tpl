{{- define "continuo-app.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- include "continuo-app.name" . -}}
{{- end -}}
{{- end -}}

{{- define "continuo-app.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.labels" -}}
helm.sh/chart: {{ include "continuo-app.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: {{ include "continuo-app.name" . }}
{{- end -}}

{{- define "continuo-app.selectorLabels" -}}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/name: {{ .service }}
{{- end -}}

{{- define "continuo-app.configMapName" -}}
{{- printf "%s-config" (include "continuo-app.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.credentialsSecretName" -}}
{{- printf "%s-credentials" (include "continuo-app.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.schedulesConfigMapName" -}}
{{- printf "%s-schedules" (include "continuo-app.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.migrationsJobName" -}}
{{- printf "%s-db-migrate" (include "continuo-app.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "continuo-app.cancelConfigMapName" -}}
{{- printf "%s-cancel-config" (include "continuo-app.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
