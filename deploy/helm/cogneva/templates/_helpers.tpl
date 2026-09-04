{{/* 通用标签与命名助手 */}}
{{- define "cogneva.labels" -}}
app.kubernetes.io/name: cogneva
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "cogneva.namespace" -}}
{{ .Values.global.namespace }}
{{- end -}}

{{- define "cogneva.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end -}}
