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

{{/*
读配置的 Pod 模板带的指纹（checksum/config）。

必须把两份配置模板一起哈希。`app-config.yaml` 渲染出卷里的 cogneva-json /
cogneva-prompts / cogneva-evolution-config，`configmap.yaml` 渲染出 env 那份
cogneva-config——所有通知与告警出口的地址都在这里。只哈希前者时，运维在 values 里
配一个 `notification.webhookUrl` 再 `helm upgrade`：ConfigMap 变了、Pod 模板没变，
Deployment 不滚动，进程仍按启动时读到的那份 env 跑，出口永远挂不上，而升级读数全绿。

env 与卷里的文件是同一个失败形状——两者都只在进程启动时被读一次——所以是同一个指纹。
*/}}
{{- define "cogneva.configChecksum" -}}
{{- $app := include (print $.Template.BasePath "/app-config.yaml") . -}}
{{- $env := include (print $.Template.BasePath "/configmap.yaml") . -}}
{{- printf "%s\n%s" $app $env | sha256sum -}}
{{- end -}}
