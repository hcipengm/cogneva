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

{{/*
层库清单，渲染成 COGNEVA_IMAGE_STORE_ROOTS 的 `name=mountPath,name=mountPath`。

containerd 那一条恒在（它就是 `imageStoreMountPath`），其余来自
`claimStore.extraImageStores`。名字是序列身份的一半：两个库用同一个名字，两条
不同目录的读数会合成一条序列，所以 values 里的名字是按库取的，不是按位置取的。
*/}}
{{- define "cogneva.imageStores" -}}
{{- $pairs := list (printf "containerd=%s" .Values.claimStore.imageStoreMountPath) -}}
{{- range .Values.claimStore.extraImageStores -}}
{{- $pairs = append $pairs (printf "%s=%s" .name .mountPath) -}}
{{- end -}}
{{- join "," $pairs -}}
{{- end -}}

{{/*
日志后端的两行环境变量，给**不挂 `cogneva-json`** 的工作负载用。

主应用与进化进程从 files/cogneva.json 的 observability 段读同一组值（app-config.yaml
在渲染时把这里的值改写成那边的一份）；节点侧守护进程与一次性 Job 不挂那份配置，
它们的订阅者只从环境读。两处都指向 `observability.loki`，地址因此仍只有一份。

为什么这些工作负载也要接上去：它们的日志此前只写容器 stdout，容器一删就没了，
而「这个 Job 为什么失败」「这台节点的走查为什么起不来」正需要那段日志——留下
的退出码说不出它停在哪一步。
*/}}
{{/*
容器的民用时区。

不带它就退回镜像默认的 UTC：`kubectl logs` 每行印 UTC 时刻、结尾一个 `Z`，而读它
的人墙上是 +08——同一件事两个时刻，差额只存在于人脑子里；按本机日切的文件名也跟着
偏。时间是部署的属性、不是代码里的常量，所以它来自 values：一处改，所有工作负载一起改。

库不跟这一项走。分区边界（`FOR VALUES FROM` 的字面量按会话时区解释）与额度日界都
是按 UTC 定义的判定，改库的时区改变的是判定本身，不只是显示。
*/}}
{{- define "cogneva.timeEnv" -}}
- name: TZ
  value: {{ .Values.global.timezone | quote }}
{{- end -}}

{{- define "cogneva.logEnv" -}}
- name: COGNEVA_LOKI_ENABLED
  value: {{ .Values.observability.loki.enabled | quote }}
- name: COGNEVA_LOKI_ENDPOINT
  value: {{ .Values.observability.loki.endpoint | quote }}
{{- end -}}

{{/*
日志推手要连的端口：监控栈 Loki 的 push 端点。地址在
`.Values.observability.loki.endpoint` 里（URL 自带这个数），而 egress 白名单要的是
端口本身。两个工作负载各写一遍时，换掉端点端口只会改到其中几处：推送被策略静默丢弃，
日志里只剩反复重试的 warn，而「被丢掉的日志」与「本来就没有日志」从外面看一样。
*/}}
{{- define "cogneva.lokiPushPort" -}}
{{- /* lit: 3100 fixed — loki's own listen port on the operator-provided monitoring endpoint; the endpoint's URL carries the same number, and no chart workload renames it */ -}}
3100
{{- end -}}

{{/*
网页检索与渲染后端的地址：`webSearch.base` / `webFetch.base` 留空时按 port 与
Service 名派生。

地址与容器端口分两处写时，改端口的那个部署会拿到一条指向旧端口的地址，症状是
查询/抓取超时——读起来像后端挂了，真因是两个数已经分家。所以缺省不写字面量，
端口只出现在 values 的 `port` 一处。
*/}}
{{- define "cogneva.searxngBase" -}}
{{- if .Values.webSearch.base -}}
{{- .Values.webSearch.base -}}
{{- else -}}
{{- printf "http://searxng:%d" (int .Values.webSearch.port) -}}
{{- end -}}
{{- end -}}

{{- define "cogneva.crawl4aiBase" -}}
{{- if .Values.webFetch.base -}}
{{- .Values.webFetch.base -}}
{{- else -}}
{{- printf "http://crawl4ai:%d" (int .Values.webFetch.port) -}}
{{- end -}}
{{- end -}}
