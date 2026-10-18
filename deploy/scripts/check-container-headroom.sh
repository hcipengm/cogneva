#!/usr/bin/env bash
# 容器内存余量：判「声明的上限」还装不装得下「这个容器实际要过的量」。
#
# 为什么需要它：资源面上所有读数都在判**集群总量**——声明之和装不装得进配额、配额
# 装不装得进节点、节点上的不可回收内存有没有吃掉页缓存的底线。这些加起来可以全绿，
# 而某一个容器的上限比它自己的数据还小。redis 那次就是这样：它的上限写死在模板里、
# 数据是 485MB、上限是 512Mi，集群记账一直绿，直到它每 20 秒被杀一次。
#
# 所以这条读数判两件事，都只在**单个容器**上有意义：
#
#   1. 峰值不可回收用量对声明上限。用 RSS 而不是 working_set：working_set 含页缓存，
#      而页缓存是内核按需回收的——一个写了很多文件的构建 Pod 会以 90% 的 working_set
#      长期存在却永远不会被杀，那不是余量不足。RSS 是匿名页与共享内存，逼近上限时
#      内核没有可回收的东西可以让。窗口内的**峰值**而不是当期值：被杀掉的那一瞬间
#      恰恰是读数抓不到的一拍，而 `max_over_time` 抓得到（redis 那次同一个 Pod 反复
#      重启，峰值一直贴在上限上）。
#
#   2. 窗口内有没有 OOMKilled 终止。这一条不需要阈值猜：被杀过就是被杀过。它和判据 1
#      分开记，因为两者的修法不同——判据 2 红说明已经付过代价，判据 1 红说明正在逼近。
#      陈旧的终止记录不算：终止时刻落在窗口之外就是历史，不是当前状态。
#
# 上游读数读不到与不成立是两种退出码。Prometheus 里整族指标为空（cAdvisor 没采到、
# namespace 写错、kube-state-metrics 没起）报 2，不报绿：没有序列的容器在这条读数里
# 与「余量充足」长得一模一样，而它们要修的地方完全不同。
#
# 用法：bash deploy/scripts/check-container-headroom.sh [--record]
# 退出码：0 全部有余量且窗口内无 OOM；1 有越界（逐条点名）；2 判不了。
#
# `--record` 把这一拍追加成序列文件的一行（默认
# `${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/container-headroom.tsv`）：
#   时刻 <TAB> 结局 <TAB> 最高比值 <TAB> 判据1 命中数 <TAB> 判据2 命中数 <TAB> 判过的容器数
# 判红与读不到也各写一行：只在绿的时候写，序列上的洞就分不开「没跑」与「跑了但红了」。
#
# 环境：
#   COGNEVA_KUBECTL            默认 "sudo -n k3s kubectl"（只用来发现 Prometheus 的 Service）
#   COGNEVA_HEADROOM_NS        默认 cogneva
#   COGNEVA_HEADROOM_RATIO     峰值对上限的红线，默认 0.85（含等于）
#   COGNEVA_HEADROOM_WINDOW    窗口，Prometheus 时长语法，默认 24h
#   COGNEVA_PROMETHEUS_URL     默认按 svc 发现
#   COGNEVA_PROM_QUERY_CMD     测试注入点：给它一个可执行文件，参数是 PromQL，
#                              标准输出当作 /api/v1/query 的响应体，不再走 curl
#   COGNEVA_HEADROOM_SERIES    序列文件路径（`--record` 时写）
set -euo pipefail

KUBECTL="${COGNEVA_KUBECTL:-sudo -n k3s kubectl}"
NS="${COGNEVA_HEADROOM_NS:-cogneva}"
RATIO="${COGNEVA_HEADROOM_RATIO:-0.85}"
WINDOW="${COGNEVA_HEADROOM_WINDOW:-24h}"
SERIES="${COGNEVA_HEADROOM_SERIES:-${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/container-headroom.tsv}"
record=0
for arg in "$@"; do
    case "$arg" in
        --record) record=1 ;;
        *) printf 'CONTAINER-HEADROOM: 不认识的参数：%s（只有 --record）\n' "$arg" >&2; exit 2 ;;
    esac
done

# 写不进序列时只告警、不改判词：这一步是读数，不是判据；但它必须留痕，
# 否则「文件没长」与「这一拍没跑」在读数面上同形。
series_append() { # <结局> <最高比值> <越界数> <oom数> <容器数>
    [ "${record}" = 1 ] || return 0
    mkdir -p -- "$(dirname -- "${SERIES}")" 2>/dev/null \
        || { printf 'CONTAINER-HEADROOM: 建不出序列目录，这一拍没有留下读数：%s\n' "${SERIES}" >&2; return 0; }
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$@" >>"${SERIES}" 2>/dev/null \
        || printf 'CONTAINER-HEADROOM: 序列写不进去，这一拍没有留下读数：%s\n' "${SERIES}" >&2
}

die() { series_append unreadable - - - -; printf 'CONTAINER-HEADROOM: %s\n' "$*" >&2; exit 2; }

command -v python3 >/dev/null || die "缺少依赖：python3，判不了容器余量"

# 窗口同时要交给 PromQL（[24h]）和一个 Python 里的秒数比较；只认**单一单位**的
# 时长，别让一个写错的值同时变成「查询语法错」与「窗口无限大」。
case "${WINDOW}" in
    ''|*[!0-9smhd]*) die "COGNEVA_HEADROOM_WINDOW 只认单一单位的时长（如 24h）：${WINDOW}" ;;
esac
case "${WINDOW}" in
    [0-9]*[smhd]) ;;
    *) die "COGNEVA_HEADROOM_WINDOW 缺单位（s/m/h/d）：${WINDOW}" ;;
esac
case "${RATIO}" in
    ''|*[!0-9.]*|*.*.*|.*|*.) die "COGNEVA_HEADROOM_RATIO 不是小数（如 0.85）：${RATIO}" ;;
esac
python3 - "${RATIO}" <<'PY' || die "COGNEVA_HEADROOM_RATIO 必须在 (0,1] 内：${RATIO}"
import sys
r = float(sys.argv[1])
sys.exit(0 if 0 < r <= 1 else 1)
PY

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ---- Prometheus 端点 ----
PROM_URL="${COGNEVA_PROMETHEUS_URL:-}"
if [ -z "${COGNEVA_PROM_QUERY_CMD:-}" ] && [ -z "${PROM_URL}" ]; then
    ip="$(${KUBECTL} -n monitoring get svc cog-observability-prometheus \
            -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)"
    [ -n "${ip}" ] || die "找不到 Prometheus 的 Service，也给不出 COGNEVA_PROMETHEUS_URL"
    PROM_URL="http://${ip}:9090"
fi
PROM_URL="${PROM_URL%/}"

prom() { # prom <promql> <输出文件>
    if [ -n "${COGNEVA_PROM_QUERY_CMD:-}" ]; then
        "${COGNEVA_PROM_QUERY_CMD}" "$1" >"$2" || die "注入的 Prometheus 查询失败：$1"
        return
    fi
    curl -sS --max-time 15 --data-urlencode "query=$1" "${PROM_URL}/api/v1/query" >"$2" \
        || die "Prometheus 查询失败：$1"
}

# RSS 的同一 (pod, container) 会留下多条历史序列（每次重启换一个容器 id），
# 直接求和会把同一块内存数多遍；`max by` 才是「这个名字的容器最多用到过多少」。
prom "max by (pod, container) (max_over_time(container_memory_rss{namespace=\"${NS}\",container!=\"\",container!=\"POD\"}[${WINDOW}]))" \
    "${work}/rss.json"
prom "max by (pod, container) (kube_pod_container_resource_limits{namespace=\"${NS}\",resource=\"memory\"})" \
    "${work}/limit.json"
# 窗口内的 RSS 会把**已经删掉的 Pod** 一起带回来（range 选择器只要窗口里有样本就算，
# 不受 instant 查询的 5 分钟回看限制）。不按「此刻存在的 Pod」过滤的话，它们会因为没有
# 上限序列被读成「无内存上限的容器」——那是把历史读成了现状。
prom "max by (pod) (kube_pod_info{namespace=\"${NS}\"})" "${work}/pods.json"
prom "kube_pod_container_status_last_terminated_reason{namespace=\"${NS}\",reason=\"OOMKilled\"}" \
    "${work}/oom_reason.json"
# 终止时刻与原因在两条不同的指标上（reason 那条不带时刻），只能按 (pod, container) 在
# 判词这一侧对起来；时刻本身在这里先算成「距今多少秒」。
prom "time() - kube_pod_container_status_last_terminated_timestamp{namespace=\"${NS}\"}" \
    "${work}/oom_age.json"

if [ "${record}" = 1 ]; then
    export CONTAINER_HEADROOM_RECORD_FILE="${SERIES}"
fi
export CONTAINER_HEADROOM_RATIO="${RATIO}"
export CONTAINER_HEADROOM_WINDOW="${WINDOW}"

python3 - "${work}" "${NS}" "${WINDOW}" <<'PYEOF'
import json
import os
import re
import sys
import time

work, ns, window = sys.argv[1], sys.argv[2], sys.argv[3]
ratio_limit = float(os.environ['CONTAINER_HEADROOM_RATIO'])

_UNITS = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}


def window_seconds(text):
    m = re.fullmatch(r'(\d+)([smhd])', text)
    return int(m.group(1)) * _UNITS[m.group(2)]


def read(name):
    with open(os.path.join(work, name), encoding='utf-8') as f:
        return json.load(f)


def vector(name):
    doc = read(name)
    if doc.get('status') != 'success':
        raise SystemExit(f"CONTAINER-HEADROOM: Prometheus 返回 {doc.get('status')!r}：{name}")
    out = {}
    for s in doc['data']['result']:
        m = s['metric']
        out[(m.get('pod', ''), m.get('container', ''))] = float(s['value'][1])
    return out


def record(verdict, max_ratio='-', over='-', oom='-', judged='-'):
    path = os.environ.get('CONTAINER_HEADROOM_RECORD_FILE')
    if not path:
        return
    row = '\t'.join([str(int(time.time())), verdict,
                     '-' if max_ratio == '-' else f"{max_ratio:.4f}", over, oom, judged])
    try:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(row + '\n')
    except OSError as e:
        print(f"CONTAINER-HEADROOM: 序列写不进去，这一拍没有留下读数：{e}", file=sys.stderr)


def gib(n):
    return f"{n / 2 ** 30:.2f}Gi"


def name(pod, container):
    return f"{pod}/{container}"


rss = vector('rss.json')
limits = vector('limit.json')
pods = vector('pods.json')
oom_reason = vector('oom_reason.json')
oom_age = vector('oom_age.json')

# 整族为空不是「都余量充足」：cAdvisor 没采到 / namespace 写错 / KSM 没起，在这里
# 与一个空 namespace 同形，而它们要修的地方完全不同。
if not rss:
    record('unreadable')
    raise SystemExit(f"CONTAINER-HEADROOM: 判不了：namespace {ns} 下没有 container_memory_rss 序列"
                     "（cAdvisor 没采到、namespace 写错、或窗口外没有样本）")
if not pods:
    record('unreadable')
    raise SystemExit(f"CONTAINER-HEADROOM: 判不了：namespace {ns} 下没有 kube_pod_info 序列"
                     "（kube-state-metrics 没起，分不出「此刻存在的 Pod」与窗口里的历史）")
# 窗口里留下样本、但此刻已经不存在的 Pod 不参与判定：它们的上限序列也早没了，
# 不滤掉就会被读成「无内存上限的容器」。
live = {k[0] for k in pods}
rss = {k: v for k, v in rss.items() if k[0] in live}
if not rss:
    record('unreadable')
    raise SystemExit(f"CONTAINER-HEADROOM: 判不了：namespace {ns} 下此刻存在的 Pod 里没有容器的"
                     " RSS 序列（cAdvisor 没采到它们）")
if not limits:
    record('unreadable')
    raise SystemExit(f"CONTAINER-HEADROOM: 判不了：namespace {ns} 下没有 kube_pod_container_resource_limits"
                     "（kube-state-metrics 没起，或者所有容器都没写内存上限——这两种要修的地方不同，先查前者）")

secs = window_seconds(window)
problems = []
ratio_hits = 0
oom_hits = 0
unlimited = []
judged = 0
max_ratio = None
max_name = None
top = []

for key in sorted(rss):
    peak = rss[key]
    limit = limits.get(key)
    if limit is None:
        # 没有上限的容器不会被自己的上限杀，但它会去吃节点的那一份；列出来，不判红。
        unlimited.append(key)
        continue
    judged += 1
    if limit <= 0:
        continue
    r = peak / limit
    if max_ratio is None or r > max_ratio:
        max_ratio, max_name = r, key
    top.append((r, key, peak, limit))
    if r >= ratio_limit:
        ratio_hits += 1
        problems.append(
            f"判据1 {name(*key)}：窗口内峰值 RSS {gib(peak)} 对上上限 {gib(limit)}"
            f"（{r:.2f} ≥ {ratio_limit:.2f}）——下一次再要这么多，内核没有可回收的东西可让")

# 判据 2：终止原因要与终止时刻对得上，且时刻落在窗口内。陈旧的 OOM 记录是历史。
for key, v in sorted(oom_reason.items()):
    if v != 1:
        continue
    age = oom_age.get(key)
    if age is None:
        # 有原因没有时刻：这不是「没 OOM」，是这一对读不齐，不能当绿。
        oom_hits += 1
        problems.append(f"判据2 {name(*key)}：记着 OOMKilled，但没有 last_terminated_timestamp，"
                        "判不了它发生在不在窗口内")
        continue
    if age <= secs:
        oom_hits += 1
        problems.append(
            f"判据2 {name(*key)}：{int(age)} 秒前被 OOMKilled——已经付过代价，不是正在逼近")

top.sort(reverse=True)
head = []
for r, key, peak, limit in top[:3]:
    head.append(f"{name(*key)} {r:.2f}（{gib(peak)} / {gib(limit)}）")
if not head:
    head.append('（没有带内存上限的容器）')

unlimited_note = ''
if unlimited:
    unlimited_note = (f"\n  无内存上限的容器 {len(unlimited)} 个："
                      + "、".join(name(*k) for k in unlimited)
                      + "——它们不会被自己的上限杀，但会去争节点的那一份")

if problems:
    record('broken', max_ratio if max_ratio is not None else '-',
           str(ratio_hits), str(oom_hits), str(judged))
    print("CONTAINER-HEADROOM 红：")
    for p in problems:
        print(f"  {p}")
    print(f"  窗口 {window}、红线 {ratio_limit:.2f}；判过 {judged} 个带上限的容器"
          f"{unlimited_note}")
    print(f"  最高三笔：{'；'.join(head)}")
    sys.exit(1)

record('ok', max_ratio if max_ratio is not None else '-', '0', '0', str(judged))
print(f"CONTAINER-HEADROOM 绿：判过 {judged} 个带上限的容器，窗口 {window}、红线 {ratio_limit:.2f}"
      f"{unlimited_note}")
print(f"  最高三笔：{'；'.join(head)}")
print("  读法：峰值 RSS 问的是「这段时间里它最多要过多少」，不是此刻；"
      "与上限之间留的是被杀掉的那一瞬间抓不到的那一段。")
PYEOF
