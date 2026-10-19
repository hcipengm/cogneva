#!/usr/bin/env bash
# 跨层对账：把资源账的三层摆在同一行上，判它们之间还成不成立。
#
# 为什么需要它：三层各自都有读数，但**没有任何一条读数同时读三层**。声明层的不变式
# 在渲染期查（工作负载之和装得进自己的配额），准入层的偏离有 live 对象与漂移读数，
# 物理层的 anon/file 有 DaemonSet 在报。三条腿各自绿，机器仍然可以卡死——只要
# 「集群记账的总量」已经超过「节点真能给出的量」，或者「不可回收的内存」已经吃掉
# 了「页缓存该占的那一份」。这两种越界都不在任何一层的读数里，因为它们都是**两个
# 层的差**。
#
# 判的四条。前三条都是差，不是某一层自己的性质；第四条是投影与正本的相等——
#
#   1. 准入 ≤ 容量：`Σrequests/limits`（集群自己在配额 used 里记的数）不超过节点
#      allocatable。超过 ⇒ 声明装得下配额只是配额比机器大，调度会以 Pending 的
#      形式逐 Pod 失败，而配额读数一直是绿的。
#   2. 不可回收 ＋ 页缓存底 ≤ 总量：每个节点上 `Σanon` 加上**该节点 file 的 7 天最小**
#      仍小于 MemTotal。file 是内核按需回收的，所以「file 高」不是信号；但「anon
#      涨到 file 必须让位」是信号——让位的那一刻起，每次分配都要先回收，PSI 的
#      full 会开始计秒。这里不拿 file 当期值去比（它是一个平台期，当期值高说明
#      缓存多、机器健康），而是拿它**7 天里最低到过哪**：那个位置是内核实际被
#      压过的证据，把它当作页缓存不能失守的底线。
#   3. 准入余量 ≥ 部署器下一次滚动 Job：`hard - used`（limits 两维）必须装得下
#      部署器要建的那个 Job 的声明上限（从 live 的 evolution ConfigMap 读）。`used`
#      记的是**所有非终态 Pod**，包括渲染期清单之外、运行时按需建的那些（评测表、
#      验收演练）；渲染期门禁只把声明的工作负载加进它的和，所以它能一路绿着而准入
#      已经在拒人。这一条就是把那个差摆出来。
#   4. live 的治理对象 == 树内声明的那一份：ResourceQuota 的 `spec.hard` 与
#      LimitRange 的上下限逐字段相等，声明里 `cogneva.io/*` 的注解在 live 上同名同值。
#      这一条不是「装得下」，是「集群上跑的到底是不是这棵树」——前三条可以在
#      一只从未 apply 过本树、但数值刚好装得下的集群上全绿。2026-10-19 的标本：
#      树内已抬到 52Gi，live 还停在 46Gi，没有任何读数说得出，是人翻对象翻出来的。
#
# 两条都必须并列 PSI 的 `full`（不是只看 `some`）。anon 之和与 MemAvailable 回答的是
# 「还装得下多少不可回收的」，PSI 回答的是「这一刻有没有人真的被挡住」；两者可以
# 同时指向相反结论，而不一致本身就是结论——说明这个窗口的压力不在内存这条线上，
# 得去 io 与回写那边找。所以 PSI 只印进判词、不参与判红。
#
# 读不到与不成立是两种退出码。某一层读不到（kubectl 连不上、Prometheus 没有该节点的
# 序列、7 天窗口里没有 file 样本）⇒ 2。把「没人看」写成「一致」是这条读数最容易犯的错：
# 一个 DaemonSet 在某个节点没起来，读出来是**没有序列**而不是 0。
#
# 用法：bash deploy/scripts/check-cross-layer-reconcile.sh [--record] [--converge]
# 退出码：0 成立；1 有不成立（逐条点名层与不等式）；2 判不了（某层读不到）。
#
# `--converge` 把判据 4 从「报差」变成「填差」：当 live 的治理对象与树内渲染产物
# 不一致时，把树内那一份 apply 上去，再重读一次并按重读后的结果判决。默认只读，
# 不写集群。
#
# 为什么这条差由读数自己填：数值的正本已经是树（渲染产物是提交物，算式在 chart
# values 里），live 与它不一致只说明投影没跑，动作本身由声明唯一确定；把同一件事
# 再表述一遍，只会多一个能把读数读错的地方。
#
# 两个方向都收敛：live 低于树（声明改了、没人 apply）与 live 高于树（有人手工在集群上
# 抬过）都是「live 没有兑现声明」。后者的错法不是「多给了资源」，而是下一次声明落地时
# 没人知道该以哪一份为准——所以它同样要扶正，而不是留一条红给属主去手工追平。
#
# 自动 apply 的界只有一条，但它是测量的：**apply 之后仍装得下此刻的用量才 apply**。
#   - ResourceQuota：live.status.used 每一项都不得大于树内 hard 的同名项，且留得住判据 3
#     的余量（used + 部署器滚动 Job ≤ 树内 hard）——否则 apply 之后下一次滚动在准入被拒；
#   - LimitRange：正在跑的每个容器都要落在树内 max/min 之内——上限调低会拒绝下一次
#     滚动，下限调高会拒绝资源声明低于它的新 Pod；
#   - 读不到 live 的 Pods、或本拍有任何一条判据读不到，就不动：判不了与「装得下」不是
#     一件事，而写集群的动作只接受读得全的判据。
# 装不下时不 apply，把不等式原样报出来（退出码 1）——那是声明本身与用量矛盾，需要
# 改的是声明。
#
# `--record` 把这一拍追加成序列文件的一行（默认
# `${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/cross-layer-reconcile.tsv`）：
#   时刻 <TAB> 结局 <TAB> used_memory <TAB> allocatable_memory <TAB> Σanon <TAB>
#   file 7d 最小 <TAB> MemTotal        （数值一律字节，读不到记 `-`）
# 结局取 ok / converged / broken / unreadable（converged：这一拍按树内声明填了判据 4
# 的差，写过集群）。**判红与读不到也各写一行**：只在绿的时候写，
# 序列上的空洞就同时代表「没跑」与「跑了但红了」，而这两件事要修的地方不同。
# 有了这份序列，「昨天 14:00 的 allocatable 是多少」是读文件，不是重跑一次。
#
# 环境：
#   COGNEVA_KUBECTL          默认 "sudo -n k3s kubectl"
#   COGNEVA_PROMETHEUS_URL   默认按 svc 发现；测试用它指向一个假端点
#   COGNEVA_PROM_QUERY_CMD   测试注入点：给它一个可执行文件，参数是 PromQL，
#                            标准输出当作 /api/v1/query 的响应体，不再走 curl
#   COGNEVA_RECONCILE_NS     默认 cogneva
#   COGNEVA_RECONCILE_PROFILE     判据 4 读哪一份渲染产物，默认 k3s-single
#   COGNEVA_RECONCILE_DECLARED    直接指定渲染产物目录（覆盖 PROFILE）
#   COGNEVA_RECONCILE_SERIES 序列文件路径（`--record` 时写）
set -euo pipefail

KUBECTL="${COGNEVA_KUBECTL:-sudo -n k3s kubectl}"
NS="${COGNEVA_RECONCILE_NS:-cogneva}"
QUOTA_NAME="${COGNEVA_RECONCILE_QUOTA:-cogneva-quota}"
SERIES="${COGNEVA_RECONCILE_SERIES:-${XDG_STATE_HOME:-$HOME/.local/state}/cogneva/cross-layer-reconcile.tsv}"
record=0
converge=0
for arg in "$@"; do
    case "$arg" in
        --record) record=1 ;;
        --converge) converge=1 ;;
        *) printf 'CROSS-LAYER: 不认识的参数：%s（只有 --record / --converge）\n' "$arg" >&2; exit 2 ;;
    esac
done

# 写不进序列时只告警、不改判词：这一步是读数，不是判据；但它必须留痕，
# 否则「文件没长」与「这一拍没跑」在读数面上同形。
series_append() { # <结局> <used_mem> <alloc_mem> <Σanon> <file_min> <MemTotal>
    [ "${record}" = 1 ] || return 0
    mkdir -p -- "$(dirname -- "${SERIES}")" 2>/dev/null \
        || { printf 'CROSS-LAYER: 建不出序列目录，这一拍没有留下读数：%s\n' "${SERIES}" >&2; return 0; }
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date +%s)" "$@" >>"${SERIES}" 2>/dev/null \
        || printf 'CROSS-LAYER: 序列写不进去，这一拍没有留下读数：%s\n' "${SERIES}" >&2
}

# 读不到任何一层时也记一行：这一拍确实跑了，只是它没判成。
die() { series_append unreadable - - - - -; printf 'CROSS-LAYER: %s\n' "$*" >&2; exit 2; }

command -v python3 >/dev/null || die "缺少依赖：python3，判不了三层对账"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# ---- 层 2：live 准入面 ----
${KUBECTL} -n "${NS}" get resourcequota "${QUOTA_NAME}" -o json >"${work}/quota.json" 2>"${work}/quota.err" \
    || die "读不到 resourcequota/${QUOTA_NAME}：$(head -c 400 "${work}/quota.err")"
${KUBECTL} get nodes -o json >"${work}/nodes.json" 2>"${work}/nodes.err" \
    || die "读不到 nodes：$(head -c 400 "${work}/nodes.err")"

# 部署器滚动 Job 的声明上限：判据 3 的右边。读不到不在这里整条退出——它是三条
# 判据里一条的输入，缺席要在那一条上点名，而不是把另外两条也一起判成读不到。
${KUBECTL} -n "${NS}" get configmap cogneva-evolution-config -o json \
    >"${work}/evolution-config.json" 2>"${work}/evolution-config.err" || true

# 判据 4 的左边：树内声明。定时载体从工作树跑这条脚本，仓库根由脚本自身位置算；
# 读的是**已提交的渲染产物**而不是 values.yaml——values 可能正被别的会话改着，而
# 集群上那份是上一次渲染 apply 上去的。读不到照样不在整条退出：缺席在那一条上点名。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DECLARED_DIR="${COGNEVA_RECONCILE_DECLARED:-${REPO_ROOT}/deploy/rendered/${COGNEVA_RECONCILE_PROFILE:-k3s-single}}"
# 准入面上另一只治理对象。读不到时判据 4 点名，不在这里整条退出。
${KUBECTL} -n "${NS}" get limitrange -o json \
    >"${work}/limitranges.json" 2>"${work}/limitranges.err" || true
# --converge 的第二个测量输入：正在跑的 Pod。只在要写集群的那条路径上读它——
# 只读模式不需要，而多一次集群读就是多一次可能失败的东西。
if [ "${converge}" = 1 ]; then
    ${KUBECTL} -n "${NS}" get pods -o json >"${work}/pods.json" 2>"${work}/pods.err" || true
fi

# ---- 物理面：Prometheus 端点 ----
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
        # 测试注入点：参数是 PromQL，标准输出是 /api/v1/query 的响应体。
        "${COGNEVA_PROM_QUERY_CMD}" "$1" >"$2" || die "注入的 Prometheus 查询失败：$1"
        return
    fi
    curl -sS --max-time 15 --data-urlencode "query=$1" "${PROM_URL}/api/v1/query" >"$2" \
        || die "Prometheus 查询失败：$1"
}

# 每个节点的不可回收内存（本文件的判据 2 左边）。先按 (node, slice) 取一条，再按节点求和：
# Pod 重建过的地方，同一个 slice 会留下多条标签不同的历史序列，直接 sum 会把同一块内存
# 数两遍（实测：一个 slice 的历史序列叠起来，7 天窗口内的结果能到总量的几十倍）。
prom 'sum by (node) (max by (node, slice) (cogneva_host_slice_anon_bytes))' "${work}/anon.json"
# 页缓存 7 天最低到过哪（判据 2 的右边那部分）。`min by` 折的是同一 (node, slice) 的历史序列，
# 不是把各 slice 的最小值再取小——它是「这个 slice 在这台机器上最低到过哪」。
prom 'sum by (node) (min by (node, slice) (min_over_time(cogneva_host_slice_file_bytes[7d])))' \
    "${work}/file_min.json"
# node-exporter 的序列只带 instance=<节点IP:9100>，不带节点名；不 join 的话它就与
# 上面两条按 node 命名的序列永远对不上，而「对不上」在这条读数里的样子是一条缺席。
prom 'node_memory_MemTotal_bytes * on (instance) group_left (nodename) node_uname_info' \
    "${work}/memtotal.json"
# 不算判据，只并列：full 是「所有人一起被挡住」，some 只是「有人等」。
prom 'rate(node_pressure_memory_stalled_seconds_total[5m]) * on (instance) group_left (nodename) node_uname_info' \
    "${work}/psi_full.json"

if [ "${record}" = 1 ]; then
    export CROSS_LAYER_RECORD_FILE="${SERIES}"
fi
# converge 侧要写集群：它拿到的是同一条 kubectl 入口（可能是 "sudo -n k3s kubectl"，
# 由 python 侧按空白切开），以及命名空间。
export CROSS_LAYER_CONVERGE="${converge}"
export CROSS_LAYER_KUBECTL="${KUBECTL}"
export CROSS_LAYER_NS="${NS}"
python3 - "${work}" "${NS}/${QUOTA_NAME}" "${DECLARED_DIR}" <<'PYEOF'
import json
import os
import re
import sys

work, quota_id, declared_dir = sys.argv[1], sys.argv[2], sys.argv[3]

_SUFFIX = {
    '': 1, 'm': 1e-3,
    'k': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15, 'E': 1e18,
    'Ki': 2 ** 10, 'Mi': 2 ** 20, 'Gi': 2 ** 30, 'Ti': 2 ** 40, 'Pi': 2 ** 50, 'Ei': 2 ** 60,
}


def qty(text):
    m = re.fullmatch(r'(\d+(?:\.\d+)?)([a-zA-Z]*)', str(text).strip())
    if not m or m.group(2) not in _SUFFIX:
        raise ValueError(f"无法解析数量 {text!r}")
    return float(m.group(1)) * _SUFFIX[m.group(2)]


def gib(n):
    return f"{n / 2 ** 30:.2f}Gi"


def read(name):
    with open(os.path.join(work, name), encoding='utf-8') as f:
        return json.load(f)


quota = read('quota.json')
nodes = read('nodes.json')


def vector(name):
    """Prometheus 的 vector 结果 -> {label 值: float}，按 node 或实例归并。"""
    doc = read(name)
    if doc.get('status') != 'success':
        raise SystemExit(f"CROSS-LAYER: Prometheus 返回 {doc.get('status')!r}：{name}")
    out = {}
    for s in doc['data']['result']:
        key = (s['metric'].get('node') or s['metric'].get('nodename')
               or s['metric'].get('instance') or '')
        out[key] = out.get(key, 0.0) + float(s['value'][1])
    return out


anon = vector('anon.json')
file_min = vector('file_min.json')
memtotal = vector('memtotal.json')
psi_full = vector('psi_full.json')


def record(verdict, used_mem=None, alloc_mem=None):
    """这一拍追加成序列的一行。写不进去只告警：它是读数，不是判据。"""
    path = os.environ.get('CROSS_LAYER_RECORD_FILE')
    if not path:
        return

    def cell(v):
        return '-' if v is None else str(int(round(v)))

    row = '\t'.join([
        str(int(__import__('time').time())), verdict,
        cell(used_mem), cell(alloc_mem),
        cell(sum(anon.values())), cell(sum(file_min.values())), cell(sum(memtotal.values())),
    ])
    try:
        with open(path, 'a', encoding='utf-8') as f:
            f.write(row + '\n')
    except OSError as e:
        print(f"CROSS-LAYER: 序列写不进去，这一拍没有留下读数：{e}", file=sys.stderr)

alloc = {}
for n in nodes.get('items', []):
    name = n['metadata']['name']
    a = n.get('status', {}).get('allocatable', {})
    alloc[name] = {'cpu': qty(a['cpu']) if 'cpu' in a else None,
                   'memory': qty(a['memory']) if 'memory' in a else None}

used = quota.get('status', {}).get('used', {})
hard = quota.get('spec', {}).get('hard', {})

problems = []
unreadable = []

# ---- 判据 1：集群记账的声明和 ≤ 节点能给出的容量 ----
# 少一个节点的 allocatable，和就偏小，而偏小的和更容易判绿 —— 缺的那一维要报出来。
for name in sorted(alloc):
    for res in ('cpu', 'memory'):
        if alloc[name][res] is None:
            unreadable.append(f"节点 {name} 的 status.allocatable 里没有 {res}，判据 1 的和会偏小")
sum_cpu = sum(v['cpu'] for v in alloc.values() if v['cpu'] is not None)
sum_mem = sum(v['memory'] for v in alloc.values() if v['memory'] is not None)

def show(res, n):
    return f"{n:.3g}" if res == 'cpu' else gib(n)


observed = {}
for res, total in (('cpu', sum_cpu), ('memory', sum_mem)):
    key = f'requests.{res}'
    if key not in used or key not in hard:
        # 配额里没有这一项，不等于这一项装得下：判据 1 的左边根本读不出来。
        unreadable.append(f"resourcequota 的 used/hard 里没有 {key}，判据 1 读不出来"
                          f"（used 有的键：{sorted(used)}）")
        continue
    u = qty(used[key])
    observed[res] = u
    h = qty(hard[key])
    if u > total:
        problems.append(
            f"判据1 {key}：集群记账 {show(res, u)} 已超过节点 allocatable 合计 "
            f"{show(res, total)}——配额比机器大，"
            f"声明装得下配额不代表装得下机器")
    else:
        print(f"  判据1 {key}: used {show(res, u)} ≤ allocatable {show(res, total)}  ✓"
              f"（配额 {show(res, h)}，{'比机器大' if h > total else '在机器之内'}）")

# ---- 判据 3：准入余量 ≥ 部署器下一次滚动 Job ----
# used 记的是所有非终态 Pod（含运行时按需建的那些），所以这个差问的是「下一次要建
# 的 Pod 进不进得来」；渲染期门禁问的是「声明和装不装得进配额」，两个问题的答案
# 可以相反，而只有前者决定闭环能不能继续落地。
try:
    with open(os.path.join(work, 'evolution-config.json'), encoding='utf-8') as f:
        evdata = json.load(f).get('data') or {}
except (OSError, ValueError):
    evdata = None
rollout_job = {}
if evdata is None:
    unreadable.append("读不到 configmap/cogneva-evolution-config：判据 3 读不出部署器滚动 Job 的声明上限")
else:
    for res, label, key in (
            ('cpu', 'limits.cpu', 'COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_CPU_LIMIT'),
            ('memory', 'limits.memory', 'COGNEVA_MAINLINE_DEPLOYER_ROLLOUT_JOB_MEMORY_LIMIT')):
        raw = (evdata.get(key) or '').strip()
        if not raw:
            unreadable.append(f"evolution ConfigMap 里没有 {key}：判据 3 读不出滚动 Job 的 {label}")
            continue
        if label not in used or label not in hard:
            unreadable.append(f"resourcequota 的 used/hard 里没有 {label}，判据 3 读不出来")
            continue
        try:
            job, u, h = qty(raw), qty(used[label]), qty(hard[label])
        except ValueError as e:
            unreadable.append(f"判据 3 的数量解析不了：{e}")
            continue
        rollout_job[label] = job
        if h - u < job:
            problems.append(
                f"判据3 {label}：used {show(res, u)} + 部署器滚动 Job {show(res, job)} "
                f"> 配额 {show(res, h)}——下一次滚动 Job 在准入就被拒，部署器每轮空转；"
                f"used 含渲染期清单之外的运行期自建 Pod，所以只查声明和的门禁看不见它")
        else:
            print(f"  判据3 {label}: used {show(res, u)} + 滚动 Job {show(res, job)} "
                  f"≤ 配额 {show(res, h)}  ✓（准入余量 {show(res, h - u)}）")

# 有 anon 或 file 样本、但 node-exporter 没报这个节点 MemTotal 的：判据 2 的分母不在，
# 这个节点会被下面的循环整个跳过 —— 跳过与成立在读出来的那一行上长得一样，所以在这里点名。
for node in sorted((set(anon) | set(file_min)) - set(memtotal)):
    unreadable.append(f"节点 {node} 有 host_slice 序列但没有 node_memory_MemTotal_bytes"
                      f"（node-exporter 对这个节点没读数），判据 2 的分母不在")

# ---- 判据 2：每节点 Σanon + file 的 7 天最小 < MemTotal ----
for node in sorted(memtotal):
    total = memtotal[node]
    if node not in anon:
        unreadable.append(f"节点 {node} 没有 cogneva_host_slice_anon_bytes 序列"
                          f"（DaemonSet 没在那个节点起来，不是它用了 0）")
        continue
    if node not in file_min:
        unreadable.append(f"节点 {node} 的 7 天窗口里没有 cogneva_host_slice_file_bytes 样本，"
                          f"推不出页缓存底线")
        continue
    floor = file_min[node]
    here = anon[node]
    psi = psi_full.get(node)
    psi_txt = "PSI full 无读数" if psi is None else f"PSI full(5m) {psi * 1000:.2f}ms/s"
    line = (f"  判据2 node={node}: Σanon {gib(here)} + file 7d 最小 {gib(floor)} "
            f"= {gib(here + floor)} vs MemTotal {gib(total)}；{psi_txt}")
    if here + floor >= total:
        problems.append(
            f"判据2 node={node}: 不可回收 {gib(here)} + 页缓存底线 {gib(floor)} "
            f"≥ MemTotal {gib(total)}——内核对页缓存的回收已在给 anon 让位")
        print(line + "  ✗")
    else:
        print(line + "  ✓")
    # 0.1ms/s 以下视同没有；判词里印到 0.01ms/s，所以这个门限与显示精度一致，
    # 不会出现「印着 0.00 却说它在计秒」。
    if psi is not None and psi >= 1e-4 and here + floor < total:
        print(f"    └ 不一致本身就是结论：anon 这条线仍有头寸，PSI full 却在计秒，"
              f"压力不在不可回收内存上，去 io / 回写那条线找")

# ---- 判据 4：live 的治理对象就是树内声明的那一份 ----
# 声明是正本，live 是它的投影；而投影滞后与「值本来就不同」在读数上同形，两者
# 要修的地方却不同。判的是**逐字段相等**，不是「装得下」——后者可能答对，而对象
# 根本不是从这棵树 apply 上去的（2026-10-19：树内已抬到 52Gi，live 还停在 46Gi，
# 没有任何一条读数说得出这件事，是靠人翻对象翻出来的）。
def qty_or_str(v):
    try:
        return ('q', qty(v))
    except (TypeError, ValueError):
        return ('s', str(v))


def face_diff(where, d, l):
    out = []
    for k in sorted(set(d) | set(l)):
        if k not in l:
            out.append(f"{where}.{k}：树内声明 {d[k]!r}，live 没有这一项")
        elif k not in d:
            out.append(f"{where}.{k}：live 有 {l[k]!r}，树内声明没有")
        elif qty_or_str(d[k]) != qty_or_str(l[k]):
            out.append(f"{where}.{k}：live {l[k]!r} ≠ 树内声明 {d[k]!r}")
    return out


declared = {}
try:
    import glob
    import yaml
except ImportError:
    yaml = None
if yaml is None:
    unreadable.append("没有 PyYAML：判据 4 读不出树内声明")
else:
    try:
        for path in sorted(glob.glob(os.path.join(declared_dir, '*.yaml'))):
            with open(path, encoding='utf-8') as f:
                for d in yaml.safe_load_all(f):
                    if d and d.get('kind') in ('ResourceQuota', 'LimitRange'):
                        declared[(d['kind'], d['metadata']['name'])] = d
    except (OSError, ValueError) as e:
        unreadable.append(f"读不出树内声明（{declared_dir}）：{e}")
    if not declared:
        unreadable.append(f"树内声明目录里没有 ResourceQuota/LimitRange：{declared_dir}")

live_objs = {('ResourceQuota', quota['metadata']['name']): quota}
try:
    with open(os.path.join(work, 'limitranges.json'), encoding='utf-8') as f:
        for item in json.load(f).get('items', []):
            live_objs[('LimitRange', item['metadata']['name'])] = item
except (OSError, ValueError):
    unreadable.append("读不到 live 的 limitrange 列表：判据 4 判不了 LimitRange")

def object_diffs(kind, name, d, live_obj):
    """一只治理对象的逐字段差，以及声明里 cogneva.io/* 注解的个数。

    返回 (None, n) 表示 live 上根本没有这只对象：那是差的一种（整套声明没有 apply
    到这集群），但它的修法与逐字段不同，单独报。
    """
    if live_obj is None:
        return None, 0
    diffs = []
    if kind == 'ResourceQuota':
        diffs += face_diff(f"{kind}/{name} spec.hard",
                           d.get('spec', {}).get('hard') or {},
                           live_obj.get('spec', {}).get('hard') or {})
    else:
        want = {e.get('type'): e for e in (d.get('spec', {}).get('limits') or [])}
        have = {e.get('type'): e for e in (live_obj.get('spec', {}).get('limits') or [])}
        for typ in sorted(set(want) | set(have), key=str):
            if typ not in have:
                diffs.append(f"{kind}/{name} limits[{typ}]：树内声明有，live 没有")
                continue
            if typ not in want:
                diffs.append(f"{kind}/{name} limits[{typ}]：live 有，树内声明没有")
                continue
            for face in ('default', 'defaultRequest', 'max', 'min'):
                diffs += face_diff(f"{kind}/{name} limits[{typ}].{face}",
                                   want[typ].get(face) or {}, have[typ].get(face) or {})
    # 声明里带 cogneva.io/ 前缀的注解是这棵树对这只对象的声明面（运行期预算等）；
    # live 上别的注解（安装来源）不在比较范围内。
    want_ann = {k: v for k, v in ((d.get('metadata') or {}).get('annotations') or {}).items()
                if k.startswith('cogneva.io/')}
    have_ann = (live_obj.get('metadata') or {}).get('annotations') or {}
    for k, v in sorted(want_ann.items()):
        if k not in have_ann:
            diffs.append(f"{kind}/{name} 注解 {k}：树内声明 {v!r}，live 没有"
                         f"（这份声明没有 apply 到这只对象）")
        elif str(have_ann[k]) != str(v):
            diffs.append(f"{kind}/{name} 注解 {k}：live {have_ann[k]!r} ≠ 树内声明 {v!r}")
    return diffs, len(want_ann)


def report_drift(kind, name, diffs):
    problems.append(
        f"判据4 {kind}/{name}：live 与树内声明不一致（{len(diffs)} 处，"
        f"第一处：{diffs[0]}）——改了声明没 apply，或有人手工改过 live；"
        f"两种都把「声明」这张正本变成了没人兑现的纸")
    for extra in diffs[:6]:
        print(f"    └ {extra}")


drift = []
for (kind, name), d in sorted(declared.items()):
    live_obj = live_objs.get((kind, name))
    diffs, ann_count = object_diffs(kind, name, d, live_obj)
    if diffs is None:
        drift.append((kind, name, d, [f"{kind}/{name}：树内声明有它，live 没有"]))
        continue
    if diffs:
        drift.append((kind, name, d, diffs))
    else:
        print(f"  判据4 {kind}/{name}: 与树内声明逐字段相等"
              f"（cogneva.io/* 注解 {ann_count} 个）  ✓")

# ---- --converge：把判据 4 的差按树内声明填上 ----
# 界是测量的，不是策略的：apply 之后仍装得下此刻的用量才 apply（见脚本头部）。
# 判不了（读不到 Pod、数量解析不了）一律不动——写集群的动作只接受读得到的判据。
CONVERGE = os.environ.get('CROSS_LAYER_CONVERGE') == '1'
KUBECTL = __import__('shlex').split(os.environ.get('CROSS_LAYER_KUBECTL') or 'kubectl')
NAMESPACE = os.environ.get('CROSS_LAYER_NS') or 'cogneva'


def pod_containers(pod):
    spec = pod.get('spec') or {}
    return (spec.get('containers') or []) + (spec.get('initContainers') or [])


def converge_blockers():
    try:
        with open(os.path.join(work, 'pods.json'), encoding='utf-8') as f:
            pods = json.load(f).get('items', [])
    except (OSError, ValueError) as e:
        return [f"读不到 live 的 Pod 列表（{e}）：判不了 apply 之后装不装得下，不写集群"]
    out = []
    used_now = quota.get('status', {}).get('used') or {}
    for kind, name, d, _ in drift:
        if kind == 'ResourceQuota':
            for res, val in sorted((d.get('spec', {}).get('hard') or {}).items()):
                if res not in used_now:
                    continue
                try:
                    u, h = qty(used_now[res]), qty(val)
                except ValueError:
                    out.append(f"{kind}/{name} {res}：live used {used_now[res]!r} 或树内声明 "
                               f"{val!r} 解析不了——判不了就不写")
                    continue
                if u > h:
                    out.append(f"{kind}/{name} {res}：live used {used_now[res]} 大于树内声明 "
                               f"{val}——apply 之后新 Pod 进不来")
                    continue
                # 收窄天花板（live 高于树）也必须留住判据 3 的那份余量：否则这一拍把
                # 天花板扶正，下一拍部署器的滚动 Job 就进不来，等于用一条判据换掉另一条。
                job = rollout_job.get(res)
                if job is not None and u + job > h:
                    out.append(f"{kind}/{name} {res}：apply 之后 live used {used_now[res]} + "
                               f"部署器滚动 Job 超过树内声明 {val}——下一次滚动会在准入被拒")
        else:
            for entry in (d.get('spec', {}).get('limits') or []):
                if entry.get('type') != 'Container':
                    continue
                maxs, mins = entry.get('max') or {}, entry.get('min') or {}
                for pod in pods:
                    pname = (pod.get('metadata') or {}).get('name', '?')
                    for c in pod_containers(pod):
                        res = c.get('resources') or {}
                        lim, req = res.get('limits') or {}, res.get('requests') or {}
                        try:
                            for key, val in maxs.items():
                                if key in lim and qty(lim[key]) > qty(val):
                                    out.append(
                                        f"{kind}/{name} max.{key}={val}：Pod {pname}/"
                                        f"{c.get('name')} 的 limit {lim[key]} 超过它——"
                                        f"apply 之后它下一次滚动就进不来")
                            for key, val in mins.items():
                                if key in req and qty(req[key]) < qty(val):
                                    out.append(
                                        f"{kind}/{name} min.{key}={val}：Pod {pname}/"
                                        f"{c.get('name')} 的 request {req[key]} 低于它——"
                                        f"apply 之后它下一次滚动就进不来")
                        except ValueError as e:
                            out.append(f"{kind}/{name}：Pod {pname}/{c.get('name')} 的数量"
                                       f"解析不了（{e}）——判不了就不写")
    return out


def apply_declared():
    import subprocess
    import tempfile
    failed = []
    for kind, name, d, diffs in drift:
        fd, path = tempfile.mkstemp(prefix='governance-', suffix='.yaml')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                yaml.safe_dump(d, f, allow_unicode=True, sort_keys=False)
            r = subprocess.run(KUBECTL + ['apply', '-f', path],
                               capture_output=True, text=True)
        except OSError as e:
            failed.append(f"{kind}/{name}: 起不来 kubectl（{e}）")
            continue
        finally:
            try:
                os.unlink(path)
            except OSError:
                pass
        if r.returncode != 0:
            failed.append(f"{kind}/{name}: kubectl apply 退出码 {r.returncode}："
                          f"{(r.stderr or r.stdout).strip()[:300]}")
        else:
            print(f"  判据4 {kind}/{name}: 按树内声明 apply（{len(diffs)} 处差）")
    return failed


def refetch_live():
    """apply 之后重读 live；返回 (live_objs, 错误说明或 None)。"""
    import subprocess
    for args, fname in ((['get', 'resourcequota', '-o', 'json'], 'quota.json'),
                        (['get', 'limitrange', '-o', 'json'], 'limitranges.json')):
        r = subprocess.run(KUBECTL + ['-n', NAMESPACE] + args, capture_output=True, text=True)
        if r.returncode != 0:
            return None, f"{' '.join(args)} 退出码 {r.returncode}：{(r.stderr or '').strip()[:300]}"
        with open(os.path.join(work, fname), 'w', encoding='utf-8') as f:
            f.write(r.stdout)
    objs = {('ResourceQuota', quota['metadata']['name']): None}
    with open(os.path.join(work, 'quota.json'), encoding='utf-8') as f:
        objs[('ResourceQuota', quota['metadata']['name'])] = json.load(f)
    with open(os.path.join(work, 'limitranges.json'), encoding='utf-8') as f:
        for item in json.load(f).get('items', []):
            objs[('LimitRange', item['metadata']['name'])] = item
    return objs, None


converged = []
if drift:
    if CONVERGE and unreadable:
        # 这一拍有任何一处判不了，就不写集群：写集群的动作只接受读得全的判据，
        # 半套读数下 apply 出去的值，下一拍还得靠另一套读数去解释。
        for kind, name, d, diffs in drift:
            report_drift(kind, name, diffs)
        # 记在 unreadable 上：这一拍的结局就是「读不全」，而「没写集群」是它的后果；
        # 放进 problems 不会印出来（unreadable 的分支先退出），读者就只看得到差、看不到
        # 为什么没填。
        unreadable.append("判据4 自动收敛未执行：本拍有读不到的判据，"
                          "live 与树内的差留到读得全的一拍再填")
    elif CONVERGE:
        blockers = converge_blockers()
        if blockers:
            for b in blockers:
                problems.append(f"判据4 自动收敛未执行：{b}")
            for kind, name, d, diffs in drift:
                report_drift(kind, name, diffs)
        else:
            failed = apply_declared()
            for f in failed:
                problems.append(f"判据4 自动收敛失败：{f}")
            if not failed:
                fresh, err = refetch_live()
                if err:
                    problems.append(f"判据4 自动收敛后重读失败：{err}")
                else:
                    remaining = []
                    for kind, name, d, _ in drift:
                        diffs, _ = object_diffs(kind, name, d, fresh.get((kind, name)))
                        if diffs is None:
                            remaining.append((kind, name,
                                              [f"{kind}/{name}：apply 之后 live 仍没有它"]))
                        elif diffs:
                            remaining.append((kind, name, diffs))
                    if remaining:
                        for kind, name, diffs in remaining:
                            report_drift(kind, name, diffs)
                    else:
                        converged = [f"{kind}/{name}" for kind, name, _, _ in drift]
                        print(f"  判据4: 按树内声明收敛（{', '.join(converged)}）后逐字段相等  ✓")
    else:
        for kind, name, d, diffs in drift:
            report_drift(kind, name, diffs)

if unreadable:
    # 判不了也记一行：这一拍跑过了，只是没读全——不记的话，序列上的空洞会把
    # 「没跑」和「跑了但读不到」混成同一件事。
    record('unreadable', observed.get('memory'), sum_mem)
    print("CROSS-LAYER 判不了：")
    for u in unreadable:
        print("  - " + u)
    sys.exit(2)

if problems:
    record('broken', observed.get('memory'), sum_mem)
    print(f"CROSS-LAYER 不成立 [{quota_id}]：")
    for p in problems:
        print("  - " + p)
    sys.exit(1)

if converged:
    # 收敛过的一拍与本来就绿的一拍不是同一件事：前者这一拍写过集群。序列里分开记，
    # 读的人不必去翻 journal 才知道「数值是谁在什么时候改的」。
    record('converged', observed.get('memory'), sum_mem)
else:
    record('ok', observed.get('memory'), sum_mem)
print(f"CROSS-LAYER OK [{quota_id}]：三层在同一行上仍彼此装得下，"
      f"live 的治理对象与树内声明逐字段相等")
PYEOF
