#!/usr/bin/env bash
# 告警身份的「主体维」门禁：一条规则去重用的身份，必须是它讲的那个对象。
#
# 为什么需要这条门禁：告警的实例身份是「规则名 + 返回序列上的每个标签」（减去固定的
# 抓取侧标签），冷却按身份记账。Prometheus 给每条被抓的序列都挂 pod/container，
# 所以一条没把它们聚合掉的规则，其身份会随**报信的那个 Pod 重建**而换新：同一个持续
# 中的状况，每次滚动之后都以一条**全新告警**再报一次，各自开各自的冷却，各自驱动一支
# 小队。2026-10-01/02 实测 78 条里有 22 条带 pod，其中 17 条 pod 只是波动维（主体是
# 节点 / source / outcome / dir / loop 之类），当场修掉；2026-10-06 复查又捞到一条残留
# （`llm_upstream_rejecting_while_pool_reads_available`：主体是上游，身份却随安全网关
# Pod 走）。这条门禁就是那份复查的机械化。
#
# 判据不是「这条序列带不带 pod」——**每条被抓的序列都带**（30 天窗口 87 条叶子指标里
# 80 条带 pod）——而是「这条表达式有没有把它聚合掉」。所以要读的是**表达式结构**，不是
# 求值结果：条件此刻不成立的规则返回零条序列，而「没有序列」与「没有这个标签」在读数上
# 分不开，按求值结果普查会把这些规则静默算成干净。
#
# 读数规则（在下面的 lint 里，自检每次运行都跑）：
#   - 最外层聚合的 by/without 子句单独决定结果标签集；不带子句的聚合把标签全丢掉
#   - and/unless 取左侧的标签集；or 与算术/比较要两侧都收敛
#   - 区间函数（increase/rate/...）与透传函数把内层的标签集带出来
#   - 缺席函数（absent/absent_over_time）相反：不从被选序列继承标签，只把实参选择器里的
#     等值（`=`）匹配器抄成身份——实参不是选择器时结果是空标签（见 lint 里那组实测）
#   - 只查 pod/container，**不查 instance**：node-exporter 的 instance 是节点地址，
#     在那族规则里它是主体而不是报信者（`node_disk_usage_*` 靠它区分节点）
#
# 射程是**这个仓发出去的每一条规则**，不是只有 chart 里那一份 JSON。判据同一套，因为
# 身份随 Pod 走这件事与规则写在哪无关：2026-10-20 之前这份脚本只读 cogneva.json，而
# deploy/k3s/observability/manifests/ 里两份 PrometheusRule 的规则一条都没被看过——
# 那里正是宿主读数通道的告警（生产者 down / 读不到输入 / 端点抓不到 / 凭证面）住的地方。
# 只看 chart 会让「新规则落在 manifest 里」成为一条静默绕过这条门禁的路。
#
# 用法：bash deploy/scripts/check-alert-rule-subjects.sh [cogneva.json 路径] [manifest 目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${1:-${ROOT}/deploy/helm/cogneva/files/cogneva.json}"
MANIFESTS="${2:-${ROOT}/deploy/k3s/observability/manifests}"
[ -f "${CONFIG}" ] || { echo "找不到规则文件：${CONFIG}" >&2; exit 2; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "${CONFIG}" "${MANIFESTS}" <<'PYEOF'
import glob, json, os, re, sys

def find(o):
    if isinstance(o, dict):
        if isinstance(o.get('rules'), list) and o['rules'] and 'promql' in o['rules'][0]:
            return o['rules']
        for v in o.values():
            r = find(v)
            if r: return r
    if isinstance(o, list):
        for v in o:
            r = find(v)
            if r: return r

AGG = r'(sum|max|min|avg|count|group|stddev|stdvar|last|topk|bottomk|quantile)'
RANGE = r'(increase|rate|irate|delta|idelta|deriv|resets|changes|avg_over_time|min_over_time|max_over_time|sum_over_time|count_over_time|last_over_time|present_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)'
# 缺席函数单列一格：它们的身份**不是**从被选序列继承的，方向与 RANGE 那族相反（见 lint）。
ABSENT = r'(absent|absent_over_time)'
NOLABEL = r'(vector|scalar|time|pi|rand)'
PASSTHRU = r'(abs|ceil|floor|sqrt|exp|ln|log2|log10|round|clamp|clamp_max|clamp_min|sort|sort_desc|sgn|timestamp|day_of_week|day_of_month|days_in_month|hour|minute|month|year)'
# 缺席函数的实参：裸度量名带一个选择器，后面可以跟区间与 `offset`/`@` 修饰。只有这个
# 形态才会把等值匹配器带进结果身份；实参是聚合或函数调用时实测结果是空标签。
SELECTOR_WITH_LABELS = re.compile(
    r'^[a-zA-Z_:][a-zA-Z0-9_:]*\s*\{([^}]*)\}'
    r'\s*(?:\[[^\]]*\])?\s*(?:(?:offset|@)\s+[^\s)]+\s*)*$')

SUBQ = re.compile(r'\[[^\]]*\]\s*$')
SCALAR = re.compile(r'^[-+]?(\d+(\.\d*)?|\.\d+)(e[-+]?\d+)?$')
BOOLSCAL = re.compile(r'^\s*bool\s+[^\s]+')
MOD = re.compile(r'^\s*(on|ignoring)\s*\([^)]*\)\s*')

def strip_parens(e):
    e = e.strip()
    m = SUBQ.search(e)
    if m: e = e[:m.start()].strip()
    while e.startswith('(') and e.endswith(')'):
        d = 0; good = True
        for i, ch in enumerate(e):
            if ch == '(': d += 1
            elif ch == ')':
                d -= 1
                if d == 0 and i != len(e) - 1:
                    good = False; break
        if not good: break
        e = e[1:-1].strip()
    return e

def last_arg(e):
    """实参列表里最后一个实参。`quantile_over_time(0.99, v)`、`topk(5, v)` 这类
    两参形态的第一个实参是**参数**（分位、条数），标签集只由第二个实参决定；
    把整个实参串当一个操作数读，会读成一个没有运算符的叶子，于是收敛判定只看
    得到 `0.99, ((sum by ...))` 这样一段文本、把一条已经收敛的规则判成未收敛。"""
    depth = 0; last = 0
    for i, ch in enumerate(e):
        if ch in '([{': depth += 1
        elif ch in ')]}': depth -= 1
        elif ch == ',' and depth == 0: last = i + 1
    return e[last:].strip()

def close_of(e, open_idx):
    d = 0
    for i in range(open_idx, len(e)):
        if e[i] == '(': d += 1
        elif e[i] == ')':
            d -= 1
            if d == 0: return i
    return -1

def top_ops(e):
    depth = 0; i = 0; out = []; n = len(e)
    while i < n:
        ch = e[i]
        if ch in '([{': depth += 1
        elif ch in ')]}': depth -= 1
        elif depth == 0 and i > 0:
            for tok in (' and ', ' or ', ' unless '):
                if e.startswith(tok, i):
                    out.append((tok.strip(), e[:i], e[i + len(tok):])); i += len(tok); break
            else:
                for tok in ('group_left', 'group_right'):
                    if e.startswith(tok, i):
                        out.append((tok, e[:i], e[i + len(tok):])); i += len(tok); break
                else:
                    hit = None
                    for tok in ('>=', '<=', '==', '!=', '+', '-', '/', '*', '%', '^', '>', '<'):
                        if e.startswith(tok, i):
                            hit = tok; break
                    if hit:
                        out.append((hit, e[:i], e[i + len(hit):])); i += len(hit)
                    else:
                        i += 1; continue
            continue
        i += 1
    return out

def lint(expr, depth=0):
    """None 表示这条表达式已经把报信者聚合掉；否则给出仍然带着它的那段子表达式。"""
    if depth > 40: return 'DEPTH:' + expr[:60]
    e = strip_parens(expr)
    if not e: return 'EMPTY:' + expr[:60]
    if SCALAR.match(e): return None
    if BOOLSCAL.match(e): return None
    m = re.match(NOLABEL + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1: return None
    m = re.match(PASSTHRU + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1: return lint(e[m.end():-1], depth + 1)
    # `absent` / `absent_over_time` 与下面那族区间函数**方向相反**：区间函数把内层的标签
    # 带出来，缺席函数不从被选的序列上继承任何标签，只把**实参选择器里的等值（`=`）匹配器**
    # 抄成身份。实测（2026-10-16，cog-observability-prometheus；用一个不存在的度量名逼出
    # 那条唯一的合成序列，再看它的标签）：
    #   absent(m{pod="p1",job="j1"})        -> {pod="p1", job="j1"}
    #   absent(m{pod=~"p1",job="j1"})       -> {job="j1"}   （正则匹配器不算等值）
    #   absent(m{pod!="p1",job="j1"})       -> {job="j1"}   （否定匹配器也不算）
    #   absent(m)                           -> {}
    #   absent(max by (pod) (m))            -> {}           （实参是聚合/函数时为空标签）
    #   absent_over_time(m{pod="p1"}[5m])   -> {pod="p1"}   （矩阵选择器同理）
    # 判据因此是「实参是选择器、且里面用 `=` 点名了 pod/container」才红。把缺席函数当区间
    # 函数去 lint 实参，会把身份只带常量标签的缺席规则误报成没收敛——这正是它被单列的原因。
    m = re.match(ABSENT + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        sel = re.match(SELECTOR_WITH_LABELS, last_arg(e[m.end():-1]))
        if not sel: return None
        for matcher in sel.group(1).split(','):
            if re.match(r'\s*(pod|container)\s*=(?!=|~)', matcher):
                return 'LEAF:' + e[:120]
        return None
    m = re.match(RANGE + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1: return lint(last_arg(e[m.end():-1]), depth + 1)
    m = re.match(AGG + r'\s*(by|without)\s*\(([^)]*)\)', e)
    if m:
        kind, labs = m.group(2), [x.strip() for x in m.group(3).split(',') if x.strip()]
        if kind == 'by':
            return None if ('pod' not in labs and 'container' not in labs) else 'KEEPS:' + e[:80]
        return None if ('pod' in labs and 'container' in labs) else 'KEEPS:' + e[:80]
    m2 = re.match(AGG + r'\s*\(', e)
    if m2 and close_of(e, m2.end() - 1) == len(e) - 1:
        if e.startswith('count_values'): return 'KEEPS:' + e[:80]
        # `topk` / `bottomk` / `quantile` 与单参聚合不同：它们**保留**标签，标签集
        # 由第二个实参决定（第一个是条数或分位）。一律算收敛会让一条身份随 Pod
        # 走的 topk 规则静默通过——这与 `quantile_over_time` 是同一个盲区。
        if re.match(r'(topk|bottomk|quantile)\b', e):
            return lint(last_arg(e[m2.end():-1]), depth + 1)
        return None
    ops = top_ops(e)
    if not ops: return 'LEAF:' + e[:120]
    op, lhs, rhs = ops[0]
    if op in ('and', 'unless'):
        return lint(lhs, depth + 1)
    if op == 'or':
        a, b = lint(lhs, depth + 1), lint(rhs, depth + 1)
        return None if (a is None and b is None) else (a or b)
    a, b = lint(lhs, depth + 1), lint(MOD.sub('', rhs), depth + 1)
    return None if (a is None and b is None) else (a or b)

# --- 第二读数：比较的两侧能不能相遇 -------------------------------------------
# C78 那一半：PromQL 的比较按标签集配对，一侧无标签（`12 * max(x)`、裸聚合）就永远
# 配不上另一侧的序列——表达式合法、求值器不报错、规则永远静默。所以这里判的是
# **每一侧的 arity**，不赌「表里出现过这条规则」。
SIDE_SCALAR, SIDE_EMPTY, SIDE_LABELLED, SIDE_UNKNOWN = 'scalar', 'empty', 'labelled', 'unknown'

def side_arity(e, depth=0):
    """一侧在自己那一半带来什么：SCALAR 与谁都配；EMPTY（没有标签的向量）只与 EMPTY
    配；LABELLED 要标签集精确相等，而文本定不了，于是两侧都带标签时报 UNKNOWN 而不是猜。"""
    if depth > 40: return SIDE_UNKNOWN
    e = strip_parens(e)
    if not e: return SIDE_UNKNOWN
    if SCALAR.match(e) or BOOLSCAL.match(e): return SIDE_SCALAR
    m = re.match(NOLABEL + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        return SIDE_EMPTY if m.group(1) == 'vector' else SIDE_SCALAR
    m = re.match(PASSTHRU + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        return side_arity(e[m.end():-1], depth + 1)
    m = re.match(ABSENT + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        # arity 这一面不细究实参形态：实参是选择器时结果确实带那几个等值标签（LABELLED），
        # 是聚合或函数时实测为空标签（EMPTY），两种都可能。照实参的 arity 读——与给缺席
        # 函数单列之前的行为一致，判词不动。
        return side_arity(last_arg(e[m.end():-1]), depth + 1)
    m = re.match(RANGE + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        return side_arity(last_arg(e[m.end():-1]), depth + 1)
    m = re.match(AGG + r'\s*(by|without)\s*\(([^)]*)\)', e)
    if m:
        if m.group(2) == 'without': return SIDE_UNKNOWN
        return SIDE_LABELLED if m.group(3).strip() else SIDE_EMPTY
    m2 = re.match(AGG + r'\s*\(', e)
    if m2 and close_of(e, m2.end() - 1) == len(e) - 1:
        if e.startswith('count_values'): return SIDE_UNKNOWN
        # 两参形态（`topk(5, v)`、`quantile(0.9, v)`）保留标签，一侧的 arity 由第二个
        # 实参定。按单参聚合一律读成无标签，会让「`topk` 那侧带标签、另一侧是裸聚合」
        # 这种配不上的比较静默通过——与上面 RANGE 那一处是同一个盲区。
        if re.match(r'(topk|bottomk|quantile)\b', e):
            return side_arity(last_arg(e[m2.end():-1]), depth + 1)
        return SIDE_EMPTY
    ops = top_ops(e)
    if not ops:
        # 裸选择器：这套部署里每条序列都来自抓取，至少带抓取侧自己的标签
        return SIDE_LABELLED
    op, lhs, rhs = ops[0]
    if MOD.match(rhs) or MOD.match(lhs): return SIDE_UNKNOWN
    l, r = side_arity(lhs, depth + 1), side_arity(rhs, depth + 1)
    if op in ('and', 'unless'): return l
    if op == 'or': return l if l == r else SIDE_UNKNOWN
    if l == SIDE_SCALAR: return r
    if r == SIDE_SCALAR: return l
    if l == SIDE_EMPTY or r == SIDE_EMPTY: return SIDE_EMPTY
    return SIDE_UNKNOWN

def pairing(e):
    """C78 的形状，作为判词：一侧无标签、另一侧带标签的比较，一条序列也产不出来。
    文本定不了的时候返回 None——`on()`/`ignoring()` 的连接、以及两侧都带标签而名字
    可能不同的情形，都不从表达式单独下判。"""
    for op, lhs, rhs in top_ops(strip_parens(e)):
        if op not in ('>', '<', '>=', '<=', '==', '!='): continue
        l, r = side_arity(lhs), side_arity(rhs)
        if l == SIDE_EMPTY and r == SIDE_LABELLED:
            return op + ': label-less left side against a labelled right side'
        if r == SIDE_EMPTY and l == SIDE_LABELLED:
            return op + ': label-less right side against a labelled left side'
    return None

# --- 豁免表 -------------------------------------------------------------------
# 豁免就是盲区，所以每条都要写清「pod 在这里为什么是主体」；名字对不上任何规则
# 也算失败——豁免像规则一样会过期，过期后静默放行的正是修完又退回去的那一条。
POD_SUBJECT = {
    'pod_restarting_fast': '讲的就是这个 Pod 在重启',
    'pod_restarting_repeatedly': '同上',
    'pod_oom_killed': '同上',
    'container_memory_near_limit': '讲的就是这个容器',
    'container_memory_limit_sits_under_its_page_cache': '讲的就是这个容器——判的是它自己的上限装不装得下它自己走出来的那棵树。代价与前一条相同：DaemonSet 每滚一次就换一代身份，同一状况在新一代上重开一条；收这个代价是因为把 pod/container 聚合掉之后，这条读数就只剩一个数，说不出是哪个容器的上限比它自己的足迹还小',
    'redis_restarted': 'redis 是 StatefulSet，Pod 名稳定，Pod 就是它',
    'pod_stuck_terminating': '讲的是这个 Pod 的删除侧',
    'pod_failed_without_a_restart': '讲的就是这个 Pod 进了 Failed 阶段；Pod 名是这条事实本身，聚合掉它就只剩一个数，说不出是哪个 Pod 没被替换',
    'orphans_unreaped': '讲的是这个 Pod 里没被回收的进程',
    'container_cpu_throttled': '讲的就是这个容器被自己的 CPU 上限按住多久',
    'container_log_near_rotation_cap': '讲的就是这个容器的日志有没有被轮转',
    'container_disk_write_rate_high': '讲的就是这个容器在写盘（宿主面只说盘忙，不说谁在写）',
}

# 第三种：被别处的测试钉住、这里改不动的。空集是它现在的样子——最后一个成员
# `stream_consumer_stalled` 原本躺在这里，理由是「契约测试要求表达式以裸指标开头」；
# 2026-10-23 那条规则改成按 (stream, consumer_group) 聚合，测试仍绿，说明当初那句理由
# 描述的约束并不存在，而它挡住的是这条规则真正的缺陷：上报它的 Pod 进了告警实例的键，
# 一次停摆被拆成每个副本一行、每次滚动再开一对（7 天 379 行，去掉上报者后 79 个）。
# 留这个概念是为了下一次：真有测试钉住了某条规则的形状时，豁免写在这里、理由写清。
CONTRACT_PINNED = {}

# 控制集：这 18 条读出来必须是「已收敛」，否则是 lint 自己被改坏了（这类改坏会让
# 整张表的判词系统性偏松或偏严，而它不会报错）。
FIXED = ['goal_class_not_carried', 'task_checkpoint_not_persisted', 'task_checkpoint_not_superseded',
         'task_resume_point_unusable', 'verification_killed_by_budget', 'deployment_killed_by_budget',
         'build_gate_refused', 'background_loop_restarted', 'background_loop_role_probe_failing',
         'registry_store_walk_failing', 'node_disk_usage_high', 'node_disk_usage_critical',
         'node_disk_pressure', 'node_memory_pressure', 'node_capacity_changed', 'build_gate_not_in_force',
         'workload_no_available_replica', 'sandbox_command_oom_killed']

# 第二读数的控制集。两两只差一个 scalar() 包装——**控制集必须走被检代码的那条路**，
# 否则它证明不了任何事（这份文件的前身就在这里栽过一次：切片多带一个右括号，凡被函数
# 包起来的表达式一律误报，而两个自检恰好都是裸选择器，全绿）。
PAIRING_CONTROLS = [
    ('a > 12 * max(b)', True),
    ('a > scalar(12 * max(b))', False),
    ('a > 12 * max by (stream, consumer_group) (b)', False),
    ('a > 12', False),
    ('max(a) > max(b)', False),
    ('a > on(stream) max(b)', False),
    ('sum by (stream) (a) > 12 * sum by (stream) (b)', None),
    # 两参形态的 arity 由第二个实参定：`quantile_over_time(0.99, 12)` 是标量、
    # `topk(5, x)` 保留标签。读到第一个实参（分位/条数）上，前一条会被读成带标签、
    # 后一条被读成无标签，两条的判词都反了。
    ('quantile_over_time(0.99, 12) > max(a)', None),
    ('topk(5, x) > 12 * max(a)', True),
]

def find_container(o):
    """装着规则清单的那一层。停用名单与 rules 是兄弟键，所以要拿容器而不是列表。"""
    if isinstance(o, dict):
        if isinstance(o.get('rules'), list) and o['rules'] and 'promql' in o['rules'][0]:
            return o
        for v in o.values():
            r = find_container(v)
            if r: return r
    if isinstance(o, list):
        for v in o:
            r = find_container(v)
            if r: return r


# 宿主读数通道的这几条告警：序列来自 ScrapeConfig 的 **Node 角色**发现——一个进程、一个
# 端口，relabel 只挂 node/instance/job，抓取面上根本没有 pod/container 可聚合。门禁要防的
# 「同一条告警随报信 Pod 重建而换身份」在这里不存在，所以这不是「漏聚合」，是这条抓取路径
# 上**没有那个维**。
#
# 构建缓存回收器那一组（`CognevaHostTargetGc*`）读的是同一条通道：那几格是宿主自己的
# 定时器与状态文件的读数，由同一条 host-readings 抓取路径送出，没有第二个进程在报信。
#
# 但这种豁免最容易变成盲区，所以它带前提：premise 从树上的 ScrapeConfig 读出来（见
# scrape_jobs_without_pod），哪天这条通道改成 Pod 发现、或 relabel 开始挂 pod/container，
# 这些名字就自动回到「未收敛」一栏，而不是继续被静默放行。
NO_POD_REPORTER = {
    'CognevaHostReadingProducerDown': 'host_reading_producer_up',
    'CognevaHostReadingProducerUnreadable': 'host_reading_producer_unreadable',
    'CognevaHostReadingsEndpointDown': 'up{job="cogneva-host-readings"}',
    'CognevaHostCredentialSurfaceStale': 'cogneva_host_credential_surface_age_seconds',
    'CognevaHostCredentialSurfaceCadenceUnknown': 'cogneva_host_credential_surface_period_seconds',
    'CognevaHostCredentialSurfaceNeverRan': 'cogneva_host_credential_surface_measured',
    'CognevaHostCredentialSurfaceDrift': 'cogneva_host_credential_surface_aligned',
    'CognevaHostCredentialSurfaceUnreadable': 'cogneva_host_credential_surface_unreadable',
    'CognevaBackupPackageOwnerWrong': 'cogneva_backup_package_owner_uid',
    'CognevaBackupPackageUnreadable': 'cogneva_backup_package_reading_state{state="unreadable"}',
    'CognevaBackupPackageMissing': 'cogneva_backup_package_reading_state{state="empty"}',
    'CognevaBackupPackageStale': 'cogneva_backup_package_age_seconds',
    'CognevaHostTargetGcStale': 'cogneva_host_target_gc_age_seconds',
    'CognevaHostTargetGcCadenceUnknown': 'cogneva_host_target_gc_measured',
    'CognevaHostTargetGcNeverRan': 'cogneva_host_target_gc_measured',
    'CognevaHostTargetGcScriptUnverified': 'cogneva_host_target_gc_script_committed',
    'CognevaHostTargetGcProbeFailed': 'cogneva_host_target_gc_outcome{outcome="probe_failed"}',
    'CognevaHostTargetGcFloorUnreachable': 'cogneva_host_target_gc_outcome{outcome="floor_unreachable"}',
    # NVMe 那一族读的是同一台宿主上的盘，走同一条 host-readings 抓取通道（Node 角色，
    # 抓取面上没有 pod/container）。它们的身份由规则名与盘区分：`device` 是身份的一半，
    # 保留它是必须的（一块盘磨到界与另一块无关，两块都要报），而报信的从来不是 Pod——
    # 读数由宿主上的生产者算好、聚合器送出去，聚合器重建也不改变盘是谁的。
    'CognevaHostNvmeWearHigh': 'cogneva_host_nvme_percentage_used',
    'CognevaHostNvmeWearAccelerating': 'cogneva_host_nvme_wear_rate_pct_per_day',
    'CognevaHostNvmeHealthWarning': 'cogneva_host_nvme_media_errors_total',
    'CognevaHostNvmeUnreadable': 'cogneva_host_nvme_devices',
    'CognevaHostNvmeReplacementDetected': 'cogneva_host_nvme_replacements_total',
    'CognevaHostNvmeLeadTimeTooShort': 'cogneva_host_nvme_lead_days',
    'CognevaHostNvmeBaselineLost': 'cogneva_host_nvme_replacements_total',
    'CognevaFileSizeCensusUnjudged': 'cogneva_file_size_census_unjudged',
    'CognevaHostReadingSeriesUndeclared': 'host_reading_series_undeclared',
    'CognevaHostReadingSeriesVanished': 'host_reading_series_vanished',
    # 「读数点名的指标，抓取面上一人都不发」：数与上面几条同源，读的是同一条
    # host-readings 通道。身份要的是「有几条读数没有生产者」这件事本身，不是「谁数的」；
    # 逐条点名由同族的 `reading_producer_missing` 带 reading/metric 标签回答。
    'CognevaReadingProducerAbsent': 'reading_producer_absent',
    'CognevaReadingWitnessUnresolved': 'reading_witness_unresolved',
    'CognevaTasksPublishersDisagree': 'tasks_publishers_agree',
    'CognevaRefusalRedispatchBroken': 'cogneva_refusal_redispatch_state',
    'CognevaPrincipleSlugNoLanding': 'principle_carriers_slug_no_landing',
    'CognevaE2eRingFailed': 'e2e_ten_rings_failed_settled',
    'CognevaE2eRingUnread': 'e2e_ten_rings_unread',
    'CognevaE2eRingNoCheck': 'e2e_ten_rings_unchecked',
    'CognevaVerdictSeriesUnread': 'cogneva_verdict_series_unread',
    'CognevaFileSizeCensusStale': 'cogneva_file_size_census_stale',
    # A2A 对端面走的是同一条 host-readings 通道（Node 角色，抓取面上没有 pod/container）：
    # 一个进程、一个端口，报信者不是 Pod，聚合器重建也不改变「这一格是谁的」。
    'CognevaA2aConformanceDrifted': 'a2a_conformance_drift_cells',
    'CognevaA2aDiscoveryClosed': 'a2a_reach_defect',
    'CognevaA2aTckDrifted': 'a2a_tck_conformant',
    'CognevaResourceSixColumnConnExhausted': 'resource_six_column_state{column="conn",state="red"}',
    'CognevaResourceSixColumnSlotOverAdmitted': 'resource_six_column_state{column="slot",state="red"}',
}
HOST_READING_JOB = 'cogneva-host-readings'
POD_LABELS = ('pod', 'container')


def scrape_jobs_without_pod(dir_path):
    """从 manifest 的 ScrapeConfig 里读出：哪些抓取 job 的发现方式不带 pod。

    判据是两条都在：发现角色只有 Node（`role: Pod` 或 ServiceMonitor 那类按 Pod 发现的不算），
    且没有任何 relabel 往 pod/container 上写标签。少一条就不算——宁可让上面那六条回到红，
    也不要拿一个读错的前提去放行。"""
    try:
        import yaml
    except ImportError:
        return set()
    jobs = set()
    for f in sorted(glob.glob(os.path.join(dir_path, '*.yaml'))):
        try:
            docs = list(yaml.safe_load_all(open(f, encoding='utf-8')))
        except Exception:
            continue
        for doc in docs:
            if not isinstance(doc, dict) or doc.get('kind') != 'ScrapeConfig':
                continue
            spec = doc.get('spec') or {}
            roles = {str(c.get('role', '')).lower()
                     for c in (spec.get('kubernetesSDConfigs') or [])}
            if roles != {'node'}:
                continue
            writes_pod = False
            for rl in (spec.get('relabelings') or []) + (spec.get('metricRelabelings') or []):
                target = str(rl.get('targetLabel', '')) or ' '.join(rl.get('sourceLabels') or [])
                if any(lbl in target for lbl in POD_LABELS):
                    writes_pod = True
                    break
            if not writes_pod and spec.get('jobName'):
                jobs.add(spec['jobName'])
    return jobs


def manifest_rules(dir_path):
    """manifest 目录里的 (文件名, alert 或 record 名, expr)。目录不在就返回 None——
    这份脚本要能只拿一份规则文件用（定向测试就是这么用的），不能因为旁边没有
    集群清单就拒绝干活。返回的名字里 alert 与 record 都收：record 规则的输出
    会被告警再读一次，它的标签集同样是「这条规则讲谁」的那一半。"""
    if not os.path.isdir(dir_path):
        return None, []
    try:
        import yaml
    except ImportError:
        print(f'FAIL: 要读 {dir_path} 里的 PrometheusRule，但缺 pyyaml', file=sys.stderr)
        sys.exit(2)
    files = sorted(glob.glob(os.path.join(dir_path, '*.yaml'))
                   + glob.glob(os.path.join(dir_path, '*.yml')))
    out = []
    for f in files:
        try:
            docs = list(yaml.safe_load_all(open(f, encoding='utf-8')))
        except Exception as e:
            print(f'FAIL: 读不懂 {f}：{e}', file=sys.stderr)
            sys.exit(2)
        for doc in docs:
            if not isinstance(doc, dict) or doc.get('kind') != 'PrometheusRule':
                continue
            for group in (doc.get('spec') or {}).get('groups') or []:
                for rule in group.get('rules') or []:
                    out.append((os.path.basename(f),
                                rule.get('alert') or rule.get('record'),
                                rule.get('expr')))
    return files, out


def main(path, manifests_dir):
    container = find_container(json.load(open(path, encoding='utf-8')))
    rules = container['rules'] if container else None
    if not rules:
        print(f'FAIL: 没读到任何规则：{path}', file=sys.stderr)
        return 1
    by_name = {r['name']: r for r in rules}
    exempt = {**POD_SUBJECT, **CONTRACT_PINNED}
    problems = []

    # 停用名单与在跑的规则必须是不相交的两份：名字同时出现在两边时，那条规则
    # 每一拍被求值、每拍重报一次，而启动时的结算又把它的行关掉——同一拍里开与关
    # 互相打架，读数上只会看到一行时亮时灭。停用是一件单向的事：要说它不在了，
    # 就把它从 rules 里拿掉。
    retired = container.get('retired_rules', [])
    for name in retired:
        if name in by_name:
            problems.append(f'停用名单里的 {name} 同时在 rules 里（停用与在跑不能是同一个名字）')
        if not name.strip():
            problems.append('停用名单里有空名字')
    if len(set(retired)) != len(retired):
        problems.append('停用名单里有重复的名字')

    # 第五件：停用名单里的名字不许以**散文的写法**（反引号）再出现在任何规则摘要里。
    # 停用改的是「名单」那一侧——名单和「它不在了」是同一件事，改的时候一起改；而散文里
    # 以**指路口吻**点名它的地方散在摘要里，改不改都不报错。读者是被派去找东西的：他带着
    # 一个已经不存在的名字去找，什么都不找到，而读数面上「找不到」与「不存在」同形，他分不
    # 出是印错了还是这条路真的没了。停用/改名本来常常就是为了消掉歧义（两处写同一个条件、
    # 于是同一个状况有两个数），留在散文里的旧名字把那个歧义原样放回来。
    #
    # 判据取**反引号形态**，不取裸子串：同一个名字在这里有两种正当的裸出现——停用名单那一行
    # 本身（它就是上面第一件的依据），以及沿革注释与单测夹具里讲「它曾存在、为什么停用」的
    # 地方（改掉等于抹掉理由）。裸子串会把这两种一起判红，而它们恰恰是判据自己的依据。
    BACKTICKED = re.compile(r'`([^`\n]+)`')
    prose_hits = 0
    for r in rules:
        for token in dict.fromkeys(BACKTICKED.findall(r.get('summary') or '')):
            if token in retired:
                prose_hits += 1
                problems.append(
                    f'{r["name"]}：摘要里点名了已停用的 {token}，读者会去找一条不存在的规则')

    unreduced = [r['name'] for r in rules if lint(r['promql'])]
    offenders = [n for n in unreduced if n not in exempt]
    for n in offenders:
        problems.append(f'{n}：表达式把报信者的标签带进了身份 -- {lint(by_name[n]["promql"])}')

    for n in FIXED:
        if n not in by_name:
            problems.append(f'控制集里的 {n} 不在这份规则里（规则被改名或删了，控制集跟着过期）')
        elif lint(by_name[n]['promql']):
            problems.append(f'{n}：控制集要求它已收敛，实际 -- {lint(by_name[n]["promql"])}')
    for n in exempt:
        if n not in by_name:
            problems.append(f'豁免表里的 {n} 不在这份规则里（豁免跟着规则一起过期）')
        elif not lint(by_name[n]['promql']):
            problems.append(f'{n}：豁免表说它的主体是 Pod，实际已收敛 -- 这条豁免该删了')

    unpaired = [(r['name'], pairing(r['promql'])) for r in rules if pairing(r['promql'])]
    for n, why in unpaired:
        problems.append(f'{n}：比较的两侧配不上，这条规则产不出序列 -- {why}')
    for expr, want in PAIRING_CONTROLS:
        got = pairing(expr)
        if (got is not None) != bool(want):
            problems.append(f'第二读数的控制集被改坏了：{expr} -> {got}')

    # --- manifest 里的 PrometheusRule -------------------------------------------------
    # 与 chart 同一套判词。区别只在于这里没有控制集（控制集钉的是 chart 那 18 条），
    # 豁免表照旧按名字匹配；名字与 chart 撞车另算一条——同一个告警名两个来源，身份去重
    # 与冷却记账都分不开它们。
    files, m_rules = manifest_rules(manifests_dir)
    no_pod_ok = bool(files) and HOST_READING_JOB in scrape_jobs_without_pod(manifests_dir)
    m_unreduced = m_unpaired = 0
    waived = set()
    for src, name, expr in m_rules:
        if not name:
            problems.append(f'{src}：一条规则既没有 alert 也没有 record 名，身份无从谈起')
            continue
        if name in by_name:
            problems.append(f'{src}: {name} 与 chart 里的规则同名 —— 同一个告警身份有两个来源')
        if not expr:
            problems.append(f'{src}: {name} 没有 expr')
            continue
        why = lint(expr)
        if why:
            m_unreduced += 1
            if name in NO_POD_REPORTER and no_pod_ok:
                waived.add(name)
            elif name not in exempt:
                problems.append(f'{src}: {name}：表达式把报信者的标签带进了身份 -- {why}')
        paired_bad = pairing(expr)
        if paired_bad:
            m_unpaired += 1
            problems.append(f'{src}: {name}：比较的两侧配不上，这条规则产不出序列 -- {paired_bad}')

    names_in_manifest = {n for _, n, _ in m_rules}
    for n in (NO_POD_REPORTER if files is not None else ()):
        if n not in names_in_manifest:
            problems.append(f'免聚合表里的 {n} 不在这批 manifest 规则里（豁免跟着规则一起过期）')
        elif files is not None and not no_pod_ok and n not in exempt:
            problems.append(
                f'{n}：放行的前提没了 —— {HOST_READING_JOB} 不再由 Node 角色发现，'
                f'或 relabel 开始写 pod/container；这条规则要重新聚合并删掉豁免')

    print(f'规则 {len(rules)} 条，未收敛 {len(unreduced)} 条'
          f'（其中豁免 {len(unreduced) - len(offenders)} 条），两侧配不上 {len(unpaired)} 条，'
          f'已停用 {len(retired)} 条，摘要点名停用名 {prose_hits} 处',
          flush=True)
    if files is None:
        print(f'manifest 未检查：{manifests_dir} 不存在', flush=True)
    else:
        print(f'manifest 规则 {len(m_rules)} 条（{len(files)} 份 PrometheusRule），'
              f'未收敛 {m_unreduced} 条（其中无 pod 维可聚合 {len(waived)} 条），'
              f'两侧配不上 {m_unpaired} 条', flush=True)
    if problems:
        for p in problems:
            print('FAIL: ' + p, file=sys.stderr)
        return 1
    print('ALERT SUBJECT OK：每条规则的身份维都是它讲的那个对象')
    return 0

sys.exit(main(sys.argv[1], sys.argv[2]))
PYEOF
