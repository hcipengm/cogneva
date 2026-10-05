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
#   - 只查 pod/container，**不查 instance**：node-exporter 的 instance 是节点地址，
#     在那族规则里它是主体而不是报信者（`node_disk_usage_*` 靠它区分节点）
#
# 用法：bash deploy/scripts/check-alert-rule-subjects.sh [cogneva.json 路径]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${1:-${ROOT}/deploy/helm/cogneva/files/cogneva.json}"
[ -f "${CONFIG}" ] || { echo "找不到规则文件：${CONFIG}" >&2; exit 2; }

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "${CONFIG}" <<'PYEOF'
import json, re, sys

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
RANGE = r'(increase|rate|irate|delta|idelta|deriv|resets|changes|absent|absent_over_time|avg_over_time|min_over_time|max_over_time|sum_over_time|count_over_time|last_over_time|present_over_time|stddev_over_time|stdvar_over_time|quantile_over_time)'
NOLABEL = r'(vector|scalar|time|pi|rand)'
PASSTHRU = r'(abs|ceil|floor|sqrt|exp|ln|log2|log10|round|clamp|clamp_max|clamp_min|sort|sort_desc|sgn|timestamp|day_of_week|day_of_month|days_in_month|hour|minute|month|year)'

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
    m = re.match(RANGE + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1: return lint(e[m.end():-1], depth + 1)
    m = re.match(AGG + r'\s*(by|without)\s*\(([^)]*)\)', e)
    if m:
        kind, labs = m.group(2), [x.strip() for x in m.group(3).split(',') if x.strip()]
        if kind == 'by':
            return None if ('pod' not in labs and 'container' not in labs) else 'KEEPS:' + e[:80]
        return None if ('pod' in labs and 'container' in labs) else 'KEEPS:' + e[:80]
    m2 = re.match(AGG + r'\s*\(', e)
    if m2 and close_of(e, m2.end() - 1) == len(e) - 1:
        if e.startswith('count_values'): return 'KEEPS:' + e[:80]
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
    m = re.match(RANGE + r'\s*\(', e)
    if m and close_of(e, m.end() - 1) == len(e) - 1:
        return side_arity(e[m.end():-1], depth + 1)
    m = re.match(AGG + r'\s*(by|without)\s*\(([^)]*)\)', e)
    if m:
        if m.group(2) == 'without': return SIDE_UNKNOWN
        return SIDE_LABELLED if m.group(3).strip() else SIDE_EMPTY
    m2 = re.match(AGG + r'\s*\(', e)
    if m2 and close_of(e, m2.end() - 1) == len(e) - 1:
        return SIDE_UNKNOWN if e.startswith('count_values') else SIDE_EMPTY
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
    'redis_restarted': 'redis 是 StatefulSet，Pod 名稳定，Pod 就是它',
    'pod_stuck_terminating': '讲的是这个 Pod 的删除侧',
    'orphans_unreaped': '讲的是这个 Pod 里没被回收的进程',
}

# 第三种：身份正是这条规则**讲的那个东西**，而且有测试把它钉住。stream_consumer_stalled
# 必须一条序列一个消费者，契约测试要求表达式以裸指标开头且不含 sum(/avg( ——所以任何
# 聚合都过不了那份测试，哪怕它留着 stream / consumer_group、仍然点名了规则讲的那个
# 消费者。那份测试的机制比它陈述的不变式更严；要动是它自己那一笔的事（规则与测试同
# 一个 rev），不是在门禁红着的时候动。
CONTRACT_PINNED = {
    'stream_consumer_stalled': '契约测试钉住裸序列形态，见上面那段',
}

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
]

def main(path):
    rules = find(json.load(open(path, encoding='utf-8')))
    if not rules:
        print(f'FAIL: 没读到任何规则：{path}', file=sys.stderr)
        return 1
    by_name = {r['name']: r for r in rules}
    exempt = {**POD_SUBJECT, **CONTRACT_PINNED}
    problems = []

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

    print(f'规则 {len(rules)} 条，未收敛 {len(unreduced)} 条'
          f'（其中豁免 {len(unreduced) - len(offenders)} 条），两侧配不上 {len(unpaired)} 条',
          flush=True)
    if problems:
        for p in problems:
            print('FAIL: ' + p, file=sys.stderr)
        return 1
    print('ALERT SUBJECT OK：每条规则的身份维都是它讲的那个对象')
    return 0

sys.exit(main(sys.argv[1]))
PYEOF
