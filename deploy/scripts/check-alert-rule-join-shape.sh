#!/usr/bin/env bash
# 向量比较的「多的一侧」门禁：一条带 on() 的规则，如果比较两侧里有一侧在同一组
# 连接键下会有多条序列，必须显式写 group_left / group_right。
#
# 为什么需要这条门禁：不写的话 Prometheus 在**求值**时直接拒绝——
# `multiple matches for labels: many-to-one matching must be explicit
# (group_left/group_right)`。这条错误读文本看不见：表达式语法合法，promtool 绿，
# 任何按指标名做的检查也绿。于是规则在安宁时一直好好的，**一旦被监视的那种形状出现
# 就自己先死掉**，而读到的人看到的是「安静」。2026-10-20 实测：
# `build_gate_slot_held_too_long` 的左边每个 (dir, role) 下有每个槽一条系列，
# 右边只有一条；单槽被占时一对一、不报错，两槽同时被占（正是它要抓的局面）就 422。
# 同一次 217 条规则的现网普查里只有这一条报错——所以这是一条，不是一族；但那次普查
# 是一次性的，仓库里当时没有任何东西会重跑它。
#
# 判据是**表达式结构**，不是求值结果：条件不成立时规则返回零条序列，而「没有序列」
# 与「这张形状不存在」在读数上分不开，按求值结果普查会把这些规则静默算成干净。
# 只有两侧都是带显式 by(...) 的聚合时才判得动：
#   - 两侧的 by 集都含于连接键 -> 一对一，绿；
#   - 一侧的 by 集超出连接键、另一侧不超 -> 超出那侧是多的一侧，必须有 group_*；
#   - 两侧都超出 -> 两侧都是多的一侧，必须有一个 group_*。
# 判不动的（裸选择器、without(...)、函数套函数、多个向量做算术、ignoring(...) 的
# 键集要按指标现有标签补齐）**计数上报**，不折进「绿」里：说不清不是一致。
#
# 第二段判的是**集合运算**（and / unless）不带 on()/ignoring() 的连接。集合运算
# 默认按**全部标签**匹配：两侧标签集必须逐字相等才算连上，一侧多一个标签就永远连不上。
# and 连不上 = 永远空、规则一生不响；unless 连不上 = 左边照单全收、规则恒亮。两种都
# 不报错，只是读数与事实相反。2026-10-22 实测：`build_gate_host_readings_unpublished`
# 两侧都写 `max without (pod, container, instance)`，左边是 ceiling、右边是宿主读数，
# ceiling 带 dimension 标签而宿主读数不带，于是「宿主读数缺席」这一侧从来没有匹配过，
# 规则在宿主读数齐全时照样点火——17:39 起一直亮着，只在 evolution 进程重启、连 ceiling
# 也一起消失的那几十秒里被 resolved，读起来像「抖动」，实际是「恒亮」。
# 判据分两档：
#   - 两侧都是 by(...)：by 集不相等 = 连不上，违规；
#   - 任一侧是 without(...)：命中标签集是「指标自己带了什么减去这几个」，静态读不到
#     指标带了哪些标签（本仓没有标签名册），判不了 —— 逐条点名并计数，不改判词，把
#     「把匹配键写成 on()/ignoring() 就能判」这件事摆在读者面前；
#   - 其它形状（裸选择器、函数套函数）计数，不点名。
# 只判**顶层**（不在聚合括号里）的集合运算；嵌在聚合里的另行计数，不假称判过。
#
# 用法：bash deploy/scripts/check-alert-rule-join-shape.sh [cogneva.json 路径] [清单目录]
# 判词：rc=0 每一个判得动的比较都声明了多的一侧；rc=1 有点不出来的（逐条点名规则与
#       运算符）；rc=3 规则读不了（看不了一眼不等于一致）。
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${1:-${ROOT}/deploy/helm/cogneva/files/cogneva.json}"
MANIFESTS="${2:-${ROOT}/deploy/k3s/observability/manifests}"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 3; }
[ -f "${CONFIG}" ] || { echo "找不到规则文件：${CONFIG}（判词记 3：看不了，不是一致）" >&2; exit 3; }

python3 - "${CONFIG}" "${MANIFESTS}" <<'PYEOF'
import json
import os
import re
import sys

COMPARISON_ON = re.compile(r'(?:>=|<=|==|!=|>|<)\s*(?:bool\s+)?on\s*\(')
AGG = re.compile(r'^(min|max|sum|count|avg|group|stddev|stdvar|topk|bottomk|quantile|count_values)\b')
CMP = ('>=', '<=', '==', '!=', '>', '<')
# 结合力从松到紧：or/unless < 算术 < 聚合。
LOW = (' or ', ' and ', ' unless ')
ARITH = (' * ', ' / ', ' + ', ' - ', ' % ')


def mask_strings(s):
    """把引号内的字符换成占位，长度不变：选择器里的标点不能被读成结构。"""
    out = list(s)
    quote = None
    for i, ch in enumerate(s):
        if quote:
            if ch == quote:
                quote = None
            else:
                out[i] = '_'
        elif ch in '"\'':
            quote = ch
    return ''.join(out)


def strip_outer_parens(s):
    s = s.strip()
    while s.startswith('(') and s.endswith(')'):
        depth = 0
        for i, ch in enumerate(s):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0 and i != len(s) - 1:
                    return s
        s = s[1:-1].strip()
    return s


def operand_before(s):
    """运算符左边那个操作数：从尾部往回取到同级的第一个分隔处。"""
    s = s.rstrip()
    depth = 0
    for i in range(len(s) - 1, -1, -1):
        ch = s[i]
        if ch == ')':
            depth += 1
        elif ch == '(':
            depth -= 1
            if depth < 0:
                return s[i + 1:]
        elif depth == 0:
            if ch == ',':
                return s[i + 1:]
            for w in (' or', ' and', ' unless'):
                if s[:i + 1].endswith(w):
                    return s[i + 1:]
    return s


def operand_after(s):
    """运算符右边那个操作数：从头部取到同级的第一个分隔处。"""
    s = s.lstrip()
    depth = 0
    for i, ch in enumerate(s):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth < 0:
                return s[:i]
        elif depth == 0:
            if ch == ',':
                return s[:i]
            for w in LOW:
                if s.startswith(w, i):
                    return s[:i]
    return s


def cut_top(s, seps):
    """按给定分隔符第一次出现处切开，返回 (左, 右)；找不到右则 (s, None)。"""
    depth = 0
    for i, ch in enumerate(s):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        elif depth == 0:
            for sep in seps:
                if s.startswith(sep.strip(), i):
                    return s[:i], s[i + len(sep.strip()):]
    return s, None


def is_scalar(expr):
    return bool(re.match(r'^\s*-?\d+(\.\d+)?\s*$', expr))


def join_operand(expr):
    """集合运算一侧的 (分组种类, 标签集)；判不动时返回 (None, 原因)。

    集合运算不带 on()/ignoring() 时按**全部标签**匹配，所以要判就得分得清两侧各带
    哪些标签。比较运算符本身不产生标签：`a > on(x) b` 的结果沿用左向量的标签（左是
    标量时沿用右向量的），所以穿过比较去读它后面那个向量。by 给出的是精确集合；不带
    分组的聚合把所有标签都收掉，集合为空；without 给的是补集，而补集的底是什么要看
    指标自己带了哪些标签——本门没有那份名册，所以它只能算「判不动」，不能算绿。
    """
    e = strip_outer_parens(expr)
    left, right = cut_top(e, CMP)
    if right is not None:
        if is_scalar(left):
            return join_operand(right)
        return join_operand(left)
    m = AGG.match(e)
    if not m:
        return None, 'not an aggregation'
    # 这四个不带 by() 时保留输入标签（它们挑行，不合并行），所以「没写分组＝空集」
    # 对它们不成立。
    keeps_labels = m.group(1) in ('topk', 'bottomk', 'quantile', 'count_values')
    rest = e[m.end():].lstrip()
    spec = ('none', frozenset())
    for kind in ('by', 'without'):
        if rest.startswith(kind):
            rest = rest[len(kind):].lstrip()
            if not rest.startswith('('):
                return None, 'malformed %s' % kind
            depth = 0
            labels = None
            for i, ch in enumerate(rest):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        labels = frozenset(
                            x.strip() for x in rest[1:i].split(',') if x.strip())
                        rest = rest[i + 1:]
                        break
            if labels is None:
                return None, 'unbalanced %s' % kind
            spec = (kind, labels)
            break
    # 聚合要把它自己那段整个吃掉：`sum by (a) (x) + sum by (b) (y)` 的左侧不是
    # 「标签集为 {a} 的聚合」，而是一整个算术表达式。
    rest = rest.lstrip()
    if not rest.startswith('('):
        return None, 'aggregation does not cover the operand'
    close = matching_paren(rest, 0)
    if close is None or rest[close + 1:].strip():
        return None, 'aggregation does not cover the operand'
    if keeps_labels and spec[0] == 'none':
        return None, 'label-preserving aggregation without by'
    return spec, spec[0]


def set_join_width(masked):
    """(顶层集合运算, 嵌套集合运算) 的位置表。"""
    top = []
    nested = 0
    depth = 0
    i = 0
    while i < len(masked):
        ch = masked[i]
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        else:
            for w in ('and', 'unless', 'or'):
                before_ok = i == 0 or not (masked[i - 1].isalnum() or masked[i - 1] == '_')
                after = i + len(w)
                after_ok = after >= len(masked) or not (
                    masked[after].isalnum() or masked[after] == '_')
                if masked.startswith(w, i) and before_ok and after_ok:
                    if depth == 0:
                        top.append((w, i))
                    else:
                        nested += 1
                    i += len(w) - 1
                    break
        i += 1
    return top, nested


def judge_set_joins(promql):
    """(违规, 判不了的点名, 判不动的个数, 嵌套个数, 判得动的个数)。"""
    masked = mask_strings(promql)
    top, nested = set_join_width(masked)
    violations = []
    named = []
    silent = 0
    judged = 0
    for op, pos in top:
        if op == 'or':
            # or 是并集：两侧标签集不同只是各留各的，不会让一句话变成假的。
            judged += 1
            continue
        tail = masked[pos + len(op):]
        if re.match(r'\s*(on|ignoring)\s*\(', tail):
            # 显式给了匹配键：判得动，由上面那段 on() 的比较判。
            judged += 1
            continue
        ls, _ = join_operand(operand_before(masked[:pos]))
        rs, _ = join_operand(operand_after(tail))
        if ls is None or rs is None:
            silent += 1
            continue
        if 'without' in (ls[0], rs[0]):
            # 两侧哪怕写的是同一条 without(...)，底下的指标也可能带不同的标签，
            # 静态读不到，所以形状相同不构成「连得上」的证据。
            named.append((op, ls, rs))
            continue
        if ls[1] != rs[1]:
            violations.append((op, sorted(ls[1]), sorted(rs[1])))
        else:
            judged += 1
    return violations, named, silent, nested, judged


def label_set(expr):
    """(label 集合, 判据)；集合为 None 表示判不动，第二项说明为什么。"""
    e = strip_outer_parens(expr)
    _, right = cut_top(e, LOW)
    if right is not None:
        return None, 'top-level or/and/unless'
    left, right = cut_top(e, ARITH)
    if right is not None:
        # 向量与标量运算不改变 label 集；两个向量做算术时结果标签要按连接算，
        # 这里不猜。
        a, _ = label_set(left)
        b, _ = label_set(right)
        if a is not None and is_scalar(right):
            return a, 'scalar on the right'
        if b is not None and is_scalar(left):
            return b, 'scalar on the left'
        return None, 'arithmetic between vectors'
    m = AGG.match(e)
    if not m:
        return None, 'not an aggregation'
    rest = e[m.end():].lstrip()
    for kind in ('by', 'without'):
        if rest.startswith(kind):
            rest = rest[len(kind):].lstrip()
            if not rest.startswith('('):
                return None, 'malformed %s' % kind
            depth = 0
            labels = None
            for i, ch in enumerate(rest):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0:
                        labels = rest[1:i]
                        break
            if labels is None:
                return None, 'unbalanced %s' % kind
            if kind == 'without':
                return None, 'without names the complement'
            return {x.strip() for x in labels.split(',') if x.strip()}, 'by'
    return None, 'aggregation without by/without'


def matching_paren(s, open_idx):
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == '(':
            depth += 1
        elif s[i] == ')':
            depth -= 1
            if depth == 0:
                return i
    return None


def judge(promql):
    """(违规列表, 判不动的比较数, 判得动的比较数, 不带 on() 的比较数)"""
    masked = mask_strings(promql)
    violations = []
    unjudged = 0
    judged = 0
    no_on = 0
    i = 0
    while i < len(masked):
        op = next((o for o in CMP if masked.startswith(o, i)), None)
        if op is None:
            i += 1
            continue
        rhs = masked[i + len(op):]
        lead = len(rhs) - len(rhs.lstrip())
        m = re.match(r'^(?:bool\s+)?(on|ignoring)\s*\(', rhs.lstrip())
        if not m:
            # 不带 on()/ignoring() 的比较按全部标签连接：两条序列在同一组标签上各只有
            # 一条，所以不会 many-to-one，不需要这条门禁。数出来是为了让读者看清射程。
            no_on += 1
            i += len(op)
            continue
        pos = i + len(op) + lead
        open_idx = pos + m.end() - 1
        close_idx = matching_paren(masked, open_idx)
        if close_idx is None:
            unjudged += 1
            i += len(op)
            continue
        inner = masked[open_idx + 1:close_idx]
        tail = masked[close_idx + 1:].lstrip()
        grouped = bool(re.match(r'^group_(left|right)\b', tail))
        if grouped:
            tail = re.sub(r'^group_(left|right)\s*', '', tail, count=1).lstrip()
            if tail.startswith('('):
                g = matching_paren(tail, 0)
                if g is None:
                    unjudged += 1
                    i = close_idx + 1
                    continue
                tail = tail[g + 1:].lstrip()
        if m.group(1) != 'on':
            # ignoring 的键集是「指标现有标签减去这些」，不读现网就补不齐。
            unjudged += 1
            i = close_idx + 1
            continue
        keys = {x.strip() for x in inner.split(',') if x.strip()}
        left_set, _ = label_set(operand_before(masked[:i]))
        right_set, _ = label_set(operand_after(tail))
        if left_set is None or right_set is None:
            unjudged += 1
        else:
            judged += 1
            if (not left_set <= keys or not right_set <= keys) and not grouped:
                violations.append((op, sorted(left_set), sorted(right_set), sorted(keys)))
        i = close_idx + 1
    return violations, unjudged, judged, no_on


def stray_manifest_joins(manifest_dir):
    """清单里带 on() 的比较：这条门读不到它们的结构，但也不许装作看不见。

    subject 门的前身就是被这条路绕过的——规则改落在 manifest 里，chart 里那份 JSON
    一条都没少，门却再也没看过新规则。这里不解析 YAML：只在清单正文里找「比较运算符紧接
    on(...)」这一种形状，找到就点名文件与行号，让射程的缺口当场可见。
    """
    hits = []
    if not manifest_dir or not os.path.isdir(manifest_dir):
        return None, 0
    try:
        names = sorted(os.listdir(manifest_dir))
    except OSError:
        return None, 0
    files = 0
    for name in names:
        if not name.endswith(('.yaml', '.yml')):
            continue
        files += 1
        full = os.path.join(manifest_dir, name)
        try:
            with open(full, encoding='utf-8') as fh:
                for no, line in enumerate(fh, 1):
                    if COMPARISON_ON.search(line):
                        hits.append('%s:%d' % (name, no))
        except OSError:
            return None, 0
    return hits, files


def main(path, manifest_dir):
    try:
        with open(path, encoding='utf-8') as fh:
            doc = json.load(fh)
        rules = doc['observability']['infra_watch']['rules']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print('check-alert-rule-join-shape: 规则读不了：%s' % exc)
        print('check-alert-rule-join-shape: 判词记 3（看不了，不是一致）')
        return 3
    if not isinstance(rules, list) or not rules:
        # 零条与「一条都不违规」在汇总行上同形（判得动 0、违规 0 处）。文件在而结构变了，
        # 读数不能读成干净。
        print('check-alert-rule-join-shape: %s 里一条规则都没有读到——零条与「全都合规」'
              '同形，这一跑什么都没判，判词记 3' % path)
        return 3
    bad = 0
    unjudged = 0
    judged = 0
    no_on = 0
    set_judged = 0
    set_named = 0
    set_silent = 0
    set_nested = 0
    for rule in rules:
        violations, skipped, ok, plain = judge(rule.get('promql') or '')
        unjudged += skipped
        judged += ok
        no_on += plain
        for op, lb, rb, keys in violations:
            bad += 1
            print('%s: `%s on(%s)` 把 %s 与 %s 相连，却没写 group_left/group_right'
                  % (rule.get('name', '?'), op, ','.join(keys), lb, rb))
        set_bad, set_unjudgeable, set_hidden, set_nested_one, set_ok = judge_set_joins(
            rule.get('promql') or '')
        set_nested += set_nested_one
        set_silent += set_hidden
        set_judged += set_ok
        for op, lb, rb in set_bad:
            bad += 1
            print('%s: `%s` 两边不带 on()/ignoring() 时按全部标签匹配，左边 %s 与右边 %s '
                  '不可能相等——这条规则要么永远不成立（and），要么恒成立（unless）'
                  % (rule.get('name', '?'), op, lb, rb))
        for op, ls, rs in set_unjudgeable:
            set_named += 1
            print('%s: `%s` 两侧用了 %s(...)，命中的标签集取决于指标自己带了什么，'
                  '本门判不了；要它可判，把匹配键写成 on(...)/ignoring(...)'
                  % (rule.get('name', '?'), op, ls[0] if ls[0] == 'without' else rs[0]))
    print('check-alert-rule-join-shape: %d 条规则；判得动 %d 个带 on() 的比较，'
          '判不动 %d 个，违规 %d 处；另 %d 个比较不带 on()（不在本门射程内）'
          % (len(rules), judged, unjudged, bad, no_on))
    print('check-alert-rule-join-shape: 集合运算（顶层 and/unless）判不了 %d 个、'
          '形状判不动 %d 个、嵌在聚合里不在射程 %d 个；判得动的 %d 个'
          % (set_named, set_silent, set_nested, set_judged))
    stray, manifest_files = stray_manifest_joins(manifest_dir)
    if stray is None:
        print('check-alert-rule-join-shape: 清单目录读不了（%s）：射程有没有缺口不知道，'
              '判词记 3' % manifest_dir)
        return 3
    if manifest_files == 0:
        # 目录在而里面一份 .yaml 都没有：射程守卫印的「清单里没有带 on() 的比较」与
        # 「清单里什么都没有」是同一句话。规则搬到别处去了，这道守卫守的是空的。
        print('check-alert-rule-join-shape: %s 里一份 .yaml 都没有读到——'
              '「清单里没有带 on() 的比较」与「清单是空的」同形，射程守卫此刻什么都没守，'
              '判词记 3' % manifest_dir)
        return 3
    if stray:
        for hit in stray:
            print('%s: 清单里有一处带 on() 的比较，本门不读清单里的结构' % hit)
        print('check-alert-rule-join-shape: 先把它纳入射程再判绿（%d 处）' % len(stray))
        return 1
    print('check-alert-rule-join-shape: 射程守卫——清单里没有带 on() 的比较（%s）'
          % (manifest_dir or '未给目录'))
    return 1 if bad else 0


sys.exit(main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None))
PYEOF
