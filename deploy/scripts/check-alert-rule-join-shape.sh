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
        return None
    for name in sorted(os.listdir(manifest_dir)):
        if not name.endswith(('.yaml', '.yml')):
            continue
        full = os.path.join(manifest_dir, name)
        try:
            with open(full, encoding='utf-8') as fh:
                for no, line in enumerate(fh, 1):
                    if COMPARISON_ON.search(line):
                        hits.append('%s:%d' % (name, no))
        except OSError:
            return None
    return hits


def main(path, manifest_dir):
    try:
        with open(path, encoding='utf-8') as fh:
            doc = json.load(fh)
        rules = doc['observability']['infra_watch']['rules']
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print('check-alert-rule-join-shape: 规则读不了：%s' % exc)
        print('check-alert-rule-join-shape: 判词记 3（看不了，不是一致）')
        return 3
    bad = 0
    unjudged = 0
    judged = 0
    no_on = 0
    for rule in rules:
        violations, skipped, ok, plain = judge(rule.get('promql') or '')
        unjudged += skipped
        judged += ok
        no_on += plain
        for op, lb, rb, keys in violations:
            bad += 1
            print('%s: `%s on(%s)` 把 %s 与 %s 相连，却没写 group_left/group_right'
                  % (rule.get('name', '?'), op, ','.join(keys), lb, rb))
    print('check-alert-rule-join-shape: %d 条规则；判得动 %d 个带 on() 的比较，'
          '判不动 %d 个，违规 %d 处；另 %d 个比较不带 on()（不在本门射程内）'
          % (len(rules), judged, unjudged, bad, no_on))
    stray = stray_manifest_joins(manifest_dir)
    if stray is None:
        print('check-alert-rule-join-shape: 清单目录读不了（%s）：射程有没有缺口不知道，'
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
