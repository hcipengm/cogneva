#!/usr/bin/env bash
# 启动相位那三条序列的「写者必须钉住」门禁：凡读它们的地方，要么在选择器里点名是哪个
# 工作负载，要么把 `job` 放进每一处折叠键。
#
# 为什么需要这条门禁（2026-10-29 现网实测）：`cogneva_boot_rss_bytes`、
# `cogneva_boot_phase_elapsed_seconds`、`cogneva_boot_phase_cpu_seconds` 有两个发布者，
# 因为它们是库里的代码，应用与进化 Pod 都跑它。应用的七格全走（process-start /
# config-loaded / embedding-dense / embedding-sparse / reranker / plugins-initialized /
# serving），进化 Pod 只走其中四格，而且量级差四十倍：同一天 `plugins-initialized` 上
# 应用是 4591 MiB、进化 Pod 是 97 MiB。于是「只按 `phase` 折叠」不是把重复压成一个，
# 而是**换主语**：`max by (phase)` 取当时量级大的那个，`min_over_time` 之类跨时间的
# 窗口聚合取当时恰好在线的那一个。`boot_phase_footprint_grew` 就是这样报出「应用涨了
# 47.03 倍」的——两个分母都是进化 Pod 的值，应用在同一时刻只涨了 1.003 倍。
#
# 判据读的是**表达式结构**，不是求值结果：一条规则此刻安静与它是否混了两个写者无关。
# 两种合法写法，任一即可：
#   1. 每一处选择器都把写者钉住：`cogneva_boot_rss_bytes{job="cogneva"}`；
#   2. 选择器不钉，但表达式里**每一处聚合**的 `by (...)` 里都有 `job`：
#      `max by (phase, job) (cogneva_boot_rss_bytes)`。
# 只钉一处、另一处裸着的写法不合法：外层那个不钉的聚合会把内层已经分开的两条再合回去。
# `job=~".*"` / `job=~".+"` 这种全匹配不算钉住——它一个写者也没排除。
#
# 射程是仓里发出去的每一处：chart 的进程内规则库、`manifests/` 里的 PrometheusRule，
# 以及同一目录下面板的 `expr`。只判规则那一份会让「新面板按 `phase` 画一条合并曲线」
# 成为一条静默绕过这里的话；而面板那一侧本来就按 `job` 分了两条 target，判据是现成的。
#
# 用法：bash deploy/scripts/check-boot-series-writer-pinned.sh [cogneva.json 路径] [manifest 目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG="${1:-${ROOT}/deploy/helm/cogneva/files/cogneva.json}"
MANIFESTS="${2:-${ROOT}/deploy/k3s/observability/manifests}"

[ -f "${CONFIG}" ] || { echo "找不到规则文件：${CONFIG}" >&2; exit 2; }
[ -d "${MANIFESTS}" ] || {
    # 点名了目录就必须读得到：退 0 并印「manifest 未检查」，与「manifest 里每一处都合规」
    # 在判词上是同一句话。
    echo "FAIL: manifest 目录 ${MANIFESTS} 不存在——点名了就必须读得到" >&2
    exit 3
}

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "${CONFIG}" "${MANIFESTS}" <<'PYEOF'
import glob, json, os, re, sys

SERIES = ('cogneva_boot_rss_bytes',
          'cogneva_boot_phase_elapsed_seconds',
          'cogneva_boot_phase_cpu_seconds')

# 聚合函数：它们才是把一个以上的序列折成一条的地方。区间函数（rate / increase /
# `*_over_time`）不折叠写者——它们在同一组标签内取窗口，`min_over_time` 折的是
# **时间**，写者的分开靠的是外面那层 `by (...)`。所以这里只列真聚合。
AGG = ('sum', 'max', 'min', 'avg', 'count', 'group', 'stddev', 'stdvar',
       'topk', 'bottomk', 'quantile', 'count_values')

SEL = re.compile(r'\b(' + '|'.join(SERIES) + r')\s*(?:\{([^}]*)\})?')
AGG_CALL = re.compile(r'\b(' + '|'.join(AGG) + r')\b')
BY_TAIL = re.compile(r'\s+by\s*\(([^)]*)\)')
WITHOUT_TAIL = re.compile(r'\s+without\s*\(([^)]*)\)')
JOB_MATCHER = re.compile(r'\bjob\s*(=~|!~|=|!=)\s*"([^"]*)"')
CATCH_ALL = ('.*', '.+', '')


def job_pinned(selector):
    """选择器里有没有一个真的排除掉写者的 job 匹配器。
    `job=~".*"` / `job=~".+"` / `job=~""` 一个写者也没排除，不算钉住。"""
    for op, value in JOB_MATCHER.findall(selector or ''):
        if op in ('=~', '!~') and value in CATCH_ALL:
            continue
        return True
    return False


def labels_of(text):
    return [x.strip() for x in text.split(',') if x.strip()]


def sites(config_path, manifests_dir):
    """(来源, 名字, 表达式)。来源是给人看的相对路径。"""
    out = []
    cfg = json.load(open(config_path, encoding='utf-8'))

    def find(o):
        if isinstance(o, dict):
            if isinstance(o.get('rules'), list) and o['rules'] and 'promql' in o['rules'][0]:
                return o['rules']
            for v in o.values():
                r = find(v)
                if r:
                    return r
        if isinstance(o, list):
            for v in o:
                r = find(v)
                if r:
                    return r
        return None

    container = find(cfg)
    if not container:
        print(f'FAIL: 没读到任何规则：{config_path}', file=sys.stderr)
        sys.exit(1)
    for rule in container:
        if rule.get('promql'):
            out.append((os.path.basename(config_path), rule.get('name'), rule['promql']))

    try:
        import yaml
    except ImportError:
        print('FAIL: 要读 manifests 里的 PrometheusRule 与面板，但缺 pyyaml', file=sys.stderr)
        sys.exit(2)

    for f in sorted(glob.glob(os.path.join(manifests_dir, '*.yaml'))
                    + glob.glob(os.path.join(manifests_dir, '*.yml'))):
        name = os.path.basename(f)
        try:
            docs = list(yaml.safe_load_all(open(f, encoding='utf-8')))
        except Exception as e:
            print(f'FAIL: 读不懂 {f}：{e}', file=sys.stderr)
            sys.exit(2)
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            if doc.get('kind') == 'PrometheusRule':
                for group in (doc.get('spec') or {}).get('groups') or []:
                    for rule in group.get('rules') or []:
                        if rule.get('expr'):
                            out.append((name, rule.get('alert') or rule.get('record'),
                                        rule['expr']))
            elif doc.get('kind') == 'ConfigMap':
                # 面板：data 里某个键装着一整份 Grafana JSON。
                for key, value in (doc.get('data') or {}).items():
                    if not isinstance(value, str) or '"panels"' not in value:
                        continue
                    try:
                        board = json.loads(value)
                    except Exception:
                        continue
                    for title, expr in panel_exprs(board):
                        out.append((f'{name}[{key}]', title, expr))
    return out


def panel_exprs(node, title=None):
    """面板里每一处 `expr`。标题取最近一层 `title`，用来点名是哪一格。"""
    if isinstance(node, dict):
        here = node.get('title', title)
        for k, v in node.items():
            if k == 'expr' and isinstance(v, str):
                yield here, v
            else:
                yield from panel_exprs(v, here)
    elif isinstance(node, list):
        for v in node:
            yield from panel_exprs(v, title)


def verdict(expr):
    """这一处表达式合法返回 None，否则返回拒绝理由。"""
    hits = list(SEL.finditer(expr))
    if not hits:
        return None
    if all(job_pinned(m.group(2)) for m in hits):
        return None
    # 没在选择器上钉住：那么每一处折叠都必须把 `job` 留在结果里。
    bad = []
    for m in AGG_CALL.finditer(expr):
        fn = m.group(1)
        tail = expr[m.end():]
        by = BY_TAIL.match(tail)
        without = WITHOUT_TAIL.match(tail)
        if not (by or without) and not re.match(r'\s*\(', tail):
            # 这个名字出现在别处（标签值之类），不是一次聚合调用。
            continue
        if by:
            if 'job' not in labels_of(by.group(1)):
                bad.append(f'{fn} by ({by.group(1).strip()})')
        elif without:
            if 'job' in labels_of(without.group(1)):
                bad.append(f'{fn} without ({without.group(1).strip()})')
        else:
            bad.append(f'{fn}(...) 没有 by 子句')
    if not bad:
        return None
    return '折叠键没有留住写者：' + '；'.join(bad)


CONTROLS = [
    # (表达式, 是否应判合法, 这一条对照在证明什么)
    ('max by (phase, job) (cogneva_boot_rss_bytes)', True, '折叠键带 job'),
    ('max by (phase) (cogneva_boot_rss_bytes)', False, '折叠键不带 job（2026-10-29 的现网形状）'),
    ('sum(cogneva_boot_rss_bytes)', False, '没有 by 子句的聚合'),
    ('max by (phase) (cogneva_boot_rss_bytes) and sum(cogneva_boot_rss_bytes{job="cogneva"})',
     False, '内层钉住、外层没钉——合回去的那一层必须红'),
    ('cogneva_boot_phase_elapsed_seconds{job="cogneva"}', True, '选择器点名写者'),
    ('max by (phase) (cogneva_boot_phase_elapsed_seconds{job=~".*"})', False,
     '全匹配正则不算钉住，折叠键又没带 job（2026-10-29 的形状）'),
    ('cogneva_boot_phase_elapsed_seconds{job=~".*"}', True,
     '没有聚合就不折写者：全匹配虽不算钉住，但没有任何东西被合回去'),
    ('max by (phase) (cogneva_boot_rss_bytes{job="cogneva"})', True,
     '选择器钉住了，聚合键可以不带 job'),
    ('max without (pod) (cogneva_boot_rss_bytes)', True, 'without 没丢 job'),
    ('max without (job) (cogneva_boot_rss_bytes)', False, 'without 把 job 丢了'),
    ('min_over_time((max by (phase, job) (cogneva_boot_rss_bytes))[3d:5m])', True,
     '区间函数不折叠写者，判的是外层聚合'),
    ('rate(cogneva_boot_rss_bytes[5m])', True,
     '区间函数不折叠：每个写者各一条序列，不混主语'),
    ('node_cpu_seconds_total', None, '不读这三条序列的表达式不在射程内'),
]


def main():
    problems = []
    for expr, want_ok, why in CONTROLS:
        got = verdict(expr)
        got_ok = got is None
        if want_ok is None:
            if not got_ok:
                problems.append(f'自检被改坏了（{why}）：{expr} 被读成了违规')
        elif got_ok != want_ok:
            problems.append(f'自检被改坏了（{why}）：{expr} 判成了'
                            f'{"合法" if got_ok else "违规"}')
    checked = 0
    in_scope = 0
    for src, name, expr in sites(sys.argv[1], sys.argv[2]):
        checked += 1
        if not any(s in expr for s in SERIES):
            continue
        in_scope += 1
        why = verdict(expr)
        if why:
            problems.append(f'{src}: {name}: {why}')
            problems.append(f'    表达式：{expr}')
    if problems:
        print('FAIL: 启动相位那三条序列有读点没钉住写者')
        for p in problems:
            print('  - ' + p)
        return 1
    if in_scope == 0:
        # 一处读点都没有，与「每一处都合规」在判词上是同一句话：判据没被用上≠判据过了。
        print(f'FAIL: 扫了 {checked} 处表达式，没有一处读启动相位那三条序列——'
              f'射程没覆盖到，这不等于合规', file=sys.stderr)
        return 1
    print(f'OK：扫了 {checked} 处表达式，其中 {in_scope} 处读启动相位序列，'
          f'每一处都钉住了写者（选择器点名 job，或每一处折叠键里都有 job）')
    return 0


sys.exit(main())
PYEOF
