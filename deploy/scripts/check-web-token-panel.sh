#!/usr/bin/env bash
# 「按 actor 的令牌花销」读面的消费者门禁。
#
# 网关有一条 `/api/v1/observability/token-usage`：数值取自耐久折叠表，答的是
# 「过去 N 小时各调用方烧了多少」。这条路由的**判据是给不写 SQL 的人看的**，
# 也就是说它的成品不是那条路由，是**有人打开就能看见的那一屏**。
#
# 坏起来没有症状：路由在、契约测试也绿、账上那一行读 landed——因为那一行的字面量
# 就是路由自己的路径串，它在**路由被定义**的那一刻就成立了。面板从来没被任何一格
# 问过，于是它可以整块消失而不让任何东西失败。这正是「一条 assertion 里写了两件事、
# check 只量了第一件」的形状：API 那半有判据，面板那半没有。
#
# 判据（读到的是**从入口可达**这件事，不是某个文件里有没有某个串）：
#   1. 路由路径由 `web/src/api/client.ts` 声明，且那儿导出一个取它的函数。
#   2. 从入口 `web/src/main.tsx` 沿 import 图可达的文件里，**至少一处**调用那个
#      函数。死文件也能「调用」，所以可达性是判据的一半——不可达的调用点没有读者。
#   3. 那个调用点必须被**一格渲染出来的组件**用到（可达图里某处出现它的 JSX 标签），
#      不是只被 import——取数与渲染可以分两个文件（取数的 hook 被渲染的那一屏导入），
#      所以判的是「有渲染出来的读者」，不是「调用点自己会渲染」。
#   4. 那一屏必须读响应的**覆盖端点**（`covered_since`/`covered_until`）。
#      折叠每整点落一行，没折的区间与没花销的区间给出同一个答案（空列表）；
#      不读端点的那一屏会把「没读到」显示成「没花钱」。
#
# 同一本账有三条读面，每条各自过一遍上面四条，谁也不替谁作证：
#   `/token-usage`         按执行者合并——答「谁花的」；
#   `/token-usage/series`  按窗口分行  ——答「什么时候花的」；
#   `/token-usage/outcomes` 每次尝试的结局——答「为什么一分没花」。
# 只判第一条时，画时间轴的那一屏可以整块消失而门全绿：它的一条也不在被判的路径上。
# 第三条读的是逐笔账本而不是折叠表，所以它自报的区间叫 `since`/`until`：账本没有
# 「还没折到这里」这回事，答得出的就是它读过的。判据的形状不变——那一屏必须把
# 它实际读到的区间显示出来，而不是把请求的区间当结果。
#
# 用法：bash deploy/scripts/check-web-token-panel.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = "api/client.ts"
ENTRY = "main.tsx"
COVERAGE = ("covered_since", "covered_until")

# 一本账的几条读面：(请求路径, 取它的函数, 那一屏必须读出来的区间字段)。
READS = [
    ("/api/v1/observability/token-usage", "getTokenUsage", COVERAGE),
    ("/api/v1/observability/token-usage/series", "getTokenUsageSeries", COVERAGE),
    ("/api/v1/observability/token-usage/outcomes", "getTokenOutcomes", ("since", "until")),
]

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这条路由」无从问起")
client_src = open(client, encoding="utf-8").read()

# ── import 图 ──
IMPORT = re.compile(r"""^\s*(?:import|export)\b[^'"]*?from\s+['"]([^'"]+)['"]""", re.M)
EXT = ("", ".ts", ".tsx", "/index.ts", "/index.tsx")


def read(rel):
    return open(os.path.join(WEB, rel), encoding="utf-8").read()


def resolve(rel, spec):
    base = spec[2:] if spec.startswith("@/") else os.path.normpath(os.path.join(os.path.dirname(rel), spec))
    for ext in EXT:
        if os.path.isfile(os.path.join(WEB, base + ext)):
            return base + ext
    return None


def reachable():
    seen, queue = set(), [ENTRY]
    if not os.path.isfile(os.path.join(WEB, ENTRY)):
        sys.exit(f"读不到入口 {ENTRY}——可达性无从算起")
    while queue:
        rel = queue.pop()
        if rel in seen:
            continue
        seen.add(rel)
        for spec in IMPORT.findall(read(rel)):
            target = resolve(rel, spec)
            if target and target not in seen:
                queue.append(target)
    return seen


graph = reachable()


def direct_imports(rel):
    out = set()
    for spec in IMPORT.findall(read(rel)):
        target = resolve(rel, spec)
        if target and target in graph:
            out.add(target)
    return out


def imported_by(src_chain, dst):
    """src_chain 里的文件（经 import）能不能走到 dst。"""
    seen, queue = set(), list(src_chain)
    while queue:
        rel = queue.pop()
        if rel in seen:
            continue
        seen.add(rel)
        if rel == dst:
            return True
        queue.extend(direct_imports(rel))
    return False


# 渲染出来的组件：导出了 PascalCase 组件名，且可达图里某处出现它的 JSX 标签。
screens = [
    (rel, name)
    for rel in sorted(graph)
    for name in re.findall(r"export\s+function\s+([A-Z]\w+)\s*\(", read(rel))
    if any(re.search(rf"<{name}[\s/>]", read(other)) for other in graph)
]

problems = []
summary = []

for ROUTE, FETCHER, RANGE_FIELDS in READS:
    # ── 1. 路由路径与取它的函数 ──
    # 按字面量判，不按裸子串：`/token-usage` 是 `/token-usage/series` 的前缀，
    # 子串判法会让「基础读面的声明被删掉」在还留着 series 那条时照样通过。
    # 两端各要一个定界符，收尾允许 `${`（模板串里接查询参数的那种写法）。
    if not re.search(r"""['"`]""" + re.escape(ROUTE) + r"""(?:['"`]|\$\{)""", client_src):
        problems.append(f"{CLIENT} 里没有 {ROUTE}：前端这一侧没有声明这条读面的路径")
        continue
    if not re.search(rf"export\s+(?:async\s+)?function\s+{FETCHER}\b", client_src):
        problems.append(f"{CLIENT} 没有导出 {FETCHER}：路径在了，但没有人能取它")
        continue

    # ── 2. 可达的消费者 ──
    callers = [
        rel
        for rel in sorted(graph)
        if rel != CLIENT and re.search(rf"\b{FETCHER}\s*\(", read(rel))
    ]
    if not callers:
        problems.append(
            f"{FETCHER}：从 {ENTRY} 可达的源码里没有任何一处调用它——"
            "读面没有活的外部消费者（死文件里的调用点不算，它不可达）"
        )
        continue

    # ── 3、4. 有渲染出来的读者，且那一屏读了覆盖端点 ──
    for rel in callers:
        readers = [s for s in screens if imported_by([s[0]], rel)]
        if not readers:
            problems.append(
                f"{rel}: 调用了 {FETCHER}，但可达图里没有一格渲染出来的组件用到它"
                "（只有 import、没有渲染不算读者）"
            )
            continue
        screen_files = {s[0] for s in readers} | {rel}
        shown = "".join(read(f) for f in screen_files)
        if not any(f in shown for f in RANGE_FIELDS):
            problems.append(
                f"{rel}: 那一屏（{', '.join(sorted(screen_files))}）没有读它实际答出的区间"
                f"（{RANGE_FIELDS[0]}/{RANGE_FIELDS[1]}）——"
                "不读区间的屏会把「这段没折叠」或「这段读的是别处」显示成「这期间没花销」"
            )
        else:
            summary.append(f"{FETCHER}: " + ", ".join(f"{s[0]} ← {s[1]}" for s in readers))

if problems:
    sys.exit("令牌花销读面的消费者不合格：\n  " + "\n  ".join(problems))

print("ok：" + "；".join(summary))
PYEOF
