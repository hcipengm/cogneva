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
# 同一本账有四条读面，每条各自过一遍上面四条，谁也不替谁作证：
#   `/token-usage`          按执行者合并——答「谁花的」；
#   `/token-usage/series`   按窗口分行  ——答「什么时候花的」；
#   `/token-usage/outcomes` 每次尝试的结局——答「试过但没成」；
#   `/token-usage/account`  省下账与判定账——答「根本没发起 / 本来在花、现在停了」。
# 只判第一条时，画时间轴的那一屏可以整块消失而门全绿：它的一条也不在被判的路径上。
# 后两条读的是逐笔账本与两本旁账而不是折叠表，所以它们自报的区间叫 `since`/`until`：
# 账本没有「还没折到这里」这回事，答得出的就是它读过的。判据的形状不变——那一屏必须
# 把它实际读到的区间显示出来，而不是把请求的区间当结果。
# 第三条与第四条各答一种零花销，是一种的两个半边：少了第三条，「根本没发起」会读成
# 「没人用模型」；少了第四条，「本来在花、无声停了」同样读成「没人用模型」。
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
    ("/api/v1/observability/token-usage/account", "getTokenAccount", ("since", "until")),
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

# ── 5. 额度窗 ──
# 上面四条读的都是「花了多少」。没有一条答得出「这一窗允许多少、还剩多少、什么
# 时候换窗」，而订阅制的上游没有单价可乘，这三格就是成本读数本身。那份账在持有
# 凭证的网关发布的上游池快照里（`cluster-overview` 的 `llm_pool.pace`），花销那一
# 屏必须把它读出来——它可以整块消失而上面四条全绿：路由在、类型在、`tsc` 绿，
# 屏上却没有任何一处说过额度。
#
# 判据与上面同形，只是问的是字段而不是路由：调用逐窗口那一读的屏（含渲染出来的
# 调用方的组件）里，**至少一个文件**同时读 `llm_pool` 与 `.pace`。缺 `pace` 是最坏
# 的那种缺——快照照样读、池子照样显示「可用」，而「这一窗还剩多少」无声地没了。
#
# 两个形状上的坑，都在第一版里踩过：
#   - 不能把这一屏的文件**拼起来**再找子串：`views/registry.tsx` 里有个
#     `WorkspaceView`，"workspace" 就含 "pace"，于是「额度读数被删掉」永远为假。
#     逐个文件判，且要求的是字段访问 `\.pace\b`，不是裸词。
#   - `client.ts` 里那一行是它自己的声明，不是调用点：把它算成消费者，可达性会从
#     「谁调用了这一读」变成「谁 import 了 client」，整棵树都成了那一屏。
PACE_FIELD = re.compile(r"\.pace\b")
for rel in sorted(graph):
    if rel == CLIENT or not re.search(r"\bgetTokenUsageSeries\s*\(", read(rel)):
        continue
    readers = [s for s in screens if imported_by([s[0]], rel)]
    screen_files = {s[0] for s in readers} | {rel}
    consumer = [
        f
        for f in sorted(screen_files)
        if re.search(r"\bllm_pool\b", read(f)) and PACE_FIELD.search(read(f))
    ]
    missing = [] if consumer else ["llm_pool", ".pace"]
    if missing:
        problems.append(
            f"{rel}: 花销这一屏（{', '.join(sorted(screen_files))}）没有读上游额度窗"
            f"（缺 {', '.join(missing)}）——花销面只答「花了多少」，答不出还需不需要省着花、"
            "什么时候换窗"
        )
    else:
        summary.append("额度窗: " + ", ".join(consumer))

# ── 6. 时区 ──
# 这一屏上每一个时刻都是浏览器按**本地时区**渲染的（`toLocaleString` 的默认行为），
# 而账本、日志、SQL 记的都是 UTC。差八小时的时候，同一件事在页面上是「10-26 22:00」、
# 在日志里是「10-26 14:00」——读的人只会得出一个结论：其中一个是错的。现场已经发生过
# 一次：8 小时的时间差被当成「面板说还有花销、他却说四天零成功」。
#
# 判据不是「页面上出现 UTC 三个字」：那句话可以是一句不指向任何数的装饰。要的是
# **本地时区名字由代码算出来**（`localZoneLabel()` 有实现、有调用点），这样换一台
# 时区不同的机器，写出来的名字也跟着变，而不是钉死某个偏移量再去骗人。
ZONE_DEF = re.compile(r"export\s+function\s+localZoneLabel\s*\(")
ZONE_CALL = re.compile(r"\blocalZoneLabel\s*\(")
definitions = [rel for rel in sorted(graph) if ZONE_DEF.search(read(rel))]
callers = [
    rel
    for rel in sorted(graph)
    if rel not in definitions and ZONE_CALL.search(read(rel))
]
if not definitions:
    problems.append(
        "没有地方定义 localZoneLabel()：页面上的时刻是本地时区渲染的，"
        "而账本与日志是 UTC，不写出时区名字读的人只能猜"
    )
elif not callers:
    problems.append(
        "localZoneLabel() 有定义但没有调用点：时区名字没有出现在任何一屏上"
    )
else:
    summary.append("时区: " + ", ".join(f"{rel} ← {s}" for rel, s in [(c, definitions[0]) for c in callers]))

# ── 7. 执行者名单 ──
# 「谁在花」那一栏按执行者分组，每个名字配一句它是什么。名字的正本是网关的
# `actor_labels.rs`（闭集决定什么值能落到这一列），读法在这个屏上。两份名单没有
# 一处会比较：网关添一个执行者，这一屏照旧那份亲手抄的表，会把它显示成「不在
# 网关登记的执行者闭集里，可能是新调用方或写错了名字」——一次正常的扩词被报成
# 一次事故；反过来，闭集删掉一个名字而屏上还留着它的读法，那一行就再也不会出现，
# 而读法成了一段关于不存在的角色的话。
#
# 判的是**名单相等**，不是「屏上写了几个名字」：少一个多一个都红。读法本身（那句
# 中文）判不了——它是对名字的解释，不是可以被机器复核的事实。
LABELS_RS = "crates/cog-gateway/src/actor_labels.rs"
BACKEND_FIXED = re.compile(r"pub\s+const\s+ACTOR_LABELS\s*:\s*&\[&str\]\s*=\s*&\[(.*?)\];", re.S)
BACKEND_UNKNOWN = re.compile(r'pub\s+const\s+UNKNOWN_ACTOR\s*:\s*&str\s*=\s*"([^"]+)"')
FRONT_MAP = re.compile(r"const\s+ACTOR_MEANINGS\s*:\s*Record<string,\s*string>\s*=\s*\{(.*?)\n\};", re.S)
FRONT_KEY = re.compile(r"^\s*(?:'([^']+)'|([A-Za-z_][\w-]*))\s*:", re.M)

labels_path = os.path.join(ROOT, LABELS_RS)
if not os.path.isfile(labels_path):
    problems.append(f"读不到 {LABELS_RS}——执行者的闭集不在它声明的地方，名单无从比起")
else:
    backend_src = open(labels_path, encoding="utf-8").read()
    fixed = BACKEND_FIXED.search(backend_src)
    unknown = BACKEND_UNKNOWN.search(backend_src)
    if not fixed or not unknown:
        problems.append(
            f"{LABELS_RS} 里找不到 ACTOR_LABELS / UNKNOWN_ACTOR 的声明——"
            "闭集换了形状，这段比对在空转"
        )
    else:
        declared = set(re.findall(r'"([^"]+)"', fixed.group(1))) | {unknown.group(1)}
        fronts = [(rel, FRONT_MAP.search(read(rel))) for rel in sorted(graph)]
        fronts = [(rel, m) for rel, m in fronts if m]
        if not fronts:
            problems.append(
                "可达图里没有 ACTOR_MEANINGS 这张表：执行者名的读法没有一处可比的声明"
            )
        elif len(fronts) > 1:
            problems.append(
                "ACTOR_MEANINGS 有 "
                + str(len(fronts))
                + " 份声明（"
                + ", ".join(rel for rel, _ in fronts)
                + "）——两份读法就是两份名单"
            )
        else:
            rel, block = fronts[0]
            shown = {
                quoted or bare for quoted, bare in FRONT_KEY.findall(block.group(1))
            }
            missing = sorted(declared - shown)
            foreign = sorted(shown - declared)
            if missing:
                problems.append(
                    f"{rel}: 闭集声明了 {', '.join(missing)}，这一屏没有它的读法——"
                    "它会显示成「不在闭集里」，把一次正常的扩词报成一次事故"
                )
            if foreign:
                problems.append(
                    f"{rel}: 写了 {', '.join(foreign)} 的读法，"
                    f"而闭集（{LABELS_RS}）里没有这个名字——那段读法不会出现在任何一行上"
                )
            if not missing and not foreign:
                summary.append(f"执行者名单: {len(declared)} 个名字，{rel} 与之逐名相等")

# ── 8. 占位符不算成员 ──
# 折叠表里有两类取值混在同一列上：真正的成员（某条上游、某种协议），和占位符
# `-`——池级熔断在选线路之前就把请求挡下了，账上拿它占「没试过任何一条」。它
# 是真话，所以不能删、也不能不列；但把它数进「平台上有几条上游」，读者会以为
# 多接了一条线路，而它恰恰是「一条都没试」。
#
# 这条判的是**个数取的是哪一列**：那一格必须来自剔掉占位符的清单，而不是
# 折叠表的每一行。判不了的是那句中文读法写得好不好——那是解释，不是可复核的
# 事实；判得了的是「上游 / 协议这两维声明了谁是占位符」以及「那个数没退回全量」。
PANEL = re.compile(r"const\s+DIMENSIONS\s*=\s*\[")
count_src = [(rel, read(rel)) for rel in sorted(graph) if PANEL.search(read(rel))]
if not count_src:
    problems.append("可达图里找不到声明 DIMENSIONS 的那一屏：个数取哪一列无从判起")
else:
    rel, src = count_src[0]
    if re.search(r"\{list\.length\}\s*</div>", src):
        problems.append(
            f"{rel}: 那一格又退回成 {{list.length}} 了——折叠表的每一行都算成员时，"
            "「未发起」占位符会被数成一条上游 / 一种协议"
        )
    elif not re.search(r"\{counted\.length\}\s*</div>", src):
        problems.append(
            f"{rel}: 那一格读不出「剔掉占位符之后的清单长度」——"
            "判不了占位符是否被算进了成员数"
        )
    else:
        listed = [
            name
            for name in ("UPSTREAM", "API_STYLE")
            if not (
                (m := re.search(rf"const\s+{name}\s*:\s*Dimension\s*=\s*\{{(.*?)\n\}};", src, re.S))
                and "isPlaceholder" in m.group(1)
            )
        ]
        if listed:
            problems.append(
                f"{rel}: {', '.join(listed)} 没有声明谁是占位符——"
                "个数与清单会把「未发起」当成一个成员"
            )
        else:
            summary.append(f"占位符: {rel} 的上游/协议两维各自标出谁是占位符，个数取的是成员清单")

if problems:
    sys.exit("令牌花销读面的消费者不合格：\n  " + "\n  ".join(problems))

print("ok：" + "；".join(summary))
PYEOF
