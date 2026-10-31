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

# 环境那一维的闭集住在这儿（`LlmOrigin` 的 `declared` / `as_str` 是一份契约）。
# 与上面那条闭集源同一个理由写成一列而不是一个路径：搬家的那一天，红的是「读不
# 出来」，而不是一份陈旧的判词。
USAGE_LEDGER_REL = "crates/cog-core/src/contract/usage_ledger.rs"

# 这四维的折叠表里都可能有占位符：上游与协议的 `-`、环境与执行者的 `unknown`。
DIMENSIONS_WITH_PLACEHOLDERS = ("UPSTREAM", "API_STYLE", "ORIGIN", "ACTOR")
# (屏上的常量名, 代码里那一臂的匹配式, 那一臂所在的文件, 说给人听的名字)。
# 每一行比的是一个字面量：屏幕要认得出占位符，靠的就是这两个字面量相等。
#
# 不按裸子串判：`unknown` 这个词在这一屏里出现过很多次（读法、注释、别的维），
# 只有「常量声明的那个字面量」和「代码里那一臂的字面量」是两处可以互相比对的声明。
def code_text(relative):
    """读一份代码源，读不到就把「读不到」本身当读数返回（空串）。"""
    path = os.path.join(ROOT, relative)
    if not os.path.isfile(path):
        return ""
    return open(path, encoding="utf-8").read()


PLACEHOLDER_ANCHORS = [
    (
        "UNKNOWN_ORIGIN",
        r'LlmOrigin::Unknown\s*=>\s*"([^"]+)"',
        (USAGE_LEDGER_REL, code_text(USAGE_LEDGER_REL)),
        "`LlmOrigin::Unknown`",
    ),
    (
        "UNKNOWN_ACTOR_NAME",
        r'UNKNOWN_ACTOR\s*:\s*&str\s*=\s*"([^"]+)"',
        (LABELS_RS, code_text(LABELS_RS)),
        "`UNKNOWN_ACTOR`",
    ),
]

# ── 8. 占位符不算成员 ──
# 折叠表里有两类取值混在同一列上：真正的成员（某条上游、某种协议、某个环境），
# 和占位符——`-` 是池级熔断在选线路之前就挡下的那一次（一个上游都没试），
# `unknown` 是调用方没声明自己是谁 / 在哪个环境跑。两个都是真话，所以不能删、
# 也不能不列；但把它们数进「平台上有几条上游」「有几个环境」，读者会以为多接了
# 一条线路、多了一个环境，而它们恰恰是「一条都没试」「没人说」。
# `unknown` 这一格还多一层依据：`LlmOrigin::Unknown` 的契约原文就是「不是一个
# 调用能来自的地方，因此不进任何一个环境的总数」——屏幕把它数进个数，等于和写
# 账本的那份契约对着说。
#
# 这条判的是**个数取的是哪一列**：那一格必须来自剔掉占位符的清单，而不是
# 折叠表的每一行；并且四个维各自声明谁是占位符，声明的字面量必须等于代码里
# 那一臂（`-` 与 `POOLED_REFUSAL_UNNAMED`、`unknown` 与 `LlmOrigin::Unknown` /
# `UNKNOWN_ACTOR`）——自己抄一份常量，两边漂开时屏幕会照着旧的那个说话。
# 判不了的是那句中文读法写得好不好——那是解释，不是可复核的事实。
PANEL = re.compile(r"const\s+DIMENSIONS\s*=\s*\[")
count_src = [(rel, read(rel)) for rel in sorted(graph) if PANEL.search(read(rel))]
if not count_src:
    problems.append("可达图里找不到声明 DIMENSIONS 的那一屏：个数取哪一列无从判起")
else:
    rel, src = count_src[0]
    if re.search(r"\{list\.length\}\s*</div>", src):
        problems.append(
            f"{rel}: 那一格又退回成 {{list.length}} 了——折叠表的每一行都算成员时，"
            "「未发起」与「未声明」这两个占位符会被数成一个成员"
        )
    elif not re.search(r"\{counted\.length\}\s*</div>", src):
        problems.append(
            f"{rel}: 那一格读不出「剔掉占位符之后的清单长度」——"
            "判不了占位符是否被算进了成员数"
        )
    else:
        listed = [
            name
            for name in DIMENSIONS_WITH_PLACEHOLDERS
            if not (
                (m := re.search(rf"const\s+{name}\s*:\s*Dimension\s*=\s*\{{(.*?)\n\}};", src, re.S))
                and "isPlaceholder" in m.group(1)
            )
        ]
        if listed:
            problems.append(
                f"{rel}: {', '.join(listed)} 没有声明谁是占位符——"
                "个数与清单会把「未发起」「未声明」当成一个成员"
            )
        else:
            summary.append(
                f"占位符: {rel} 的"
                + "/".join(DIMENSIONS_WITH_PLACEHOLDERS)
                + "各维都标出谁是占位符，个数取的是成员清单"
            )

    # 闭集里登记了、窗口里没出现的取值，标成「本窗口没有调用」。占位符不能走这条路：
    # `unknown` 的意思是「没有人报」，把它列成「这个环境今天没跑」，正是要把两件事
    # 合回去。判的是剔除那一处还在，且判据里带着 isPlaceholder。
    absent_body = re.search(
        r"function\s+registeredButAbsent\s*\(.*?\n\}", src, re.S
    )
    if not absent_body:
        problems.append(
            f"{rel}: 找不到 registeredButAbsent()——「登记了但本窗口没出现」列的是哪些取值，"
            "判不了占位符是否被混进了那一段"
        )
    elif "isPlaceholder" not in absent_body.group(0):
        problems.append(
            f"{rel}: registeredButAbsent() 没有剔除占位符——`unknown` 会被列成"
            "「本窗口没有调用」的环境，而它根本不是环境"
        )

    # 声明的字面量必须等于代码里那一臂。两边各写一份常量时，漂开的唯一症状是
    # 屏幕不再认得出占位符：个数多出一个，且没有任何东西报错。
    for constant, value_re, value_src, what in PLACEHOLDER_ANCHORS:
        m = re.search(rf"const\s+{constant}\s*=\s*'([^']+)'", src)
        code = re.search(value_re, value_src[1])
        if not m:
            problems.append(f"{rel}: 找不到常量 {constant}——占位符的字面量没有单一声明")
            continue
        if not code:
            problems.append(
                f"{value_src[0]} 里找不到 {what} 的声明——占位符字面量无从比起"
            )
            continue
        if m.group(1) != code.group(1):
            problems.append(
                f"{rel}: {constant} = {m.group(1)!r}，而 {value_src[0]} 里 {what} 是 "
                f"{code.group(1)!r}——两边漂开时屏幕不再认得出这个占位符，"
                "个数会多出一个而没有任何东西报错"
            )
        else:
            summary.append(f"占位符字面量: {constant} == {what}（{code.group(1)!r}）")

# ── 9. 注释：一处组件，不是散落的 title= ──
# 原生 `title=` 的提示框由浏览器画在页面之外：选不中、复制不了，出现前有一段系统
# 延迟，同一排里两个 `title` 也说不清各自属于哪一格。这一屏的每一句解释都要能被
# 选中抄走（读数出了问题，人要把它贴进工单里），所以注释只能来自同一个组件；
# 散落的 `title=` 是这件事悄悄退回去的唯一形状，而它退回去时没有任何东西会红。
#
# 判的是**DOM 元素上的 title**，不是「组件还有一个 title 参数」：`ViewFrame` / `Section`
# 这类组件收的 `title` 是可见的标题文字，不是提示框。区分办法看标签名首字母——
# 小写的是 DOM 元素。`iframe` 的 `title` 是给读屏软件的，那是无障碍要求，放行。
TITLE_ATTR = re.compile(r"\btitle\s*=")
TITLE_ALLOWED_TAGS = {"iframe"}


def strip_comments(src):
    """注释里的 `title=` 不是属性：先去掉注释再判，否则判词会被自己那句说明触发。"""
    return re.sub(r"(?m)//.*$", "", re.sub(r"/\*.*?\*/", "", src, flags=re.S))


def tag_at(src, at):
    """`title=` 所在的那个标签名；回退不到 `<`（或中间隔着别的标签）就当不是属性。"""
    lt = src.rfind("<", 0, at)
    if lt < 0 or ">" in src[lt:at]:
        return None
    m = re.match(r"<\s*([A-Za-z][\w.-]*)", src[lt:])
    return m.group(1) if m else None


stray = []
for rel in sorted(graph):
    if not rel.endswith(".tsx"):
        continue
    src = strip_comments(read(rel))
    for m in TITLE_ATTR.finditer(src):
        name = tag_at(src, m.start())
        if name is None or not name[0].islower() or name in TITLE_ALLOWED_TAGS:
            continue
        stray.append(f"{rel}: <{name} title=")
if stray:
    problems.append(
        "DOM 元素上又出现了原生 title=（"
        + "；".join(stray[:4])
        + (f"…共 {len(stray)} 处" if len(stray) > 4 else "")
        + "）：提示框选不中、复制不了，注释必须走同一个组件"
    )

# 那个组件本身：导出 `Note`，且它画出来的是一块**页面内的面板**（`role="note"`），
# 悬停和聚焦都能打开（只有悬停的话，只用键盘的人永远读不到它）。
NOTE_DEF = re.compile(r"export\s+function\s+Note\s*\(")
note_defs = [rel for rel in sorted(graph) if NOTE_DEF.search(read(rel))]
if not note_defs:
    problems.append(
        "可达图里没有导出 Note 的组件：全站注释没有了唯一的出处，"
        "下一句解释只能退回成原生 title="
    )
else:
    note_rel = note_defs[0]
    note_src = read(note_rel)
    lacking = [
        what
        for what, pattern in (
            ("页面内的面板（role: 'note'）", r"""['"]note['"]"""),
            ("悬停打开", r"onMouseEnter"),
            ("聚焦打开", r"onFocus"),
        )
        if not re.search(pattern, note_src)
    ]
    rendered_by = [
        rel for rel in sorted(graph) if rel != note_rel and "<Note" in read(rel)
    ]
    if lacking:
        problems.append(
            f"{note_rel}: 注释组件缺 {'、'.join(lacking)}——"
            "缺面板它就退回成一个选不中的提示框，缺聚焦则只用键盘的人读不到它"
        )
    elif not rendered_by:
        problems.append(
            f"{note_rel}: 注释组件定义了但没有一处渲染它——"
            "第一句要解释的话出现时，人会另起一个 title="
        )
    else:
        summary.append(f"注释: {note_rel}，{len(rendered_by)} 个文件在用它")

# 面板的字要能选中、复制。判的是**面板自己的规则块**里有这两条：写在别处（或根本没有）
# 与「能选中」是两件事，而 `position: fixed` 是它能在表格里活下来的原因——`absolute`
# 会被表格与滚动容器裁掉，那些容器正是注释最多的地方。
NOTE_CSS = os.path.join(WEB, "styles/tokens.css")
if not os.path.isfile(NOTE_CSS):
    problems.append(
        "读不到 styles/tokens.css——注释面板的规则不在它声明的地方，"
        "「能选中、复制」无从判起"
    )
else:
    css = open(NOTE_CSS, encoding="utf-8").read()
    panel = re.search(r"\.note-panel\s*\{(.*?)\}", css, re.S)
    if not panel:
        problems.append("styles/tokens.css 里没有 .note-panel 的规则：注释画出来也选不中")
    else:
        body = panel.group(1)
        missing = [x for x in ("position: fixed", "user-select: text") if x not in body]
        if missing:
            problems.append(
                "styles/tokens.css 的 .note-panel 缺 "
                + "、".join(missing)
                + "：缺 user-select 就选不中，缺 fixed 会被表格与滚动容器裁掉"
            )
        else:
            summary.append("注释面板: position: fixed ＋ user-select: text")

# ── 10. 窗口按钮的位置 ──
# 这一屏上有两种时刻：额度窗是当刻读数、累计是整本折叠账，两者的数**不随窗口变**；
# 下面一组（花销 / 零花销 / 谁在花）才跟着那个按钮变。按钮长在页头时，它罩住的是
# 四件事里的四件，读的人按下 6 小时会以为额度窗也变了；组头写了「哪几块跟着变」，
# 那个误会才有答案。判两半：按钮在那一组里（在额度窗与累计之后），且那句话还在。
WINDOWS_DECL = re.compile(r"const\s+WINDOWS\b")
windowed = [
    rel
    for rel in sorted(graph)
    if WINDOWS_DECL.search(read(rel)) and re.search(r">\s*累计", read(rel))
]
if not windowed:
    problems.append("找不到声明 WINDOWS 的那一屏：窗口按钮还在不在，无从问起")
else:
    for rel in windowed:
        src = read(rel)
        m = re.search(r"WINDOWS\.map\s*\(", src)
        if not m:
            problems.append(f"{rel}: 声明了 WINDOWS 却没有一处渲染它——窗口选不了了")
            continue
        at = m.start()
        pace_at = src.find("<AllowanceWindow")
        cum = re.search(r">\s*累计", src)
        early = []
        if pace_at >= 0 and at < pace_at:
            early.append("额度窗")
        if cum and at < cum.start():
            early.append("累计")
        if early:
            problems.append(
                f"{rel}: 窗口按钮排在了 {'、'.join(early)} 前面——那两节的读数不随窗口变"
                "（一个是当刻快照，一个是整本折叠账），按钮罩住它们等于说它们也会变"
            )
        for token, why in (
            ("跟着它变", "没写清哪几块跟着它变"),
            ("不跟", "没写清哪几节不跟它变"),
        ):
            if token not in src:
                problems.append(f"{rel}: 窗口那一排没有那句话（缺「{token}」）——{why}")
        if not early and "跟着它变" in src and "不跟" in src:
            summary.append(f"窗口: {rel} 的按钮长在分组头上，并写明哪几块跟、哪几节不跟")

# ── 11. 谁在花：一张表两级行 ──
# 这一段以前是三块各画各的：一条分摊条、一份清单、一张表。三块都对，但它们的行
# 粒度不一样——条与清单是四格行，表也是四格行，而读者要的是「是不是有一个执行者
# 吃掉大半」；四格行答的是「他换过几条线路」。三块分开画时，列宽、失败格的有无、
# 数字的小数位都会各自漂开，屏幕上最明显的那件事（谁最宽）得自己做一次分组才
# 看得出来。所以判的是**形状**：一张表、两级行、列宽一处定义、失败格无条件出现。
TABLE_DEF = re.compile(r"export\s+function\s+ActorTable\s*\(")
table_defs = [rel for rel in sorted(graph) if TABLE_DEF.search(read(rel))]
if not table_defs:
    problems.append(
        "可达图里没有 ActorTable：「谁在花」又退回成一条分摊条加一张表的画法，"
        "两处的行粒度迟早对不上"
    )
else:
    table_rel = table_defs[0]
    tsrc = strip_comments(read(table_rel))
    before = len(problems)
    tables = len(re.findall(r"<table[\s>]", tsrc))
    if tables != 1:
        problems.append(
            f"{table_rel}: 有 {tables} 张 <table>——两级行不在同一张表里，"
            "列宽与对齐只能靠两处手抄"
        )
    if "group.rows.map(" not in tsrc:
        problems.append(
            f"{table_rel}: 一级行点不开到四格行——"
            "「同一个执行者换过上游」这件事在这一屏上就没有地方说得出来"
        )
    if not re.search(r"const\s+W\s*=\s*\{", tsrc) or tsrc.count("width: W.") < 8:
        problems.append(
            f"{table_rel}: 列宽没有单点定义（少于 8 格从同一份常量取宽）——"
            "宽度写第二次，两级行就会有一天差几像素"
        )
    if re.search(r"(?:failed|failed_calls)\s*>\s*0\s*&&", tsrc):
        problems.append(
            f"{table_rel}: 失败那一格又变成条件渲染了——"
            "零失败的行走少一格时，它后面的数字会整体左移一列"
        )
    if "styles.length === 1" not in tsrc or "分协议见下" not in tsrc:
        problems.append(
            f"{table_rel}: 一级行的缓存占比没有分协议的限制——"
            "两种协议的 input 含义不同，先加起来再除是一个协议的分母除以另一个的分子"
        )
    if tsrc.count("actorTotals(") != 1:
        problems.append(
            f"{table_rel}: 合计算了 {tsrc.count('actorTotals(')} 遍——"
            "分摊条、一级行、占比的分母各算一遍，它们迟早各说各的"
        )
    computed = [
        what for what, token in (
            ("缓存占比的分子分母", "cacheFormula("),
            ("每调用输入的来路", "perCallNote("),
            ("加权均耗的公式", "Σ(每行均耗"),
        )
        if token not in tsrc
    ]
    if computed:
        problems.append(
            f"{table_rel}: 算出来的格子没把来路写进注释（缺 {'、'.join(computed)}）——"
            "比值与差值是前端算的，分子分母不写出来就没人能复核"
        )
    if "registeredActorNames(" not in tsrc or "本窗口没有调用" not in tsrc:
        problems.append(
            f"{table_rel}: 闭集里登记了、本窗口没出现的执行者不再列出来——"
            "只列出现过的，读者会把这个数当成全集"
        )
    if len(problems) == before:
        summary.append(f"两级表: {table_rel}（一张表、两级行、列宽单点）")

# ── 12. 零花销：三条腿并成一节 ──
# 结局、省下账、判定账说的是同一件事——一分没花——的三种成因。分成三节时，
# 少了任何一块，剩下的两种就会读成同一件事；而它们的**单位不同**（尝试次数 /
# 决定次数 / 窗口个数），所以并成一节、但必须在同一节里写明不能相加。
ZERO_BILLS = ("<OutcomeBreakdown", "<SpendAccount")
merged = [
    rel
    for rel in sorted(graph)
    if all(token in read(rel) for token in ZERO_BILLS)
]
if not merged:
    problems.append(
        "没有哪一屏同时渲染 OutcomeBreakdown 与 SpendAccount："
        "「试过被挡回来」与「决定不发 / 该花却停了」又回到了各自一节，"
        "三分之二的零花销会读成同一件事"
    )
else:
    for rel in merged:
        src = read(rel)
        zero = re.search(r">\s*零花销", src)
        who = re.search(r">\s*谁在花", src)
        legs = [src.find(token) for token in ZERO_BILLS]
        if not zero:
            problems.append(f"{rel}: 那两本账上面没有「零花销」这一节的标题——三种成因的关系没有写进标题")
        elif any(at < zero.start() for at in legs):
            problems.append(
                f"{rel}: 结局或省下账不在「零花销」那一节里——"
                "分开画时，少了任何一块，剩下的两种就会读成同一件事"
            )
        elif who and any(at > who.start() for at in legs):
            problems.append(
                f"{rel}: 那两本账有一本被「谁在花」隔到了后面——同一件事的三条腿被拆成了两节"
            )
        if "不能相加" not in src:
            problems.append(
                f"{rel}: 没有写清三段的单位不同、不能相加——"
                "尝试次数、决定次数、窗口个数加在一起，读者会把它当成一份花销"
            )
        if zero and "不能相加" in src and not any(at < zero.start() for at in legs):
            summary.append(f"零花销: {rel} 三条腿并成一节，并写明单位不同")

# ── 13. 缓存的两半 ──
# 命中为 0 有两种成因，修法相反：压根没让上游缓存任何东西（标记没送出去，或这家
# 不收），与标记送到了但前缀每发都在变。分开它们的只有「写进去多少」这一列，
# 而这一列是三态——没量过（`null`／键缺席）、量到的 0、写了 N。判两件：那一格
# 印得出来，且**缺读数不许被压成 0**（`?? 0` 会把「没量到」说成一个结论，这正是
# 这一列要分开的两件事）。
write_defs = [
    rel
    for rel in sorted(graph)
    if re.search(r"export\s+function\s+cacheWriteValue\s*\(", read(rel))
]
# 印它的是**调用点**：定义那个函数自己的文件里当然有这两个名字，把它算成消费者
# 就是「谁 import 了谁就算读了」——那一屏可以整块消失而这一段照样绿。
write_cells = [
    rel
    for rel in sorted(graph)
    if rel not in write_defs
    and "cacheWriteValue(" in read(rel)
    and "cacheWriteNote(" in read(rel)
]
if not write_cells:
    problems.append(
        "没有哪一屏印出缓存写入那一半（`cacheWriteValue` 没有调用点）："
        "命中为 0 的两种成因从此只有一种读得出来"
    )
else:
    summary.append("缓存写入: " + ", ".join(write_cells))

# 聚合那一层必须真的按三态分，而不是把缺读数当 0。
if not write_defs:
    problems.append(
        "找不到 cacheWrite()：缓存写入那一半的聚合没有单一声明，"
        "「组里有一行没报就是没量过」这条规则没有地方可判"
    )
else:
    body = re.search(r"export\s+function\s+cacheWrite\s*\(.*?\n\}", read(write_defs[0]), re.S)
    text = body.group(0) if body else ""
    if "=== null" not in text or "=== undefined" not in text or "return null" not in text:
        problems.append(
            f"{write_defs[0]}: cacheWrite() 没有把「没量到」判成 null——"
            "把缺读数当 0，等于把「没量过」说成「上游说它写了 0」"
        )
    if re.search(r"tokens_cache_created\s*\?\?", text):
        problems.append(
            f"{write_defs[0]}: cacheWrite() 用 `?? 0` 把缺读数补成了零——"
            "那正是这一列要分开的两件事"
        )

# 整棵树上都不许把这一格 `?? 0`：一处补零，那一处的读数就永远读不出「没量过」。
coerced = [
    rel
    for rel in sorted(graph)
    if re.search(r"tokens_cache_created\s*\?\?", read(rel))
]
if coerced:
    problems.append(
        "把 tokens_cache_created 补成了 0（" + ", ".join(coerced) + "）——"
        "缺读数与量到 0 是两件事，补零之后屏上再也分不开"
    )

# 「空着」要说出它在等什么。这一半只随成功的那几次调用落账，所以一段窗口整片
# 没数最常见的成因是那段窗口一次都没成功——账上的事实，不是采集坏了。缺这一句，
# 读者会把规律读成故障，去查一条不存在的采集线。
note_bodies = [
    (rel, re.search(r"export\s+function\s+cacheWriteNote\s*\(.*?\n\}", read(rel), re.S))
    for rel in sorted(graph)
    if re.search(r"export\s+function\s+cacheWriteNote\s*\(", read(rel))
]
if not note_bodies:
    problems.append(
        "找不到 cacheWriteNote()：缺读数时那一格要说的「为什么没有」没有单一声明"
    )
for rel, body in note_bodies:
    text = body.group(0) if body else ""
    if "成功调用" not in text:
        problems.append(
            f"{rel}: 缺读数的说明没有说出「这一半要等一次成功调用才落账」——"
            "读者会把整片「还没进账」读成采集坏了，而它是账上的事实"
        )

# 「下一次可能进账」只能取自池子的当刻快照，不能写死。配额窗的复位时刻是上游
# 自己说的、随时会变：写进前端包里的那一天过了就成了假读数，而且不会有任何东西红。
landing_defs = [
    rel
    for rel in sorted(graph)
    if re.search(r"export\s+function\s+cacheWriteLanding\s*\(", read(rel))
]
landing_cells = [
    rel for rel in sorted(graph) if rel not in landing_defs and "cacheWriteLanding(" in read(rel)
]
if not landing_defs:
    problems.append(
        "找不到 cacheWriteLanding()：「下一次可能进账」没有单一来源，"
        "迟早有人把它写成一个写死的日期"
    )
else:
    body = re.search(
        r"export\s+function\s+cacheWriteLanding\s*\(.*?\n\}", read(landing_defs[0]), re.S
    )
    text = body.group(0) if body else ""
    if not re.search(r"recovery_at|next_attempt_at", text):
        problems.append(
            f"{landing_defs[0]}: cacheWriteLanding() 没有读池子快照里的复位/探测时刻——"
            "「下一次可能进账」只能来自当刻读数，不能是常量"
        )
    literal = re.search(r"20\d\d-\d\d-\d\d", text)
    if literal:
        problems.append(
            f"{landing_defs[0]}: cacheWriteLanding() 里写死了日期 {literal.group(0)}——"
            "复位时刻会变，写死的那个过了就成假读数，且没有东西会红"
        )
if not [rel for rel in landing_cells if "llm_pool" in read(rel)]:
    problems.append(
        "没有哪一屏把「下一次可能进账」印出来（`cacheWriteLanding` 的调用点没读 llm_pool）："
        "缓存写入空着时，读者只能猜它什么时候才会有数"
    )
else:
    summary.append(
        "缓存写入下次进账: " + ", ".join(rel for rel in landing_cells if "llm_pool" in read(rel))
    )

if problems:
    sys.exit("令牌花销读面的消费者不合格：\n  " + "\n  ".join(problems))

print("ok：" + "；".join(summary))
PYEOF
