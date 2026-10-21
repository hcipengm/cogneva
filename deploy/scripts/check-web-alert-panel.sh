#!/usr/bin/env bash
# 「站内告警中心」读面与确认面的消费者门禁。
#
# 告警中心的成品不是 `/api/v1/alerts/active` 这个路由，是**有人打开那一屏、
# 看见哪条没人管、点一下确认**。这三步里有四种坏形状，全都不出声：
#   1. 面板整块消失，或取数函数留在 `client.ts` 里没人调——没有任何东西会失败；
#   2. 列表取回来了、也画出来了，但确认按钮接的是一个空回调，点下去什么也不发生；
#   3. 确认写进了库，列表却不带那一列——点完 200、刷新又变回未确认（写状态而
#      没人读得到，等于没写）；
#   4. 告警重新 firing 时不清确认标记——同一个 dedup_key 复用一行，上一次发作的
#      确认会盖在新一次上，人以为有人在看。
#
# 判据（前三条问「从入口可达」，后三条问服务端那一半）：
#   1. 两条路由由 `web/src/api/client.ts` 声明，且导出取它们的函数；
#   2. 从 `main.tsx` 沿 import 图可达的文件里各至少有一处调用；
#   3. 那一屏被渲染，且同一格组件**同时**接到告警列表与确认回调；
#   4. 网关登记了 `POST /api/v1/alerts/{id}/ack`；
#   5. 告警表的读取语句带 `acknowledged_at`/`acknowledged_by`，网关的条目也带；
#   6. `set_alert` 的新 firing 边把两列清空。
#
# 用法：bash deploy/scripts/check-web-alert-panel.sh [仓库根目录]
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
FETCHERS = ("getActiveAlerts", "acknowledgeAlert")
GATEWAY = "crates/cog-gateway/src/lib.rs"
STORE = "crates/cog-observability/src/alert_store.rs"
ENTRY_TYPE = "crates/cog-gateway/src/alert_store.rs"

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这两条路由」无从问起")
client_src = open(client, encoding="utf-8").read()

# ── 1. 路由路径与取它的函数 ──
if "/api/v1/alerts/active" not in client_src:
    sys.exit(f"{CLIENT} 里没有 /api/v1/alerts/active：前端这一侧没有声明活跃告警的路由")
# 确认走的是 `/api/v1/alerts/<id>/ack`，路径在模板串里被拆成两段，问的是拼起来
# 之后的那条路由，不是某个字面量。
if not re.search(r"/api/v1/alerts/\$\{[^}]*\}/ack", client_src):
    sys.exit(f"{CLIENT} 里没有 /api/v1/alerts/<id>/ack：确认按钮没有服务端的落点")
for fn in FETCHERS:
    if not re.search(rf"export\s+(?:async\s+)?function\s+{fn}\b", client_src):
        sys.exit(f"{CLIENT} 没有导出 {fn}：路径在了，但没有人能取它")

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
    if not os.path.isfile(os.path.join(WEB, ENTRY)):
        sys.exit(f"读不到入口 {ENTRY}——可达性无从算起")
    seen, queue = set(), [ENTRY]
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

# ── 2. 每个取数函数都有可达的调用点 ──
callers = {}
for fn in FETCHERS:
    found = [
        rel
        for rel in sorted(graph)
        if rel != CLIENT and re.search(rf"\b{fn}\s*\(", read(rel))
    ]
    if not found:
        sys.exit(
            f"从 {ENTRY} 可达的源码里没有任何一处调用 {fn}："
            "这条路由没有活的消费者（死文件里的调用点不算，它不可达）"
        )
    callers[fn] = found

# ── 3. 那一屏被渲染，且同时接上列表与确认回调 ──
screens = [
    (rel, name)
    for rel in sorted(graph)
    for name in re.findall(
        r"export\s+(?:default\s+)?function\s+([A-Z]\w+)\s*\(", read(rel)
    )
    if any(re.search(rf"<{name}[\s/>]", read(other)) for other in graph)
]

wired = []
for rel in set(callers["getActiveAlerts"]) | set(callers["acknowledgeAlert"]):
    for screen_rel, _ in screens:
        if screen_rel == rel:
            continue
        for tag in re.findall(r"<([A-Z]\w+)((?:\s[^<>]*)?)/?>", read(screen_rel), re.S):
            if "alerts=" in tag[1] and "acknowledge=" in tag[1]:
                wired.append((screen_rel, tag[0], rel))
if not wired:
    sys.exit(
        "没有一格渲染出来的组件同时接到 alerts= 与 acknowledge=："
        "告警列表与确认按钮必须落在同一屏上——只接列表就是「看得见、动不了」，"
        "只接回调就是「点了看不见」"
    )

# ── 4. 网关登记了确认路由 ──
gw = os.path.join(ROOT, GATEWAY)
if not os.path.isfile(gw):
    sys.exit(f"读不到 {GATEWAY}")
gw_src = open(gw, encoding="utf-8").read()
if not re.search(r'"/api/v1/alerts/\{id\}/ack"', gw_src):
    sys.exit(f"{GATEWAY} 没有登记 /api/v1/alerts/{{id}}/ack：界面点下去只有 404")
if "acknowledge_alert" not in gw_src:
    sys.exit(f"{GATEWAY} 没有调用 acknowledge_alert：路由在，写入不在")

# ── 5. 确认标记必须被读出来 ──
store = os.path.join(ROOT, STORE)
if not os.path.isfile(store):
    sys.exit(f"读不到 {STORE}")
store_src = open(store, encoding="utf-8").read()
for col in ("acknowledged_at", "acknowledged_by"):
    if f"ADD COLUMN IF NOT EXISTS {col}" not in store_src:
        sys.exit(f"{STORE}: 表结构里没有 {col}——确认没有地方落盘")
    selects = len(re.findall(rf"last_seen_at,\s*\\\s*\n\s*acknowledged_at, acknowledged_by FROM alerts", store_src))
    if selects < 2:
        sys.exit(
            f"{STORE}: 清单的 SELECT 里没有 {col}（活跃与历史两处都要带）——"
            "确认写进库了，读的那一面看不见，刷新后又变回未确认"
        )
entry = os.path.join(ROOT, ENTRY_TYPE)
entry_src = open(entry, encoding="utf-8").read()
if "pub acknowledged_at" not in entry_src or "pub acknowledged_by" not in entry_src:
    sys.exit(
        f"{ENTRY_TYPE}: 网关的告警条目没有 acknowledged_at/by——"
        "库里有标记，接口层把它丢了，前端永远收不到"
    )

# ── 6. 重新 firing 必须清掉标记 ──
fired = re.search(
    r"AlertTransition::Fired => \{(.*?)Ok\(AlertTransition::Fired\)", store_src, re.S
)
if not fired:
    sys.exit(f"{STORE}: 找不到 set_alert 的 Fired 分支——本门问的那一段无从检查")
if "acknowledged_at = NULL" not in fired.group(1):
    sys.exit(
        f"{STORE}: 新 firing 边没有清掉 acknowledged_at——"
        "同一个 dedup_key 复用一行，上一次发作的确认会盖在新一次上，"
        "人会以为这条已经有人在看"
    )

shown = ", ".join(f"{s} ← {tag} ← {rel}" for s, tag, rel in sorted(set(wired)))
print(f"ok：告警列表与确认各有着落，渲染在 {shown}；确认写完可读、重新 firing 会清")
PYEOF
