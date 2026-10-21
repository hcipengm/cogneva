#!/usr/bin/env bash
# 「站内通知」读面的消费者门禁。
#
# 网关有三条路由（列表、单条已读、全部已读）和一条 WebSocket 推送。通信面在
# 生产上的成品不是这些路由，是**用户打开就能看见这一条通知**的那一屏。
#
# 坏起来没有症状：路由在、契约测试绿、库里也有行，`cogneva_notification_delivery_total`
# 甚至全是 0 也不报错——因为没有出口时那条计数本来就该是 0。真正会丢的是两处：
#   1. 传输层把 `notification` 信封塞进 `normalizeAgentEvent`。通知没有 `type` 字段，
#      那个函数对它的第一句就是 `return null`，于是每一条推送都被**静默丢掉**，
#      而 WebSocket 这条腿看起来完全正常（连接是通的、消息也收到了）。
#   2. 面板整块消失。取数函数还在 `client.ts` 里，没人渲染它，也不会有任何东西失败。
#
# 判据（每一条都问「从入口可达」，不是问文件里有没有某个串）：
#   1. 三条路由的路径由 `web/src/api/client.ts` 声明，且导出取它们的函数。
#   2. 从入口 `web/src/main.tsx` 沿 import 图可达的文件里，每个函数至少有一处调用。
#   3. 列表的调用点必须被一格**渲染出来的组件**用到（可达图里出现它的 JSX 标签）。
#   4. 传输层里 `message.type === 'notification'` 那一支必须调 `onNotification`
#      且**不得**调 `onEvent`——就是上面那一处静默丢弃的形状。
#   5. 推送必须被接上：除传输层与 `useEventStream` 之外，可达源码里还要有一处引用
#      `onNotification`（只把回调定义出来、没人传，等于没有实时腿）。
#
# 用法：bash deploy/scripts/check-web-notification-panel.sh [仓库根目录]
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
ROUTE = "/api/v1/notifications"
FETCHERS = ("listNotifications", "markNotificationRead", "markAllNotificationsRead")
TRANSPORT = "transport/websocket.ts"
STREAM_HOOK = "hooks/useEventStream.ts"
BRANCH = re.compile(r"message\.type\s*===\s*'notification'")

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

# ── 1. 路由路径与取它的函数 ──
client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这几条路由」无从问起")
client_src = open(client, encoding="utf-8").read()
if ROUTE not in client_src:
    sys.exit(f"{CLIENT} 里没有 {ROUTE}：前端这一侧没有声明通知的路由")
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

# ── 3. 列表那一屏必须被渲染出来 ──
def direct_imports(rel):
    out = set()
    for spec in IMPORT.findall(read(rel)):
        target = resolve(rel, spec)
        if target and target in graph:
            out.add(target)
    return out


def imported_by(src, dst):
    """src 经 import 能不能走到 dst。"""
    seen, queue = set(), [src]
    while queue:
        rel = queue.pop()
        if rel in seen:
            continue
        seen.add(rel)
        if rel == dst:
            return True
        queue.extend(direct_imports(rel))
    return False


screens = [
    (rel, name)
    for rel in sorted(graph)
    for name in re.findall(
        r"export\s+(?:default\s+)?function\s+([A-Z]\w+)\s*\(", read(rel)
    )
    if any(re.search(rf"<{name}[\s/>]", read(other)) for other in graph)
]

rendered = []
for rel in callers["listNotifications"]:
    readers = [s for s in screens if imported_by(s[0], rel)]
    if not readers:
        sys.exit(
            f"{rel}: 调用了 listNotifications，但可达图里没有一格渲染出来的组件"
            "用到它（只有 import、没有渲染不算读者）"
        )
    # 取到的列表与未读数还必须**递进一个被渲染的组件**：hooks 取数、App 持有状态、
    # 面板把 items 与 unread_count 画出来——只取不画时上面三条全绿，通知仍然没人看得见。
    handed_over = False
    for screen_rel, _ in readers:
        for tag in re.findall(r"<([A-Z]\w+)((?:\s[^<>]*)?)/?>", read(screen_rel), re.S):
            if "items=" in tag[1] and "unreadCount=" in tag[1]:
                handed_over = True
    if not handed_over:
        sys.exit(
            f"{rel}: 列表取回来了，但没有一格渲染出来的组件同时接到 items 与 unreadCount——"
            "取数这一半在、显示那一半不在"
        )
    rendered.extend(readers)

# ── 4. 传输层不许把通知当 agent 事件丢掉 ──
if TRANSPORT in graph:
    src = read(TRANSPORT)
    m = BRANCH.search(src)
    if not m:
        sys.exit(
            f"{TRANSPORT} 里没有 `message.type === 'notification'` 这一支："
            "服务端推来的通知没有落地的地方"
        )
    # 分支体：从 `{` 到配对 `}`（本文件的分支都是平铺 if/else if，缩进足够判界）。
    start = src.find("{", m.end())
    depth, i = 0, start
    while i < len(src):
        if src[i] == "{":
            depth += 1
        elif src[i] == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    body = src[start : i + 1]
    if "onNotification" not in body:
        sys.exit(
            f"{TRANSPORT}: `notification` 那一支没有调用 onNotification——"
            "推送被收到了、但没有任何人得到它"
        )
    if "onEvent" in body:
        sys.exit(
            f"{TRANSPORT}: `notification` 那一支还在走 onEvent——"
            "通知没有 `type` 字段，agent 事件归一化会把它判成 null 静默丢掉"
        )
else:
    sys.exit(f"可达图里没有 {TRANSPORT}：WebSocket 传输层无从检查")

# ── 5. 推送必须被接上 ──
wired = [
    rel
    for rel in sorted(graph)
    if rel not in (TRANSPORT, STREAM_HOOK) and "onNotification" in read(rel)
]
if not wired:
    sys.exit(
        "可达源码里除传输层与 useEventStream 之外没有任何一处引用 onNotification："
        "实时推送没有接上任何消费者（只有列表轮询，新通知要等下一拍）"
    )

shown = ", ".join(f"{rel} ← {name}" for rel, name in sorted(set(rendered)))
print(f"ok：三条通知路由各有可达调用点，渲染在 {shown}，推送接到 {', '.join(wired)}")
PYEOF
