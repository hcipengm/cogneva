#!/usr/bin/env bash
# 「这一面接到的帧，总得有一条路把它送到」——四个实时面共用的订阅覆盖门禁。
#
# 每个面各有一条 check-web-live-*.sh，判的是「折叠写了、分派调了、画面读了」。四条
# 都绿也不等于屏幕上会动：帧还要**送到**那个页面上。送不到的路只有一条——网关把
# 每种帧发到某个频道族（`agent:` / `task:` / `hooks`），而 App 建连时只订了其中
# 一部分。少订一族，那一族的所有面一起哑掉，而四条面门禁全是绿的、屏幕上也不会
# 有任何一格显示「断了」：静止和「没有事情发生」长得一模一样。
#
# 判据（三段，缺一段这个检查就退化成查字符串）：
#   1. 每个折叠模块（`store/live*.ts`，不带 Store 后缀、且没有 import 的那几个）
#      都必须在下面这张表里登记它靠哪些帧活——新加一个面而忘了登记，这里红。
#   2. 表里登记的帧类型必须真出现在那个模块里，并且真是网关会发的一种：从
#      `crates/cog-gateway/src/websocket_protocol.rs` 的 `event_channels()` 里按
#      match 臂读出「哪种帧走哪个频道族」，查不到就是登记了一个不存在的帧。
#   3. App 建连时订的频道，必须覆盖每个面依赖的那种帧会去的**每一族**。少一族就红。
#
# 局限写在这里，读的人不必猜：第 2 段读的是源码形状（按 `=>` 切臂），它证明「这种帧
# 由 event_channels 分发到某族」，不证明运行期的路由决策；一条帧会去多族时（
# `task_status_change` 带 `hook_event` 走 hooks）按全部族要求订阅，宁可多订。
#
# 用法：bash deploy/scripts/check-web-frame-channels.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
STORE = os.path.join(WEB, "store")

# 每个折叠模块靠哪些帧活。帧类型用线上那串（与 Rust 变体的 snake_case 一致）。
# 值是 `"*"` 的模块靠**每一帧**活（数到达时刻的那种）：它就要求 App 订阅网关能
# 路由的每一个族，少一族，这一格就少算那一族上的一切。
FACES = {
    "liveAgents.ts": ["agent_start", "agent_end", "state_change", "agent_error", "heartbeat"],
    "liveAlerts.ts": ["resource_alert"],
    "liveComms.ts": ["task_status_change"],
    "liveProjects.ts": ["task_status_change"],
    "livePulse.ts": "*",
}

# snake_case → 线上变体名。规则的按规则推，不规则的写在这里。
IRREGULAR = {"re_act_step_start": "ReActStepStart", "re_act_step_end": "ReActStepEnd"}

def variant_of(event_type):
    if event_type in IRREGULAR:
        return IRREGULAR[event_type]
    return "".join(part[:1].upper() + part[1:] for part in event_type.split("_") if part)

def read(path, what):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {what}：{path}")
    return open(path, encoding="utf-8").read()

# ---- 1. 每个折叠模块都登记了，并且登记的帧真在它里面出现 -------------------------
shipped = sorted(
    name
    for name in os.listdir(STORE)
    if re.fullmatch(r"live[A-Za-z]*\.ts", name)
)
for name in shipped:
    source = read(os.path.join(STORE, name), f"web/src/store/{name}")
    if re.search(r"^\s*import\b", source, re.M):
        continue  # 带 import 的是 store 外壳（zustand），折叠函数在它引的那个模块里
    if name not in FACES:
        sys.exit(
            f"store/{name} 是一个折叠模块，但没有登记在 check-web-frame-channels.sh 的表里："
            "没人知道它靠哪些帧活，也就没人知道 App 少订一族时它会不会哑掉"
        )
for name, types in FACES.items():
    source = read(os.path.join(STORE, name), f"web/src/store/{name}")
    if types == "*":
        continue  # 靠每一帧活的模块不点名任何类型
    for event_type in types:
        if f"'{event_type}'" not in source and f'"{event_type}"' not in source:
            sys.exit(
                f"表里说 store/{name} 靠 `{event_type}` 活，可这个模块里没有这串名字："
                "要么表过期了，要么它其实靠别的帧"
            )

# ---- 2. 从网关源码读出「哪种帧走哪个频道族」 ------------------------------------
GW = os.path.join(ROOT, "crates/cog-gateway/src/websocket_protocol.rs")
gw = read(GW, "crates/cog-gateway/src/websocket_protocol.rs")
start = gw.find("pub fn event_channels")
if start < 0:
    sys.exit("websocket_protocol.rs 里找不到 pub fn event_channels：这个门禁的判据没了依据")
end = gw.find("\n}\n", start)
if end < 0:
    sys.exit("event_channels 的函数体没有闭合，读不出 match 臂")
body = gw[start:end]

# 按 `=>` 切臂：每段 `=>` 之前出现的变体属于它之后那段写出的频道。一条臂里可能
# 写出不止一族（`task_status_change` 带 `hook_event` 时走 hooks，其余走 task:），
# 所以这里把这段里出现的每一族都记上——漏记一族，就等于替 App 少要一份订阅。
parts = body.split("=>")
routes = {}
for index in range(1, len(parts)):
    variants = re.findall(r"AgentEvent::([A-Za-z_]+)", parts[index - 1])
    families = set(re.findall(r'format!\("([a-z_]+):', parts[index]))
    if "HOOKS_CHANNEL" in parts[index]:
        families.add("hooks")
    if not families:
        continue
    for variant in variants:
        routes.setdefault(variant, set()).update(families)

if not routes:
    sys.exit("event_channels 里一个频道族都没读出来：切臂的判据失效了（源码形状变了？）")

# ---- 3. App 订的频道必须覆盖每个面依赖的每一族 ----------------------------------
app = read(os.path.join(WEB, "App.tsx"), "web/src/App.tsx")
decl = re.search(r"channels:\s*\[([^\]]*)\]", app)
if not decl:
    sys.exit(
        "App.tsx 里找不到 `channels: [...]`：订阅列表的写法变了，"
        "这个门禁读不到它，四个实时面会不会哑掉就无从判断"
    )
subscribed = re.findall(r"['\"]([^'\"]+)['\"]", decl.group(1))

def covers(subscription, family):
    if subscription == family:
        return True
    if subscription.endswith(":*"):
        return subscription[:-2] == family
    return subscription.startswith(family + ":")

missing = []
for name, types in sorted(FACES.items()):
    if types == "*":
        # 数到达的那一格：网关能路由到的每一族都得订，少一族就少算一族上的一切。
        for family in sorted({f for fams in routes.values() for f in fams}):
            if not any(covers(s, family) for s in subscribed):
                missing.append(f"{name} 靠每一帧，`{family}:` 上的一切都要数，而 App 只订了 {subscribed}")
        continue
    for event_type in types:
        variant = variant_of(event_type)
        families = routes.get(variant)
        if not families:
            sys.exit(
                f"网关的 event_channels 里没有 `{variant}`（表里 store/{name} 靠的 `{event_type}`）："
                "要么这个帧根本不上总线，要么变体改名了——两种都不该静默通过"
            )
        for family in sorted(families):
            if not any(covers(s, family) for s in subscribed):
                missing.append(f"{name} 靠 `{event_type}`，它走 `{family}:`，而 App 只订了 {subscribed}")

if missing:
    sys.exit("订阅覆盖不全（这一族的面会一起哑掉，而屏幕上没有任何一格会显示断了）：\n  - " + "\n  - ".join(missing))

for name, types in sorted(FACES.items()):
    if types == "*":
        fams = sorted({f for fams in routes.values() for f in fams})
        print(f"ok   - store/{name} 靠每一帧 → {', '.join(f + ':' for f in fams)}，App 已订")
        continue
    for event_type in types:
        fams = sorted(routes[variant_of(event_type)])
        print(f"ok   - store/{name} 靠 `{event_type}` → {', '.join(f + ':' for f in fams)}，App 已订")
PYEOF

echo "check-web-frame-channels: 通过"
