#!/usr/bin/env bash
# 「观测面是一排工具板」的消费者门禁。
#
# 观测面曾经是一张从上滚到下的大表：概况、拓扑、监管者、后端、工作负载、模型用量
# 六段叠在一屏里，读的人要找「trace 在哪」得滚到底，而**事件回放根本没有读者**——
# `/api/v1/events` 这条历史查询一直在，界面上从没有谁读它。有了通路没有消费者，
# 和没有通路是一回事。
#
# 判据：
#   1. 观测面有一条板条（`BOARDS`），七块板都在里面，且每块都真的被渲染
#      （`{board === '…' && <XBoard />}`）——写在名单里而没人渲染，等于没有这块板；
#   2. 每块板从 `main.tsx` 可达，并且它读自己的路由：client.ts 里有那个取数函数、
#      有那条路径；
#   3. 每块板把那句话的边界说出来：
#      - 事件回放：窗口是最近 24 小时、一次最多 1000 条，读满要算截断；
#      - 事件回放走 `normalizeAgentEvent` 归一——与实时流同一个函数，两处各写一份
#        会把同一帧说成两件事；
#      - 日志：`count` 是这一页的行数，不是归档总量；
#      - trace：读回的帧数与轨迹自报的帧数不一致时要写出来；
#   4. 模型花销不在这一面再算一遍——它有自己的面（Token），两处各算一次会读出两个数。
#
# 用法：bash deploy/scripts/check-web-observability-boards.sh [仓库根目录]
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
VIEW = "views/ObservabilityView.tsx"

# 板 id → (组件名, 模块, 取数函数, 路由)
BOARDS = {
    "overview": ("OverviewBoard", "views/ObservabilityView.tsx", "getClusterOverview", "/api/v1/cluster/overview"),
    "topology": ("ClusterTopology", "topology/ClusterTopology.tsx", "getClusterTopology", "/api/v1/cluster/topology"),
    "metrics": ("MetricsBoard", "observability/MetricsBoard.tsx", "getEvolutionMetrics", "/api/v1/evolution/metrics"),
    "logs": ("LogsBoard", "observability/LogsBoard.tsx", "listRawLogs", "/api/v1/raw_logs"),
    "traces": ("TracesBoard", "observability/TracesBoard.tsx", "listTraces", "/api/v1/traces"),
    "events": ("EventsBoard", "observability/EventsBoard.tsx", "listEvents", "/api/v1/events"),
    "probes": ("ProbesBoard", "observability/ProbesBoard.tsx", "getBackendHealth", "/api/v1/health/backends"),
}

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")
client_path = os.path.join(WEB, CLIENT)
if not os.path.isfile(client_path):
    sys.exit(f"读不到 {CLIENT}")
client_src = open(client_path, encoding="utf-8").read()


def read(rel):
    path = os.path.join(WEB, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 {rel}")
    return open(path, encoding="utf-8").read()


IMPORT = re.compile(r"""^\s*(?:import|export)\b[^'"]*?from\s+['"]([^'"]+)['"]""", re.M)
EXT = ("", ".ts", ".tsx", "/index.ts", "/index.tsx")


def resolve(rel, spec):
    base = spec[2:] if spec.startswith("@/") else os.path.normpath(os.path.join(os.path.dirname(rel), spec))
    for ext in EXT:
        if os.path.isfile(os.path.join(WEB, base + ext)):
            return base + ext
    return None


def reachable():
    if not os.path.isfile(os.path.join(WEB, ENTRY)):
        sys.exit(f"读不到入口 {ENTRY}")
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
view_src = read(VIEW)
problems = []

# ── 1. 板条上有七块板，每块都被渲染 ──
listed = set(re.findall(r"id:\s*'([a-z]+)'", view_src))
for board in BOARDS:
    if board not in listed:
        problems.append(f"{VIEW}: 板条里没有 {board} 这块板")
for board, (component, _module, _fetcher, _route) in BOARDS.items():
    if not re.search(rf"board\s*===\s*'{board}'\s*&&\s*<{component}\b", view_src):
        problems.append(
            f"{VIEW}: {board} 这块板没有渲染（`{{board === '{board}' && <{component} />}}`）——"
            "名单里有、屏上没有，等于没有这块板"
        )

# ── 2/3. 每块板可达、读自己的路由，并把自己那句话的边界说出来 ──
for board, (component, module, fetcher, route) in BOARDS.items():
    if module not in graph:
        problems.append(f"{module}: 从 {ENTRY} 不可达——{board} 板在屏上不会出现")
        continue
    if not re.search(rf"export\s+(?:async\s+)?function\s+{fetcher}\b", client_src):
        problems.append(f"{CLIENT}: 没有导出 {fetcher}——{board} 板取不到自己的读数")
    if route not in client_src:
        problems.append(f"{CLIENT}: 没有声明 {route}——{board} 板读的路径不在前端这一侧")

events = read(BOARDS["events"][1])
for word in ("1000", "24"):
    if word not in events:
        problems.append(
            f"{BOARDS['events'][1]}: 没有写出窗口边界（{word}）——24 小时与 1000 条上限不写出来，"
            "「读回 N 条」会被当成「一共发生了 N 件」"
        )
if "normalizeAgentEvent" not in events:
    problems.append(
        f"{BOARDS['events'][1]}: 历史事件没有走 normalizeAgentEvent——自己再写一份帧映射，"
        "同一帧在直播与回放里会被说成两件事"
    )
if not re.search(r">=\s*1000", events):
    problems.append(f"{BOARDS['events'][1]}: 读满 1000 条时没有判截断——上限被当成了总数")

logs = read(BOARDS["logs"][1])
if "这一页" not in logs:
    problems.append(
        f"{BOARDS['logs'][1]}: 没有把 count 说成「这一页」的行数——它是这一页返回的行数，"
        "不是归档总量"
    )

traces = read(BOARDS["traces"][1])
if not re.search(r"trace\.events\.length\s*<\s*trace\.event_count", traces):
    problems.append(
        f"{BOARDS['traces'][1]}: 没有比较读回的帧数与轨迹自报的帧数——"
        "少读了帧却按全量展示，分布就是编的"
    )

# ── 4. 模型花销不在这一面再算一遍 ──
if "getTokenUsage" in view_src:
    problems.append(
        f"{VIEW}: 又在观测面里读了一次模型用量——它有自己的一面（Token），"
        "两处各算一次，同一段时间会读出两个数"
    )

if problems:
    sys.exit("观测面的工具板不合格：\n  " + "\n  ".join(problems))

print(
    "ok：观测面是七块工具板（"
    + "、".join(BOARDS)
    + "），每块从入口可达、读自己的路由，事件回放写明 24 小时 / 1000 条上限并走同一个帧归一函数"
)
PYEOF
