#!/usr/bin/env bash
# 「集群拓扑」读面的消费者门禁。
#
# `/api/v1/cluster/topology` 的成品不是那份 JSON，是**有人打开就能从集群点到
# 节点、再点到 Pod 和容器**的那一屏。这一面坏起来没有症状：路由在、数据全、
# 组件也在，只是它把话说错了。实测错过的四种：
#   1. 节点读失败（权限不够）时，节点列表是空的，而 Pod 全都写着自己在哪台机器上——
#      旧写法把它们一次性塞进「没有落在任何节点上的 Pod」，整屏假话；
#   2. Job 跑完了是 Succeeded，旧写法按「不是 Running 就是红点」，十几个完成的 Job
#      把真正 Failed 的那两个淹掉；
#   3. Pod 行不显示命名空间，读的人以为这一屏是整台集群（它其实只读一个命名空间）；
#   4. 集群那一行的名字是接口占位 `in-cluster`。
#
# 判据：
#   1. `client.ts` 声明 `/api/v1/cluster/topology` 并导出 `getClusterTopology`；
#   2. 从 `main.tsx` 可达的源码里有一处调用它，且有一格渲染出来的组件用得上；
#   3. 那一屏渲染了 `unplaced`（节点没读到的那一栏）——不渲染它，这些 Pod 就等于
#      从屏上消失；
#   4. 那一屏显示每个 Pod 的命名空间；
#   5. 那一屏按阶段判颜色，且 `Succeeded` 被单独判过（不是「非 Running 即红」）；
#   6. 完成的与失败的分开数；
#   7. 服务端 `place_pods` 真的把「写着在节点上、但没匹配到节点」的 Pod 放进
#      `unplaced`，且有一条按名字守这件事的测试。
#
# 用法：bash deploy/scripts/check-web-topology-panel.sh [仓库根目录]
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
ROUTE = "/api/v1/cluster/topology"
FETCHER = "getClusterTopology"
SERVER = "crates/cog-gateway/src/cluster.rs"
SERVER_TEST = "a_pod_on_a_node_that_was_not_read_is_not_called_unscheduled"

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这条路由」无从问起")
client_src = open(client, encoding="utf-8").read()

# ── 1. 路由与取它的函数 ──
if ROUTE not in client_src:
    sys.exit(f"{CLIENT} 里没有 {ROUTE}：前端这一侧没有声明拓扑读取")
if not re.search(rf"export\s+(?:async\s+)?function\s+{FETCHER}\b", client_src):
    sys.exit(f"{CLIENT} 没有导出 {FETCHER}：路径在了，但没有人能取它")

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


def direct_imports(rel):
    out = set()
    for spec in IMPORT.findall(read(rel)):
        target = resolve(rel, spec)
        if target and target in graph:
            out.add(target)
    return out


def imported_by(src_chain, dst):
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


screens = [
    (rel, name)
    for rel in sorted(graph)
    for name in re.findall(r"export\s+function\s+([A-Z]\w+)\s*\(", read(rel))
    if any(re.search(rf"<{name}[\s/>]", read(other)) for other in graph)
]

problems = []

# ── 2. 可达的调用点与渲染出来的读者 ──
# 这一面的取数函数是**交给 hook 的引用**（`useResource('…', getClusterTopology, …)`），
# 不是直接调用，所以判的是「import 之外出现过这个名字」，再要求它被一格渲染出来的
# 组件用上——只有 import 不算。
def uses_fetcher(src):
    stripped = IMPORT.sub("", src)
    return bool(re.search(rf"\b{FETCHER}\b", stripped))


callers = [rel for rel in sorted(graph) if rel != CLIENT and uses_fetcher(read(rel))]
if not callers:
    sys.exit(
        f"从 {ENTRY} 可达的源码里没有任何一处调用 {FETCHER}："
        "这条路由没有活的消费者（死文件里的调用点不算，它不可达）"
    )

readers = [s for s in screens if any(imported_by([s[0]], rel) for rel in callers)]
if not readers:
    sys.exit(
        f"{', '.join(callers)}: 调用了 {FETCHER}，但可达图里没有一格渲染出来的组件用到它"
        "（只有 import、没有渲染不算读者）"
    )

screen_files = sorted({s[0] for s in readers} | set(callers))
shown = "".join(read(f) for f in screen_files)
where = ", ".join(screen_files)

# ── 3. 节点没读到的 Pod 有自己的一栏 ──
if "unplaced" not in shown:
    problems.append(
        f"{where}: 没有渲染 unplaced——节点列表读失败时，写着「在某台节点上」的 Pod "
        "会从屏上消失（或者被塞进「没有落在任何节点上」，那是一句假话）"
    )

# ── 4. 每个 Pod 的命名空间看得见 ──
if not re.search(r"\bpod\.namespace\b", shown):
    problems.append(
        f"{where}: Pod 行没有显示 pod.namespace——这一面只读一个命名空间，"
        "不写出来的那一屏会被读成整台集群"
    )

# ── 5. 阶段颜色把 Succeeded 单独判过 ──
phase_fn = re.search(r"function\s+phaseTone\s*\([^)]*\)[^{]*\{(.*?)\n\}", shown, re.S)
if not phase_fn:
    problems.append(
        f"{where}: 没有 phaseTone 这样的判色函数——按「不是 Running 就红」判，"
        "跑完的 Job 会被画成失败"
    )
elif "'Succeeded'" not in phase_fn.group(1) and '"Succeeded"' not in phase_fn.group(1):
    problems.append(
        f"{where}: 判色函数里没有单独判 Succeeded——完成的 Job 会被画成红点，"
        "真正的 Failed 就淹在里面"
    )
elif not re.search(r"\bphaseTone\s*\(", shown.split("function phaseTone")[1]):
    problems.append(f"{where}: phaseTone 定义了却没人调用——判色还是原来那一行")

# ── 6. 完成的与失败的分开数 ──
for field in ("finished", "failed"):
    if not re.search(rf"\b{field}\s*:", shown):
        problems.append(
            f"{where}: 没有把 {field} 单独数出来——「Pod N」把跑完的和正在跑的加在一起，"
            "读的人会以为有 N 个东西在跑"
        )

# ── 7. 服务端：节点没匹配上的 Pod 进 unplaced，并有测试守着 ──
server = os.path.join(ROOT, SERVER)
if not os.path.isfile(server):
    sys.exit(f"读不到 {SERVER}")
server_src = open(server, encoding="utf-8").read()
if not re.search(r"pub\s+unplaced:\s*Vec<PodView>", server_src):
    problems.append(f"{SERVER}: ClusterView 没有 unplaced 这一栏")
place = re.search(r"fn\s+place_pods\s*\(.*?\n\}", server_src, re.S)
if not place:
    problems.append(f"{SERVER}: 找不到 place_pods——本门问的那一段无从检查")
elif not re.search(r"unplaced\s*\.\s*push", place.group(0)):
    problems.append(
        f"{SERVER}: place_pods 没有把「写着在节点上、但没匹配到节点」的 Pod 放进 unplaced"
    )
if SERVER_TEST not in server_src:
    problems.append(
        f"{SERVER}: 没有测试 {SERVER_TEST}——这条分流可以被人改回去而不让任何东西红"
    )

if problems:
    sys.exit("拓扑读面的消费者不合格：\n  " + "\n  ".join(problems))

print(
    "ok：拓扑读取有活的消费者（"
    + ", ".join(f"{s[0]} ← {s[1]}" for s in readers)
    + "），未读到的节点与未调度的 Pod 分栏、阶段按 Succeeded 判色、命名空间看得见"
)
PYEOF
