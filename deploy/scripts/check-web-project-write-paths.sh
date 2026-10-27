#!/usr/bin/env bash
# 「项目面能在界面上被改」的消费者门禁。
#
# 项目面的成品不是四个读路由，是**客户看见这棵树拆错了、当场把它改对**。四个读面
# 写得越细，这件事缺得越刺眼：一张能读到每一层深度与每一个归因缺口的树，读者会
# 默认它是有人在维护的。
#
# 这条门判的是「写入口从界面可达」这一条链，链上每一段都能单独断而不让别的东西红：
#   1. `client.ts` 声明了写端点的取数函数——没有它，界面上无从发起；
#   2. 函数只被声明是没用的：`projectWrite.tsx` 要真的 import 并在某个组件里调用；
#   3. `ProjectsView.tsx` 要真的把那些组件画出来（引了不用等于没接）；
#   4. 写完要重读：每个写组件都收一个 `onDone`/`onCreated` 并在**成功之后**调它。
#      不重读的写，界面上会留着旧数——读者会以为写没生效，再点一次；
#   5. 看板要带卡片：只有计数的列画不出能挪的卡（服务端 `BoardCard` ＋ 客户端
#      `cards` ＋ 渲染 `c.cards.map` 三段齐）；
#   6. 服务端四条写路由都在（`/api/v1/projects`、`/projects/{id}/tasks`、
#      `/tasks/{id}/plan`、`/tasks/{id}/move`），以及审计时间线 `/projects/{id}/audit`。
#
# 用法：bash deploy/scripts/check-web-project-write-paths.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os
import re
import sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = os.path.join(WEB, "api/client.ts")
WRITE = os.path.join(WEB, "views/projectWrite.tsx")
VIEW = os.path.join(WEB, "views/ProjectsView.tsx")
GATEWAY = os.path.join(ROOT, "crates/cog-gateway/src/lib.rs")
PROJECTS_RS = os.path.join(ROOT, "crates/cog-gateway/src/projects.rs")

for path in (CLIENT, WRITE, VIEW, GATEWAY, PROJECTS_RS):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {os.path.relpath(path, ROOT)}")

client = open(CLIENT, encoding="utf-8").read()
write = open(WRITE, encoding="utf-8").read()
view = open(VIEW, encoding="utf-8").read()
gateway = open(GATEWAY, encoding="utf-8").read()
projects = open(PROJECTS_RS, encoding="utf-8").read()


def die(msg):
    sys.exit(msg)


# ── 1. 取数函数 ──
WRITERS = {
    "createProject": "POST /api/v1/projects",
    "createProjectTask": "POST /api/v1/projects/{id}/tasks",
    "updateTask": "PATCH /api/v1/tasks/{id}",
    "saveTaskPlan": "PUT /api/v1/tasks/{id}/plan",
    "moveTask": "POST /api/v1/tasks/{id}/move",
}
for fn in WRITERS:
    if not re.search(rf"export\s+async\s+function\s+{fn}\b", client):
        die(f"client.ts 没有导出 {fn}：{WRITERS[fn]} 这一端没有前端取数")
for fn in ("getProjectAudit", "downloadProjectCsv"):
    if not re.search(rf"export\s+async\s+function\s+{fn}\b", client):
        die(f"client.ts 没有导出 {fn}：改动时间线与导出在界面上无从取")

# ── 2. 写组件 import 并调用 ──
for fn in WRITERS:
    # 按**那段 import 语句**判，不按「名字单独占一行」判：后者在有人把 import 排成
    # 一行时会把一条正确的链判红，而它红的样子与「真的没 import」一模一样。
    if not re.search(
        rf"import\s*\{{[^}}]*\b{fn}\b[^}}]*\}}\s*from\s*['\"]@/api/client['\"]",
        write,
        re.S,
    ):
        die(f"projectWrite.tsx 没有从 client 里 import {fn}：函数声明了，没有人在调")
    if not re.search(rf"\b{fn}\(", write):
        die(f"projectWrite.tsx 只 import 了 {fn} 却没调它：写入口没接通")

# ── 3. 视图真的画出来 ──
for comp in ("NewProjectForm", "NewTaskForm", "ReparentControl", "PlanButton",
             "MoveCardControl", "ExportCsvButton", "AuditTimeline"):
    if comp not in view:
        die(f"ProjectsView.tsx 里没有 {comp}：组件写好了，界面上没有它")
    if not re.search(rf"<{comp}\b", view):
        die(f"ProjectsView.tsx 只 import 了 {comp}，没有把它渲染出来")

# ── 4. 写完重读 ──
for prop in ("onDone", "onCreated"):
    if write.count(prop) < 4:
        die(f"projectWrite.tsx 里 {prop} 出现 {write.count(prop)} 次：写完之后要重读，"
            f"不重读的写会留下旧数，读者会以为没生效")
for comp in ("MoveCardControl", "PlanForm", "NewTaskForm", "NewProjectForm", "ReparentControl"):
    start = write.find(f"export function {comp}")
    if start < 0:
        die(f"读不到 {comp} 的定义")
    # 函数体取到下一个顶层 `export ` 为止：按「第一个行首右括号」切会停在函数里
    # 第一个内层块的结尾，于是回调写在后面的组件看起来像从没调过它。
    nxt = write.find("\nexport ", start + 1)
    body = write[start:] if nxt < 0 else write[start:nxt]
    if not re.search(r"on(Done|Created)\(", body):
        die(f"{comp} 收了回调却从没调它：写成功之后读数不会重读")

# ── 5. 看板卡片三段齐 ──
if not re.search(r"pub\s+struct\s+BoardCard\b", projects):
    die("projects.rs 里没有 BoardCard：看板只回计数时，界面画不出能挪的卡")
if "cards_hidden" not in projects:
    die("projects.rs 的看板没报 cards_hidden：截断之后「还有几张」没人说得出来")
if not re.search(r"cards:\s*BoardCard\[\]", client):
    die("client.ts 的 BoardColumn 没有 cards：服务端带了，前端读不到")
if "c.cards.map" not in view:
    die("ProjectsView.tsx 没有渲染 c.cards：卡片带回来了，屏上还是没有")

# ── 6. 服务端路由 ──
for route in (
    '"/api/v1/projects"',
    '"/api/v1/projects/{id}/tasks"',
    '"/api/v1/tasks/{id}/plan"',
    '"/api/v1/tasks/{id}/move"',
    '"/api/v1/projects/{id}/audit"',
):
    path = route.strip('"')
    # 按「整条路径字面量」判，不按裸子串判：`/tasks/{id}/move` 是
    # `/tasks/{id}/moveX` 的子串，子串判据在路由被改名那一刻仍然是绿的。
    if not re.search(re.escape(f'"{path}"') + r"|'" + re.escape(path) + r"'", gateway):
        die(f"网关没有登记 {path}：前端在调的这条路不存在")

print("项目面写入口：取数函数、可达组件、写完重读、看板卡片、服务端路由，五段齐")
PYEOF
