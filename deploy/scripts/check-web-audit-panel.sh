#!/usr/bin/env bash
# 「审计」读面的消费者门禁。
#
# 审计面的成品不是 `/api/v1/audit/events` 这个路由，是**有人打开那一屏、看见那一
# 段时间里留了什么、按触发类型或智能体筛一遍**。这里有好几种不出声的坏形状：
#   1. 视图文件写好了，但活动栏、页标题、视图注册表里没有它——文件在树上，
#      从界面上永远打不开，谁也不会失败；
#   2. 取数函数留在 `client.ts` 里没人调，或者调了却不传筛选条件——输入框画在
#      屏上，请求里永远是全量，读者以为筛过了；
#   3. 网关的 handler 收下了参数却没用，服务端永远回同一段；
#   4. 读取不再只读：这一页多出一个写口，一张能被它改写的表就不算审计表。
#
# 判据：
#   1. `client.ts` 声明了 `/api/v1/audit/events` 并导出 `listAuditEvents`；
#   2. 从 `main.tsx` 可达的源码里有一处调用它，且这一处把筛选条件传进了请求
#      （`trigger_type`/`agent_id`/`since` 至少出现在调用点所在的文件里）；
#   3. 这一面在四个登记处都齐：`ActivityId` 联合类型、`VIEW_TITLES`、
#      活动栏的 `ITEMS`、`registry.tsx` 的 `case`；
#   4. 网关登记了 `/api/v1/audit/events`，且 handler 把四个筛选参数都交给了归档层；
#   5. 这一页只读：视图里没有 `.put(`/`.post(`/`.delete(` 这类写调用。
#
# 用法：bash deploy/scripts/check-web-audit-panel.sh [仓库根目录]
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
FETCHER = "listAuditEvents"
ROUTE = "/api/v1/audit/events"
GATEWAY = "crates/cog-gateway/src/lib.rs"
HANDLER = "crates/cog-gateway/src/audit.rs"

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这条路由」无从问起")
client_src = open(client, encoding="utf-8").read()

# ── 1. 路由与取它的函数 ──
if ROUTE not in client_src:
    sys.exit(f"{CLIENT} 里没有 {ROUTE}：前端这一侧没有声明审计读取")
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

# ── 2. 取数函数有可达的调用点，且筛选条件真的进了请求 ──
callers = [
    rel for rel in sorted(graph) if rel != CLIENT and re.search(rf"\b{FETCHER}\s*\(", read(rel))
]
if not callers:
    sys.exit(
        f"从 {ENTRY} 可达的源码里没有任何一处调用 {FETCHER}："
        "这条路由没有活的消费者（死文件里的调用点不算，它不可达）"
    )
filters = ("trigger_type", "agent_id", "since")


def call_args(src, name):
    """
    调用点括号里的那一段（按括号配平，能跨行）。

    不能问「这个名字在这个文件里出现过没有」：响应里也有 `row.trigger_type`
    这一列，把参数从请求里删掉之后那个字段还在，门照样绿。问的必须是**传给
    请求的那一段**。
    """
    out = []
    for match in re.finditer(rf"\b{name}\s*\(", src):
        depth, start = 0, match.end() - 1
        for index in range(start, len(src)):
            if src[index] == "(":
                depth += 1
            elif src[index] == ")":
                depth -= 1
                if depth == 0:
                    out.append(src[start : index + 1])
                    break
    return out


unwired = []
for rel in callers:
    src = read(rel)
    args = call_args(src, FETCHER)
    if not any(all(name in arg for name in filters) for arg in args):
        unwired.append(rel)
if unwired:
    sys.exit(
        f"{', '.join(unwired)} 调了 {FETCHER} 却没收齐 {', '.join(filters)}："
        "筛选框画在屏上、请求里永远是全量，读者会以为已经筛过了"
    )

# ── 3. 这一面在四个登记处都齐 ──
store = os.path.join(WEB, "shell/store.ts")
bar = os.path.join(WEB, "shell/ActivityBar.tsx")
registry = os.path.join(WEB, "views/registry.tsx")
for path in (store, bar, registry):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {path}")
store_src, bar_src, registry_src = (open(p, encoding="utf-8").read() for p in (store, bar, registry))
if not re.search(r"ActivityId\s*=\s*[^;]*['\"]audit['\"]", store_src, re.S):
    sys.exit("shell/store.ts: ActivityId 里没有 'audit'：这一面没有身份")
if not re.search(r"VIEW_TITLES[^{]*\{[^}]*audit\s*:", store_src, re.S):
    sys.exit("shell/store.ts: VIEW_TITLES 里没有 audit——页签与命令面板里找不到它")
if not re.search(r"\{\s*id:\s*['\"]audit['\"]", bar_src):
    sys.exit("shell/ActivityBar.tsx: 活动栏里没有 audit 这一格——从界面上打不开")
if not re.search(r"case\s+['\"]audit['\"]\s*:\s*return\s*<AuditView", registry_src):
    sys.exit("views/registry.tsx: 没有 case 'audit' → <AuditView：开了页也没有组件画")

# ── 4. 网关：路由在，四个筛选参数都交给了归档层 ──
gw = os.path.join(ROOT, GATEWAY)
handler = os.path.join(ROOT, HANDLER)
for path in (gw, handler):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {path}")
gw_src = open(gw, encoding="utf-8").read()
handler_src = open(handler, encoding="utf-8").read()
if f'"{ROUTE}"' not in gw_src:
    sys.exit(f"{GATEWAY} 没有登记 {ROUTE}：审计页打下去只有 404")
query = re.search(r"pub async fn list_audit_events_handler\((.*?)\n\}", handler_src, re.S)
if not query:
    sys.exit(f"{HANDLER}: 找不到 list_audit_events_handler——本门问的那一段无从检查")
body = query.group(1)
for name in filters:
    if f"query.{name}" not in body:
        sys.exit(
            f"{HANDLER}: handler 没有把 query.{name} 交给归档层——"
            "前端筛了，服务端照样回全量，屏幕上会出现「筛过了」的假象"
        )

# ── 5. 这一页只读 ──
view = [rel for rel in callers if rel.endswith(".tsx")]
for rel in view:
    src = read(rel)
    if re.search(r"\bapi\.(put|post|delete)\s*\(|api\.sendBytes\s*\(", src):
        sys.exit(f"{rel}: 审计页里有写调用——一张能被这一页改写的表不算审计表")

print(f"ok：审计读取有活的消费者（{', '.join(sorted(set(view)))}），四个登记处齐，服务端四个筛选参数都用上，页面只读")
PYEOF
