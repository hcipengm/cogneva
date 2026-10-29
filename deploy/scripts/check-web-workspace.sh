#!/usr/bin/env bash
# 工作区（文件树 / 搜索 / 配额）的消费者门禁。
#
# 搜索的成品不是 `/api/v1/ide/search` 这个路由，是**在左栏打两个字、看见哪个文件里
# 有、点一下落到那一行**。这三步各有自己的坏形状，全都不出声：
#   1. 路由在、取数函数在 `client.ts` 里没人调——搜索框根本没接上；
#   2. 面板画出来了，但点结果只 `openTab` 不交落点——文件开了，停在第一行，读者
#      还得自己再搜一次才能到那一处；
#   3. 落点交出去了，文件页不消费——`reveal` 一直挂着，下一次打开别的文件时
#      突然跳到上一处命中的位置；
#   4. 后端把「不跟符号链接」「空查询拒绝」这两条判据删掉——搜索能看见根外的文件，
#      或者一个空串把每一行都算成命中。
#
# 判据：
#   1. `client.ts` 声明了 `/api/v1/ide/search` 并导出 `searchWorkspace`；
#   2. 从 `main.tsx` 沿 import 图可达的源码里有一处调用 `searchWorkspace(`；
#   3. 可达的源码里渲染了 `<SearchPanel`；
#   4. 那一格同时 `openTab(` 与 `setReveal(`，文件页消费 `reveal`；
#   5. 网关登记了 `/api/v1/ide/search`，且 `ide.rs` 里 `search_workspace` 仍在
#      跳过符号链接、仍拒绝空查询。
#   6. 配额读数摆到了屏幕上：`client.ts` 声明 `/api/v1/ide/usage` 并导出
#      `getWorkspaceUsage`，从 `main.tsx` 可达的组件调它并渲染 `<WorkspaceUsage`。
#      ——上限在服务端是一条硬判据，撞上就拒写；只有撞上那一刻才看得见上限，
#      读者会写了一下午才被拦下。
#   7. 网关登记了 `/api/v1/ide/usage`，且 `write_handler` / `upload_handler` 在落盘
#      前仍在过 `check_capacity`——配额只画在屏幕上、写之前不问，等于没上限。
#   8. 保存是「读到的这一份还在不在」的比较交换：`saveWorkspaceFile` 收第三个参数
#      （底稿指纹），文件页把它带上、并且真的处理 409；网关 `write_handler` 在写
#      之前仍过 `ensure_unchanged`。
#      ——工作区里不止一个写者（另一个会话、智能体的写工具），整份覆盖不问一句
#      就会静默抹掉对方刚写下去的东西，屏幕上还什么异常都看不出来。
#
# 用法：bash deploy/scripts/check-web-workspace.sh [仓库根目录]
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
FETCHER = "searchWorkspace"
GATEWAY = "crates/cog-gateway/src/lib.rs"
IDE = "crates/cog-gateway/src/ide.rs"

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")

client = os.path.join(WEB, CLIENT)
if not os.path.isfile(client):
    sys.exit(f"读不到 {CLIENT}——没有它，「谁在读这条路由」无从问起")
client_src = open(client, encoding="utf-8").read()

# ── 1. 路由与取它的函数 ──
if "/api/v1/ide/search" not in client_src:
    sys.exit(f"{CLIENT} 里没有 /api/v1/ide/search：前端这一侧没有声明工作区搜索")
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

# ── 2. 取数函数有可达的调用点 ──
callers = [
    rel for rel in sorted(graph) if rel != CLIENT and re.search(rf"\b{FETCHER}\s*\(", read(rel))
]
if not callers:
    sys.exit(
        f"从 {ENTRY} 可达的源码里没有任何一处调用 {FETCHER}："
        "搜索框没有活的消费者（死文件里的调用点不算，它不可达）"
    )

# ── 3. 面板被渲染 ──
# 只在 .tsx 里找 JSX：`Promise<SearchPanel>` 这种类型参数也长得像 `<SearchPanel`。
rendered = [
    rel
    for rel in sorted(graph)
    if rel.endswith(".tsx") and re.search(r"<SearchPanel[\s/>]", read(rel))
]
if not rendered:
    sys.exit("没有任何可达的组件渲染 <SearchPanel：面板在文件里，但不在任何一屏上")

# ── 4. 点结果要交出落点，文件页要消费它 ──
panel = [rel for rel in callers if "openTab(" in read(rel) and "setReveal(" in read(rel)]
if not panel:
    sys.exit(
        "调用点所在的组件没有同时 openTab( 与 setReveal(："
        "只开页会把文件停在第一行，读者还得自己再找一次那一处"
    )

screens = [rel for rel in graph if "searchWorkspace(" not in read(rel) and "<FilesView" in read(rel)]
consumed = [
    rel
    for rel in graph
    if re.search(r"reveal\b[^;]*\.path|reveal\.path", read(rel))
    and "setReveal(null)" in read(rel)
]
if not consumed:
    sys.exit(
        "没有一处文件页消费 reveal（读到正文后按行号定位并 setReveal(null)）："
        "落点不被消费就会一直挂着，下一次打开别的文件会突然跳到上一处"
    )
if not screens:
    sys.exit("没有一屏同时渲染 <FilesView——文件页不该在没有它的地方被消费")

# ── 5. 后端：路由在，两条判据在 ──
gw = os.path.join(ROOT, GATEWAY)
if not os.path.isfile(gw):
    sys.exit(f"读不到 {GATEWAY}")
gw_src = open(gw, encoding="utf-8").read()
if '"/api/v1/ide/search"' not in gw_src:
    sys.exit(f"{GATEWAY} 没有登记 /api/v1/ide/search：搜索框打下去只有 404")

ide = os.path.join(ROOT, IDE)
if not os.path.isfile(ide):
    sys.exit(f"读不到 {IDE}")
ide_src = open(ide, encoding="utf-8").read()
if "fn search_workspace" not in ide_src:
    sys.exit(f"{IDE} 里没有 search_workspace：路由指向一个不存在的判据")
if "is_symlink()" not in ide_src:
    sys.exit(
        f"{IDE}: search_workspace 没有跳过符号链接——"
        "顺着链接走等于让被搜索的内容决定搜索能看见什么"
    )
if "non-empty q" not in ide_src:
    sys.exit(f"{IDE}: 空查询没有被拒绝——一个空串会把每一行都算成命中")

# ── 6. 配额读数：取数函数有可达的调用点，组件被渲染 ──
if "/api/v1/ide/usage" not in client_src:
    sys.exit(f"{CLIENT} 里没有 /api/v1/ide/usage：屏幕上不会出现「还剩多少」")
if not re.search(r"export\s+(?:async\s+)?function\s+getWorkspaceUsage\b", client_src):
    sys.exit(f"{CLIENT} 没有导出 getWorkspaceUsage：路径在了，但没有人能取它")
usage_callers = [rel for rel in sorted(graph) if rel != CLIENT and "getWorkspaceUsage(" in read(rel)]
if not usage_callers:
    sys.exit(
        f"从 {ENTRY} 可达的源码里没有一处调用 getWorkspaceUsage："
        "配额读数没有活的消费者，读者只能在写入被拒时才知道有上限"
    )
if not [
    rel
    for rel in graph
    if rel.endswith(".tsx") and re.search(r"<WorkspaceUsage[\s/>]", read(rel))
]:
    sys.exit("没有任何可达的组件渲染 <WorkspaceUsage：组件在文件里，但不在任何一屏上")

# ── 7. 后端：用量路由在，写之前仍问容量 ──
if '"/api/v1/ide/usage"' not in gw_src:
    sys.exit(f"{GATEWAY} 没有登记 /api/v1/ide/usage：配额组件打下去只有 404")
if "fn measure_usage" not in ide_src:
    sys.exit(f"{IDE} 里没有 measure_usage：用量无从量起")
def handler_body(name):
    """
    一个 handler 的函数体，边界停在测试模块之前。

    不能停在下一条 `pub async fn` 上：`#[cfg(test)] mod tests` 里也调这些函数，
    截到文件尾就把测试里的调用也算成「handler 里还在过判据」，于是删掉真实调用
    之后门照样绿。
    """
    start = ide_src.find(f"pub async fn {name}")
    if start < 0:
        sys.exit(f"{IDE} 里没有 {name}")
    stops = [i for i in (ide_src.find("\n#[cfg(test)]", start), ide_src.find("\npub async fn ", start + 1)) if i > 0]
    return ide_src[start:min(stops) if stops else len(ide_src)]


for handler in ("write_handler", "upload_handler"):
    if "check_capacity(" not in handler_body(handler):
        sys.exit(
            f"{IDE}: {handler} 落盘前没有过 check_capacity——"
            "上限只画在屏幕上，写到磁盘那一步不问，等于没有上限"
        )

# ── 8. 保存前的比较交换：底稿指纹带上去了，409 有人接 ──
if not re.search(r"saveWorkspaceFile\s*\([^)]*expectHash", client_src):
    sys.exit(
        f"{CLIENT}: saveWorkspaceFile 不收底稿指纹——"
        "整份覆盖无从比较，谁先写谁说了算"
    )
if "expect_hash" not in client_src:
    sys.exit(f"{CLIENT}: 请求体里没有 expect_hash：指纹收进来了，却没发出去")
forced = [
    rel
    for rel in sorted(graph)
    if rel.endswith(".tsx")
    and re.search(r"saveWorkspaceFile\s*\(([^)]*,\s*){2}", read(rel))
]
if not forced:
    sys.exit(
        "没有任何可达的文件页把底稿指纹传给 saveWorkspaceFile："
        "服务端的比较交换永远收不到第二个值，覆盖照旧是无声的"
    )
if not [
    rel
    for rel in forced
    # 按形式判，不认裸的 "409"：注释里提一句 409 不算处理了冲突。
    if re.search(r"status\s*===\s*409", read(rel)) and "setConflict(" in read(rel)
]:
    sys.exit(
        "带着指纹保存的那一处没有处理 409：冲突被当成一次普通的保存失败，"
        "读者只会看到「保存失败」，不知道对方的版本还在盘上"
    )
if "fn ensure_unchanged" not in ide_src:
    sys.exit(f"{IDE} 里没有 ensure_unchanged：底稿比对没有落地")
if "expect_hash" not in ide_src:
    sys.exit(f"{IDE}: WriteBody 不收 expect_hash：客户端带了，服务端不看")
if "ensure_unchanged(" not in handler_body("write_handler"):
    sys.exit(f"{IDE}: write_handler 落盘前不比底稿——409 永远不会出现")

# ── 9. 工作台：段数不设上限，宽高跟着段数与窗口走，两种排法都在 ──
# 这一条来自一个具体的坏形状：工作台只肯开三段，第四段点下去什么都不发生；排法
# 固定成一列几栏，开得多了一格窄成竖条。两种坏法都不报错、不报红，只是「明明有
# 这个功能却用不了」，而用不了的人只会以为软件就到此为止。
workspaces = [
    rel
    for rel in sorted(graph)
    if rel.endswith(".tsx") and re.search(r"<ConversationPane[\s/>]", read(rel))
]
if not workspaces:
    sys.exit("没有任何可达的组件渲染 <ConversationPane：工作台不在任何一屏上")

for rel in workspaces:
    src = read(rel)
    # 段数上限：写死的常量 + 拿它把按钮关掉，两样凑齐才是「只能开三段」。
    for cap in re.findall(r"MAX_\w+\s*=\s*(\d+)", src):
        sys.exit(
            f"{rel} 里有一个写死的段数上限（MAX_… = {cap}）："
            "开第 N+1 段时按钮会安静地什么都不做，读者只会以为软件只支持这么几段"
        )
    if re.search(r"disabled=\{[^}]*order\.length\s*>=", src):
        sys.exit(f"{rel} 按段数把「新对话」按钮关掉了：段数应当是读者决定的，不是常量决定的")
    if not re.search(r"(order|shown)\.map\(", src):
        sys.exit(f"{rel} 没有按会话列表逐段渲染窗格：多开的对话不会出现在工作台上")
    # 宽与高都要跟着段数和容器大小走：只排一维的话，段数一多就变成竖着的一条线。
    if "gridTemplateColumns" not in src or "gridTemplateRows" not in src:
        sys.exit(
            f"{rel} 没有同时排宽与排高（缺 gridTemplateColumns / gridTemplateRows）："
            "只按宽度分栏时，段数一多每一格就被压成一条竖线"
        )
    if "ResizeObserver" not in src:
        sys.exit(f"{rel} 没有量容器大小：窗口改尺寸、侧栏被拖之后，排法不会跟着变")
    # 两种排法：编辑器式的并排，与一次全屏看一段的标签页。
    if not ("'tabs'" in src and "'columns'" in src):
        sys.exit(
            f"{rel} 只有一种排法：并排与标签页是两种不同的注意力分配，"
            "只剩一种时，段数一多就没有「把这一段放到最大」的做法"
        )

print("WORKSPACE OK：搜索、配额、保存冲突三条链路的取数、渲染、落点、后端判据都在；"
      "工作台段数不设上限、宽高自适应、并排与标签页两种排法都在")
PYEOF
