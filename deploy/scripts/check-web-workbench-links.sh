#!/usr/bin/env bash
# 工作台上每一格、每一盏灯，点进去必须真的有一页。
#
# 这是属主亲自撞上的一类坏法：工作台的「能力」格点进去开的是治理页，因为那一格引入
# 时把 `href` 直接写成了 `"governance"`；同一份文件里那盏灯的 `href` 写成 `"deploy"`，
# 而左栏那一面的 id 是 `"delivery"`，点下去是一句「没有叫 deploy 的这一页」。两处都
# 不报错、不告警、不崩——数据照常读，格子照常画，只是每次点都落到别处或者落空。
#
# 根因是 `href` 在三个地方各写了一遍而没人对账：网关发一个字符串，前端再 `as
# ActivityId` 硬转一次，类型断言把「这个 id 存不存在」这一问整个跳过了。所以这条门
# 去对账：网关发出的每一个 `href`，都必须在**三层**里都真的存在——
#
#   1. `web/src/shell/store.ts` 的 `ActivityId` 联合里有它；
#   2. `web/src/shell/ActivityBar.tsx` 的活动栏名单里有它（否则这一面在界面上没有入口，
#      读者只能靠命令面板撞见）；
#   3. `web/src/views/registry.tsx` 里有它的 `case`（否则活动面切过去了也没有组件画）。
#
# 缺一层就是「点得动但到不了」。三层都在、名字对不上，就是今天这个 bug。
#
# 另判一条：工作台四格各用自己的图标。两格共用一个图标时，这两件事在屏幕上长得一样，
# 读者靠图标分格就会分错——「能力」原来借的正是治理的盾牌。
#
# 用法：bash deploy/scripts/check-web-workbench-links.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
STORE = "shell/store.ts"
BAR = "shell/ActivityBar.tsx"
REGISTRY = "views/registry.tsx"
WORKBENCH_VIEW = "views/WorkbenchView.tsx"
GATEWAY = "crates/cog-gateway/src/workbench.rs"

def read(rel, base=WEB):
    path = os.path.join(base, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 {rel}")
    return open(path, encoding="utf-8").read()

store = read(STORE)
bar = read(BAR)
registry = read(REGISTRY)
view = read(WORKBENCH_VIEW)
gateway = read(GATEWAY, ROOT)

# ── 三层各自的全集 ──
union = re.search(r"export type ActivityId =([\s\S]*?);", store)
if not union:
    sys.exit(f"{STORE} 里读不出 `ActivityId` 联合类型：这一条判据数不出「有哪些面」")
activity_ids = set(re.findall(r"'([a-z0-9_-]+)'", union.group(1)))
if not activity_ids:
    sys.exit(f"{STORE} 的 `ActivityId` 联合类型里一个成员都没取到——正则在旧形状上失效了")

# 活动栏名单：`{ id: 'xxx', label: …, icon: … }` 那种条目，另加只写 id 的短条目。
bar_ids = set(re.findall(r"\{\s*id:\s*'([a-z0-9_-]+)'", bar))
if not bar_ids:
    sys.exit(f"{BAR} 里一个活动项都没取到——名单换了形状，这条门会静静地放行")

registry_ids = set(re.findall(r"case '([a-z0-9_-]+)':", registry))
if not registry_ids:
    sys.exit(f"{REGISTRY} 里一个 case 都没取到——对照表换了形状，这条门会静静地放行")

# ── 网关发出的 href ──
# 取的是赋值形态 `href: "…"`。写成别的形状（常量、函数返回值、拼接）时判不了，
# 所以那几种一律红：判不了的 href 与错的 href 在读数上一样，而这一条的全部价值
# 就是不许它们悄悄存在。
problems = []
targets = []
for lineno, line in enumerate(gateway.splitlines(), 1):
    if "href" not in line:
        continue
    for literal in re.findall(r'href:\s*"([a-z0-9_-]+)"', line):
        targets.append((lineno, literal))
    if re.search(r"\bhref\s*:", line) and not re.search(r'href:\s*"[a-z0-9_-]+"', line):
        if "pub href" in line or "///" in line:
            continue  # 字段声明与注释里出现的名字不是一条指向
        problems.append(
            f"{GATEWAY}:{lineno}: 这个 href 不是字面量（{line.strip()}）——"
            "判不了它指向哪一面，门就不会在它指错时红"
        )

if not targets:
    sys.exit(f"{GATEWAY} 里一个 `href: \"…\"` 都没取到：这一条判据数不出指向")

for lineno, href in targets:
    missing = []
    if href not in activity_ids:
        missing.append(f"`ActivityId` 联合类型（{STORE}）")
    if href not in bar_ids:
        missing.append(f"活动栏名单（{BAR}）")
    if href not in registry_ids:
        missing.append(f"面到组件的对照表（{REGISTRY}）")
    if missing:
        problems.append(
            f"{GATEWAY}:{lineno}: href `{href}` 在 " + "、".join(missing) + " 里不存在——"
            "这一格点下去要么落到别处，要么是一句「没有叫这一面的页」"
        )

# ── 四格各用自己的图标 ──
icons = re.search(r"const CELL_ICONS[^=]*=\s*\{(.*?)\n\};", view, re.S)
if not icons:
    problems.append(f"{WORKBENCH_VIEW}: 读不出 `CELL_ICONS`——这一条判不了四格是否各用一个图标")
else:
    pairs = re.findall(r"(?:^|\n)\s*([a-z_]+):\s*'([a-z0-9_-]+)'", icons.group(1))
    if not pairs:
        problems.append(f"{WORKBENCH_VIEW}: `CELL_ICONS` 里一个格子都没取到")
    seen = {}
    for cell, icon in pairs:
        if icon in seen:
            problems.append(
                f"{WORKBENCH_VIEW}: 「{cell}」与「{seen[icon]}」共用图标 `{icon}`——"
                "两格在屏幕上长得一样，读者靠图标分格就会分错"
            )
        seen[icon] = cell

if problems:
    sys.exit("工作台的指向不合格：\n  " + "\n  ".join(problems))

print(
    "ok：工作台的 %d 个 href 都落在真有的面上（联合类型、活动栏、对照表三层齐），"
    "四格各用自己的图标" % len(targets)
)
PYEOF
