#!/usr/bin/env bash
# 「每一面都画得出自己那份读数的形状」——前端可视化门禁。
#
# 坏起来也没有症状：每一面都读得到、数都对、表都齐，只是全是一个样子的表格。表格
# 回答「是哪几条」，回答不了「多不多、偏不偏、什么时候开始」——而那正是运维打开
# 这一面时想问的。一整屏表格读起来像一份导出文件，读者要自己在脑子里把一列数字排
# 成条形才能看出谁最紧张。
#
# 判据：
#   1. 有一套共用的画法：`ui/plots.tsx` 里导出横条组、分布条、折线、对着上限的一格。
#      每一面都用这一套，而不是各自在 JSX 里拼 div —— 各自拼的话，「没量的画成 0」
#      这种错会在每一面各犯一次。
#   2. 画法只用主题令牌上色：`ui/plots.tsx` 里不许出现十六进制色值。写死色值的那
#      几笔在换主题时不会跟着走，会变成深色底上的浅色块。
#   3. 画出来的那几笔有可读名：每个图都带 `role="img"` 与 `aria-label`，每一格的原值
#      都有一条读得到的路——颜色与长度对读屏软件是不可见的，不给名字就等于没有这一幅。
#      「路」是闭集但认机制不认属性名：原生 `title=`、SVG 的 `<title>` 元素、页内的
#      `Note`（悬停/聚焦展开、文字可选可复制，走 `aria-describedby`）都算。判据是
#      「原值读得到」，不是「必须用某个属性」——把提示换成更好的机制，不该让这一格
#      看起来像丢了提示；反过来，一个画法一条路都没有，这一条就红。
#   4. 「没量出来」不许画成 0：`Meter` 必须有 `value === null` 的那一支，且那一支
#      不落成空条（斜纹 + 写明原因）。把「没人量过」画成「量出来是零」是这一面上
#      最贵的一种谎。
#   5. **每一面**都画得出东西，不是几个面。每一个 `views/*.tsx` 要么用上第 1 条那套
#      画法，要么用上树里已有的画法（甘特条、花销柱、令牌占比、集群拓扑），要么在
#      文件头写明「画面：」——即这一面的内容本身就是画面（并排 diff、编辑器、几段
#      对话）。三种之外没有第四种，也不按名单放行。
#
# 用法：bash deploy/scripts/check-web-face-visualization.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }

python3 - "$ROOT" <<'PYEOF'
import glob
import os
import re
import sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")

def read(rel):
    path = os.path.join(WEB, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 web/src/{rel}")
    return open(path, encoding="utf-8").read()

# 1. 一套共用的画法
plots_path = os.path.join(WEB, "ui/plots.tsx")
if not os.path.isfile(plots_path):
    sys.exit(
        "没有 web/src/ui/plots.tsx：各面只能自己在 JSX 里拼条形，"
        "「没量的画成 0」这种错会在每一面各犯一次"
    )
plots = open(plots_path, encoding="utf-8").read()

for name, why in (
    ("BarList", "横条组：回答「谁最多、差多少」"),
    ("Strip", "分布条：回答「这一堆里各有几个」"),
    ("Sparkline", "折线：回答「什么时候起变的」"),
    ("Meter", "对着一个上限的一格：回答「离满还差多少」"),
):
    if not re.search(r"export function %s\(" % name, plots):
        sys.exit(f"ui/plots.tsx 里没有导出 `{name}`：{why}，这一问就只能靠表里的数字自己去排")

# 2. 只用主题令牌上色
hexes = re.findall(r"#[0-9a-fA-F]{3,8}\b", plots)
if hexes:
    uniq = sorted(set(hexes))
    sys.exit(
        "ui/plots.tsx 里写死了十六进制色值 "
        + "、".join(uniq[:5])
        + "：换主题时这几笔不会跟着走，会变成深色底上的浅色块。色值只从 tokens.css 的角色令牌取"
    )

# 3. 每一幅都有可读名，每一格都能悬停读出原值
if plots.count('role="img"') < 4:
    sys.exit(
        "ui/plots.tsx 里有画法没带 `role=\"img\"`：颜色与长度对读屏软件是不可见的，"
        "不给名字等于这一幅不存在"
    )
# 原值可达的路：逐画法判，不是全文件数总数。全文件数到 4 条，四条可以全挤在同一个
# 画法里，另外三个画法一条都没有——那正是这条判据原先数不出来的一种坏法。
# 认的是「这一格的原值还读不读得到」，所以三种机制都算：原生 `title=` 属性、SVG 的
# `<title>` 元素、页内 `Note` 组件。判到属性名上，换一次提示机制就会假红。
VALUE_PATHS = (r"\btitle=", r"<title[\s>]", r"<Note[\s>]")
for name in ("BarList", "Strip", "Sparkline", "Meter"):
    body = re.search(r"export function %s\(.*?\n}\n" % name, plots, re.S)
    if not body:
        sys.exit(f"ui/plots.tsx 里读不出 `{name}` 的函数体：这一条判断不了它有没有标可读名")
    text = body.group(0)
    if "aria-label" not in text:
        sys.exit(f"ui/plots.tsx 的 `{name}` 没有 `aria-label`：这一幅没有可读名")
    if not any(re.search(pattern, text) for pattern in VALUE_PATHS):
        sys.exit(
            f"ui/plots.tsx 的 `{name}` 没有任何一条给原值的路（`title=`、SVG `<title>`、"
            "`Note` 都没有）：颜色与长度对读屏软件不可见，读者就只剩相对长短，"
            "答不出「这一格到底是多少」"
        )

# 4. 「没量出来」不许画成 0。判的是形状不是有没有这个词：`null` 要在**算长度的那
#    一步**就分出去，`null` 的那一支要画出与「零」不同的样子。只查「函数体里出现过
#    `value === null`」是不够的——那一支被改成 0 之后，句子还在。
meter = re.search(r"export function Meter\(.*?\n}\n", plots, re.S)
if not meter:
    sys.exit("ui/plots.tsx 里读不出 `Meter` 的函数体")
if not re.search(r"const ratio\s*=\s*value === null[^;]*\?\s*null\s*:", meter.group(0)):
    sys.exit(
        "ui/plots.tsx 的 `Meter` 不是先判 `value === null` 再算长度：那一格一旦被算成"
        "数字，画出来就是一根空条，而空条读作 0——「没人量过」被说成了「量出来是零」"
    )
if not re.search(r"ratio === null\s*\?[\s\S]{0,400}repeating-linear-gradient", meter.group(0)):
    sys.exit(
        "ui/plots.tsx 的 `Meter` 在没量出来时没有画出与「零」不同的样子（斜纹）："
        "读者分不出这两件事，这一幅就是在替数据编话"
    )

# 5. 每一面都画得出东西。画法是闭集，按**出处**认，不按名字认：一个面自己写了
#    个叫 `Meter` 的函数并渲染它，那不是这套画法——同名不同物的两个东西，按裸名字
#    判会把「这一面没画」判成「画了」。
DRAW_MODULES = {
    "@/ui/plots": ("BarList", "Strip", "Sparkline", "Meter"),
    "@/observability/SpendBars": ("SpendBars",),      # 按窗口排开的花销柱
    "@/topology/ClusterTopology": ("ClusterTopology",),  # 集群 → 节点 → Pod 的拓扑
}

# 画在某一面自己的文件里、只有这一面用的画法。每加一项要写清它画的是什么——
# 这一项的用处是「知道它画在哪」，不是「允许这一面不画」。
LOCAL_DRAWINGS = {"GanttBar": "计划条与实际条并排的一行"}

# `registry.tsx` 不是面：它是面名到组件的对照表，自己不画也不读。
SKIP = {"registry.tsx"}

def imported_names(src):
    """这个文件从哪些模块导入了哪些名字。"""
    found = {}
    for names, module in re.findall(
        r"import\s+(?:type\s+)?\{([^}]*)\}\s*from\s*'([^']+)'", src, re.S
    ):
        found.setdefault(module, set()).update(
            part.strip().split(" as ")[-1].strip()
            for part in names.split(",")
            if part.strip()
        )
    return found

faces = sorted(glob.glob(os.path.join(WEB, "views", "*.tsx")))
if not faces:
    sys.exit("找不到 web/src/views/*.tsx：这一条判据数不出面来")

DRAWERS = [name for names in DRAW_MODULES.values() for name in names]
unpictured = []
for face in faces:
    name = os.path.basename(face)
    if name in SKIP:
        continue
    src = open(face, encoding="utf-8").read()
    imports = imported_names(src)

    drawn = False
    for module, names in DRAW_MODULES.items():
        bound = imports.get(module, set())
        if any(n in bound and ("<%s" % n) in src for n in names):
            drawn = True
            break
    if not drawn and "<svg" in src:
        drawn = True  # 自己画的图（任务依赖图）
    if not drawn:
        for local in LOCAL_DRAWINGS:
            # 定义与渲染都在这份文件里，才算这一面画出它。
            if ("function %s(" % local) in src and ("<%s" % local) in src:
                drawn = True
                break
    if drawn:
        continue
    # 第三种：这一面的内容本身就是画面。声明要落在文件头那段说明里——写在别处的
    # 声明没人会读到，而这一条的全部价值是让下一个读这一面的人知道为什么没有图。
    head = "\n".join(src.splitlines()[:60])
    if re.search(r"画面：(\S.{9,})", head):
        continue
    unpictured.append(name)

if unpictured:
    sys.exit(
        "这几面一个形状都画不出来：" + "、".join(unpictured)
        + "。它们要么用 `@/ui/plots` 那套画法（或树里已有的甘特条、花销柱、拓扑）把"
        "自己那份读数的形状画出来，要么在文件头写明「画面：」——这一面的内容本身"
        "就是画面。一整屏只有表格，读者要自己在脑子里把一列数字排成条形"
    )

# 画法不自己去读服务端：它是「把交给它的一份读数画出来」，取数在各面自己的读面里。
if "@/api/client" in plots or "useResource" in plots or "fetch(" in plots:
    sys.exit(
        "ui/plots.tsx 自己在读服务端：画法只负责把交给它的一份数画出来。它一旦自己取数，"
        "同一屏上就会出现两份来源不同的同一件事——而上一次就是这么错的"
    )

print(f"每一面都画得出东西：{len(faces) - len(SKIP)} 个面，共用 {len(DRAWERS)} 种画法")
PYEOF
