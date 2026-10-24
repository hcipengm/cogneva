#!/usr/bin/env bash
# 配色主题门的判据：**每个主题都自己填满一组颜色，且有人能选到它**。
#
# 主题坏起来没有症状：少填一个令牌不会报错，它会继承上一个主题的颜色，在一屏里
# 留下两种风格；选择器只接一半（卡片画得出来、点下去不生效）也照样编译得过。
# 于是这条门查四件事：
#   1. 注册表里每个主题都定义了前端用到的**全部**颜色令牌（漏一个就红）。
#   2. `tokens.css` 的 `:root` 与默认主题逐字相等——不等的话，首帧（还没跑 JS）
#      和渲染后的第一屏是两种配色，而且没有任何东西会失败。
#   3. 从入口可达的一屏**渲染**了选择器，并且同一个标签上接齐了列表、当前值、回调。
#   4. 进页面时先套用上次选的主题（`bootTheme()` 在 `main.tsx` 渲染之前），
#      选择后写进 localStorage——否则换主题只在这一次会话里有效。
#
# 用法：bash deploy/scripts/check-web-theme-picker.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
THEMES_FILE = "styles/themes.ts"
TOKENS_CSS = "styles/tokens.css"
ENTRY = "main.tsx"
STORE = "shell/theme.ts"

if not os.path.isdir(WEB):
    sys.exit(f"读不到前端源码目录：{WEB}")


def read(rel):
    path = os.path.join(WEB, rel)
    if not os.path.isfile(path):
        sys.exit(f"读不到 {rel}")
    return open(path, encoding="utf-8").read()


# ── 前端实际用到的颜色令牌 ──
USED = re.compile(r"var\(--([a-z0-9-]+)")
used = set()
for dirpath, _, names in os.walk(WEB):
    for name in names:
        if not name.endswith((".ts", ".tsx", ".css")):
            continue
        src = open(os.path.join(dirpath, name), encoding="utf-8").read()
        used.update(USED.findall(src))
# 字体不是配色：主题不负责它。
used = {key for key in used if not key.startswith("font-")}
if not used:
    sys.exit("前端源码里没有读到任何 var(--x)——这份判据无从量起")

# ── 1. 每个主题都填满这组令牌 ──
themes_src = read(THEMES_FILE)
blocks = re.split(r"\n  \{\n", themes_src)
themes = {}
for block in blocks[1:]:
    id_match = re.search(r"id:\s*'([a-z0-9-]+)'", block)
    if not id_match:
        continue
    tokens_block = re.search(r"tokens:\s*\{(.*?)\n    \}", block, re.S)
    if not tokens_block:
        sys.exit(f"主题 {id_match.group(1)} 没有 tokens 块")
    # 键有时带引号（`'ink-1000'`）有时不带（`line`），两种都得认。
    tokens = dict(re.findall(r"'?([a-z0-9-]+)'?:\s*'(#[0-9a-fA-F]{3,8})'", tokens_block.group(1)))
    themes[id_match.group(1)] = tokens

if len(themes) < 6:
    sys.exit(f"注册表只有 {len(themes)} 个主题：少于 6 个时「换配色」只是换个名字")

problems = []
for theme_id, tokens in sorted(themes.items()):
    missing = sorted(used - set(tokens))
    if missing:
        problems.append(
            f"主题 {theme_id} 没有定义 {', '.join('--' + m for m in missing)}"
            "（缺的会悄悄继承上一个主题的颜色）"
        )
    extra = sorted(set(tokens) - used)
    if extra:
        problems.append(
            f"主题 {theme_id} 定义了没人用的 {', '.join('--' + e for e in extra)}"
            "（用了又没人读，说明令牌名拼错了）"
        )

# ── 2. 首帧与默认主题逐字相等 ──
default_match = re.search(r"DEFAULT_THEME\s*=\s*'([a-z0-9-]+)'", themes_src)
if not default_match:
    sys.exit(f"{THEMES_FILE} 没有导出 DEFAULT_THEME")
default_id = default_match.group(1)
if default_id not in themes:
    sys.exit(f"默认主题 {default_id} 不在注册表里")

root_match = re.search(r":root\s*\{(.*?)\n\}", read(TOKENS_CSS), re.S)
if not root_match:
    sys.exit(f"{TOKENS_CSS} 里没有 :root 块")
css_tokens = dict(re.findall(r"--([a-z0-9-]+):\s*(#[0-9a-fA-F]{3,8});", root_match.group(1)))
for key, value in themes[default_id].items():
    css_value = css_tokens.get(key)
    if css_value is None:
        problems.append(f"tokens.css 的 :root 没有 --{key}（默认主题里定义了它）")
    elif css_value.lower() != value.lower():
        problems.append(
            f":root 的 --{key} 是 {css_value}，默认主题 {default_id} 里是 {value}"
            "（首帧与渲染后的第一屏会是两种配色）"
        )

# ── 3. 可达的一屏渲染了选择器，且接齐三件 ──
IMPORT = re.compile(r"""^\s*(?:import|export)\b[^'"]*?from\s+['"]([^'"]+)['"]""", re.M)
EXT = ("", ".ts", ".tsx", "/index.ts", "/index.tsx")


def resolve(rel, spec):
    base = spec[2:] if spec.startswith("@/") else os.path.normpath(os.path.join(os.path.dirname(rel), spec))
    for ext in EXT:
        if os.path.isfile(os.path.join(WEB, base + ext)):
            return base + ext
    return None


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

picker_calls = []
for rel in sorted(seen):
    for tag in re.findall(r"<ThemePicker\b[^>]*>", read(rel), re.S):
        picker_calls.append((rel, tag))
if not picker_calls:
    problems.append(
        "从入口可达的源码里没有任何一处渲染 <ThemePicker>："
        "主题注册表在，但没有一格画出来的选择器"
    )
for rel, tag in picker_calls:
    for prop in ("themes=", "active=", "onPick="):
        if prop not in tag:
            problems.append(f"{rel} 的 <ThemePicker> 没接 {prop}（少哪一个都会让选择器变成摆设）")

# 回调必须真的落到 applyTheme：接了一个空函数同样编译得过。
store_src = read(STORE)
if "applyTheme(" not in store_src:
    problems.append(f"{STORE} 没有调用 applyTheme：选主题不会改文档根上的变量")
if "THEME_STORAGE_KEY" not in store_src or "localStorage" not in store_src:
    problems.append(f"{STORE} 没有把选择写进 localStorage：换主题活不过刷新")

# ── 4. 进页面先套用上次的选择 ──
main_src = read(ENTRY)
if "bootTheme()" not in main_src:
    problems.append(f"{ENTRY} 没有在渲染前调用 bootTheme()：首屏会用默认配色闪一下")
boot_pos = main_src.find("bootTheme()")
render_pos = main_src.find("createRoot(")
if boot_pos < 0 or render_pos < 0 or boot_pos > render_pos:
    problems.append(f"{ENTRY} 里 bootTheme() 发生在 createRoot() 之后：第一帧已经用错配色画出来了")

if problems:
    sys.exit("配色主题不合格：\n  " + "\n  ".join(problems))

print(
    f"ok：{len(themes)} 个主题 × {len(used)} 个颜色令牌；"
    f"默认 {default_id} 与 :root 逐字一致；选择器渲染在 {picker_calls[0][0]}"
)
PYEOF
