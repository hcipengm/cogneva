#!/usr/bin/env bash
# 顶栏与设置面入口的消费者门禁：问号、模型、设置三条路都得真的通。
#
# 属主点名过这三件：「右上角 ? 必须能用」「LLM 设置入口必须有」「设置面固定入口，
# 同时右上角恢复直达」。三者坏起来都是一个样子——图标在、位置对、点下去什么都不发生，
# 或者点下去开的是别处。这类坏法不会让任何东西报错，只让人以为界面坏了。
#
# 判据：
#   1. 顶栏的帮助按钮调的是 `setPalette('help')`（不是打开别的东西、也不是空 onClick）；
#   2. 帮助面板真的按 `mode === 'help'` 换了一份源，而且那一份里每一条都带 `run`——
#      「问一句 → 去一处」的表里，落不到去处的条目就是一句说明；
#   3. 顶栏的模型入口调的是 `openLlmSetup`（直达配置，不是先跳设置页再让人自己找），
#      并且它的提示语带着读数时刻（`llm.at`）——状态点说「配了没有」，得同时说
#      「这一句是什么时候读的」；
#   4. 设置面有固定的模型入口（那一格 + 打开配置的按钮），不到顶栏也找得到；
#   5. 侧栏/活动栏有设置这一项（固定入口）。
#
# 用法：bash deploy/scripts/check-web-shell-entries.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
TITLE = "shell/TitleBar.tsx"
PALETTE = "shell/Palette.tsx"
SETTINGS = "views/SettingsView.tsx"
ACTIVITY = "shell/ActivityBar.tsx"

for rel in (TITLE, PALETTE, SETTINGS, ACTIVITY):
    if not os.path.isfile(os.path.join(WEB, rel)):
        sys.exit(f"读不到 {rel}")
src = {rel: open(os.path.join(WEB, rel), encoding="utf-8").read() for rel in (TITLE, PALETTE, SETTINGS, ACTIVITY)}

problems = []

# ── 1. 问号开的是帮助面板 ──
if not re.search(r"setPalette\(\s*'help'\s*\)", src[TITLE]):
    problems.append(
        f"{TITLE}: 帮助按钮没有调 setPalette('help')——图标在、点下去却不开帮助，"
        "读者只会以为这一面坏了"
    )

# ── 2. 帮助面板按 mode 换源，且每条都有去处 ──
if not re.search(r"mode\s*===\s*'help'\s*\?\s*help\s*:\s*commands", src[PALETTE]):
    problems.append(
        f"{PALETTE}: 帮助没有自己的一份条目（`mode === 'help' ? help : commands`）——"
        "问号打开的会是动作面板"
    )
help_block = re.search(r"const\s+help\s*=\s*useMemo<Command\[\]>\((.*?)\n  \},", src[PALETTE], re.S)
if not help_block:
    problems.append(f"{PALETTE}: 找不到帮助条目表——这一面问的那一段无从检查")
else:
    body = help_block.group(1)
    # 每条直接写出来的条目（从 `id: 'help:` 到它自己的 `},`）都必须带 `run`：
    # 落不到去处的「帮助」只是一句说明。
    entries = re.split(r"id:\s*'help:", body)[1:]
    if not entries:
        problems.append(f"{PALETTE}: 帮助条目表里没有一条 help: 开头的条目")
    if any("run:" not in entry.split("},", 1)[0] for entry in entries):
        problems.append(
            f"{PALETTE}: 有帮助条目没有去处（缺 run）——落不到去处的「帮助」只是一句说明"
        )
    # 批量生成的条目（`go(...)` 那种）的去处在工厂里，工厂少了 run，整批都没去处。
    if "run:" not in body:
        problems.append(f"{PALETTE}: 帮助条目表里一处 run 都没有——没有一条能真的走到某个地方")

# ── 3. 模型入口直达配置，并带读数时刻 ──
if "openLlmSetup" not in src[TITLE]:
    problems.append(
        f"{TITLE}: 顶栏没有直达模型配置的入口（openLlmSetup）——"
        "「设置面固定入口，同时右上角恢复直达」里的后一半没了"
    )
if not re.search(r"\bllm\.at\b", src[TITLE]):
    problems.append(
        f"{TITLE}: 模型入口的提示里没有读数时刻（llm.at）——"
        "状态点说「配了没配」，却不说是哪一刻读到的"
    )
if not re.search(r"llm\.status\?\.configured|llm\.status\.configured", src[TITLE]):
    problems.append(
        f"{TITLE}: 模型入口没有读 llm.status.configured——不读状态的入口只能靠读者自己猜配没配"
    )

# ── 4. 设置面有固定的模型入口 ──
if "openLlmSetup" not in src[SETTINGS]:
    problems.append(
        f"{SETTINGS}: 设置面没有模型配置入口——顶栏是直达，不是唯一的入口"
    )
if "GET /api/v1/llm/status" not in src[SETTINGS]:
    problems.append(
        f"{SETTINGS}: 设置面的模型状态没有写出来源（GET /api/v1/llm/status）——"
        "一格没有来路的「已配置」读者只能选择信或不信"
    )

# ── 5. 设置是固定的一项 ──
if "settings" not in src[ACTIVITY]:
    problems.append(f"{ACTIVITY}: 活动栏里没有设置这一项——设置是固定入口，不该只藏在顶栏")

if problems:
    sys.exit("顶栏与设置面的入口不合格：\n  " + "\n  ".join(problems))

print("ok：问号开出的是帮助表（每条都有去处），顶栏模型入口直达配置并带读数时刻，设置面与侧栏各有固定入口")
PYEOF
