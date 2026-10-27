#!/usr/bin/env bash
# 「代码平台与凭据」那一格的读面门禁。
#
# 这一格的成品是**打开设置就能看见 GitHub 连没连、Gitee 连没连，并能在这一页贴一段
# 令牌**，不是 `/api/v1/admin/contribution-status` 那份 JSON。它坏起来不出声的三种方式：
#   1. 只有一个「已连接平台」格子，读的是 `contrib.provider`——那是一个值，两家都连上时
#      只报得下其中一个，另一家连没连在屏上不存在；服务端其实按平台各给了一份；
#   2. 粘贴令牌只写在浮层里，设置页上没有入口：人在这页读完状态，要去别处才能写，
#      而「写了没有生效」这件事没人说；
#   3. 下拉里只有一家（或者选中的那一家没被带进请求）：选 Gitee、写 GitHub，两次都
#      报成功，而错的那一家一直没连上。
#
# 判据：
#   1. 服务端状态里有按平台分开的两份（`github` / `gitee`），且前端类型收下了它们；
#   2. 设置页渲染的是这两份，不是把自己拼出来的 `provider` 反推；
#   3. 设置页有一个「粘贴令牌」行，里面有选平台的下拉，两个平台都在选项里；
#   4. 下拉的选中值真的进了写入请求（`connectContribution(platform, …)`，不是写死一家）；
#   5. 写完的那句话里有「写入 ≠ 生效」的意思——判据是上面那张表读回来变「已连接」；
#   6. 模型、上网、凭据三格的最右上各有一个入口（模型与上网那两格已经有一个了，
#      凭据那格不能是唯一没有的那个）。
#
# 用法：bash deploy/scripts/check-web-credentials-panel.sh [仓库根目录]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
[ "${1:-}" = "" ] || ROOT="$1"
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$ROOT" <<'PYEOF'
import os, re, sys

ROOT = sys.argv[1]
WEB = os.path.join(ROOT, "web/src")
CLIENT = os.path.join(WEB, "api/client.ts")
VIEW = os.path.join(WEB, "views/SettingsView.tsx")

for path in (CLIENT, VIEW):
    if not os.path.isfile(path):
        sys.exit(f"读不到 {os.path.relpath(path, ROOT)}")

client = open(CLIENT, encoding="utf-8").read()
view = open(VIEW, encoding="utf-8").read()
problems = []

# ── 1. 服务端按平台各给一份，前端类型收下了 ──
status = re.search(r"export interface ContributionStatus \{(.*?)\n\}", client, re.S)
if not status:
    problems.append("client.ts: 没有 ContributionStatus 这个类型")
else:
    body = status.group(1)
    for platform in ("github", "gitee"):
        if not re.search(rf"\b{platform}\?:\s*\{{\s*configured", body):
            problems.append(
                f"client.ts: ContributionStatus 里没有 {platform}?.configured——"
                "按平台各给的那一份没被收下，屏上只能退回单一的 provider"
            )

# ── 2. 设置页渲染的是按平台的那两份 ──
for platform in ("github", "gitee"):
    if not re.search(rf"contrib\?\.{platform}\?\.configured", view):
        problems.append(
            f"SettingsView: 没有读 contrib?.{platform}?.configured——{platform} 连没连"
            "在屏上不存在，而屏上照样有一格写着「已连接」（另一家的）"
        )

# ── 3. 有一个粘贴令牌行，下拉里两家都在 ──
if "粘贴令牌" not in view:
    problems.append("SettingsView: 没有「粘贴令牌」这一行——这一页读得到状态，写不进去")
if not re.search(r"<select", view):
    problems.append("SettingsView: 粘贴令牌那一行没有选平台的下拉——一段令牌只能靠人猜着贴")
for platform in ("github", "gitee"):
    if not re.search(rf"<option\s+value=\"{platform}\"", view):
        problems.append(f"SettingsView: 下拉里没有 {platform} 这个选项")

# ── 4. 选中的平台真的进了写入请求 ──
if not re.search(r"connectContribution\s*\(\s*platform\b", view):
    problems.append(
        "SettingsView: 写入时没有把选中的平台带进去（`connectContribution(platform, …)`）——"
        "下拉选什么都没用，两家的令牌都写进同一家"
    )

# ── 5. 写完要说明「写入 ≠ 生效」 ──
if "写入 ≠ 生效" not in view:
    problems.append(
        "SettingsView: 写完之后没有说「写入 ≠ 生效」——人会把「写入被接受」当成"
        "「已连上」，然后认为没变化的那张表是坏的"
    )

# ── 6. 三格各有一个右上入口 ──
# 模型与上网两格早就有；凭据这一格没有的话，它自己就会退回「到别处去配」。
HEADER = re.compile(r"<header className=\"flex items-center gap-3 px-4 py-2\">(.*?)</header>", re.S)
headers = HEADER.findall(view)
if len(headers) < 3:
    problems.append(f"SettingsView: 只找到 {len(headers)} 个带右侧入口的节标题，应有模型/上网/凭据三格")
entry_by_header = {
    "代码平台与凭据": "openCredentials",
    "上网检索与抓取": "openWebSearch",
    "模型上游": "openLlmSetup",
}
for title, opener in entry_by_header.items():
    match = re.search(
        rf"<h2 className=\"text-\[12px\]\">{re.escape(title)}</h2>(.*?)</header>", view, re.S
    )
    if not match:
        problems.append(f"SettingsView: 找不到「{title}」这一节的标题")
        continue
    if opener not in match.group(1):
        problems.append(
            f"SettingsView: 「{title}」那一节的右上没有入口（{opener}）——"
            "三格并列，只有它没有，读者会以为这一格不用配"
        )

if problems:
    sys.exit("代码平台与凭据的读面不合格：\n  " + "\n  ".join(problems))

print("ok：GitHub / Gitee 各占一行、各自报连接状态，粘贴令牌行可选平台且真的写进选中的那家")
PYEOF
