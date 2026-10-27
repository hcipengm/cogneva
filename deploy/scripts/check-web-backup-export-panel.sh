#!/usr/bin/env bash
# 「导出一份备份包」这一屏的消费者门禁。
#
# 空白机重装只认两样东西：公开仓库的源码、一个备份包。所以「把包从旧机器上取下来」
# 必须是属主自己点得动的一件事，而不是「进 Pod 里敲 tar」。这条页面就是那条路。
#
# 坏起来没有症状：`/api/v1/admin/backup/export` 与 `/packages` 两条路由在、备份额的
# 单元测试也全绿，而这一屏整块消失、或者少掉「不填口令就是不加密」那句话，都不会
# 让任何东西失败。少那句话的后果不是难看——是有人把明文整库拷进云盘，还以为自己
# 导的是密文包。
#
# 判据（读到的是「从入口可达」这件事，不是某个文件里有没有某个串）：
#   1. 三条路由由 `web/src/api/client.ts` 声明，并且各自有一个导出的取数函数；
#   2. 设置面 `views/SettingsView.tsx` 从入口 `web/src/main.tsx` 沿 import 图**可达**，
#      并且它渲染了导出的那一格（`<BackupExportSection />`）——不可达的组件没有读者；
#   3. 那一格里三个动作都接上了：列表（useResource 取数）、导出、下载/删除；
#   4. 口令框是 `type="password"`，且旁白写着「不加密」与「整库数据」两件事——
#      这两句是这一屏唯一告诉人「这个包是明文」的地方；
#   5. 有 Job 在跑时「导出」按钮被禁用：不然连点两次会起两趟，两趟各自对备份卷做
#      保留裁剪，先跑完的那趟会把后跑完那趟刚写的包删掉，而两趟都自报成功。
#
# 用法：bash deploy/scripts/check-web-backup-export-panel.sh [仓库根目录]
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
SETTINGS = "views/SettingsView.tsx"
ENTRY = "main.tsx"

for rel in (CLIENT, SETTINGS, ENTRY):
    if not os.path.isfile(os.path.join(WEB, rel)):
        sys.exit(f"读不到 {rel}")
src = {
    rel: open(os.path.join(WEB, rel), encoding="utf-8").read()
    for rel in (CLIENT, SETTINGS, ENTRY)
}

problems = []

# ── 1. 路由与取数函数都由 client.ts 声明 ──
ROUTES = {
    "/api/v1/admin/backup/export": "startBackupExport",
    "/api/v1/admin/backup/packages": "listBackupPackages",
    "/api/v1/admin/backup/packages/${encodeURIComponent(name)}": None,
}
for route, fn in ROUTES.items():
    if route not in src[CLIENT]:
        problems.append(
            f"{CLIENT}: 没有声明 {route}——页面上的按钮点下去会落到一个不存在的路由"
        )
for fn in ("listBackupPackages", "startBackupExport", "deleteBackupPackage", "downloadBackupPackage"):
    if not re.search(rf"export\s+async\s+function\s+{fn}\b", src[CLIENT]):
        problems.append(f"{CLIENT}: 没有导出 {fn}——这一屏的四个动作里有一个没有取数函数")

# ── 2. 设置面从入口可达，并且渲染了那一格 ──
def imports_of(rel):
    text = src.get(rel)
    if text is None:
        path = os.path.join(WEB, rel)
        if not os.path.isfile(path):
            return []
        text = open(path, encoding="utf-8").read()
    out = []
    for match in re.finditer(r"from\s+'(@/|\.{1,2}/)([^']+)'", text):
        prefix, rest = match.group(1), match.group(2)
        base = rest if prefix == "@/" else os.path.normpath(os.path.join(os.path.dirname(rel), rest))
        for candidate in (base + ".tsx", base + ".ts", base + "/index.tsx", base + "/index.ts"):
            candidate = candidate.lstrip("./")
            if os.path.isfile(os.path.join(WEB, candidate)) and candidate not in out:
                out.append(candidate)
                break
    return out

seen, queue = {ENTRY}, [ENTRY]
while queue:
    for dep in imports_of(queue.pop()):
        if dep not in seen:
            seen.add(dep)
            queue.append(dep)
if SETTINGS not in seen:
    problems.append(
        f"{SETTINGS}: 从 {ENTRY} 沿 import 图不可达——这一屏写好了也没有读者"
    )
if not re.search(r"<BackupExportSection\s*/>", src[SETTINGS]):
    problems.append(
        f"{SETTINGS}: 没有渲染 <BackupExportSection />——组件在、设置面里看不到它"
    )

# ── 3. 四个动作都接上 ──
# 取数函数既可能被直接调用，也可能整条交给 `useResource`（那里传的是函数本身），
# 所以这里判的是「这个名字在设置面里出现了」，不是「跟着一对括号」。
for fn in ("listBackupPackages", "startBackupExport", "deleteBackupPackage", "downloadBackupPackage"):
    if not re.search(rf"\b{fn}\b", src[SETTINGS]):
        problems.append(f"{SETTINGS}: 没有接上 {fn}——那一格缺一个动作")

# ── 4. 口令框与一句不能少的话 ──
if not re.search(r'type="password"', src[SETTINGS]):
    problems.append(f"{SETTINGS}: 口令框不是 type=\"password\"——口令会明文显示在屏幕上")
for phrase, why in (
    ("不加密", "「留空 = 不加密」是这一屏唯一说明包是明文的地方"),
    ("整库数据", "只说「没有凭证」会让人以为包里没有敏感内容；包里是整库数据"),
):
    if phrase not in src[SETTINGS]:
        problems.append(f"{SETTINGS}: 没有写「{phrase}」——{why}")

# ── 5. 有 Job 在跑时不能点第二趟 ──
if not re.search(r"disabled=\{[^}]*\brunning\b[^}]*\}", src[SETTINGS]):
    problems.append(
        f"{SETTINGS}: 导出按钮没有按 running 禁用——连点两次会起两趟，"
        "两趟各自对备份卷做保留裁剪，先跑完的那趟会删掉后跑完那趟刚写的包"
    )

if problems:
    sys.exit("备份包导出这一屏不合格：\n  " + "\n  ".join(problems))

print(
    "ok：三条路由与四个取数函数都在，设置面从入口可达并渲染了这一格，"
    "口令框是密码框、明文那两句都在，有 Job 在跑时导不出来第二趟"
)
PYEOF
