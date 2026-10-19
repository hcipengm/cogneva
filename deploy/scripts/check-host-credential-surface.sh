#!/usr/bin/env bash
# 宿主凭证面门禁：宿主上的 systemd 单元碰得到哪个凭证，必须与登记表逐条对齐。
#
# 为什么需要这条：业务 Pod 那张表（check-credential-holder-surface.sh）只管集群里
# 的两个 Secret 对象。宿主上的 unit 是另一条完全不同的路——它以 root 跑、能读整块
# 盘、要往外网推东西。「零带外凭证」说得再响，只要宿主上有一个 unit 把仓库口令写在
# `Environment=RESTIC_PASSWORD=…`（单元文件谁都能读）或者一个 0644 的 env 文件里，
# 那句话就只覆盖了 Pod 那一层，而没有任何东西会红。
#
# 判据面「能推导的就不手写」：
#   1. 单元里出现的凭证指令从 `deploy/systemd/*.service|*.timer` 解析：三类——
#      `LoadCredential=`、`LoadCredentialEncrypted=`、`EnvironmentFile=`。集合为空直接
#      判判据失效（空集合让"每个指令都有登记"恒真）。
#   2. 两侧互判：单元里有的 ⊆ 登记表（多出来的点名），登记表里的 ⊆ 单元（陈旧的
#      登记点名）。
#   3. 加密凭证的名字必须与安装脚本 `systemd-creds encrypt --name=` 用的是同一批：
#      加密时叫 A、单元里按 B 去要，症状是单元起不来，而不是这里红——那就在这里核。
#   4. 单元里不许出现字面量的密钥赋值（`RESTIC_PASSWORD=`、`AWS_SECRET_ACCESS_KEY=`）：
#      单元文件是 0644 的公开物，写进去等于把密钥写进公布面。
#   5. 渲染物 / chart 里不许出现 `RESTIC_PASSWORD`：业务 Pod 零凭证这条，在「宿主
#      要存东西」之后最容易破——把口令顺手加进备份 Job 的 env 是最短的那条路。
#   6. ExecStart 指向的脚本必须还在树里：指针锈了等于这条单元没有任何被维护的读点。
#   7. `--live`：单元真的装在了宿主上、源文件的权限与登记表一致（只 stat，不读内容）。
#
# 用法：bash deploy/scripts/check-host-credential-surface.sh [仓库根目录] [--live]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LIVE=0
for arg in "$@"; do
    case "$arg" in
        --live) LIVE=1 ;;
        -*) echo "未知参数：$arg" >&2; exit 2 ;;
        *) ROOT="$arg" ;;
    esac
done
cd "$ROOT"

command -v python3 >/dev/null || { echo "缺少依赖：python3" >&2; exit 2; }
python3 - "$LIVE" <<'PYEOF'
import glob
import os
import re
import subprocess
import sys

LIVE = sys.argv[1] == "1"
UNIT_DIR = "deploy/systemd"
REGISTRY = "deploy/systemd/credential-holders.tsv"
INSTALLER = "deploy/scripts/install-backup-offsite.sh"
PROFILES = "deploy/rendered"

KINDS = ("LoadCredential", "LoadCredentialEncrypted", "EnvironmentFile")
DIRECTIVE = re.compile(
    r"^\s*(LoadCredential(?:Encrypted)?|EnvironmentFile)=(-?)([^:\s]+)(?::(\S.*))?\s*$")

failures = []


def fail(msg):
    failures.append(msg)


def registry_rows():
    rows = []
    with open(REGISTRY, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            body = line.rstrip("\n")
            if not body or body.startswith("#") or body.startswith("unit\t"):
                continue
            cols = body.split("\t")
            if len(cols) != 7:
                fail(f"{REGISTRY}:{lineno} 不是 7 列（{len(cols)} 列）：这一行既不会被读，也不会被核")
                continue
            rows.append(cols)
    return rows


def unit_directives():
    """单元里出现的凭证指令。返回 (unit, kind, name, source) 四元组表。"""
    found = []
    files = sorted(glob.glob(f"{UNIT_DIR}/*.service") + glob.glob(f"{UNIT_DIR}/*.timer"))
    if not files:
        fail(f"{UNIT_DIR} 下没有单元：判据失效（空集合会让两侧互判恒真）")
        return found
    for path in files:
        unit = os.path.basename(path)
        with open(path, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                if line.lstrip().startswith("#"):
                    continue
                m = DIRECTIVE.match(line)
                if not m:
                    continue
                kind, dash, name_or_path, maybe = m.groups()
                if kind == "EnvironmentFile":
                    found.append((unit, kind, "-", name_or_path))
                else:
                    if maybe is None:
                        fail(f"{unit}:{lineno} 的 {kind} 缺 `名字:路径` 里的一半")
                        continue
                    found.append((unit, kind, name_or_path, maybe))
    return found


def check_literal_secrets():
    """单元文件里不许有字面量密钥。EnvironmentFile 之外没有任何正当理由写这些名字。"""
    pattern = re.compile(r"^\s*Environment=.*(RESTIC_PASSWORD|SECRET_ACCESS_KEY|ACCESS_KEY_ID)\s*=")
    for path in sorted(glob.glob(f"{UNIT_DIR}/*.service") + glob.glob(f"{UNIT_DIR}/*.timer")):
        with open(path, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                if pattern.search(line):
                    fail(f"{os.path.basename(path)}:{lineno} 在 Environment= 里写字面量密钥："
                         "单元文件是公开物，密钥只能经 LoadCredentialEncrypted 进来")


def check_no_password_in_rendered():
    hits = subprocess.run(
        ["grep", "-rln", "RESTIC_PASSWORD", "deploy/rendered", "deploy/helm", "deploy/k3s"],
        capture_output=True, text=True)
    for line in hits.stdout.splitlines():
        fail(f"{line} 引用了 RESTIC_PASSWORD：业务 Pod 零凭证的边界在这里被破")


def check_encrypt_names(rows):
    """加密时用的名字必须与单元里要的名字是同一批。"""
    names_in_unit = sorted({r[2] for r in rows if r[1] == "LoadCredentialEncrypted"})
    if not os.path.exists(INSTALLER):
        fail(f"{INSTALLER} 不在树里：加密名字无从核")
        return
    text = open(INSTALLER, encoding="utf-8").read()
    m = re.search(r"for name in ([^;]+); do", text)
    if not m:
        fail(f"{INSTALLER} 里找不到 `for name in …; do`：加密名字无从核")
        return
    names_in_installer = sorted(set(m.group(1).split()))
    for name in names_in_unit:
        if name not in names_in_installer:
            fail(f"单元要 `{name}`，但安装脚本不加密这个名字：单元起不来")
    for name in names_in_installer:
        if name not in names_in_unit:
            fail(f"安装脚本加密了 `{name}`，但单元不要它：加密了不会有人读")


def check_execstart():
    for path in sorted(glob.glob(f"{UNIT_DIR}/*.service")):
        for line in open(path, encoding="utf-8"):
            if not line.startswith("ExecStart="):
                continue
            target = line.split("=", 1)[1].strip().split()[0]
            # 只有「从本仓 deploy/scripts 装出去的脚本」才可能在树里找不到。
            # /opt/cogneva/ 那条是产品二进制，它的存在归发布物管。
            if not target.startswith("/usr/local/lib/cogneva/"):
                continue
            base = os.path.basename(target)
            if not os.path.exists(f"deploy/scripts/{base}"):
                fail(f"{os.path.basename(path)} 的 ExecStart 指向 {target}，"
                     f"树里没有 deploy/scripts/{base}")


def check_live(rows):
    if not LIVE:
        return
    seen = set()
    for unit, kind, name, source in rows:
        seen.add(unit)
        got = subprocess.run(["systemctl", "cat", unit], capture_output=True, text=True)
        if got.returncode != 0:
            fail(f"--live：{unit} 没有装在宿主上（systemctl cat 失败）")
            continue
        if kind == "EnvironmentFile":
            needle = f"EnvironmentFile={source}"
        else:
            needle = f"{kind}={name}:{source}"
        if needle not in got.stdout:
            fail(f"--live：宿主上的 {unit} 里找不到 `{needle}`，与树里的单元不一致")
        try:
            mode = oct(os.stat(source).st_mode & 0o777)[2:].zfill(4)
        except FileNotFoundError:
            fail(f"--live：{source} 不存在（{unit} 起不来）")
            continue
        expected = next((r[4] for r in rows if r[3] == source), None)
        if expected and mode != expected:
            fail(f"--live：{source} 权限是 {mode}，登记表要求 {expected}")
    installed = subprocess.run(["systemctl", "list-unit-files", "--no-legend",
                                "cogneva-backup-offsite.timer"], capture_output=True, text=True)
    if "cogneva-backup-offsite.timer" not in installed.stdout:
        fail("--live：cogneva-backup-offsite.timer 没有装（enabled 之前这条腿根本不会跑）")


def main():
    rows = registry_rows()
    found = unit_directives()
    check_literal_secrets()
    check_no_password_in_rendered()
    check_encrypt_names(rows)
    check_execstart()

    reg = {(r[0], r[1], r[2], r[3]) for r in rows}
    live = set(found)
    for item in sorted(live - reg):
        fail(f"单元里有凭证指令没有登记：{item[0]} 的 {item[1]}={item[2]}:{item[3]}")
    for item in sorted(reg - live):
        fail(f"登记表里的这一行在单元里找不到：{item[0]} 的 {item[1]}={item[2]}:{item[3]}")

    check_live(rows)

    if failures:
        print("HOST CREDENTIAL SURFACE FAIL：", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        sys.exit(1)

    encrypted = sum(1 for r in rows if r[1] == "LoadCredentialEncrypted")
    print(f"HOST CREDENTIAL SURFACE OK：{len(rows)} 条登记与 {len({r[0] for r in rows})} 个单元对齐；"
          f"其中加密凭证 {encrypted} 个，env 文件 {len(rows) - encrypted} 个；"
          f"渲染物与 chart 里零 RESTIC_PASSWORD"
          + ("；--live 已核宿主实际安装与权限" if LIVE else ""))


main()
PYEOF
