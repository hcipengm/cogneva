#!/usr/bin/env bash
# 宿主凭证面门禁：宿主上每个 systemd 单元碰得到哪个凭证、凭什么可以碰，必须与
# 那条指令自己头上那行声明对齐。
#
# 为什么需要这条：业务 Pod 那张表（check-credential-holder-surface.sh）只管集群里
# 的两个 Secret 对象。宿主上的 unit 是另一条完全不同的路——它以 root 跑、能读整块
# 盘、要往外网推东西。「零带外凭证」说得再响，只要宿主上有一个 unit 把仓库口令写在
# `Environment=RESTIC_PASSWORD=…`（单元文件谁都能读）或者一个 0644 的 env 文件里，
# 那句话就只覆盖了 Pod 那一层，而没有任何东西会红。
#
# 判据面「能推导的就不手写」：
#   1. 单元、指令种类、名字、源路径全部从 `deploy/systemd/*.service|*.timer` 解析。
#      集合为空直接判判据失效（空集合让"每条指令都有声明"恒真）。
#   2. 人写的那一半只有三条，而且写在**那条指令自己头上**，一行 `# cred:`：
#        perms    这个源文件在宿主上该是几位权限（`--live` 下实测，静态下只核取值合法）
#        rotator  谁能换掉它
#        note     凭什么可以有这一行（` — ` 之后那句）
#      机器推得出「谁碰得到哪个凭证」，推不出「它该是几位权限、谁能换、凭什么留着」；
#      那份判断写在指令头上跟着指令走，不另立一张登记表。登记表 2026-10-29 删掉，正是
#      因为它把前四列（单元/种类/名字/路径）又手抄了一遍：抄错那一行不会有任何一拍红，
#      而指令多一条、表里少一行，两个错都只在有人记得时才关得上。
#   3. 每条凭证指令都必须有一行 `# cred:`：写不出凭什么的凭证，不该在宿主上。
#      反过来，`# cred:` 挂在一条已经不存在（或根本不是凭证指令）的行上面，也判红。
#   4. 加密凭证的名字必须与安装脚本 `systemd-creds encrypt --name=` 用的是同一批：
#      加密时叫 A、单元里按 B 去要，症状是单元起不来，而不是这里红——那就在这里核。
#   5. 单元里不许出现字面量的密钥赋值（`RESTIC_PASSWORD=`、`AWS_SECRET_ACCESS_KEY=`）：
#      单元文件是 0644 的公开物，写进去等于把密钥写进公布面。
#   6. 渲染物 / chart 里不许出现 `RESTIC_PASSWORD`：业务 Pod 零凭证这条，在「宿主
#      要存东西」之后最容易破——把口令顺手加进备份 Job 的 env 是最短的那条路。
#   7. ExecStart 指向的脚本必须还在树里：指针锈了等于这条单元没有任何被维护的读点。
#   8. `--live`：单元真的装在了宿主上、源文件的权限与 `# cred:` 声明一致（只 stat，
#      不读内容）。缺装时还要分两种：该单元那条 EnvironmentFile 的源路径也不在 ⇒ 这个
#      通道从没配过，声明不适用（印一行读数、不判红）；源路径在而单元没了 ⇒ 失败。
#      不分这一刀，一台没开异地副本的宿主上这条腿恒红，红久了就没人看。
#   9. 扫描面的分母必须印出来：判词读作「宿主凭证面干净」，而真正扫过的只是本仓
#      `deploy/systemd` 下那几个文件。`--live` 另外报出「宿主本机单元目录里还有几个
#      带凭证指令的单元不在本仓」——只印、不判红：它们归各自所在的树判，把它们登记进
#      公开仓的登记表等于把私有树的名字搬到公开面。目录可由 `COGNEVA_HOST_UNIT_DIRS`
#      覆盖（测试吃注入的夹具，不吃跑测试那台机器的真机单元表）。
#
# 用法：bash deploy/scripts/check-host-credential-surface.sh [仓库根目录] [--live]
#
# 退出码：0 对齐；1 有失败（逐条点名）；3 读不到（`--live` 下 systemctl 跑不起来之类，
# 或门自己出错）——3 不是 1：1 是「去改」，3 是「这一跑什么都没判」。
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
# 部署面（渲染物 + chart + k3s 清单）的文件数下限。这条面判的是「这些文件里没有
# RESTIC_PASSWORD」，而一份没读到的部署面与一份干净的同形；真树今天 4 百多个文件。
SURFACE_FILES_FLOOR = 100
INSTALLER = "deploy/scripts/install-backup-offsite.sh"

DIRECTIVE = re.compile(
    r"^\s*(LoadCredential(?:Encrypted)?|EnvironmentFile)=(-?)([^:\s]+)(?::(\S.*))?\s*$")
# 指令头上那行声明：`# cred: perms=0400 rotator=operator — 凭什么留着`
DECL = re.compile(r"^\s*#\s*cred:\s*(?P<body>\S.*)$")
FIELD = re.compile(r"\b(?P<key>perms|rotator)=(?P<val>[^\s—]+)")
# 合法的权限字面量：只有这三个在宿主上说得通。写别的（`600`、`-rw-------`）不是更严，
# 是核不了——`--live` 的比对是字符串相等，写法不统一就永远红，红久了没人看。
PERMS = ("0400", "0600", "0644")

failures = []
# 不判红、但要印出来的读数：某个通道从来没配过，它名下的凭证声明不适用。与失败分开，
# 是因为两者的修法相反——失败要人去修，不适用要人去决定「这个通道要不要开」。
notices = []
# 读不到：门自己问不出来（systemctl 跑不起来之类）。它既不等于「对齐」，也不等于
# 「漂移」——两种判词都要有读数为前提，而这里的前提本身缺了。退出码 3 与读数家族同形。
unreadable = []


def fail(msg):
    failures.append(msg)


def parse_cred_line(where, body):
    """把一行 `# cred:` 解成 (perms, rotator, note)；解不全就点名，返回 None。"""
    note = ""
    if " — " in body:
        body, note = body.split(" — ", 1)
    fields = {}
    for m in FIELD.finditer(body):
        fields[m.group("key")] = m.group("val")
    rest = FIELD.sub("", body).strip()
    ok = True
    if rest:
        fail(f"{where} 的 `# cred:` 里有认不出的字段 {rest!r}：只认 perms= 与 rotator=")
        ok = False
    perms = fields.get("perms")
    if perms is None:
        fail(f"{where} 的 `# cred:` 没写 perms=")
        ok = False
    elif perms not in PERMS:
        fail(f"{where} 的 perms={perms} 不在闭集 {'/'.join(PERMS)} 里")
        ok = False
    rotator = fields.get("rotator")
    if not rotator:
        fail(f"{where} 的 `# cred:` 没写 rotator=：这个凭证谁能换掉，是这一行存在的理由之一")
        ok = False
    if not note.strip():
        fail(f"{where} 的 `# cred:` 没写 ` — ` 之后那句：凭什么可以有这一行。"
             "写不出来的，就不该有这一行")
        ok = False
    return (perms, rotator, note.strip()) if ok else None


# 扫面里到底有几个单元文件。判词必须把它印出来：这条门的判词读作「宿主凭证面干净」，
# 而它真正扫过的只是本仓 deploy/systemd 下那几个文件——不印分母，绿的就成了整台宿主。
# 2026-10-20 实测：本仓 2 个单元、宿主实装 59 个用户单元，门只覆盖其中 1 个持有凭证的。
scanned = {"units": 0}


def unit_directives():
    """单元里的凭证指令与它们头上那行声明。

    返回 [unit, kind, name, source, perms, rotator, note] 七元组表——列位与它取代的
    登记表一致，下游（`--live` 的权限比对、通道标记）不用另改一套读法。
    """
    rows = []
    files = sorted(glob.glob(f"{UNIT_DIR}/*.service") + glob.glob(f"{UNIT_DIR}/*.timer"))
    scanned["units"] = len(files)
    if not files:
        fail(f"{UNIT_DIR} 下没有单元：判据失效（空集合会让「每条指令都有声明」恒真）")
        return rows
    for path in files:
        unit = os.path.basename(path)
        pending = None          # [(行号, 正文)]：上一条指令之后攒下的 `# cred:` 行
        with open(path, encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, 1):
                body = line.rstrip("\n")
                if not body.strip():
                    continue
                if body.lstrip().startswith("#"):
                    m = DECL.match(body)
                    if m:
                        pending = (pending or []) + [(lineno, m.group("body").strip())]
                    continue
                m = DIRECTIVE.match(body)
                if not m:
                    # 别的指令会把攒下的声明留在身后，交给下面统一报「挂空了」
                    if pending:
                        for decl_line, _ in pending:
                            fail(f"{unit}:{decl_line} 的 `# cred:` 挂在一条不是凭证指令的"
                                 f"行上（下一行是 {body.strip()[:40]!r}）：声明必须紧贴它要"
                                 "约束的那条 LoadCredential…／EnvironmentFile= 指令")
                        pending = None
                    continue
                kind, dash, name_or_path, maybe = m.groups()
                where = f"{unit}:{lineno}"
                if kind == "EnvironmentFile":
                    entry = (unit, kind, "-", name_or_path)
                else:
                    if maybe is None:
                        fail(f"{where} 的 {kind} 缺 `名字:路径` 里的一半")
                        if pending:
                            pending = None
                        continue
                    entry = (unit, kind, name_or_path, maybe)
                decl = None
                if not pending:
                    fail(f"{where} 的 {kind}={name_or_path} 上没有 `# cred:` 行：这个凭证该是"
                         "几位权限、谁能换、凭什么留在宿主上，是声明出来的判断；"
                         "写不出来就该把它拿掉")
                elif len(pending) > 1:
                    fail(f"{where} 上面挂了 {len(pending)} 行 `# cred:`"
                         f"（第 {', '.join(str(d[0]) for d in pending)} 行）："
                         "一条指令一行声明，多出来的那几行说的是哪条？")
                else:
                    decl = parse_cred_line(f"{unit}:{pending[0][0]}", pending[0][1])
                    pending = None
                if decl is None:
                    # 声明解不全：仍然把这条指令记下来（下游按七列读），缺的字段留空，
                    # 免得后面按字典取键时炸出 traceback——判词已经点过名了。
                    decl = ("", "", "")
                rows.append(list(entry) + list(decl))
        if pending:
            header = "  ".join(f"{ln}: {body}" for ln, body in pending)
            fail(f"{unit} 末尾有 `# cred:` 没有挂到任何指令上（{header}）："
                 "那条指令被删掉了，声明还在")
    return rows


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
    # 部署面一个文件都没读到的时候，grep 的「零命中」与「部署面里没有这个变量」印的是
    # 同一句话。grep 自己的退出码要先收下：>1 是它读不动（目录不在、权限），1 才是没有
    # 命中——那正是这条判据要找的绿。
    surface = ["deploy/rendered", "deploy/helm", "deploy/k3s"]
    missing = [d for d in surface if not os.path.isdir(d)]
    if missing:
        fail(f"{missing} 不在树里：没读到部署面就判不了它里面有没有 RESTIC_PASSWORD")
        return
    files = sum(len(names) for root in surface for _, _, names in os.walk(root))
    if files < SURFACE_FILES_FLOOR:
        fail(f"部署面只读到 {files} 个文件（下限 {SURFACE_FILES_FLOOR}）：零个文件与"
             "「没有一个引用 RESTIC_PASSWORD」在输出上同形，这一跑什么都没判")
        return
    hits = subprocess.run(
        ["grep", "-rln", "RESTIC_PASSWORD", *surface],
        capture_output=True, text=True)
    if hits.returncode > 1:
        fail(f"读不出部署面（grep 退出 {hits.returncode}）：{hits.stderr.strip()}——"
             "没读到就判不了里面有没有 RESTIC_PASSWORD")
        return
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
    # 先证明 systemctl 这一侧问得出来，再问具体单元。没有这一步，「systemctl 坏了」与
    # 「单元没装」逐字同形：两者都是 `systemctl cat` 非零，而修法完全相反（一个是修宿主
    # 的 systemd/权限，一个是把单元装上）。分开之后，问不出来走 unreadable（退出 3），
    # 不问任何一条单元——不问就判，等于把「不知道」印成「没有」。
    try:
        probe = subprocess.run(["systemctl", "--version"],
                               capture_output=True, text=True)
        probe_rc, why = probe.returncode, (probe.stderr or "").strip().splitlines()
    except OSError as exc:
        probe_rc, why = 1, [f"{type(exc).__name__}: {exc}"]
    if probe_rc != 0:
        unreadable.append(
            "--live：systemctl 跑不起来"
            + (f"（{why[0][:120]}）" if why else "")
            + "——宿主安装面这一跑读不到，不是「单元没装」")
        return
    # 按列位取名，不按位置解包：这七列里任何一列改形状，解包会抛 ValueError，
    # 门于是打出一段 traceback 然后退出 1——看起来是红，实际没判任何一条。
    # 每个单元只 `systemctl cat` 一次：同一单元的多条凭证共用同一份输出，
    # 逐行去问会让一个未安装的单元印出与声明条数一样多的同一句话。
    by_unit = {}
    # 每个单元的「这个通道配过没有」的标记：单元里那条 EnvironmentFile 的源路径。
    # 安装脚本把它写在凭证之后、单元装好之后，所以它在 ⇔ 这个通道至少装过一次。
    # 没有 EnvironmentFile 的单元没有这个标记，缺装一律按失败判（没有更好的判法
    # 就不许把它读成「不适用」）。
    channel_marker = {}
    for cols in rows:
        by_unit.setdefault(cols[0], []).append(cols)
        if cols[1] == "EnvironmentFile" and cols[0] not in channel_marker:
            channel_marker[cols[0]] = cols[3]
    for unit in sorted(by_unit):
        got = subprocess.run(["systemctl", "cat", unit], capture_output=True, text=True)
        if got.returncode != 0:
            marker = channel_marker.get(unit)
            if marker and not os.path.exists(marker):
                notices.append(
                    f"--live：{unit} 未装，且 {marker} 不存在——这个通道从来没配过，"
                    f"名下 {len(by_unit[unit])} 条声明不适用（要开这个通道就跑 "
                    f"deploy/scripts/install-backup-offsite.sh）")
                continue
            if marker:
                fail(f"--live：{unit} 没有装在宿主上，但 {marker} 还在"
                     f"——通道配过、单元却没了（被删或被覆盖），名下 {len(by_unit[unit])} 条"
                     f"凭证声明一条也核不了")
            else:
                fail(f"--live：{unit} 没有装在宿主上（systemctl cat 失败）"
                     f"——它名下的 {len(by_unit[unit])} 条凭证声明一条也核不了")
            continue
        for cols in by_unit[unit]:
            kind, name, source, perms = cols[1], cols[2], cols[3], cols[4]
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
            if perms and mode != perms:
                fail(f"--live：{source} 权限是 {mode}，`# cred:` 声明的是 {perms}")
    # 定时器与它那个单元同一口径：通道没配过就没有「腿不会跑」这回事，只有「这条腿
    # 要不要开」这个决定。判据用同一个标记，两处不会各说各话。
    marker = channel_marker.get("cogneva-backup-offsite.service")
    installed = subprocess.run(["systemctl", "list-unit-files", "--no-legend",
                                "cogneva-backup-offsite.timer"], capture_output=True, text=True)
    if "cogneva-backup-offsite.timer" not in installed.stdout:
        if marker and not os.path.exists(marker):
            notices.append("--live：cogneva-backup-offsite.timer 不适用（通道未配置）")
        else:
            fail("--live：cogneva-backup-offsite.timer 没有装（enabled 之前这条腿根本不会跑）")


def report_host_scope():
    """印出宿主实装与本仓扫面的差集。

    本门只判本仓 `deploy/systemd` 里那几个单元；宿主上另外还装着用户单元、会话单元、
    别的树的单元。它们不在判据内，但必须让读者看见这个差——否则「OK」会被读成
    「整台宿主上没有凭证面」。不判红：那些单元的归属在其他树（私有仓），把它们登记
    进一个公开仓的登记表，等于把私有树的名字搬到公开面。这里只给分母。
    """
    if not LIVE:
        return
    declared = {os.path.basename(p) for p in
                glob.glob(f"{UNIT_DIR}/*.service") + glob.glob(f"{UNIT_DIR}/*.timer")}
    # 只看「这台机器上加的」单元目录，不走 `systemctl list-unit-files`：后者会把发行版
    # 自带的几百个 vendor 单元（app-*、dbus-* 之类）一起算进来，分母因此变成一个与凭证
    # 面无关的大数，读数就没法读。~/.config 与 /etc/systemd/system 才是本机加的那一层。
    # 目录可由 `COGNEVA_HOST_UNIT_DIRS`（冒号分隔）覆盖：测试必须吃注入的目录，不能吃
    # 跑测试那台机器的真机单元表——那会让判词跟着宿主的负载与安装史翻。
    dirs = [d for d in os.environ.get("COGNEVA_HOST_UNIT_DIRS", "").split(":") if d] or [
        os.path.expanduser("~/.config/systemd/user"), "/etc/systemd/system"]
    total, holders = 0, []
    for d in dirs:
        files = sorted(glob.glob(f"{d}/*.service") + glob.glob(f"{d}/*.timer"))
        total += len(files)
        for path in files:
            unit = os.path.basename(path)
            if unit in declared:
                continue
            try:
                text = open(path, encoding="utf-8", errors="replace").read()
            except OSError as exc:
                notices.append(f"--live：{path} 读不到（{type(exc).__name__}）——这一格没有分母")
                continue
            for lineno, line in enumerate(text.splitlines(), 1):
                if line.lstrip().startswith("#"):
                    continue
                if DIRECTIVE.match(line):
                    holders.append(f"{unit}:{lineno}")
                    break
    if holders:
        shown = "、".join(holders[:8]) + ("…" if len(holders) > 8 else "")
        notices.append(
            f"--live：宿主本机单元 {total} 个，其中 {len(holders)} 个带凭证指令却不在本仓 "
            f"{UNIT_DIR}（{shown}）——本门不覆盖它们，这些单元的凭证面归各自所在的树判")
    else:
        notices.append(
            f"--live：宿主本机单元 {total} 个（~/.config/systemd/user 与 /etc/systemd/system），"
            f"除本仓 {UNIT_DIR} 外没有一个带凭证指令——本门覆盖本仓 {scanned['units']} 个")


def main():
    rows = unit_directives()
    check_literal_secrets()
    check_no_password_in_rendered()
    check_encrypt_names(rows)
    check_execstart()
    check_live(rows)
    report_host_scope()

    if failures:
        print("HOST CREDENTIAL SURFACE FAIL：", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        for u in unreadable:
            print(f"  - 另，读不到：{u}", file=sys.stderr)
        sys.exit(1)

    if unreadable:
        print("HOST CREDENTIAL SURFACE UNREADABLE：", file=sys.stderr)
        for u in unreadable:
            print(f"  - {u}", file=sys.stderr)
        sys.exit(3)

    for n in notices:
        print(n)

    encrypted = sum(1 for r in rows if r[1] == "LoadCredentialEncrypted")
    units = len({r[0] for r in rows})
    print(f"HOST CREDENTIAL SURFACE OK：{len(rows)} 条凭证指令各带一行 `# cred:` 声明、"
          f"覆盖 {units} 个单元"
          f"（扫面：{UNIT_DIR} 下 {scanned['units']} 个单元文件）；"
          f"其中加密凭证 {encrypted} 个，env 文件 {len(rows) - encrypted} 个；"
          f"渲染物与 chart 里零 RESTIC_PASSWORD"
          + ("；--live 已核宿主实际安装与权限" if LIVE else ""))


try:
    main()
except Exception as exc:  # 门自己出错也要印成一句判词，别只留一段 traceback 让读者猜
    import traceback
    traceback.print_exc()
    print(f"HOST CREDENTIAL SURFACE FAIL：门在读面时出错（{type(exc).__name__}: {exc}）"
          "——这不是凭证面的读数，是门自己坏了", file=sys.stderr)
    # 3 而不是 1：调用方按码分派时，1 的意思是「有漂移，去改」，而这里一个字的漂移都
    # 没判出来。让「门坏了」与「面红了」共用一个码，等于把后者的处置交给前者。
    sys.exit(3)
PYEOF
