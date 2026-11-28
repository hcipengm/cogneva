#!/usr/bin/env python3
"""判 `FOREIGN_PENDING` 的出口标记与宿主读数名册是否对得上。

这一档治的病：告警规则读一个没登记的名字是危险的（规则永远沉默，沉默与正常同形），
而登记一个还没有读者的名字是正常的（序列先于告警存在）。两道盘点门各守一个方向，
中间态原先没法表达，于是「加一条规则」只能靠抢同一个窗口同步落地。pending 这一档
把中间态做成可数的，代价是每条必须自报**出口**：

  Witness        名字在名册里；它是不是真的在发布，由那条走名册的见证读数判。
  Unregistered   不在名册里，但本仓闭集在发布它。
  NoReading      两边都没有：没有已登记的读者能判它，条目自己写清在等什么。

判据按证据位置分两侧：形状（两表互斥、被读到即升档、自报与闭集一致、条数等于常数）
在 cogneva 的契约测试里；**标记与名册是否一致只能在另一棵树**——名册长在这棵树里（每条读数
是脚本头上的一句自声明），cogneva 的测试跑在公开仓 CI，那里没有这棵树。把名册复制过去
就是第二份正本。

判的是 cogneva 的 **HEAD**（`git cat-file` 读 `<rev>:<路径>`），不是它的共享工作树：
别人的草稿不该替所有人做决定。

出口码：0 对上了（读数印出两边的分母，不只是一个「ok」）；1 对不上（点名哪一份脚本声明了
它、标记写了什么）；3 **问不出来**（cogneva 检出不在、那份文件不在这一版里、名册读不到）
——3 不是 0：它是「这一跑什么都没判」；4 那些字节读到了，但它们自己不合形状（条目认不出、
变体名写错、条数与常数对不上、名册里的名字不合拼法）。1 是关于提交内容的发现，3 是关于这台
机器此刻看不看得见证据——两者修法不同，所以不合并成一个码。

判哪一份字节：默认 cogneva 的 **HEAD**（别人的草稿不该替所有人做决定）。`--index` 判
`GIT_INDEX_FILE` 指的那一份索引的 `:0:<路径>`——提交门在提交落定之前用它，判的正是这一笔
将要带走的字节。

Usage: python3 scripts/gates/check-pending-exits.py [--list] [--index]
       COGNEVA_SHAPES_REPO=<cogneva 检出>   # 默认 <另一棵树>/../cogneva
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path

# 另一棵树在哪：本脚本随公开仓走，推不出那个路径。`parents[2]` 是它原来住在那一侧时
# 的推法，搬过来之后指向公开仓自己的上两级——所以改成问本机配置（`cogneva.privateRoot`）。
PRIVATE_CHECKOUT = Path(
    os.environ.get("COGNEVA_PRIVATE_ROOT")
    or subprocess.run(["git", "config", "--get", "cogneva.privateRoot"],
                      capture_output=True, text=True).stdout.strip()
)
SHAPES_REPO = Path(os.environ.get("COGNEVA_SHAPES_REPO") or PRIVATE_CHECKOUT.parent / "cogneva")
SHAPES_PATH = "crates/cogneva/tests/observability_alert_rule_contract_test.rs"
# 名册的唯一读法：扫树上那几行自声明（不再有一份人写的表与它并排）。读法装在哪儿与要扫
# 哪棵树是两件事：夹具里读法还是真的那一支，树换成假的（`COGNEVA_READINGS_DECLARED_READER`）。
ROSTER_READER = Path(
    os.environ.get("COGNEVA_READINGS_DECLARED_READER")
    or PRIVATE_CHECKOUT / "scripts/host/cogneva-readings-declared.sh"
)

BAND = re.compile(
    r"const FOREIGN_PENDING: &\[\(&str, PendingExit\)\] = &\[(.*?)\n\];", re.S
)
# 空档是**一行**写的（`= &[];`），只有有内容时才是每行一条、以 `\n];` 收尾。只认多行
# 形状，抓到的就不是这个数组，而是它后面几百行代码——判出来的话是「认不出的条目」，
# 而它点名的字节与这一档毫无关系。空档正是这道门第一次执行时的那份字节。
BAND_EMPTY = re.compile(r"const FOREIGN_PENDING: &\[\(&str, PendingExit\)\] = &\[\];")
COUNT = re.compile(r"const FOREIGN_PENDING_AT_CENSUS: usize = (\d+);")
ENTRY = re.compile(
    r'\(\s*"([a-z][a-z0-9_]*)"\s*,\s*PendingExit::(Witness|Unregistered|NoReading)\s*'
    r'(?:\(\s*"((?:[^"\\]|\\.)*)"\s*\))?\s*\)'
)
NAME = re.compile(r"^[a-z][a-z0-9_]*$")


def unreadable(message):
    print(f"pending 出口未判：{message}", file=sys.stderr)
    sys.exit(3)


def malformed(message):
    print(f"pending 出口不合形状：{message}", file=sys.stderr)
    sys.exit(4)


def judgeable_bytes(use_index):
    if not SHAPES_REPO.is_dir():
        unreadable(f"cogneva 检出不在：{SHAPES_REPO}（要判就给它 COGNEVA_SHAPES_REPO）")
    where = ":0:" + SHAPES_PATH if use_index else "HEAD:" + SHAPES_PATH
    try:
        out = subprocess.run(
            ["git", "-C", str(SHAPES_REPO), "cat-file", "blob", where],
            capture_output=True,
            check=True,
        )
    except (subprocess.CalledProcessError, OSError) as exc:
        unreadable(
            f"读不出 {where}（{exc}）——工作树里有不算，判的是提交"
        )
    return out.stdout.decode("utf-8")


def parse_band(text):
    if BAND_EMPTY.search(text):
        rest = ""
    else:
        band = BAND.search(text)
        if band is None:
            malformed(f"{SHAPES_PATH} 里找不到 FOREIGN_PENDING 的数组字面量")
        rest = band.group(1)
    count = COUNT.search(text)
    if count is None:
        malformed(f"{SHAPES_PATH} 里找不到 FOREIGN_PENDING_AT_CENSUS")

    entries = []
    for match in ENTRY.finditer(rest):
        name, kind, payload = match.group(1), match.group(2), match.group(3)
        if kind == "Witness" and payload is not None:
            malformed(f"{name}：Witness 不带理由，却写了 `{payload}`")
        if kind != "Witness" and payload is None:
            malformed(f"{name}：{kind} 必须带一句理由，这一条没有")
        entries.append((name, kind, payload))
    # 形状认不出就停：静默跳过一条等于少判一个名字，而那正是这一档的宾语。
    leftover = ENTRY.sub("", rest)
    leftover = re.sub(r"//[^\n]*", "", leftover)
    leftover = re.sub(r"[\s,]", "", leftover)
    if leftover:
        malformed(
            f"FOREIGN_PENDING 里有认不出的条目（解析后还剩 `{leftover[:120]}`）——"
            "只认 `(\"名字\", PendingExit::Witness)` 与 `(\"名字\", PendingExit::X(\"理由\"))`"
        )
    if int(count.group(1)) != len(entries):
        malformed(
            f"FOREIGN_PENDING 有 {len(entries)} 条，"
            f"FOREIGN_PENDING_AT_CENSUS 写着 {count.group(1)}——这一档的条数读不出来"
        )
    return entries


def roster_rows():
    """名册：`cogneva-readings-declared.sh` 印的四列，第一列是生产者名、第二列是脚本路径。

    走那一支而不是在这里再扫一遍树：两处各扫一次，改口径（声明窗口、标记写法）时只会改到
    其中一处，而分家的样子是「同一个问题两个答案」。
    """
    reader = ROSTER_READER
    if not reader.is_file():
        unreadable(f"名册读法不在：{reader}")
    out = subprocess.run(
        ["bash", str(reader), "--root", str(PRIVATE_CHECKOUT)],
        capture_output=True, text=True, env={**os.environ, "COGNEVA_READINGS_DECLARED_ROOT": str(PRIVATE_CHECKOUT)},
    )
    if out.returncode != 0:
        unreadable(f"名册读不到：{(out.stderr or '').strip()[:200] or '（没有理由）'}")
    rows = []
    for line in out.stdout.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        cells = line.split("\t")
        name = cells[0].strip()
        if not name:
            continue
        if not NAME.match(name):
            malformed(f"{ROSTER_READER}: 名字不合拼法：`{name}`")
        rows.append((name, cells[1].strip() if len(cells) > 1 else ""))
    if not rows:
        unreadable(f"名册是空的：{reader}——条数是分母，读成 0 行会把「名册没了」判绿")
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--list", action="store_true", help="把每条 pending 与它在表里的位置印出来"
    )
    parser.add_argument(
        "--index",
        action="store_true",
        help="判 GIT_INDEX_FILE 指的那一份索引，而不是 cogneva 的 HEAD",
    )
    args = parser.parse_args()

    entries = parse_band(judgeable_bytes(args.index))
    rows = roster_rows()
    registered = {}
    for name, where in rows:
        registered.setdefault(name, where)

    complaints = []
    for name, kind, payload in entries:
        hit = registered.get(name)
        if kind == "Witness" and hit is None:
            complaints.append(
                f"{name} 标 Witness，与名册不一致：没有任何脚本头里的自声明点名它"
            )
        elif kind != "Witness" and hit is not None:
            complaints.append(f"{name} 标 {kind}，但 {hit} 头部声明了它")
        if kind != "Witness" and not (payload or "").strip():
            complaints.append(f"{name} 标 {kind} 却没写理由")

    if args.list:
        for name, kind, _ in entries:
            hit = registered.get(name)
            where = hit or "名册里没有"
            print(f"  {name}\t{kind}\t{where}")

    kinds = {}
    for _, kind, _ in entries:
        kinds[kind] = kinds.get(kind, 0) + 1
    numerator = " / ".join(f"{k} {kinds[k]}" for k in sorted(kinds)) or "无条目"
    print(
        f"pending {len(entries)} 条（{numerator}），"
        f"名册 {len(rows)} 行（分母）"
    )

    if complaints:
        print("FOREIGN_PENDING 的出口标记与名册对不上：", file=sys.stderr)
        for line in complaints:
            print(f"  {line}", file=sys.stderr)
        sys.exit(1)
    print("PENDING EXITS OK：每条 pending 自报的出口都与名册一致")


if __name__ == "__main__":
    main()
