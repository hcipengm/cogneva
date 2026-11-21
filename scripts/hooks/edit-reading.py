#!/usr/bin/env python3
"""`代码` 那一格：由程序算出来，不许手填。

一行的写法只有一种，写成别的（散文、路径、commit 号）一律读作**未填**：

    代码: test = <测试文件>::<测试函数>

三个条件全真才算「改完了」，任何一条不成立就读 false：

  1. **文件在本批 diff 里**——相对这一轮的 `code_rev` 有改动。这一条把「测试是这一批
     才写的」钉住：一个早就绿着的测试不能拿来冒充这一批的活。
  2. **函数真的在那儿**——文件里找得到 `fn <名字>`，且它前面有 `#[test]` 或
     `#[tokio::test]`。
  3. **这一次跑绿了**——由测试那一次跑出来的名单（`--passed`）给出；给了名单就必须在
     里面，没给就只判前两条（那是「编辑期」的读数，第 5 步之后才是最终读数）。

为什么不是「触点动过」：触点只说明有人碰了那条路径，改了一半、改错了地方、只是顺路
经过，三种在它下面同形。这条判据问的是另一件事——这一条待办的验证跑过没有、测的是
不是这一次的改动。

退出码：0 整批都真；1 有假的（逐条点名）；2 读不到（台账/树/git）。
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402

# 一格两个名字曾经并存（`代码:` 与 `判据读数:`），而同一件事两个名字必然分叉：
# 1.5 写进去的那个，2.5 必须读得出来。两个都认，写的时候只写 `判据读数`。
CELL = re.compile(r"(?:代码|判据读数)\s*:\s*(?:test\s*=\s*)?((?:brain:)?[^\s:]+)::([^\s:｜|]+)")
# 计数型条目：`判据读数: <普查面> → <目标>`（`->` 也认）。
COUNT = re.compile(r"判据读数\s*:\s*([a-z0-9-]+)\s*(?:→|->)\s*(\d+)")


def surface_count(census: pathlib.Path, root: pathlib.Path, surface: str) -> int | None:
    """跑一次普查，取那一面的条数。读不到返回 `None`：读不到不是 0。"""
    out = subprocess.run(
        [sys.executable, str(census), "--root", str(root), "--surface", surface, "--tsv"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        return None
    rows = [line for line in out.stdout.splitlines() if line.strip() and "\t" in line]
    # 只去掉列名那一行：计数行没有制表符，本来就不在这里面。
    rows = [line for line in rows if not line.split("\t")[0].strip().endswith("surface")]
    return len(rows)


def parse_cell(item) -> tuple[str, str] | None:
    """从那一行里读出 (文件, 函数)。写不成这个形状就是没填。"""
    match = CELL.search(item.text)
    if not match:
        return None
    return match.group(1), match.group(2)


def in_diff(root: pathlib.Path, base: str, path: str) -> bool:
    # `brain:` 那几族不在这一棵树的 diff 里：它们由台账仓自己的门判，这里放行。
    if path.startswith("brain:"):
        return True
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", base],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(2)
    return path in {line.strip() for line in out.stdout.splitlines()}


def resolve_test_file(root: pathlib.Path, path: str) -> pathlib.Path:
    """`brain:` 前缀指台账那个仓：几族的证据本来就在那儿（普查器、宿主侧的夹具）。
    不认这个前缀，它们永远填不上——而「只认这一棵树」不是普适。"""
    if path.startswith("brain:"):
        repo = os.environ.get("COGNEVA_LEDGER_REPO") or str(
            pathlib.Path.home() / "work/cogneva-brain"
        )
        return pathlib.Path(repo) / path[len("brain:") :]
    return root / path


def defines_test(root: pathlib.Path, path: str, name: str) -> bool:
    file = resolve_test_file(root, path)
    if not file.is_file():
        return False
    text = file.read_text(errors="ignore")
    # shell 夹具也是测试（`progress-watchdog.test.sh` 这类）：判据不认它们，等于几族
    # 待办永远填不上。它们的「用例名」是文件里那些 `ok "..."` 行的标签。
    if path.endswith((".sh", ".py")):
        return name in text
    # 函数定义前 200 字里有测试属性：够近，又不会被别的函数借去。
    for match in re.finditer(rf"fn\s+{re.escape(name)}\s*\(", text):
        head = text[max(0, match.start() - 200) : match.start()]
        if "#[test]" in head or "#[tokio::test]" in head:
            return True
    return False


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--source", default=None, help="台账来源（默认这台机器的那一本）")
    parser.add_argument("--base", default=None, help="这一轮的 code_rev；不给就问 round")
    parser.add_argument("--stage", choices=["edit", "final"], default="edit",
                        help="edit：2.4 之后、编译之前（不要求绿）；final：第 5 步之后（要绿，且要红过）")
    parser.add_argument("--passed", default=None, help="这次测试跑绿的测试名名单（一行一个）")
    parser.add_argument("--census", default=None,
                        help="普查器路径（计数型条目要它）：不给则计数型条目判不了")
    parser.add_argument("--red-at-base", default=None,
                        help="在 code_rev 上跑红的测试名名单：红过才算「没有这次改动就会红」")
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()
    source = args.source or todo_source.default_spec()
    try:
        items = todo_source.load(source)
    except todo_source.Unreadable as e:
        print(f"edit-reading: 台账读不到：{e} —— 没判。", file=sys.stderr)
        return 2

    base = args.base
    if base is None:
        try:
            import round as round_mod
            current = round_mod.current_round()
            base = current[2] if current and len(current) > 2 else None
        except Exception:
            base = None
    if not base:
        print("edit-reading: 不知道这一批的起点（没有开着一轮，也没给 --base）—— 没判。", file=sys.stderr)
        return 2

    red_at_base: set[str] | None = None
    if args.red_at_base:
        names = pathlib.Path(args.red_at_base).read_text(errors="ignore").splitlines()
        red_at_base = {name.strip() for name in names if name.strip()}

    passed: set[str] | None = None
    if args.passed:
        names = pathlib.Path(args.passed).read_text(errors="ignore").splitlines()
        passed = {name.strip() for name in names if name.strip()}

    # 普查器跟着树走：判的是这棵树的面，工具就该在这棵树里（带外那本 brain 仓里的
    # 是同一个脚本，但一道门依赖仓外的东西时，它随时会因为「那边没同步」而判不了）。
    census: pathlib.Path | None = None
    default_census = root / "scripts/bounds-census.py"
    if not args.census and default_census.is_file():
        census = default_census
    if args.census:
        candidate = pathlib.Path(args.census)
        census = candidate if candidate.is_file() else None
        if census is None:
            print(f"edit-reading: 普查器不在：{candidate} —— 计数型条目判不了。", file=sys.stderr)
            return 2

    fake: list[str] = []
    real: list[str] = []
    for item in items:
        # 计数型先判：它比形状硬，而且不用编译。
        counted = COUNT.search(item.text)
        if counted:
            surface, target = counted.group(1), int(counted.group(2))
            if census is None:
                fake.append(f"{item.id}：计数型条目，但没给 --census —— 判不了")
                continue
            count = surface_count(census, root, surface)
            if count is None:
                fake.append(f"{item.id}：普查面 {surface} 跑不出来 —— 判不了")
                continue
            if count > target:
                fake.append(f"{item.id}：{surface} 还剩 {count} 处（目标 {target}）")
                continue
            real.append(item.id)
            continue

        parsed = parse_cell(item)
        if parsed is None:
            fake.append(f"{item.id}：`代码` 格不是 `test = 文件::函数` 的形状")
            continue
        path, name = parsed
        # 开工之前就做完的那些条目：证据本来就不在这一批的 diff 里。它们要显式写
        # `(preexisting)`，否则「借一个早就绿的测试冒充这一批」与「这一条早就做完了」
        # 在判据下同形——前一版就分不开这两件事。
        preexisting = "(preexisting)" in item.text
        if not preexisting and not in_diff(root, base, path):
            fake.append(f"{item.id}：点名的 {path} 在这一批的 diff 里没有改动")
            continue
        if not defines_test(root, path, name):
            fake.append(f"{item.id}：{path} 里没有带测试属性的 `fn {name}`")
            continue
        if args.stage == "final" and passed is None:
            fake.append(f"{item.id}：终态档要绿名单，没给 --passed —— 判不了")
            continue
        if passed is not None and name not in passed:
            fake.append(f"{item.id}：`{name}` 没在这一次的绿名单里")
            continue
        if args.stage == "final" and red_at_base is not None and name not in red_at_base:
            fake.append(
                f"{item.id}：`{name}` 在 code_rev 上就是绿的——它没有测到这一次的改动"
            )
            continue
        real.append(item.id)

    for item_id in real:
        print(f"  代码=true  {item_id}")
    for line in fake:
        print(f"  代码=false {line}")
    print(f"edit-reading: {len(real)} 条为真，{len(fake)} 条为假（共 {len(items)} 条）")
    return 0 if not fake else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
