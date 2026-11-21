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
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402

CELL = re.compile(r"代码\s*:\s*test\s*=\s*([^\s:]+)::([A-Za-z0-9_]+)")


def parse_cell(item) -> tuple[str, str] | None:
    """从那一行里读出 (文件, 函数)。写不成这个形状就是没填。"""
    match = CELL.search(item.text)
    if not match:
        return None
    return match.group(1), match.group(2)


def in_diff(root: pathlib.Path, base: str, path: str) -> bool:
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", base],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:
        raise SystemExit(2)
    return path in {line.strip() for line in out.stdout.splitlines()}


def defines_test(root: pathlib.Path, path: str, name: str) -> bool:
    file = root / path
    if not file.is_file():
        return False
    text = file.read_text(errors="ignore")
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
    parser.add_argument("--passed", default=None, help="这次测试跑绿的测试名名单（一行一个）")
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

    passed: set[str] | None = None
    if args.passed:
        names = pathlib.Path(args.passed).read_text(errors="ignore").splitlines()
        passed = {name.strip() for name in names if name.strip()}

    fake: list[str] = []
    real: list[str] = []
    for item in items:
        parsed = parse_cell(item)
        if parsed is None:
            fake.append(f"{item.id}：`代码` 格不是 `test = 文件::函数` 的形状")
            continue
        path, name = parsed
        if not in_diff(root, base, path):
            fake.append(f"{item.id}：点名的 {path} 在这一批的 diff 里没有改动")
            continue
        if not defines_test(root, path, name):
            fake.append(f"{item.id}：{path} 里没有带测试属性的 `fn {name}`")
            continue
        if passed is not None and name not in passed:
            fake.append(f"{item.id}：`{name}` 没在这一次的绿名单里")
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
