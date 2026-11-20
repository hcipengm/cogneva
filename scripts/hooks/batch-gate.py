#!/usr/bin/env python3
"""默认做完整批：没做的待办必须逐条点名，并给出四条理由之一。

这一推里，台账上**所有未完成的待办**要么做了（它的触点在改动里），要么被
`Split:` 逐条点名排除。两句判据：

  1. **没点名的排除 = 拒**。`Split:` 必须列出它排除的 S 号：
         Split: 1 — S48, S63 — <为什么这两条这一批做不了>
     没有被点名的未完成待办出现在「没做」的集合里 ⇒ 拒，并把它们列出来。
  2. **理由要过真假判定**（四条里三条可机检）：
     - 1 验证面不同   两侧映射到的 cargo 包集合必须不同，相同即假；
     - 2 失败不可归因 机器判不了真假，但必须点名会重叠的测试目标，记进历史；
     - 3 不可逆/不对称 必须这一批里有一处碰 deploy/、migrations/、scripts/host/；
     - 4 一次读不完   被排除那几行的正文估算 token 必须超过窗口里它能占的份额
                      （窗口由模型声明导出，`--window-tokens` 或 `COGNEVA_CONTEXT_WINDOW`）。

台账由 `COGNEVA_TODO_LEDGER` 指定（或 `--ledger`）。读不到 ⇒ 退 2「没判」，不是绿。

退出码：0 允许；1 拒；2 读不到。
"""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import subprocess
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402
def resolve_ledger(explicit: str | None) -> pathlib.Path:
    """台账路径：显式 → 环境变量 → 本机配置文件 → 默认（不存在就是没有台账）。

    本机那一格是 `~/.config/cogneva/todo-ledger`，一行路径。台账在哪里是这台机器的
    事实，写进公开仓就成了「谁的机器」的猜测。
    """
    if explicit:
        return pathlib.Path(explicit)
    env = os.environ.get("COGNEVA_TODO_LEDGER")
    if env:
        return pathlib.Path(env)
    host = pathlib.Path.home() / ".config/cogneva/todo-ledger"
    if host.exists():
        line = host.read_text(errors="ignore").strip().splitlines()
        if line and line[0].strip():
            return pathlib.Path(line[0].strip()).expanduser()
    return pathlib.Path.home() / ".local/state/cogneva/todo-ledger.md"


OPEN_WORDS = ("未落", "部分", "进行中", "待落", "待裁", "未开始")
# 台账上的「这一轮」：行里写 `round: current` 的算本轮批次，其余是积压。积压不会被
# 悄悄丢掉——工具每次运行都把它的条数印出来，而且它一旦被选中就必须写回 `round:`。
# 没有一行标 `round:` 时，全部未完成待办都算本轮（默认全量）。
IRREVERSIBLE = ("deploy/", "migrations/", "scripts/host/")


def estimate_tokens(text: str) -> int:
    cjk = len(re.findall(r"[一-鿿]", text))
    return cjk + max(0, len(text) - cjk) // 4


def open_todos(spec: str):
    """台账 → 未完成条目。来源的形状见 `todo_source.py`；读不到抛 Unreadable。"""
    return todo_source.load(spec)


def changed(root: pathlib.Path, base: str) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True, text=True,
    ).stdout
    return [l.strip() for l in out.splitlines() if l.strip()]


def messages(root: pathlib.Path, base: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "log", "--format=%B", f"{base}..HEAD"],
        capture_output=True, text=True,
    ).stdout


def packages(paths: list[str]) -> set[str]:
    return {"/".join(p.split("/")[:2]) for p in paths if "/" in p}


def judge(kind: str, excluded: list[str], detail: str, touched: list[str], ledger_rows: dict[str, str], window: int) -> str | None:
    if kind == "1":
        sides = [s.strip() for s in re.split(r"[|｜]", detail)]
        if len(sides) != 2:
            return "Split: 1 要写两侧边界（`Split: 1 — <S 号…> — <一侧路径> | <另一侧路径>`）"
        left = [p for p in touched if p.startswith(sides[0])]
        right = [p for p in touched if p.startswith(sides[1])]
        if not left or not right or packages(left) == packages(right):
            return f"Split: 1 说验证面不同，但两侧落在同一批 cargo 包（{sorted(packages(left))}）"
        return None
    if kind == "3":
        if not any(p.startswith(IRREVERSIBLE) for p in touched):
            return "Split: 3 说不可逆或代价不对称，但这一批没有一处碰 deploy/、migrations/、scripts/host/"
        return None
    if kind == "4":
        if window <= 0:
            return "Split: 4 要拿窗口份额比，但没给窗口（--window-tokens 或 COGNEVA_CONTEXT_WINDOW）"
        cost = sum(estimate_tokens(ledger_rows.get(name, "")) for name in excluded)
        share = window // 2
        if cost <= share:
            return f"Split: 4 说这些读不完，但它们估算只有 {cost} token ≤ 窗口份额 {share}"
        return None
    if kind == "2":
        if not re.search(r"[a-z0-9_]+\.rs|tests?/", detail):
            return "Split: 2 要点名会重叠的测试目标（文件或测试名）"
        return None
    return f"Split: {kind} 不在四条里（只认 1 验证面 / 2 归因 / 3 不可逆 / 4 读不完）"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--source", default=None)
    parser.add_argument("--window-tokens", type=int,
                        default=int(os.environ.get("COGNEVA_CONTEXT_WINDOW", "0")))
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()

    try:
        todos = open_todos(args.source or todo_source.default_spec())
    except todo_source.Unreadable as e:
        print(f"batch-gate: {e} —— 这一推没判。", file=sys.stderr)
        return 2
    if not todos:
        print("batch-gate: 台账里没有未完成的待办。")
        return 0

    touched = changed(root, args.base)
    rows = {item.id: item.title for item in todos}
    done = {i.id for i in todos if i.touches and any(p in touched for p in i.touches)}
    untouched = [i.id for i in todos if i.id not in done]

    if not untouched:
        print(f"batch-gate: 台账上 {len(todos)} 条未完成待办，这一推全部动到了。")
        return 0

    msg = messages(root, args.base)
    split = re.search(r"^Split:\s*([1-4])\s*—\s*([^—\n]*)—\s*(.*)$", msg, re.M)
    if not split:
        print(
            f"FAIL: 台账上还有 {len(untouched)} 条未完成待办没动，提交信息里也没有 "
            "`Split: <1-4> — <S 号…> — <理由>`。\n      "
            + "、".join(untouched),
            file=sys.stderr,
        )
        return 1

    kind, declared_raw, detail = split.group(1), split.group(2), split.group(3).strip()
    declared = [s.strip() for s in re.split(r"[,，\s]+", declared_raw) if s.strip()]
    unaccounted = [name for name in untouched if name not in declared]
    if unaccounted:
        print(
            "FAIL: 这些未完成待办既没做也没被 `Split:` 点名："
            + "、".join(unaccounted),
            file=sys.stderr,
        )
        return 1

    problem = judge(kind, declared, detail, touched, rows, args.window_tokens)
    if problem:
        print(f"FAIL: {problem}", file=sys.stderr)
        return 1

    print(f"batch-gate: 做了 {len(done)} 条，按第 {kind} 条排除 {len(declared)} 条（已逐条点名）。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
