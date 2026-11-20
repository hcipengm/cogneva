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
     - 3 不可逆/不对称 被排除的那几条与这一批之间，**恰有一侧**碰
                      deploy/、migrations/、scripts/host/（回滚一次提交回不到原状
                      的那一侧必须单独落地）；两侧都碰或都不碰 ⇒ 假；
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


def irreversible(path: str) -> bool:
    """这个路径动的是**不可回退**的面：回滚一次提交回不到原状。

    三种：`deploy/`（推下去就滚动）、`scripts/host/`（现场的工具）、迁移目录。
    迁移目录按**目录段**判而不是按前缀：迁移住在 `crates/<crate>/migrations/` 里，
    按前缀判的话这条理由永远匹配不到任何迁移。
    """
    p = path.lstrip("./")
    # 测试文件不算：它们不改任何已部署的东西，回滚一次提交就回到原状。按目录前缀
    # 一刀切会把「改了 deploy/ 下的一个夹具」读成一次滚动，于是这条理由又能被借。
    segments = p.split("/")
    if "tests" in segments or p.endswith(".test.sh"):
        return False
    return (
        p.startswith("deploy/")
        or p.startswith("scripts/host/")
        or p.startswith("migrations/")
        or "/migrations/" in p
    )


def estimate_tokens(text: str) -> int:
    cjk = len(re.findall(r"[一-鿿]", text))
    return cjk + max(0, len(text) - cjk) // 4


def open_todos(spec: str):
    """台账 → 未完成条目。来源的形状见 `todo_source.py`；读不到抛 Unreadable。"""
    return todo_source.load(spec)


def changed(root: pathlib.Path, base: str) -> list[str]:
    """自开工以来改过的文件：基准是 round 事件里记的 code_rev，不是「与主线的差」。

    两道门（编译前、推之前）必须量同一件事，否则同一个批次在两道门上是两个数。
    """
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", base or "origin/main...HEAD"],
        capture_output=True, text=True,
    ).stdout
    files = [l.strip() for l in out.splitlines() if l.strip()]
    return files


def messages(root: pathlib.Path, base: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "log", "--format=%B", f"{base}..HEAD"],
        capture_output=True, text=True,
    ).stdout


def packages(paths: list[str]) -> set[str]:
    return {"/".join(p.split("/")[:2]) for p in paths if "/" in p}


def judge(kind: str, excluded: list[str], detail: str, touched: list[str], items: dict, window: int) -> str | None:
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
        # 这条理由的全部内容是「被拆开的两侧里，恰有一侧动的是回不去的东西」：
        # 那一侧必须单独落地，因为回滚一次提交回不到原状，混在别人里面会让别人
        # 也没法单独回滚。所以判的是**两侧的不对称**，不是「这批里有没有 deploy/」：
        # 两侧都碰 ⇒ 拆开没有把不可回退的那一侧拎出来，理由不成立；
        # 两侧都不碰 ⇒ 没有回不去的东西，理由更不成立。
        batch_irr = sorted(p for p in touched if irreversible(p))
        excl_irr = sorted(
            p for name in excluded for p in getattr(items.get(name), "touches", []) if irreversible(p)
        )
        if batch_irr and excl_irr:
            return (
                "Split: 3 说不可逆或不对称，但两侧都碰了不可回退的面：这一批动了 "
                + "、".join(batch_irr[:3])
                + "，被排除的那几条也要动 "
                + "、".join(excl_irr[:3])
                + "。两侧都回不去，拆开并没有把不可回退的那一侧单独拎出来"
            )
        if not batch_irr and not excl_irr:
            return (
                "Split: 3 说不可逆或不对称，但两侧都没碰 deploy/、migrations/、scripts/host/——"
                "没有「回滚一次提交回不到原状」的东西，这条理由不成立"
            )
        return None
    if kind == "4":
        if window <= 0:
            return "Split: 4 要拿窗口份额比，但没给窗口（--window-tokens 或 COGNEVA_CONTEXT_WINDOW）"
        cost = sum(
            estimate_tokens(getattr(items.get(name), "text", "")) for name in excluded
        )
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

    # 推之前先问「这一轮开过没有」：没有 round 记录的一推，等于没人说过这一批
    # 是什么，判不了「做完了没有」。
    try:
        import round as round_mod
        current = round_mod.current_round()
    except todo_source.Unreadable as e:
        # 读不到与「没有开着的一轮」是两件事：前者要修读的路，后者要开一轮。
        print(f"batch-gate: 事件流读不到 —— {e}；这一推没判。", file=sys.stderr)
        return 2
    except Exception:
        current = None
    if not current:
        print("batch-gate: 事件流里没有开着的一轮（先 `scripts/hooks/round.py open`），这一推没判。",
              file=sys.stderr)
        return 2
    try:
        pinned = round_mod.ledger_at(current[1], todo_source.default_spec())
        round_ids = {i.id for i in todo_source.load(f"md:{pinned}")}
    except todo_source.Unreadable as e:
        print(f"batch-gate: 这一轮钉住的那版台账读不到：{e} —— 这一推没判。", file=sys.stderr)
        return 2

    try:
        items, without, _deferred = todo_source.partition(args.source or f"md:{pinned}")
        if without:
            print(
                "batch-gate: 这些未完成待办没有 `touches:`，它们既做不了也判不了："
                + "、".join(without),
                file=sys.stderr,
            )
            return 2
        todos = items
    except todo_source.Unreadable as e:
        print(f"batch-gate: {e} —— 这一推没判。", file=sys.stderr)
        return 2
    if not todos:
        print("batch-gate: 台账里没有未完成的待办。")
        return 0

    touched = changed(root, current[2] or "")
    items = {item.id: item for item in todos}
    done = {i.id for i in todos if i.touches and any(p in touched for p in i.touches)}
    todos = [i for i in todos if not round_ids or i.id in round_ids]
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

    problem = judge(kind, declared, detail, touched, items, args.window_tokens)
    if problem:
        print(f"FAIL: {problem}", file=sys.stderr)
        return 1

    print(f"batch-gate: 做了 {len(done)} 条，按第 {kind} 条排除 {len(declared)} 条（已逐条点名）。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
