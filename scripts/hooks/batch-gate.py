#!/usr/bin/env python3
"""默认做完整批：没做的待办必须逐条点名，并给出两条理由之一。

这一推里，本轮批次里的**所有未完成待办**要么做完了（五格行改成 `已落` 且差距为空；
三格行填了 `代码` 格），要么被 `Split:` 逐条点名排除。触点不是判据——「有文件改过」
与「做完了」不是一件事（属主 2026-11-21 定），「能不能写出终态」才是批次成员资格的
判据（属主 2026-11-24 定）。两句判据：

  1. **没点名的排除 = 拒**。`Split:` 必须列出它排除的 S 号：
         Split: 2 — S48, S63 — <为什么这两条这一批做不了>
     没有被点名的未完成待办出现在「没做」的集合里 ⇒ 拒，并把它们列出来。
  2. **理由要过真假判定**（只认两条）：
     - 2 失败不可归因 机器判不了真假，但必须点名会重叠的测试目标，记进历史；
     - 4 一次读不完   被排除那几行的正文估算 token 必须超过窗口里它能占的份额
                      （窗口由模型声明导出，`--window-tokens` 或 `COGNEVA_CONTEXT_WINDOW`）。

批次成员由这一轮钉住的那版台账定；**做没做完读现在这本台账**——钉住的那版只回答
「这一批有哪几条」，拿它判完成度会把刚做完的行永远读成没做。

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


def estimate_tokens(text: str) -> int:
    cjk = len(re.findall(r"[一-鿿]", text))
    return cjk + max(0, len(text) - cjk) // 4


def open_todos(spec: str):
    """台账 → 未完成条目。来源的形状见 `todo_source.py`；读不到抛 Unreadable。"""
    return todo_source.load(spec)



def is_touched(path: str, touched: list[str]) -> bool:
    """触点动过没有。目录触点读作「它下面的任何一个文件动了」。

    按文件逐条相等去比，目录触点永远对不上——一条不可能被满足的判据与一条永远为真的
    判据一样没用，而这一条卡住的是整批（缺口不空就不许编译）。
    """
    p = path.rstrip("/")
    if p in touched:
        return True
    prefix = p + "/"
    return any(f.startswith(prefix) for f in touched)


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


def judge(kind: str, excluded: list[str], detail: str, touched: list[str], items: dict, window: int) -> str | None:
    if kind in ("1", "3"):
        # 这两条在 2026-11-24 之前可以用；属主收掉了：默认全部是一批，只有
        # 「失败不可归因」与「一次读不完」能把一条拆离。
        return (
            f"Split: {kind} 不再是合法理由（只认 2 失败不可归因 / 4 一次读不完）："
            "默认全部是一批，不能按验证面或不可逆性挑子集"
        )
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
    return f"Split: {kind} 不是合法理由（只认 2 失败不可归因 / 4 一次读不完）"


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
    # 这一轮钉住的只是**清单**（这一批有哪几条）；做没做完读**现在这本**台账。
    # 拿钉住的那一版判完成度，刚做完的行会永远读成没做——那一版写的时候它们还没做。
    live = todo_source.normalize(args.source or todo_source.default_spec())
    try:
        pinned = round_mod.ledger_at(current[1], todo_source.default_spec())
        round_ids = {i.id for i in todo_source.load(f"md:{pinned}")}
        items, without, deferred = todo_source.partition(live)
        known = todo_source.all_ids(live)
    except todo_source.Unreadable as e:
        print(f"batch-gate: 台账读不到：{e} —— 这一推没判。", file=sys.stderr)
        return 2

    vanished = sorted(round_ids - known)
    if vanished:
        # 被删掉不等于做完：钉住过的那条从台账里消失，要么是有人做完了却删了行，
        # 要么是它被悄悄拿掉了。两种都要人来看，不能读成「这一批全绿」。
        print(
            "FAIL: 这一轮钉住的条目在台账里被删掉了（删掉不当作做完）："
            + "、".join(vanished),
            file=sys.stderr,
        )
        return 1

    # 触点不是判据（属主 2026-11-24）：没写触点、写了 `unknown` 的行都在本批里，
    # 只是「这一遍该动哪些文件」读不出来。它们照旧要按终态做完或被 Split 点名。
    if without or deferred:
        print(
            f"batch-gate: {len(without) + len(deferred)} 条没写触点（`unknown` {len(deferred)} 条），"
            "仍在本批，按终态判。",
            file=sys.stderr,
        )
    todos = [i for i in items if not round_ids or i.id in round_ids]
    if not todos:
        print("batch-gate: 本轮批次里没有未完成的待办。")
        return 0

    touched = changed(root, current[2] or "")
    # 台账仓那一侧的改动面：这一族有一部分产物写在另一个仓里（那一侧自己的门、这本
    # 台账本身），由同一个人在同一轮里写。不读那一侧，那几条永远读作「没做」，而这一推
    # 永远发不出去——判据看不到的东西，不能按「没做」处理。
    try:
        ledger_repo = round_mod.ledger_repo(todo_source.default_spec())
    except Exception:
        ledger_repo = None
    if ledger_repo is not None:
        out = subprocess.run(
            ["git", "-C", str(ledger_repo), "diff", "--name-only",
             (current[4] if len(current) > 4 else current[1]) or "HEAD"],
            capture_output=True, text=True,
        )
        if out.returncode == 0:
            touched = touched + [l.strip() for l in out.stdout.splitlines() if l.strip()]
    items = {item.id: item for item in todos}
    todos = [i for i in todos if not round_ids or i.id in round_ids]
    # 做完了没有**只认 `代码` 格**，不认触点（属主 2026-11-21：「不能以触点发现有改动
    # 就算，而是要改完算」）。触点在一条待办里是「该改哪些文件」的提示：碰巧落在同一条
    # 路径上的改动、改了一半的改动，在触点判据下与做完的改动同形。`代码` 格要求写的人
    # 点名「树里那条没了就会红的测试」，那是机器读得到的最小证据。
    # 「填了」不等于「填得对」：那一格里点名的路径要真的在树里，否则这一格只是
    # 一句关于证据的话，而它不是证据。判据只要求「写出的路径存在」——它不去猜哪条
    # 测试该绿（那要跑，属于闸门），但它能让一个凭记忆写的路径当场红。
    done: set[str] = set()
    phantom: list[str] = []
    for item in todos:
        cell = item.cells.get("代码")
        if not cell:
            continue
        # 路径从**一个**词边界开始：`<别的仓>/scripts/x.py` 里的那个
        # `scripts/x.py` 是一段更长的路径，截出来判存在会把它读成「写错了」。
        # `.github/scripts/` 是树里的第三个脚本落点，漏掉它会让那条格永远红。
        named = re.findall(
            r"(?<![\w./-])(?:\.github|crates|deploy|scripts|web)/[A-Za-z0-9_./{},*-]+", cell
        )
        missing = [
            path
            for path in named
            if "{" not in path and "}" not in path and "*" not in path
            and not (root / path).exists()
        ]
        if missing:
            phantom.append(f"{item.id}: " + ", ".join(sorted(set(missing))[:3]))
            continue
        done.add(item.id)
    if phantom:
        print(
            "batch-gate: 这些待办的 `代码` 格点名了树里不存在的路径——那一格不是证据：\n  "
            + "\n  ".join(phantom),
            file=sys.stderr,
        )
        return 1
    untouched = [i.id for i in todos if i.id not in done]

    if not untouched:
        print(f"batch-gate: 本轮批次 {len(todos)} 条未完成待办，这一推全部做完了。")
        return 0

    msg = messages(root, args.base)
    split = re.search(r"^Split:\s*([1-4])\s*—\s*([^—\n]*)—\s*(.*)$", msg, re.M)
    if not split:
        print(
            f"FAIL: 台账上还有 {len(untouched)} 条未完成待办没做，提交信息里也没有 "
            "`Split: <2|4> — <S 号…> — <理由>`（只认 2 失败不可归因 / 4 一次读不完）。\n      "
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
