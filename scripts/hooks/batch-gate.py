#!/usr/bin/env python3
"""默认做完整批：没做的待办必须逐条点名，并给出三条理由之一。

这一推里，台账上**所有未完成的待办**要么做完了（那一行填了 `代码` 格：树里有了实现，
并点名了那条「没有它就会红」的测试），要么被 `Split:` 逐条点名排除。触点不是判据——
「有文件改过」与「做完了」不是一件事（属主 2026-11-21 定）。两句判据：

  1. **没点名的排除 = 拒**。`Split:` 必须列出它排除的 S 号：
         Split: 2 — S48, S63 — <为什么这两条这一批做不了>
     没有被点名的未完成待办出现在「没做」的集合里 ⇒ 拒，并把它们列出来。
  2. **理由要过真假判定**（四条里三条可机检）：
     - 2 失败不可归因 机器判不了真假，但必须点名会重叠的测试目标，记进历史；
     - 3 不可逆/不对称 被排除的那几条与这一批之间，**恰有一侧**碰
                      deploy/、migrations/、scripts/host/（回滚一次提交回不到原状
                      的那一侧必须单独落地）；两侧都碰或都不碰 ⇒ 假；
     - 4 一次读不完   被排除那几行的正文估算 token 必须超过窗口里它能占的份额
                      （界是把这一批与**这个宿主真的收过的最大一批**比：那条读数由
                      `round.py close` 写进 `rounds-closed.tsv`，不由谁报一个窗口）。

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


def packages(paths: list[str]) -> set[str]:
    return {"/".join(p.split("/")[:2]) for p in paths if "/" in p}


def closed_batch_bound() -> int | None:
    """这个宿主上真的收过的最大一批是几条。

    读数由 `round.py close` 写在 `rounds-closed.tsv` 里（一个关得掉的轮，按定义是
    批里每一条都到了终态的那一轮）。它替代了「模型自报一个窗口」：窗口不是树上能
    量到的东西，而「这个宿主一次收过几条」能——有人真收过 7 条，那 7 条一批就不是
    「读不完」。读不到就是 None（量不到），不是 0。
    """
    path = pathlib.Path(
        os.environ.get(
            "COGNEVA_CLOSED_ROUNDS",
            str(pathlib.Path.home() / ".local/state/cogneva/rounds-closed.tsv"),
        )
    )
    if not path.is_file():
        return None
    best: int | None = None
    for line in path.read_text(errors="ignore").splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        try:
            size = int(parts[2])
        except ValueError:
            continue
        best = size if best is None else max(best, size)
    return best


def judge(kind: str, excluded: list[str], detail: str, touched: list[str], items: dict) -> str | None:
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
        bound = closed_batch_bound()
        if bound is None:
            return (
                "Split: 4 说这些一次读不完，但这个宿主还没有收过任何一批——量不到，"
                "不许当成读得完"
            )
        if len(excluded) <= bound:
            return (
                f"Split: 4 说这些读不完，但这个宿主收过 {bound} 条一批："
                f"这一侧只有 {len(excluded)} 条，不算读不完"
            )
        return None
    if kind == "2":
        if not re.search(r"[a-z0-9_]+\.rs|tests?/", detail):
            return "Split: 2 要点名会重叠的测试目标（文件或测试名）"
        return None
    return f"Split: {kind} 不在三条里（只认 2 归因 / 3 不可逆 / 4 读不完）"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--source", default=None)
    # 没有窗口旋钮：`Split: 4` 的界来自 `rounds-closed.tsv`（见 `closed_batch_bound`）。
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

    problem = judge(kind, declared, detail, touched, items)
    if problem:
        print(f"FAIL: {problem}", file=sys.stderr)
        return 1

    print(f"batch-gate: 做了 {len(done)} 条，按第 {kind} 条排除 {len(declared)} 条（已逐条点名）。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
