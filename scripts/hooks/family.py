#!/usr/bin/env python3
"""这一轮的默认批次：所有未完成的待办，一次改完，再编译一次。

默认是**合并**：把所有还没做完的待办要点名的文件取并集，这就是本轮该改的全部。
拆分不是常规动作，是必须举证的例外，只认四条理由（写在拒绝信息里，也写进提交历史）：

  1. 验证面不同     一个要 CI 才判、一个要现场数据、一个要另一台机器
  2. 失败不可归因   两处改动会在同一批测试里互相掩盖，红了说不清是谁
  3. 不可逆/不对称  一处只改代码，另一处要滚动、要迁移、要动现场
  4. 一次读不完     触点多到一遍读不完，半改的风险上升——沿家族边界拆，每份自身闭合

待办的来源是一张台账（路径由 `COGNEVA_TODO_LEDGER` 指定；它不在本仓）：每一项未完成的待办在自己的行里写
`touches: <路径>[, <路径>…]`。没有这一段的未完成待办会被当场点名——没有触点的
待办不是一个可执行的批次，它只是一句话。

用法：

    family.py --all-todos            # 默认批次：所有未完成待办的触点并集 + 工作区改动
    family.py --from-git             # 只看工作区改动（闭合性检查，不含未开工的待办）
    family.py --changed a.rs b.rs    # 显式给一组改动

退出码：0 覆盖了；1 有缺口（缺口就是该继续编辑的文件）；2 读不到（台账/仓库）。
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

DEFAULT_LEDGER = pathlib.Path.home() / ".local/state/cogneva/todo-ledger.md"
def resolve_ledger(explicit: str | None) -> pathlib.Path:
    """台账路径：显式 → 环境变量 → 本机配置文件 → 默认（不存在就是没有台账）。

    本机那一格是 `~/.config/cogneva/todo-ledger`，一行路径。台账在哪里是这台机器的
    事实，写进公开仓就成了「谁的机器」的猜测。
    """
    if explicit:
        text = str(explicit)
        return pathlib.Path(text[3:] if text.startswith("md:") else text)
    env = os.environ.get("COGNEVA_TODO_LEDGER")
    if env:
        return pathlib.Path(env)
    host = pathlib.Path.home() / ".config/cogneva/todo-ledger"
    if host.exists():
        line = host.read_text(errors="ignore").strip().splitlines()
        if line and line[0].strip():
            return pathlib.Path(line[0].strip()).expanduser()
    return pathlib.Path.home() / ".local/state/cogneva/todo-ledger.md"


OPEN_WORDS = ("未落", "部分", "进行中", "待落", "待裁")

# 四张配套表：改了一处，另一处必须跟着改，否则那道门会红。
REGISTRY = "crates/cogneva/tests/rust_capacity_constants_registry_test.rs"
CHART = "deploy/helm/cogneva"
RULE_CONFIG = "deploy/helm/cogneva/files/cogneva.json"
FIXTURES = "deploy/k3s/observability/tests"
METRIC_NAMES = "crates/cog-core/src/contract/metric_names.rs"
METRIC_HELP = "crates/cog-core/src/contract/metric_help.rs"


def run(*args: str, cwd: pathlib.Path | None = None) -> str:
    return subprocess.run(args, cwd=cwd, capture_output=True, text=True).stdout


def working_tree_changes(root: pathlib.Path) -> list[str]:
    files: list[str] = []
    # 这一轮已经提交的部分也算「做了」：拿工作区去比，会把刚提交的改动读成没做。
    # 基准是**这一轮开工时的那个代码 rev**（round 事件里记着）：两道门（编译前、推之前）
    # 因此量的是同一个「自开工以来改了什么」，数字不会再对不上。
    base = ""
    try:
        import round as round_mod
        current = round_mod.current_round()
        if current and current[2]:
            base = current[2]
    except Exception:
        base = ""
    # `origin/main...HEAD` 是**没有开着一轮时**的退路，不能与「自 code_rev 以来」叠加：
    # 叠加之后量出来的是「与主线的全部差」，它比这一轮宽得多，于是推之前那道门（只读
    # `diff <code_rev>`）报 4 条没做、这道门报 0 条——两道门数出两个数，2026-11-20 实测。
    for args in ([a for a in (
        ["diff", "--name-only", base] if base else ["diff", "--name-only", "origin/main...HEAD"],
        ["diff", "--name-only", "HEAD"],
        ["ls-files", "--others", "--exclude-standard"],
    ) if a is not None]):
        files += [line.strip() for line in run("git", "-C", str(root), *args).splitlines() if line.strip()]
    return sorted(set(files))


def open_todos(ledger: pathlib.Path):
    """(带触点的未完成待办, 没写触点的, 明确 unknown 不排的)——与推之前那道门同一段代码。"""
    items, without, deferred = todo_source.partition(f"md:{ledger}")
    return [(i.id, i.touches) for i in items], without, deferred


def show(root: pathlib.Path, path: str) -> str:
    file = root / path
    return file.read_text(errors="ignore") if file.exists() else ""


def cap_markers(text: str) -> int:
    return len(re.findall(r"//\s*cap:\s*(?:bound|fixed)", text))


def capacity_owed(root: pathlib.Path, changed: set[str]) -> list[tuple[str, str]]:
    rows = {
        m.group(1): int(m.group(2))
        for m in re.finditer(r'\(\s*"(crates/[^"]+\.rs)"\s*,\s*(\d+)\s*\)', show(root, REGISTRY))
    }
    owed: list[tuple[str, str]] = []
    for path in sorted(changed):
        if path in rows and cap_markers(show(root, path)) != rows[path]:
            owed.append((REGISTRY, f"the row for {path} no longer counts its bounds"))
    return owed


def static_owed(root: pathlib.Path, changed: set[str]) -> list[tuple[str, str]]:
    if not any(path.startswith(CHART) for path in changed):
        return []
    script = root / "deploy/scripts/sync-static-manifests.sh"
    if not script.exists():
        return []
    if subprocess.run(["bash", str(script), "--check"], capture_output=True).returncode == 0:
        return []
    return [("deploy/k3s", "chart projection drifted: run deploy/scripts/sync-static-manifests.sh")]


def rules_owed(root: pathlib.Path, changed: set[str]) -> list[tuple[str, str]]:
    if RULE_CONFIG not in changed:
        return []
    diff = run("git", "-C", str(root), "diff", "HEAD", "--", RULE_CONFIG)
    removed = set(re.findall(r'^-\s*"name":\s*"([a-z0-9_]+)"', diff, re.M))
    added = set(re.findall(r'^\+\s*"name":\s*"([a-z0-9_]+)"', diff, re.M))
    owed: list[tuple[str, str]] = []
    for name in sorted(removed - added):
        for fixture in (root / FIXTURES).glob(f"*{name}*.test.yaml"):
            rel = str(fixture.relative_to(root))
            if rel not in changed:
                owed.append((rel, f"fixture for the removed rule {name}"))
    for name in sorted(added - removed):
        if not any(name in p for p in changed):
            owed.append((FIXTURES + "/", f"a fixture for the new rule {name}"))
    return owed


def metrics_owed(root: pathlib.Path, changed: set[str]) -> list[tuple[str, str]]:
    if METRIC_NAMES not in changed:
        return []
    diff = run("git", "-C", str(root), "diff", "HEAD", "--", METRIC_NAMES)
    removed = set(re.findall(r"^-\s*[A-Z0-9_]+\s*=>\s*\"([a-z0-9_]+)\"", diff, re.M))
    added = set(re.findall(r"^\+\s*[A-Z0-9_]+\s*=>\s*\"([a-z0-9_]+)\"", diff, re.M))
    help_text = show(root, METRIC_HELP)
    return [
        (METRIC_HELP, f"help entry for the removed metric {name}")
        for name in sorted(removed - added)
        if name in help_text
    ]


CHECKS = (capacity_owed, static_owed, rules_owed, metrics_owed)



def is_touched(path: str, touched: set[str]) -> bool:
    """这一条待办的触点，这一轮动过没有。

    触点**可以是一个目录**（「这一族要动 `crates/cog-storage/src/postgres`」）：台账里
    写成目录的那些条目，按文件逐条相等去比永远对不上，于是缺口永远为真、这一批永远
    做不完——一条不可能被满足的判据与一条永远为真的判据一样没用。目录的读法是
    「它下面的任何一个文件动了」。
    """
    p = path.rstrip("/")
    if p in touched:
        return True
    prefix = p + "/"
    return any(f.startswith(prefix) for f in touched)



def other_repo_changes(repo: pathlib.Path, base: str | None) -> list[str]:
    """另一个仓改了哪些文件：从 `base` 算起（含工作区）；没有基准就只看工作区。

    台账仓里的那一半工作（那一族自己的门、这本台账本身）由同一个人在同一轮里写，
    但它不在这个仓的 diff 里。不读那一侧，那些行永远读作「还没做」——而它们已经在
    另一个仓里做完了。
    """
    rev = base or "HEAD"
    out = subprocess.run(
        ["git", "-C", str(repo), "diff", "--name-only", rev],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        return []
    return [l.strip() for l in out.stdout.splitlines() if l.strip()]


def report(root: pathlib.Path, changed: list[str], batch: list[str], todos, missing_todos,
           deferred=(), foreign=(), other: pathlib.Path | None = None, debt=(),
           editing: bool = False) -> int:
    touched = set(changed)
    # 触点可以落在台账所在的那个仓里（这一族的产物本来就写在那儿）：那一侧的改动面
    # 由 `foreign` 带进来。
    every = touched | set(foreign)

    # **判「做完没有」只认三格，不认触点**（属主 2026-11-21：「不能以触点发现有改动
    # 就算，而是要改完算」）。触点回答的是「这一轮该改哪些文件」，一个文件改过不等于
    # 那条待办做完了——改了一半、改错了地方、或者只是碰巧落在同一条路径上，三种都在
    # 触点判据下读成「做了」。三格回答的是另一件事：树里有没有实现（`代码`）、跑着的
    # 那一版有没有它（`上线`）、判据量过没有（`验证`）。
    #
    # 这道门（推之前 / 编译前）判的是 `代码` 那一格：这一推要带走的实现是不是都在树里。
    # `上线` 与 `验证` 只能在推之后、在现网量，它们由 `round.py terminal` 判，判词是
    # 「三格齐」——那时的「已落」才是终态。
    gaps = [(name, miss) for name, _paths, miss in todos if "代码" in miss]
    # `--editing` 问的是另一个问题：**这一遍编辑走完了没有**（触点到的地方都动过）。
    # 编译前的垫片问的是这个——它要在「改完一遍再编译一次」这件事上把住门，而不是
    # 在「做完没有」上（那是推之前与终态门的事）。两个问题分开判，因为答案是两件事：
    # 一条待办的触点全都动过，也可能还没做完。
    editing_gaps = [
        (name, [p for p in paths if not is_touched(p, every)])
        for name, paths, _miss in todos
        if paths and not any(is_touched(p, every) for p in paths)
    ]

    owed: list[tuple[str, str]] = []
    for check in CHECKS:
        owed += check(root, touched)

    complete = sum(1 for _, _p, miss in todos if not miss)
    if debt:
        # 这一面不进批次：它是整本台账欠的终态，不是这一轮要改的文件。
        print(
            f"family: 台账上另有 {len(debt)} 行写着 `已落` 却没有三格（欠终态；"
            f"`--terminal` 判它）：{', '.join(debt)}"
        )
    print(
        f"family: 工作区改了 {len(changed)} 个文件；本轮默认批次 {len(batch)} 个；"
        f"未完成待办 {len(todos)} 项（三格齐 {complete}，缺格 {len(gaps)}）"
    )
    for name, paths, miss in todos:
        state = "三格齐" if not miss else "缺 " + "、".join(miss)
        print(f"  待办 {name}: {len(paths)} 个触点；{state}")
    if missing_todos:
        print(f"  ⚠ 这些待办没写 `touches:`，批次算不出它们要动什么：{', '.join(missing_todos)}")
    if editing:
        for name, missing_paths in editing_gaps:
            print(
                f"  TODO 待办 {name} <- 这一遍编辑还没动到它（触点 {', '.join(missing_paths[:3])}"
                f"{'…' if len(missing_paths) > 3 else ''}）"
            )
    else:
        for name, miss in gaps:
            print(
                f"  TODO 待办 {name} <- 缺 `代码` 格：这一条还没有「树里有了实现」的证据"
                f"（缺 {'、'.join(miss)}）"
            )

    # 触点的读数照旧印出来：它不再是判据，但「这一轮该动哪儿」仍然要靠它看。
    if gaps:
        for name, paths, miss in todos:
            if "代码" not in miss:
                continue
            for path in paths:
                if is_touched(path, every):
                    continue
                where = "这个树里" if (root / path).exists() else (
                    "台账仓里" if other is not None and (other / path).exists()
                    else "（两棵树里都没有这条路）"
                )
                print(f"    待办 {name} 的触点 {path} 还没动（{where}）")
    for path, why in owed:
        print(f"  TODO {path}  <- {why}")

    # 没有触点的待办让「默认批次」算不出来。它不是一条警告：一条算不出来的待办
    # 可以悄悄漏过去，而漏过去的正是「这一轮没做的那些」。
    if missing_todos:
        print(
            f"\n这些未完成待办没有 `touches:`，本轮批次算不出来（{len(missing_todos)} 项）："
            f"{', '.join(missing_todos)}\n"
            "每条待办在自己的行里写 `touches: <路径>[, <路径>…]`，路径从树里推导（引用它的"
            "装配点、测试、注册表、chart、夹具），推不出来就写 `touches: unknown` 并挡住批次。"
        )
        return 1

    # `touches: unknown` 的待办不在批次里，但它必须被 `Split:` 逐条点名——所以它也
    # 让这一批「没清空」。两个门（编译前、推之前）因此数出同一个数。
    if deferred:
        print(
            f"\n有 {len(deferred)} 条待办写了 `touches: unknown`（本轮不排）："
            f"{', '.join(deferred)}\n"
            "推之前要在提交信息里用 `Split: <1-4> — <S 号…> — <理由>` 逐条点名，否则推不出去。"
        )
        return 1

    if (editing_gaps if editing else gaps) or owed:
        print(
            "\n默认是**合并**：把这一轮所有待办一次改完，再编译一次。要拆，必须点出四条之一：\n"
            "  1 验证面不同（CI/现场/另一台机器）  2 失败不可归因（同一批测试里红会互相掩盖）\n"
            "  3 不可逆或代价不对称（要滚动/迁移/动现场）  4 一次读不完（沿家族边界拆，每份自身闭合）\n"
            "写法：提交信息里一行  Split: <1|2|3|4> — <拆分边界>\n"
            "确实不需要改的单个文件：  Skip: <path> — <为什么这一族不动它>"
        )
        return 1
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--ledger", default=None)
    parser.add_argument("--changed", nargs="*", default=None)
    parser.add_argument("--from-git", action="store_true")
    parser.add_argument("--all-todos", action="store_true")
    parser.add_argument("--editing", action="store_true",
                        help="判「这一遍编辑走完了没有」（触点都动过）。编译前的垫片问的是这个")
    parser.add_argument("--terminal", action="store_true",
                        help="判整本台账的欠终态：已落却缺格的行，一行都不许有")
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()
    if not (root / "crates").exists():
        print(f"family: {root} 不像这个仓库", file=sys.stderr)
        return 2

    # ── 台账在一轮开着的时候不许动 ────────────────────────────────────────────
    #
    # 这一轮钉住的是台账的**某一版**；批中途改台账，会让「这一批是哪几条」与
    # 「自开工以来改了什么」两件事同时失效——而现场唯一的做法是重钉，重钉又把
    # 改动基准重置（2026-11-21 实测：重钉一次，33 条里 32 条从「做过」变回「没做」）。
    #
    # 所以这条是硬的：**台账只能在一批推上去之后改**。要改台账，先 `round.py close`
    # 并推送这一批；要改的是「这一批做哪几条」，那是 `rebound` 的事，而它不该由
    # 中途改台账来触发。
    if args.all_todos:
        try:
            import round as round_mod
            cur = round_mod.current_round()
            if cur and cur[1]:
                spec = todo_source.default_spec()
                repo = round_mod.ledger_repo(spec)
                if repo is not None:
                    ledger_rel = str(pathlib.Path(spec[3:]).resolve().relative_to(repo))
                    head = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                          capture_output=True, text=True).stdout.strip()
                    if head and head != cur[1]:
                        diff = subprocess.run(
                            ["git", "-C", str(repo), "diff", "--name-only", cur[1], "HEAD", "--", ledger_rel],
                            capture_output=True, text=True).stdout.strip()
                        if diff:
                            print(
                                "family: 这一轮开着的时候台账被改过（" + ledger_rel + "）。\n"
                                "  台账只能在一批**推上去之后**改：先把这一批编译、测试、闸门、推送，"
                                "再改台账、再 `round.py open` 新一轮。\n"
                                "  批中途改台账 ⇒ 必须重钉 ⇒ 改动基准被重置 ⇒ 做过的活读回没做。",
                                file=sys.stderr,
                            )
                            return 2
        except Exception:
            pass

    changed = args.changed or working_tree_changes(root)
    foreign: list[str] = []
    todos: list[tuple[str, list[str], list[str]]] = []
    missing: list[str] = []
    deferred: list[str] = []
    batch = list(changed)
    pinned_rev = None
    if args.all_todos:
        # 有开着的一轮：批次＝那一轮钉住的 rev 上的台账，不是此刻的台账。
        try:
            import round as round_mod
            current = round_mod.current_round()
            if current:
                # 台账仓的改动基准是**第一次开工**那个 rev，不是重钉后的那一格。
                pinned_rev = current[4] if len(current) > 4 else current[1]
                pinned = round_mod.ledger_at(current[1], args.ledger or todo_source.default_spec())
                args.ledger = str(pinned)
        except Exception:  # 读不到事件流就退回此刻的台账，并在下面说清楚
            pass
        ledger_path = resolve_ledger(args.ledger)
        # 读的是哪一本、由谁读：两个 worker 在同一台机器上各有一本，读数不写身份，
        # 出问题时就分不清「这一本是空的」还是「读错了别人的那一本」。
        print(f"family: 台账 {ledger_path}（worker {todo_source.worker_name()}）")
        if not ledger_path.exists():
            # 「没有台账」与「台账里没有待办」不是同一件事。前者是这台 worker 本来就不
            # 记待办（批次只按工作区算），后者是「全做完了」。说清楚是哪一种，别让
            # 沉默替它们回答。
            print(
                f"family: 没有配置 worker 台账（{ledger_path} 不存在）——"
                "本轮批次只按工作区改动算，不假装台账是空的"
            )
            todos, missing, deferred = [], [], []
        else:
            todos_items, missing, deferred = todo_source.partition(f"md:{ledger_path}")
        # 只判**这一轮钉住的那一批**：推之前那道门就是这么筛的（`round_ids`），
        # 这里不筛的话，同一批会被数成两个数——2026-11-20 实测：推前门报 4 条、
        # 编译前的门报 10 条，差的正是「没被选中本轮批次」的那几条。
        round_ids: set[str] = set()
        try:
            import round as round_mod
            current = round_mod.current_round()
            if current and current[3]:
                round_ids = {str(i) for i in current[3]}
        except Exception:
            round_ids = set()
        todos_items = [i for i in todos_items if not round_ids or i.id in round_ids]
        todos = [(i.id, i.touches, i.missing_cells()) for i in todos_items]
        # 触点必须指得到东西：写错的路径与「凭记忆写的路径」在批次里同形，而后者是
        # 这一整套最初要治的病。指不到就挡住批次，让写的人去核对。
        bad: list[str] = []
        checked: list[tuple[str, list[str], list[str]]] = []
        # 触点可以落在两棵树上：这棵代码树，或台账所在的那个仓（`specs/`、`scripts/` 等）。
        # 判存在要两棵都问，否则台账仓里的路径会被读成「写错了」。
        ledger_repo = None
        try:
            import round as round_mod
            # 台账仓要从**真正的台账路径**算，不能用钉住的临时副本（它在 /tmp，不是任何仓）。
            ledger_repo = round_mod.ledger_repo(todo_source.default_spec())
        except Exception:
            ledger_repo = None
        for name, paths, miss in todos:
            gone = [
                p for p in paths
                if not (root / p).exists()
                and not (ledger_repo is not None and (ledger_repo / p).exists())
            ]
            if gone:
                bad.append(f"{name}: {', '.join(gone)}")
                continue
            checked.append((name, paths, miss))
        if bad:
            print("family: 这些待办的触点在树里不存在，批次算不出来：")
            for line in bad:
                print(f"  {line}")
            return 2
        todos = checked
        if deferred:
            print(f"family: 本轮不排的待办（touches: unknown）{len(deferred)} 项：{', '.join(deferred)}")
        for _name, paths, _miss in todos:
            batch += paths
        # 台账仓那一侧的改动面：从这一轮钉住的那个 rev 算起，含工作区。没有开着一轮
        # 时只看工作区——那正是「这一轮还没开始记」的形状，不能替它编一段历史。
        if ledger_repo is not None:
            foreign = other_repo_changes(ledger_repo, pinned_rev)
    batch = sorted(set(batch))

    if not changed and not todos:
        return 0
    debt: list[str] = []
    try:
        debt = [i.id for i in todo_source.terminal_debt(f"md:{resolve_ledger(args.ledger)}")]
    except Exception:
        debt = []
    if args.terminal:
        # 「欠终态」是它自己的判据：整本台账上再没有一行写着 `已落` 却缺格。
        # 与批次门分开跑，因为它判的是另一个问题（台账的债，不是这一轮的范围）。
        if debt:
            print("FAIL: 这些行写着 `已落` 却没有三格（代码 / 上线 / 验证）：" + "、".join(debt))
            return 1
        print("terminal: 台账上没有欠终态的行。")
        return 0
    return report(root, changed, batch, todos, missing, deferred, foreign, ledger_repo, debt,
                  editing=args.editing)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
