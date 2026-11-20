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

DEFAULT_LEDGER = pathlib.Path.home() / ".local/state/cogneva/todo-ledger.md"
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
    for args in (
        ["diff", "--name-only", "origin/main...HEAD"],
        ["diff", "--name-only", "HEAD"],
        ["ls-files", "--others", "--exclude-standard"],
    ):
        files += [line.strip() for line in run("git", "-C", str(root), *args).splitlines() if line.strip()]
    return sorted(set(files))


def open_todos(ledger: pathlib.Path) -> tuple[list[tuple[str, list[str]]], list[str]]:
    """(带触点的未完成待办, 没有触点的未完成待办名)。"""
    with_touch: list[tuple[str, list[str]]] = []
    without: list[str] = []
    defer: list[str] = []
    rows: list[tuple[str, list[str], str, str]] = []  # (名字, 触点, 状态, 整行)
    if not ledger.exists():
        return with_touch, without, defer
    for line in ledger.read_text(errors="ignore").splitlines():
        if not line.startswith("| S"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        name = cells[0]
        # 状态在第三格（`| 项 | 内容 | 状态 | 判据 |`），但有的行把状态写进内容那一格；
        # 判据那一格经常引用「未落」这样的词，所以不读它——否则一条已落的行会被当成待办。
        status_cells = cells[2:3]
        if not any(word in cell for cell in status_cells for word in OPEN_WORDS):
            continue
        # 只认单元格里的那一段：行文里引用 `touches:` 这个写法的地方不算声明。
        found = re.search(r"[｜|]\s*touches:\s*([^|｜\n]+)", line)
        if not found:
            without.append(name)
            continue
        raw = found.group(1).strip()
        # `unknown` 只允许带理由，且它意味着「本轮明确不排」——不排要在推的时候逐条
        # 点名（`Split: <1-4> — <S 号…> — 理由`），所以它不是走后门，是把话说在前面。
        if raw.startswith("unknown"):
            if "—" not in raw and "-" not in raw:
                without.append(f"{name}（写了 unknown 但没说为什么）")
                continue
            defer.append(name)
            continue
        paths = [p.strip() for p in re.split(r"[,，]", raw) if p.strip()]
        # 每个触点都得长得像一条路径：带反引号、空格、或别的散文，说明这不是一份触点清单。
        looks_like_path = re.compile(r"^[\w./-]+$")
        if not paths or any(not looks_like_path.match(p) for p in paths):
            without.append(name)
            continue
        with_touch.append((name, paths))
    # 「没写触点」只对本轮的待办成立：积压里的 `unknown` 是「本轮不排」，不是缺陷。
    without = [w for w in without if "round: current" in next(
        (l for l in ledger.read_text(errors="ignore").splitlines() if l.startswith(f"| {w.split('（')[0]} ")), "round: current")]
    # 本轮：行里标 `round: current` 的算这一轮；一行都没标时全部未完成待办都算本轮。
    rows = {name for name, _ in with_touch}
    if rows:
        all_lines = ledger.read_text(errors="ignore")
        if "round: current" in all_lines:
            current: list[tuple[str, list[str]]] = []
            for name, paths in with_touch:
                line = next((l for l in all_lines.splitlines() if l.startswith(f"| {name} ")), "")
                if "round: current" in line:
                    current.append((name, paths))
            backlog = len(with_touch) - len(current)
            with_touch = current
            if backlog:
                print(f"family: 积压 {backlog} 条未完成待办不在本轮（台账里标 `round: current` 的才进批次）")
    return with_touch, without, defer


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


def report(root: pathlib.Path, changed: list[str], batch: list[str], todos, missing_todos) -> int:
    touched = set(changed)
    gaps = [p for p in batch if p not in touched and not (root / p).exists() is False and p not in touched]
    gaps = [p for p in batch if p not in touched]

    owed: list[tuple[str, str]] = []
    for check in CHECKS:
        owed += check(root, touched)

    print(f"family: 工作区改了 {len(changed)} 个文件；本轮默认批次 {len(batch)} 个；未完成待办 {len(todos)} 项")
    for name, paths in todos:
        print(f"  待办 {name}: {len(paths)} 个触点")
    if missing_todos:
        print(f"  ⚠ 这些待办没写 `touches:`，批次算不出它们要动什么：{', '.join(missing_todos)}")
    for path in gaps:
        print(f"  TODO {path}  <- 这一轮的待办要点名它，但它还没改")
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

    if gaps or owed:
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
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()
    if not (root / "crates").exists():
        print(f"family: {root} 不像这个仓库", file=sys.stderr)
        return 2

    changed = args.changed or working_tree_changes(root)
    todos: list[tuple[str, list[str]]] = []
    missing: list[str] = []
    deferred: list[str] = []
    batch = list(changed)
    if args.all_todos:
        ledger_path = resolve_ledger(args.ledger)
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
            todos, missing, deferred = open_todos(ledger_path)
        # 触点必须指得到东西：写错的路径与「凭记忆写的路径」在批次里同形，而后者是
        # 这一整套最初要治的病。指不到就挡住批次，让写的人去核对。
        bad: list[str] = []
        checked: list[tuple[str, list[str]]] = []
        for name, paths in todos:
            gone = [p for p in paths if not (root / p).exists()]
            if gone:
                bad.append(f"{name}: {', '.join(gone)}")
                continue
            checked.append((name, paths))
        if bad:
            print("family: 这些待办的触点在树里不存在，批次算不出来：")
            for line in bad:
                print(f"  {line}")
            return 2
        todos = checked
        if deferred:
            print(f"family: 本轮不排的待办（touches: unknown）{len(deferred)} 项：{', '.join(deferred)}")
        for _, paths in todos:
            batch += paths
    batch = sorted(set(batch))

    if not changed and not todos:
        return 0
    return report(root, changed, batch, todos, missing)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
