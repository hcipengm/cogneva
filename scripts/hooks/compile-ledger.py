#!/usr/bin/env python3
"""The compile ledger: what a push has to be able to say about its own verification.

One line per cargo invocation, written by the shim installed by
`scripts/hooks/install.sh`:

    <UTC time>  <worktree>  <arguments>  <HEAD>  <tree fingerprint>

`check` answers the two questions a push is judged on:

  1. Was the tree compiled *after* its last edit? (newest modified file vs the
     newest compile recorded for this worktree)
  2. Did this batch compile more than once, and if so does a commit in the batch
     carry a `Recompiled: <why>` line?

Exit codes: 0 fine; 1 refused; 2 the ledger cannot be read (nothing judged).
"""

from __future__ import annotations

import argparse
import datetime as dt
import pathlib
import subprocess
import sys

COUNTS = {"check", "build", "test", "clippy", "run", "bench", "doc", "nextest", "install", "fix"}
SKIPS = {"fmt", "metadata", "tree", "locate-project", "pkgid", "vendor", "search", "login", "publish"}


def parse_time(text: str) -> dt.datetime | None:
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def subcommand(argv: str) -> str | None:
    for token in argv.split():
        if not token.startswith("-"):
            return token
    return None


def is_compile(argv: str) -> bool:
    sub = subcommand(argv)
    if sub in SKIPS:
        return False
    return sub in COUNTS


def ledgers(ledger: pathlib.Path, worktree: pathlib.Path) -> list[dt.datetime]:
    times: list[dt.datetime] = []
    for line in ledger.read_text(errors="ignore").splitlines():
        parts = line.split("\t")
        if len(parts) < 3 or not is_compile(parts[2]):
            continue
        try:
            where = pathlib.Path(parts[1]).resolve()
        except OSError:
            continue
        if where != worktree:
            continue
        when = parse_time(parts[0])
        if when:
            times.append(when)
    return sorted(times)


# 只有 Rust 源需要「改完再编译」。脚本（.sh/.py）与清单由它们自己的门判，拿它们去
# 要求一次 cargo 编译，等于让改一行注释也付一次编译成本。
CODE_SUFFIXES = {".rs"}


def is_code(path: pathlib.Path) -> bool:
    return path.suffix in CODE_SUFFIXES


def changed_since(root: pathlib.Path, base: str) -> list[pathlib.Path]:
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True,
        text=True,
    ).stdout
    files = [root / line for line in out.splitlines() if line.strip()]
    files = [p for p in files if is_code(p)]
    head = subprocess.run(
        ["git", "-C", str(root), "log", "--format=%H", f"{base}..HEAD"],
        capture_output=True,
        text=True,
    ).stdout.split()
    return files, head


def check(root: pathlib.Path, base: str, ledger: pathlib.Path) -> int:
    times = ledgers(ledger, root)
    if not times:
        print("compile-discipline: 这棵树上没有编译记录，这一推不判。")
        return 0
    files, commits = changed_since(root, base)
    if not files:
        print("compile-discipline: 与基线相比没有改动，这一推不判。")
        return 0

    newest = max((p.stat().st_mtime for p in files if p.exists()), default=0.0)
    last = times[-1].timestamp()
    newest_dt = dt.datetime.fromtimestamp(newest, tz=dt.timezone.utc)
    if newest > last:
        print(
            "FAIL: 这一推涉及的文件在最后一次编译之后又改过。\n"
            f"      最新的修改：{newest_dt.isoformat()}；最后一次编译：{times[-1].isoformat()}\n"
            "      先把这一批编辑全部做完，再编译一次；这就是「默认全量编辑」的判据。",
            file=sys.stderr,
        )
        return 1

    first = dt.datetime.min.replace(tzinfo=dt.timezone.utc)
    if commits:
        stamp = subprocess.run(
            ["git", "-C", str(root), "log", "--format=%ct", f"{base}..HEAD"],
            capture_output=True,
            text=True,
        ).stdout.split()
        if stamp:
            # 这一批的边界是**最近一笔提交**，不是最早那一笔：一堆提交常常来自几轮
            # 各自独立的编辑，把它们的编译全算进「这一批」，会把「最后一版编译了几次」
            # 问成一个几千分钟的历史问题。判的是最后一次提交之后、这一推之前的那一轮。
            first = dt.datetime.fromtimestamp(max(int(s) for s in stamp), tz=dt.timezone.utc)
    in_batch = [t for t in times if t >= first]
    if len(in_batch) > 1:
        messages = subprocess.run(
            ["git", "-C", str(root), "log", "--format=%B", f"{base}..HEAD"],
            capture_output=True,
            text=True,
        ).stdout
        if "Recompiled:" not in messages:
            print(
                f"FAIL: 这一批编译了 {len(in_batch)} 次，提交信息里没有 `Recompiled: <为什么第一次不够>`。\n"
                "      一批一次编译是默认；多出来的每一次都要点名理由（第一版红了要修，是合法理由）。",
                file=sys.stderr,
            )
            return 1

    print(f"compile-discipline: 一批 {len(in_batch)} 次编译，最后一次在全部编辑之后。")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["check"])
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--ledger", default=str(pathlib.Path.home() / ".local/state/cogneva/compile-ledger.tsv"))
    args = parser.parse_args(argv[1:])
    return check(pathlib.Path(args.root).resolve(), args.base, pathlib.Path(args.ledger))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
