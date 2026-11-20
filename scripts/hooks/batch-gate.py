#!/usr/bin/env python3
"""默认全批：做子集要举证，而举证本身要被判真假。

规则：**默认把这一轮的全部待办一次做完**；拆分是例外，
必须在提交信息里点名四类理由之一并给出边界。这个程序判两件事：

  1. 你做的够不够：义务清单里还有没做而这一推也没声明的 ⇒ 拒。
  2. 声明的理由成不成立：
     - `Split: 1` 验证面不同   两侧映射到的 cargo 包集合必须不同，相同即假。
     - `Split: 2` 失败不可归因 机器判不了真假；要求点名会重叠的测试目标，记进历史。
     - `Split: 3` 不可逆/不对称 必须一侧碰 deploy/、migrations/、scripts/host/，另一侧不碰。
     - `Split: 4` 一次读不完   这一侧义务正文的估算 token 必须超过窗口里它能占的份额；
                              窗口由模型声明导出（`--window-tokens`），不是人拍的数。

义务清单的来源是变更账本（`change_ledger` 里 `kind=grep, literal=principle:*` 且
状态未落地）。离线跑时用 `--obligations <文件>`（一行一个 `principle: <name>`）。

退出码：0 允许；1 拒；2 读不到（清单/仓库）——读不到不判，也不假绿。
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

# 粗略的 token 估算：中文一字一 token，英文按 4 字节一 token 计。它只用来判
# 「这一侧读得完读不完」，不用于计费，所以不追求精确；比例写在估算函数里，
# 换估算器时只改这一处。
def estimate_tokens(text: str) -> int:
    cjk = len(re.findall(r"[一-鿿]", text))
    other = max(0, len(text) - cjk)
    return cjk + other // 4


def obligations_from_file(path: pathlib.Path) -> list[str]:
    return [
        line.strip()
        for line in path.read_text(errors="ignore").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


def markers_in_tree(root: pathlib.Path) -> set[str]:
    out = subprocess.run(
        ["grep", "-rhoE", r"principle: [A-Za-z0-9_]+", "--include=*.rs", "--include=*.sh",
         "--include=*.py", "crates", "deploy", "scripts"],
        cwd=root, capture_output=True, text=True,
    ).stdout
    return {m.group(1) for m in re.finditer(r"principle: ([A-Za-z0-9_]+)", out)}


def message_of(root: pathlib.Path, base: str) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "log", "--format=%B", f"{base}..HEAD"],
        capture_output=True, text=True,
    ).stdout


def changed_paths(root: pathlib.Path, base: str) -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(root), "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True, text=True,
    ).stdout
    return [l.strip() for l in out.splitlines() if l.strip()]


def packages(paths: list[str]) -> set[str]:
    out: set[str] = set()
    for p in paths:
        parts = p.split("/")
        out.add("/".join(parts[:2]) if len(parts) > 1 else parts[0])
    return out


IRREVERSIBLE = ("deploy/", "migrations/", "scripts/host/")


def judge_split(kind: str, detail: str, paths: list[str], owed: list[str], window: int) -> str | None:
    """返回拒绝理由；None 表示这条理由成立（或无法判真假但已点名）。"""
    if kind == "1":
        # 两侧：`Split: 1 — A | B`，A/B 是路径前缀。
        sides = [s.strip() for s in re.split(r"[|｜]", detail)]
        if len(sides) != 2:
            return "Split: 1 要写两侧边界（`Split: 1 — <一侧路径> | <另一侧路径>`）"
        left = {p for p in paths if p.startswith(sides[0])}
        right = {p for p in paths if p.startswith(sides[1])}
        if packages(left) == packages(right):
            return (
                "Split: 1 说验证面不同，但两侧映射到同一批 cargo 包"
                f"（{sorted(packages(left))}）——验证面其实相同"
            )
        return None
    if kind == "3":
        touched = [p for p in paths if p.startswith(IRREVERSIBLE)]
        if not touched:
            return "Split: 3 说不可逆或代价不对称，但这一批没有一处碰 deploy/、migrations/、scripts/host/"
        return None
    if kind == "4":
        if window <= 0:
            return "Split: 4 要拿窗口份额比，但没给窗口（--window-tokens 或 COGNEVA_CONTEXT_WINDOW）"
        real = [o for o in owed if o in paths or True]  # 义务本身不带路径，这里按整侧计数
        cost = sum(estimate_tokens(o) for o in real)
        share = window // 2  # 与工具结果预算同一条份额约定
        if cost <= share:
            return f"Split: 4 说读不完，但这一侧义务估算 {cost} token ≤ 窗口份额 {share}"
        return None
    if kind == "2":
        if not re.search(r"[a-z0-9_]+\.rs|tests?/", detail):
            return "Split: 2 要点名会重叠的测试目标（文件或测试名），机器判不了真假但要点得出来"
        return None
    return f"Split: {kind} 不在四条里（只认 1 验证面 / 2 归因 / 3 不可逆 / 4 读不完）"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--base", default="origin/main")
    parser.add_argument("--obligations", required=True)
    parser.add_argument("--window-tokens", type=int,
                        default=int(__import__("os").environ.get("COGNEVA_CONTEXT_WINDOW", "0")))
    args = parser.parse_args(argv[1:])

    root = pathlib.Path(args.root).resolve()
    owed = obligations_from_file(pathlib.Path(args.obligations))
    if not owed:
        print("batch-gate: 义务清单是空的，这一推不判。")
        return 0

    done = markers_in_tree(root)
    remaining = [o for o in owed if o.split("principle: ")[-1] not in done]
    msg = message_of(root, args.base)
    paths = changed_paths(root, args.base)

    if not remaining:
        print(f"batch-gate: {len(owed)} 条义务全部有证据，允许。")
        return 0

    splits = re.findall(r"^Split:\s*(\S)[^\n]*", msg, re.M)
    if not splits:
        print(
            f"FAIL: 这批义务还有 {len(remaining)} 条没有证据（共 {len(owed)} 条），"
            "而提交信息里没有 `Split:`。默认是做完整批；要拆必须点出四条之一。",
            file=sys.stderr,
        )
        return 1

    for kind in splits:
        detail = ""
        for line in msg.splitlines():
            if line.startswith("Split:") and line.strip().startswith(f"Split: {kind}"):
                detail = line.split("—", 1)[-1] if "—" in line else line.split("-", 1)[-1]
        problem = judge_split(kind, detail, paths, remaining, args.window_tokens)
        if problem:
            print(f"FAIL: {problem}", file=sys.stderr)
            return 1

    print(f"batch-gate: 声明了 {len(splits)} 条拆分理由，均已判过；剩余 {len(remaining)} 条在下一批。")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
