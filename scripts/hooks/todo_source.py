#!/usr/bin/env python3
"""待办的来源：一个形状，几种台账。

「默认全量」的规则与台账长什么样无关，只与三件事有关：

  1. **哪些条目还没做完**（未完成集合）；
  2. **每条要动哪些文件**（触点）；
  3. **谁在跑**（决定读哪一本台账）。

所以这里只定义一个接口：来源 → `[Item(id, title, touches)]`。今天的两种实现：

  - `md:<路径>`            worker 台账（`| S 号 | … | 状态 | … | touches: <路径…>`），
                           带外 Claude Code 用它，每个 worker 一份；
  - `change-ledger`        平台的变更账本（`change_ledger` 里 state=pending 的行），
                           cogneva 自己的进化用它。它要一条宿主配置的查询命令
                           （`COGNEVA_CHANGE_LEDGER_CMD`），因为库在集群里。

新的台账只要实现同一个形状，不需要改判据——这是这一层存在的全部理由。
"""

from __future__ import annotations

import os
import pathlib
import re
import subprocess

OPEN_WORDS = ("未落", "部分", "进行中", "待落", "待裁", "未开始")


class Item:
    __slots__ = ("id", "title", "touches")

    def __init__(self, id: str, title: str, touches: list[str]):
        self.id = id
        self.title = title
        self.touches = touches

    def __repr__(self) -> str:  # pragma: no cover - 只为读数好看
        return f"Item({self.id!r}, {len(self.touches)} touches)"


class Unreadable(Exception):
    """台账读不出来。它不是「没有待办」，调用方必须把它和空集合分开。"""


def _md_items(path: pathlib.Path) -> list[Item]:
    if not path.exists():
        raise Unreadable(f"{path} 不存在")
    out: list[Item] = []
    for line in path.read_text(errors="ignore").splitlines():
        if not line.startswith("| S"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3 or not any(w in cells[2] for w in OPEN_WORDS):
            continue
        found = re.search(r"[｜|]\s*touches:\s*([^|｜\n]+)", line)
        touches: list[str] = []
        if found:
            raw = found.group(1).strip()
            if not raw.startswith("unknown"):
                touches = [p.strip() for p in re.split(r"[,，]", raw) if p.strip()]
        out.append(Item(cells[0], cells[1][:80], touches))
    return out


def _change_ledger_items() -> list[Item]:
    command = os.environ.get("COGNEVA_CHANGE_LEDGER_CMD")
    if not command:
        raise Unreadable(
            "没有配置变更账本的查询命令（COGNEVA_CHANGE_LEDGER_CMD）："
            "它要打印 `<id>\\t<scope>\\t<title>`，每行一条未完成变更"
        )
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Unreadable(f"变更账本查询失败（退出码 {result.returncode}）：{result.stderr.strip()[:200]}")
    out: list[Item] = []
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        ident, scope = parts[0].strip(), parts[1].strip()
        title = parts[2].strip() if len(parts) > 2 else ""
        # `scope` 是一个路径前缀（例如 `crates/cog-storage` 或一个具体文件）；
        # 不是路径的（`crates` 这种整棵树的）就不当触点——它指不到具体文件，
        # 这种条目只能靠 `Split:` 点名，或者先被拆细。
        touches = [scope] if "/" in scope or scope.endswith(".rs") else []
        out.append(Item(ident, title, touches))
    return out


def load(spec: str) -> list[Item]:
    """`md:<path>` 或 `change-ledger`。读不到抛 [`Unreadable`]。"""
    if spec.startswith("md:"):
        return _md_items(pathlib.Path(spec[3:]).expanduser())
    if spec == "change-ledger":
        return _change_ledger_items()
    raise Unreadable(f"不认识的台账来源：{spec}（只认 md:<路径> 与 change-ledger）")


def default_spec() -> str:
    """这台机器上，这个 worker 的台账是哪一本。"""
    explicit = os.environ.get("COGNEVA_TODO_SOURCE")
    if explicit:
        return explicit
    env = os.environ.get("COGNEVA_TODO_LEDGER")
    if env:
        return f"md:{env}"
    host = pathlib.Path.home() / ".config/cogneva/todo-ledger"
    if host.exists():
        first = host.read_text(errors="ignore").strip().splitlines()
        if first and first[0].strip():
            return f"md:{first[0].strip()}"
    return f"md:{pathlib.Path.home() / '.local/state/cogneva/todo-ledger.md'}"
