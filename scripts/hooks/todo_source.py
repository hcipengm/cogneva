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

# 状态词是**闭集**，而且只有两个：未落 / 已落。没有第三个词——每多一个词就多一种把行挪出
# 批次的手法。一条待办被写下来的时候就已经判过「它值得做」，所以不存在「退役」这条路：
# 真发现它不对，改的是这条待办自己，不是给它换一个不进批次的词。
# 从前这里是一片近义词（部分、进行中、待落、未开始、program、round: current……），每多一个
# 词就多一种把行挪出批次的手法——子集就是这样一次次被「合规地」造出来的。闭集之后，
# 想让一行不进批次只有一条路：把它改成已落（或被点名退役），而那要写清理由。
OPEN_STATUS = "未落"
CLOSED_PREFIXES = ("已落",)


class Item:
    __slots__ = ("id", "title", "text", "touches", "deferred")

    def __init__(self, id: str, title: str, text: str, touches: list[str], deferred: bool = False):
        self.id = id
        self.title = title
        # 整行原文（不截断）。`title` 是给人看的前 80 字，用来估「这一条读起来要多少
        # token」会把最长的几条估成同一个数——一条待办的正文都在标题之外。
        self.text = text
        self.touches = touches
        # `touches: unknown — 理由`：明确本轮不排。它不是「忘了写触点」——
        # 两者的修法不同（前者要 Split 点名，后者要先补触点），所以这里分开。
        self.deferred = deferred

    def __repr__(self) -> str:  # pragma: no cover - 只为读数好看
        return f"Item({self.id!r}, {len(self.touches)} touches)"


class Unreadable(Exception):
    """台账读不出来。它不是「没有待办」，调用方必须把它和空集合分开。"""


def _md_items(path: pathlib.Path) -> list[Item]:
    if not path.exists():
        raise Unreadable(f"{path} 不存在")
    out: list[Item] = []
    bad_status: list[str] = []
    malformed: list[str] = []
    for line in path.read_text(errors="ignore").splitlines():
        if not line.startswith("| S"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            # 一行以 `| S` 开头却没有状态列，**不能当它不在**：静默跳过会让一条待办
            # 整族地从批次里消失，而读数照样绿。这与「状态词不在闭集里」是同一类
            # 毛病——读不到不是没有——所以同样点名拒掉。
            malformed.append(f"{cells[0] if cells else line[:20]}（只有 {len(cells)} 列，没有状态列）")
            continue
        status = cells[2]
        if status.startswith(CLOSED_PREFIXES):
            continue
        if not status.startswith(OPEN_STATUS):
            bad_status.append(f"{cells[0]}（状态：{status[:20]}）")
            continue
        # **取最后一个** `touches:`：正文里提到这个词（例如描述这条规则本身）不该
        # 把后面的散文当成路径表。触点永远写在行尾那一格。
        found = None
        for found in re.finditer(r"[｜|]\s*touches:\s*([^|｜\n]+)", line):
            pass
        touches: list[str] = []
        # `touches: unknown — 理由` 是「这一条不进批次」的正式写法，但**理由里不能有逗号**：
        # 下面先按逗号切触点，切完第一段就不是 `unknown` 了，整句又被当成一个路径，于是
        # 每一道读台账的门都报「这个路径不在树里」，而且报的是理由那半句——2026-11-20 实测，
        # 一条我刚写下的待办把整批卡死了两次。逗号在这里是分隔符，不是标点。
        #
        # 没有 `unknown` 这个后门：一行待办要么点得出它要动的文件，要么批次算不出来。
        # `touches: unknown — 理由`：明确本轮不排。这一支此前**只写在注释里、没有实现**——
        # 解析器只是把恰好等于 `unknown` 的那一段丢掉，于是那一行变成「没有触点」，
        # 每一道读台账的门都报「算不出批次」。2026-11-20 实测：一条待办因此卡了整批三次。
        deferred = bool(found) and found.group(1).strip().startswith("unknown")
        if found:
            raw = found.group(1).strip()
            # 路径后面常跟着一句注解（`… .py（以及普查文档）`）：注解不是路径的一部分。
            touches = [
                re.split(r"[（(]", p.strip())[0].strip()
                for p in re.split(r"[,，]", raw)
                if p.strip() and p.strip() != "unknown"
            ]
            touches = [p.strip(" ）)") for p in touches if p.strip(" ）)")]
        out.append(Item(cells[0], cells[1][:80], line.strip(), touches, deferred))
    if malformed:
        raise Unreadable(
            "这些行以 `| S` 开头却没有状态列，读不出它做没做：" + "、".join(malformed)
        )
    if bad_status:
        raise Unreadable(
            "状态词不在闭集里（**只有两个词**：`未落` / `已落`）：" + "、".join(bad_status)
        )
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
    bad_state: list[str] = []
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        # 与 worker 台账同一个闭集，而且**就是这两词**：`pending`（未落）/ `landed`（已落）。
        # 迁移 121 把 retired 并进 landed、undecided 并进 pending，并在库里加了 CHECK——
        # 第三个词现在写不进去。这里只认这两个，别的一律点名拒掉。
        if len(parts) > 3:
            state = parts[3].strip()
            if state not in ("pending", "landed"):
                bad_state.append(f"{parts[0]}（state={state}）")
                continue
            if state == "landed":
                continue
        ident, scope = parts[0].strip(), parts[1].strip()
        title = parts[2].strip() if len(parts) > 2 else ""
        # `scope` 是一个路径前缀（例如 `crates/cog-storage` 或一个具体文件）；
        # 不是路径的（`crates` 这种整棵树的）就不当触点——它指不到具体文件，
        # 这种条目只能靠 `Split:` 点名，或者先被拆细。
        touches = [scope] if "/" in scope or scope.endswith(".rs") else []
        out.append(Item(ident, title, title, touches))
    if bad_state:
        raise Unreadable(
            "变更账本里有不在闭集里的状态（只认 pending / landed）：" + "、".join(bad_state[:10])
        )
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


def partition(spec: str) -> tuple[list[Item], list[str], list[str]]:
    """(有触点的未完成条目, 没写触点的, 明确 `unknown` 不排的)。

    两道门（编译前、推之前）必须用这**一个**函数，否则同一个批次在两道门上是两个数。
    """
    items = load(spec)
    with_touch: list[Item] = []
    without: list[str] = []
    deferred: list[str] = []
    for item in items:
        if item.deferred:
            deferred.append(item.id)
            continue
        if not item.touches:
            # `unknown` 不带理由的，按「没写触点」算：那是漏，不是明确的推迟。
            without.append(item.id)
            continue
        with_touch.append(item)
    return with_touch, without, deferred
