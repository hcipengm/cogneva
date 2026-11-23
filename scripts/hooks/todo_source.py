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
import sys
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

# 一条待办的终态是三格都填：**代码**（树里有了实现，点名那条「没有它就会红」的测试）、
# **上线**（跑着的那一版里有它，点名 rev 与那一次的读数）、**验证**（判据对着真东西量
# 过一次）。三格缺任何一格都还不是终态——「改了文件」与「做完了」之间的差就在这三格里。
CELL_TAGS = ("代码", "上线", "验证")
_CELL_RE = re.compile(r"(?:^|[｜|\s])(代码|上线|验证)\s*[:：]\s*")


def parse_cells(text: str) -> dict[str, str]:
    """从一行里读出三格。每个标签取到下一个标签 / 单元格分隔符为止。

    写得出来就读得出来，读不出来就是没写：没有「大概是这个意思」的余地——这一格
    存在的全部意义是让机器判得出终态，猜出来的终态与没有终态是一样的。
    """
    hits = list(_CELL_RE.finditer(text))
    # `判据读数: test = 文件::函数` 就是 `代码` 那一格的正式写法：读数由程序判，
    # 不由人写。两个名字并存会让 family 看不到格、edit-reading 看得到——同一件事
    # 两个答案（2026-11-21 实测：49 条全真而 family 报 32 条缺格）。
    cells: dict[str, str] = {}
    for index, hit in enumerate(hits):
        stop = hits[index + 1].start() if index + 1 < len(hits) else len(text)
        value = text[hit.end():stop]
        # 单元格分隔符之后的内容不属于这一格。
        value = re.split(r"[|｜]", value)[0].strip().strip("｜|").strip()
        if value and hit.group(1) not in cells:
            cells[hit.group(1)] = value
    # 一个名字：读数就是代码格。`test = ...` 的读数算作代码格已填。
    reading = re.search(r"判据读数\s*:\s*([^｜|]+)", text)
    if reading and "代码" not in cells:
        cells["代码"] = reading.group(1).strip()
    return cells


class Item:
    __slots__ = ("id", "title", "text", "touches", "deferred", "cells", "fixed", "gap", "now")

    def __init__(self, id: str, title: str, text: str, touches: list[str], deferred: bool = False,
                 cells: dict[str, str] | None = None, fixed: bool = False, gap: str = "",
                 now: str = ""):
        self.id = id
        self.title = title
        # 整行原文（不截断）。`title` 是给人看的前 80 字，用来估「这一条读起来要多少
        # token」会把最长的几条估成同一个数——一条待办的正文都在标题之外。
        self.text = text
        self.touches = touches
        # `touches: unknown — 理由`：明确本轮不排。它不是「忘了写触点」——
        # 两者的修法不同（前者要 Split 点名，后者要先补触点），所以这里分开。
        self.deferred = deferred
        # 三格：代码 / 上线 / 验证。**判「做没做完」只认这三格**，不认触点。
        # 触点是「这一轮该改哪些文件」的提示；一个文件改了不等于那条待办做完了
        # （改了一半、改错了地方、或者只是碰巧在同一条路径上）；判据见
        # `deploy/scripts/tests/batch-gate.test.sh` 的场景 3b。
        self.cells = cells or {}
        # 五格行（`序号 | 终态 | 现在 | 差距 | 状态`）的判据不在三格上：它自己写出了
        # 「差距」那一格。走到这里的五格行都是未落——`已落` 且差距为空的行在
        # `_fixed_item` 里就返回 None（不进批次），所以这里不必再判一次终态。
        self.fixed = fixed
        # 五格行里那一格「还差什么」。它随轮一起走（写进轮的载荷）：接着做的人
        # 打开轮就看得见缺口，不必把整本台账重判一遍（S68）。
        self.gap = gap
        # 「现在」那一格：随行去平台，与终态、差距同一种形状。
        self.now = now

    def missing_cells(self) -> list[str]:
        if self.fixed:
            # 五格行缺的永远是它自己写的那一格。借三格的标签去报「缺 代码、上线、
            # 验证」会把读数人引到一格这一行根本没有的列上。
            return ["差距"]
        return [tag for tag in CELL_TAGS if not self.cells.get(tag)]

    def is_complete(self) -> bool:
        return not self.missing_cells()

    def __repr__(self) -> str:  # pragma: no cover - 只为读数好看
        return f"Item({self.id!r}, {len(self.touches)} touches, missing={self.missing_cells()})"


class Unreadable(Exception):
    """台账读不出来。它不是「没有待办」，调用方必须把它和空集合分开。"""


def _md_items(path: pathlib.Path) -> list[Item]:
    """未完成的条目。**缺格的「已落」不在其中**：它们是另一面（`terminal_debt`）。

    两面分开是因为它们回答两个不同的问题：这一轮要做什么（批次），与整本台账还欠
    多少个终态（债）。混在一起，一个历史遗留的缺格会把本轮批次撑到几十条、把这一推
    卡死；而完全不管，那 41 行假的「已落」就永远没人回头补。
    """
    return _md_split(path)[0]


# 五格行：`| 序号 | 终态 | 现在 | 差距 | 状态 |`。序号是字母加数字，子号带点
# （N01、CC02、S12、S20.7 都收）——只认 `字母+数字` 会把 S20.1 那十三行整族留给
# 三格分支，于是「现在」那一格被当成状态词，整本台账读出四十几条假红。
_FIXED_ROW = re.compile(r"^\|\s*[A-Za-z]+[0-9]+(?:\.[0-9]+)*\s*\|")
_FIXED_STATUS = ("未落", "已落")
_GAP_EMPTY = ("", "—", "-", "–")


def _fixed_item(line: str, notouch: list[str], malformed: list[str],
                legacy_status: list[str]) -> tuple[Item, bool] | None:
    """读一行五格台账。返回 (条目, 是否按未完成读)；不是这个形状时返回 None。

    「已落」只在差距那一格为空时才成立。写着已落却还留着差距的行，
    按未完成读并进 debt——与三格那种「填不满的已落」同一处置：跳过它等于承认那个假的终态，
    而拒读会让整本台账一个门都开不开。

    形状不对（列数不足）与状态词不在闭集里的行**点名拒读**：静默跳过它们，这一行就从
    每一个批次里消失了，而读数照样绿——「读不到」被读成「没有」。

    但「列数不足」只对**长得像五格行**的那些行成立：同一本台账里还有更早的
    `| id | 一句话 | 状态 | 证据 |` 四列表格（历史现场），它们的末格是证据路径、不是状态词。
    把那些也拒掉，整本台账就一个门都开不开——门判的是「这一条做到没有」，不是「每一个
    历史表格都合今天的形状」。
    """
    cells = [c.strip() for c in line.strip().strip("|").split("|")]
    if len(cells) < 5:
        if cells and cells[-1] in _FIXED_STATUS:
            # 末格是状态词却只有四格：这是五格行，少了一列。它读不出做没做，点名。
            malformed.append(f"{cells[0]}（五格行只有 {len(cells)} 列）")
        return None
    status = cells[-1]
    if status not in _FIXED_STATUS:
        # 同一本台账里还有更早的几代 `| id | … | 状态 |` 四列表格，最后一格是自由文字
        # （「部分已落；剩余移交」）。它们的格数本就不足五格，在上一段里已经有名字。
        # 到了这里还有五格以上、末格却不是状态词的行，是**五格行写坏了**：拒读并点名。
        # 把写坏的当旧表略过，等于让一条待办整族地从每个批次里消失，而读数照样绿。
        malformed.append(f"{cells[0]}（状态：{status[:16]}）")
        return None
    found = None
    for found in re.finditer(r"[｜|]\s*touches:\s*([^|｜\n]+)", line):
        pass
    touches: list[str] = []
    deferred = False
    if found:
        raw = found.group(1).strip()
        deferred = raw.startswith("unknown")
        touches = [
            re.split(r"[（(]", p.strip())[0].strip()
            for p in re.split(r"[,，]", raw)
            if p.strip() and p.strip() != "unknown"
        ]
        touches = [p.strip(" ）)") for p in touches if p.strip(" ）)")]
    # 差距那一格可以带触点尾巴（`— ｜touches: …`）：五格行的触点没有第六列可放，写在
    # 差距格尾。判终态时先把它剥掉——不剥的话，一条真正做完的行会被读成「写着已落却
    # 留着差距」的债，结束一轮时它会一直欠在那里，而它欠的东西其实只是触点提示。
    gap = re.split(r"[｜|]\s*touches:", cells[3])[0].strip()
    # 「已落且差距为空」这一档**不需要触点**：终态真的到了，它不进任何一批，触点对它
    # 没有意义。先判这一档、再要求触点——反过来的版本会把一条做到终态的行报成
    # 「算不出批次」，于是台账里做得最干净的那几行反而让整本读不动
    # （2026-11-23 实测：N04／S57／N38）。
    if status.startswith(CLOSED_PREFIXES) and gap in _GAP_EMPTY:
        return None
    if not touches and not deferred:
        # 没有触点的行算不出批次。
        notouch.append(cells[0])
        return None
    item = Item(cells[0], cells[1][:80], line.strip(), touches, deferred,
                fixed=True, gap=gap, now=cells[2])
    # 未落的行进批次；写着已落却留着差距的按未完成读（debt），与三格那种同一处置。
    return item, status.startswith(CLOSED_PREFIXES)


# 一次判读里同一本台账会被读两遍（`load()` 与 `terminal_debt()` 各一次）。不缓存的话，
# 每次读数都印两遍同一句「按旧格式略过」——读的人会以为台账里有两份。键里带 mtime，
# 台账一改就重读；缓存只留最后一份。
_SPLIT_CACHE: dict[tuple[str, int, int], tuple[list[Item], list[Item]]] = {}


def _md_split(path: pathlib.Path) -> tuple[list[Item], list[Item]]:
    if not path.exists():
        raise Unreadable(f"{path} 不存在")
    st = path.stat()
    key = (str(path), st.st_mtime_ns, st.st_size)
    cached = _SPLIT_CACHE.get(key)
    if cached is not None:
        return cached
    out: list[Item] = []
    debt: list[Item] = []
    bad_status: list[str] = []
    legacy_status: list[str] = []
    bad_closed: list[str] = []
    malformed: list[str] = []
    notouch: list[str] = []
    for line in path.read_text(errors="ignore").splitlines():
        # 第二种行形状（属主 2026-11-23 定）：`| 序号 | 终态 | 现在 | 差距 | 状态 |`。
        # 它与 `| S…` 那一种判的是同一件事——这一条做到终态没有；差别只在「差距」这一格
        # 是显式写出来的，而不是从三格缺哪几格推出来。两形状都读，因为台账正在换格式，
        # 只认一种就会把另一种**整族读成没有待办**，而那个读数看起来是绿的。
        #
        # 形状由**格数**决定，不由序号前缀决定：`| S` 那几行已经整族改写成五格，
        # 按前缀把它们分给三格分支，读的人就把「现在」那一格当成了状态词，整本
        # S 账读不成（2026-11-23 实测：round.py 直接拒读，逐条报「状态：树里……」）。
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if not line.startswith("| S") and _FIXED_ROW.match(line):
            # 不是 S 序号的表：五格行、四列的旧表、以及写坏的行都从这里过一遍。
            # 旧表在 `_fixed_item` 里按旧格式略过（印计数），写坏的进 malformed 拒读。
            item = _fixed_item(line, notouch, malformed, legacy_status)
            if item is not None:
                (debt if item[1] else out).append(item[0])
            continue
        if _FIXED_ROW.match(line) and len(cells) >= 5 and cells[-1] in _FIXED_STATUS:
            item = _fixed_item(line, notouch, malformed, legacy_status)
            if item is not None:
                (debt if item[1] else out).append(item[0])
            continue
        # 以 `| S` 开头、最后一格不是状态词的还有第三种形状：三格 + 行尾的
        # round/touches 段，格数凑巧也是五格。它靠 cells[2] 认自己，落到
        # 下面那条分支去读。
        if not line.startswith("| S"):
            continue
        if len(cells) < 3:
            # 一行以 `| S` 开头却没有状态列，**不能当它不在**：静默跳过会让一条待办
            # 整族地从批次里消失，而读数照样绿。这与「状态词不在闭集里」是同一类
            # 毛病——读不到不是没有——所以同样点名拒掉。
            malformed.append(f"{cells[0] if cells else line[:20]}（只有 {len(cells)} 列，没有状态列）")
            continue
        status = cells[2]
        three = parse_cells(line)
        want_debt = False
        if status.startswith(CLOSED_PREFIXES):
            # 「已落」是**终态**：三格都填才允许这么写。缺一格的 `已落` 是这一族最坏的
            # 形状——它把「文件改了」读成「做完了」，而且从那一刻起这一行不再被任何
            # 门看（已落的行不进批次）。
            #
            # 缺格的一行**按未完成读**，不是跳过、也不是拒读：跳过就等于承认那个假的
            # 「已落」，而拒读会让整本台账一个门都开不了——一条写错的旧记录不该把
            # 别的活一起停掉。它进批次，读数里明写缺哪格，等补满三格再改回 `已落`。
            missing = [tag for tag in CELL_TAGS if not three.get(tag)]
            if missing:
                bad_closed.append(f"{cells[0]}（缺 {'、'.join(missing)}）")
                want_debt = True
            else:
                continue
        if not status.startswith(OPEN_STATUS) and not status.startswith(CLOSED_PREFIXES):
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
        item = Item(cells[0], cells[1][:80], line.strip(), touches, deferred, three)
        if want_debt:
            debt.append(item)
        else:
            out.append(item)
    if notouch:
        raise Unreadable(
            "这些行没有 `touches:`，批次算不出来（要么点名它要动的文件，要么写 `touches: unknown — 理由`）："
            + "、".join(notouch)
        )
    if legacy_status:
        print(
            f"todo_source: {len(legacy_status)} 行不是五格行（状态不是 `未落`/`已落`），按旧格式略过："
            + "、".join(legacy_status[:6])
            + ("…" if len(legacy_status) > 6 else ""),
            file=sys.stderr,
        )
    if malformed:
        raise Unreadable(
            "这些行短得读不出它做没做（`| S` 行缺状态列，或五格行不足 5 列）："
            + "、".join(malformed)
        )
    if bad_status:
        raise Unreadable(
            "状态词不在闭集里（**只有两个词**：`未落` / `已落`）：" + "、".join(bad_status)
        )
    _SPLIT_CACHE.clear()
    _SPLIT_CACHE[key] = (out, debt)
    return out, debt


def normalize(spec: str) -> str:
    """裸路径与 `md:<路径>` 是同一个来源。

    调用方有两种写法：`default_spec()` 给的是裸路径，别的几处给的是 `md:` 前缀。
    代码曾经只认后者——于是 `spec[3:]` 把 `/home/...` 切成了 `me/...`，读写都落在一个
    凭空造出来的相对路径上，而它长得像一个合法的台账地址。
    """
    if spec.startswith("md:") or spec == "change-ledger":
        return spec
    return "md:" + spec


def spec_path(spec: str) -> pathlib.Path:
    """台账文件本身（`md:` 与裸路径都收）。"""
    return pathlib.Path(normalize(spec)[3:]).expanduser()


def all_ids(spec: str) -> set[str]:
    """台账里出现过的**所有** id：未完成的与已落的都算。

    用途只有一个：判「钉住的那一条是不是还在台账里」。只 `load` 得到未完成的那些，
    而一条刚做完的待办会从那一侧消失——那是正常的收尾，不是丢了。
    """
    spec = normalize(spec)
    if not spec.startswith("md:"):
        return {item.id for item in load(spec)}
    path = spec_path(spec)
    if not path.exists():
        raise Unreadable(f"{path} 不存在")
    ids: set[str] = set()
    for line in path.read_text(errors="ignore").splitlines():
        # 两种行形状都算（与 `_md_split` 同一套）。只认 `| S…` 那一种时，五格台账里
        # 的每一条都答「不在台账里」——而这个函数的唯一用途就是分辨「做完了从批次里
        # 消失」与「被人删掉了」（2026-11-23 实测：新台账 7 条全被报成缺失）。
        if not line.startswith("| S") and not _FIXED_ROW.match(line):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if cells and cells[0]:
            ids.add(cells[0])
    return ids


def terminal_debt(spec: str) -> list[Item]:
    """整本台账上写着 `已落`、却没填满三格的那些行。

    它们不是本轮批次的一部分，但它们**不是**终态：那个 `已落` 是从前的判据
    （「触点在改动里」）留下的，而那一套已经不再是判据。这份清单是「还欠多少个终态」
    的唯一读数——没有它，那些行从此不再被任何门看。
    """
    spec = normalize(spec)
    if not spec.startswith("md:"):
        return []
    return _md_split(spec_path(spec))[1]


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
    spec = normalize(spec)
    if spec.startswith("md:"):
        return _md_items(spec_path(spec))
    if spec == "change-ledger":
        return _change_ledger_items()
    raise Unreadable(f"不认识的台账来源：{spec}（只认 md:<路径> 与 change-ledger）")


def _git_config(key: str) -> str | None:
    """当前工作树那个仓里的一条配置；没有、或读不了，都是 None。"""
    try:
        out = subprocess.run(["git", "config", "--get", key], capture_output=True, text=True)
    except FileNotFoundError:
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def ledger_from_repo_config() -> str | None:
    """本机仓配置给的两格：地址 `cogneva.privateRoot` 与文件名 `cogneva.workLedger`。

    为什么走配置而不是走常量：带外 worker 的台账住在一个私有仓里，它的**名字和位置**
    都不该写进公开树——写进去等于把要藏的东西放进被它守的那棵树。地址与文件名各是一
    条本机配置，换机器只改配置；公开树里一个字都不留。

    `cogneva.workLedger` 是绝对路径就原样用；是相对路径就接在 `cogneva.privateRoot`
    后面。只有地址没有文件名时返回 None——「有一个仓」与「知道是哪本台账」是两件事，
    不拿仓里随便一本顶替（那会让两个 worker 读同一本，其中一边的活永远没人做）。
    """
    ledger = _git_config("cogneva.workLedger")
    if not ledger:
        return None
    path = pathlib.Path(ledger).expanduser()
    if not path.is_absolute():
        root = _git_config("cogneva.privateRoot")
        if not root:
            return None
        path = pathlib.Path(root).expanduser() / path
    return str(path)


def default_spec() -> str:
    """这台机器上，**这个 worker** 的台账是哪一本。

    解析顺序（S62）：显式 `COGNEVA_TODO_SOURCE` → `COGNEVA_WORKER_LEDGER` →
    本机仓配置（`cogneva.privateRoot` ＋ `cogneva.workLedger`）→ 本机声明
    `~/.config/cogneva/todo-ledger`。都没有，就返回一个不存在的路径，让调用方报
    「读不到」——**不回落**到一份共享清单。回落会让两个 worker 读到同一本台账：
    批次在两边都算得出来，而其中一边的活永远没人做。
    """
    explicit = os.environ.get("COGNEVA_TODO_SOURCE")
    if explicit:
        return explicit
    for key in ("COGNEVA_WORKER_LEDGER", "COGNEVA_TODO_LEDGER"):
        value = os.environ.get(key)
        if value:
            return f"md:{value}"
    from_config = ledger_from_repo_config()
    if from_config:
        return f"md:{from_config}"
    host = pathlib.Path.home() / ".config/cogneva/todo-ledger"
    if host.exists():
        first = host.read_text(errors="ignore").strip().splitlines()
        if first and first[0].strip():
            return f"md:{first[0].strip()}"
    return f"md:{pathlib.Path.home() / '.local/state/cogneva/todo-ledger.md'}"


def worker_name() -> str:
    """这台机器上跑的是谁：显式 → tmux 会话名 → 主机名（S62 用来认领台账）。"""
    explicit = os.environ.get("COGNEVA_WORKER")
    if explicit and explicit.strip():
        return explicit.strip()
    tmux = os.environ.get("TMUX")
    if tmux:
        out = subprocess.run(["tmux", "display-message", "-p", "#S"], capture_output=True, text=True)
        name = out.stdout.strip()
        if out.returncode == 0 and name:
            return name
    return os.uname().nodename


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
