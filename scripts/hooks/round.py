#!/usr/bin/env python3
"""一轮的开始与关闭：只存一个坐标，不存清单。

「这一轮从哪一版账开始」是唯一必须留下来的信息。它不进文件、不抄清单——清单抄一份
到别处就多一份会漂的副本，而正本只有台账。所以：

    round.py open      # 记下台账此刻的 git rev，开一条轮
    round.py status    # 按那条轮钉住的清单读台账，现算还剩哪些
    round.py close     # 全部做完才能关掉这一轮

**这一轮住在哪本书里由台账来源导出，不由人挑**：

  - 带外 worker 的台账（`md:<路径>`，住在一个与其远端对齐的 git 仓里）
    ⇒ 书是**那个仓的 `refs/cogneva/round`**，载荷就是同一组键。它不需要集群；
  - 平台自己的变更账本（`change-ledger`，在库里、没有 git rev）⇒ 书是**集群事件流**里
    的一条 `round_opened`。

「事件流连不上就悄悄改读 git」正是这一族在修的缺陷：读不到一本书不等于另一本书里没有
那一轮。所以书只有一本，且是从「这本台账住哪儿」算出来的，没有开关、没有回退。

坐标住在 git 对象库里，不在工作树上：不随 commit 走、不是文档、不是第二份状态。上一
版 docstring 写着「坐标先寄在 git 里」而这本根本没实现，门便只能在有集群的机器上跑。

批次由 `family.py` / `batch-gate.py` 用同一个读数现算：读那条轮钉住的清单，取所有
未完成条目。台账在工作过程中怎么变，这一轮的批次都不变。
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import subprocess
import sys

PSQL_ENV = "COGNEVA_EVENT_PSQL"
ROUND_REF = "refs/cogneva/round"
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402


def ledger_repo(spec: str) -> pathlib.Path | None:
    """台账所在的那个 git 仓。change-ledger 没有 git rev，返回 None。

    `default_spec()` 给的是裸路径（`/…/specs/work-ledger-….md`），而别处给的是
    `md:<路径>`。两种都收：此前只认带 `md:` 的那一种，于是 family／batch-gate
    拿裸路径来问时永远得到 None，台账仓那一侧的触点（`specs/…`、台账仓里的
    `scripts/…`）全被读成「树里不存在」，而这条分支从没真的跑过。
    """
    if spec.startswith("md:"):
        spec = spec[3:]
    elif ":" in spec:
        return None
    path = pathlib.Path(spec).expanduser().resolve()
    out = subprocess.run(
        ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
        capture_output=True, text=True,
    ).stdout.strip()
    return pathlib.Path(out) if out else None


def git_origin_address(repo: pathlib.Path) -> str | None:
    """台账仓的地址（`remote.origin.url`）。没有配 origin 就是 None。

    载荷里记**地址**而不是本机路径：同一本书在不同机器上有不同的检出路径，地址才是
    跨机器认得出的那个身份。没有地址是「这台机器上的这个仓不指向任何远端」这条事实，
    不拿本地路径顶替它——两种情况的读者要做的事不同。
    """
    out = subprocess.run(
        ["git", "-C", str(repo), "config", "--get", "remote.origin.url"],
        capture_output=True, text=True,
    )
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def psql_command() -> str:
    """读/写事件流的那条命令：环境变量优先，其次是仓库自带的那个入口。

    第二档不是兜底而是正本：`scripts/host/event-psql.sh` 就在同一个仓里，它接收
    一段 SQL 并把结果行印出来。没有它的时候每一处调用方都得自己把环境变量拼对，
    而拼错与「流里没有开着的一轮」在读数上是同一句话——一轮开着也读不出来。
    """
    import os
    cmd = os.environ.get(PSQL_ENV, "")
    if cmd:
        return cmd
    shipped = pathlib.Path(__file__).resolve().parent.parent / "host" / "event-psql.sh"
    if shipped.is_file():
        return str(shipped)
    raise todo_source.Unreadable(
        f"没有配置事件流的读写命令：{PSQL_ENV} 没设，仓库里也没有 {shipped}。"
        "这条命令要接收一段 SQL 并在 `cogneva_event` 上执行"
    )


def psql(sql: str) -> str:
    """读/写事件流的那条命令。宿主配置给出，或者用仓库自带的那个入口。"""
    cmd = psql_command()
    result = subprocess.run(f"{cmd} {shlex.quote(sql)}", shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise todo_source.Unreadable(f"事件流命令失败：{result.stderr.strip()[:200]}")
    return result.stdout


def round_book(source: str | None = None) -> str:
    """这一轮住哪本书：由**台账来源**导出，不由人挑。

    `md:<路径>` 的台账住在一个 git 仓里（带外 worker 的台账：会话与平台共用的那本，
    远端本地对齐），书就是那个仓的 `refs/cogneva/round`；`change-ledger` 是平台自己
    的账，在库里、没有 rev，书是事件流。没有第三种台账，所以也没有开关与回退。
    """
    spec = todo_source.normalize(todo_source.default_spec() if source is None else source)
    if spec == "change-ledger":
        return "event"
    if ledger_repo(spec) is None:
        raise todo_source.Unreadable(
            f"台账 {spec} 不在一个 git 仓里，也不是平台的变更账本——这一轮钉不到任何一本书上"
        )
    return "git"


def git_round_repo(spec: str | None = None) -> pathlib.Path:
    """git 书住的那个仓：台账所在的那个 git 仓。

    轮钉的是台账的 rev，所以书与台账必须在同一个仓；没有 rev 的台账（平台的
    change-ledger 在库里）用不了这本书，要明说，不能悄悄换成事件流。
    """
    source = todo_source.default_spec() if spec is None else spec
    repo = ledger_repo(todo_source.normalize(source))
    if repo is None:
        raise todo_source.Unreadable(
            f"git 书要一本有 git rev 的台账；{source} 没有——这台机器上的轮要钉在台账仓"
        )
    return repo


def git_round_payload(repo: pathlib.Path) -> tuple[str, dict] | None:
    """(blob sha, 载荷)；这个 ref 不存在返回 None。

    ref 在而载荷读不出 ⇒ Unreadable：那是书坏了，不是「没有开着的一轮」。
    """
    probe = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", ROUND_REF],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        if probe.stderr.strip():
            raise todo_source.Unreadable(
                f"git 书读不到 {ROUND_REF}：{probe.stderr.strip()[:200]}"
            )
        return None
    sha = probe.stdout.strip()
    if not sha:
        return None
    body = subprocess.run(
        ["git", "-C", str(repo), "cat-file", "-p", sha], capture_output=True, text=True,
    )
    if body.returncode != 0:
        raise todo_source.Unreadable(
            f"{ROUND_REF} 指向的 {sha[:12]} 读不出字节：{body.stderr.strip()[:200]}"
        )
    try:
        payload = json.loads(body.stdout)
    except ValueError as e:
        raise todo_source.Unreadable(f"{ROUND_REF} 的载荷不是 JSON：{e}")
    if not isinstance(payload, dict):
        raise todo_source.Unreadable(f"{ROUND_REF} 的载荷不是一份对象读法")
    return sha, payload


def write_git_round(repo: pathlib.Path, payload: dict, old: str | None) -> str:
    """把载荷写进 ref（CAS：`old` 是期望的当前值，None 表示「此刻必须不存在」）。"""
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    blob = subprocess.run(
        ["git", "-C", str(repo), "hash-object", "-w", "--stdin"],
        input=body, capture_output=True, text=True,
    )
    if blob.returncode != 0:
        raise todo_source.Unreadable(f"写不进 git 书：{blob.stderr.strip()[:200]}")
    new = blob.stdout.strip()
    result = subprocess.run(
        ["git", "-C", str(repo), "update-ref", "--create-reflog", ROUND_REF, new, old or ""],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise todo_source.Unreadable(
            f"{ROUND_REF} 没写成（期望旧值 {str(old)[:12] or '不存在'}）：{result.stderr.strip()[:200]}"
        )
    return new


def current_round() -> tuple[str, str, str, list[str], str] | None:
    """(event_id, ledger_rev, code_rev, batch ids, first_rev)；没有开着的一轮返回 None。"""
    if round_book() == "git":
        found = git_round_payload(git_round_repo())
        if found is None:
            return None
        _sha, payload = found
        if payload.get("closed_at"):
            return None
        batch = payload.get("batch") or []
        if not isinstance(batch, list):
            raise todo_source.Unreadable(f"{ROUND_REF} 的 batch 不是数组：{batch!r}")
        rev = str(payload.get("ledger_rev") or "")
        return (
            str(payload.get("event_id") or ""),
            rev,
            str(payload.get("code_rev") or ""),
            [str(b) for b in batch],
            str(payload.get("first_rev") or rev),
        )
    return current_round_in_stream()


def current_round_in_stream() -> tuple[str, str, str, list[str], str] | None:
    """(event_id, ledger_rev)；没有开着的一轮返回 None。

    「开着」＝流里最后一条 `round_opened` 没有配对的 `round_closed`（按 subject_id 配）。
    """
    sql = (
        "select o.event_id || E'\\t' || coalesce(o.attributes::jsonb->>'ledger_rev','') "
        "|| E'\\t' || coalesce(o.attributes::jsonb->>'code_rev','') "
        "|| E'\\t' || coalesce(o.attributes::jsonb->>'batch','[]') "
        # 第五格是**这一轮第一次开工**的那个 rev：重钉不改它。改成「自开工以来改了什么」
        # 的量程一旦跟着重钉走，之前做过的活就全部读作没做（2026-11-21 实测）。
        "|| E'\\t' || coalesce(o.attributes::jsonb->>'first_rev', o.attributes::jsonb->>'ledger_rev','') "
        "from cogneva_event o where o.kind='round_opened' and not exists ("
        " select 1 from cogneva_event c where c.kind='round_closed'"
        " and c.subject_id=o.subject_id) order by o.at desc, o.event_id desc limit 1"
    )
    out = psql(sql).strip()
    if not out:
        return None
    parts = out.split("\t")
    event_id = parts[0]
    rev = parts[1] if len(parts) > 1 else ""
    code_rev = parts[2] if len(parts) > 2 else ""
    try:
        batch = json.loads(parts[3]) if len(parts) > 3 and parts[3] else []
    except ValueError:
        batch = []
    # 老事件没有那一格：退回 `rev`，与它写下时的量程一致。
    first_rev = parts[4] if len(parts) > 4 else rev
    return event_id, rev, code_rev, [str(b) for b in batch], first_rev


def ledger_at(rev: str, spec: str) -> pathlib.Path:
    """把那个 rev 上的台账取到临时文件。

    临时文件是**读**的中间物（`git show` 只写 stdout），不是状态：它不承载「这一轮
    是什么」，只承载「那一版台账的字节」。
    """
    path = todo_source.spec_path(spec).resolve()
    repo = ledger_repo(spec)
    if repo is None:
        raise todo_source.Unreadable("这个来源没有 git rev，读不出那一版台账")
    blob = subprocess.run(
        ["git", "-C", str(repo), "show", f"{rev}:{path.relative_to(repo)}"],
        capture_output=True, text=True,
    )
    if blob.returncode != 0:
        raise todo_source.Unreadable(f"{rev[:12]} 上没有那份台账：{blob.stderr.strip()[:160]}")
    # 文件名带台账路径的短哈希：同一台机器上两个 worker 各有一本台账，而两本台账
    # 常常停在同一个 rev 上——只按 rev 命名，后写的那个会把前一个的字节盖掉，读的人
    # 于是拿到「另一个 worker 的待办」，而它长得完全正常。
    digest = hashlib.sha1(str(path).encode()).hexdigest()[:8]
    tmp = pathlib.Path("/tmp") / f"cogneva-ledger-{digest}-{rev[:12]}.md"
    tmp.write_text(blob.stdout)
    return tmp


def open_round(spec: str | None, allow_open: bool = False, code_rev_override: str | None = None) -> int:
    if current_round() and not allow_open:
        print("round: 已经有一轮开着。先 `round.py close`。")
        return 1
    source = spec or todo_source.default_spec()
    # 批次的正本是「这一轮开了哪些条」，不是某个 rev 上的台账：平台自己的变更账本
    # 在库里，没有 git rev，而 worker 的 markdown 台账有。两个来源共用同一条判据。
    repo = ledger_repo(source)
    if repo is not None:
        rev = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                             capture_output=True, text=True).stdout.strip()
    else:
        rev = ""
    # 打开一轮的时候就把**整批**列出来，并把它记进事件：单位必须在编辑之前
    # 定下来，否则「这一轮要改什么」由动手的人临场挑，默认全量就退成子集。
    import os
    import uuid
    source = todo_source.normalize(source)
    # 批次用 `partition`，不用 `load`：`touches: unknown` 的行明确本轮不排，钉进批次
    # 就等于用一个自己写过「不排」的词把它排进来——那一条后来永远关不掉这一轮。它与
    # 两道编译门用的是同一个函数（`partition` 的 docstring），批次在三个地方是同一个数。
    if source.startswith("md:"):
        batch, _without, deferred = todo_source.partition(source)
    else:
        batch, deferred = [], []
    if deferred:
        print(f"round: 本轮不排（`touches: unknown`）{len(deferred)} 条：{', '.join(deferred)}")
    if batch:
        batch_plan_holds(batch)
        print(f"round: 本批 {len(batch)} 条（同一批编辑，不挑子集）：")
        for item in batch:
            where = "、".join(item.touches[:3]) + ("…" if len(item.touches) > 3 else "")
            print(f"  {item.id}  {where}")
    else:
        print("round: 台账里没有未完成的待办。")
    event_id = uuid.uuid4().hex[:26]
    subject = f"round-{event_id}"
    code_rev = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    # `batch` 把这一批的身份写进事件：后面那道门按**这个**清单判「同批」，
    # 而不是每次自己重算一份——重算出来的东西读起来一样，却不是同一件事。
    ids = json.dumps([i.id for i in batch])
    # 重钉继承「第一次开工」那一格：重钉要的是按新台账重算批次清单，不是把
    # 「自开工以来改了什么」这条量程清零。
    _existing = current_round()
    # **重钉不改量程，两个坐标都不改**：批次清单按新台账重算（这是重钉的用途），
    # 而「自开工以来改了什么」的基准必须留在第一次开工那一刻。2026-11-21 实测：
    # 只护住 first_rev（台账仓那一侧）而让 code_rev 跟着重钉走，结果 33 条里
    # 只有 1 条读作「动过」——重钉一次，做过的活全部变回没做。
    first_rev = _existing[4] if _existing else rev
    if _existing and _existing[2]:
        code_rev = _existing[2]
    if code_rev_override:
        code_rev = code_rev_override
    if round_book(source) == "git":
        repo = git_round_repo(source)
        # 读的那一侧按 default_spec 找书。两处不是同一个仓，写下的轮谁也读不到——
        # 那会是一个「开过」而处处读作「没开」的轮，比拒开更坏。
        if repo != git_round_repo():
            raise todo_source.Unreadable(
                f"这一轮钉在 {repo}，而本机读的是 {git_round_repo()}：两处不是同一本书"
            )
        found = git_round_payload(repo)
        payload = {
            "event_id": event_id,
            "ledger_rev": rev,
            "ledger_address": git_origin_address(repo),
            "ledger_file": str(todo_source.spec_path(source).resolve().relative_to(repo.resolve())),
            "code_rev": code_rev,
            "batch": [i.id for i in batch],
            # 缺口随行走（S68）：接着做的人打开轮就看得见每条还差什么，不必把
            # 整本台账重判一遍。空串是「这一条没有缺口」（五格行的差距为空）。
            "gaps": {i.id: i.gap for i in batch},
            "first_rev": first_rev,
            "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        write_git_round(repo, payload, found[0] if found else None)
        print(
            f"round: 书=git {ROUND_REF}@{repo}；开了一条钉在台账仓 {rev[:12]} 的轮；"
            "批次由那个 rev 现算。"
        )
        return 0
    attrs_sql = (
        f"jsonb_build_object('ledger_rev', '{rev}', 'ledger_path', '{todo_source.spec_path(source)}', "
        # `batch` 存**数组**，不是一段含数组的字符串：同一个键两种形状，Rust 那一侧
        # 按数组读，读到现在这 33 条会当场报「这一轮没有清单」。2026-11-22 复核发现。
        f"'code_rev', '{code_rev}', 'batch', '{ids}'::jsonb, 'first_rev', '{first_rev}')::text"
    )
    psql(
        "insert into cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, "
        "step, outcome, idempotency_key, actor, writer, attributes) values "
        f"('{event_id}','round_opened','round','{subject}','{subject}','open','ok',"
        f"'{event_id}','{os.environ.get('COGNEVA_ACTOR', 'worker')}',"
        f"'{os.environ.get('COGNEVA_WRITER', 'worker')}',{attrs_sql})"
    )
    print(f"round: 书=事件流；开了一条 round_opened 事件（钉在台账仓 {rev[:12]}）；批次由那个 rev 现算。")
    return 0



def batch_plan_holds(batch) -> None:
    """把这一批交给平台的 planner 判一次：计划站不住就不开这一轮。

    定批归 planner（默认全量、只按三条判据拆），固定程序判合格。这里只做转交与
    判词透传；没有平台二进制就**开不了**——一个「没判过」的轮会被读成「判过了」。
    """
    binary = os.environ.get("COGNEVA_BIN") or shutil.which("cogneva")
    if not binary:
        raise todo_source.Unreadable(
            "这一轮开不了：没有平台二进制（`COGNEVA_BIN` 或 PATH 上的 `cogneva`）。"
            "定批与计划由 planner 判，Python 这一侧不再自己算一份"
        )
    payload = [
        {
            "id": i.id,
            "title": i.title,
            "touches": list(i.touches),
            "item": {
                "id": i.id,
                "terminal": i.title,
                "now": getattr(i, "now", ""),
                "gap": getattr(i, "gap", ""),
                "state": "未落",
            },
        }
        for i in batch
    ]
    proc = subprocess.run(
        [binary, "batch-plan"],
        input=json.dumps(payload, ensure_ascii=False),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise todo_source.Unreadable(
            f"这一批不成立，开不出轮：{proc.stderr.strip() or proc.returncode}"
        )


def remaining() -> list[str]:
    """这一轮钉住的那批 id。

    批次的正本是 round 事件里记下的那份 id 清单，不是「某个 rev 上的台账」——
    后者只对 git 里的台账成立，而平台自己的变更账本在库里，没有 rev。所以任何
    来源都用同一条判据：这一轮开了哪些条，就做哪些条。
    """
    current = current_round()
    if current is None:
        return []
    return current[3]


def batch_touches() -> list[tuple[str, list[str]]]:
    """这一轮的 (id, 触点)，取自**开工那一刻**的台账。

    触点用来判「动没动到」；它跟着批次一起钉住，台账后续怎么变都不影响这一轮。
    """
    current = current_round()
    if current is None:
        return []
    source = todo_source.default_spec()
    if source.startswith("md:") and current[1]:
        try:
            path = ledger_at(current[1], source)
            items = {i.id: i for i in todo_source.load(f"md:{path}")}
            return [(i, items[i].touches) for i in current[3] if i in items]
        except todo_source.Unreadable:
            pass
    items = {i.id: i for i in todo_source.load(source)}
    return [(i, items[i].touches) for i in current[3] if i in items]


def status() -> int:
    current = current_round()
    if current is None:
        print("round: 没有开着的一轮。动手之前先 `round.py open`。")
        return 1
    return status_report(current)


def status_report(current: tuple[str, str, str, list[str], str]) -> int:
    rev = current[1]
    left = remaining()
    # 每一条缺哪一格就说哪一格：三格是本轮判「做没做完」的**唯一**判据（触点只提示
    # 该改哪些文件）。只印一个 id 的话，读的人还得自己去台账里找缺的是哪一格。
    cells: dict[str, list[str]] = {}
    known: set[str] | None = None
    spec = todo_source.default_spec()
    try:
        items = {i.id: i for i in todo_source.load(spec)}
        cells = {i: items[i].missing_cells() for i in left if i in items}
        # 「不在未完成那一侧」有两个意思：这一条做完了（行还在台账里，落在终态那一侧），
        # 或者它被人从台账里删了。判它们的是同一本台账的 id 全集；读不到就不判，也不猜。
        # 不查这一问的话，做完的条目会从批次里消失、读数写「未完成」，于是**每一轮都关不掉**。
        known = todo_source.all_ids(spec)
    except todo_source.Unreadable as e:
        print(f"round: 台账读不到（{e}）；下面只列 id")
    book = "事件流" if round_book() == "event" else f"git {ROUND_REF}"
    print(f"round: 书={book}，钉在 {rev[:12]}，还剩 {len(left)} 条")
    gaps = {i.id: i.gap for i in items.values()} if items else {}
    terminal = 0
    for i in left:
        gap = gaps.get(i) or ""
        if gap and gap.strip("—–- "):
            # 五格台账的「还差什么」就在那一行里：直接印它，比把三格的空位
            # 重新数一遍更接近读的人要知道的那件事。
            print(f"  未完成 {i}  {gap}")
            continue
        miss = cells.get(i)
        if miss is None and known is not None and i in known:
            # 在台账里、但不在「未完成」那一侧：它已经落在终态那一侧（五格行的
            # `已落`＋空差距，或三格行的三格齐＋`已落`）。这一轮钉的清单来自开工那一刻，
            # 行做完就从那一侧消失——消失是收尾，不是丢件。
            terminal += 1
            print(f"  已落 {i}")
            continue
        if miss is None:
            print(f"  未完成 {i}（未完成那一侧没有它，台账里也没有这一行）")
            continue
        if not miss:
            terminal += 1
            print(f"  三格齐 {i}")
        else:
            print(f"  未完成 {i}  缺 {'、'.join(miss)}")
    if terminal:
        print(f"round: 其中 {terminal} 条三格齐（终态）；台账里仍写 `未落` 的那些等提交换行。")
    # 另一面：整本台账上写着 `已落`、却没填满三格的那些行。它们不在这一批里（不属于
    # 这一轮的编辑范围），但它们**不是**终态——那个 `已落` 是旧判据留下的。不印出来，
    # 它们就再也不会被任何人看第二眼。
    try:
        debt = todo_source.terminal_debt(todo_source.default_spec())
    except todo_source.Unreadable:
        debt = []
    if debt:
        print(
            f"round: 台账上另有 {len(debt)} 行写着 `已落` 却没有三格（欠终态），"
            f"不在本轮批次：{', '.join(i.id for i in debt)}"
        )
    try:
        tree = pathlib.Path(subprocess.run(["git", "rev-parse", "--show-toplevel"],
                                           capture_output=True, text=True).stdout.strip())
    except OSError:
        tree = pathlib.Path(".")
    # 下一步由平台的 `ledger next` 算（同一套读数：台账、工作树、编译账）。没有二进制
    # 就说「量不到」——这一侧不再自己算一份，两份算法迟早各说各的。
    binary = os.environ.get("COGNEVA_BIN") or shutil.which("cogneva")
    if binary:
        proc = subprocess.run([binary, "ledger", "next"], capture_output=True, text=True)
        step = (proc.stdout.strip() or proc.stderr.strip() or "量不到").removeprefix("ledger: ")
        print(f"round: 下一步 —— {step}")
    else:
        print("round: 下一步 —— 量不到（没有平台二进制：`COGNEVA_BIN` 或 PATH 上的 `cogneva`）")
    return 0 if terminal == len(left) else 1


CLOSED_ROUNDS_ENV = "COGNEVA_CLOSED_ROUNDS"
DEFAULT_CLOSED_ROUNDS = pathlib.Path.home() / ".local/state/cogneva/rounds-closed.tsv"


def closed_rounds_path() -> pathlib.Path:
    return pathlib.Path(os.environ.get(CLOSED_ROUNDS_ENV, str(DEFAULT_CLOSED_ROUNDS)))


def record_closed_round(payload: dict) -> None:
    """把「这一批真的收过」记成一行读数。

    记的是**这一轮开门时那一批**有几条，而不是关轮那一刻还算得出来的那几条：轮关得掉
    的前提是整批都到了终态，于是重钉之后现算的批是空的——按它记，这条读数永远是 0，
    而它正是 `Split: 4`（一次读不完）的界。所以原始清单从 `first_rev` 上取：那一版台账
    里还开着的那几条，就是这一轮开门时点下的那几条。
    """
    path = closed_rounds_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    carried = original_batch(payload)
    line = "\t".join(
        [
            dt.datetime.now(dt.timezone.utc).isoformat(),
            str(payload.get("first_rev") or payload.get("ledger_rev", ""))[:12],
            str(len(carried)),
            ",".join(carried),
        ]
    )
    with path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def original_batch(payload: dict) -> list[str]:
    """这一轮开门时点下的那几条：从 `first_rev` 那一版台账里现算。

    读不到（那一版没了、或台账不在 git 仓里）就退回载荷里记的 `batch`——两种都是
    「这一批有几条」的读法，只是取的地方不同；静默返回空会让这条读数看起来像
    「这一轮什么也没收」。
    """
    source = todo_source.default_spec()
    first = payload.get("first_rev")
    if first:
        try:
            path = ledger_at(first, source)
            return [i.id for i in todo_source.load(f"md:{path}")]
        except (todo_source.Unreadable, OSError):
            pass
    return list(payload.get("batch") or [])


COMPILE_LEDGER_ENV = "COGNEVA_COMPILE_LEDGER"
DEFAULT_COMPILE_LEDGER = pathlib.Path.home() / ".local/state/cogneva/compile-ledger.tsv"


def _last_compile_line(tree: pathlib.Path) -> list[str] | None:
    """这一棵树最后一次编译的账行；读不到返回 None（量不到，不是没编过）。"""
    path = pathlib.Path(os.environ.get(COMPILE_LEDGER_ENV, str(DEFAULT_COMPILE_LEDGER)))
    if not path.is_file():
        return None
    last = None
    for line in path.read_text(errors="ignore").splitlines():
        parts = line.split("\t")
        if len(parts) >= 4 and parts[1] == str(tree):
            last = parts
    return last


def next_step(tree: pathlib.Path) -> str:
    """**下一步是什么**，由读数算出来，不由人记着。

    读的三样：`family.py --editing`（还有哪几行点名的文件没动到）、编译账里这一棵树
    的最后一条（什么时候编的、编的哪个 HEAD）、以及工作区此刻的 HEAD。顺序就是流程的
    顺序——先改完，再编一次，最后才谈推；读不到的那一格说「量不到」，不猜。
    """
    family = tree / "scripts/hooks/family.py"
    if not family.is_file():
        return "量不到（这棵树里没有 family.py）"
    # 台账地址必须和这一轮读到的是同一本：不传 `--ledger` 时 family 读它自己的默认
    # 解析（本机的 worker 台账），于是「下一步」会按**另一本账**算出来——读数是对的，
    # 判的不是这一轮（2026-11-23 实测：夹具里它对着真台账说「还有 41 条」）。
    spec = todo_source.default_spec()
    ledger_arg = todo_source.normalize(spec)
    if ledger_arg.startswith("md:"):
        ledger_arg = ledger_arg[3:]
    try:
        proc = subprocess.run(
            ["python3", str(family), "--root", str(tree), "--ledger", ledger_arg,
             "--all-todos", "--editing"],
            capture_output=True, text=True,
        )
    except OSError:
        return "量不到（family.py 跑不起来）"
    todo_lines = [l for l in proc.stdout.splitlines() if l.strip().startswith("TODO ")]
    if proc.returncode == 0 and not todo_lines:
        pass
    elif todo_lines:
        return f"继续编辑——还有 {len(todo_lines)} 条没动到它们点名的文件"
    else:
        return "量不到（family.py 没判成：见上面那行）"
    # 编辑完了、还没提交时**不能**编：垫片在脏树上拒绝编译，理由是没有推上去的字节
    # 与编出来的是两份。这一步在次序里真实存在，所以它也要被说出来。
    dirty = subprocess.run(["git", "-C", str(tree), "status", "--porcelain"],
                           capture_output=True, text=True).stdout.strip()
    if dirty:
        return "先提交（这一批的编辑已到位，编译垫片在脏树上不编）"
    head = subprocess.run(["git", "-C", str(tree), "rev-parse", "HEAD"],
                          capture_output=True, text=True).stdout.strip()
    ledger = _last_compile_line(tree)
    if ledger is None:
        return "编译一次（这个宿主还没有这棵树的编译账）"
    compiled_head = ledger[3]
    if compiled_head != head:
        return f"编译一次（账上最后一次编的是 {compiled_head[:12]}，此刻 HEAD 是 {head[:12]}）"
    return "推（先跑 `deploy/scripts/check-before-push.sh`；绿了才推）"


def stop_gate() -> int:
    """收工钩子要的那一问：**现在能不能停**。

    `status` 的往返码答的是「这一批做完了没有」，而「压根没有开着的一轮」与
    「开着的一轮还有缺口」都退 1——按裸往返码分支的读者把这两件事读成了一件：
    收工钩子于是把「没有轮」读成「这一轮没关」，关完一轮之后照样退 2 拦人
    （2026-11-23 实测：`close` 成功，钩子下一行印的就是「没有开着的一轮」）。
    这一问只答三态，往返码是它自己的协议，不由人挑：
      0 = 可以停（没有开着的一轮，或这一轮已终态）；
      2 = 不许停（开着的一轮还有缺口，缺口照 `status` 原样打印）；
      3 = 判不了（书读不出来，由 `main` 统一退）——读不出来不是「可以停」。
    """
    current = current_round()
    if current is None:
        return 0
    return 0 if status_report(current) == 0 else 2


def close() -> int:
    current = current_round()
    if current is None:
        print("round: 没有开着的一轮。")
        return 1
    if status() != 0:
        print("round: 还有没做完的条目，这一轮关不掉。", file=sys.stderr)
        return 1
    if round_book() == "git":
        repo = git_round_repo()
        found = git_round_payload(repo)
        if found is None:
            print("round: 没有开着的一轮。")
            return 1
        sha, payload = found
        payload["closed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        write_git_round(repo, payload, sha)
        record_closed_round(payload)
        print(f"round: 这一轮关了（书=git {ROUND_REF}@{repo}；关的时刻在同一份载荷里）。")
        return 0
    import os
    import uuid
    event_id = uuid.uuid4().hex[:26]
    psql(
        "insert into cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, "
        "step, outcome, idempotency_key, causation_id, actor, writer) values "
        f"('{event_id}','round_closed','round','{current[0]}','{current[0]}','close','ok',"
        f"'{event_id}','{current[0]}','{os.environ.get('COGNEVA_ACTOR','worker')}',"
        f"'{os.environ.get('COGNEVA_WRITER','worker')}')"
    )
    print("round: 这一轮关了（追加了一条 round_closed）。")
    return 0



def plan_defects(batch_ids: list[str], plan: dict) -> list[str]:
    """计划必须覆盖**整批**，且每条待办至少两种来源形状的验收。

    与 Rust 侧 `Plan::validate` 同一条判据（那边是平台路径的权威，这边是带外
    worker 的入口）：一份少了几条的计划等于把「同批编辑」悄悄缩回子集。
    """
    defects: list[str] = []
    covered = plan.get("items") if isinstance(plan, dict) else None
    if not isinstance(covered, list):
        return ["计划里没有 `items`：它要逐条回答这一批每一条怎么做完"]
    seen = {item.get("id") for item in covered if isinstance(item, dict)}
    missing = [i for i in batch_ids if i not in seen]
    if missing:
        defects.append("计划漏了这些待办：" + "、".join(missing))
    for item in covered:
        if not isinstance(item, dict):
            continue
        shapes = item.get("acceptance") or []
        if len(shapes) < 2 and not item.get("one_shape_because"):
            defects.append(
                f"{item.get('id')}：验收只有 {len(shapes)} 种来源形状；要两种，"
                "或者在 `one_shape_because` 里说清为什么第二种不存在"
            )
    for key in ("input_type", "invariants", "counterexamples"):
        if not plan.get(key):
            defects.append(f"计划缺 `{key}`：这三样是「这一类」的定义，不是可选项")
    return defects


def record_plan(plan_path: str) -> int:
    current = current_round()
    if current is None:
        print("round: 没有开着的一轮。先 `round.py open`。", file=sys.stderr)
        return 1
    import json
    plan = json.loads(pathlib.Path(plan_path).read_text())
    batch = remaining()
    defects = plan_defects(batch, plan)
    if defects:
        print("round: 这份计划不能作为这一批的编辑输入：", file=sys.stderr)
        for d in defects:
            print("  - " + d, file=sys.stderr)
        return 2
    import uuid
    event_id = uuid.uuid4().hex[:26]
    if round_book() == "git":
        repo = git_round_repo()
        found = git_round_payload(repo)
        if found is None or found[1].get("closed_at"):
            print("round: 没有开着的一轮。", file=sys.stderr)
            return 1
        sha, payload = found
        payload["plan"] = plan
        payload["planned_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        write_git_round(repo, payload, sha)
        print(f"round: 计划已记进 git 书（覆盖 {len(batch)} 条）；编辑只认这一份。")
        return 0
    attrs = json.dumps(plan, ensure_ascii=False).replace("'", "''")
    psql(
        "insert into cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, "
        "step, outcome, idempotency_key, causation_id, actor, writer, attributes) values "
        f"('{event_id}','batch_planned','round','{current[0]}','{current[0]}','plan','ok',"
        f"'{event_id}','{current[0]}','{os.environ.get('COGNEVA_ACTOR','worker')}',"
        f"'{os.environ.get('COGNEVA_WRITER','worker')}', '{attrs}')"
    )
    print(f"round: 计划已记进事件（覆盖 {len(batch)} 条）；编辑只认这一份。")
    return 0



def planned() -> int:
    """这一轮有没有一份被接受的计划。0＝有，1＝没有（还没到能编辑的时候）。"""
    current = current_round()
    if current is None:
        print("round: 没有开着的一轮。", file=sys.stderr)
        return 1
    if round_book() == "git":
        found = git_round_payload(git_round_repo())
        if found and found[1].get("plan"):
            return 0
        print(
            "round: 这一轮还没有计划。整批必须先变成编辑的输入："
            "`round.py plan --file <plan.json>`（覆盖全部待办、每条至少两种来源形状）。",
            file=sys.stderr,
        )
        return 1
    out = psql(
        "select count(*) from cogneva_event where kind='batch_planned' "
        f"and subject_id='{current[0]}'"
    ).strip()
    if out.isdigit() and int(out) > 0:
        return 0
    print(
        "round: 这一轮还没有计划。整批必须先变成编辑的输入："
        "`round.py plan --file <plan.json>`（覆盖全部待办、每条至少两种来源形状）。",
        file=sys.stderr,
    )
    return 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=["open", "status", "stop-gate", "close", "rebound", "plan", "planned"],
    )
    parser.add_argument("--source", default=None)
    parser.add_argument("--file", default=None)
    # 只给「校正被写坏的基准」用：重钉本身不改量程，所以正常不该给这个参数。
    parser.add_argument("--code-rev", default=None)
    args = parser.parse_args(argv[1:])
    try:
        if args.action == "open":
            return open_round(args.source)
        if args.action == "rebound":
            # 这一轮没变，变的是它钉的那一版台账（例如台账随后才提交）。重钉是一次
            # **计划修订**：追加一条新的 round_opened，读取方取最新的一条。
            return open_round(args.source, allow_open=True, code_rev_override=args.code_rev)
        if args.action == "plan":
            if not args.file:
                print("round: plan 要一个 --file <plan.json>", file=sys.stderr)
                return 2
            return record_plan(args.file)
        if args.action == "planned":
            return planned()
        if args.action == "status":
            return status()
        if args.action == "stop-gate":
            return stop_gate()
        return close()
    except todo_source.Unreadable as e:
        # 读不到不是「没有」：退 3，调用方分得开这两种情况。
        print(f"round: 这一轮的书读不到 —— {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv))
