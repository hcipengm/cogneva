#!/usr/bin/env python3
"""一轮的开始与关闭：只存一个坐标，不存清单。

「这一轮从哪一版账开始」是唯一必须留下来的信息。它不进文件、不抄清单——清单抄一份
到别处就多一份会漂的副本，而正本只有台账。所以：

    round.py open      # 记下台账此刻的 git rev（refs/cogneva/round）
    round.py status    # 按那个 rev 读台账，现算还剩哪些
    round.py close     # 全部做完才能删掉这个 ref

坐标住在 git 对象库里（`refs/cogneva/round`），不在工作树上：不随 commit 走、不是文档、
不是第二份状态；时间在 reflog 里（`git reflog show refs/cogneva/round`）。

批次由 `family.py` / `batch-gate.py` 用同一个 ref 现算：读那个 rev 上的台账，取所有
未完成条目。台账在工作过程中怎么变，这一轮的批次都不变——它钉在 rev 上。

**终态**：ref 那一层退休，换成事件流里的一条 `round_opened`（载荷就是同一个 rev）。
今天流还是空的，所以坐标先寄在 git 里。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shlex
import subprocess
import sys

PSQL_ENV = "COGNEVA_EVENT_PSQL"
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import todo_source  # noqa: E402


def ledger_repo(spec: str) -> pathlib.Path | None:
    """`md:<路径>` 时，台账所在的那个 git 仓。change-ledger 没有 git rev，返回 None。"""
    if not spec.startswith("md:"):
        return None
    path = pathlib.Path(spec[3:]).expanduser().resolve()
    out = subprocess.run(
        ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
        capture_output=True, text=True,
    ).stdout.strip()
    return pathlib.Path(out) if out else None


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


def current_round() -> tuple[str, str, str, list[str], str] | None:
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
    path = pathlib.Path(spec[3:]).expanduser().resolve()
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


def open_round(spec: str | None, allow_open: bool = False) -> int:
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
    batch = todo_source.load(f"md:{source[3:]}") if source.startswith("md:") else []
    if batch:
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
    first_rev = _existing[4] if _existing else rev
    attrs_sql = (
        f"jsonb_build_object('ledger_rev', '{rev}', 'ledger_path', '{source[3:]}', "
        f"'code_rev', '{code_rev}', 'batch', '{ids}', 'first_rev', '{first_rev}')::text"
    )
    psql(
        "insert into cogneva_event (event_id, kind, subject_kind, subject_id, unit_of_work, "
        "step, outcome, idempotency_key, actor, writer, attributes) values "
        f"('{event_id}','round_opened','round','{subject}','{subject}','open','ok',"
        f"'{event_id}','{os.environ.get('COGNEVA_ACTOR', 'worker')}',"
        f"'{os.environ.get('COGNEVA_WRITER', 'worker')}',{attrs_sql})"
    )
    print(f"round: 开了一条 round_opened 事件（钉在台账仓 {rev[:12]}）；批次由那个 rev 现算。")
    return 0


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
    rev = current[1]
    left = remaining()
    print(f"round: 事件流里钉在 {rev[:12]}，还剩 {len(left)} 条")
    for i in left:
        print(f"  未完成 {i}")
    return 0 if not left else 1


def close() -> int:
    current = current_round()
    if current is None:
        print("round: 没有开着的一轮。")
        return 1
    if status() != 0:
        print("round: 还有没做完的条目，这一轮关不掉。", file=sys.stderr)
        return 1
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
        "action", choices=["open", "status", "close", "rebound", "plan", "planned"]
    )
    parser.add_argument("--source", default=None)
    parser.add_argument("--file", default=None)
    args = parser.parse_args(argv[1:])
    try:
        if args.action == "open":
            return open_round(args.source)
        if args.action == "rebound":
            # 这一轮没变，变的是它钉的那一版台账（例如台账随后才提交）。重钉是一次
            # **计划修订**：追加一条新的 round_opened，读取方取最新的一条。
            return open_round(args.source, allow_open=True)
        if args.action == "plan":
            if not args.file:
                print("round: plan 要一个 --file <plan.json>", file=sys.stderr)
                return 2
            return record_plan(args.file)
        if args.action == "planned":
            return planned()
        if args.action == "status":
            return status()
        return close()
    except todo_source.Unreadable as e:
        # 读不到不是「没有」：退 3，调用方分得开这两种情况。
        print(f"round: 事件流读不到 —— {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv))
