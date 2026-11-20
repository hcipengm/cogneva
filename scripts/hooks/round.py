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


def current_round() -> tuple[str, str, str] | None:
    """(event_id, ledger_rev)；没有开着的一轮返回 None。

    「开着」＝流里最后一条 `round_opened` 没有配对的 `round_closed`（按 subject_id 配）。
    """
    sql = (
        "select o.event_id || E'\\t' || (o.attributes::jsonb->>'ledger_rev') "
        "|| E'\\t' || coalesce(o.attributes::jsonb->>'code_rev','') "
        "from cogneva_event o where o.kind='round_opened' and not exists ("
        " select 1 from cogneva_event c where c.kind='round_closed'"
        " and c.subject_id=o.subject_id) order by o.at desc, o.event_id desc limit 1"
    )
    out = psql(sql).strip()
    if not out:
        return None
    parts = out.split("\t")
    event_id, rev = parts[0], parts[1]
    code_rev = parts[2] if len(parts) > 2 else ""
    return event_id, rev, code_rev


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
    tmp = pathlib.Path("/tmp") / f"cogneva-ledger-{rev[:12]}.md"
    tmp.write_text(blob.stdout)
    return tmp


def open_round(spec: str | None, allow_open: bool = False) -> int:
    if current_round() and not allow_open:
        print("round: 已经有一轮开着。先 `round.py close`。")
        return 1
    source = spec or todo_source.default_spec()
    repo = ledger_repo(source)
    if repo is None:
        print("round: 这个来源没有 git rev（change-ledger 要按事件流记账），开不了轮。",
              file=sys.stderr)
        return 2
    rev = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    if not rev:
        print("round: 台账所在仓没有 HEAD，开不了轮。", file=sys.stderr)
        return 2
    import json
    import os
    import uuid
    event_id = uuid.uuid4().hex[:26]
    subject = f"round-{event_id}"
    code_rev = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    attrs_sql = (
        f"jsonb_build_object('ledger_rev', '{rev}', 'ledger_path', '{source[3:]}', "
        f"'code_rev', '{code_rev}')::text"
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
    current = current_round()
    if current is None:
        return []
    rev = current[1]
    source = todo_source.default_spec()
    path = ledger_at(rev, source)
    return [i.id for i in todo_source.load(f"md:{path}")]


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


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["open", "status", "close", "rebound"])
    parser.add_argument("--source", default=None)
    args = parser.parse_args(argv[1:])
    try:
        if args.action == "open":
            return open_round(args.source)
        if args.action == "rebound":
            # 这一轮没变，变的是它钉的那一版台账（例如台账随后才提交）。重钉是一次
            # **计划修订**：追加一条新的 round_opened，读取方取最新的一条。
            return open_round(args.source, allow_open=True)
        if args.action == "status":
            return status()
        return close()
    except todo_source.Unreadable as e:
        # 读不到不是「没有」：退 3，调用方分得开这两种情况。
        print(f"round: 事件流读不到 —— {e}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv))
