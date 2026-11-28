#!/usr/bin/env python3
"""库里的有损面：树普查看不见的那一半。

`loss-census.py` 扫代码里的截断／丢弃／删文件／静默回退；库里有三类丢数据根本不经过那些
语句，因此不在它的入口里：

  1. **外键的引用动作**：`ON DELETE CASCADE`／`SET NULL`／`SET DEFAULT` 在父行被删时
     悄悄带走或改掉子行——DDL 写在迁移里（历史，不进树普查），删父行的那条 `DELETE`
     本身看上去只是删了一行；
  2. **分区子表的摘除**：维护器按时把老分区 `DROP` 掉，行不是被谁删的，是容器没了；
  3. 保留期清扫只数代码里的 `DELETE`；库这一侧能问的是「这张表此刻有多少行已经超过
     它自己声明的保留窗口」——这一格留给读数（`db-pressure` 的每表最老一行），不在这里
     判，因为判它需要每一张表的窗口来源。

判据与树那本同一条：**默认无损**。真要有损，必须写出理由，而且理由必须是「不丢会让
性能／资源／容量撑不住」——「省事」「先这样」不是理由；理由写在那个约束自己的声明处
（建表 DDL 或声明表形状的那一行旁边），形如：

    // db-loss: <表>.<约束> — loss — 不丢会让 …

`keep` 表示它其实不丢（例如引用动作被改成 RESTRICT 之前的过渡）；不带理由的候选一律
判红，人口每次现算。

用法：
    python3 scripts/db-loss-census.py            # 打印机读行
    python3 scripts/db-loss-census.py --check    # 有未处置即非零
    python3 scripts/db-loss-census.py --fks-file F --partitions-file G   # 夹具
"""
import argparse
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

FK_SQL = """
select conrelid::regclass::text || '.' || conname,
       confdeltype, confupdtype
  from pg_constraint
 where contype = 'f'
   and (confdeltype in ('c', 'n', 'd') or confupdtype in ('c', 'n'))
 order by 1
"""

PARTITION_SQL = """
select c.relname
  from pg_partitioned_table p
  join pg_class c on c.oid = p.partrelid
 order by 1
"""

MARKER = re.compile(
    r"(?://|--)\s*db-loss:\s*([A-Za-z0-9_.]+)\s*[—-]\s*(loss|keep)\s*[—-]\s*(.*)$",
    re.MULTILINE,
)
# 有损的理由必须回答同一问：**不丢**会坏成什么（性能或功能）。
RECOVERABLE = ("不丢",)


def psql_rows(sql: str) -> list[list[str]]:
    cmd = os.environ.get("COGNEVA_DB_CENSUS_PSQL", "").strip()
    if cmd:
        argv = shlex.split(cmd) + ["-tA", "-F", "|", "-c", sql]
    else:
        argv = [
            "sudo", "-n", "kubectl", "-n", "cogneva", "exec", "postgres-0", "--",
            "psql", "-U", "cog_user", "-d", "cogneva", "-tA", "-F", "|", "-c", sql,
        ]
    out = subprocess.run(argv, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(
            "db-loss-census: 库问不动：%s" % (out.stderr.strip() or out.returncode)
        )
    return [line.split("|") for line in out.stdout.splitlines() if line.strip()]


def read_rows(path: str) -> list[list[str]]:
    return [line.rstrip("\n").split("|") for line in open(path, encoding="utf-8") if line.strip()]


def markers() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in MARKER.finditer(text):
            target, disposition, reason = match.group(1), match.group(2), match.group(3).strip()
            if not reason:
                raise SystemExit("%s: db-loss %s 没有理由" % (path, target))
            out[target] = {"disposition": disposition, "reason": reason,
                           "path": str(path.relative_to(ROOT))}
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--fks-file")
    parser.add_argument("--partitions-file")
    args = parser.parse_args(argv[1:])

    in_place = markers()
    fks = read_rows(args.fks_file) if args.fks_file else psql_rows(FK_SQL)
    partitions = (
        read_rows(args.partitions_file) if args.partitions_file else psql_rows(PARTITION_SQL)
    )

    problems: list[str] = []
    seen: set[str] = set()

    for row in fks:
        target, del_action, upd_action = row[0], row[1], row[2]
        action = {"c": "CASCADE", "n": "SET NULL", "d": "SET DEFAULT"}.get(del_action)
        if action is None:
            action = {"c": "CASCADE", "n": "SET NULL"}.get(upd_action, "?")
        entry = in_place.get(target)
        if entry is None:
            problems.append(
                "外键 %s 的引用动作是 %s：父行一动，子行被悄悄带走或改掉，而没有一条 db-loss 判词"
                % (target, action)
            )
            continue
        seen.add(target)
        if entry["disposition"] == "loss":
            if not any(word in entry["reason"] for word in RECOVERABLE):
                problems.append(
                    "外键 %s 判成 loss，但理由没说「不丢会撑不住什么」：%s" % (target, entry["reason"])
                )
                continue
        print("fk\t%s\t%s\t%s\t%s" % (target, action, entry["disposition"], entry["reason"]))

    for row in partitions:
        target = row[0]
        entry = in_place.get(target)
        if entry is None:
            problems.append(
                "分区表 %s 的老分区会被 DROP：行不是被删的，是容器没了，而没有一条 db-loss 判词"
                % target
            )
            continue
        seen.add(target)
        if entry["disposition"] == "loss" and not any(
            word in entry["reason"] for word in RECOVERABLE
        ):
            problems.append("分区表 %s 判成 loss，但理由没说「不丢会撑不住什么」" % target)
            continue
        print("partition\t%s\tDROP\t%s\t%s" % (target, entry["disposition"], entry["reason"]))

    for target in in_place:
        if target not in seen:
            problems.append("判词 %s 在库的人口里找不到（判词不许比人口大）" % target)

    print(
        "db-loss\tpopulation_fks=%d\tpopulation_partitions=%d\tmarkers=%d\tundisposed=%d"
        % (len(fks), len(partitions), len(in_place), len(problems))
    )
    if problems:
        print("未处置 %d 条：" % len(problems), file=sys.stderr)
        for problem in problems:
            print("  " + problem, file=sys.stderr)
        return 1 if args.check else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
