#!/usr/bin/env python3
"""库里的界：把树扫不到的那一半也按同一套规矩过一遍。

树普查（`bounds-census.py`）看得见代码与部署清单里的数；住在**数据库里**的界它一条
也看不见。库侧的界有两处：

  1. `pg_settings` 里形如界的设置（时长、上限、容量）；
  2. 平台自己的表里存着的界与记录（`declared_duration_seconds`、`run_deadline_seconds`、
     `segment_skew_bound_seconds`……）。

规矩与树那本一样：默认删，删不掉要写理由；然后按 derived（点名一条已发布读数或一个
导出公式）／declaration（写在某一处部署声明里，改那一处就是改这次部署）／data（记下来
的事实，不是旋钮）／server-default（服务端编译进去、这棵树一个字没动的值）四种之一。

人口是**每次现算**的，不是手抄的名单：

* 设置：`setting ~ '^[0-9]+'` 且（有单位或名字形如 timeout/limit/size/…）。其中
  `source = 'default'` 且值等于编译默认的，归 `server-default`（不是我们的旋钮，但人口
  里要点名，免得「换了个值」被读成「没有这条」）；其余必须在这本书里有一行。
* 列：public schema 里数值类型、名字形如 seconds/_ms/_limit/_size/_ttl/_ratio/_bound/
  _threshold/_cap/_max/_min 的列，每一列必须有一行。

跑法：
    python3 scripts/db-bounds-census.py                 # 打印机读行
    python3 scripts/db-bounds-census.py --check         # 有未登记即非零
    python3 scripts/db-bounds-census.py --settings-file F --columns-file G   # 夹具

连接：默认 `sudo -n kubectl -n cogneva exec postgres-0 -- psql -U cog_user -d cogneva`，
可用 `COGNEVA_DB_CENSUS_PSQL` 换成任意一条能跑 SQL 的命令（宿主上就是 kubectl）。
"""
import argparse
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# 处置不另设一张中央账：它必须写在**这个界自己的声明处**，与树那一本同一条规矩。
#   * 设置：`source` 不是缺省、值也不是编译默认的，必须在 deploy/ 的某个声明里找到
#     `<name>=<value>`（写成别的值＝库与声明分家，同样红）；
#   * 列：名字形如界的数值列，必须在代码里有一行 `// db-column: <表>.<列> — <处置> — <理由>`
#     （写在建表 DDL 或读写它的那一行旁边），`derived` 还要点名来源。
# 测量值形状的列（latency_ms、*_size）按形状就是 data，不需要逐列登记。

DISPOSITIONS = {"derived", "fixed", "not-a-bound"}
BOUND_COLUMN = re.compile(r"(_seconds|_bound|_limit|_ttl|_cap|_threshold)$")
MEASUREMENT_COLUMN = re.compile(r"(latency|_ms$|_size$|^avg_)")
MARKER = re.compile(
    r"(?://|--)\s*db-column:\s*([A-Za-z0-9_.]+)\s*[—-]\s*(derived|fixed|not-a-bound)"
    r"\s*[—-]\s*(.+)$",
    re.MULTILINE,
)

SETTINGS_SQL = """
select name, setting, boot_val, coalesce(unit, '-'), source
  from pg_settings
 where setting ~ '^[0-9]+'
   and (unit is not null
        or name ~ '(timeout|limit|ttl|max_|min_|size|age|keep|retention|interval)')
 order by name
"""

COLUMNS_SQL = """
select table_name || '.' || column_name
  from information_schema.columns
 where table_schema = 'public'
   and data_type in ('integer','bigint','smallint','numeric','double precision','real')
   and column_name ~ '(seconds|_ms|_limit|_size|_ttl|_ratio|_bound|_threshold|_cap|_max|_min)'
 order by 1
"""


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
            "db-bounds-census: 库问不动：%s" % (out.stderr.strip() or out.returncode)
        )
    rows = []
    for line in out.stdout.splitlines():
        line = line.rstrip("\n")
        if not line:
            continue
        rows.append(line.split("|"))
    return rows


def read_rows(path: str) -> list[list[str]]:
    return [line.rstrip("\n").split("|") for line in open(path, encoding="utf-8") if line.strip()]


def markers() -> dict[str, dict]:
    """代码里每一行 `// db-column:` / `-- db-column:`：键是 `表.列`。"""
    out: dict[str, dict] = {}
    for path in sorted((ROOT / "crates").rglob("*.rs")):
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in MARKER.finditer(text):
            target, disposition, reason = match.group(1), match.group(2), match.group(3).strip()
            if disposition == "derived" and reason in ("", "-"):
                raise SystemExit("%s: db-column %s 标 derived 却没点名来源" % (path, target))
            out[target] = {"disposition": disposition, "reason": reason, "path": str(path.relative_to(ROOT))}
    return out


def declared_in_tree() -> dict[str, str]:
    """deploy/ 里写下的 `<name>=<value>`：键 name，值 value。rendered 是生成物，不算声明。"""
    out: dict[str, str] = {}
    for path in sorted((ROOT / "deploy").rglob("*")):
        if not path.is_file() or path.suffix not in (".yaml", ".yml", ".toml", ".conf", ".sh"):
            continue
        if "/rendered/" in str(path):
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in re.finditer(r"\b([a-z_]+)=([0-9]+)\b", text):
            out.setdefault(match.group(1), match.group(2))
    return out


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--settings-file")
    parser.add_argument("--columns-file")
    args = parser.parse_args(argv[1:])

    in_place = markers()
    declared = declared_in_tree()
    settings = (
        read_rows(args.settings_file) if args.settings_file else psql_rows(SETTINGS_SQL)
    )
    columns = (
        read_rows(args.columns_file) if args.columns_file else psql_rows(COLUMNS_SQL)
    )

    problems: list[str] = []
    seen_markers: set[str] = set()
    server_default = 0
    declared_settings = 0
    for row in settings:
        if len(row) < 5:
            problems.append("settings 行的格数不对：%r" % (row,))
            continue
        name, value, boot, unit, source = row[:5]
        # 没被动过：服务端缺省（`source=default`，含它启动时算出来的值与按 rlimit
        # 夹过的值），或文件里只是 initdb 写下的编译默认。
        if source == "default" or (source == "configuration file" and value == boot):
            server_default += 1
            continue
        # 被动过的：必须在 deploy/ 的声明里找到同一个值。值对不上＝库与声明分家。
        if name not in declared:
            problems.append(
                "库设置 %s=%s（source=%s）在 deploy/ 里没有声明：改它的是库，不是这份清单"
                % (name, value, source)
            )
            continue
        if declared[name] != value:
            problems.append(
                "库设置 %s=%s 与 deploy/ 的声明 %s 不一致" % (name, value, declared[name])
            )
            continue
        declared_settings += 1
        print("setting\t%s\t%s\t%s\tdeclaration\tdeploy/ 的声明" % (name, value, unit))

    for row in columns:
        target = row[0]
        if MEASUREMENT_COLUMN.search(target.split(".")[-1]):
            print("column\t%s\t-\tnot-a-bound\t测量值（按名字形状）" % target)
            continue
        if not BOUND_COLUMN.search(target.split(".")[-1]):
            # 名字不像界、也不像测量值：人口收进来了就必须交代它是什么。
            problems.append("数值列 %s 既不像界也不像测量值，需要一行 db-column 判词" % target)
            continue
        entry = in_place.get(target)
        if entry is None:
            problems.append(
                "界形状的库列 %s 没有判词：在它的建表/读写那一行旁边写 `// db-column: %s — derived|data|declaration — 理由`"
                % (target, target)
            )
            continue
        seen_markers.add(target)
        print("column\t%s\t-\t%s\t%s" % (target, entry["disposition"], entry["reason"]))

    for target in in_place:
        if target not in seen_markers and "." in target:
            problems.append("判词 %s 在库的人口里找不到（判词不许比人口大）" % target)

    print(
        "db-bounds\tpopulation_settings=%d\tserver_default=%d\tdeclared_in_tree=%d"
        "\tpopulation_columns=%d\tin_place_markers=%d\tundisposed=%d"
        % (len(settings), server_default, declared_settings, len(columns), len(in_place), len(problems))
    )
    if problems:
        print("未处置 %d 条：" % len(problems), file=sys.stderr)
        for problem in problems:
            print("  " + problem, file=sys.stderr)
        return 1 if args.check else 0
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
