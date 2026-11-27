#!/usr/bin/env python3
"""表的三种形状读一遍：事实／投影／权威文档——并问权威文档一句：**为什么不能由流重算**。

最优形状只有两种：
  * **投影**：内容是事件的函数（水位、重建命令、重建测试三样齐）——平台自己算出来的东西
    全归这一类；
  * **外部输入**：流里没有、也算不出来的那几样（凭证散列、外部身份、并发原语当前持有者
    这类"行本身就是真值"的东西）——每一张都要写出它持有什么是流给不出的。

第三种「权威文档」只在这两句之间成立：它要么其实是个投影（现在是"当前值、历史在流里"
的说法），要么是外部输入。判据：`Document { why }` 里必须点名**流里没有的那份输入**；
只写"读者问的是现在"不算——那正是投影也被问到的话，`task_current` 就是这么做的。

这份普查把现状读出来并按这条判据分类，给台账一张转换清单；新增的文档用
`--check-staged` 在提交时判（存量不拦，清单在台账里）。

用法：
    python3 scripts/table-shapes-census.py            # 清单
    python3 scripts/table-shapes-census.py --check-staged   # 只判这一笔新增的 Document
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SHAPES = ROOT / "crates/cog-storage/src/table_shapes.rs"

# 表的终态分类**已经有一本正本**：`TABLE_CENSUS`（Stream / Projection / Config / Legacy）。
# 这份脚本不再另造一套判断，只把两件事读出来：
#   1. 同一张表同时是「只追加的事实」与「会被 UPDATE 的文档」（两张表各自的门看不见交叉）；
#   2. `Document` 行在正本里属于哪一类：Legacy（带 successor，退休清单）／Projection／Config／
#      正本里没有（需要当场回答「它是外部输入还是遗漏」）。
CENSUS_CLASSES = ("Stream", "Projection", "Config", "Legacy")

def fact_tables(text: str) -> list[str]:
    i = text.find("pub const FACT_TABLES")
    if i < 0:
        return []
    block = text[i : text.index("\n];", i)]
    return re.findall(r'^\s*"([a-z0-9_]+)",', block, re.M)


def census_classes(text: str) -> dict[str, tuple[str, str]]:
    """TABLE_CENSUS 的每一行：表名 -> (类别, successor/why 片段)。"""
    i = text.find("&[CensusRow]")
    if i < 0:
        return {}
    body = text[i:]
    body = body[: body.find("\n];")]
    rows = re.findall(
        r'table: "([a-z0-9_]+)",\s*class: CensusClass::([A-Za-z]+)'
        r'(?:\s*\{\s*successor: (?:Some\("([^"]*)"\)|None)[^}]*\})?',
        body,
    )
    return {table: (kind, successor or "") for table, kind, successor in rows}


def documents(text: str) -> list[tuple[str, str]]:
    block = text[text.index("pub const MUTABLE_TABLES") :]
    block = block[: block.index("\n];")]
    return re.findall(
        r'table: "([a-z0-9_]+)",\s*shape: TableShape::Document \{\s*why: "([^"]*)"', block
    )


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-staged", action="store_true")
    parser.add_argument("--text", help="读这份源码而不是工作树（夹具用）")
    args = parser.parse_args(argv[1:])

    text = Path(args.text).read_text(encoding="utf-8") if args.text else SHAPES.read_text(encoding="utf-8")
    rows = documents(text)
    classes = census_classes(text)
    # 同一张表不能既是「只追加的事实」又是「会被 UPDATE 的文档」：两边的门各看一边，
    # 交叉处谁也看不见。这一格把它印出来（存量，不拦提交）。
    overlap = sorted(set(fact_tables(text)) & {t for t, _ in rows})
    for table in overlap:
        print("fact-and-mutable\t%s\t同表出现在 FACT_TABLES 与 MUTABLE_TABLES" % table)
    if overlap:
        print("table-shapes-overlap\t%d\t%s" % (len(overlap), ",".join(overlap)))

    buckets = {"Legacy": [], "Projection": [], "Config": [], "unclassified": []}
    for table, _why in rows:
        kind, successor = classes.get(table, ("", ""))
        if kind not in CENSUS_CLASSES:
            buckets["unclassified"].append(table)
            print("document-unclassified\t%s\t正本里没有这一行：它要么是外部输入（写明流里没有的那份输入），要么是漏登记" % table)
            continue
        buckets[kind].append(table)
        suffix = (" -> " + successor) if successor else ""
        print("document-%s\t%s%s" % (kind.lower(), table, suffix))
    print(
        "documents=%d\tlegacy_to_retire=%d\tprojection=%d\tconfig=%d\tunclassified=%d"
        % (len(rows), len(buckets["Legacy"]), len(buckets["Projection"]), len(buckets["Config"]), len(buckets["unclassified"]))
    )

    if not args.check_staged:
        return 0

    # 只判这一笔新增的 Document 行：存量是台账里的转换清单，不在这里拦。
    staged = subprocess.run(
        ["git", "-C", str(ROOT), "diff", "--cached", "--unified=0", "--", str(SHAPES.relative_to(ROOT))],
        capture_output=True,
        text=True,
    ).stdout
    added = [line for line in staged.splitlines() if line.startswith("+") and "TableShape::Document" in line]
    if not added:
        return 0
    # 新增的文档必须和它的 why 一起出现（同一条 MUTABLE_TABLES 项里）；这里按整份文件判
    # 新增的表名：它们在文档清单里，就必须写得出外部输入。
    bad = [table for table, why in convertible if table in staged]
    if bad:
        print(
            "新增的权威文档没有写出「流里没有的那份输入」：%s\n"
            "  要么把它做成投影（水位＋重建命令＋重建测试），要么在 why 里点名那份输入。" % ", ".join(bad),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
