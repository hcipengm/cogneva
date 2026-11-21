#!/usr/bin/env python3
"""Every bound in a tree, from every surface it can hide on.

「界」＝任何一个「到点或到量就改变行为」的数。它在树上藏在四个地方，只扫其中一个是
不完备的（2026-11-19 实测：第一版只扫了常量，漏了 503 个配置数值与 805 个自报标记）：

  面一 markers     `// cap: bound` / `// cap: fixed` —— 代码自己声明过的界；
                   以及 `// wall:` —— 一次等待结束时世界剩下什么（2026-11-20 加）
  面二 consts      形如界的 `const`（哪怕没标记：没标记本身就是缺陷）
  面三 config      部署配置里的数值叶子 + serde 默认函数
  面四 destroys    `DELETE FROM` / `truncate` / `take(` / `redact_*` —— 越过就毁数据

用法：

    bounds-census.py [--root <仓库根>] [--surface markers|consts|config|destroys|walls] [--tsv]

不带 `--surface` 时四面全扫。输出是 TSV（`--tsv`）或分组清单；末行是各面的计数。
任何工作树、任何仓库都能跑：路径全部相对 `--root`，不认写死的绝对路径。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

BOUNDISH = re.compile(
    r"(CAP|MAX|MIN|LIMIT|BUDGET|WINDOW|TTL|DAYS|SECS|SECONDS|TIMEOUT|ROWS|CHARS|BYTES|"
    r"ATTEMPT|RETRY|RETRIES|BATCH|SIZE|KEEP|RETAIN|PERIOD|INTERVAL|TICK|STALE|SILENCE|"
    r"THRESHOLD|BOUND|DEADLINE|QUOTA|FLOOR|CEILING|SHARE|DIVISOR)",
    re.I,
)
CONST_RE = re.compile(
    r"^\s*(?:pub(?:\([a-z]+\))?\s+)?const\s+([A-Z0-9_]+)\s*:\s*([A-Za-z0-9_:<>]+)\s*=\s*(.+?);\s*$"
)
MARKER_RE = re.compile(r"//\s*cap:\s*(bound|fixed)\s*(?:—|-|:)?\s*(.*)$")
# 第二种自报标记：一次等待回答的三问（在等什么 / 等不到留下什么事件 / 能不能续跑）。
# 它与 `cap:` 是两个问题，一行对一行地待在同一个文档块里——只扫 `cap:` 的那一版把
# 「这个数从哪儿来」当成全部，于是「等不到时世界剩下什么」这条读数根本没进普查。
WALL_RE = re.compile(r"//\s*wall:\s*(.*)$")
SERDE_DEFAULT_RE = re.compile(
    r"fn\s+(default_[a-z0-9_]+)\(\)\s*->\s*([A-Za-z0-9_:<>]+)\s*\{\s*\n\s*([^\n]+)\n"
)
DESTROY_RE = re.compile(r"DELETE FROM|\btruncate\b|\.chars\(\)\.take\(|\.bytes\(\)\.take\(|redact_secrets\(|redact_json_secrets\(")


def rust_files(root: Path):
    for path in sorted((root / "crates").rglob("*.rs")):
        text = str(path)
        if "/tests/" in text or path.name.endswith("_test.rs"):
            continue
        yield path


def surface_markers(root: Path):
    """面一：代码自己声明的界。标记下面那一行通常就是被它注解的那个数。"""
    for path in rust_files(root):
        lines = path.read_text(errors="ignore").splitlines()
        for index, line in enumerate(lines):
            match = MARKER_RE.search(line)
            if not match:
                continue
            kind, why = match.group(1), match.group(2).strip()
            # 标记下面那一行通常就是被它注解的那个数。往下跳过紧跟着的 `// wall:` 与
            # 别的注释：`cap:` 与 `wall:` 在同一个块里是两行，把 wall 那行当成「这个数」
            # 会让普查的 name 列在每一处都印成一条说明。
            j = index + 1
            while j < len(lines) and lines[j].strip().startswith("//"):
                j += 1
            target = lines[j].strip() if j < len(lines) else ""
            yield {
                "surface": "marker",
                "path": str(path.relative_to(root)),
                "line": index + 1,
                "kind": kind,
                "name": target[:120],
                "detail": why[:160],
            }
        for index, line in enumerate(lines):
            match = WALL_RE.search(line)
            if not match:
                continue
            yield {
                "surface": "marker",
                "path": str(path.relative_to(root)),
                "line": index + 1,
                "kind": "wall",
                "name": "",
                "detail": match.group(1).strip()[:200],
            }


def surface_walls(root: Path):
    """第五面：**一次等待有没有答它的三问**。

    与「界从哪儿来」是不同的两件事：`cap:` 说这个数为什么是这个数，`// wall:` 说这条
    等待结束时世界剩下什么。2026-11-21 那一轮把 69 个墙常量全部答完（两道门钉住了后续），
    而普查一直没有这一面——于是「还欠几条」只能靠读那份测试里的棘轮表，读不到就是不知道。

    点名的是**没有答的**：一个名字像墙、初值是数字或 Duration 的常量，前面那块注释里
    没有 `// wall:` 一行。与那道门同一套判据，这里的产出是读数，不是判词。
    """
    import re as _re
    name_re = _re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?const\s+([A-Z0-9_]*(?:TIMEOUT|DEADLINE|WALL)[A-Z0-9_]*)\s*:\s*([A-Za-z0-9_:<>]+)\s*=\s*(.+)$")
    for path in rust_files(root):
        lines = path.read_text(errors="ignore").splitlines()
        for index, line in enumerate(lines):
            match = name_re.match(line)
            if not match:
                continue
            name, ty, value = match.group(1), match.group(2), match.group(3).strip()
            numeric = any(k in ty for k in ("u8", "u16", "u32", "u64", "usize", "i64", "f64", "Duration"))
            starts = value.startswith(("Duration::", "std::time::Duration::")) or (value[:1].isdigit() if value else False)
            if not numeric or not starts:
                continue
            block = "\n".join(lines[max(0, index - 12):index])
            if "// wall:" in block:
                continue
            yield {
                "surface": "wall",
                "path": str(path.relative_to(root)),
                "line": index + 1,
                "kind": "unanswered",
                "name": name,
                "detail": "没有回答「在等什么 / 等不到留下什么事件 / 能不能续跑」",
            }


def surface_consts(root: Path):
    """面二：形如界的常量。没有 `cap:` 标记的单独标出来——那是待补的账。"""
    for path in rust_files(root):
        lines = path.read_text(errors="ignore").splitlines()
        for index, line in enumerate(lines):
            match = CONST_RE.match(line)
            if not match:
                continue
            name, ty, value = match.groups()
            if not BOUNDISH.search(name):
                continue
            marked = index > 0 and MARKER_RE.search(lines[index - 1]) is not None
            yield {
                "surface": "const",
                "path": str(path.relative_to(root)),
                "line": index + 1,
                "kind": "marked" if marked else "unmarked",
                "name": name,
                "detail": value.strip()[:120],
            }


def surface_config(root: Path):
    """面三：配置里人填的数。serde 默认函数与部署配置的数值叶子。"""
    for path in rust_files(root):
        text = path.read_text(errors="ignore")
        for match in SERDE_DEFAULT_RE.finditer(text):
            name, ty, body = match.groups()
            yield {
                "surface": "config",
                "path": str(path.relative_to(root)),
                "line": text[: match.start()].count("\n") + 1,
                "kind": "serde-default",
                "name": name,
                "detail": f"{ty} = {body.strip()[:100]}",
            }

    config = root / "deploy/helm/cogneva/files/cogneva.json"
    if not config.exists():
        return
    data = json.loads(config.read_text())

    def walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                yield from walk(value, path + [key])
        elif isinstance(node, list):
            for index, value in enumerate(node):
                yield from walk(value, path + [str(index)])
        elif isinstance(node, (int, float)) and not isinstance(node, bool):
            key = ".".join(path)
            yield {
                "surface": "config",
                "path": str(config.relative_to(root)),
                "line": 0,
                "kind": "deployment-config",
                "name": key,
                "detail": str(node),
            }

    yield from walk(data, [])


def surface_destroys(root: Path):
    """面四：越过就毁数据的语句。"""
    roots = [root / "crates", root / "deploy", root / "scripts", root / ".github"]
    for base in roots:
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix not in {".rs", ".sh", ".py", ".sql", ".yaml", ".yml"}:
                continue
            if "/rendered/" in str(path):
                continue
            try:
                lines = path.read_text(errors="ignore").splitlines()
            except OSError:
                continue
            for index, line in enumerate(lines):
                if not DESTROY_RE.search(line):
                    continue
                if line.lstrip().startswith("//") or line.lstrip().startswith("#"):
                    continue
                yield {
                    "surface": "destroy",
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "kind": "destroy",
                    "name": line.strip()[:120],
                    "detail": "",
                }


def surface_state_files(root: Path):
    """第六面：**盘上还有没有「每个 id 一份文件」的记录**。

    这是「两份记录必然分叉」那条的普查面：一处把状态写进 `<id>.json`，就有第二份正本，
    而分叉时没人知道哪份对。终态是盘上不再有这类记录文件——计数必须归零。

    判的是**写法**不是文件名：`format!("{...}.json")` / `join(format!(...json))` 这类
    「拼出来的文件名」，以及 `.json` 字面量配上 `write` / `create` 的写路径。一个固定的
    `config.json` 不是这一类（它不是「每个 id 一份」），所以只有拼出来的才算。
    """
    import re as _re
    dynamic = _re.compile(r'format!\(\s*"[^"]*\{[^"]*\.json"')
    fixed_write = _re.compile(r'(write|create|File::create)\s*\([^;]{0,120}\.json')
    for path in rust_files(root):
        lines = path.read_text(errors="ignore").splitlines()
        for index, line in enumerate(lines):
            if dynamic.search(line) or fixed_write.search(line):
                yield {
                    "surface": "state-file",
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "kind": "record-on-disk",
                    "name": line.strip()[:80],
                    "detail": "盘上又一份记录：分叉时没人知道哪份对",
                }


def surface_facts_without_events(root: Path):
    """第七面：**有表、没有对应事件的事实面**。

    终态是「每一种事实先落一条事件，表是派生量」。这条面把 `CREATE TABLE` 出来的表名
    收上来，再减去「表名里带 `event`／`snapshot`／`_current`／`cap`」的那几类（事件本身、
    快照、投影、读数），剩下的每一个都是一条待对账的事实面。计数不归零，就说明还有事实
    只在表里有一份。
    """
    import re as _re
    create = _re.compile(r'CREATE TABLE IF NOT EXISTS\s+([a-z0-9_]+)', _re.I)
    derived = ("event", "snapshot", "current", "_cap", "rollup", "audit_log")
    seen: dict[str, str] = {}
    for path in rust_files(root):
        for match in create.finditer(path.read_text(errors="ignore")):
            table = match.group(1)
            seen.setdefault(table, str(path.relative_to(root)))
    for table, path in sorted(seen.items()):
        if any(mark in table for mark in derived):
            continue
        yield {
            "surface": "fact-table",
            "path": path,
            "line": 0,
            "kind": "no-event",
            "name": table,
            "detail": "这张表还没有对应的 event kind：它是正本还是派生量，读的人分不出",
        }


def surface_index_rows(root: Path):
    """面：**纯索引行**——一行里没有可判的终态，只说「见某处」。

    索引行与正文分处两地时，两边会分叉（正文改了、索引没改），而分叉没人看得出。
    终态是这种行不再存在：每条待办自己说得出怎么判，而不是指向别处。

    台账由 `--ledger` 给（默认这台机器那一本）；读不到就一条都不产出——读不到不是 0。
    """
    import os
    ledger = os.environ.get("COGNEVA_TODO_LEDGER")
    if not ledger:
        host = Path.home() / ".config/cogneva/todo-ledger"
        if host.exists():
            first = host.read_text(errors="ignore").strip().splitlines()
            ledger = first[0].strip() if first else None
    if not ledger or not Path(ledger).is_file():
        return
    for number, line in enumerate(Path(ledger).read_text(errors="ignore").splitlines(), 1):
        if not line.startswith("| S"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 3:
            continue
        body = cells[1]
        is_index = ("索引" in body) or ("见 §" in body) or ("见第" in body)
        if is_index and "判据读数" not in line:
            yield {
                "surface": "index-row",
                "path": f"{Path(ledger).name}:{number}",
                "line": number,
                "kind": "no-own-criterion",
                "name": cells[0],
                "detail": "这一行只说「见某处」，自己说不出怎么判",
            }


SURFACES = {
    "markers": surface_markers,
    "index-rows": surface_index_rows,
    "state-files": surface_state_files,
    "facts-without-events": surface_facts_without_events,
    "walls": surface_walls,
    "consts": surface_consts,
    "config": surface_config,
    "destroys": surface_destroys,
}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--surface", choices=sorted(SURFACES), default=None)
    parser.add_argument("--tsv", action="store_true")
    args = parser.parse_args(argv[1:])

    root = Path(args.root).resolve()
    if not (root / "crates").exists():
        print(f"FAIL: {root} 不像一个 cogneva 检出（没有 crates/）", file=sys.stderr)
        return 2

    chosen = [args.surface] if args.surface else list(SURFACES)
    rows: list[dict] = []
    for name in chosen:
        for row in SURFACES[name](root):
            row["surface_name"] = name
            rows.append(row)

    if args.tsv:
        print("surface\tkind\tpath\tline\tname\tdetail")
        for row in rows:
            print(
                f"{row['surface']}\t{row['kind']}\t{row['path']}\t{row['line']}\t"
                f"{row['name']}\t{row['detail']}"
            )
    else:
        for surface in chosen:
            selected = [row for row in rows if row["surface_name"] == surface]
            print(f"== {surface}: {len(selected)}")
            for row in selected[:40]:
                print(f"  {row['path']}:{row['line']}\t{row['kind']}\t{row['name']}")
            if len(selected) > 40:
                print(f"  … 还有 {len(selected) - 40} 条（--tsv 看全）")

    for surface in chosen:
        selected = [row for row in rows if row["surface_name"] == surface]
        kinds: dict[str, int] = {}
        for row in selected:
            kinds[row["kind"]] = kinds.get(row["kind"], 0) + 1
        summary = " ".join(f"{k}={v}" for k, v in sorted(kinds.items()))
        print(f"{surface}\t{len(selected)}\t{summary}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
