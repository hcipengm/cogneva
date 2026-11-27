# census-keep: shapes=truncations — 工具输出里的列宽与短摘要：完整值就在源文件/行号里（每行已带 path:line），删掉只会让一份报告撑满终端；由源文件本身兜住
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
import functools
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
        skip = _cfg_test_line_set(lines)
        for index, line in enumerate(lines):
            if index in skip:
                continue
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
            # 只有函数体里带**数字字面量**才算一个新的叶子：函数体只是一个具名常量
            # （`DEFAULT_REDIS_READ_BLOCK_MS`）时，那个数已经在那条常量上判过；
            # 返回 true/false 的也不是数。否则同一笔欠账会被数两遍。
            code = body.split("//")[0]
            if not any(ch.isdigit() and (i == 0 or not (code[i-1].isalnum() or code[i-1] in "_."))
                       for i, ch in enumerate(code)):
                continue
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
    「拼出来的文件名」，以及写路径里**带占位符**的那个 `format!`。一个固定的
    `config.json`（或测试夹具里那些 `broken.json`）不是这一类——它不是「每个 id 一份」，
    所以只有拼出来的才算；按 `.json` 一刀切会把它们一起数进来。
    """
    import re as _re
    dynamic = _re.compile(r'format!\(\s*"[^"]*\{[^"]*\.json"')
    # 写路径也要**带占位符**才算：`write(dir.join("config.json"))` 是一个固定名字，
    # 它不是「每个 id 一份」的那一类，按 `.json` 一刀切会把它数进来（2026-11-23 由
    # `bounds-census-shapes.test.sh` 那一格钉出来：面自己的判词与它自己的实现不一致）。
    dynamic_write = _re.compile(
        r'(write|create|File::create)\s*\([^;]{0,160}format!\([^)]*\{[^)]*\.json'
    )
    for path in rust_files(root):
        lines = path.read_text(errors="ignore").splitlines()
        for index, line in enumerate(lines):
            if dynamic.search(line) or dynamic_write.search(line):
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
        if not is_index or "判据读数" in line:
            continue
        # 容器行（只指向子项）本身没有独立终态，这不丢人；真正丢人的是**指向的东西
        # 不存在**——索引与正文分叉。所以数的是「点了名却找不到行」的那些 id。
        import re as _re
        named = set(_re.findall(r"S\d+(?:\.\d+)?", body))
        ledger_ids = set()
        for row in Path(ledger).read_text(errors="ignore").splitlines():
            if row.startswith("| S"):
                ledger_ids.add(row.split("|")[1].strip())
        dangling = sorted(n for n in named if n != cells[0] and n not in ledger_ids)
        if dangling:
            yield {
                "surface": "index-row",
                "path": f"{Path(ledger).name}:{number}",
                "line": number,
                "kind": "no-own-criterion",
                "name": cells[0],
                "detail": f"索引点名了不存在的行：{', '.join(dangling)}",
            }


# ─── 全量形状（2026-11-26 扩） ─────────────────────────────────────────────
#
# 目标：宁可多找、不可漏。每条候选都带一个处置状态；退出码按「有未处置」判。
# 处置来自源码本身：`cap: derived/fixed`、`not-a-bound`、`Preview::new(`、
# `redact_body(`，以及可重建投影登记表。没有处置的候选一律算未处置。

KNOWN_EXT = {
    ".rs", ".sh", ".py", ".sql", ".yaml", ".yml", ".toml", ".js", ".ts", ".tsx",
    ".json", ".env", ".ini", ".properties", ".proto", ".xml", ".md", ".html", ".css",
}
CODE_DIRS = ("crates", "deploy", "scripts", ".github", "web")
SKIP_PARTS = (
    "/tests/", "/rendered/", "/node_modules/", "/target/", "/__pycache__/",
    # 构建产物：不是源码，也不是声明。
    "/dist/",
    # 迁移是历史：一条已经跑过的 `LIMIT`／`INTERVAL` 不是今天的旋钮，改不了也不该改。
    # 它进清单只会把「存量」读大，读的人还得逐条确认它不可动。
    "/migrations/",
)

CLOCK_RE = re.compile(
    r"Duration::from_(?:secs|millis|micros|nanos)\(\s*[0-9A-Za-z_]|"
    r"\bsleep(?:_until)?\(\s*[0-9A-Za-z_]|\binterval\(\s*[0-9A-Za-z_]|"
    r"\btimeout\(\s*[0-9A-Za-z_]|\bsetTimeout\(|\bsetInterval\(|"
    r"\b[A-Za-z0-9_]*(?:secs|millis|seconds|interval|timeout|deadline|ttl|lease|backoff)"
    r"[A-Za-z0-9_]*\s*[:=]\s*(?:\d|[A-Z][A-Z0-9_]{2,}\b|Duration::|env_|self\.|Some\()",
    re.I,
)
# 空间界只认「构造或比较」：一次带参的 take/capacity/clamp，或一个与数字的比较。
# 只出现字段名（`_chars`、`_limit`）不是界——那是类型声明，收紧掉这批误报。
# `take(`／`slice(` 是**截断**（有损），由 loss-census 判，不在界这边重复计数；
# 这里只认容量与比较：带容量的构造、clamp、与数字比较的长度/上限。
# 空间面只认**容量构造**：`with_capacity(N)` 与 `cap/limit: N` 这类分配上限。
# `.clamp(0.0, 1.0)` 是值域归一、`count > 0` 是空判、`i+1 < len` 是索引守卫——
# 它们不是「到量就改变行为」的界，按定义排除。
SPACE_RE = re.compile(
    r"with_capacity\(\s*\d|\b(?:cap|capacity|limit|max_items|batch_size)\s*[:=]\s*\d",
    re.I,
)
TRUNC_RE = re.compile(
    r"\.take\(|\btruncate\(|\.truncate\(|\[\s*\.\.\s*\d+\s*\]|\bsplit_at\(|"
    r"\bhead\s+-n\b|\bslice\(\s*0\s*,|\.substring\(|\.substr\(|\[\s*:\s*\d+\s*\]|"
    r"\$\{[a-zA-Z_][a-zA-Z0-9_]*:\s*0\s*:"
)
DISCARD_RE = re.compile(
    r"\.remove\(|\.retain\(|\.drain\(|\.evict|\bMAXLEN\b|\bEXPIRE\b|\.pop\(|\bprune\b|"
    r"\bmem::take\(|\bdel\s+[a-z_]|\bUNLINK\b|\bXTRIM\b|\.clear\(|\bresize\(|"
    r"\bttlSecondsAfterFinished\b|\blifecycle\b",
    re.I,
)
NARROW_RE = re.compile(r"\bas\s+(?:u8|u16|u32|i8|i16|i32|f32)\b")
SILENT_RE = re.compile(r"let\s+_\s*=|\bok\(\)\s*;|\.unwrap_or_default\(\)|\.unwrap_or\(|\bif\s+let\s+Ok\(_\)")
YAML_NUM_RE = re.compile(
    r"^\s*[A-Za-z0-9_.-]+:\s*(\d+(?:\.\d+)?(?:m|Mi|Gi|Ki|ms|s|h|d)?)\s*(?:#.*)?$"
)
# SQL 的界只认「带数字/参数的窗口与长度」；DROP/DELETE 属于有损面，由 loss-census 判。
SQL_DDL_RE = re.compile(
    r"\bLIMIT\s+(?:\d|\$)|\bINTERVAL\s+'|\bVARCHAR\s*\(\s*\d|\bNUMERIC\s*\(\s*\d|"
    r"\bPARTITION\s+BY\s+RANGE|\bDEFAULT\s+\d+",
    re.I,
)


def _code_part(line: str, suffix: str) -> str:
    """去掉行内注释，但不切字符串里的分隔符。

    `"https://example"` 里的 `//` 不是注释；按裸 `//` 切会把这一行剩下的代码
    全扔掉，那一行上的界就静默漏了。所以 Rust/JS 走一个认字符串的扫描器。
    """
    if suffix in (".rs", ".js", ".ts", ".tsx"):
        out = []
        in_string = False
        escaped = False
        at = 0
        while at < len(line):
            ch = line[at]
            if escaped:
                out.append(ch)
                escaped = False
                at += 1
                continue
            if in_string and ch == "\\":
                out.append(ch)
                escaped = True
                at += 1
                continue
            if ch == '"':
                in_string = not in_string
                out.append(ch)
                at += 1
                continue
            if not in_string and line.startswith("//", at):
                break
            out.append(ch)
            at += 1
        return "".join(out)
    if suffix in (".sh", ".py", ".toml", ".yaml", ".yml"):
        return line.split("#")[0]
    if suffix == ".sql":
        return line.split("--")[0]
    return line


def _brace_deltas(lines, suffix):
    """每一行在「代码态」里的花括号增减，以及这一行是不是有代码。

    只按行切引号数不清块：Rust 里有跨行字符串、`r#"…"#` 原样串、字符字面量
    `'{'` 与生命周期 `'a`，任何一种都会把「这一行有几个 `{`」数错；数错的方向
    是提前退出 `#[cfg(test)]` 块，把整段用例当成产品代码再普查一遍（宁可多不
    可漏的那一侧，但那份量是噪声）。所以状态跨行保留，只数真正在代码态里的括号。
    """
    deltas = [0] * len(lines)
    has_code = [False] * len(lines)
    if suffix != ".rs":
        for index, line in enumerate(lines):
            code = _code_part(line, suffix)
            has_code[index] = bool(code.strip())
            deltas[index] = code.count("{") - code.count("}")
        return deltas, has_code
    mode = "code"  # code | line_comment | block_comment | string | raw | char
    block_depth = 0
    hashes = 0
    for index, line in enumerate(lines):
        at = 0
        while at < len(line):
            ch = line[at]
            if mode == "code":
                if line.startswith("//", at):
                    mode = "line_comment"
                    break
                if line.startswith("/*", at):
                    mode = "block_comment"
                    block_depth = 1
                    at += 2
                    continue
                if ch == '"':
                    mode = "string"
                    at += 1
                    continue
                if ch == "r" and re.match(r'r#*"', line[at:at + 40]):
                    match = re.match(r'r(#*)"', line[at:])
                    hashes = len(match.group(1))
                    mode = "raw"
                    at += len(match.group(0))
                    continue
                if ch == "'":
                    # 字符字面量才进 char 态；`'a` 这类生命周期不进。
                    if re.match(r"'(?:\\.|[^\\'])'", line[at:]):
                        mode = "char"
                        at += 1
                        continue
                    at += 1
                    continue
                if ch == "{":
                    deltas[index] += 1
                elif ch == "}":
                    deltas[index] -= 1
                elif not ch.isspace():
                    has_code[index] = True
                at += 1
                continue
            if mode == "line_comment":
                break
            if mode == "block_comment":
                if line.startswith("/*", at):
                    block_depth += 1
                    at += 2
                    continue
                if line.startswith("*/", at):
                    block_depth -= 1
                    at += 2
                    if block_depth == 0:
                        mode = "code"
                    continue
                at += 1
                continue
            if mode == "string":
                if ch == "\\":
                    at += 2
                    continue
                if ch == '"':
                    mode = "code"
                at += 1
                continue
            if mode == "raw":
                closing = '"' + "#" * hashes
                if line.startswith(closing, at):
                    mode = "code"
                    at += len(closing)
                    continue
                at += 1
                continue
            if mode == "char":
                if ch == "\\":
                    at += 2
                    continue
                if ch == "'":
                    mode = "code"
                at += 1
                continue
        if mode == "line_comment":
            mode = "code"
        if mode == "char":
            mode = "code"
    return deltas, has_code


def code_lines(path: Path, suffix: str):
    """一文件一遍：跳过 `#[cfg(test)]` 块与注释，产出（行号，代码）。"""
    try:
        lines = path.read_text(errors="ignore").splitlines()
    except OSError:
        return
    deltas, has_code = _brace_deltas(lines, suffix)
    inside = False
    opened = False
    depth = 0
    for index, line in enumerate(lines):
        stripped = line.strip()
        if not inside and stripped.startswith("#[cfg(test)]"):
            inside = True
            opened = False
            depth = 0
        if inside:
            depth += deltas[index]
            if depth > 0:
                opened = True
            if opened and depth <= 0:
                inside = False
            continue
        code = _code_part(line, suffix)
        if not code.strip():
            continue
        yield index, code

def source_files(root: Path):
    """封闭的文件集合：认识的扩展名逐个产出；不认识的在代码目录里也要报出来。"""
    for base in CODE_DIRS:
        directory = root / base
        if not directory.exists():
            continue
        for path in sorted(directory.rglob("*")):
            if not path.is_file():
                continue
            rel = "/" + str(path.relative_to(root))
            if any(part in rel for part in SKIP_PARTS):
                continue
            if path.name.endswith(("_test.rs", ".test.ts", ".test.tsx", ".spec.ts")):
                continue
            yield path, path.suffix


def surface_unknown(root: Path):
    for path, suffix in source_files(root):
        if suffix == "" and path.name in (
            "Dockerfile", "Makefile", "Jenkinsfile", "Procfile", "Gemfile",
        ):
            yield {
                "surface": "unknown",
                "kind": "no-criterion",
                "path": str(path.relative_to(root)),
                "line": 1,
                "name": path.name,
                "detail": "无扩展名的脚本/构建文件还没有判据：它同样能写死期限与上限",
            }
            continue
        if suffix and suffix not in KNOWN_EXT:
            yield {
                "surface": "unknown",
                "kind": "no-criterion",
                "path": str(path.relative_to(root)),
                "line": 1,
                "name": path.name[:120],
                "detail": "这一类文件还没有判据：先给它一张形状表，再谈覆盖",
            }


def _cfg_test_line_set(lines: list[str]) -> set[int]:
    """文件内 `#[cfg(test)]` 块覆盖的行号：夹具里的常量与标记不是界。"""
    inside = False
    depth = 0
    out: set[int] = set()
    for i, line in enumerate(lines):
        if not inside and line.strip().startswith("#[cfg(test)]"):
            inside = True
            depth = 0
        if inside:
            out.add(i)
            depth += line.count("{") - line.count("}")
            if depth <= 0 and ("}" in line or ";" in line):
                inside = False
    return out


def _inside_string(code: str, at: int) -> bool:
    """这个位置是不是落在一个字符串字面量里。"""
    in_string = False
    quote = ""
    escaped = False
    for ch in code[:at]:
        if escaped:
            escaped = False
            continue
        if in_string and ch == "\\":
            escaped = True
            continue
        if in_string:
            if ch == quote:
                in_string = False
            continue
        if ch in "\"'`":
            in_string = True
            quote = ch
    return in_string


def _has_literal_number(code: str) -> bool:
    """一个**数字字面量**：`300`、`64 * 1024`；`u64`、`i32` 里的数字不算。

    只看前一个字符：前面是字母、数字、下划线或点，这个数字就是标识符或类型名的
    一部分（`u64` 是类型，不是 64 这个量）。
    """
    for i, ch in enumerate(code):
        if not ch.isdigit():
            continue
        if i == 0:
            return True
        prev = code[i - 1]
        if not (prev.isalnum() or prev in "_."):
            return True
    return False


def _line_surface(root: Path, name: str, pattern: re.Pattern, kind: str):
    for path, suffix in source_files(root):
        if suffix not in KNOWN_EXT:
            continue
        for index, code in code_lines(path, suffix):
            if not pattern.search(code):
                continue
            # 新代码门要能挡住真正的构造，而不是名字提及：时钟与空间两类必须带一个
            # 数字或一次带参调用（`timeout(window)`、`with_capacity(26)`、`take(64)`），
            # 只出现标识符或类型转换的行（`as_millis() as u64`、字段声明）不算。
            if name in ("clocks", "spaces", "deploy-numbers", "json-numbers", "sql-ddl", "shell-clocks"):
                if not (_has_literal_number(code) or re.search(r"\(\s*[A-Za-z0-9_]+\s*[,)]", code)):
                    continue
                # 字符串里的数字（SQL 里的 `limit=100`、页面里的提示计时）不是代码里的界。
                match_at = pattern.search(code)
                if match_at and _inside_string(code, match_at.start()):
                    continue
            # 局部计数器（`let mut secs = 0`、`local released = 0`）不是界：它数的是
            # 这一次循环里走了几个，不是任何阈值。
            if re.match(r"\s*(?:let\s+mut|local|var)\b", code):
                continue
            # 裸的计数器归零（Lua 的 `backoff = 0`）：不是界，是一次重置。
            if re.match(r"\s*[a-z_]+\s*=\s*0\s*;?\s*$", code):
                continue
            # 枚举判别值（`Timeout = 1,`）与时间戳解析（`.parse().ok()?`）都不是界。
            if re.match(r"\s*[A-Z][A-Za-z0-9_]*\s*=\s*\d+,?\s*$", code):
                continue
            if ".parse().ok()?" in code:
                continue
            if True:
                yield {
                    "surface": name,
                    "kind": kind,
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "name": code.strip()[:120],
                    "detail": "",
                }


FILE_DESTROY_RE = re.compile(
    r"remove_file\(|remove_dir_all\(|\bunlink\(|\brm\s+-[rf]|DROP\s+DATABASE|"
    r"git\s+reset\s+--hard|docker\s+prune|\bkubectl\s+delete|\bdelete_object\(",
    re.I,
)
DROP_RE = re.compile(
    r"try_send\(|\bselect!|\babort\(|\bdrop\(|\.skip\(|\.filter\(|\bsample|"
    r"\btry_publish\(|\bif_full\(",
)
LOSSY_RE = re.compile(
    r"from_utf8_lossy|to_string_lossy|\bas\s+f32\b|\bday_key|\btruncate_to_day|"
    r"\bfloor_|\bbucket_|\bto_ascii_lowercase|\.to_lowercase\(\)",
)
SILENT_OTHER_RE = re.compile(
    r"except\s*:\s*pass|catch\s*\([^)]*\)\s*\{\s*\}|\|\|\s*true|2>\s*/dev/null|"
    r"unwrap_or_else\(\|_\||\.unwrap_or\(",
)
JSON_NUM_RE = re.compile(r'"([A-Za-z0-9_.-]+)"\s*:\s*(\d+(?:\.\d+)?)\s*[,}]')


def surface_file_destroys(root: Path):
    return _line_surface(root, "file-destroys", FILE_DESTROY_RE, "delete")


def surface_drops(root: Path):
    return _line_surface(root, "drops", DROP_RE, "drop")


def surface_lossy_transforms(root: Path):
    return _line_surface(root, "lossy-transforms", LOSSY_RE, "lossy")


def surface_silent_other(root: Path):
    return _line_surface(root, "silent-other", SILENT_OTHER_RE, "silent")


def surface_json_numbers(root: Path):
    for path, suffix in source_files(root):
        if suffix != ".json":
            continue
        if "lock" in path.name and suffix == ".json":
            continue
        for index, code in code_lines(path, suffix):
            if JSON_NUM_RE.search(code):
                yield {
                    "surface": "json-numbers",
                    "kind": "config",
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "name": code.strip()[:120],
                    "detail": "",
                }


def surface_clocks(root: Path):
    return _line_surface(root, "clocks", CLOCK_RE, "clock")


def surface_spaces(root: Path):
    return _line_surface(root, "spaces", SPACE_RE, "space")


def surface_truncations(root: Path):
    return _line_surface(root, "truncations", TRUNC_RE, "truncation")


def surface_discards(root: Path):
    return _line_surface(root, "discards", DISCARD_RE, "discard")


def surface_narrowing(root: Path):
    return _line_surface(root, "narrowing", NARROW_RE, "narrowing")


def surface_silent(root: Path):
    return _line_surface(root, "silent-fallbacks", SILENT_RE, "silent")


def surface_deploy_numbers(root: Path):
    for path, suffix in source_files(root):
        if suffix not in (".yaml", ".yml", ".toml"):
            continue
        for index, line in code_lines(path, suffix):
            key = line.split(':', 1)[0].strip().lower()
            boundish = any(word in key for word in (
                "timeout", "interval", "secs", "seconds", "period", "cooldown",
                "replicas", "limit", "limits", "requests", "memory", "cpu",
                "ttl", "days", "keep", "budget", "for", "threshold", "size",
            ))
            if boundish and YAML_NUM_RE.match(line):
                yield {
                    "surface": "deploy-numbers",
                    "kind": "config",
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "name": line.strip()[:120],
                    "detail": "",
                }


# 宿主/部署脚本里的时钟界：`timeout N`、`sleep N`、`--max-time N`。
#
# 脚本是界的另一个藏身处：公开仓的 crate 与部署清单被普查了几轮，宿主读数这类
# 「跑在机器上、不在镜像里」的脚本却一直不在人口里——2026-11-27 实测：从 `--root`
# 指到另一个仓时，`scripts/host/*.sh` 一条候选也扫不出来，于是那边新加多少秒都不会红。
# 所以这里单独给 shell 一张形状：只登记**写死数字**的三类构造；`timeout "$X"` 的值在
# 变量上，按 by-variable 处理（它自己的赋值处再判）。
SHELL_CLOCK_RE = re.compile(
    r"(?:^|[\s;&|(])(?:sudo\s+(?:-n\s+)?)?(?:timeout|sleep)\s+(?P<num>[0-9]+(?:\.[0-9]+)?[smh]?)\b"
    r"|--max-time\s+(?P<max>[0-9]+(?:\.[0-9]+)?)"
)


def surface_shell_clocks(root: Path):
    for path, suffix in source_files(root):
        if suffix != ".sh":
            continue
        # 夹具里的 `timeout 1` 是标本，不是生产读数。
        if path.name.endswith(".test.sh"):
            continue
        for index, code in code_lines(path, ".sh"):
            match = SHELL_CLOCK_RE.search(code)
            if not match:
                continue
            yield {
                "surface": "shell-clocks",
                "kind": "clock",
                "path": str(path.relative_to(root)),
                "line": index + 1,
                "name": code.strip()[:120],
                "detail": "",
            }


def surface_sql_ddl(root: Path):
    for path, suffix in source_files(root):
        if suffix != ".sql":
            continue
        for index, line in code_lines(path, suffix):
            if SQL_DDL_RE.search(line):
                yield {
                    "surface": "sql-ddl",
                    "kind": "sql",
                    "path": str(path.relative_to(root)),
                    "line": index + 1,
                    "name": line.strip()[:120],
                    "detail": "",
                }


def projection_tables(root: Path) -> set[str]:
    """可重建投影登记表：表名必须与「重建命令」出现在同一登记行上。

    只抓到名字不算——名字出现在别处（查询、注释）也会被旧版收进来，于是
    「可重建」这句话没有任何东西作证。这里要求同一行里同时有表名与 rebuild。
    """
    names: set[str] = set()
    for path in (root / "crates").rglob("projection_readers.rs"):
        try:
            lines = path.read_text(errors="ignore").splitlines()
        except OSError:
            continue
        for line in lines:
            if "rebuild" not in line.lower():
                continue
            for match in re.finditer(r'"([a-z0-9_]+)"', line):
                names.add(match.group(1))
    return names


DISPOSITION_WINDOW = 3


# 判词的形式：Rust 的 `//`、shell 的 `#`（`#` 后要有空白——`#[attr]` 不是注释）、
# 以及 HTML 里只可能作为注释写下的 `<!-- -->`。
_MARK = r"^[ \t]*(?://[ \t]*|#[ \t]+|<!--[ \t]*)"
KEEP_RE = re.compile(
    _MARK + r"(?:keep|不可删除)[ \t]*[:：][ \t]*(.+)$", re.MULTILINE
)
NOT_A_BOUND_RE = re.compile(_MARK + r"not-a-bound[ \t]*[:：]", re.MULTILINE)
SOURCE_RE = re.compile(_MARK + r"cap[ \t]*:[ \t]*(?:derived|fixed)", re.MULTILINE)


def _own_annotation(lines: list[str], index: int) -> str:
    """这个数**自己**的那一段注释：从它往上下各取连续的 `//` 行。

    判词不许借邻居：两条常量紧挨着时，上一条的 `keep:` 落在一个按距离取的窗口里，
    下一条哪怕只有 `not-a-bound:` 也会读到邻居的理由（2026-11-27 实测：`MAX_TREE_ENTRIES`
    的 `not-a-bound` 被上一条 `MAX_FILE_BYTES` 的 `keep:` 兜成了「已处置」）。
    一段注释以代码行为界；行本身是标记行时，向下取到那个数的注释块结束。
    """
    def is_comment(line: str) -> bool:
        stripped = line.lstrip()
        # `#[attr]` 与 `#!` 不是注释；`# ` 才是。
        return (
            stripped.startswith("//")
            or stripped.startswith("<!--")
            or (stripped.startswith("#") and len(stripped) > 1 and stripped[1] in " \t")
        )

    lo = index
    while lo > 0 and is_comment(lines[lo - 1]):
        lo -= 1
    hi = index
    while hi + 1 < len(lines) and is_comment(lines[hi + 1]):
        hi += 1
    return "\n".join(lines[lo : hi + 1])


@functools.lru_cache(maxsize=8)
def _disposed_consts(root_str: str) -> frozenset[str]:
    """全树里「已经带判词的常量」的名字。

    一个调用点写 `timeout(SOME_CONST)` 时，那个数已经在那条常量上判过了；在引用处
    再判一遍是把同一笔欠账数两次。这里收的是带 `cap:`/`keep:`/`not-a-bound` 的
    `const NAME` 行——调用点只要引用了其中任何一个，就算处置。
    """
    import pathlib
    names: set[str] = set()
    root = pathlib.Path(root_str)
    for path in root.glob("crates/**/*.rs"):
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for i, line in enumerate(lines):
            m = re.match(r"\s*(?:pub(?:\([a-z]+\))?\s+)?const\s+([A-Z0-9_]+)\s*:", line)
            if not m:
                continue
            window = "\n".join(lines[max(0, i - 4): i + 1])
            if SOURCE_RE.search(window) or KEEP_RE.search(window) or NOT_A_BOUND_RE.search(window):
                names.add(m.group(1))
    return frozenset(names)


def disposition_of(root: Path, row: dict, projections: set[str], cache: dict) -> str:
    """默认要删。只有「删了会让功能或性能下降、或任务无法完成」才允许不删。

    判词必须写在候选所在的那一行（或紧邻上一行），形式是
    `keep: <删了会坏成什么样> 由 <读数/约定> 兜住`。只写 `cap: derived`／
    `cap: fixed` 不算——那回答的是「值从哪来」，不是「为什么不能删」。

    类型化的合法形态直接算处置：`Preview::new(`（下游拿不到全量）、
    投影登记表上的 `DELETE`（登记项点名重建命令与测试）、`redact_body(`（带擦除事实）。
    `not-a-bound:` 说明它是定义，不算界。
    """
    path = root / row["path"]
    key = str(path)
    if key not in cache:
        try:
            cache[key] = path.read_text(errors="ignore").splitlines()
        except OSError:
            cache[key] = []
    lines = cache[key]
    index = row["line"] - 1
    # 判词可以写在候选行的上面，也可以写在下面（`cap:` 标记行下面是 `keep:`）；
    # 只取这个数自己那段注释——按距离取窗口会让邻居的判词兜住这一条。
    context = _own_annotation(lines, index)
    text = lines[index] if 0 <= index < len(lines) else ""
    # 参数不是数字字面量（`sleep(poll)`、`timeout(bound)`）：这个数不在这一行，
    # 在它的来源处；调用点不是第二个数。
    # `// wall:` 已经在写「等不到留下什么、能不能续跑」——时钟界的判据就是这两问；
    # 写了续跑的那一条就是有账的。
    if row["surface"] in ("wall", "walls") and "续跑" in text:
        return "wall-accounted"
    # `// wall:` 行由 markers 面以 kind=wall 产出；同一判据，理由在 detail 里。
    if row["surface"] in ("marker", "markers") and row.get("kind") == "wall":
        # `// wall:` 这一面的存在就是为了写下「等不到留下什么、能不能续跑」；
        # 它本身就是这一格的账。
        return "wall-accounted"
    if row["surface"] in ("clocks", "spaces"):
        bare = re.sub(r"\([^()]*\)", "", text)  # 调用参数里的数字多半是下标，不是量
        if not _has_literal_number(bare):
            return "by-variable"
    # 引用了一个已判常量：数已经在那条常量上判过，引用处不再数第二遍。
    for name in _disposed_consts(str(root)):
        if name in text:
            return "by-constant"
    # 部署清单与部署配置文件里的数值是**部署自己的声明**：它写在生效的那一处，
    # 改它只改这一次部署，不是代码里看不见的默认。与代码里的界分开记。
    if row["path"].startswith("deploy/") and row["surface"] in (
        "deploy-numbers", "config", "json-numbers", "clocks", "spaces", "unknown"
    ):
        return "declaration"
    # 不是可执行的东西：说明、基线、笔记。它们不改变任何阈值行为，「没有判据」在这里
    # 的正确答案是「这一类不是代码」，而不是一条待补的形状表。
    if row["surface"] == "unknown" and row["path"].rsplit(".", 1)[-1] in (
        "txt", "baseline", "md", "html", "css", "tpl"
    ):
        return "data"
    # 文件级处置：同一形状在这一文件里的候选都归这一条理由。类结论只能这样写下来，
    # 否则一条类判词要复制到几千行上，复制本身就会漂。
    head = "\n".join(lines[:20])
    for match in re.finditer(r"census-keep:\s*shapes=([a-z0-9_,-]+)\s*(?:—|-|:)\s*(.+)", head):
        shapes = {s.strip() for s in match.group(1).split(",")}
        reason = match.group(2).strip()
        # 判词必须说清「删掉会怎样」：只写「这条约定」不算，得写出后果。
        states_consequence = bool(re.search(r"删掉|删了|删除|because", reason)) or "由" in reason
        if row["surface"] in shapes and len(reason) >= 8 and states_consequence:
            return "keep-class"
    keep = KEEP_RE.search(context)
    if keep:
        reason = keep.group(1).strip()
        # 第一问是「能不能删」。只有「删了会坏成什么样」才是答案；把上面那句
        # `cap:` 的理由复述一遍（`删掉后回到「…所描述的情形」`）不是——它说的是
        # 这个数为什么是这个数，不是删掉之后哪里会坏。这样的理由按「理由不全」
        # 记，等它被逐条回答（2026-11-27 实测：这个模板是批量生成的，978 处，
        # 属主指出它没有回答第一问）。
        restated = re.search(r"删掉后回到「.+」所描述的情形", reason) is not None
        if len(reason) >= 8 and not restated and (
            re.search(r"删掉|删了|删除", reason) or "由" in reason or "because" in reason.lower()
        ):
            return "keep"
        return "reason-incomplete"
    if NOT_A_BOUND_RE.search(context):
        return "definition"
    if SOURCE_RE.search(context):
        # 有来源但没有「为什么不能删」：仍算未处置，标成理由不全。
        return "reason-incomplete"
    return "undisposed"


SURFACES = {
    "unknown": surface_unknown,
    "clocks": surface_clocks,
    "spaces": surface_spaces,
    "deploy-numbers": surface_deploy_numbers,
    "json-numbers": surface_json_numbers,
    "sql-ddl": surface_sql_ddl,
    "shell-clocks": surface_shell_clocks,
    "markers": surface_markers,
    "consts": surface_consts,
    "config": surface_config,
}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--surface", choices=sorted(SURFACES), default=None)
    parser.add_argument("--tsv", action="store_true")
    parser.add_argument(
        "--check",
        action="store_true",
        help="判据模式：每条候选必须有处置；有未处置即非零退出",
    )
    parser.add_argument(
        "--check-since",
        metavar="REV",
        default=None,
        help="只判 REV...HEAD 这一段提交的新增行（CI 与推送用）",
    )
    parser.add_argument(
        "--check-staged",
        action="store_true",
        help="只判这一笔**暂存**的新增行：pre-commit 用索引做 diff",
    )
    parser.add_argument(
        "--check-diff",
        action="store_true",
        help="只判这一笔新增的行：diff 里出现的候选必须有处置",
    )
    parser.add_argument(
        "--check-new",
        action="store_true",
        help="只判这一笔新增的未处置项：父提交里没有的候选，必须已有处置",
    )
    args = parser.parse_args(argv[1:])

    root = Path(args.root).resolve()
    # 「像一棵可普查的树」不再要求 crates/：宿主读数这类脚本住在伙伴仓里，那边没有
    # crates/ 却有 scripts/——按旧判据，那边一条候选都扫不出来，新加的秒数永远不会红。
    # 一个目录名称都没有才判扫错了地方。
    if not any((root / d).exists() for d in CODE_DIRS):
        print(
            f"FAIL: {root} 不像一棵可普查的树（{'/'.join(CODE_DIRS)} 一个都没有）",
            file=sys.stderr,
        )
        return 2

    chosen = [args.surface] if args.surface else list(SURFACES)
    rows: list[dict] = []
    for name in chosen:
        for row in SURFACES[name](root):
            row["surface_name"] = name
            rows.append(row)

    STAGED = args.check_staged
    SINCE = args.check_since
    if args.check_diff or args.check_staged or SINCE:
        import subprocess

        def added_lines() -> dict[str, list[tuple[int, int]]]:
            out = subprocess.run(
                ["git", "-C", str(root)] + (["diff", "--cached", "-U0"] if STAGED else (["diff", "-U0", f"{SINCE}...HEAD"] if SINCE else ["diff", "-U0", "HEAD"])),
                capture_output=True,
                text=True,
            )
            ranges: dict[str, list[tuple[int, int]]] = {}
            current = None
            for line in out.stdout.splitlines():
                if line.startswith("+++ b/"):
                    current = line[6:]
                elif line.startswith("@@") and current:
                    match = re.search(r"\+(\d+)(?:,(\d+))?", line)
                    if match:
                        start = int(match.group(1))
                        count = int(match.group(2) or "1")
                        ranges.setdefault(current, []).append((start, start + count - 1))
            return ranges

        ranges = added_lines()

        def in_diff(path: str, line: int) -> bool:
            return any(start <= line <= end for start, end in ranges.get(path, []))

        projections = projection_tables(root)
        cache_lines: dict[str, list[str]] = {}
        offenders = []
        for row in rows:
            if not in_diff(row["path"], row["line"]):
                continue
            if disposition_of(root, row, projections, cache_lines) == "undisposed":
                offenders.append(row)
        if offenders:
            print(f"这一笔新增了 {len(offenders)} 处未处置的界。三种正确写法：")
            print("  1) 默认无界：删掉这个界；")
            print("  2) Bound::derived(值, \"读数名\", \"换输入就红的测试名\") 并写 keep: 无界会坏成什么样 由什么兜住；")
            print("  3) Bound::fixed(值, \"协定的名字\") 并写同样的 keep:。")
            for row in offenders[:100]:
                print(f"  {row['path']}:{row['line']}\t{row['kind']}\t{row['name'][:100]}")
        print(f"bounds-diff\tnew_unresolved={len(offenders)}", file=sys.stderr)
        return 1 if offenders else 0

    if args.check_new:
        import subprocess

        projections = projection_tables(root)
        cache_lines: dict[str, list[str]] = {}
        parents: dict[str, str] = {}

        def parent_text(path: str) -> str:
            out = subprocess.run(
                ["git", "-C", str(root), "show", f"HEAD:{path}"],
                capture_output=True,
                text=True,
            )
            return out.stdout if out.returncode == 0 else ""

        new_unsourced = []
        for row in rows:
            if disposition_of(root, row, projections, cache_lines) != "undisposed":
                continue
            path = row["path"]
            if path not in parents:
                parents[path] = parent_text(path)
            if row["name"] not in parents[path]:
                new_unsourced.append(row)
        if new_unsourced:
            print(f"这一笔新增了 {len(new_unsourced)} 处未处置的界：")
            for row in new_unsourced[:100]:
                print(f"  {row['path']}:{row['line']}\t{row['kind']}\t{row['name'][:100]}")
        print(f"bounds-new\tnew_unresolved={len(new_unsourced)}", file=sys.stderr)
        return 1 if new_unsourced else 0

    if args.check:
        projections = projection_tables(root)
        cache: dict = {}
        counts: dict[str, int] = {}
        undisposed = []
        for row in rows:
            state = disposition_of(root, row, projections, cache)
            counts[state] = counts.get(state, 0) + 1
            if state in ("undisposed", "missing-criterion", "reason-incomplete"):
                undisposed.append((state, row))
        if undisposed:
            print(f"未处置 {len(undisposed)} 条（候选共 {len(rows)} 条）：")
            for state, row in undisposed[:200]:
                print(f"  {state}\t{row['path']}:{row['line']}\t{row['kind']}\t{row['name'][:100]}")
            if len(undisposed) > 200:
                print(f"  … 还有 {len(undisposed) - 200} 条")
        summary = " ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        print(f"census\tcandidates={len(rows)}\t{summary}", file=sys.stderr)
        return 1 if undisposed else 0

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
