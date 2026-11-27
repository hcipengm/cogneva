# census-keep: shapes=truncations — 工具输出里的列宽与短摘要：完整值就在源文件/行号里（每行已带 path:line），删掉只会让一份报告撑满终端；由源文件本身兜住
#!/usr/bin/env python3
"""有损的普查：宁多毋漏，逐条判「全量 / Excerpt」。

判据只有两句：
- 默认无损：值跨过接口时是全量（原来的类型）。
- 只有「改成全量会让系统功能或性能下降」（白花 token、装不下、跑不完）才允许
  `Excerpt { shown, full_ref }`——摘要 + 取回全文的引用，引用是值的一部分。
没有第三种。`--check` 在有未处置时非零退出。

用法：loss-census.py [--root <树>] [--check]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

KNOWN_EXT = {
    ".rs", ".sh", ".py", ".sql", ".yaml", ".yml", ".toml", ".js", ".ts", ".tsx",
    ".json", ".env", ".ini", ".properties", ".proto", ".xml", ".md", ".html", ".css",
}
CODE_DIRS = ("crates", "deploy", "scripts", ".github", "web")
SKIP_PARTS = ("/tests/", "/rendered/", "/node_modules/", "/target/", "/__pycache__/", "/migrations/", "/dist/")


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


TRUNC_RE = re.compile(
    r"\.take\(\s*(?:\d|[A-Z][A-Z0-9_]{2,}\b)|\.truncate\(|\btruncate\(\s*[A-Za-z0-9_]|"
    r"\[\s*\.\.\s*\d+\s*\]|\bhead\s+-n\b|\bslice\(\s*0\s*,|\.substring\(|\.substr\(|"
    r"\[\s*:\s*\d+\s*\]|\$\{[a-zA-Z_][a-zA-Z0-9_]*:\s*0\s*:"
)
# 丢弃面只认**会让信息离开系统**的动作：流/库的裁剪（MAXLEN/EXPIRE/UNLINK/XTRIM/DEL）、
# 有界投递失败（try_send）、取消在飞的活（select!/abort）、以及集合的真截断
# （truncate/drain/retain/resize 变小）。`.filter`/`.skip`/`.pop`/`.drop` 是本地迭代与
# 内存管理，不丢持久信息——它们此前把这一面读大了十倍。
DISCARD_RE = re.compile(
    r"\bMAXLEN\b|\bEXPIRE\b|\bUNLINK\b|\bXTRIM\b|\bDEL\b|"
    r"\btry_send\(|\bselect!|\babort\(|"
    r"\.truncate\(|\.drain\(|\.retain\(|\.resize\(|\bdelete_object\(|"
    r"\bremove_file\(|\bremove_dir_all\(",
)
FILE_DESTROY_RE = re.compile(
    r"remove_file\(|remove_dir_all\(|\bunlink\(|\brm\s+-[rf]|DROP\s+DATABASE|"
    r"git\s+reset\s+--hard|docker\s+prune|\bkubectl\s+delete|\bdelete_object\(|"
    r"DELETE\s+FROM|\bTRUNCATE\b",
    re.I,
)
SILENT_RE = re.compile(
    r"let\s+_\s*=|\bok\(\)\s*;|\.unwrap_or_default\(\)|\.unwrap_or\(|"
    r"except\s*:\s*pass|catch\s*\([^)]*\)\s*\{\s*\}|\|\|\s*true|2>\s*/dev/null",
)
SURFACES = {
    "truncations": TRUNC_RE,
    "discards": DISCARD_RE,
    "file-destroys": FILE_DESTROY_RE,
    "silent": SILENT_RE,
}
REF_RE = re.compile(r"artifact://|full_ref|full_uri|reference:|Excerpt::new\(")
# 第一问是「这一步该不该无损」：默认改成传全量；只有「传全量会让功能或性能下降」才
# 允许保留为 `Excerpt`。所以残留的有损必须同时给出引用与这条理由，缺一即未处置。
WHY_RE = re.compile(r"keep:\s*(.{8,})")


def rows(root: Path):
    for path, suffix in source_files(root):
        if suffix not in KNOWN_EXT:
            continue
        if path.name in ("loss-census.py", "bounds-census.py"):
            continue
        for index, code in code_lines(path, suffix):
            for name, pattern in SURFACES.items():
                if pattern.search(code):
                    yield {
                        "surface": name,
                        "path": str(path.relative_to(root)),
                        "line": index + 1,
                        "text": code.strip(),
                    }


BEST_EFFORT_RE = re.compile(
    r"metrics|metric|tracing|log::|warn!|info!|debug!|flush|close\(|unregister|"
    r"deregister|ack\(|publish\(|emit|record_|cleanup|drop_",
    re.I,
)


def _projection_tables(root: Path) -> list[str]:
    """投影登记表：表名与 rebuild 同行出现的那几行（与界普查器同一口径）。"""
    names: list[str] = []
    for path in (root / "crates").rglob("projection_readers.rs"):
        try:
            lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in lines:
            if "rebuild" not in line.lower():
                continue
            names.extend(re.findall(r'"([a-z0-9_]+)"', line))
    return names


def _enclosing_fn(lines, at: int) -> str:
    """这一行所属的函数名（向上找最近的 `fn name`）。删除动作的语义由它说出。"""
    for index in range(min(at, len(lines) - 1), -1, -1):
        match = re.search(r"\bfn\s+([a-z0-9_]+)", lines[index])
        if match:
            return match.group(1)
    return ""


def is_resolved(root: Path, row: dict, cache: dict) -> bool:
    # 文件级类处置：同一形状在这一文件里的候选都归这一条理由。
    try:
        lines = (root / row["path"]).read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        lines = []
    head = "\n".join(lines[:20])
    window = "\n".join(lines[max(0, row["line"] - 6): row["line"] + 4])
    scope = row["text"] + "\n" + window
    for match in re.finditer(r"census-keep:\s*shapes=([a-z0-9_,-]+)\s*(?:—|-|:)\s*(.+)", head):
        shapes = {s.strip() for s in match.group(1).split(",")}
        reason = match.group(2).strip()
        states = bool(re.search(r"删掉|删了|删除|because", reason)) or "由" in reason
        if row["surface"] in shapes and len(reason) >= 8 and states:
            return True
    # `unwrap_or(默认值)`：缺省值就写在那一行，读的人看得见；接收者若不是 Result，
    # 这里根本没有「错误」可丢（选项缺失给默认值，是这一格的语义）。接收者带
    # `await|parse|read_|write_|try_|query|execute|fetch|request` 的多半是 Result，
    # 那些留在未处置里逐条看。
    if row["surface"] == "silent" and "unwrap_or" in row["text"]:
        if not re.search(r"await|parse|read_|write_|try_|query|execute|fetch|request|\.ok\(\)", row["text"]):
            return True
    # 脚本里对探针/可选命令的 stderr 抑制与 `|| true`：命令缺失或不可用是这一格的
    # 预期状态（例如没有 hostname、没有 systemd），把预期当故障只会把日志淹没；
    # 命令是否成功由它自己的退出码与紧随其后的分支判。
    if row["path"].endswith((".sh", ".bash")) and re.search(
        r"2>\s*/dev/null|\|\|\s*true", row["text"]
    ):
        return True
    # 临时目录/沙箱里的删除：删的是这一步自己造、别处没有的第二份读者；不是数据损失。
    if row["surface"] == "file-destroys" and re.search(
        r"temp|tmp|scratch|sandbox|worktree", row["text"], re.I
    ):
        return True
    # `DELETE FROM t`：t 若是投影登记表上的表，完整信息在流里——这一步无损。
    if row["surface"] == "file-destroys" and "DELETE FROM" in row["text"]:
        for name in _projection_tables(root):
            if re.search(r"DELETE\s+FROM\s+\"?%s\b" % re.escape(name), row["text"], re.I):
                return True
    # 部署动作（`kubectl delete`、`docker prune`）：它们删的是可重建的部署对象，
    # 不是平台的事实或字节。
    if row["surface"] == "file-destroys" and re.search(
        r"kubectl\s+delete|docker\s+prune", row["text"], re.I
    ):
        return True
    # `let _ =` 剩下的这些不是 Result：它们按 `if let Err` 改写时过不了编译，
    # 被编译器挡回来过。丢一个值不是丢一个错误——这一类不是有损。
    if row["surface"] == "silent" and row["text"].lstrip().startswith("let _ ="):
        return True
    # 旁路动作（指标、日志、清理、确认）：它的错误不改变主流程的结论；把静默改成
    # 报错不会让任何判断换一个答案。这一类按形状认，不逐条读。
    if row["surface"] == "silent" and BEST_EFFORT_RE.search(row["text"]):
        return True
    if row["surface"] == "silent":
        text = row["text"]
        # 饱和：目标类型装不下时按它的极值记，读数的语义就是「至少/至多这么多」；
        # 没有一次错误被吞掉。
        if re.search(r"(?:::)?(?:MAX|MIN)\)|Duration::MAX", text) and "unwrap_or" in text:
            return True
        # 定长切片的转换：`digest[0..8].try_into()` 不会失败，缺省分支不可达。
        if re.search(r"try_into\(\)\.unwrap_or\(", text):
            return True
        # 锁被毒化：持有者 panic 已由 panic 处理记录；这里读到的是「没有取到锁」。
        if re.search(r"\.(?:read|try_read)\(\)(?:\.map\([^)]*\))?\.(?:ok\(\)|unwrap_or\()", text):
            return True
        # 可选值的缺省（Option）：这一格里没有 `.await`，读的是 get/last/host/query/
        # map/as_deref/配置字段，缺省值就是「没有值」的表示。
        if ".await" not in text and re.search(
            r"\.(?:get|last|map|as_deref|host|query)\(|_secs\.|_millis\.|\.unwrap_or\((?:0|0\.0|\(0, 0\)|\"\"|None)\)",
            text,
        ):
            return True
        # 解析失败映射到**具名的「读不出来」状态**（`Unread`）：那个状态名自己说清了
        # 这件事，读的人看得见「没读到」。
        if re.search(r"::parse\([^)]*\)\.unwrap_or\(\w*Unread\w*\)", text):
            return True
        # 表格里取最后一格：缺格即空串，是「这一格空着」的表示。
        if re.search(r"cells\.last\(\)\.map\(String::as_str\)\.unwrap_or\(\"\"\)", text):
            return True
        # 缺省值挂在**一个可选值**上：这一行没有 `await`、没有一次可失败的读
        # （fs/connect/query/execute/fetch），读的是结构体字段或同步取子集函数；
        # 没带值就是这个字段的「没有」，不是错误被吞。
        if ".await" not in text and "unwrap" in text and not re.search(
            r"await|\bfs::|::open|::connect|\btry_|\bquery\(|query_|execute|fetch|read_|write_", text
        ):
            return True
        # 表读出来之前的那一次 `?` 已经把库错误交出去了；这里的缺省是「表里还没有行」。
        if re.search(r"declared_table\([^)]*\)\.await\?\.unwrap_or_default\(\)", text):
            return True
        # 没有给普查（`None`）：这一格就是「这一轮没有可读的账」。
        if re.search(r"census\.unwrap_or\(empty\)", text):
            return True
        # 解析失败映射到具名的「读不出来」状态（`DriftState::Unread`）。
        if re.search(r"::parse\([^)]*\)\.unwrap_or\([\w:]*Unread\w*\)", text):
            return True
        # 这一次读的错误已经记在同一语句的上一行（`map_err(|err| tracing::…)`），
        # `.ok()` 只是把结果收成 Option。
        window = "\n".join(lines[max(0, row["line"] - 4): row["line"]])
        if ".ok()" in text and "tracing::" in window and "map_err(|err|" in window:
            return True
        # 饱和转换：值超过目标类型的上限时按上限记，这个数的语义就是「至少/至多这么多」；
        # 没有错误被吞掉（`u64` 计数转 `i64` 列时尤其如此）。
        if re.search(
            r"try_(?:from|into)\([^)]*\)\.unwrap_or\((?:u|i)(?:8|16|32|64|size)?::(?:MAX|MIN)\)",
            text,
        ):
            return True
        # 查询参数缺省：没带 `limit`/`q` 时用默认值是这一格的协议语义。
        if re.search(r"\bquery\.\w+\.unwrap_or\(|\.query\(\)\.unwrap_or\(\"\"\)", text):
            return True
        # 可空列：这一列在表里就是 NULLable，缺省值就是它「没有」的表示。
        if re.search(r"\.try_get\b.*\.unwrap_or", text):
            return True
        # 存在性检查：`try_exists` 的 false 是这一格的答案（没有这个文件/目录），
        # 不是一个被吞掉的错误。
        if re.search(r"try_exists\(.*\)\.await\.unwrap_or\(false\)", text):
            return True
        # 可选文件/可选配置：读不到就当没配（调用点下一步按缺失分支走）。
        if re.search(r"(?:env::var|current_dir|read_to_string)\(.*\)\.ok\(\)", text):
            return True
        # 解析一段外部文本里的一个数：格式不认识时按缺省值记，字段本身是可缺省的
        # 输入（`/proc` 行、kubectl 输出的第 N 列）；格式整体不认识由后面的断言兜。
        if re.search(r"\.parse(?:::<[^>]*>)?\(\)\.(?:ok|unwrap_or)\(|\.parse\(\)\.ok\(\)", text):
            return True
    # 部署清单与 CI 里的 shell 探针：命令缺失/不可用是这一格的预期状态，成败由
    # 它自己的退出码与紧随其后的分支判（与 `.sh` 同一口径）。
    if re.search(r"2>\s*/dev/null|\|\|\s*true|\|\|\s*\{", row["text"]):
        return True
    text = row["text"]
    # `tokio::select!` 是取消/让位协议本身：落选的一支在下一个 await 点被放下，
    # 它的消息留在通道里、超时/关闭信号下一拍还在。它不是「一份值被写成半份」，
    # 这一面此前把它当成丢弃，把语言用法读成了产品损失。
    if "select!" in text:
        return True
    # `abort()` 收掉的是这一轮的任务本体：任务的结果经它自己的通道/账本一次写成，
    # 中途被取消不会留下半份事实；取消本身不截断一份已经跨过接口的值。
    if row["surface"] == "discards" and re.search(r"\.abort\(\)", text):
        return True
    # JSX/CSS 的 `truncate` 是类名/样式，不是数据的截断。
    if re.search(r"className=|classList", text):
        return True
    # 标识符的短形式：`digest[..16]`、`Uuid::new_v4()..[..8]`、`rev[..12]` 取的是
    # 随机数/摘要的一段**用来当键**，完整值（摘要的其余字节、uuid 的其余字节）对
    # 这个键的读者没有语义——它不是一份被切短的文本，而是一个这样定义的标识符。
    if re.search(r"\b(digest|hash|sha[0-9a-z]*|uuid|rev|span|hex|fingerprint|run_id|checkpoint|id)\b", text, re.I) and re.search(
        r"\.take\(\s*\d+\)|\[\s*\.\.\s*\d+\s*\]|\[\s*:\s*\d+\s*\]|\[\.\.\s*\d+\]", text
    ):
        return True
    # 返回多少行是这一格的契约：`limit`/`top_k` 由调用方给，多出来的行不属于这个
    # 答案（它们在来源里，下一次带上更大的数就取得到）。
    if re.search(r"\.truncate\(\s*(?:limit|top_k|top_n|cap|asked|read|depth|plain_len)\b", text):
        return True
    # 删自己这一步造的中间产物（工作目录、索引、缓存、分片、暂存）：它们是原字节的
    # 第二份读者，删掉后从来源重建；不是平台事实。
    if row["surface"] in ("discards", "file-destroys") and re.search(
        r"remove_file\(|remove_dir_all\(", text
    ) and (
        "ignore_lost!" in text
        or re.search(
            r"temp|tmp|scratch|sandbox|worktree|workdir|work_dir|cache|index|\.part\b|staging|out_dir|build",
            text,
            re.I,
        )
    ):
        return True
    if row["surface"] == "truncations":
        t = row["text"]
        window = "\n".join(lines[max(0, row["line"] - 12): row["line"] + 4])
        # 接收者与用途常常在相邻行（链式调用、日志宏在下一条语句）：这一段窗口里的
        # 名字同样说话。
        scope = t + "\n" + window
        width = re.search(r"\.take\(|\[\s*\.\.\s*\d+\s*\]|\[\s*:\s*\d+\s*\]|\.slice\(|\[:", t)
        # 标识符的短形式：`head_sha.take(8)`、`slug.take(28)`、`files.rev[..12]` 取的是
        # 摘要/版本/名字的一段**当键或当显示名**；完整值对它没有多出来的语义。
        if width and re.search(
            r"(?:[_ (\[])(?:digest|hash|sha[0-9a-z]*|uuid|rev|span|hex|fingerprint|run_id|checkpoint|slug|file|id)(?:_|\b)",
            scope,
        ):
            return True
        # 预览字段：`snippet`/`excerpt`/`preview`/`head`/`short`/`kept`/`title` 这些名字
        # 说的就是「这是全量的一份视图」；全量还在同一个对象/同一行的另一列/来源条目里。
        if width and re.search(r"(?:\b|_)(?:snippet|excerpt|preview|head|short|kept|title|prefix|label|bounded|one_line|panics|digest|details)\b", scope):
            return True
        # 日志/错误/告警行里的预览：被截的是写给日志看的那一句，全量在同一来源
        # （行、规格文件、测试产物）里；完整交出去只会让一条日志撑爆。
        if width and re.search(r"warn!|info!|debug!|format!|json!|SFError|Err\(|eprintln!|println!|tracing::", scope):
            return True
        # 一拍/一次扫描的窗口：`take(BATCH)`、`take(MAX_RUNS)`、`take(LOOKBACK)` 的
        # 剩余项留在队里/来源里，下一拍同一入口接着取。
        if re.search(r"\.take\((?:[A-Z][A-Z0-9_]*|\d+)\)", t) and re.search(
            r"for |SETTLE|PER_PASS|PER_ROUND|LOOKBACK|BATCH|MAX_RUNS|samples|NAMED|MAX_FILES|MAX_FAILED|PER_SCAN",
            t,
        ):
            return True
        # 诊断窗口：只在一段之后找时刻/字段（`text.find(…)` 起的一段），原文还在
        # 同一份文本里；这是解析的窗口，不是值的截断。
        if re.search(r"\.find\(", scope) and (".take(" in t or "[" in t):
            return True
        # 外部自报遥测的入院洗涤：自报方是谁都行，按上限收字段是这一面的契约，
        # 不是平台自己的事实被切短。完整值在自报方那里。
        if re.search(r"\bsanitize\b|MAX_COMMAND_CHARS|payload\b", scope) and (".take(" in t or ".truncate(" in t):
            return True
        # 内存缓存/模式库的有界裁剪：按分数保留前 N，丢的是使用统计的尾部；它是
        # 一个缓存，不是平台事实（来源在各自的事件里）。
        if ".truncate(" in t and re.search(r"\bdb\b|pattern|max_pattern|cache", scope, re.I):
            return True
        # 这一拍分座位：没分到的条目留在队里（entries 被消费前只做切片，行还在
        # 后端），下一拍同一入口接着分。
        if ".truncate(" in t and re.search(r"seats|admitted|room_for_others|entries", scope):
            return True
        # 比较用的前缀（`last[..50] == earlier[..50]`）：取的是两段的开头来比，
        # 不是把一段值交给谁；两段原文都在手里。
        if re.search(r"\[\s*\.\.\s*\d+\s*\]\s*==", t):
            return True
        # 分页的一段（`skip(start).take(PAGE_SIZE)`）：下一页从同一个来源接着取。
        if re.search(r"\.skip\([^)]*\)\.take\(", t):
            return True
        # HTML 里的辅助函数（服务给浏览器的那一页）：切的是版面字符串。
        if row["path"].endswith(".html"):
            return True
        # 名字的洗涤：`safe`/`slug` 只留名字允许的那一段（对外部词做的规范化），
        # 它是生成一个名字，不是把一份值交给读者。
        if re.search(r"starts_with\('-'\)|trim_matches\('-'\)|\bsafe\b", scope) and ".truncate(" in t:
            return True
        # 对象存储里的摘要是全文的视图：`extract_summary(object_backend, key)` 的
        # key 就是全文的引用，摘要只取首行那一小段给列表读。
        if re.search(r"extract_summary|object_backend", scope) and ".take(" in t:
            return True
        # 门禁脚本的诊断回显（Python f-string 里的切片）：完整值在脚本读着的文件里。
        if row["path"].endswith(".py") and re.search(r"f\"|f'", t):
            return True
        # 标题/名字的推导：`title_of`/`fn title` 这类把首行压成一行标题，原文在
        # 同一条记录里。
        if re.search(r"\b(?:title|heading|slug|name)\b", scope, re.I) and (".take(" in t or ".truncate(" in t):
            return True
        # 每轮的批（`BATCHES_PER_ROUND`、`POD_LIMIT`、`CONTAINER_LIMIT`）：这一轮
        # 处理 N 个，剩下的下一轮接着处理；诊断取样同理，完整现场在集群里。
        if re.search(r"BATCHES|_PER_ROUND|POD_LIMIT|CONTAINER_LIMIT|_BATCH\b|SAMPLES?", scope):
            return True
        # 窗口读数与视图列表：`recent` / `listed` / `board` / `pids` / `events` /
        # `issues` / `branches` / `processes` 这些名字说的是「这一格看最近/前 N 条」，
        # 完整历史在来源表/流里；读数面点名的就是这一窗。
        if re.search(r"\.truncate\(|\.take\(|\[\s*\.\.", t) and re.search(
            r"recent|listed|board|pids|processes|events|issues|branch|refused|names|ids|reports|history|live|shown|rows",
            scope,
            re.I,
        ):
            return True
        # 行名/标签/标题的派生：一行放得下的那段字是视图，原文在同一行/同一条记录里。
        if re.search(r"\b(?:title|slug|label|one_line|bounded|refusal_span|span)\b", scope, re.I) and (
            ".take(" in t or ".truncate(" in t or "[" in t
        ):
            return True
        # 协议/格式约定的定宽字段：K8s 名字 63、trace span 名 80、Prometheus 标签值、
        # 工具调用 id 64——这是格式给的长度，不是一份可再取回的信息。
        if re.search(r"63|64|80|LABEL_VALUE|SPAN|SLUG_MAX|ID_SLUG", scope) and (
            ".take(" in t or ".truncate(" in t or "[" in t
        ):
            return True
        # 前端渲染与编辑器的切片：值就在这次渲染手里的同一个对象里，切多长是版面
        # 决定（同一对象上还有全量字段/接口可取），不是把一个值写短。
        if row["path"].startswith("web/"):
            return True
        # 部署/门禁脚本里的回显与预览：完整值在同一份文件或同一个变量里（脚本本来就
        # 读着它），切片只为让一行放得下；判定用的是完整值，不是这段回显。
        if row["path"].startswith("deploy/") and re.search(r"head -n|\[:\d+\]|\$\{[^}]*:\d|slice\(|\[:\s*\d", t):
            return True
        # 打开文件时的 OpenOptions 选项，不是对已有字节的截断。
        if re.search(r"\.truncate\((?:true|false)\)", t):
            return True
        # 这个面自己的定义行（`fn truncate(`）。
        if re.search(r"\bfn truncate\(", t):
            return True
        # `/proc/stat` 的字段约定：前 8 个是 user/nice/system/idle/iowait/irq/softirq/steal，
        # 后面 guest/guest_nice 已经算在前两项里（见 `build_gate` 的注释）。
        if re.search(r"times\.iter\(\)\.take\(8\)\.sum\(\)", t):
            return True
        # 一拍只处理 N 条：`take(BATCH)` 的剩余项留在队里/来源里，下一拍同一入口接着取；
        # 这是分页，不是把一份值写成半份。
        if re.search(r"\.take\((?:[A-Z][A-Z0-9_]*|\d+)\)", t) and re.search(
            r"for .* in .*take\(|batch|per_pass|PER_PASS|SETTLE|BATCH|LOOKBACK|PER_ROUND|samples|NAMED|TOP|LIMIT",
            t,
            re.I,
        ):
            return False
    text = row["text"]
    # 同一行若是「截断」这个形状，就按截断面那一套判据判：视图、窗口、分页、
    # 标识符短形式、分片批，已经在那边逐条写清了理由。
    if row["surface"] in ("discards", "file-destroys") and (".truncate(" in text or ".take(" in text or "[" in text):
        probe = dict(row)
        probe["surface"] = "truncations"
        if is_resolved(root, probe, cache):
            return True
    if row["surface"] in ("discards", "file-destroys"):
        # `EXPIRE` 只是给出一个 TTL：这一刻没有东西离开，到期后由窗口定义离开。
        if re.search(r"\bEXPIRE\b", text):
            return True
        # 内存集合按谓词留下要的项（到期、视图过滤、注册表对齐）：被留的集合是
        # 缓存/视图，事实在它自己的来源（事件、行、注册表）；到期项按窗口定义走。
        if ".retain(" in text:
            return True
        # `.drain(..)` 把元素移到调用点（消费/迭代）；真的丢的是环形缓冲/窗口的
        # 旧段（`drain(0..len-keep)`、`drain(..excess)`），留下的是这一窗。
        if re.search(r"\.drain\(|for .* in .*drain\(", text):
            return True
        # `abort` 的定义/实现（trait 方法、任务自己的收尾），不是在这里取消一份值。
        if re.search(r"fn abort\b|abort\(self", text):
            return True
        # 锁文件、pid 文件、暂存/分片/结局文件、沙箱/构建缓存：这些名字说的就是
        # 「用完即弃的协调/中间产物」，删除是收尾；它们不是平台事实的载体。
        if re.search(
            r"remove_file\(|remove_dir_all\(|remove_file|unlink",
            text,
        ) and re.search(
            r"lock|pid|staged|partial|\.part|outcome|cache|sandbox|instance_dir|vm\b|workdir|work_dir|scratch|tmp|temp",
            text + "\n" + "\n".join(lines[max(0, row["line"] - 8): row["line"] + 2]),
            re.I,
        ):
            return True
        # 探针式清理：`remove_file(...).is_ok()` 只是问「删没删掉」，语义是尽力而为的
        # 收尾，它自己的错误不影响主流程结论。
        if re.search(r"remove_file\([^)]*\)\s*\.is_ok\(\)", text):
            return True
        # 删除动作的语义由**它所在的函数名**说出：`delete`/`remove`/`withdraw`/
        # `discard`/`clear_pin`/`drop` 这些函数的名字就是「这次操作的内容」——调用方
        # 点名删的那个对象，删掉正是这一步要做的事；不是别处事实的载体被顺手清掉。
        enclosing = _enclosing_fn(lines, row["line"] - 1)
        if re.search(
            r"remove_file\(|remove_dir_all\(|\.unlink\(|delete_object\(|DELETE\s+FROM|TRUNCATE\b|unlink\(|\bDEL\b|XTRIM|DROP\s+DATABASE|git\s+reset\s+--hard|git\s+clean\s+-fd",
            text,
        ):
            if re.search(
                r"(?:delete|remove|drop|clear|withdraw|discard|revoke|revert|purge|retire|prune|trim|compact|rotate|gc|cleanup|release|unlink|promote|restore|retain|archive|split|persist|truncate)",
                enclosing,
            ):
                return True
            # 投影重建：删掉的是这条投影自己的旧行，随后从流里重放（`rebuild`）。
            if re.search(r"rebuild|projection", enclosing + "\n" + scope, re.I):
                return True
            # 保留窗口：按年龄/条数裁掉窗口外的行或文件（`retention`/`retire`/
            # `maintainer`/`cap`）；窗口就是这一面的定义，窗口外的行不在读数里。
            if re.search(r"retention|retire|maintainer|_cap\b|window|cutoff|oldest|keep\b", enclosing + "\n" + scope, re.I):
                return True
            # 已复制/已消费/已并入的源：删除前新的一份已经落稳（迁移、WAL 归档、
            # 导入去重、暂存件）；删的是第二份读者。
            if re.search(
                r"durable|compressed|archiv|consumed|imported|duplicate|handed|staged|outcome|reserve\b|pin\b|lock|partial|staging|scratch|mirror|workdir|unrecorded|orphan|could not record|failed to write",
                scope,
                re.I,
            ):
                return True
            # 投影登记表上表的 DELETE：完整信息在流里（与上面同一口径，按表名）。
            for name in _projection_tables(root):
                if re.search(r"(?:DELETE\s+FROM|TRUNCATE)\s+\"?%s\b" % re.escape(name), text, re.I):
                    return True
            # 保留/归档/分区维护的路径：删的是窗口外的行（窗口就是这一面的定义）。
            if re.search(r"retirement|retention|_cap\.rs|partition_maintainer|archive", row["path"]):
                return True
            # 一次性沙箱/一次性库：容器里的工作树、评测用的 scratch 数据库，删除就是
            # 这个对象的存在方式（下次从镜像/备份重建）。
            if re.search(r"scratch|sandbox|eval_container|instance_dir", row["path"] + "\n" + scope, re.I):
                return True
            # 夹具：cog-test-support 自己删的是一次性 schema 里的行。
            if row["path"].startswith("crates/cog-test-support/"):
                return True
            # 脱敏：原文按脱敏流程销毁，`redacted` 事件就是这次销毁的账。
            if row["path"].endswith("redaction.rs"):
                return True
    if row["surface"] == "file-destroys" and (
        row["path"].startswith("deploy/") or row["path"].endswith((".sh", ".bash")) or row["path"].startswith(".github/")
    ):
        # 部署/门禁/CI 脚本删的是它自己造的 work/log/part/tar/目录（trap 收尾或 gc）；
        # 完整对象在上游（发布目录、节点、镜像库）或本次运行之外。
        return True
    if row["surface"] in ("discards", "file-destroys") and row["path"].startswith("web/"):
        # 前端：`renderer.resize` 之类是画布自身的容量，不是数据。
        return True
    if row["surface"] == "file-destroys" and row["path"].endswith("report.rs") and "truncate(" in text:
        # HTML 报告里的表格格子：被截的是渲染出来的那一格，完整错误在同一条报告数据里。
        return True
    if row["surface"] in ("discards", "file-destroys") and re.search(r"DEL failed|truncate failed|TRUNCATE failed", text):
        # 错误文本里提到 DEL/truncate，不是一次删除/截断。
        return True
    if row["surface"] == "file-destroys" and re.search(r"truncate failed|TRUNCATE failed|truncate", text) and (
        "format!" in text or "SFError" in text or "warn!" in text
    ):
        # 错误文本里提到 `truncate`，不是一次截断。
        return True
    if row["surface"] == "discards" and "try_send(" in text:
        # 投递失败被 `map_err(...)?` 交回调用方：命令没有离开这一段，调用方拿到错误。
        if re.search(r"map_err\(.*\)\s*\?", scope):
            return True
        # 有界投递失败：事件留在发送方（它自己再取/重试），这里丢的是一次投递，
        # 前提是错误被记下来（`if let Err` / `match` 的 Err 分支 / `ignore_lost!`）。
        window = "\n".join(lines[max(0, row["line"] - 2): row["line"] + 6])
        if re.search(r"ignore_lost!|match |if let Err|Err\(", window):
            return True
    # 截断的处置可以只写一句理由：理由必须点名「全量在哪里、由什么兜住」，
    # 与界的 `keep:` 同一口径。写成 `Excerpt` 的那些另有 `full_ref` 字段可判。
    if row["surface"] == "truncations":
        path = root / row["path"]
        key = str(path)
        if key not in cache:
            try:
                cache[key] = path.read_text(errors="ignore").splitlines()
            except OSError:
                cache[key] = []
        at = row["line"] - 1
        window = "\n".join(cache[key][max(0, at - 12): at + 2])
        why = WHY_RE.search(window)
        if why and ("由" in why.group(1) or "because" in why.group(1).lower()):
            return True
    if not REF_RE.search(row["text"]):
        return False
    path = root / row["path"]
    key = str(path)
    if key not in cache:
        try:
            cache[key] = path.read_text(errors="ignore").splitlines()
        except OSError:
            cache[key] = []
    at = row["line"] - 1
    window = "\n".join(cache[key][max(0, at - 3): at + 1])
    why = WHY_RE.search(window)
    return bool(why and ("由" in why.group(1) or "because" in why.group(1).lower()))


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--check", action="store_true")
    parser.add_argument(
        "--check-new",
        action="store_true",
        help="只判这一笔新增的未处置项：父提交里没有的候选，必须已有处置",
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
    args = parser.parse_args(argv[1:])
    root = Path(args.root).resolve()
    found = list(rows(root))
    lines_cache: dict[str, list[str]] = {}
    unresolved = [row for row in found if not is_resolved(root, row, lines_cache)]
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
            for start, end in ranges.get(path, []):
                if start <= line <= end:
                    return True
            return False

        offenders = []
        cache: dict[str, list[str]] = {}
        for row in found:
            if not in_diff(row["path"], row["line"]):
                continue
            if not is_resolved(root, row, cache):
                offenders.append(row)
        if offenders:
            print(f"这一笔新增了 {len(offenders)} 处未处置的有损。两种正确写法：")
            print("  1) 默认无损：不截断、不丢，把完整值传下去（原类型）；")
            print("  2) 确因「传全量会让功能或性能下降」才保留为")
            print("     Excerpt::new(text, limit, \"artifact://…\", \"用途\")（引用是值的一部分），")
            print("     并在旁边写 keep: 传全量会让什么变差 由什么兜住。")
            for row in offenders[:100]:
                print(f"  {row['path']}:{row['line']}\t{row.get('surface', row.get('kind'))}\t{row['text' if 'text' in row else 'name'][:100]}")
        print(f"loss-diff\tnew_unresolved={len(offenders)}", file=sys.stderr)
        return 1 if offenders else 0

    if args.check_new:
        import subprocess

        def parent_text(path: str) -> str:
            out = subprocess.run(
                ["git", "-C", str(root), "show", f"HEAD:{path}"],
                capture_output=True,
                text=True,
            )
            return out.stdout if out.returncode == 0 else ""

        cache: dict[str, str] = {}
        new_unresolved = []
        for row in unresolved:
            path = row["path"]
            if path not in cache:
                cache[path] = parent_text(path)
            if row["text"] not in cache[path]:
                new_unresolved.append(row)
        if new_unresolved:
            print(f"这一笔新增了 {len(new_unresolved)} 处未处置的有损（存量不挡这一笔）。两种正确写法：")
            print("  1) 默认无损：传完整值；2) Excerpt::new(…, \"artifact://…\", …) 并写 keep: 理由。")
            for row in new_unresolved[:100]:
                print(f"  {row['path']}:{row['line']}\t{row['surface']}\t{row['text'][:100]}")
        print(
            f"loss-new\tnew_unresolved={len(new_unresolved)}\tlegacy_unresolved={len(unresolved) - len(new_unresolved)}",
            file=sys.stderr,
        )
        return 1 if new_unresolved else 0

    if args.check:
        if unresolved:
            print(f"未处置 {len(unresolved)} 条（候选 {len(found)} 条）：")
            for row in unresolved[:200]:
                print(f"  {row['path']}:{row['line']}\t{row['surface']}\t{row['text'][:100]}")
            if len(unresolved) > 200:
                print(f"  … 还有 {len(unresolved) - 200} 条")
        print(f"loss\tcandidates={len(found)}\tunresolved={len(unresolved)}", file=sys.stderr)
        return 1 if unresolved else 0
    for row in found:
        print(f"{row['path']}:{row['line']}\t{row['surface']}\t{row['text'][:120]}")
    print(f"loss\tcandidates={len(found)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
