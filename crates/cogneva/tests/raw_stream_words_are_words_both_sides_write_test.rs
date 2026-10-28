//! principle: feedback_no_hardcoding_config_surface
//!
//! 原始流水（raw stream）的“流名”是一个闭集，写在一个 `enum` 的一份 `as_str` 里：
//! `RawStreamName::Task => "task_raw"`。而真正落盘、真正被读的那一侧完全不经过这个
//! 枚举——生产者是在自己的 crate 里手打一个字符串塞进 `RawMeta.stream`：
//!
//! ```text
//! cog-orchestrator  stream: "task_raw".into()
//! cog-agent         Some("agent_raw") / Some("tool_raw")
//! cog-llm           stream: "llm_raw".into()        （两个 provider 各两处）
//! cog-gateway       websocket_record("session_raw", …) / "transport_raw"
//! cog-observability stream: "system_raw".into()
//! cog-gateway       read_proto("transport_raw")      ← 这一条是读
//! ```
//!
//! `cog_storage::FileRawLogger` 把记录写进以 `meta.stream` 命名的目录，`read_proto`
//! 按调用方给的词去扫那个目录。所以两边拼错任何一边，后果都是**读到一个空集**：
//! 没有报错、没有日志、没有指标，`get_raw_log_index` 返回 `[]`，与「这一类事件今天
//! 没发生过」逐字节同形。这不是理论风险——`trace_fanout` 现在就在按 `"transport_raw"`
//! 这个词读。
//!
//! 本门做四件事，四件事在今天的树上都成立：
//!
//! 1. `RawStreamName` 的每个变体在 `as_str` 里都有臂（少一条臂 = 那个变体没有词）；
//! 2. 声明的每个词都有生产者在写（否则按这个名字读的地方永远是空的）；
//! 3. 生产者写的每个 `*_raw` 词都在声明里（否则按声明读的人读不到它）；
//! 4. 每个读方要的词都在声明里、且有人写。
//!
//! 判不了什么：读方不只是代码里的字面量——`GET` 的原始流水接口把 `stream` 当查询参数
//! 收进来，调用方传什么就扫什么，本门判不了那段自由输入；也不判磁盘上已经写下的旧
//! 文件名（改名不会搬旧数据）。生产者的识别靠「这个文件里出现了 `RawMeta {`」——
//! 只在真正拼 `RawMeta` 的地方认词，所以别处同形的字符串（指标名
//! `memory_unextracted_raw`、工具名 `call_raw`）不会被误判。
//!
//! 实测（2026-10-27）：声明 7 个词，7 个词都有生产者在写（10 个文件里拼过 `RawMeta`，
//! 跨 5 个 crate）；声明自己还被 `file_name`／`scan_dir`／`stream_name` 四处拿来拼文件名
//! 与索引；代码内的读者 1 处（`trace_fanout` 按 `transport_raw` 读）；四向全对。
//!
//! 消融：把 `crates/cog-orchestrator/src/dag_executor/orchestrator.rs` 里
//! `stream: "task_raw"` 改成 `stream: "taskx_raw"`（单边改名，声明没动），编译照过，本门
//! 红两条——一条说声明里的 `task_raw` 从此没有生产者在写，一条说 `orchestrator` 写的
//! `taskx_raw` 不在声明里；改回即绿。（改声明一侧是对称的：`as_str` 换词，同样的两条。）

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::rust_text::trailing_identifier;

use common::routes::{matching_brace, repo_root};
use common::rust_production::{
    crate_tree_sources, production_text, relative_name, sources_of_crate_tree,
};

const DECLARATION: &str = "crates/cog-observability/src/lib.rs";
const ENUM: &str = "RawStreamName";

/// 声明处的两半：`enum` 列了什么变体，`as_str` 给每个变体写了哪个词。
fn declared(file: &str, source: &str) -> Result<BTreeMap<String, String>, String> {
    let variants = enum_variants(file, source)?;
    let body = as_str_body(file, source)?;
    let mut words = BTreeMap::new();
    let mut cursor = 0;
    while let Some(offset) = body[cursor..].find("=>") {
        let arrow = cursor + offset;
        cursor = arrow + 2;
        let name = trailing_identifier(&body[..arrow]);
        if !variants.contains(&name) {
            continue;
        }
        let word = first_literal(&body[cursor..]).ok_or_else(|| {
            format!("  `{file}` 的 `{ENUM}::{name}` 在 `as_str` 里读不出词：读不出来按红处理")
        })?;
        words.insert(name, word);
    }
    for variant in &variants {
        if !words.contains_key(variant) {
            return Err(format!(
                "  `{file}` 的 `{ENUM}::{variant}` 在 `as_str` 里没有臂：这个变体没有词，\
                 而丢臂不会有任何报错"
            ));
        }
    }
    Ok(words)
}

fn enum_variants(file: &str, source: &str) -> Result<BTreeSet<String>, String> {
    let head = format!("enum {ENUM}");
    let at = source
        .find(&head)
        .ok_or_else(|| format!("  `{file}` 里找不到 `enum {ENUM}`：声明读不出来，本门判不了"))?;
    let open = source[at..]
        .find('{')
        .map(|i| at + i)
        .ok_or_else(|| format!("  `{file}` 的 `enum {ENUM}` 没有函数体"))?;
    let close = matching_brace(source.as_bytes(), open)
        .ok_or_else(|| format!("  `{file}` 的 `enum {ENUM}` 括号不闭合：读不出来按红处理"))?;
    let mut variants = BTreeSet::new();
    for part in split_items(&source[open + 1..close]) {
        let mut part = part.trim();
        while part.starts_with("#[") {
            let Some(end) = part.find(']') else { break };
            part = part[end + 1..].trim_start();
        }
        let name: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name.chars().next().is_some_and(|c| c.is_uppercase()) {
            variants.insert(name);
        }
    }
    if variants.is_empty() {
        return Err(format!("  `{file}` 的 `enum {ENUM}` 一个变体都没读出来"));
    }
    Ok(variants)
}

fn as_str_body(file: &str, source: &str) -> Result<String, String> {
    let head = format!("impl {ENUM}");
    let at = source.find(&head).ok_or_else(|| {
        format!("  `{file}` 里找不到 `impl {ENUM}`：`as_str` 读不出来，本门判不了")
    })?;
    let open = source[at..]
        .find('{')
        .map(|i| at + i)
        .ok_or_else(|| format!("  `{file}` 的 `impl {ENUM}` 没有函数体"))?;
    let close = matching_brace(source.as_bytes(), open)
        .ok_or_else(|| format!("  `{file}` 的 `impl {ENUM}` 括号不闭合：读不出来按红处理"))?;
    let body = &source[open + 1..close];
    let fn_at = body.find("fn as_str").ok_or_else(|| {
        format!("  `{file}` 的 `impl {ENUM}` 里没有 `as_str`：词表读不出来，本门判不了")
    })?;
    let brace = body[fn_at..]
        .find('{')
        .map(|i| fn_at + i)
        .ok_or_else(|| format!("  `{file}` 的 `{ENUM}::as_str` 没有函数体"))?;
    let end = matching_brace(body.as_bytes(), brace)
        .ok_or_else(|| format!("  `{file}` 的 `{ENUM}::as_str` 括号不闭合：读不出来按红处理"))?;
    Ok(body[brace + 1..end].to_string())
}

/// 顶层逗号切分（字符串里的逗号不算）。
fn split_items(body: &str) -> Vec<&str> {
    let bytes = body.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(&body[start..i]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    parts.push(&body[start..]);
    parts
}

/// 从 `text` 的第一个 `"` 起读到收尾的那个，返回词。
fn first_literal(text: &str) -> Option<String> {
    let open = text.find('"')?;
    let bytes = text.as_bytes();
    let mut i = open + 1;
    let mut word = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => {
                i += 1;
                if i < bytes.len() {
                    word.push(bytes[i] as char);
                }
            }
            b'"' => return Some(word),
            other => word.push(other as char),
        }
        i += 1;
    }
    None
}

/// 一个 `*_raw` 形状的词：`session_raw` 算，`memory_dead_letter_raw` 不算（两段以上
/// 的名字不是流名，是指标名一类的别的词）。
fn is_stream_word(word: &str) -> bool {
    let Some(stem) = word.strip_suffix("_raw") else {
        return false;
    };
    !stem.is_empty()
        && stem
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// 一段生产代码里的字面量（转义按 Rust 的算法跳过）。
fn literals(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'"' {
            i += 1;
            continue;
        }
        i += 1;
        let mut word = String::new();
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' {
                i += 1;
            }
            if i < bytes.len() {
                word.push(bytes[i] as char);
            }
            i += 1;
        }
        found.push(word);
        i += 1;
    }
    found
}

/// 一段生产代码里 `*_raw` 形状的字面量。
fn stream_words(source: &str) -> BTreeSet<String> {
    literals(source)
        .into_iter()
        .filter(|word| is_stream_word(word))
        .collect()
}

/// 这段代码向 `read_proto` 要的词。读不出来（参数不是字面量）就是读不出来，不猜。
fn read_words(file: &str, source: &str) -> Result<BTreeSet<String>, String> {
    let mut words = BTreeSet::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("read_proto(") {
        let at = cursor + offset;
        cursor = at + "read_proto(".len();
        // `fn read_proto(…)` 是定义，不是读。
        if source[..at].ends_with("fn ") {
            continue;
        }
        let rest = &source[cursor..];
        let end = rest.find(')').unwrap_or(rest.len());
        let arg = rest[..end].trim();
        // 带接收者的那一份是定义处的默认体，不是调用方给的词。
        if arg.starts_with("&self") || arg.starts_with("self") {
            continue;
        }
        let Some(word) = first_literal(arg) else {
            return Err(format!(
                "  `{file}` 向 `read_proto` 要的流名不是字面量（`{arg}`）：读不出来按红处理"
            ));
        };
        if !is_stream_word(&word) {
            return Err(format!(
                "  `{file}` 向 `read_proto` 要的 `{word}` 不是 `*_raw` 形状的词：读不出来按红处理"
            ));
        }
        words.insert(word);
    }
    Ok(words)
}

/// 这个文件是不是生产者：它拼过 `RawMeta {`（声明本身不算）。
fn is_producer(source: &str) -> bool {
    source
        .replace("struct RawMeta {", "struct RawMeta;")
        .contains("RawMeta {")
}

fn contradictions(root: &Path, sources: &[PathBuf]) -> Vec<String> {
    let mut complaints = Vec::new();
    let declaration_path = root.join(DECLARATION);
    let declaration_raw = match std::fs::read_to_string(&declaration_path) {
        Ok(raw) => raw,
        Err(e) => {
            return vec![format!(
                "  `{DECLARATION}` 读不出来（{e}）：声明读不出来，本门判不了这一棵树"
            )]
        }
    };
    let declared = match declared(DECLARATION, &production_text(&declaration_raw)) {
        Ok(declared) => declared,
        Err(complaint) => return vec![complaint],
    };
    let words: BTreeSet<String> = declared.values().cloned().collect();
    if words.len() != declared.len() {
        complaints.push(format!(
            "  `{DECLARATION}` 的 `{ENUM}` 有两个变体写了同一个词：两个流会落进同一批文件"
        ));
    }

    let mut writes: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut reads: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut producers = BTreeSet::new();
    for path in sources {
        let relative = relative_name(root, path);
        let Ok(raw) = std::fs::read_to_string(path) else {
            complaints.push(format!("  `{relative}` 读不出来：读不出来按红处理"));
            continue;
        };
        let source = production_text(&raw);
        // 声明本身不是生产者：`as_str` 里的词是词表，不是写出去的记录。
        let producer_text = if relative == DECLARATION {
            match as_str_body(&relative, &source) {
                Ok(body) => source.replace(&body, ""),
                Err(complaint) => {
                    complaints.push(complaint);
                    continue;
                }
            }
        } else {
            source.clone()
        };
        if is_producer(&producer_text) {
            producers.insert(relative.clone());
            for word in stream_words(&producer_text) {
                writes.entry(word).or_default().insert(relative.clone());
            }
        }
        match read_words(&relative, &source) {
            Ok(found) => {
                for word in found {
                    reads.entry(word).or_default().insert(relative.clone());
                }
            }
            Err(complaint) => complaints.push(complaint),
        }
    }

    for (variant, word) in &declared {
        if !writes.contains_key(word) {
            complaints.push(format!(
                "  `{ENUM}::{variant}` 称这个词是 `{word}`，却没有任何生产者在写它：按这个名字\
                 读的地方永远读到空，而空与「这一类事件没发生过」是同一个读数"
            ));
        }
    }
    for (word, files) in &writes {
        if !words.contains(word) {
            complaints.push(format!(
                "  {} 写的 `{word}` 不在 `{ENUM}` 的声明里：按声明读的人永远读不到这一条流",
                files.iter().cloned().collect::<Vec<_>>().join("、")
            ));
        }
    }
    for (word, files) in &reads {
        let named = files.iter().cloned().collect::<Vec<_>>().join("、");
        if !words.contains(word) {
            complaints.push(format!(
                "  {named} 向 `read_proto` 要的 `{word}` 不在 `{ENUM}` 的声明里"
            ));
        }
        if !writes.contains_key(word) {
            complaints.push(format!(
                "  {named} 向 `read_proto` 要的 `{word}` 没有任何生产者在写：这个读者拿到的\
                 永远是空集，而空集与「没有事件」同形"
            ));
        }
    }

    let written: usize = writes.len();
    if words.len() < 5 || written < 5 || producers.len() < 4 || reads.is_empty() {
        complaints.push(format!(
            "普查只读到 {} 个声明词、{written} 个被写的词、{} 个生产者文件、{} 个读者：\
             读得太少，本门判不了这一棵树，按红处理",
            words.len(),
            producers.len(),
            reads.len()
        ));
    }
    complaints
}

#[test]
fn every_raw_stream_word_is_a_word_both_sides_write() {
    let complaints = contradictions(&repo_root(), &crate_tree_sources(&repo_root()));
    assert!(
        complaints.is_empty(),
        "原始流水的流名写在 `{ENUM}` 的 `as_str` 里，而生产者在自己的 crate 里手打字符串\
         塞进 `RawMeta.stream`：两边拼错任何一边，读者拿到的都是空集，与「没有事件」\
         逐字节同形。\n{}",
        complaints.join("\n")
    );
}

const FIXTURE_DECLARATION: &str = "\
pub enum RawStreamName {
    Session,
    Task,
}

impl RawStreamName {
    pub fn as_str(&self) -> &'static str {
        match self {
            RawStreamName::Session => \"session_raw\",
            RawStreamName::Task => \"task_raw\",
        }
    }
}
";

const FIXTURE_WRITER: &str = "\
pub fn session() -> RawRecord {
    RawRecord { meta: RawMeta { stream: \"session_raw\".into() } }
}

pub fn task() -> RawRecord {
    RawRecord { meta: RawMeta { stream: \"task_raw\".into() } }
}
";

const FIXTURE_READER: &str = "\
pub async fn read(logger: &dyn Logger) {
    let _ = logger.read_proto(\"session_raw\").await;
}
";

fn write_fixture(root: &Path, declaration: &str, writer: &str, reader: &str) {
    let declaration_path = root.join(DECLARATION);
    std::fs::create_dir_all(declaration_path.parent().expect("parent")).expect("dir");
    std::fs::write(declaration_path, declaration).expect("write");
    let writer_path = root.join("crates/demo/src/writer.rs");
    std::fs::create_dir_all(writer_path.parent().expect("parent")).expect("dir");
    std::fs::write(writer_path, writer).expect("write");
    let reader_path = root.join("crates/demo/src/reader.rs");
    std::fs::write(reader_path, reader).expect("write");
}

fn fixture_complaints(declaration: &str, writer: &str, reader: &str) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    write_fixture(dir.path(), declaration, writer, reader);
    // 夹具只有两个词，普查下限按真树定，所以这里只取「词对不上」那一类判断。
    contradictions(dir.path(), &sources_of_crate_tree(dir.path()))
        .into_iter()
        .filter(|c| !c.contains("普查只读到"))
        .collect()
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    assert_eq!(
        fixture_complaints(FIXTURE_DECLARATION, FIXTURE_WRITER, FIXTURE_READER),
        Vec::<String>::new()
    );
}

#[test]
fn a_word_a_producer_stopped_writing_is_reported() {
    let writer = FIXTURE_WRITER.replace(
        "    RawRecord { meta: RawMeta { stream: \"task_raw\".into() } }\n",
        "    RawRecord { meta: RawMeta { stream: \"session_raw\".into() } }\n",
    );
    let complaints = fixture_complaints(FIXTURE_DECLARATION, &writer, FIXTURE_READER);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("task_raw") && c.contains("没有任何生产者在写")),
        "{complaints:?}"
    );
}

#[test]
fn a_word_no_declaration_names_is_reported() {
    let writer = FIXTURE_WRITER.replace(
        "    RawRecord { meta: RawMeta { stream: \"task_raw\".into() } }\n",
        "    RawRecord { meta: RawMeta { stream: \"widget_raw\".into() } }\n",
    );
    let complaints = fixture_complaints(FIXTURE_DECLARATION, &writer, FIXTURE_READER);
    assert!(
        complaints.iter().any(|c| c.contains("widget_raw")),
        "{complaints:?}"
    );
}

#[test]
fn a_reader_asking_for_a_stream_nobody_writes_is_reported() {
    let reader = FIXTURE_READER.replace("\"session_raw\"", "\"tool_raw\"");
    let complaints = fixture_complaints(FIXTURE_DECLARATION, FIXTURE_WRITER, &reader);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("tool_raw") && c.contains("空集")),
        "{complaints:?}"
    );
}

#[test]
fn a_variant_without_an_arm_is_reported() {
    let declaration =
        FIXTURE_DECLARATION.replace("            RawStreamName::Task => \"task_raw\",\n", "");
    let complaints = fixture_complaints(&declaration, FIXTURE_WRITER, FIXTURE_READER);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("Task") && c.contains("没有臂")),
        "{complaints:?}"
    );
}

#[test]
fn an_unreadable_declaration_is_reported() {
    let declaration = FIXTURE_DECLARATION.replace("enum RawStreamName", "enum SomethingElse");
    let complaints = fixture_complaints(&declaration, FIXTURE_WRITER, FIXTURE_READER);
    assert!(
        complaints.iter().any(|c| c.contains("判不了")),
        "{complaints:?}"
    );
}
