//! principle: feedback_no_hardcoding_config_surface
//!
//! `CompleteOptions` → `ChatOptions` 的字段映射抄了六遍。
//!
//! 路由器和五个 provider 的 `complete_stream` 里各有一张一模一样的表：四个
//! 字段从调用方给的一组选项搬到 provider 用的另一组，再 `..Default::default()`
//! 补齐。每处都看得见自己在传什么，这是抄它的理由；代价是**加字段的时候没有
//! 任何一处会编译失败**：给 `CompleteOptions` 添一个字段，六张表都还能编译，
//! 新字段在每一个 provider 上静默变成默认值，调用方却以为自己传进去了。
//!
//! 漂移在这里不是「报错不报错」的问题，是「某个旋钮从某一天起不再起作用」：
//! 请求里设了、上游收到的是默认值，两边都成功，日志上什么也看不到。
//!
//! 所以这张表只留一份（`utils::simple_options::chat_options_from`）。本门盯的是
//! 「构造 `ChatOptions` 时不再各写一份字段表」：`crates/cog-llm/src` 的生产代码
//! 里，凡是从 `CompleteOptions` 搬字段的 `ChatOptions { … }` 块，只许有一处。
//!
//! 判不了什么：
//!
//! - 别的来源构造 `ChatOptions` 的地方（测试夹具、`build_base_options` 那种从
//!   `ChatOptions` 到 `ChatOptions` 的变形）。它们不带 `CompleteOptions` 的
//!   字段，不是这张表的副本。
//! - 表里少搬了哪个字段。本门只保证表只有一份；「四个字段对不对」由
//!   `chat_options_from` 自己的单元测试与 provider 的既有测试覆盖。

mod common;

use std::path::Path;

use common::routes::{repo_root, walk};
use common::rust_production::production_lines;

/// 这张表被认出来的标志：从 `CompleteOptions` 搬字段的那几份才有这一行。
///
/// 不带结尾逗号的变体（`max_tokens: options.max_tokens.or(Some(default))`）是
/// 从 `ChatOptions` 到 `ChatOptions` 的变形，不是这张表的副本——它补的是默认
/// 值，不是搬家。
const MARKER: &str = "max_tokens: options.max_tokens,";

/// 允许写出这张表的唯一文件。
const DECLARED_IN: &str = "crates/cog-llm/src/utils/simple_options.rs";

/// 普查下限。测到的那一天 `cog-llm/src` 有 30 余个源文件。
const MIN_FILES: usize = 20;

/// 从 `text` 的 `open` 处（指向 `{`）起按大括号配对截出整块。
fn block_at(text: &str, open: usize) -> String {
    let mut depth = 0i32;
    let mut out = String::new();
    for ch in text[open..].chars() {
        out.push(ch);
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
    }
    out
}

/// 一份源文件里所有「搬家表」块。
fn tables_in(text: &str) -> Vec<String> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(pos) = text[from..].find("ChatOptions {") {
        let start = from + pos;
        let brace = start + "ChatOptions ".len();
        // 函数签名里的 `-> ChatOptions {` 是返回类型，不是一次构造：从这个
        // 大括号起截出来的「块」是整个函数体，会把函数里真正的那张表再数一遍。
        let is_signature = text[..start].trim_end().ends_with("->");
        if !is_signature {
            let block = block_at(text, brace);
            if block.contains(MARKER) {
                found.push(block);
            }
        }
        from = brace + 1;
    }
    found
}

/// 生产行拼起来的一整个文件（跨行块要连着读，所以按文件而不是按行数）。
fn production_text(path: &Path) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    production_lines(&text)
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn census(root: &Path) -> (usize, Vec<(String, String)>) {
    let mut files = Vec::new();
    walk(&root.join("crates/cog-llm/src"), &["rs"], &mut files);
    files.sort();
    let mut found = Vec::new();
    for path in &files {
        let file = path
            .strip_prefix(root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        for block in tables_in(&production_text(path)) {
            found.push((file.clone(), block));
        }
    }
    (files.len(), found)
}

#[test]
fn the_complete_options_table_lives_in_one_place() {
    let (files, found) = census(&repo_root());
    assert!(
        files >= MIN_FILES,
        "只读到 {files} 个源文件（下限 {MIN_FILES}），这条判据没有题目"
    );

    let elsewhere: Vec<&(String, String)> = found
        .iter()
        .filter(|(file, _)| file != DECLARED_IN)
        .collect();
    assert!(
        found.len() == 1 && elsewhere.is_empty(),
        "`CompleteOptions` → `ChatOptions` 的字段表只许有一份（`{DECLARED_IN}`），\
         现在有 {} 处：\n{}\n每一份都带 `..Default::default()`，所以给 \
         `CompleteOptions` 加字段时六处全都编译得过，新字段在每一个 provider 上\
         静默变成默认值。改成调用 \
         `cog_llm::utils::simple_options::chat_options_from(options)`。",
        found.len(),
        found
            .iter()
            .map(|(file, block)| format!(
                "    {file}:\n{}",
                block
                    .lines()
                    .map(|l| format!("        {}", l.trim()))
                    .collect::<Vec<_>>()
                    .join("\n")
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn the_census_finds_a_second_table_and_ignores_one_without_the_marker() {
    let one = concat!(
        "let o = ChatOptions {\n",
        "    model: options.model.clone(),\n",
        "    max_tokens: options.max_tokens,\n",
        "    api_key: options.api_key.clone(),\n",
        "};\n",
    );
    assert_eq!(tables_in(one).len(), 1);

    let two = format!("{one}{one}");
    assert_eq!(tables_in(&two).len(), 2);

    // 不带搬家标志的 `ChatOptions { … }`（从 ChatOptions 到 ChatOptions 的变形）
    // 不是这张表的副本。
    let other = concat!(
        "let o = ChatOptions {\n",
        "    model: options.model.clone().or_else(|| Some(id)),\n",
        "    max_tokens: options.max_tokens.or(Some(4096)),\n",
        "};\n",
    );
    assert_eq!(tables_in(other).len(), 0);
}
