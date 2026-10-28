//! principle: feedback_no_hardcoding_config_surface
//!
//! 一个闭集要把自己写成字符串（上总线、进配置、落数据库、上网页），还要把它读回来。
//! 写的那一半通常是 `as_str()`，读的那一半是同一个类型上的 `parse()` / `from_str()`。
//! 两边各写各的 `match`：编译器不比较两个 `match` 的臂，所以只改一边不会有任何报错。
//!
//! 后果不是一个响亮的事故，而是一个静默的错位：
//!
//! - 写出去的词读不回来 → 读方拿到 `None`／`Err`，落到默认值或「未知」，与「这个值
//!   从来没出现过」同形；配置里明明写着、面板上明明是这个词，行为却是默认的。
//! - 同一个词读回来是**另一个变体** → 值悄悄换了意思，没有任何一处会报错。
//!
//! 本门只判这两条：每个写出去的词，必须能在这个类型的读方里读回**同一个**变体。
//!
//! 写方有两处：`as_str` 的臂（字面量，或同文件里的 `const NAME: &str = "词";`／
//! `const A: &str = other::B;` 这类按全树解一层的别名），以及 `Display::fmt` 里写纯
//! 词的臂（`RuntimeState`／`IpcChannel` 这类不实现 `as_str`、直接把词写在
//! `write!(f, "idle")` 里的类型）。同一个类型两边都有写法时只看 `as_str`——`Display`
//! 那份多半就是转调它（`f.write_str(self.as_str())`）。
//!
//! 读方只在它**自己拼了词表**时才比：读方体里出现 `as_str(` 或 `ALL` 的那些是从写方
//! 派生的（`ClusterHealth::parse` 去 `ALL` 里逐个比 `as_str()`），没有第二份词表，
//! 天然不会漂移；它们在普查里单独计数，`ALL` 的全集由别的门管。
//!
//! 行内臂只判一个方向：树上一整臂 `"词" => Owner::变体`（读方不必挂在 `parse` 名下，
//! `RuntimeState` 的词就是 `restore` 里那段 `match state_str` 读回来的）不保证捡全，
//! 所以它只用来判「同一个词读回来是另一个变体」，不用来判「读方不认这个词」。
//!
//! 判不了什么：不判读方接受但不写的词（别名是正当的：`IngestPurpose::parse` 把空串
//! 当 `Learn`，那是给旧调用方的兼容面）；不判大小写折叠（手写读方在匹配前 `trim`／
//! `to_lowercase` 不影响臂里的字面量）；不判 `serde` 的 `rename`／`rename_all`——那是
//! 另一个读者，词不经过 `as_str`，本门看不见它。同一个类型名在两处出现而词表不同，
//! 本门分不出来，直接红。
//!
//! 实测（2026-10-27）：13 个类型两边都在本门视野里（另有 9 个只有具名读方、没有写方，
//! 31 个读方是派生的），18 个读方自己拼了词表，一共 52 个写出去的词，没有一个读不回来。
//! 落这一门时 `RuntimeState` 正好是词写两遍的样子——`Display` 一份、`restore` 里行内
//! 一份——两份都已改成从 `RuntimeState::as_str` 取，于是它落进「派生读方」那一栏。
//! 消融：把 `crates/cog-gateway/src/memory.rs` 的 `IngestPurpose::as_str` 里
//! `Self::Learn => "learn"` 改成 `"learning"`，编译通过、本门红并点名 `learning`；改回
//! 绿。反方向同样红：把 `parse` 的 `"archive"` 改成 `"archived"`，本门红并点名
//! `archive`。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::rust_text::{raw_string_end, split_top_level, trailing_identifier};

use common::routes::repo_root;
use common::rust_production::{
    crate_tree_sources, production_text, relative_name, sources_of_crate_tree,
};

const READER_FNS: [&str; 2] = ["from_str", "parse"];

/// 一个类型的两份词表。
#[derive(Default)]
struct Words {
    /// 变体 →（写出去的词，写在哪个文件，这条是不是 `Display` 写的）
    written: BTreeMap<String, (String, String, bool)>,
    /// 具名读方（`parse`／`from_str`）认的词 →（读出来的变体，读方在哪个文件）。这是
    /// 完整词表：写出去的词不在这张表里，就是读不回来。
    read: BTreeMap<String, (String, String)>,
    /// 行内 `"词" => Owner::变体` 攒出来的词 →（变体，文件）。**不全**：这是从整棵树上
    /// 捡来的臂，没有谁能保证同一类型的词都在这儿，所以只用它判「同一个词两个意思」。
    seen: BTreeMap<String, (String, String)>,
}

/// 从 `from` 起，第一个不在字符串里的 `{`。
fn open_brace_after(text: &str, from: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut i = from;
    while i < bytes.len() {
        if let Some(end) = raw_string_end(bytes, i) {
            i = end;
            continue;
        }
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
            b'{' => return Some(i),
            _ => {}
        }
        i += 1;
    }
    None
}

/// 从 `open`（一个 `{`）起找配对的 `}`，跳过字符串、字符字面量、生命周期与原始字符串。
fn body_end(text: &str, open: usize) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    while i < bytes.len() {
        if let Some(end) = raw_string_end(bytes, i) {
            i = end;
            continue;
        }
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                if i >= bytes.len() {
                    return None;
                }
            }
            b'\'' => {
                if i + 2 < bytes.len() && bytes[i + 2] == b'\'' {
                    i += 2;
                } else if i + 3 < bytes.len() && bytes[i + 1] == b'\\' && bytes[i + 3] == b'\'' {
                    i += 3;
                } else {
                    i += 1;
                    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_')
                    {
                        i += 1;
                    }
                    continue;
                }
            }
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// 一段源码里每个 `impl <Name>` / `impl <Trait> for <Name>` 的（类型名，函数体）。
fn impl_blocks(file: &str, source: &str) -> Result<Vec<(String, String)>, String> {
    let mut blocks = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("impl") {
        let at = cursor + offset;
        cursor = at + "impl".len();
        if source[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            continue;
        }
        let mut rest = source[cursor..].trim_start();
        if rest.starts_with('<') {
            let Some(close) = rest.find('>') else {
                continue;
            };
            rest = rest[close + 1..].trim_start();
        }
        let first = leading_type(rest);
        if first.is_empty() {
            continue;
        }
        let after_first = rest[first.len()..].trim_start();
        let name = if let Some(tail) = after_first.strip_prefix("for") {
            let target = leading_type(tail.trim_start());
            if target.is_empty() {
                continue;
            }
            target
        } else {
            first
        };
        let Some(open) = open_brace_after(source, source.len() - rest.len()) else {
            continue;
        };
        let close = body_end(source, open)
            .ok_or_else(|| format!("  `{file}` 的 `impl {name}` 括号不闭合：读不出来按红处理"))?;
        cursor = close;
        if name.chars().next().is_some_and(|c| c.is_uppercase()) {
            blocks.push((name, source[open + 1..close].to_string()));
        }
    }
    Ok(blocks)
}

/// 一段文字开头的类型／路径名（`Foo`、`Self::Foo`、`std::foo::Bar`）。
fn leading_type(text: &str) -> String {
    text.trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect()
}

/// `impl` 体里某个函数的函数体。
fn fn_body(body: &str, names: &[&str]) -> Option<String> {
    for name in names {
        let head = format!("fn {name}(");
        let Some(at) = body.find(&head) else { continue };
        let Some(open) = open_brace_after(body, at) else {
            continue;
        };
        let Some(close) = body_end(body, open) else {
            continue;
        };
        return Some(body[open + 1..close].to_string());
    }
    None
}

/// `match … { … }` 里那个花括号的内部。臂在花括号里，不剥掉这一层，顶层逗号切分就
/// 等于没切——整段 `match` 会被当成一臂。
fn match_body(body: &str) -> String {
    if let Some(at) = body.find("match ") {
        if let Some(open) = open_brace_after(body, at) {
            if let Some(close) = body_end(body, open) {
                return body[open + 1..close].to_string();
            }
        }
    }
    body.to_string()
}

fn literals(text: &str) -> Vec<String> {
    let bytes = text.as_bytes();
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

/// `Self::X`／`X::Y::Z`／`Ok(Self::X)` 里那个大写开头的变体名。
fn leading_variant(text: &str) -> Option<String> {
    let text = text.trim_start();
    let rest = text
        .strip_prefix("Ok(")
        .or_else(|| text.strip_prefix("Some("))
        .unwrap_or(text);
    let name = leading_type(rest);
    let last = name.rsplit("::").next().unwrap_or("").to_string();
    if last.chars().next().is_some_and(|c| c.is_uppercase()) {
        Some(last)
    } else {
        None
    }
}

/// 一个标识符（常量名不含 `::`）。
fn identifier_at(text: &str) -> String {
    text.trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// 本文件里的常量声明：名字 → 等号右边那段文字。
fn const_decls(source: &str) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("const ") {
        let at = cursor + offset + "const ".len();
        cursor = at;
        let name = identifier_at(&source[at..]);
        if name.is_empty() {
            continue;
        }
        let after = &source[at + name.len()..];
        let Some(equals) = after.find('=') else {
            continue;
        };
        let tail = &after[equals + 1..];
        let Some(end) = tail.find(';') else { continue };
        found.push((name, tail[..end].to_string()));
    }
    found
}

/// 全树常量表：本文件优先，其次全树。`const A: &str = other::B;` 这类别名按全树解一层。
#[derive(Default)]
struct Consts {
    by_file: BTreeMap<String, BTreeMap<String, String>>,
    global: BTreeMap<String, String>,
}

impl Consts {
    fn build(sources: &[(String, String)]) -> Consts {
        let mut literal: BTreeMap<String, String> = BTreeMap::new();
        let mut decls: Vec<(String, String, String)> = Vec::new();
        for (file, source) in sources {
            for (name, rhs) in const_decls(source) {
                if let Some(word) = literals(&rhs).into_iter().next() {
                    literal.entry(name.clone()).or_insert(word);
                }
                decls.push((file.clone(), name, rhs));
            }
        }
        let mut by_file: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for (file, name, rhs) in &decls {
            let word = literal.get(name).cloned().or_else(|| {
                let target = trailing_identifier(rhs);
                literal.get(&target).cloned()
            });
            if let Some(word) = word {
                by_file
                    .entry(file.clone())
                    .or_default()
                    .insert(name.clone(), word.clone());
                if !literal.contains_key(name) {
                    continue;
                }
                literal.insert(name.clone(), word);
            }
        }
        Consts {
            by_file,
            global: literal,
        }
    }

    fn get(&self, file: &str, name: &str) -> Option<&String> {
        self.by_file
            .get(file)
            .and_then(|map| map.get(name))
            .or_else(|| self.global.get(name))
    }
}

/// `as_str` 的臂：变体 → 它写出去的那个词。
fn written_words(
    file: &str,
    owner: &str,
    body: &str,
    consts: &Consts,
) -> Result<Vec<(String, String)>, String> {
    let mut arms = Vec::new();
    let arms_body = match_body(body);
    for arm in split_top_level(&arms_body) {
        let Some(arrow) = arm.find("=>") else {
            continue;
        };
        let variant = trailing_identifier(&arm[..arrow]);
        if !variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            continue;
        }
        let right = &arm[arrow + 2..];
        let word = match literals(right).into_iter().next() {
            Some(word) => word,
            None => {
                let name = trailing_identifier(right);
                match consts.get(file, &name) {
                    Some(word) => word.clone(),
                    None => {
                        return Err(format!(
                            "  `{file}` 的 `{owner}::as_str` 里 `{variant}` 那一臂写的是 \
                             `{name}`，本文件里找不到它的字面量：读不出来按红处理"
                        ))
                    }
                }
            }
        };
        arms.push((variant, word));
    }
    Ok(arms)
}

/// 一个词能不能当词用：没有空白、没有格式化占位符。
fn is_plain_word(word: &str) -> bool {
    !word.is_empty()
        && !word.chars().any(char::is_whitespace)
        && !word.contains('{')
        && !word.contains('}')
}

/// `Display::fmt` 的臂：变体 → 它写出去的那个词。
///
/// `as_str` 不是唯一的写方：`IpcChannel`、`RuntimeState` 这类直接实现 `Display` 的类型
/// 把词写在 `write!(f, "idle")` 里，而 `restore` 又把同一批词拼一遍读回来。只有纯词的
/// 臂算数——`write!(f, "state {} / {}", a, b)` 那种不是词表，读不出词就不看它。
fn display_words(body: &str) -> Vec<(String, String)> {
    let mut arms = Vec::new();
    for arm in split_top_level(&match_body(body)) {
        let Some(arrow) = arm.find("=>") else {
            continue;
        };
        let variant = trailing_identifier(&arm[..arrow]);
        if !variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            continue;
        }
        let Some(word) = literals(&arm[arrow + 2..]).into_iter().next() else {
            continue;
        };
        if is_plain_word(&word) {
            arms.push((variant, word));
        }
    }
    arms
}

/// 同一文件里任何一臂「字面量 → `Owner::变体`」。
///
/// 读方不必挂在 `parse`／`from_str` 名下：`RuntimeState` 的词由 `Display` 写出去，
/// 由 `restore` 里一段行内的 `match state_str { "idle" => RuntimeState::Idle, … }`
/// 读回来。只认显式写了类型名的那一侧，且臂头不许带比较号——`if s == X::Y` 不是一臂。
fn explicit_arms(source: &str, owner: &str) -> Vec<(String, String)> {
    let mut arms = Vec::new();
    let needle = format!("{owner}::");
    for (at, _) in source.match_indices(&needle) {
        let variant = identifier_at(&source[at + needle.len()..]);
        if !variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            continue;
        }
        let head_start = source[..at]
            .rfind([',', '{', '}', ';'])
            .map(|p| p + 1)
            .unwrap_or(0);
        let head = &source[head_start..at];
        // 臂的模式在最近一个 `=>` 之前；再往前还有 `=>` 就说明跨了一臂。
        let pattern = match head.rfind("=>") {
            Some(arrow) => &head[..arrow],
            None => head,
        };
        if pattern.contains("=>") || pattern.contains('=') {
            continue;
        }
        for word in literals(pattern) {
            if is_plain_word(&word) {
                arms.push((word, variant.clone()));
            }
        }
    }
    arms
}

/// 读方的臂：它认的词 → 读出来的变体。只返回字面量在左、变体在右的那些臂。
fn read_words(body: &str) -> Vec<(String, String)> {
    let mut arms = Vec::new();
    let arms_body = match_body(body);
    for arm in split_top_level(&arms_body) {
        let Some(arrow) = arm.find("=>") else {
            continue;
        };
        let words = literals(&arm[..arrow]);
        if words.is_empty() {
            continue;
        }
        let Some(variant) = leading_variant(&arm[arrow + 2..]) else {
            continue;
        };
        for word in words {
            arms.push((word, variant.clone()));
        }
    }
    arms
}

fn contradictions(root: &Path, files: &[PathBuf]) -> Vec<String> {
    let mut complaints = Vec::new();
    // 类型名 → 两份词表。一个类型的 `as_str` 与读方可以在不同的 `impl` 里，也可以在同
    // 一个，所以按类型名合。
    let mut tables: BTreeMap<String, Words> = BTreeMap::new();
    let mut derived: BTreeSet<String> = BTreeSet::new();
    let mut duplicated: BTreeSet<String> = BTreeSet::new();
    let mut sources: Vec<(String, String)> = Vec::new();
    for path in files {
        let relative = relative_name(root, path);
        let Ok(raw) = std::fs::read_to_string(path) else {
            complaints.push(format!("  `{relative}` 读不出来：读不出来按红处理"));
            continue;
        };
        let source = production_text(&raw);
        sources.push((relative, source));
    }
    // 常量要在全树上先扫一遍：`const A: &str = other::B;` 这种别名自己不在本文件里。
    let consts = Consts::build(&sources);
    for (relative, source) in &sources {
        let relative = relative.clone();
        let source = source.clone();
        let blocks = match impl_blocks(&relative, &source) {
            Ok(blocks) => blocks,
            Err(complaint) => {
                complaints.push(complaint);
                continue;
            }
        };
        for (owner, body) in blocks {
            let mut arms: Vec<(String, String, bool)> = Vec::new();
            if let Some(as_str) = fn_body(&body, &["as_str"]) {
                match written_words(&relative, &owner, &as_str, &consts) {
                    Ok(written) => {
                        arms = written
                            .into_iter()
                            .map(|(variant, word)| (variant, word, false))
                            .collect();
                    }
                    Err(complaint) => complaints.push(complaint),
                }
            }
            // `as_str` 是正本：同一个类型两边都有写法时只看 `as_str`，`Display` 那份
            // 多半就是转调它（`f.write_str(self.as_str())`）。
            if arms.is_empty() {
                if let Some(fmt) = fn_body(&body, &["fmt"]) {
                    arms = display_words(&fmt)
                        .into_iter()
                        .map(|(variant, word)| (variant, word, true))
                        .collect();
                }
            }
            for (variant, word, from_display) in arms {
                let table = tables.entry(owner.clone()).or_default();
                match table.written.get(&variant).cloned() {
                    None => {
                        table
                            .written
                            .insert(variant, (word, relative.clone(), from_display));
                    }
                    Some((existing, other, existing_display)) => {
                        if existing_display && !from_display {
                            table
                                .written
                                .insert(variant, (word, relative.clone(), false));
                        } else if !existing_display && from_display {
                            // `as_str` 已经写过这个词了，`Display` 那份让位。
                        } else if existing != word {
                            complaints.push(format!(
                                "  `{owner}::{variant}` 有两份词表：`{other}` 写 `{existing}`、\
                                 `{relative}` 写 `{word}`：同名类型，本门分不出来它俩"
                            ));
                        }
                    }
                }
            }
            // 行内读方：同一文件里任何一臂「字面量 → `Owner::变体`」都算它认这个词。
            // 具名的 `parse`／`from_str` 后写，覆盖同义词表（那份是正本）。
            for (word, variant) in explicit_arms(&source, &owner) {
                tables
                    .entry(owner.clone())
                    .or_default()
                    .seen
                    .entry(word)
                    .or_insert_with(|| (variant, relative.clone()));
            }
            let Some(reader) = fn_body(&body, &READER_FNS) else {
                continue;
            };
            // 派生读方：词从写方来，没有第二份词表。只要读方里出现 `as_str()` 或
            // `ALL`，就说明它有一臂是回写方取的词（`other => Self::ALL.iter()...`），
            // 那一臂认的词本门读不出来，按派生处理比按缺失报红更不容易误伤。
            if reader.contains("as_str(") || reader.contains("ALL") {
                derived.insert(owner);
                continue;
            }
            let arms = read_words(&reader);
            if arms.is_empty() {
                // 不是词表：`Version::parse` 解析的是版本号，`ListParser::parse` 解析的
                // 是清单。没有「词 → 变体」的臂就没有第二份词表，本门不看它。
                continue;
            }
            duplicated.insert(owner.clone());
            for (word, variant) in arms {
                tables
                    .entry(owner.clone())
                    .or_default()
                    .read
                    .insert(word, (variant, relative.clone()));
            }
        }
    }

    let mut judged = 0usize;
    let mut words = 0usize;
    let mut read_only = 0usize;
    for (owner, table) in &tables {
        // 两边都没读到词的类型不在本门范围内。
        if table.read.is_empty() && table.seen.is_empty() {
            continue;
        }
        // 只有读方、没有 `as_str` 的类型也不在本门范围内：它没有另一半词表可比。
        if table.written.is_empty() {
            read_only += 1;
            continue;
        }
        // 具名读方才是完整词表。只有行内臂时，只能判「同一个词两个意思」。
        let complete = !table.read.is_empty();
        if complete {
            judged += 1;
        }
        let readers = table
            .read
            .values()
            .map(|(_, file)| file.clone())
            .chain(table.seen.values().map(|(_, file)| file.clone()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join("`、`");
        for (variant, (word, file, _)) in &table.written {
            if complete {
                words += 1;
            }
            if let Some((read_variant, read_file)) = table.read.get(word) {
                if read_variant != variant {
                    complaints.push(format!(
                        "  `{word}` 在 `{file}` 里写的是 `{owner}::{variant}`，在 `{read_file}` 里\
                         读回来却是 `{owner}::{read_variant}`：同一个词两个意思，两边都不会报错"
                    ));
                }
                continue;
            }
            if let Some((seen_variant, seen_file)) = table.seen.get(word) {
                // 完整词表里没有，但树上有臂认这个词：只在它读成别的变体时才红。
                if seen_variant != variant {
                    complaints.push(format!(
                        "  `{owner}` 在 `{file}` 里把 `{variant}` 写成 `{word}`，而 \
                         `{seen_file}` 里 `\"{word}\"` 读回来是 `{owner}::{seen_variant}`：\
                         同一个词两个意思，两边都不会报错"
                    ));
                }
                continue;
            }
            // 没有完整词表就没有「不认这个词」这句话可说：行内臂本来就捡不全。
            if complete {
                complaints.push(format!(
                    "  `{owner}::{variant}` 在 `{file}` 里写出去的是 `{word}`，而 `{owner}` 的\
                     读方（`{readers}`）不认这个词：配置里、总线上、库里写着 `{word}` 的地方\
                     读回来是「未知」，与「这个值没出现过」同形"
                ));
            }
        }
    }

    if judged < 8 || duplicated.len() < 8 || derived.len() < 20 || words < 30 {
        complaints.push(format!(
            "普查只读到 {judged} 个既有写方又有读方的类型、其中 {} 个读方自己拼了词表、\
             {} 个读方是派生的、{words} 个写出去的词（另有 {read_only} 个类型只有读方、\
             没有 `as_str`，不在本门范围内）：读得太少，本门判不了这一棵树，按红处理",
            duplicated.len(),
            derived.len()
        ));
    }
    complaints
}

#[test]
fn every_word_written_out_is_the_word_the_type_reads_back() {
    let complaints = contradictions(&repo_root(), &crate_tree_sources(&repo_root()));
    assert!(
        complaints.is_empty(),
        "`as_str` 写出去的词必须能被同一类型的 `from_str`／`parse` 读回同一个变体；两个 \
         `match` 各写各的，编译器不比较，因此只改一边不会有任何报错。\n{}",
        complaints.join("\n")
    );
}

const FIXTURE: &str = "\
pub enum Colour {
    Red,
    Green,
}

const GREEN_WORD: &str = \"green\";

impl Colour {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Red => \"red\",
            Self::Green => GREEN_WORD,
        }
    }

    pub fn parse(word: &str) -> Option<Self> {
        match word {
            \"red\" => Some(Self::Red),
            \"green\" => Some(Self::Green),
            _ => None,
        }
    }
}

pub enum Shape {
    Round,
}

impl Shape {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Round => \"round\",
        }
    }
}
";

fn fixture_complaints(source: &str) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("crates/demo/src/lib.rs");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
    std::fs::write(path, source).expect("write");
    contradictions(dir.path(), &sources_of_crate_tree(dir.path()))
        .into_iter()
        .filter(|c| !c.contains("普查只读到"))
        .collect()
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    assert_eq!(fixture_complaints(FIXTURE), Vec::<String>::new());
}

#[test]
fn a_word_the_reader_does_not_accept_is_reported() {
    let source = FIXTURE.replace(
        "const GREEN_WORD: &str = \"green\";",
        "const GREEN_WORD: &str = \"lime\";",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().any(|c| c.contains("lime")),
        "{complaints:?}"
    );
}

#[test]
fn a_word_that_reads_back_as_another_variant_is_reported() {
    let source = FIXTURE.replace(
        "            \"green\" => Some(Self::Green),",
        "            \"green\" => Some(Self::Red),",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("green") && c.contains("两个意思")),
        "{complaints:?}"
    );
}

#[test]
fn a_derived_reader_is_not_compared() {
    let source = FIXTURE.replace(
        "            \"green\" => Some(Self::Green),",
        "            other => Self::ALL.into_iter().find(|c| c.as_str() == other),",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().all(|c| !c.contains("Colour")),
        "{complaints:?}"
    );
}

#[test]
fn an_unreadable_as_str_arm_is_reported() {
    let source = FIXTURE.replace("const GREEN_WORD: &str = \"green\";", "");
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().any(|c| c.contains("读不出来")),
        "{complaints:?}"
    );
}

/// 词写在 `Display` 里、由 `parse` 读回来的类型：`as_str` 不是唯一的写方。
const DISPLAY_FIXTURE: &str = "\
pub enum Mood {
    Calm,
    Wary,
}

impl std::fmt::Display for Mood {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mood::Calm => write!(f, \"calm\"),
            Mood::Wary => write!(f, \"wary\"),
        }
    }
}

impl Mood {
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            \"calm\" => Some(Self::Calm),
            \"wary\" => Some(Self::Wary),
            _ => None,
        }
    }
}
";

/// 词由 `as_str` 写出去，由一个没挂在 `parse` 名下的行内 `match` 读回来。
const INLINE_FIXTURE: &str = "\
pub enum Mood {
    Calm,
    Wary,
}

impl Mood {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Calm => \"calm\",
            Self::Wary => \"wary\",
        }
    }
}

pub fn remember(word: &str) -> Option<Mood> {
    match word {
        \"calm\" => Some(Mood::Calm),
        _ => None,
    }
}
";

#[test]
fn the_display_fixture_is_green_before_anything_is_changed() {
    assert_eq!(fixture_complaints(DISPLAY_FIXTURE), Vec::<String>::new());
}

#[test]
fn a_word_written_by_display_must_be_read_back() {
    let source = DISPLAY_FIXTURE.replace("write!(f, \"wary\")", "write!(f, \"warily\")");
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().any(|c| c.contains("warily")),
        "{complaints:?}"
    );
}

#[test]
fn an_inline_arm_reading_a_word_as_another_variant_is_reported() {
    let green = INLINE_FIXTURE.to_string();
    assert_eq!(fixture_complaints(&green), Vec::<String>::new());
    let source = INLINE_FIXTURE.replace(
        "\"calm\" => Some(Mood::Calm)",
        "\"calm\" => Some(Mood::Wary)",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("calm") && c.contains("两个意思")),
        "{complaints:?}"
    );
}
