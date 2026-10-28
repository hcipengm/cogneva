//! principle: feedback_no_hardcoding_config_surface
//!
//! 一个闭集在一棵树里通常写两遍：`enum Foo { A, B, C }` 是它有什么，`pub const
//! ALL: [Foo; 3] = [...]` 是「读的人当成全集的那一份」。第三个地方是长度——`3`
//! 是手写的，编译器只拿它对数组字面量，**不拿它对枚举**。
//!
//! 于是加一个变体而忘了加进 `ALL` 不会有任何报错：编译过、测试过、上线过，只是
//! 那一支从此在所有按 `ALL` 枚举的地方缺席——指标种子里没有它的序列（缺席的序列
//! 与「今天没发生过」是同一个读数）、页面的列表里没有它、门遍历不到它。这正是
//! `ALL` 存在的理由被悄悄掏空的那一步。
//!
//! 本门读生产代码（测试模块不算）：每个 `enum` 的变体集合，逐个对它的 `impl` 里
//! 那个 `const ALL` 引用到的那一组；两边必须相等。只认 `Self::X` 与 `<枚举名>::X`
//! 两种写法，所以 `All(self::Other::X)` 这种引用别的枚举的变体不会被算进来。
//!
//! 判不了什么：不判 `ALL` 的**顺序**（顺序是读者自己的事）、不判同一个变体在
//! `ALL` 里出现两次（编译器会替这一半把关，且元组变体天然会借别的枚举的名字）、
//! 也不判 `as_str`／`Display` 这些另一份拼写——它们各自有自己的门。没有 `const
//! ALL` 的枚举不在本门范围内：不是每个闭集都需要一份可枚举的全集。
//!
//! 实测（2026-10-27）：生产代码 462 个 `enum`、80 处 `const ALL`、1992 个变体，逐一对过，
//! 没有一个 `ALL` 缺变体。普查里有 2 处看着像重复，都是元组变体里引用另一个枚举造成的
//! （`TaskFilter::Status(TaskStatus::Pending)`、`FailureLocus::Killed`），本门按
//! 前缀只认自己的变体，两处都不报。
//!
//! 消融：把 `crates/cog-orchestrator/src/principle_measure.rs` 的
//! `UnsettledReason::ALL` 去掉 `CarrierNotInTree` 并把长度 5 改成 4，编译照过，本门
//! 红并点名缺的那个变体；改回即绿。

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

mod common;

use common::lex::{split_top_level, Dialect};
use common::rust_text::raw_string_end;

use common::routes::repo_root;
use common::rust_production::{
    crate_tree_sources, production_text, relative_name, sources_of_crate_tree,
};

/// 一段源码里所有 `enum <Name> { … }` 的变体。
fn enums_in(file: &str, source: &str) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    let bytes = source.as_bytes();
    let mut enums = BTreeMap::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("enum ") {
        let at = cursor + offset;
        cursor = at + "enum ".len();
        let before = source[..at].chars().next_back();
        if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
            continue;
        }
        let name: String = source[cursor..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.chars().next().is_some_and(|c| c.is_uppercase()) {
            continue;
        }
        let open = source[cursor..]
            .find('{')
            .map(|i| cursor + i)
            .ok_or_else(|| format!("  `{file}` 的 `enum {name}` 没有函数体"))?;
        let close = body_end(bytes, open)
            .ok_or_else(|| format!("  `{file}` 的 `enum {name}` 括号不闭合：读不出来按红处理"))?;
        let variants = variants_of(&source[open + 1..close]);
        if !variants.is_empty() {
            enums.insert(name, variants);
        }
        cursor = close;
    }
    Ok(enums)
}

/// 枚举体里的变体名：按顶层逗号切，去掉开头的属性。
fn variants_of(body: &str) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    for part in split_top_level(body, Dialect::Rust) {
        let mut part = part.trim();
        while part.starts_with("#[") {
            let Some(close) = part.find(']') else { break };
            part = part[close + 1..].trim_start();
        }
        let variant: String = part
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if variant.chars().next().is_some_and(|c| c.is_uppercase()) {
            variants.insert(variant);
        }
    }
    variants
}

/// 从 `open`（一个 `{`）开始找到配对的那个。字符串、字符字面量与生命周期都不算
/// 括号——枚举与 `impl` 体里三者都会出现。
fn body_end(bytes: &[u8], open: usize) -> Option<usize> {
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

/// 一段源码里每一处 `impl <Name> { … const ALL … }` 引用的变体名。
fn all_lists_in(file: &str, source: &str) -> Result<Vec<(String, BTreeSet<String>)>, String> {
    let bytes = source.as_bytes();
    let mut lists = Vec::new();
    let mut cursor = 0;
    while let Some(offset) = source[cursor..].find("impl") {
        let at = cursor + offset;
        cursor = at + "impl".len();
        let before = source[..at].chars().next_back();
        if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
            continue;
        }
        let mut rest = &source[cursor..];
        if rest.trim_start().starts_with('<') {
            let Some(close) = rest.find('>') else {
                continue;
            };
            rest = &rest[close + 1..];
        }
        let rest = rest.trim_start();
        let name: String = rest
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.chars().next().is_some_and(|c| c.is_uppercase()) {
            continue;
        }
        let after_name = rest[name.len()..].trim_start();
        if !after_name.starts_with('{') {
            // `impl Trait for Type` 之类：`ALL` 是自带表的固有项，不在这里。
            continue;
        }
        let open = source.len() - after_name.len();
        let close = body_end(bytes, open)
            .ok_or_else(|| format!("  `{file}` 的 `impl {name}` 括号不闭合：读不出来按红处理"))?;
        cursor = close;
        let body = &source[open + 1..close];
        let Some(all) = body.find("const ALL") else {
            continue;
        };
        let Some(equals) = body[all..].find('=').map(|i| all + i) else {
            return Err(format!("  `{file}` 的 `{name}::ALL` 没有 `=`：读不出来"));
        };
        let init = initializer(&body[equals + 1..])
            .ok_or_else(|| format!("  `{file}` 的 `{name}::ALL` 初始化读不出来"))?;
        lists.push((name.clone(), own_variants(init, &name)));
    }
    Ok(lists)
}

/// 从 `=` 之后到这一项的 `;`（顶层）。
fn initializer(text: &str) -> Option<&str> {
    let bytes = text.as_bytes();
    let mut depth = 0i32;
    let mut i = 0;
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
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth -= 1,
            b';' if depth == 0 => return Some(&text[..i]),
            _ => {}
        }
        i += 1;
    }
    None
}

/// 初始化里 `Self::X` 与 `<枚举名>::X` 两种写法的 X。
fn own_variants(init: &str, name: &str) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    for prefix in ["Self::", &format!("{name}::")] {
        let mut cursor = 0;
        while let Some(offset) = init[cursor..].find(prefix) {
            let at = cursor + offset;
            cursor = at + prefix.len();
            if at >= 1 {
                let before = init[..at].chars().next_back().unwrap_or(' ');
                if before.is_alphanumeric() || before == '_' || before == ':' {
                    continue;
                }
            }
            let variant: String = init[cursor..]
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            if variant.chars().next().is_some_and(|c| c.is_uppercase()) {
                variants.insert(variant);
            }
        }
    }
    variants
}

fn contradictions(root: &Path, sources: &[PathBuf]) -> Vec<String> {
    let mut complaints = Vec::new();
    let mut enums = 0usize;
    let mut lists = 0usize;
    let mut variants = 0usize;
    for path in sources {
        let relative = relative_name(root, path);
        // `target/` 之类的构建产物不是这棵树的一部分。
        if relative.contains("/target/") {
            continue;
        }
        let Ok(raw) = std::fs::read_to_string(path) else {
            complaints.push(format!("  `{relative}` 读不出来：读不出来按红处理"));
            continue;
        };
        let source = production_text(&raw);
        let declared = match enums_in(&relative, &source) {
            Ok(enums) => enums,
            Err(complaint) => {
                complaints.push(complaint);
                continue;
            }
        };
        let listed = match all_lists_in(&relative, &source) {
            Ok(lists) => lists,
            Err(complaint) => {
                complaints.push(complaint);
                continue;
            }
        };
        enums += declared.len();
        variants += declared.values().map(|v| v.len()).sum::<usize>();
        for (name, refs) in listed {
            lists += 1;
            let Some(known) = declared.get(&name) else {
                complaints.push(format!(
                    "  `{relative}` 里 `{name}::ALL` 的枚举读不出来：这一份全集对着谁判不了"
                ));
                continue;
            };
            let missing: Vec<&String> = known.iter().filter(|v| !refs.contains(*v)).collect();
            if !missing.is_empty() {
                complaints.push(format!(
                    "  `{relative}` 的 `{name}::ALL` 少了 {:?}：按 `ALL` 枚举的地方（指标种子、\
                     列表、遍历）从此没有这一支，而缺席与「没发生过」是同一个读数",
                    missing
                ));
            }
        }
    }
    if enums < 300 || lists < 50 || variants < 800 {
        complaints.push(format!(
            "普查只读到 {enums} 个枚举、{lists} 处 `const ALL`、{variants} 个变体：读得太少，\
             本门判不了这一棵树，按红处理"
        ));
    }
    complaints
}

#[test]
fn every_enumeration_names_every_variant_its_enum_declares() {
    let complaints = contradictions(&repo_root(), &crate_tree_sources(&repo_root()));
    assert!(
        complaints.is_empty(),
        "`pub const ALL` 是读的人当成全集的那一份，而编译器只拿它的长度对数组字面量、\
         不对枚举：少一个变体不会有任何报错，只有那一支从此在所有按 `ALL` 枚举的地方\
         缺席。\n{}",
        complaints.join("\n")
    );
}

const FIXTURE: &str = "\
pub enum Colour {
    Red,
    Green,
    Blue,
}

impl Colour {
    pub const ALL: [Colour; 3] = [Self::Red, Colour::Green, Colour::Blue];

    /// 元组变体引用另一个枚举，不算自己的变体。
    pub fn any(self) -> bool {
        matches!(self, Colour::Red)
    }
}

pub enum Sized {
    Small(u8),
    Large { bytes: usize },
}

impl Sized {
    pub const ALL: [Sized; 2] = [Sized::Small(0), Self::Large { bytes: 0 }];
}

pub enum NoList {
    Only,
}
";

fn write_tree(root: &Path, source: &str) {
    let path = root.join("crates/demo/src/lib.rs");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("dir");
    std::fs::write(path, source).expect("write");
}

fn fixture_complaints(source: &str) -> Vec<String> {
    let dir = tempfile::tempdir().expect("temp dir");
    write_tree(dir.path(), source);
    // 夹具只有 3 处 `ALL`，普查下限按真树定，所以这里只取「缺变体」那一类判断。
    contradictions(dir.path(), &sources_of_crate_tree(dir.path()))
        .into_iter()
        .filter(|c| c.contains("少了"))
        .collect()
}

#[test]
fn the_fixture_is_green_before_anything_is_changed() {
    assert_eq!(fixture_complaints(FIXTURE), Vec::<String>::new());
}

#[test]
fn a_variant_missing_from_the_enumeration_is_reported() {
    let source = FIXTURE.replace(
        "pub const ALL: [Colour; 3] = [Self::Red, Colour::Green, Colour::Blue];",
        "pub const ALL: [Colour; 2] = [Self::Red, Colour::Green];",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().any(|c| c.contains("Blue")),
        "{complaints:?}"
    );
}

#[test]
fn a_tuple_variant_naming_another_enum_is_not_counted() {
    // `Sized::Small(u8)` 与 `Colour` 的引用不能互相顶替：`Colour::ALL` 少了 Blue
    // 时，即便同一个文件里有别的枚举提到 `Blue`，也必须红。
    let source = FIXTURE.replace(
        "pub const ALL: [Colour; 3] = [Self::Red, Colour::Green, Colour::Blue];",
        "pub const ALL: [Colour; 2] = [Self::Red, Colour::Green];\n    pub fn b() -> Colour { Colour::Blue }",
    );
    let complaints = fixture_complaints(&source);
    assert!(
        complaints.iter().any(|c| c.contains("Blue")),
        "{complaints:?}"
    );
}

#[test]
fn an_enum_without_a_list_is_not_judged() {
    let complaints = fixture_complaints(FIXTURE);
    assert!(
        complaints.iter().all(|c| !c.contains("NoList")),
        "{complaints:?}"
    );
}

#[test]
fn an_unparsable_enum_is_reported() {
    let dir = tempfile::tempdir().expect("temp dir");
    write_tree(
        dir.path(),
        "pub enum Broken {\n    A,\n\nimpl Broken {\n    pub const ALL: [Broken; 1] = [Self::A];\n}\n",
    );
    let complaints = contradictions(dir.path(), &sources_of_crate_tree(dir.path()));
    assert!(
        complaints.iter().any(|c| c.contains("读不出来")),
        "{complaints:?}"
    );
}
