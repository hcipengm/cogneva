//! The production half of a source file, as both censuses and the deployer
//! gates in this directory read it.
//!
//! Every one of them has to answer the same question -- "is this line shipped
//! code or a fixture?" -- and every one of them used to answer it with
//! "anything before the first `#[cfg(test)]`". That answer is wrong wherever a
//! file carries a gated item before its production tail, and it fails in the
//! direction a gate cannot afford: unread lines and clean lines look identical.
//!
//! Shared rather than copied so the rule has one shape. A gate that stops
//! reading is a bug in the gate, and a bug fixed in one copy stays in the rest.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// 一次普查至少该读到多少个生产源文件。
///
/// 测到的那一天 `crates/*/src` 加 `build.rs` 有 700 余个。
pub const MIN_PRODUCTION_FILES: usize = 500;

/// 这一棵树里「会随产物跑」的 Rust 源码。
///
/// 每个 crate 的 `src/` 全部，加上 crate 根上的 `build.rs`：构建脚本事在编译期跑，
/// 读的却是同一台机器、同一份环境变量，写在里面的第二个读法一样会与源码分叉。
/// 结果按路径排序，读的人不必再排一次。
///
/// 不读三处，各有理由：`tests/` 是门自己住的房子（样例里出现 needle 是它们的工作）；
/// `examples/` 与 `benches/` 不随产物发布，读者是开发者；crate 之外的 `.rs`（构建器
/// 脚本、工具）不在工作区里。
///
/// 下限长在这里，不长在调用点：二十来份普查各自带一个「至少读到 N 个文件」时，
/// 只要这份走错了路——`crates/` 读不到、返回空集、只剩一个 crate——每一份门的落点
/// 集合都会是空的，断言全部成立，二十份门一起变成 vacuous 而没有任何一份会红。
/// 放在这里，调用点免费继承。
pub fn production_sources(root: &Path) -> Vec<PathBuf> {
    let out = sources_of_tree(root);
    assert!(
        out.len() >= MIN_PRODUCTION_FILES,
        "只读到 {} 个生产源文件（下限 {MIN_PRODUCTION_FILES}）：这份普查没有一个题目，\
         任何「树上再没有第二处」的断言在空集上都成立",
        out.len()
    );
    out
}

/// 同一棵树里的生产源文件，不设下限。
///
/// 只给「测试自己搭的那棵树」用。假树的文件数是测试自己写下的那几行，下限在这里
/// 挡不住走错路的遍历，只会让每一个拿假树问问题的对照测试在 helper 里 panic——那样
/// 红的是 helper，不是被问的那条判据。
///
/// 判真树的门一律用 [`production_sources`]：下限的意义是「这棵树真的被读到了」，
/// 而那句话只有对真树才成立。
pub fn sources_of_tree(root: &Path) -> Vec<PathBuf> {
    let crates = root.join("crates");
    let entries = std::fs::read_dir(&crates)
        .unwrap_or_else(|e| panic!("{}: {e}——普查的前提是这棵树有 crates/", crates.display()));
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let crate_dir = entry.path();
        if !crate_dir.is_dir() {
            continue;
        }
        out.extend(sources_of_crate(&crate_dir));
    }
    out.sort();
    out
}

/// 一个文件在树里的名字：repo 相对、`/` 分隔。
///
/// 口径只有这一份。两套写法（`./a/b.rs` 与 `a/b.rs`）在读者眼里是同一个文件，在
/// `==` 眼里不是——这类门最贵的一次误报，代价是有人为了让它绿而去改期望值。
pub fn relative_name(root: &Path, path: &Path) -> String {
    let mut name = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    while let Some(rest) = name.strip_prefix("./") {
        name = rest.to_string();
    }
    name
}

/// 一个 crate 的生产源文件：`src/` 全部，加上 crate 根的 `build.rs`。
///
/// 分开写是为了让人能拿一个临时 crate 试它：下限挡住的是整棵树的空集，而「构建
/// 脚本算不算生产源码」这个问题，用一个两个文件的树就能问出来。
fn sources_of_crate(crate_dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    rust_sources(&crate_dir.join("src"), &mut out);
    let build_script = crate_dir.join("build.rs");
    if build_script.is_file() {
        out.push(build_script);
    }
    out
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// Whether this line opens a test-gated item: `#[cfg(test)]` or an inline
/// `mod tests {`. The item that follows is a specimen, not a deployment fact.
pub fn opens_test_module(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("#[cfg(test)]")
        || trimmed.starts_with("mod tests")
        || trimmed.starts_with("pub mod tests")
}

/// One line's brace shape: how the nesting changes, and whether a `{` opened.
#[derive(Clone, Copy, Default)]
pub struct BraceShape {
    pub delta: i32,
    pub opens: bool,
}

/// Per-line brace shapes for a whole file, carrying string, character-literal
/// and comment state across the newlines.
///
/// Counting each line on its own is not enough. A fixture string spans lines --
/// `"suggestions":[]}"#,` closes a raw string opened four lines above -- and the
/// `}` inside that text would end a skipped item early, after which the rest of
/// the test module reads as production. Over-reading is noisy; under-reading is
/// silent, and only one of those is a gate.
///
/// Three things that look like nesting and are not: braces inside string or
/// character literals, and a `'` that is a lifetime (`&'a str`) rather than the
/// opening of `'{'`. A character literal's closing quote is the next quote
/// within a few bytes; a lifetime's is not.
pub fn brace_shapes(text: &str) -> Vec<BraceShape> {
    let b = text.as_bytes();
    let mut out: Vec<BraceShape> = Vec::new();
    let mut shape = BraceShape::default();
    let mut i = 0;
    let mut block_comment = 0usize;
    let mut string = false;
    let mut raw_hashes: Option<usize> = None;
    while i < b.len() {
        let c = b[i];
        if c == b'\n' {
            out.push(shape);
            shape = BraceShape::default();
            i += 1;
            continue;
        }
        if block_comment > 0 {
            if c == b'/' && b.get(i + 1) == Some(&b'*') {
                block_comment += 1;
                i += 2;
                continue;
            }
            if c == b'*' && b.get(i + 1) == Some(&b'/') {
                block_comment -= 1;
                i += 2;
                continue;
            }
            i += 1;
            continue;
        }
        if let Some(hashes) = raw_hashes {
            if c == b'"' {
                let mut j = i + 1;
                let mut seen = 0;
                while seen < hashes && b.get(j) == Some(&b'#') {
                    seen += 1;
                    j += 1;
                }
                if seen == hashes {
                    raw_hashes = None;
                    i = j;
                    continue;
                }
            }
            i += 1;
            continue;
        }
        if string {
            if c == b'\\' {
                // A backslash that ends the line continues the string: let the
                // newline below emit the line boundary, or every later line's
                // shape is reported one line early.
                if b.get(i + 1) == Some(&b'\n') || b.get(i + 2) == Some(&b'\n') {
                    i += 1;
                } else {
                    i += 2;
                }
                continue;
            }
            if c == b'"' {
                string = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'/' if b.get(i + 1) == Some(&b'*') => {
                block_comment = 1;
                i += 2;
                continue;
            }
            b'"' => {
                string = true;
                i += 1;
                continue;
            }
            b'\'' => {
                let mut j = i + 1;
                if b.get(j) == Some(&b'\\') {
                    j += 1;
                }
                let mut close = None;
                while j < b.len() && j <= i + 5 {
                    if b[j] == b'\'' {
                        close = Some(j);
                        break;
                    }
                    j += 1;
                }
                i = match close {
                    Some(at) => at + 1,
                    None => i + 1,
                };
                continue;
            }
            b'r' | b'b' | b'c' => {
                let mut j = i + 1;
                if b.get(j) == Some(&b'"') {
                    string = true;
                    i = j + 1;
                    continue;
                }
                if c == b'b' || c == b'c' {
                    if b.get(j) != Some(&b'r') {
                        i += 1;
                        continue;
                    }
                    j += 1;
                }
                let mut hashes = 0;
                while b.get(j) == Some(&b'#') {
                    hashes += 1;
                    j += 1;
                }
                if b.get(j) == Some(&b'"') {
                    raw_hashes = Some(hashes);
                    i = j + 1;
                    continue;
                }
                i += 1;
                continue;
            }
            b'{' => {
                shape.delta += 1;
                shape.opens = true;
            }
            b'}' => shape.delta -= 1,
            _ => {}
        }
        i += 1;
    }
    out.push(shape);
    out
}

/// One line's brace delta, for the scanner's own tests.
pub fn brace_delta(line: &str) -> i32 {
    brace_shapes(line)
        .first()
        .copied()
        .unwrap_or_default()
        .delta
}

/// The production half of one source file, in order.
///
/// The boundary is per item, not "everything after the first test module":
/// `mainline_deployer.rs` carries a `#[cfg(test)] fn` about a third of the way
/// in and roughly two thousand lines of production after it. Cutting at the
/// first marker leaves that tail unread while the census still reads clean --
/// which is the one failure a gate cannot have, because "I never looked there"
/// and "I looked and found nothing" are the same reading.
pub fn production_lines(text: &str) -> Vec<&str> {
    let lines: Vec<&str> = text.lines().collect();
    let shapes = brace_shapes(text);
    let mut out = Vec::new();
    let mut gated = false;
    let mut skipping: Option<(i32, bool)> = None;
    let mut waiting = 0usize;
    for (index, line) in lines.iter().copied().enumerate() {
        let shape = shapes.get(index).copied().unwrap_or_default();
        let trimmed = line.trim_start();
        if skipping.is_none() {
            if opens_test_module(line) {
                // `#[cfg(test)]` decorates the next item; `mod tests {` *is* the
                // item and opens a body on this very line.
                gated = !shape.opens;
                if shape.opens {
                    skipping = Some((shape.delta, true));
                }
                waiting = 0;
                continue;
            }
            if gated {
                // Blanks, comments and further attributes still belong to the
                // item the gate decorates.
                if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                    continue;
                }
                gated = false;
                skipping = Some((0, false));
                waiting = 0;
            } else {
                if trimmed.starts_with("//") {
                    continue;
                }
                out.push(line);
                continue;
            }
        }
        // Inside a skipped item, or starting one on this line.
        let (depth, opened) = skipping.take().unwrap_or((0, false));
        let next = depth + shape.delta;
        let opened = opened || shape.opens;
        let ended = (opened && next <= 0) || (!opened && trimmed.ends_with(';'));
        if ended {
            waiting = 0;
        } else if !opened {
            // An item whose body never opens is a parse this scanner does not
            // understand; after a while it gives up and reads the lines as
            // production. Reading too much shows up as a red gate; reading too
            // little shows up as nothing at all.
            waiting += 1;
            if waiting > 50 {
                waiting = 0;
            } else {
                skipping = Some((next.max(0), opened));
            }
        } else {
            skipping = Some((next.max(0), opened));
        }
    }
    out
}

/// The production lines of one file, joined back into text.
///
/// The deployer gates search for whole fragments (`self.dispatch_job(`, an
/// `async fn roll_out(`), so they want text rather than a line list. Joining
/// keeps a construct that spans lines intact and drops the fixture bodies in
/// between, which is the point.
pub fn production_text(text: &str) -> String {
    production_lines(text).join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 下限是这套普查唯一防 vacuous 的东西，而一条从不触发的断言与一条被删掉的
    /// 断言在树上同形：这里给它一个一定会触发的树，让它自己证明还活着。
    #[test]
    fn a_tree_with_almost_nothing_in_it_is_refused() {
        let root = std::env::temp_dir().join(format!("census-floor-{}", std::process::id()));
        let src = root.join("crates/only/src");
        std::fs::create_dir_all(&src).expect("a scratch crate");
        std::fs::write(src.join("lib.rs"), "pub fn f() {}\n").expect("one source file");

        let panic = std::panic::catch_unwind(|| production_sources(&root))
            .expect_err("一个只有两个文件的工作区不该被当成普查读过去");
        let message = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            message.contains(&MIN_PRODUCTION_FILES.to_string()),
            "拒读的理由要说出下限是多少，读到的是：{message}"
        );

        std::fs::remove_dir_all(&root).ok();
    }

    /// 一个文件在树里只有一种写法：`./` 前缀与 Windows 分隔符都折掉。
    #[test]
    fn a_file_has_one_name_in_the_tree() {
        let root = Path::new("/tree");
        assert_eq!(
            relative_name(root, Path::new("/tree/crates/a/src/b.rs")),
            "crates/a/src/b.rs"
        );
        assert_eq!(
            relative_name(root, Path::new("/tree/./crates/a/src/b.rs")),
            "crates/a/src/b.rs"
        );
        assert_eq!(
            relative_name(root, Path::new("/tree/crates/a/src/b.rs")),
            relative_name(root, &Path::new("/tree/crates").join("a/src/b.rs"))
        );
    }

    /// 下限只保护「判真树的普查」这一种读者。
    ///
    /// 测试自己搭的树有几个文件由测试说了算，下限在那里挡不住走错路的遍历，只会让
    /// 拿假树问问题的对照测试在 helper 里 panic——红的是 helper，不是被判的那条规则。
    /// 两条入口的差别就是这一条：同一棵树，带下限的那条拒读，不带的那条照读。
    #[test]
    fn the_floor_belongs_to_the_census_not_to_the_tree() {
        let root = std::env::temp_dir().join(format!("census-fresh-tree-{}", std::process::id()));
        let crate_dir = root.join("crates/only");
        std::fs::create_dir_all(crate_dir.join("src")).expect("a scratch crate");
        std::fs::write(crate_dir.join("src/lib.rs"), "pub fn f() {}\n").expect("a source file");

        assert_eq!(
            sources_of_tree(&root)
                .iter()
                .map(|p| relative_name(&root, p))
                .collect::<Vec<_>>(),
            vec!["crates/only/src/lib.rs"],
            "不带下限的入口要把这棵树里有的文件都读出来"
        );
        let refused = std::panic::catch_unwind(|| production_sources(&root));
        assert!(refused.is_err(), "同一棵树走判真树的那条入口，下限必须拒读");

        std::fs::remove_dir_all(&root).ok();
    }

    /// 构建脚本算生产源码：它在编译期跑，读的是同一份环境变量。今天四个 crate 有
    /// `build.rs`，这一条管的是「第五个 crate 在 build.rs 里写下第二份读法」。
    #[test]
    fn a_build_script_is_part_of_a_crate_s_sources() {
        let root = std::env::temp_dir().join(format!("census-build-rs-{}", std::process::id()));
        let crate_dir = root.join("crates/only");
        std::fs::create_dir_all(crate_dir.join("src")).expect("a scratch crate");
        std::fs::write(crate_dir.join("src/lib.rs"), "pub fn f() {}\n").expect("a source file");
        std::fs::write(crate_dir.join("build.rs"), "fn main() {}\n").expect("a build script");

        let mut names: Vec<String> = sources_of_crate(&crate_dir)
            .iter()
            .map(|p| relative_name(&root, p))
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["crates/only/build.rs", "crates/only/src/lib.rs"],
            "src/ 与 build.rs 都是这个 crate 的生产源码"
        );

        std::fs::remove_dir_all(&root).ok();
    }
}
