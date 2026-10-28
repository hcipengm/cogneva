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

// This file is read two ways: as a sibling of `fsio` inside `tests/common/`,
// and straight onto a crate root through `#[path]` by `cog-reflection/src`,
// `bootstrap/src` and a handful of integration fixtures -- where `super` is the
// crate root and there is no `fsio` to find. Naming the file relative to this
// one compiles in both places, and keeps `walk_files` the only recursion.
//
// Inside `tests/common/` that naming mounts a second copy of a file the harness
// already mounts as `common::fsio`. The copy is the price of compiling here
// without a sibling, and it is inert: the module is free functions over paths,
// with no state and no type whose identity two copies could split.
#[allow(clippy::duplicate_mod)]
#[path = "fsio.rs"]
mod fsio;

use self::fsio::walk_files;

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

/// `crates/` 下每一个 `.rs`，夹具、示例、构建脚本都在内。
///
/// 这一份问的是「这棵树里还有没有这个字节」，不是「产物里有没有」。用它的是
/// 「登记表与树上出现的东西对不对得上」那一族：夹具里写着第二份拼法，同样是登记
/// 表要认的现场。
///
/// `target/` 与 `.git` 在递归里跳过：它们不是源，而且会把这棵树读成几万个文件。
pub fn all_crate_files(root: &Path) -> Vec<PathBuf> {
    let out = crate_files_of(root);
    assert!(
        out.len() >= MIN_PRODUCTION_FILES,
        "只读到 {} 个 crate 源文件（下限 {MIN_PRODUCTION_FILES}）：这棵树的 crate 代码\
         一个都没读到，任何「再没有第二处」的断言在空集上都成立",
        out.len()
    );
    out
}

/// `crates/` 下每一个 `.rs`，不设下限：给测试自己搭的树用。
pub fn crate_files_of(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_files(&root.join("crates"), &["rs"], &["target", ".git"], &mut out);
    out.sort();
    out
}

/// 上面那一份去掉夹具目录。比 [`production_sources`] 宽：`examples/` 与 `benches/`
/// 也在内——它们同样被编译。用这一份的门问的是「这棵工作树的 crate 代码里还写不写
/// 这个词」，不是「产出的镜像里有没有它」；后者用 [`production_sources`]。
pub fn crate_tree_sources(root: &Path) -> Vec<PathBuf> {
    let out = sources_of_crate_tree(root);
    assert!(
        out.len() >= MIN_PRODUCTION_FILES,
        "只读到 {} 个 crate 源文件（下限 {MIN_PRODUCTION_FILES}）：这棵树的 crate 代码\
         一个都没读到，任何「再没有第二处」的断言在空集上都成立",
        out.len()
    );
    out
}

/// 同一份遍历，不设下限：给测试自己搭的那棵树用，理由同 [`sources_of_tree`]。
pub fn sources_of_crate_tree(root: &Path) -> Vec<PathBuf> {
    crate_files_of(root)
        .into_iter()
        .filter(|path| !path.components().any(|c| c.as_os_str() == "tests"))
        .collect()
}

/// `crates/<crate>/{src,tests}` 下的每一个 `.rs`，带下限。
///
/// 第三种人口：夹具在内，示例与基准不在内。用它的是「这条规则对生产代码和夹具
/// 都成立吗」这一类门——夹具是替身客户端、替身循环，它一样建连接、一样起循环，
/// 只是寿命由测试决定，所以同一条判据要看它；示例与基准不是任何进程的一部分。
pub fn src_and_test_sources(root: &Path) -> Vec<PathBuf> {
    let out = sources_of_src_and_tests(root);
    assert!(
        out.len() >= MIN_PRODUCTION_FILES,
        "只读到 {} 个 src/ 与 tests/ 下的源文件（下限 {MIN_PRODUCTION_FILES}）：这条门\
         一条都没读到就会在空集上通过",
        out.len()
    );
    out
}

/// 同一份人口，不设下限：给测试自己搭的那棵树用，理由同 [`sources_of_tree`]。
pub fn sources_of_src_and_tests(root: &Path) -> Vec<PathBuf> {
    crate_files_of(root)
        .into_iter()
        .filter(|path| {
            matches!(
                relative_name(root, path).split('/').nth(2),
                Some("src") | Some("tests")
            )
        })
        .collect()
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
    walk_files(&crate_dir.join("src"), &["rs"], &[], &mut out);
    let build_script = crate_dir.join("build.rs");
    if build_script.is_file() {
        out.push(build_script);
    }
    out
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
                // shape is reported one line early. `\` + CR + LF is the same
                // continuation.
                //
                // What decides is the byte *after* the backslash, never the one
                // after that: a backslash always escapes exactly the character
                // next to it. Reading a byte further out swallows an escaped
                // quote that ends a line (`value: \"1\"`), and the `"` right
                // there is then taken for the string's end -- every line below
                // it is read as code, braces and quotes included.
                if b.get(i + 1) == Some(&b'\n') || b.get(i + 1) == Some(&b'\r') {
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
                // Past the backslash an escape covers **two** bytes, the
                // backslash and the character it escapes: stopping after the
                // backslash leaves `j` sitting on the escaped quote, so a
                // literal like `'\''` closes on its own escape and every quote
                // or brace after it on the same line reads as code.
                let mut j = i + 1;
                if b.get(j) == Some(&b'\\') {
                    j += 2;
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
    production_numbered_lines(text)
        .into_iter()
        .map(|(_, line)| line)
        .collect()
}

/// The production half with its comments left in.
///
/// Same boundary, one projection different: a gate that reads a marker out of a
/// comment (to find the branch the comment sits above) cannot use the
/// comment-stripped projection, because the marker is simply gone -- and the
/// failure reads as "the branch is gone". Keeping both projections here means
/// the boundary is still computed once: a caller picks what it reads, never
/// where the test half starts.
pub fn production_lines_keeping_comments(text: &str) -> Vec<&str> {
    production_numbered_impl(text, true)
        .into_iter()
        .map(|(_, line)| line)
        .collect()
}

/// One flag per line: true when that line is production.
///
/// The projection for a caller that blanks the test half in place instead of
/// dropping it -- dropping shifts every offset after the first cut, and a gate
/// that reads a byte offset cannot use a shorter text.
pub fn production_line_mask(text: &str) -> Vec<bool> {
    let total = text.lines().count();
    let mut mask = vec![false; total];
    for (number, _) in production_numbered_impl(text, true) {
        if let Some(slot) = mask.get_mut(number - 1) {
            *slot = true;
        }
    }
    mask
}

/// The production half with the file's own line numbers kept.
///
/// A gate that reports `file:line` cannot use a filtered list: dropping a line
/// renumbers everything after it and the finding points at the wrong statement.
/// Same boundary as [`production_lines`], only the projection differs -- the
/// caller picks what it reads, never where the test half starts.
pub fn production_numbered_lines(text: &str) -> Vec<(usize, &str)> {
    production_numbered_impl(text, false)
}

fn production_numbered_impl(text: &str, keep_comments: bool) -> Vec<(usize, &str)> {
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
                if trimmed.starts_with("//") && !keep_comments {
                    continue;
                }
                out.push((index + 1, line));
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

/// The production half of one file, comments included, joined back into text.
pub fn production_text_keeping_comments(text: &str) -> String {
    production_lines_keeping_comments(text).join("\n")
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

    /// 工作树的几份普查之间只差那几个减法：夹具、示例、构建脚本各留一份。
    ///
    /// 钉的就是这些减法。写宽了（比如顺手把 `examples/` 也排除）不会有任何一条门
    /// 变红——它们的断言在更小的集合上照样成立，只是从此看不见示例里写死的第二份
    /// 拼法；夹具那一侧同理，读进去了就是把测试数据当成部署事实。
    #[test]
    fn the_working_tree_censuses_differ_by_exactly_their_cuts() {
        let root = std::env::temp_dir().join(format!("census-crate-files-{}", std::process::id()));
        let crate_dir = root.join("crates/only");
        for (path, body) in [
            ("src/lib.rs", "pub fn f() {}\n"),
            ("build.rs", "fn main() {}\n"),
            ("tests/a.rs", "#[test]\nfn t() {}\n"),
            ("examples/e.rs", "fn main() {}\n"),
            ("benches/b.rs", "fn main() {}\n"),
        ] {
            let path = crate_dir.join(path);
            std::fs::create_dir_all(path.parent().expect("a file below the crate root"))
                .expect("a scratch directory");
            std::fs::write(&path, body).expect("a source file");
        }

        let names = |files: Vec<PathBuf>| -> Vec<String> {
            let mut out: Vec<String> = files.iter().map(|p| relative_name(&root, p)).collect();
            out.sort();
            out
        };
        assert_eq!(
            names(crate_files_of(&root)),
            vec![
                "crates/only/benches/b.rs",
                "crates/only/build.rs",
                "crates/only/examples/e.rs",
                "crates/only/src/lib.rs",
                "crates/only/tests/a.rs",
            ],
            "工作树那一份连夹具一起读"
        );
        assert_eq!(
            names(sources_of_crate_tree(&root)),
            vec![
                "crates/only/benches/b.rs",
                "crates/only/build.rs",
                "crates/only/examples/e.rs",
                "crates/only/src/lib.rs",
            ],
            "去掉夹具之后，示例、基准、构建脚本都还在这一份里"
        );
        assert_eq!(
            names(sources_of_src_and_tests(&root)),
            vec!["crates/only/src/lib.rs", "crates/only/tests/a.rs"],
            "只要 src 与 tests 的那一份：夹具在内，示例、基准、构建脚本都不在"
        );
        assert!(
            std::panic::catch_unwind(|| crate_tree_sources(&root)).is_err(),
            "带下限的那条入口在同一棵树上必须拒读"
        );
        assert!(
            std::panic::catch_unwind(|| src_and_test_sources(&root)).is_err(),
            "第三种人口的下限同样在真树上生效，假树照读"
        );

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

    /// 文件中间的一个 `#[cfg(test)]` 项不许把后面的产出整段截掉。
    ///
    /// 这是这条切口唯一会**静默**错的方式：按「第一处 `#[cfg(test)]` 之前」切的人
    /// 看不见后面那一段，而「没读到」与「那里什么都没有」在判词上是同一句话。实际
    /// 发生过：`mainline_rollout.rs` 里一个只给用例换读数来源的 setter 带着
    /// `#[cfg(test)]`，切在它那里就把两千行产出丢掉，三处调用点一处都读不到。
    #[test]
    fn a_test_only_item_in_the_middle_does_not_truncate_the_tail() {
        let source = "impl Thing {\n    #[cfg(test)]\n    fn tweak(&mut self) {\n        self.x = 1;\n    }\n\n    fn real_work(&self) {\n        call_me();\n    }\n}\n";
        let production = production_text(source);
        assert!(
            production.contains("fn real_work"),
            "门装的 setter 之后的产出被整段丢掉了：{production}"
        );
        assert!(
            !production.contains("fn tweak"),
            "被 `#[cfg(test)]` 装点的项本身仍然算进了产出面：{production}"
        );
        // 注释是另一条独立的投影：要读注释里那句话的人用它，边界仍是同一个。
        let with_comments = production_text_keeping_comments(source);
        assert!(
            with_comments.contains("fn real_work") && !with_comments.contains("fn tweak"),
            "保留注释的那一版边界变了：{with_comments}"
        );
    }

    /// 行尾的被转义引号（`value: \"1\"`）不是字符串的结束符。
    ///
    /// 反斜杠只转义**紧邻的那一个**字符，所以判续行时只能看反斜杠后第一个字节。
    /// 旧写法看到的是后第二个字节：`\` + `"` + 换行 时它看见了换行，续行判对了，
    /// 可那个被转义的 `"` 却留在原地被当成闭合引号——从下一行起整个文件按代码读，
    /// `#[cfg(test)] mod tests` 的括号账提前归零，模块之后的产出整段消失。
    #[test]
    fn an_escaped_quote_at_a_line_end_does_not_close_the_string() {
        let source = r#"pub fn real_work() {
    call_me();
}

#[cfg(test)]
mod tests {
    fn fixture() {
        let yaml = "\
            value: \"1\"
        ";
        let _ = yaml;
    }
}

pub fn real_work_after_the_module() {
    call_me();
}
"#;
        let production = production_text(source);
        assert!(
            production.contains("fn real_work_after_the_module"),
            "掩码把字符串提前收了尾，模块之后的产出整段被吞掉：{production}"
        );
        assert!(
            production.contains("fn real_work"),
            "模块之前的产出也不该丢：{production}"
        );
        assert!(
            !production.contains("let yaml"),
            "夹具正文被当成产出读了：{production}"
        );
    }

    /// 同一个判据的 CRLF 那一支：`\` + CR + LF 也是续行。
    ///
    /// 判据看的是反斜杠后第一个字节，所以 `\r` 与 `\n` 都得算。少了这一条，把条件
    /// 「简化」成只认 `\n` 不会有任何一条门变红，直到有人在 CRLF 检出上跑这套普查。
    #[test]
    fn a_crlf_continuation_keeps_the_string_open() {
        let source = "#[cfg(test)]\r\nmod tests {\r\n    fn fixture() {\r\n        \
                      let yaml = \"\\\r\n            value: 1\r\n        \";\r\n        \
                      let _ = yaml;\r\n    }\r\n}\r\n\r\n\
                      pub fn real_after_the_module() {\r\n    call_me();\r\n}\r\n";
        let production = production_text(source);
        assert!(
            production.contains("fn real_after_the_module"),
            "CRLF 的续行让字符串提前收了尾：{production}"
        );
        assert!(
            !production.contains("let yaml"),
            "CRLF 那份夹具被当成产出读了：{production}"
        );
    }

    /// 字符字面量里的转义引号不许把同一行后面的 `'{'` 顶成真括号。
    ///
    /// `'\''` 里被转义的那个 `'` 是**内容**，闭合在它后面一个。只跳过反斜杠会让内容
    /// 自己当上闭合，i 于是落在 `'` 与 `,` 之间重新开一个字面量，把 `'{'` 读成代码。
    /// 这一支在模块外，影响的是本行 delta（真实现里在 `mainline_deployer.rs:9815`），
    /// 与上面那条合起来才是全树 31 份文件漏读的全部原因。
    #[test]
    fn an_escaped_quote_in_a_char_literal_is_content_not_a_terminator() {
        let source = "fn f() {\n    if v.starts_with(['\"', '\\'', '|', '{']) {\n        \
                      call_me();\n    }\n}\n";
        let shapes = brace_shapes(source);
        assert_eq!(
            shapes[1].delta, 1,
            "这一行只有一个真括号，读到的是 {}（多出来的是字面量里的那个）：{:?}",
            shapes[1].delta, source
        );
    }

    /// 真实现里的那份标本：`mainline_deployer.rs` 的测试模块有五千余行，掩码一旦提前
    /// 收尾，它们整段被当成产出，十几份源码扫描普查同时红。
    ///
    /// 合成样例只钉规则；这一格钉的是**这一份文件上的实际后果**，因为规则的错法
    /// 不止一种，而它们都长成「读到的产出比真的多」。
    #[test]
    fn the_fixtures_below_a_multiline_string_stay_out_of_production() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../cog-reflection/src/mainline_deployer.rs");
        let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "读不到标本 {}（{e}）：这一格判的就是它，读不到就不算判过",
                path.display()
            )
        });
        // 标本还在吗。名字改了就让这一格红着提醒换标本——空断言通过比没有这一格更坏。
        const SPECIMEN: &str = "fn fake_kubectl_field_removal_under_an_image_bump";
        assert!(
            text.contains(SPECIMEN),
            "标本 {SPECIMEN} 已经不在 {} 里了，换一个测试模块深处的名字",
            path.display()
        );
        let production = production_text(&text);
        assert!(
            !production.contains(SPECIMEN),
            "测试模块深处的夹具被当成产出读了：掩码在它之前就收了尾"
        );
    }
}
