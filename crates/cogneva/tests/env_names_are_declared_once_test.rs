//! principle: feedback_no_hardcoding_config_surface
//! An environment variable name is a string two parties have to agree on: the
//! process that sets it (the chart, a supervisor, a test harness) and every
//! place that reads it. Spelling it again at each read site is how the two
//! sides drift apart without anything failing — `env::var` answers a missing
//! name with `Err`, and almost every caller turns that into the default it
//! would have used anyway. The knob stops knobbing, and the reading looks
//! exactly like "no one configured it".
//!
//! So a name may be spelled as a literal only in the one place that declares
//! it; every other read goes through that declaration. A name that appears at
//! two sites today is the licence for a third to appear tomorrow, which is why
//! this counts sites rather than trusting intent.
//!
//! Names the platform does not own are listed below: `HOME`, `HOSTNAME`,
//! `RUST_LOG` and the build-time variables belong to the operating system and
//! the toolchain, and declaring them here would claim an ownership this tree
//! does not have.
//!
//! 实测（2026-10-27）：加固前 12 个名字出现在两处以上——其中节点名声明在
//! observability 的三个守护进程里各写一遍、安装期指纹在两个 crate 里各声明
//! 一个同值常量。本轮全部收拢到一处，判据随后全绿。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

mod common;

use common::rust_production::production_lines;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Names this tree does not own, so it does not get to declare them either.
const FOREIGN: &[(&str, &str)] = &[
    ("HOME", "操作系统给的家目录，任何进程读的都是同一份约定"),
    ("HOSTNAME", "容器运行时给的名字，平台只是读者"),
    ("RUST_LOG", "标准日志过滤变量，由 tracing 生态定义"),
    ("OUT_DIR", "构建脚本由工具链注入的目录，每个 crate 各读各的"),
    ("CARGO_MANIFEST_DIR", "构建期由工具链注入"),
];

fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, out);
        } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

/// The name this line spells: a literal read, or a literal declaration.
///
/// `env::var(SOMETHING_ENV)` reads through a declaration and is not a second
/// spelling, so it contributes nothing here. A declaration counts as a site for
/// the same reason a read does: two crates that each declare their own constant
/// for one variable are two spellings that agree only until someone edits one.
fn spelled_name(line: &str) -> Option<&str> {
    if let Some(name) = read_name(line) {
        return Some(name);
    }
    declared_name(line)
}

/// The name a `env::var` / `env::var_os` call reads as a literal, if it is one.
fn read_name(line: &str) -> Option<&str> {
    let call = line
        .find("env::var_os(")
        .map(|at| (at, "env::var_os(".len()))
        .or_else(|| line.find("env::var(").map(|at| (at, "env::var(".len())))?;
    let rest = line[call.0 + call.1..].trim_start();
    let rest = rest.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some(&rest[..end])
}

/// Whether a value is shaped like an environment variable name.
///
/// A declaration is only counted when its value looks like one. Every `const
/// X: &str = "replan"` in the tree is a declared word, not a second spelling of
/// some other crate's word, and treating those as env sites would flood this
/// gate with true statements about the wrong thing.
fn env_shaped(value: &str) -> bool {
    value.len() >= 3
        && value
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && value.starts_with(|c: char| c.is_ascii_uppercase())
}

/// The value of a `const`/`static` whose whole value is one string literal.
fn declared_name(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    if !trimmed.starts_with("pub const ")
        && !trimmed.starts_with("const ")
        && !trimmed.starts_with("pub static ")
        && !trimmed.starts_with("static ")
    {
        return None;
    }
    let at = line.find("= \"")?;
    let rest = &line[at + 3..];
    let end = rest.find('"')?;
    // 值必须是整条字面量：拼接、`concat!` 或引用别的常量都不是第二处拼写。
    if !rest[end + 1..]
        .trim_start_matches([' ', ';'])
        .trim()
        .is_empty()
    {
        return None;
    }
    let value = &rest[..end];
    env_shaped(value).then_some(value)
}

/// Every production line that spells an env name, grouped by that name.
///
/// Build scripts are read too: they spell names at compile time, and a version
/// that arrives as an empty string is the same silent default as a missing
/// runtime variable.
fn literal_sites(root: &Path) -> BTreeMap<String, Vec<String>> {
    let mut crates: Vec<PathBuf> = std::fs::read_dir(root.join("crates"))
        .expect("crates/ 不在仓库里")
        .flatten()
        .map(|entry| entry.path())
        .collect();
    crates.sort();
    let mut sites: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for krate in crates {
        let src = krate.join("src");
        if !src.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk(&src, &mut files);
        let build_script = krate.join("build.rs");
        if build_script.is_file() {
            files.push(build_script);
        }
        files.sort();
        for path in files {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            let file = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let lines = if file.ends_with("build.rs") {
                // 构建脚本没有测试项，只有注释要跳过。
                text.lines()
                    .filter(|line| {
                        let t = line.trim_start();
                        !t.starts_with("//") && !t.starts_with("/*") && !t.starts_with('*')
                    })
                    .collect::<Vec<_>>()
            } else {
                production_lines(&text)
            };
            for line in lines {
                if let Some(name) = spelled_name(line) {
                    sites
                        .entry(name.to_string())
                        .or_default()
                        .push(format!("{file}: {}", line.trim()));
                }
            }
        }
    }
    sites
}

fn foreign(name: &str) -> Option<&'static str> {
    FOREIGN
        .iter()
        .find(|(owned, _)| *owned == name)
        .map(|(_, why)| *why)
}

#[test]
fn every_env_name_is_spelled_in_one_place() {
    let sites = literal_sites(&repo_root());
    assert!(
        sites.len() > 50,
        "只读出 {} 个环境变量名，这条判据没有题目",
        sites.len()
    );

    let duplicated: Vec<String> = sites
        .iter()
        .filter(|(name, hits)| hits.len() > 1 && foreign(name).is_none())
        .map(|(name, hits)| {
            let mut rendered = format!("  {name}（{} 处）", hits.len());
            for hit in hits {
                rendered.push_str("\n      ");
                rendered.push_str(hit);
            }
            rendered
        })
        .collect();

    assert!(
        duplicated.is_empty(),
        "这些环境变量名在多处各写了一遍。改名时只改一处不会报错：另一处照旧\
         读一个再也没人设置的名字，而 `env::var` 读不到与「本来就没配」在\
         调用点上同形，旋钮静默失效。把名字收进一个 `const`，其余读那个常量：\n{}",
        duplicated.join("\n")
    );
}

#[test]
fn a_foreign_name_that_stops_being_foreign_is_reported() {
    let sites = literal_sites(&repo_root());
    let stale: Vec<&str> = FOREIGN
        .iter()
        .map(|(name, _)| *name)
        .filter(|name| !sites.contains_key(*name))
        .collect();
    assert!(
        stale.is_empty(),
        "这些名字已经不在代码里出现了：{stale:?}。豁免是给一处现场发的许可，\
         现场没了许可就该收回——留着它，下次同一个名字重新出现时就没人拦。"
    );
}

#[test]
fn the_reader_tells_a_declaration_read_from_a_literal_and_ignores_comments() {
    assert_eq!(
        spelled_name(r#"let a = std::env::var("COGNEVA_FOO");"#),
        Some("COGNEVA_FOO")
    );
    assert_eq!(
        spelled_name(r#"let b = env::var_os("COGNEVA_BAR");"#),
        Some("COGNEVA_BAR")
    );
    assert_eq!(spelled_name("let c = std::env::var(COGNEVA_ENV);"), None);

    // 声明也是一处拼写：两个 crate 各写一个同值常量，就是两处。
    assert_eq!(
        spelled_name(r#"pub const DATA_DIR_ENV: &str = "COGNEVA_DATA_DIR";"#),
        Some("COGNEVA_DATA_DIR")
    );
    assert_eq!(
        spelled_name(r#"const FOO: &str = "COGNEVA_FOO";"#),
        Some("COGNEVA_FOO")
    );
    // 引用别的常量、或拼接出来的值都不是第二处拼写。
    assert_eq!(
        spelled_name("pub const ENV_FINGERPRINT: &str = cog_core::env::INSTANCE_FINGERPRINT;"),
        None
    );
    assert_eq!(
        spelled_name(r#"pub use crate::host_ledger::NODE_ENV;"#),
        None
    );

    // 注释里提到一个名字不是第二处拼写。
    let text = "// env::var(\"COGNEVA_FOO\")\nfn f() { env::var(\"COGNEVA_FOO\"); }\n";
    let names: Vec<&str> = production_lines(text)
        .into_iter()
        .filter_map(spelled_name)
        .collect();
    assert_eq!(names, vec!["COGNEVA_FOO"]);
}
