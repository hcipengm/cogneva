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
//! Sites are counted by the *literal*, not by the call shape around it. The
//! first version of this gate only saw `env::var("X")` and `const Y = "X"`, so
//! a name read through a helper (`token("X")`, `env_u64("X", 0)`,
//! `get("X")`, `m.insert("X", …)`) was invisible — and that is where most of
//! the duplication actually lived: 61 platform names were spelled more than
//! once in production code while this gate was green.
//!
//! Names the platform does not own are listed below: `HOME`, `HOSTNAME`,
//! `RUST_LOG` and the build-time variables belong to the operating system and
//! the toolchain, and declaring them here would claim an ownership this tree
//! does not have.
//!
//! 实测（2026-10-27）：加固前 12 个名字出现在两处以上——其中节点名声明在
//! observability 的三个守护进程里各写一遍、安装期指纹在两个 crate 里各声明
//! 一个同值常量。本轮全部收拢到一处，判据随后全绿。
//!
//! 实测（2026-10-28）：把判据从「调用形状」改成「字面量」之后普查出 61 个名字
//! 在生产代码里出现两处以上（最多的是 `config.rs` 里 `get("X")` 与
//! `parse("X", …)` 同一行写两遍）。全部收拢后本判据全绿；其中两个构建期名字
//! 只能跨三个二进制各写一次，见 [`BUILD_TIME`]。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

mod common;

use common::routes::repo_root;

use common::rust_production::{production_lines, production_sources, relative_name};

/// Names this tree does not own, so it does not get to declare them either.
const FOREIGN: &[(&str, &str)] = &[
    ("HOME", "操作系统给的家目录，任何进程读的都是同一份约定"),
    ("HOSTNAME", "容器运行时给的名字，平台只是读者"),
    ("RUST_LOG", "标准日志过滤串，由 tracing 生态定义"),
    ("OUT_DIR", "构建脚本由工具链注入的目录，每个 crate 各读各的"),
];

/// Names that cross the build boundary: a build script emits them with
/// `cargo:rustc-env`, and the compile of the binary it belongs to reads them
/// back with `env!`. That reader is a *different program* from the writer, so
/// the name has to be spelled on both sides; the crate that consumes it then
/// declares it once more, because `env!` only accepts a literal.
///
/// Each entry pins the exact files. A fourth file spelling the name is still
/// red, and a listed file that stops spelling it makes the entry stale — the
/// licence is per-site, not per-name.
const BUILD_TIME: &[(&str, &[&str])] = &[
    (
        "COGNEVA_GIT_REVISION",
        &[
            "crates/cog-core/src/env.rs",
            "crates/cogneva/build.rs",
            "crates/cogneva/src/build_info.rs",
        ],
    ),
    (
        "COGNEVA_VERSION_ID",
        &[
            "crates/cog-core/src/env.rs",
            "crates/cogneva/build.rs",
            "crates/cogneva/src/build_info.rs",
        ],
    ),
];

/// Whether a value is shaped like an environment variable name the platform
/// owns: an all-caps word under the project prefix.
///
/// The prefix is what makes this a predicate about *names* rather than about
/// words. `"GET"`, `"USD"` and `"EVAL"` are all-caps literals too, and counting
/// them would flood this gate with true statements about the wrong thing. No
/// length floor: a short name under the prefix is still a name, and dropping
/// one silently is the failure this gate exists to prevent.
fn platform_env_name(value: &str) -> bool {
    (value.starts_with("COGNEVA_") || value.starts_with("COG_"))
        && value
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Whether a value is shaped like *any* environment variable name.
///
/// Used for the `env::var("…")` position, where the call itself says what the
/// literal is, so the prefix is not needed to read it.
fn env_shaped(value: &str) -> bool {
    value.len() >= 3
        && value
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
        && value.starts_with(|c: char| c.is_ascii_uppercase())
}

/// Every string literal on a line, in order.
///
/// A line can hold several (`m.insert("COGNEVA_X".into(), "cfg.key".into())`),
/// and missing the second half is how the shape read through a helper went
/// unseen.
fn literals(line: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut rest = line;
    while let Some(start) = rest.find('"') {
        let after = &rest[start + 1..];
        let mut escaped = false;
        let mut end = None;
        for (i, c) in after.char_indices() {
            if escaped {
                escaped = false;
                continue;
            }
            match c {
                '\\' => escaped = true,
                '"' => {
                    end = Some(i);
                    break;
                }
                _ => {}
            }
        }
        let Some(end) = end else { break };
        out.push(&after[..end]);
        rest = &after[end + 1..];
    }
    out
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

/// Every env name a production line spells, in the order they appear.
///
/// Two sources, and the union is the point: a literal under the project prefix
/// counts wherever it sits (a read through a helper, a declaration, a map
/// entry, a build-script emission), and a bare `env::var("…")` counts for
/// names the platform does not prefix. A `env::var(SOMETHING_ENV)` read goes
/// through a declaration and contributes nothing — that is the shape being
/// asked for.
fn spelled_names(line: &str) -> Vec<&str> {
    let mut names: Vec<&str> = literals(line)
        .into_iter()
        .filter(|value| platform_env_name(value))
        .collect();
    if let Some(name) = read_name(line) {
        if env_shaped(name) && !names.contains(&name) {
            names.push(name);
        }
    }
    names
}
/// Every production line that spells an env name, grouped by that name.
///
/// Build scripts are read too: they spell names at compile time, and a version
/// that arrives as an empty string is the same silent default as a missing
/// runtime variable.
fn literal_sites(root: &Path) -> BTreeMap<String, Vec<(String, String)>> {
    let mut sites: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for path in production_sources(root) {
        let text =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let file = relative_name(root, &path);
        // 构建脚本没有测试项，`production_lines` 在它上面就是「跳过注释」。
        for line in production_lines(&text) {
            for name in spelled_names(line) {
                sites
                    .entry(name.to_string())
                    .or_default()
                    .push((file.clone(), line.trim().to_string()));
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

fn build_time(name: &str) -> Option<&'static [&'static str]> {
    BUILD_TIME
        .iter()
        .find(|(pinned, _)| *pinned == name)
        .map(|(_, files)| *files)
}

#[test]
fn every_env_name_is_spelled_in_one_place() {
    let sites = literal_sites(&repo_root());
    assert!(
        sites.len() > 50,
        "只读出 {} 个环境变量名，这条判据没有题目",
        sites.len()
    );

    let mut duplicated: Vec<String> = Vec::new();
    for (name, hits) in &sites {
        if foreign(name).is_some() {
            continue;
        }
        let files: BTreeSet<&str> = hits.iter().map(|(file, _)| file.as_str()).collect();
        if let Some(pinned) = build_time(name) {
            let expected: BTreeSet<&str> = pinned.iter().copied().collect();
            if files != expected {
                let mut rendered =
                    format!("  {name}（构建期名字，钉在 {expected:?}，实际在 {files:?}）");
                for (file, line) in hits {
                    rendered.push_str(&format!("\n      {file}: {line}"));
                }
                duplicated.push(rendered);
            }
            continue;
        }
        if hits.len() < 2 {
            continue;
        }
        let mut rendered = format!("  {name}（{} 处）", hits.len());
        for (file, line) in hits {
            rendered.push_str(&format!("\n      {file}: {line}"));
        }
        duplicated.push(rendered);
    }

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
fn a_build_time_licence_whose_sites_moved_is_reported() {
    let sites = literal_sites(&repo_root());
    for (name, pinned) in BUILD_TIME {
        let hits = sites
            .get(*name)
            .unwrap_or_else(|| panic!("{name} 已经不在代码里出现：这条许可该收回了"));
        let files: BTreeSet<&str> = hits.iter().map(|(f, _)| f.as_str()).collect();
        let expected: BTreeSet<&str> = pinned.iter().copied().collect();
        assert_eq!(
            files, expected,
            "{name} 的落点变了。构建期名字只允许钉住的那几处各写一次"
        );
    }
}

#[test]
fn the_reader_tells_a_declaration_read_from_a_literal_and_ignores_comments() {
    assert_eq!(
        spelled_names(r#"let a = std::env::var("COGNEVA_FOO");"#),
        vec!["COGNEVA_FOO"]
    );
    assert_eq!(
        spelled_names(r#"let b = env::var_os("COGNEVA_BAR");"#),
        vec!["COGNEVA_BAR"]
    );
    // 名字经声明读取：不是第二处拼写。
    assert!(spelled_names("let c = std::env::var(COGNEVA_ENV);").is_empty());

    // 声明也是一处拼写：两个 crate 各写一个同值常量，就是两处。
    assert_eq!(
        spelled_names(r#"pub const DATA_DIR_ENV: &str = "COGNEVA_DATA_DIR";"#),
        vec!["COGNEVA_DATA_DIR"]
    );
    assert_eq!(
        spelled_names("pub const ENV_FINGERPRINT: &str = cog_core::env::INSTANCE_FINGERPRINT;"),
        Vec::<&str>::new()
    );

    // 经包装函数读取的形态同样算：这正是旧判据漏掉的那一族。
    assert_eq!(
        spelled_names(r#"let k = env_u64("COGNEVA_POOL_CHECK_SECS", 30);"#),
        vec!["COGNEVA_POOL_CHECK_SECS"]
    );
    assert_eq!(
        spelled_names(r#"let t = token("COGNEVA_REDIS_URL");"#),
        vec!["COGNEVA_REDIS_URL"]
    );
    assert_eq!(
        spelled_names(r#"m.insert("COGNEVA_X".into(), "cfg.key".into());"#),
        vec!["COGNEVA_X"]
    );
    assert_eq!(
        spelled_names(r#"let r = env!("COGNEVA_GIT_REVISION");"#),
        vec!["COGNEVA_GIT_REVISION"]
    );
    assert_eq!(
        spelled_names(r#"let some = env!(not_a_literal);"#),
        Vec::<&str>::new()
    );

    // 平台前缀之外的全大写字面量不是「名字」：`"GET"`、`"USD"` 到处都有，
    // 把它们当第二处拼写会让这条判据满屏都是真的、但说的不是同一件事。
    assert!(spelled_names(r#"let m = "GET"; let u = "USD";"#).is_empty());

    // 注释里提到一个名字不是第二处拼写。
    let text = "// env::var(\"COGNEVA_FOO\")\nfn f() { env::var(\"COGNEVA_FOO\"); }\n";
    let names: Vec<&str> = production_lines(text)
        .into_iter()
        .flat_map(spelled_names)
        .collect();
    assert_eq!(names, vec!["COGNEVA_FOO"]);
}
