//! principle: feedback_no_hardcoding_config_surface
//! A word that crosses a boundary -- a JSON field, a log line, a metric label --
//! has to come from the declaration that owns it, and not out of `Debug`.
//!
//! `format!("{:?}", x).to_lowercase()` looks like it produces the wire word and
//! does for every variant that is one capitalised word. It stops agreeing with
//! serde and with `as_str` the moment a variant gains a second word: an
//! `EvolutionStatus::CompileChecked` read through this pattern is
//! `compilechecked`, while the record the same enum was stored in says
//! `compile_checked`. Both look like a status; only one of them is compared
//! against anything, and the comparison that misses reads as a row in a state
//! nobody reports.
//!
//! So the pattern is refused outright: a word comes from the enum's own
//! `as_str`, from its serde spelling, or from a declared constant. There is no
//! third way where `Debug` is a private implementation detail we may borrow.
//!
//! 实测（2026-10-27）：修前 `crates/` 下 12 处（evolution 状态、用户状态与类型、
//! 任务类型、记忆条目 kind、trace 档位、心跳状态、GitHub issue 状态）；其中记忆
//! 条目的 `kind` 与 evolution 的状态已经是活的错词（`errorpattern`、
//! `compilechecked`），只是恰好没有人按那一种拼法去比。消融对照：把
//! `crates/cog-gateway/src/processes.rs` 里一句改回
//! `format!("{:?}", t.task_type).to_lowercase()`，本门红并点名该文件与该行。

mod common;

use common::routes::repo_root;

use common::rust_production::{production_sources, production_text, relative_name};

/// Whether the `format!` starting at `at` has `.to_lowercase()` applied to it.
///
/// The macro call is scanned to its matching `)` first: without that, a
/// `format!` argument and a `.to_lowercase()` on the *next* statement would be
/// read as one expression and this gate would report code that is fine.
fn lowercases_the_format(text: &str, at: usize) -> bool {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = at;
    while i < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    let rest = &text[i + 1..];
                    let rest = rest.trim_start();
                    return rest.starts_with(".to_lowercase()")
                        || rest.starts_with(".to_ascii_lowercase()");
                }
            }
            _ => {}
        }
        i += 1;
    }
    false
}

/// Every `format!("{:?}", ..).to_lowercase()` in one file's production half.
fn debug_lowercase_spots(text: &str) -> Vec<usize> {
    let needles = ["format!(\"{:?}\"", "format!(\"{:?} \""];
    let mut spots = Vec::new();
    for needle in needles {
        let mut from = 0;
        while let Some(rel) = text[from..].find(needle) {
            let at = from + rel;
            if lowercases_the_format(text, at) {
                spots.push(at);
            }
            from = at + needle.len();
        }
    }
    spots
}

fn line_of(text: &str, at: usize) -> usize {
    text[..at].lines().count()
}

#[test]
fn no_wire_word_is_derived_from_debug_formatting() {
    // Only each crate's `src` and its build script: a test file is nothing but
    // fixtures, and a fixture that spells the pattern on purpose is the check
    // beside this one, not an offence.
    let root = repo_root();
    let mut offenders = Vec::new();
    for path in production_sources(&root) {
        let Ok(raw) = std::fs::read_to_string(&path) else {
            continue;
        };
        let text = production_text(&raw);
        for at in debug_lowercase_spots(&text) {
            offenders.push(format!(
                "{}:{}",
                relative_name(&root, &path),
                line_of(&text, at)
            ));
        }
    }

    assert!(
        offenders.is_empty(),
        "a word on the wire is being derived from Debug formatting at {}; take it from the enum's \
         own as_str or its serde spelling instead -- lowercased Debug agrees only while every \
         variant is a single capitalised word",
        offenders.join(", ")
    );
}

/// 判据自检：认得出正例，也认得出两个看着像但其实不是的反例。少了这一段，
/// `lowercases_the_format` 哪天退化成「凡见 `format!` 就报」，上面那条照样绿。
#[test]
fn the_pattern_check_reads_the_expression_and_not_a_substring() {
    assert!(lowercases_the_format(
        r#"format!("{:?}", x).to_lowercase()"#,
        0
    ));
    assert!(lowercases_the_format(
        "format!(\"{:?}\", x)\n        .to_ascii_lowercase()",
        0
    ));
    assert!(!lowercases_the_format(r#"format!("{:?}", x)"#, 0));
    assert!(
        !lowercases_the_format(
            r#"log::warn!("{}", format!("{:?}", x)); name.to_lowercase()"#,
            20
        ),
        "a to_lowercase() on the next statement is not applied to the format"
    );
    assert_eq!(
        debug_lowercase_spots(r#"let a = format!("{:?}", x).to_lowercase();"#).len(),
        1
    );
    assert_eq!(
        debug_lowercase_spots(r#"let a = format!("{:?}", x);"#).len(),
        0
    );
}
