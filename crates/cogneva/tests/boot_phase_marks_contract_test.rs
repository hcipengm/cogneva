//! principle: feedback_no_hardcoding_config_surface
//! The boot footprint reading is one snapshot per named startup phase, and the
//! phase name is the series' identity: the alert rule groups by it, and the
//! dashboard sorts it. A mark whose name is not in the closed list is dropped
//! with a debug line — at the level the deployment runs at, nothing is printed,
//! and the phase simply does not exist. What the reader then sees is the phases
//! that did mark, which is exactly what a phase that never ran looks like: the
//! rule has no series for it, so nothing covers it and nothing says so.
//!
//! The closed list is `cog_observability::boot_footprint::PHASES`; every call
//! site has to name one of its entries, and every entry has to be named by some
//! call site — a phase nobody marks is a series the rule waits for forever.
//!
//! 反证（消融对照）：把 `cog-memory/src/reranker.rs` 那一处改成
//! `mark("reranker-model")`，两条判据同时红——一条报这个名字不在名单里，
//! 一条报 `reranker` 没有任何调用点；改回来即绿。

use std::collections::BTreeSet;
use std::path::PathBuf;

mod common;

use common::rust_production::{production_lines, production_sources, relative_name};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The call the marks are made with, as it appears in the source.
const CALL: &str = "boot_footprint::mark(";

/// One `mark(...)` call site: the file, the line, and the argument as written.
#[derive(Debug)]
struct Mark {
    location: String,
    argument: String,
}

/// Every `boot_footprint::mark(...)` in this text, with the argument text.
fn marks_in(rel: &str, text: &str) -> Vec<Mark> {
    let mut found = Vec::new();
    for (number, line) in production_lines(text).into_iter().enumerate() {
        let Some(at) = line.find(CALL) else {
            continue;
        };
        let rest = &line[at + CALL.len()..];
        // The argument is a literal in every call today; a call that spans
        // lines or passes a variable is reported as written, and the caller
        // turns that into a failure rather than guessing at it.
        let argument = rest
            .split([')', ';'])
            .next()
            .unwrap_or_default()
            .trim()
            .to_string();
        found.push(Mark {
            location: format!("{rel}:{number}"),
            argument,
        });
    }
    found
}

/// The phase named by an argument, if the argument is a plain string literal.
fn phase_of(argument: &str) -> Option<&str> {
    let rest = argument.strip_prefix('"')?;
    let (phase, tail) = rest.split_once('"')?;
    tail.trim().is_empty().then_some(phase)
}

fn sources() -> Vec<(String, String)> {
    let root = repo_root();
    production_sources(&root)
        .into_iter()
        .map(|path| {
            let rel = relative_name(&root, &path);
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            (rel, text)
        })
        .collect()
}

#[test]
fn every_mark_names_a_declared_phase() {
    let phases: BTreeSet<&str> = cog_observability::boot_footprint::PHASES
        .iter()
        .copied()
        .collect();
    let mut marks = Vec::new();
    for (rel, text) in sources() {
        marks.extend(marks_in(&rel, &text));
    }

    let mut complaints = Vec::new();
    for mark in &marks {
        match phase_of(&mark.argument) {
            Some(phase) if phases.contains(phase) => {}
            Some(phase) => complaints.push(format!(
                "{}: 标记的阶段 {phase:?} 不在 PHASES 里——这一拍会被静默丢掉，\
                 读数里就少了这一段，而「少了」正是「这段启动没跑」的样子",
                mark.location
            )),
            None => complaints.push(format!(
                "{}: 参数 {:?} 不是字面量，这道门判断不了它是不是声明过的阶段",
                mark.location, mark.argument
            )),
        }
    }
    assert!(
        complaints.is_empty(),
        "这些启动标记写了一个 PHASES 读不出来的名字：\n{}",
        complaints.join("\n")
    );
    assert!(
        marks.len() >= cog_observability::boot_footprint::PHASES.len(),
        "只找到 {} 个标记点、{} 个阶段，这条判据没有题目",
        marks.len(),
        phases.len()
    );
}

#[test]
fn every_declared_phase_is_marked_somewhere() {
    let mut marked = BTreeSet::new();
    for (rel, text) in sources() {
        for mark in marks_in(&rel, &text) {
            if let Some(phase) = phase_of(&mark.argument) {
                marked.insert(phase.to_string());
            }
        }
    }
    let unmarked: Vec<&str> = cog_observability::boot_footprint::PHASES
        .iter()
        .copied()
        .filter(|phase| !marked.contains(*phase))
        .collect();
    assert!(
        unmarked.is_empty(),
        "这些阶段在名单里，但没有任何调用点标记它们：{unmarked:?}。\
         告警规则按 phase 分组，名单里的阶段没有序列时，规则对它是空集——\
         不是「没涨」，是「没有这一条」，而两者在看板上长得一样。"
    );
}

#[test]
fn the_census_reddens_on_an_undeclared_name_and_ignores_comments() {
    let text = "\
        /// 说明里提到 boot_footprint::mark(\"serving\") 不算。\n\
        fn a() { cog_observability::boot_footprint::mark(\"serving\"); }\n\
        fn b() { cog_observability::boot_footprint::mark(\"embedding_dense\"); }\n\
        // cog_observability::boot_footprint::mark(\"nope\");\n\
        #[cfg(test)]\n\
        mod tests { fn c() { cog_observability::boot_footprint::mark(\"nope\"); } }\n";
    let marks = marks_in("x.rs", text);
    assert_eq!(marks.len(), 2, "应当只留下两行代码，实得 {marks:?}");
    assert_eq!(phase_of(&marks[0].argument), Some("serving"));
    assert_eq!(phase_of(&marks[1].argument), Some("embedding_dense"));

    let phases = cog_observability::boot_footprint::PHASES;
    assert!(
        phases.contains(&"serving"),
        "题目变了：serving 不再是声明过的阶段"
    );
    assert!(
        !phases.contains(&"embedding_dense"),
        "题目变了：下划线写法进了名单，这条反证就没有反面了"
    );
}
