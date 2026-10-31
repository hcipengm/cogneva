//! principle: feedback_a_cheap_check_must_precede_the_action_that_only_grew_expensive
//! Gate: the idempotency read runs before the gate that now costs a sandbox rebuild.
//!
//! Two checks in one flow, and their order is free while both are cheap. When one
//! of them grows expensive -- here the eval gate went from reading a field in
//! memory to submitting an A/B rebuild and waiting up to
//! `DEFAULT_EVAL_TIMEOUT_SECS` for it -- the order becomes a cost criterion: the
//! pure read that answers "has this change already been recorded" has to run
//! first, or a change with a terminal row already in the account pays the hour
//! and is then refused by the very check that had the answer.
//!
//! Nothing goes red when the order is wrong. Both orders compile, both return
//! `Ok(())` for the already-recorded change, and the only trace is an hour of
//! wall clock plus -- when the expensive gate blocks -- a second `Failed` row of
//! the kind the idempotency read exists to prevent. So the gate reads the
//! production source: an expensive gate that runs with no idempotency read
//! ahead of it in the same file is the defect, wherever it is written next.

use std::path::PathBuf;

#[path = "../../cogneva/tests/common/rust_production.rs"]
mod rust_production;

use rust_production::{production_numbered_lines, production_sources, relative_name};

/// The pure read: one ledger lookup answering "does this change already carry a
/// terminal row", which stops the run before anything is spent.
const CHEAP: &str = ".always_recorded(";

/// The gate that grew expensive: it submits a sandbox A/B rebuild and waits for
/// its terminal state.
const EXPENSIVE: &str = ".eval_gate_blocks(";

/// 仓库根：一份算法，全仓共用（本文件按路径把那一份编进来）。
#[path = "../../cogneva/tests/common/root.rs"]
mod root;

use root::repo_root;

/// This crate's shipping sources: the shared census, cut down to the crate.
fn source_files() -> Vec<PathBuf> {
    let root = repo_root();
    let prefix = format!(
        "{}/",
        relative_name(&root, &root.join("crates/cog-reflection/src"))
    );
    production_sources(&root)
        .into_iter()
        .filter(|path| relative_name(&root, path).starts_with(&prefix))
        .collect()
}

/// The 1-based line numbers where an expensive gate runs with no idempotency
/// read ahead of it.
///
/// Comment lines are dropped by the shared scanner: the prose that explains this
/// invariant names both calls, and reading a doc comment would make the
/// correctly written site look like a defect. The definition of each method does
/// not match either needle -- both are needles on the *call* (`.name(`), so the
/// `async fn` that defines them is not counted as a use.
fn expensive_without_cheap_ahead(text: &str) -> Vec<usize> {
    let code: Vec<(usize, &str)> = production_numbered_lines(text)
        .into_iter()
        .map(|(no, line)| (no, line.trim()))
        .filter(|(_, line)| !line.is_empty())
        .collect();

    let mut seen_cheap = false;
    let mut offenders = Vec::new();
    for (line_no, line) in code {
        if line.contains(CHEAP) {
            seen_cheap = true;
            continue;
        }
        if line.contains(EXPENSIVE) && !seen_cheap {
            offenders.push(line_no);
        }
    }
    offenders
}

#[test]
fn no_expensive_gate_runs_before_the_idempotency_read() {
    let mut offenders = Vec::new();
    for file in source_files() {
        let text =
            std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        for line in expensive_without_cheap_ahead(&text) {
            offenders.push(format!("{}:{line}", file.display()));
        }
    }
    assert!(
        offenders.is_empty(),
        "these call sites reach {EXPENSIVE} with no {CHEAP} ahead of them in the file: the eval \
         gate submits a sandbox A/B rebuild and waits up to its timeout for a terminal state, so \
         a change that already carries a terminal row pays that wait and is then refused by the \
         read that had the answer -- and a blocking gate writes a second `Failed` row on top. \
         Put the pure read first: {offenders:#?}"
    );
}

/// A scan that no longer recognises either call would report the tree as clean.
/// The gate has to say when it cannot see what it judges: a renamed method turns
/// "no offender" into "nothing was looked at", and the two read the same from the
/// outside.
#[test]
fn the_scan_sees_both_calls_in_the_tree() {
    let mut cheap = 0usize;
    let mut expensive = 0usize;
    for file in source_files() {
        let text =
            std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        for (_, line) in production_numbered_lines(&text) {
            if line.contains(CHEAP) {
                cheap += 1;
            }
            if line.contains(EXPENSIVE) {
                expensive += 1;
            }
        }
    }
    assert!(
        cheap > 0 && expensive > 0,
        "the scan found {CHEAP} {cheap} times and {EXPENSIVE} {expensive} times in this crate's \
         production sources. Zero on either side means the gate judged nothing: a method that \
         was renamed or moved reads exactly like a tree with no offender. Update the needle."
    );
}

/// The reader has to measure the order of two statements in one flow, not the
/// presence of two strings in a file: a test that matched any occurrence would
/// stay green through the defect it exists for.
#[test]
fn the_reader_reports_the_order_and_not_the_occurrence() {
    let cheap_first = "let seen = self.always_recorded(&change_id).await?;\n\
                       if !seen {\n\
                           self.eval_gate_blocks(&change_id, source).await;\n\
                       }\n";
    assert!(expensive_without_cheap_ahead(cheap_first).is_empty());

    let expensive_first = "self.eval_gate_blocks(&change_id, source).await;\n\
                           self.always_recorded(&change_id).await?;\n";
    assert_eq!(expensive_without_cheap_ahead(expensive_first), vec![1]);

    // Comments and blank lines between the two are still the same flow.
    let separated = "self.always_recorded(&change_id).await?;\n\
                     \n\
                     // the gate below costs a rebuild\n\
                     self.eval_gate_blocks(&change_id, source).await;\n";
    assert!(expensive_without_cheap_ahead(separated).is_empty());

    // The prose that explains the invariant names both calls; it must not be
    // read as one of them.
    let explained = "// `always_recorded` has to stay ahead of `eval_gate_blocks`.\n\
                     // `.eval_gate_blocks(` is the expensive one.\n\
                     fn f() {}\n";
    assert!(expensive_without_cheap_ahead(explained).is_empty());
}
