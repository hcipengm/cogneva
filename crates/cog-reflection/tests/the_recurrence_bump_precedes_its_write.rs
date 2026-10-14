//! Gate: the recurrence bump is computed before the row is written, everywhere.
//!
//! `record_learning` is the only writer of `recurrence_count`, and it writes the
//! value it is handed. Every call site that bumps the count has to bump the
//! learning it is about to write -- run the increment after the write and the
//! incremented value is dropped, the store keeps the pre-increment value, and
//! every stored row stays at the initial 1 however often the pattern recurs.
//! That is not a hypothesis: the store held 1 for three weeks, `min_recurrence`
//! could not be met, and nothing went red because the only trace of the count
//! was the value in memory, which was right.
//!
//! The write order is invisible at the call site's type: both orders compile,
//! both return the same `Learning` to the caller, and the difference only shows
//! in what a later read-back finds. That is why the defect survived and why this
//! gate reads the production source rather than a behaviour: one call site fixed
//! leaves every other one silently wrong, and the next one written by hand will
//! be wrong again unless something looks at the shape.

use std::path::{Path, PathBuf};

/// The statement that must never follow `record_learning`.
const BUMP: &str = "update_recurrence(";

/// The only writer of `recurrence_count`.
const WRITE: &str = "record_learning(";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-reflection")
        .to_path_buf()
}

/// Every `.rs` file under the crate's `src/`, recursively.
fn source_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
            let path = entry.expect("readable dir entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    let mut files = Vec::new();
    walk(
        &workspace_root().join("crates/cog-reflection/src"),
        &mut files,
    );
    files.sort();
    files
}

/// Everything up to the first test-only item. A fixture that stores a learning
/// and bumps it in a different order is not a call site the engine runs.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

/// The 1-based line numbers where a `record_learning` call is immediately
/// followed by an `update_recurrence` call -- the order that drops the bump.
///
/// Comment lines are dropped before pairing: the prose that explains this
/// invariant names both calls, and pairing off a doc comment would make every
/// correctly written site look like a defect.
fn bump_after_write_lines(text: &str) -> Vec<usize> {
    let code: Vec<(usize, &str)> = production_source(text)
        .lines()
        .enumerate()
        .map(|(i, line)| (i + 1, line.trim()))
        .filter(|(_, line)| !line.is_empty() && !line.starts_with("//"))
        .collect();

    let mut offenders = Vec::new();
    for pair in code.windows(2) {
        let (line_no, write) = pair[0];
        if write.contains(WRITE) && pair[1].1.contains(BUMP) {
            offenders.push(line_no);
        }
    }
    offenders
}

#[test]
fn no_call_site_bumps_the_recurrence_after_the_write() {
    let mut offenders = Vec::new();
    for file in source_files() {
        let text =
            std::fs::read_to_string(&file).unwrap_or_else(|e| panic!("{}: {e}", file.display()));
        for line in bump_after_write_lines(&text) {
            offenders.push(format!("{}:{line}", file.display()));
        }
    }
    assert!(
        offenders.is_empty(),
        "these call sites write the learning before bumping its recurrence: {WRITE} is the only \
         writer of `recurrence_count` and it writes the value it is handed, so the increment is \
         dropped and every stored row stays at 1 -- the state the store was in for three weeks. \
         Compute the bump first: {offenders:#?}"
    );
}

/// The reader has to pair a write with the statement that follows it, not with
/// the whole file: a test that matched any bump anywhere would stay green
/// through the defect it exists for.
#[test]
fn the_reader_pairs_the_write_with_the_statement_after_it() {
    let dropped = "self.recorder.record_learning(l.clone()).await?;\n\
                   self.matcher.update_recurrence(&mut l).await?;\n";
    assert_eq!(bump_after_write_lines(dropped), vec![1]);

    let computed_first = "self.matcher.update_recurrence(&mut l).await?;\n\
                          self.recorder.record_learning(l.clone()).await?;\n";
    assert!(bump_after_write_lines(computed_first).is_empty());

    // A blank line or a comment between the two is still the same order.
    let separated = "self.recorder.record_learning(l.clone()).await?;\n\
                     \n\
                     // store, then count\n\
                     self.matcher.update_recurrence(&mut l).await?;\n";
    assert_eq!(bump_after_write_lines(separated), vec![1]);

    // A write with no bump anywhere near it is not a defect.
    let write_only = "self.recorder.record_learning(l.clone()).await?;\n\
                      self.promoter.promote_if_ready(&l).await;\n";
    assert!(bump_after_write_lines(write_only).is_empty());

    // The prose that explains the invariant names both calls; it must not be
    // read as one of them.
    let explained = "// `record_learning` writes the value it is handed, so the\n\
                     // `update_recurrence` above it has to run first.\n\
                     fn f() {}\n";
    assert!(bump_after_write_lines(explained).is_empty());
}
