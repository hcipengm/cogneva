//! principle: feedback_a_corpus_that_carries_the_probes_own_command_hides_a_hit_in_itself
//! principle: feedback_a_gate_whose_witness_input_fails_a_different_precondition_proves_nothing
//! principle: feedback_a_repeated_observation_of_one_object_is_one_row_keyed_and_stamped_by_the_object
//!
//! What a red CI run says about the principles whose gates it built.
//!
//! A failing job names the file it failed in, and a file's own header names the
//! principles it enforces. Together those two sentences are evidence that a
//! principle's gate is red right now -- the third way a violation reaches the
//! platform, after a host gate refusing a tool call and a check the platform
//! runs against its own tree.
//!
//! The evidence enters through the same queue a refusing host gate writes to, so
//! the account's one intake needs no second path to read it and the row it mints
//! is the row any other organ's refusal mints. What is added here is only the
//! mapping from a log to the slugs.
//!
//! Two shapes in a real failing log decided how that mapping has to work:
//!
//!   * the runner echoes the very pattern it was told to search with, so a
//!     reader looking for the marker text found its own search string as a hit.
//!     A hit is a failure site only when a path with a line number follows the
//!     marker, and that is what is checked;
//!   * every job that runs the same check reports the same site, and this
//!     platform runs a test job and a coverage job over the same tree. Keying a
//!     refusal by the job would mint two rows for one failure, so it is keyed by
//!     the revision and stamped with the commit's own instant instead of the
//!     clock: a second job, a retried pass and a restarted process then all land
//!     on the same row.

use std::collections::BTreeSet;
use std::path::Path;

use chrono::{DateTime, TimeZone, Utc};
use cog_core::{PrincipleLedgerStore, PrincipleRefusal, PLATFORM_DECLARATION_PREFIXES};

use crate::landing::run_git;

/// The markers a failure site is printed after.
///
/// The first is what a failing assertion prints, the second is what a compiler
/// error points with. Neither is sufficient on its own -- see the module
/// comment -- so what follows the marker is read, not the marker.
const FAILURE_MARKERS: [&str; 2] = ["panicked at ", "--> "];

/// How many candidate files one red revision may cost a read.
///
/// Each candidate costs a `git show`, on the path that settles landings, so a
/// log that names hundreds of files must not spawn hundreds of processes.
/// Measured on the failing revisions this was written against: two and three
/// `crates/**/*.rs` paths named per run, with the rest of the log's path-like
/// text being files outside the repository.
const MAX_FILES_READ: usize = 16;

/// The repository-relative Rust sources a job's log names as a failure site.
///
/// Only Rust sources: a declaration is a header of the module that enforces it,
/// so a path that cannot carry one is a path the log mentioned for its own
/// reasons, and reading it could not produce a slug.
pub fn failing_paths(log: &str) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for line in log.lines() {
        for marker in FAILURE_MARKERS {
            let Some((_, rest)) = line.split_once(marker) else {
                continue;
            };
            if let Some(path) = path_before_a_line_number(rest) {
                paths.insert(path);
            }
        }
    }
    paths
}

/// The path the text after a marker names, when it names one at all.
///
/// The line number is what makes a token a site: `crates/x/a.rs:331:5` is where
/// something failed, while a marker followed by a quote is the runner echoing
/// the search it was told to run. What is left of `path:line` or `path:line:col`
/// is the path.
fn path_before_a_line_number(rest: &str) -> Option<String> {
    // A panic prints `path:line:col:` with the colon that ends the site still
    // attached, so the token is trimmed of it before it is taken apart.
    let token = rest.split_whitespace().next()?.trim_end_matches(':');
    let (head, column) = token.rsplit_once(':')?;
    if !is_a_number(column) {
        return None;
    }
    let path = match head.rsplit_once(':') {
        Some((path, line)) if is_a_number(line) => path,
        _ => head,
    };
    if !path.ends_with(".rs") || path.starts_with('/') || path.starts_with("../") {
        return None;
    }
    Some(path.to_string())
}

fn is_a_number(text: &str) -> bool {
    !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit())
}

/// The principle slugs a source file's header declares.
///
/// Read from the start of a line, with no tolerance for indentation: the reader
/// that builds the registry anchors its pattern the same way, and a reader that
/// accepted a declaration the registry does not see would answer for a gate that
/// nothing measures.
pub fn declared_slugs(source: &str) -> Vec<String> {
    let mut slugs = Vec::new();
    for line in source.lines() {
        let Some(rest) = PLATFORM_DECLARATION_PREFIXES
            .iter()
            .find_map(|prefix| line.strip_prefix(prefix))
        else {
            continue;
        };
        let slug = rest.split_whitespace().next().unwrap_or("");
        if is_a_slug(slug) {
            slugs.push(slug.to_string());
        }
    }
    slugs
}

/// A slug is the memory's filename, so it is spelled like one: no punctuation,
/// no spaces, and nothing that could only be prose.
fn is_a_slug(text: &str) -> bool {
    !text.is_empty()
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// Report what one red revision's log says about the principles it names.
///
/// Answers nothing: every outcome is either a report to the account or a line in
/// the log, and neither is a reason to hold a landing. A revision whose commit
/// time cannot be read is not reported at all rather than stamped with the
/// current clock -- a refusal stamped with "now" mints a fresh row on every pass
/// over the same failure, which is the duplicate the revision-keyed record
/// exists to prevent.
pub async fn report(workdir: &Path, store: &dyn PrincipleLedgerStore, rev: &str, log: &str) {
    if rev.is_empty() || log.is_empty() {
        return;
    }
    let paths = failing_paths(log);
    if paths.is_empty() {
        return;
    }
    let Some(refused_at) = commit_instant(workdir, rev).await else {
        tracing::warn!(
            rev = %rev,
            paths = paths.len(),
            "a red revision named a file but its commit time could not be read; the refusal is \
             not reported rather than stamped with the clock, which would mint a row per pass"
        );
        return;
    };

    let mut slugs: BTreeSet<String> = BTreeSet::new();
    let mut unreadable = 0usize;
    let mut read = 0usize;
    for path in paths.iter().take(MAX_FILES_READ) {
        match run_git(workdir, &["show", &format!("{rev}:{path}")]).await {
            Ok(source) => {
                read += 1;
                slugs.extend(declared_slugs(&source));
            }
            // A path in a log is not necessarily a path this revision holds: it
            // can come from another job's tree, from a dependency, or from a
            // file the revision no longer has. Counted rather than logged one by
            // one, because the count is what separates "this run named no
            // principle" from "nothing this run named could be read".
            Err(_) => unreadable += 1,
        }
    }
    if slugs.is_empty() {
        tracing::info!(
            rev = %rev,
            paths = paths.len(),
            read,
            unreadable,
            "a red revision named no principle: no file it failed in declares one"
        );
        return;
    }

    let events: Vec<PrincipleRefusal> = slugs
        .iter()
        .map(|slug| PrincipleRefusal {
            slug: slug.clone(),
            refused_at,
            source: format!("ci:{rev}"),
            count: 1,
        })
        .collect();
    match store.report_refusals(&events).await {
        Ok(queued) => tracing::info!(
            rev = %rev,
            paths = paths.len(),
            read,
            unreadable,
            slugs = slugs.len(),
            queued,
            "a red revision's declared principles were reported as refusals"
        ),
        Err(e) => tracing::warn!(
            rev = %rev,
            slugs = slugs.len(),
            error = %e,
            "a red revision's refusals could not be queued; the landing is unaffected"
        ),
    }
}

/// When the revision was committed, as the instant its refusals are stamped
/// with.
///
/// The commit's own time rather than the observation's: it is a property of the
/// revision, so everything that observes the same failure computes the same key.
async fn commit_instant(workdir: &Path, rev: &str) -> Option<DateTime<Utc>> {
    let out = run_git(workdir, &["show", "-s", "--format=%ct", rev])
        .await
        .ok()?;
    let secs: i64 = out.trim().parse().ok()?;
    Utc.timestamp_opt(secs, 0).single()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{
        PrincipleCensus, PrincipleCheckKindCensus, PrincipleFold, PrincipleMeasurement,
        PrincipleReading, PrincipleRow, SFResult,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A log excerpt carrying every shape the runner printed for one failing
    /// check: the command it echoed (which contains the search pattern), the two
    /// panic lines it printed twice, and the `##[error]` copies.
    ///
    /// Verbatim but for the surrounding lines, from the failing run this mapping
    /// was written against.
    const RED_LOG: &str = "\
2026-10-16T17:24:25.8656855Z \u{1b}[36;1m  grep --no-group-separator -A 4 -E \"^(test .* FAILED|failures:|test result: FAILED|error(\\[E[0-9]+\\])?:)|panicked at\" \\\n\
2026-10-16T17:30:25.0726342Z thread 'the_table_covers_the_population_it_measures' (23427) panicked at crates/cogneva/tests/file_size_census_contract_test.rs:331:5:\n\
2026-10-16T17:30:25.1263572Z test result: FAILED. 5 passed; 1 failed\n\
2026-10-16T17:30:53.0084832Z ##[error]thread 'the_table_covers_the_population_it_measures' (23427) panicked at crates/cogneva/tests/file_size_census_contract_test.rs:331:5:\n\
";

    #[test]
    fn a_marker_the_runner_echoed_is_not_a_failure_site() {
        // The echo names the marker and nothing after it, so the log's own
        // search string must not be read as a file.
        let paths = failing_paths(RED_LOG);
        assert_eq!(
            paths.iter().cloned().collect::<Vec<_>>(),
            vec!["crates/cogneva/tests/file_size_census_contract_test.rs".to_string()]
        );
    }

    #[test]
    fn a_compiler_error_points_at_a_file_too() {
        let paths = failing_paths("error[E0308]: mismatched types\n --> crates/x/src/y.rs:12:5\n");
        assert!(paths.contains("crates/x/src/y.rs"));
    }

    #[test]
    fn a_path_without_a_line_number_is_not_read() {
        // Nothing to say where it failed, so nothing to say it failed.
        assert!(failing_paths("panicked at crates/x/src/y.rs is missing").is_empty());
        assert!(failing_paths("panicked at (not a path)").is_empty());
        // A marker that names a file without pointing anywhere in it is a
        // sentence about a failure -- or a command echoing one -- rather than
        // the site of one, and the line number is the only part of the site that
        // says which.
        assert!(failing_paths("  grep -A 4 -E 'panicked at crates/x/src/y.rs' \\").is_empty());
        assert!(failing_paths("panicked at crates/x/src/y.rs:not-a-number").is_empty());
    }

    #[test]
    fn only_rust_sources_are_candidates() {
        assert!(failing_paths("panicked at scripts/run.sh:3:1").is_empty());
        assert!(failing_paths("panicked at /abs/path/x.rs:3:1").is_empty());
    }

    /// A shell gate declares in the comment syntax its language has. The shape
    /// is the same one the registry reads, so a gate the account counts is a gate
    /// a red run can name.
    #[test]
    fn a_shell_gate_declares_with_its_own_prefix() {
        let source = "# principle: feedback_a\n#principle: feedback_b\n  # principle: feedback_c\n";
        assert_eq!(declared_slugs(source), vec!["feedback_a".to_string()]);
    }

    #[test]
    fn the_declaration_prefix_has_to_start_the_line() {
        let source = "//! principle: feedback_a\n  //! principle: feedback_b\n/// principle: feedback_c\n//! principle: feedback_d trailing words\n//! principle: not-a-slug!\n";
        assert_eq!(
            declared_slugs(source),
            vec!["feedback_a".to_string(), "feedback_d".to_string()]
        );
    }

    /// Records what it was asked to queue, and is unimplemented for everything
    /// else: the refusal queue is the only face this module writes.
    #[derive(Default)]
    struct RecordingStore {
        events: Mutex<Vec<PrincipleRefusal>>,
    }

    #[async_trait]
    impl PrincipleLedgerStore for RecordingStore {
        async fn record_readings(&self, _: &[PrincipleReading]) -> SFResult<usize> {
            unimplemented!()
        }
        async fn declare(&self, _: &[PrincipleReading]) -> SFResult<usize> {
            unimplemented!()
        }
        async fn fold_declarations(&self, _: usize) -> SFResult<PrincipleFold> {
            unimplemented!()
        }
        async fn pending_declarations(&self) -> SFResult<u64> {
            unimplemented!()
        }
        async fn apply_principle_measurement(&self, _: &PrincipleMeasurement) -> SFResult<()> {
            unimplemented!()
        }
        async fn record_human_only(&self, _: &str, _: Option<&str>) -> SFResult<()> {
            unimplemented!()
        }
        async fn unsettled_judgement(&self, _: usize) -> SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn outstanding(&self, _: usize) -> SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn reopen_platform_gate_drift(
            &self,
            _: &HashMap<String, String>,
        ) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn refused(&self, _: usize) -> SFResult<Vec<PrincipleRow>> {
            unimplemented!()
        }
        async fn get(&self, _: &str) -> SFResult<Option<PrincipleRow>> {
            unimplemented!()
        }
        async fn slugs(&self) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn census(&self) -> SFResult<PrincipleCensus> {
            unimplemented!()
        }
        async fn check_kind_census(&self) -> SFResult<PrincipleCheckKindCensus> {
            unimplemented!()
        }
        async fn report_refusals(&self, events: &[PrincipleRefusal]) -> SFResult<usize> {
            self.events.lock().unwrap().extend_from_slice(events);
            Ok(events.len())
        }
        async fn fold_refusals(&self, _: usize) -> SFResult<usize> {
            unimplemented!()
        }
        async fn pending_refusals(&self) -> SFResult<u64> {
            unimplemented!()
        }
    }

    /// A repository holding one revision whose failing test declares two
    /// principles.
    async fn repo_with_a_declaring_file() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "t@example.com"],
            vec!["config", "user.name", "t"],
        ] {
            run_git(path, &args).await.unwrap();
        }
        std::fs::create_dir_all(path.join("crates/x/tests")).unwrap();
        std::fs::write(
            path.join("crates/x/tests/a.rs"),
            "//! principle: feedback_one\n//! principle: feedback_two\n",
        )
        .unwrap();
        std::fs::write(path.join("crates/x/tests/b.rs"), "fn test() {}\n").unwrap();
        run_git(path, &["add", "--", "crates"]).await.unwrap();
        run_git(path, &["commit", "-q", "-m", "one"]).await.unwrap();
        let rev = run_git(path, &["rev-parse", "HEAD"]).await.unwrap();
        (dir, rev.trim().to_string())
    }

    #[tokio::test]
    async fn a_red_revision_reports_one_refusal_per_declared_principle() {
        let (dir, rev) = repo_with_a_declaring_file().await;
        let log = "thread 'a' (1) panicked at crates/x/tests/a.rs:3:5:\n\
                   thread 'b' (2) panicked at crates/x/tests/b.rs:1:1:\n";
        let store = RecordingStore::default();
        report(dir.path(), &store, &rev, log).await;

        let events = store.events.lock().unwrap().clone();
        let mut slugs: Vec<&str> = events.iter().map(|e| e.slug.as_str()).collect();
        slugs.sort_unstable();
        assert_eq!(slugs, vec!["feedback_one", "feedback_two"]);
        // The file that declares nothing contributes no refusal, and the instant
        // is the revision's own, which is what makes a second observation of the
        // same failure a no-op on the account's key rather than a second row.
        assert!(events.iter().all(|e| e.source == format!("ci:{rev}")));
        let expected = commit_instant(dir.path(), &rev).await.unwrap();
        assert!(events.iter().all(|e| e.refused_at == expected));

        // The second job of the same run sees the same site: reported again, it
        // computes exactly the same key -- all three columns of it, the instant
        // included. That is what makes the account's upsert fold the two jobs
        // into one row instead of minting a row per job.
        let store2 = RecordingStore::default();
        report(dir.path(), &store2, &rev, log).await;
        let key = |events: &[PrincipleRefusal]| {
            let mut keys: Vec<(String, DateTime<Utc>, String)> = events
                .iter()
                .map(|e| (e.slug.clone(), e.refused_at, e.source.clone()))
                .collect();
            keys.sort();
            keys
        };
        assert_eq!(
            key(&events),
            key(&store2.events.lock().unwrap().clone()),
            "a second observer of one failure must compute the same key"
        );
    }

    #[tokio::test]
    async fn a_revision_whose_time_cannot_be_read_is_not_reported() {
        // A tree is not a commit: the file it names is readable, but there is no
        // instant belonging to it. The refusal is then not reported at all --
        // stamping it with the clock instead would mint a fresh row on every
        // pass over the same failure, which is why the instant is taken from
        // the revision to begin with. A revision git cannot resolve at all would
        // not tell these two behaviours apart, because the file would be
        // unreadable too and no slug would come out either way.
        let (dir, _) = repo_with_a_declaring_file().await;
        let tree = run_git(dir.path(), &["rev-parse", "HEAD^{tree}"])
            .await
            .unwrap()
            .trim()
            .to_string();
        assert!(
            commit_instant(dir.path(), &tree).await.is_none(),
            "a tree must not have an instant of its own, or this case proves nothing"
        );
        let store = RecordingStore::default();
        report(
            dir.path(),
            &store,
            &tree,
            "thread 'a' (1) panicked at crates/x/tests/a.rs:3:5:\n",
        )
        .await;
        assert!(store.events.lock().unwrap().is_empty());
    }
}
