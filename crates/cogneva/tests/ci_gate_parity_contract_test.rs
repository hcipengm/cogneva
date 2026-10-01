//! Every job CI treats as gating has an answer in the pre-land verification.
//!
//! The verification stage runs before a change is committed, and its whole
//! purpose is to be the last place a defective change can still be refused for
//! free. It carries its own list of what to check — and that list is a
//! transcription of *some* of what CI checks. Nothing makes the two move
//! together, so each time CI grows a gating job the transcription falls one
//! behind, and the gap is invisible from both ends: CI is green for everything
//! that reaches it, and a change that fails only the new job passes verification
//! and is committed.
//!
//! What it costs is not the refusal — it is that the refusal arrives after the
//! change has been committed, built in release, and landed. On 2026-09-28 that
//! happened with `clippy`: `cargo clippy --workspace -- -D warnings` reported
//! `function start_failure_is_stale is never used`, which is rustc's own
//! `dead_code` promoted by `-D warnings`. The verification stage compiles the
//! same crate with the default lint level, where that is a warning, so the
//! change passed, landed, and was reverted eleven minutes later.
//!
//! The same hole had already appeared as `fmt`, and it was closed the same way
//! this test is trying not to be: by adding the one missing check to the
//! verification stage. That fixes the instance and leaves the carrier, which is
//! the transcription itself.
//!
//! So the contract is read from CI, not restated: `release-tag.needs` is the set
//! of jobs the workflow itself declares as "all green before a release", so a
//! new gating job changes that declaration first and this test goes red on the
//! next run — unless someone comes here and answers for it, one line per job,
//! with the reason it is or is not enforced before the commit.
//!
//! A job that is answered below but not actually enforced is a decision someone
//! made and can be reviewed. A job that is missing is a decision nobody made.
//! Those are different failures, and only the second one is silent.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The workflow whose `release-tag` job declares the gating set.
const WORKFLOW: &str = ".github/workflows/ci.yml";

/// The job whose `needs` is the declaration. Named rather than searched for:
/// a passing test has to read the list it claims to judge, and finding it by
/// pattern would let a renamed job turn this into a test that judges nothing.
const DECLARING_JOB: &str = "release-tag";

/// Gating jobs the verification stage enforces before the change is committed,
/// each with the CI command it answers.
///
/// The command is spelled out so that the entry is checkable against the
/// workflow: an entry here whose command no longer appears in the job of the
/// same name fails the test below, rather than quietly licensing a check that
/// was renamed out of existence.
const ENFORCED: &[(&str, &str)] = &[
    // The formatter runs on the tree before the verdict is reached, and a tree
    // the formatter can settle is conformed rather than refused.
    ("fmt", "cargo fmt --all -- --check"),
    // The main suite, with `--no-fail-fast` so the refusal names every failure
    // rather than the first crate to give up.
    ("test", "cargo test --workspace --no-fail-fast"),
];

/// Gating jobs the verification stage does not enforce, and why.
///
/// Every entry here is a change that passed verification and could still be
/// reverted after the release build. They are listed rather than omitted so
/// that the list reads as a decision, and so that closing one is a one-line
/// edit rather than an archaeology.
const NOT_ENFORCED: &[(&str, &str)] = &[
    (
        "check",
        "cargo check --workspace is a strictly narrower criterion than the \
         `test` job's compile, which the stage already runs: anything check \
         rejects, the test compile rejects too, and it is the same compile.",
    ),
    (
        "clippy",
        "The observed failure of 2026-09-28, answered narrowly rather than \
         fully. The stage runs the same whole-tree `cargo clippy --workspace \
         -- -D warnings` before the suite, and refuses a change only on \
         diagnostics whose primary span lands on a line that change writes — \
         the worktree diff against the tree the linter read, and nothing at all \
         on a baseline the formatter would rewrite, since that rewrite would be \
         in the same diff. It is narrower than the job, not equivalent and \
         cheaper: a lint whose span stays on a line the change never wrote \
         passes here and dies in the job, as deleting the only caller of an \
         unmodified function leaves `dead_code` on that function's line. \
         Reading the tree's own lint set per revision would close that half and \
         was declined for its cost — a second whole-tree compile per change, \
         against a build slot that is the ceiling on how many changes the stage \
         can retire.",
    ),
    (
        "coverage",
        "cargo llvm-cov --fail-under-lines 40 judges the whole workspace's \
         line coverage, not the change's, and needs its own instrumented \
         rebuild. Same shape as clippy: a tree-level criterion that would need \
         a baseline before it could judge a change.",
    ),
    (
        "entry-scripts",
        "shellcheck over every tracked *.sh, the deploy script tests, and the \
         PowerShell bootstrap's syntax. Deterministic, and reachable from the \
         stage — but it is a different toolchain on a different surface, so it \
         is not covered by any compile the stage already runs. A change that \
         touches a script passes verification and dies here.",
    ),
    (
        "deploy-parity",
        "check-deploy-parity.sh, render-deploy.sh --check and the git identity \
         wiring check all read deploy/ and the chart. Reachable from the stage \
         for the same cost as any other shell step, and nothing in the Cargo \
         workspace covers it. A change that touches deploy/ passes verification \
         and dies here.",
    ),
    (
        "bootstrap-cross-platform",
        "cargo check -p cogneva-bootstrap. Narrower than the `test` job's \
         compile of the same crate, on the same host toolchain — the cross \
         platform part is the runner, not the criterion.",
    ),
    (
        "version-contract",
        "Runs the version_contract example against the repository's own git \
         history and tags. It judges the tag and the workspace version, neither \
         of which a change can move by being applied — a change that trips it \
         is one that edited the version declaration, and that is what the \
         example is for.",
    ),
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The job names `DECLARING_JOB` needs, read from the workflow.
///
/// Returns them unparsed-but-checked: callers assert on the set, and the
/// function itself refuses to return an empty one, because an empty set would
/// make every assertion below pass for the wrong reason.
fn gating_jobs(workflow: &str) -> BTreeSet<String> {
    let body = workflow
        .split_once(&format!("  {DECLARING_JOB}:"))
        .unwrap_or_else(|| panic!("{WORKFLOW} has no `{DECLARING_JOB}` job; it is the declaration this contract reads"))
        .1;

    let mut lines = body.lines().skip_while(|l| l.trim() != "needs:");
    assert!(
        lines.next().is_some(),
        "the `{DECLARING_JOB}` job declares no `needs:` list; with nothing to read, \
         this test would judge an empty set and pass while covering nothing"
    );

    let jobs: BTreeSet<String> = lines
        .take_while(|l| l.trim_start().starts_with("- "))
        .map(|l| l.trim().trim_start_matches("- ").to_string())
        .collect();

    assert!(
        jobs.len() > 1,
        "read {} gating job(s) from `{DECLARING_JOB}.needs`, which cannot be the \
         whole declaration; a parse that silently under-reads turns this contract \
         into a test that judges nothing",
        jobs.len()
    );
    jobs
}

/// The workflow, or a failure naming why it could not be read.
///
/// Missing the file is not a pass: a contract that reads nothing agrees with
/// everything.
fn workflow() -> String {
    let path = workspace_root().join(WORKFLOW);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()))
}

/// The names the base branch goes by, in the order they are tried.
///
/// The name differs by checkout and the difference means nothing: CI's checkout
/// calls it `origin/main`, the deployer's clone keeps the base branch as a local
/// `main` and names the forge it pulls from `upstream`, and a developer's clone
/// has `origin`. Every one of them is the base branch, so the first that
/// resolves is the one to read.
const BASE_NAMES: &[&str] = &["origin/main", "main", "upstream/main", "local/main"];

/// The declaration as it stands on the base branch.
///
/// `workflow()` reads the tree under judgment, and so do the tables above: they
/// answer a question that the change itself asked. A change that drops a job
/// from `release-tag.needs` and drops its row from the tables removes the
/// question and the answer in one edit, and every test in this file still
/// passes while a gate CI used to run has stopped running. The declaration on
/// the base branch is the one this change did not write.
///
/// Not resolving it is a failure rather than a fallback to the tree under
/// judgment: a contract that reads nothing agrees with everything.
fn base_workflow() -> String {
    let root = workspace_root();
    for name in BASE_NAMES {
        let out = std::process::Command::new("git")
            .current_dir(&root)
            .args(["show", &format!("{name}:{WORKFLOW}")])
            .output()
            .unwrap_or_else(|e| panic!("could not run git in {}: {e}", root.display()));
        if out.status.success() {
            return String::from_utf8(out.stdout)
                .unwrap_or_else(|e| panic!("{name}:{WORKFLOW} is not utf-8: {e}"));
        }
    }
    panic!(
        "none of {BASE_NAMES:?} resolves {WORKFLOW} in {}; this contract judges a \
         change against the gating set the base branch declares, so a checkout \
         that cannot see the base branch cannot tell whether the change dropped \
         one of its jobs. Fetch the base branch rather than letting the contract \
         pass by reading nothing.",
        root.display()
    );
}

#[test]
fn every_gating_job_is_answered_before_the_commit() {
    let workflow = workflow();
    let declared = gating_jobs(&workflow);

    let enforced: BTreeSet<&str> = ENFORCED.iter().map(|(job, _)| *job).collect();
    let not_enforced: BTreeSet<&str> = NOT_ENFORCED.iter().map(|(job, _)| *job).collect();

    let unanswered: Vec<&str> = declared
        .iter()
        .map(String::as_str)
        .filter(|job| !enforced.contains(job) && !not_enforced.contains(job))
        .collect();
    assert!(
        unanswered.is_empty(),
        "CI gates on {unanswered:?}, and the pre-land verification stage has no \
         answer for them. A change that fails only these passes verification, is \
         committed, built in release, and lands — the refusal arrives after the \
         work it was supposed to save. Add each to ENFORCED or to NOT_ENFORCED \
         with the reason: an unanswered job is a decision nobody made, which is \
         the only failure mode here that is silent."
    );

    for job in declared.iter().map(String::as_str) {
        if let Some((_, reason)) = ENFORCED.iter().find(|(j, _)| *j == job) {
            assert!(
                !reason.trim().is_empty(),
                "`{job}` is listed as enforced in {WORKFLOW} but its entry names \
                 no command; an entry with nothing to check cannot be checked"
            );
        } else if let Some((_, reason)) = NOT_ENFORCED.iter().find(|(j, _)| *j == job) {
            assert!(
                !reason.trim().is_empty(),
                "`{job}` is listed as not enforced but carries no reason. \"Not \
                 enforced\" with no reason is indistinguishable from \"forgotten\"."
            );
        }
    }
}

/// The gating set grows with a change; it does not shrink with one.
///
/// The tests above read the declaration from the tree under judgment, which is
/// also the tree the tables above were written in. A change that drops a job
/// from `release-tag.needs` and drops its row from those tables is therefore
/// internally consistent, and the only thing that would notice the job is no
/// longer gating is the base branch's copy of the declaration.
///
/// So a job cannot be dropped by the change that stops answering for it: doing
/// that takes an edit to this assertion in the same commit, which is not a
/// consequence of some other line but the decision itself, made where a reviewer
/// reads it. Adding a job stays what it was — one line in `release-tag.needs`
/// and one row below, or a red test until someone writes the row.
#[test]
fn the_gating_set_does_not_shrink_with_a_change() {
    let base = gating_jobs(&base_workflow());
    let head = gating_jobs(&workflow());
    let dropped: Vec<&str> = base.difference(&head).map(String::as_str).collect();
    assert!(
        dropped.is_empty(),
        "{dropped:?} gate on the base branch and this change drops them. A job \
         that stops gating stops being run, and the declaration is the only place \
         the gating set is written down, so nothing else would have noticed. A \
         drop is a decision, and it has to be taken here -- in this assertion, \
         where a reviewer reads it -- rather than in a `needs:` list, where it \
         looks like tidying."
    );
}

/// A job name that appears in both tables means the two lists disagree about
/// it, and whichever one the reader consults first decides the answer.
#[test]
fn no_gating_job_is_answered_twice() {
    let enforced: BTreeSet<&str> = ENFORCED.iter().map(|(job, _)| *job).collect();
    let not_enforced: BTreeSet<&str> = NOT_ENFORCED.iter().map(|(job, _)| *job).collect();
    let both: Vec<&&str> = enforced.intersection(&not_enforced).collect();
    assert!(
        both.is_empty(),
        "{both:?} appear in both ENFORCED and NOT_ENFORCED; the two tables have \
         to agree about what the verification stage does"
    );
}

/// The job a table entry names has to exist in the workflow at all — otherwise
/// a renamed CI job leaves a stale answer behind that the parity test above
/// would happily keep counting as coverage.
#[test]
fn no_answer_names_a_job_that_no_longer_gates() {
    let workflow = workflow();
    let declared = gating_jobs(&workflow);
    let stale: Vec<&str> = ENFORCED
        .iter()
        .chain(NOT_ENFORCED.iter())
        .map(|(job, _)| *job)
        .filter(|job| !declared.contains(*job))
        .collect();
    assert!(
        stale.is_empty(),
        "{stale:?} are answered here but no longer gate anything in {WORKFLOW}. \
         A renamed or dropped job leaves the old answer looking like coverage."
    );
}
