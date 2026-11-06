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

use crate::common;

use common::routes::{read_repo, repo_root};

use std::collections::BTreeSet;

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
    // The live-service checks are one script on both sides: the job runs this
    // line, and the sandbox's verification runs the same script as its own gate
    // (the `check-` prefix is what makes it one). The command is the same bytes
    // on both sides rather than a transcription, which is the whole reason the
    // sixty-four steps became a list.
    // What is still not the same is the server: CI's is a throwaway container,
    // the sandbox's is the cluster's own PostgreSQL -- which is why the script
    // builds a database of its own before it judges anything.
    ("live", "bash deploy/scripts/check-live-services.sh"),
];

/// Gating jobs the verification stage enforces in part: what it runs, and what
/// it does not.
///
/// The binary split above was the whole model until the stage started reading
/// the workflow's own `run:` lines: back then a job was either one of the three
/// cargo commands the stage ran or a surface it never touched, and "not
/// enforced" was the honest answer for anything else. Now the stage runs every
/// single-line gate the workflow names, so several jobs are neither: most of
/// what they judge runs before the commit, and a named residue does not.
///
/// Keeping those in `NOT_ENFORCED` would say a change that trips them passes
/// verification -- false for `deploy-parity`, whose parity and render checks
/// are the stage's own gates. Moving them to `ENFORCED` would say the residue
/// is covered -- also false. So the row carries both, and the residue is the
/// part a reader has to know about, because it is the part that still fails
/// after the change has landed.
const PARTLY_ENFORCED: &[(&str, &str, &str)] = &[
    (
        "test",
        "The job's command with its scope word swapped: `--workspace` becomes \
         one `-p` per package the change touches and per package that depends \
         on them, transitively -- every dependency kind, dev-dependencies \
         included, because a dev-dependency links two members for `cargo test` \
         exactly as a normal edge links them for `cargo check`. Every other \
         argument -- `--no-fail-fast`, the message format -- is left alone. A \
         change that only edits files inside workspace members is judged by \
         those packages' suites, and by the suites of everything they would \
         break, before the commit.",
        "What the dependency graph does not carry. A test in a package that \
         neither was edited nor depends on an edited one does not run -- the \
         assembly is always added (the tree-wide censuses live there), so what \
         is left of that is a test that fails on state the graph cannot see: a \
         database schema, a fixture, a rendered manifest. A change to a file \
         outside every member (the root manifest, `deploy/`, a root document) \
         has no package whose suite would cover it, so that case falls back to \
         the whole workspace. A change wide enough to reach more than six \
         packages also falls back to the whole workspace, which errs wide \
         rather than narrow.",
    ),
    (
        "check",
        "`cargo clippy --workspace --all-targets` over the same package set and \
         the same targets, and clippy is the compiler with more lints: every \
         error `cargo check` would report on that set is an error here first.",
        "The verdict, not the compile. A diagnostic whose span is not a line the \
         change writes is not attributed to it, so a call site the change broke \
         compiles red at this stage and is refused by the suite that runs next \
         rather than by the lint verdict -- the refusal exists, the attribution \
         does not. Packages with no dependency path from the change are not \
         compiled, which is the safe direction: nothing the change edits is \
         reachable from them.",
    ),
    (
        "clippy",
        "The stage runs the job's command before the suite — the argv itself, \
         held equal to this job's `run:` line by `cog-reflection`'s \
         `the_stages_linter_is_the_ci_jobs_linter`, which reads the line out of \
         this workflow rather than restating it, so the two cannot drift apart \
         one flag at a time — over the packages the change touches and \
         everything that depends on them. A finding is the change's if its \
         primary span lands on a line the change writes; a finding anywhere \
         else is the change's if the same command, run over the same packages \
         with the change taken off, did not report it. That second reading is \
         what answers the 2026-09-28 shape: deleting the only caller of a \
         function leaves `dead_code` on the function's own line, in a file the \
         diff never opened.",
        "The packages with no dependency path from the change are not compiled. \
         `-D warnings` is the job's and not the stage's, which costs nothing \
         here: the stage reads warnings and errors alike, so the level is not \
         what separates the two. The baseline compile is a second compile of the \
         same packages, paid only when the linter reports something — and not at \
         all on a baseline the formatter would rewrite, where no line can be \
         told from the rewrite's and nothing is attributed to the change.",
    ),
    (
        "entry-scripts",
        "`bash deploy/scripts/check-entry-scripts.sh` is the job's own command \
         and the stage's: the three blocks of body it used to be (shellcheck \
         over every tracked `*.sh`, the script tests, the PowerShell parse) are \
         one script now, called from this workflow and from the stage by the \
         same line. Its shellcheck section needs git and shellcheck, and the \
         image carries shellcheck for exactly this gate -- the first version of \
         this row said the section ran wherever the stage runs while the image \
         had no shellcheck in it, so the stage ran nothing at all for this job \
         and recorded it as answered.",
        "The other two sections need helm, kubectl, promtool, psql and pwsh, and \
         the image carries none of them. psql and promtool have apt packages; \
         helm, kubectl and pwsh do not, so there is no path to them that is not \
         a download the cluster network does not reach. The script exits 2 -- \
         \"I cannot run this\" -- and names every one, rather than reporting a \
         green it did not reach, which is why the residue is visible instead of \
         silent. Until the image carries the rest, a change that breaks one of \
         the script tests still passes verification.",
    ),
    (
        "repo-conventions",
        "The job's twenty-seven single-line readings -- comment citations, \
         capacity figures, shared-database counting, the live-step `if:`, \
         README paths, the image build context -- are `run: bash \
         deploy/scripts/check-*.sh` lines, which is the shape the stage reads \
         out of this workflow. They run before every commit.",
        "`check-commit-attribution.sh` reads the commits a push adds, from the \
         workflow's `BASE_SHA` and `HEAD_SHA`. The stage runs before there is a \
         push, so there is nothing for it to judge; the reading can refuse a \
         push but cannot undo an attribution already written into history.",
    ),
    (
        "deploy-parity",
        "`check-deploy-parity.sh`, `render-deploy.sh --check`, \
         `sync-static-manifests.sh --check` and the rest of the job's \
         twenty-two single-line readings are the stage's gates by the same \
         rule: a path under `scripts/check-`, or an argument that is exactly \
         `--check`.",
        "`test-acceptance-census-alert-rules.sh` is the one judgement in this \
         job written as a multi-line body, and it needs promtool. A change that \
         breaks an alert rule passes verification and dies here.",
    ),
];

/// Gating jobs the verification stage does not enforce, and why.
///
/// Every entry here is a change that passed verification and could still be
/// reverted after the release build. They are listed rather than omitted so
/// that the list reads as a decision, and so that closing one is a one-line
/// edit rather than an archaeology.
const NOT_ENFORCED: &[(&str, &str)] = &[
    (
        "coverage",
        "cargo llvm-cov --fail-under-lines 40 judges the whole workspace's \
         line coverage, not the change's, and needs its own instrumented \
         rebuild. Same shape as clippy: a tree-level criterion that would need \
         a baseline before it could judge a change.",
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

    // 注释不是清单结束的标志。2026-11-06 在 `needs:` 中间插了一行注释（说明那一格
    // 为什么必须一起判），而按「第一行不是 `- ` 就停」读，读到的是前两项：八条登记
    // 当场变成「答的是一个不再门禁的作业」，真实的那一格反倒从没进过读数。注释与
    // 空行跳过去，遇到别的形状才停。
    let jobs: BTreeSet<String> = lines
        .take_while(|l| {
            let t = l.trim();
            // 先按「继续读」的理由筛，最后才判结束。
            t.is_empty() || t.starts_with('#') || t.starts_with("- ")
        })
        .filter(|l| l.trim().starts_with("- "))
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
    read_repo(WORKFLOW)
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
    let root = repo_root();
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
    let partly: BTreeSet<&str> = PARTLY_ENFORCED.iter().map(|(job, _, _)| *job).collect();
    let not_enforced: BTreeSet<&str> = NOT_ENFORCED.iter().map(|(job, _)| *job).collect();

    let unanswered: Vec<&str> = declared
        .iter()
        .map(String::as_str)
        .filter(|job| {
            !enforced.contains(job) && !partly.contains(job) && !not_enforced.contains(job)
        })
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
        if let Some((_, runs, not_run)) = PARTLY_ENFORCED.iter().find(|(j, _, _)| *j == job) {
            assert!(
                !runs.trim().is_empty() && !not_run.trim().is_empty(),
                "`{job}` is listed as partly enforced but one of its two halves \
                 is empty: a row that says neither what runs nor what does not \
                 is not a partial answer, it is a missing one"
            );
        } else if let Some((_, reason)) = ENFORCED.iter().find(|(j, _)| *j == job) {
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
    let tables: [(&str, BTreeSet<&str>); 3] = [
        ("ENFORCED", ENFORCED.iter().map(|(job, _)| *job).collect()),
        (
            "PARTLY_ENFORCED",
            PARTLY_ENFORCED.iter().map(|(job, _, _)| *job).collect(),
        ),
        (
            "NOT_ENFORCED",
            NOT_ENFORCED.iter().map(|(job, _)| *job).collect(),
        ),
    ];
    for (i, (left_name, left)) in tables.iter().enumerate() {
        for (right_name, right) in tables.iter().skip(i + 1) {
            let both: Vec<&&str> = left.intersection(right).collect();
            assert!(
                both.is_empty(),
                "{both:?} appear in both {left_name} and {right_name}; the tables \
                 have to agree about what the verification stage does"
            );
        }
    }
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
        .map(|(job, _)| *job)
        .chain(PARTLY_ENFORCED.iter().map(|(job, _, _)| *job))
        .chain(NOT_ENFORCED.iter().map(|(job, _)| *job))
        .filter(|job| !declared.contains(*job))
        .collect();
    assert!(
        stale.is_empty(),
        "{stale:?} are answered here but no longer gate anything in {WORKFLOW}. \
         A renamed or dropped job leaves the old answer looking like coverage."
    );
}
