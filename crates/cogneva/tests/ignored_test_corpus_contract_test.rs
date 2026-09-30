//! The workflow's `--ignored` steps and the workspace's ignore-marked tests
//! have to name each other.
//!
//! A test that needs something the default run does not have -- most often a
//! server -- is marked so that `cargo test` skips it, and the only thing that
//! ever runs it is a step in `.github/workflows/ci.yml` naming its binary. The
//! local verification stage runs no `--ignored` step at all, so for the whole
//! corpus the workflow is the only reader there is. Both directions of that
//! pairing fail silently, and both have failed here:
//!
//! - The step outlives its tests. `--test <name>` fails when the file is
//!   renamed or deleted, but not when the tests inside it are deleted, renamed,
//!   or lose the marker; the step then passes having run nothing. A step that
//!   carries a test-name filter instead of a whole binary is worse: cargo
//!   accepts a filter that matches nothing and exits 0.
//! - The test is written and the step never is. `metrics_latest_reading.rs` and
//!   `metrics_retirement.rs` were written, reviewed and marked ignored, and were
//!   run by nothing at all until they were noticed in a hand census on
//!   2026-09-28. Marked-ignored looks in every output like a deliberate choice,
//!   so nothing asks again whether anyone runs it.
//!
//! The second direction is the expensive one: the corpus is exactly the tests
//! whose subject cannot be reached without a server, which is where the defects
//! that no fixture catches live. So the check is written down here instead of
//! being redone by hand: the ignore-marked set is read from the tree, the
//! naming set is read from the workflow, and a site that is in neither a step
//! nor the excused list below fails.
//!
//! The excused list is the place a decision goes, one line per site with its
//! reason. It is not an escape hatch: an excuse for a site that has since been
//! named by a step, or that no longer holds an ignored test, fails as well -- a
//! list that only ever licenses is a list that stops describing the tree.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// The workflow whose `--ignored` steps name the corpus.
const WORKFLOW: &str = ".github/workflows/ci.yml";

/// The attribute that marks a test as not part of the default run, spelled in
/// two pieces on purpose.
///
/// This file searches the tree for the marker, and its own text is part of the
/// tree: written whole, the needle would match the scanner, and the scanner
/// would report itself as an ignored test binary that no step names.
const IGNORE_MARKER: &str = concat!("#[ig", "nore");

/// Ignore-marked sites no step names, each with the reason it is left out.
///
/// An entry here says "this one is deliberately unread", which is a decision; a
/// site that is absent from both this list and the workflow is a decision nobody
/// made, which is the failure the test below exists to catch.
const UNNAMED: &[(&str, &str)] = &[
    (
        "crates/cog-redis/src/lib.rs",
        "The one test here asserts that the default configuration stalls the \
         driver for over a minute, and the waiting is the finding; running it \
         would add minutes to every CI run to re-establish a number nobody acts \
         on. Its own comment says as much.",
    ),
    (
        "crates/cog-memory/tests/reranker_boundary.rs",
        "Needs the reranker weights fetched first, so it is a manual reading \
         against a materialised model rather than a step. Same reasoning as the \
         stall above: the cost is a download per run, and the question it asks \
         is about a model that is not in the repository.",
    ),
];

/// What a step selects, as far as naming tests goes.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Target {
    /// `--test <name>`: the integration binary `crates/<crate>/tests/<name>.rs`.
    Binary(String),
    /// `--lib`: the crate's library target, so everything under its `src/`.
    Library,
    /// Neither: every target the crate has.
    Every,
}

/// One `cargo test ... -- --ignored` step in the workflow.
#[derive(Debug)]
struct Step {
    /// 1-based line in the workflow, so a failure can point at it.
    line: usize,
    krate: String,
    target: Target,
}

/// An ignore-marked site in the tree, and the step shape that would name it.
#[derive(Debug)]
struct Ignored {
    /// Workspace-relative path, as written in `UNNAMED`.
    path: String,
    krate: String,
    site: Site,
}

#[derive(Debug)]
enum Site {
    /// A top-level file under `tests/` -- its own binary, named by its stem.
    Binary(String),
    /// Anywhere under `src/`, reached by naming the crate's library target.
    Library,
}

/// The steps that ask for ignored tests, read from the workflow.
///
/// Every such step is understood or the read fails: a step this function
/// skipped would be a step whose tests nothing checks, which is the hole.
fn ignored_steps(workflow: &str) -> Vec<Step> {
    let mut steps = Vec::new();
    for (index, line) in workflow.lines().enumerate() {
        let Some(command) = line.trim().strip_prefix("run:") else {
            continue;
        };
        let command = command.trim();
        if !command.split_whitespace().any(|word| word == "--ignored") {
            continue;
        }
        let lineno = index + 1;

        let (selector, flags) = command.split_once(" -- ").unwrap_or_else(|| {
            panic!(
                "{WORKFLOW}:{lineno} runs `{command}`, which asks for ignored tests without \
                 ever separating them from the selector with ` -- `. This contract reads that \
                 boundary, so a step spelled some other way has to be answered here rather \
                 than skipped: skipping it is how its tests leave the census."
            )
        });
        let flags: Vec<&str> = flags.split_whitespace().collect();

        // A positional argument after `--` is a test-name filter. cargo accepts
        // one that matches nothing and exits 0, so a renamed test turns the step
        // green while it runs nothing -- the one shape of this step that can go
        // quiet without anyone editing it.
        if let Some(filter) = flags.iter().find(|word| !word.starts_with('-')) {
            panic!(
                "{WORKFLOW}:{lineno} runs `{command}`, whose arguments after ` -- ` include the \
                 positional `{filter}`. cargo reads that as a test-name filter and exits 0 when \
                 it matches nothing, so renaming the test this step exists for would turn the \
                 step into one that passes having run nothing. Name the whole binary (`--test \
                 <name>` or `--lib`) instead: then a rename fails loudly."
            );
        }

        let words: Vec<&str> = selector.split_whitespace().collect();
        if words.len() < 3 || words[0] != "cargo" || words[1] != "test" {
            panic!(
                "{WORKFLOW}:{lineno} runs `{command}`; this contract understands `cargo test` \
                 invocations, and it has to understand every step that asks for ignored tests \
                 or the ones it does not are its blind spot."
            );
        }
        let mut krate: Option<&str> = None;
        let mut target = Target::Every;
        let mut rest = words[2..].iter();
        while let Some(word) = rest.next() {
            match *word {
                "-p" | "--package" => krate = rest.next().copied(),
                "--test" => {
                    let name = rest.next().copied().unwrap_or_else(|| {
                        panic!("{WORKFLOW}:{lineno} runs `{command}` with `--test` and no name")
                    });
                    target = Target::Binary(name.to_string());
                }
                "--lib" => target = Target::Library,
                other => panic!(
                    "{WORKFLOW}:{lineno} runs `{command}`, and this contract does not know what \
                     `{other}` selects. It judges which tests a step runs, so an argument it \
                     cannot read is one it would quietly ignore -- teach it this shape rather \
                     than let the step be counted while nothing checks it."
                ),
            }
        }
        let krate = krate
            .unwrap_or_else(|| {
                panic!(
                    "{WORKFLOW}:{lineno} runs `{command}` with no `-p <crate>`, so this contract \
                     cannot tell which crate's tests the step names"
                )
            })
            .to_string();
        steps.push(Step {
            line: lineno,
            krate,
            target,
        });
    }
    steps
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
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

/// The crate directory a `-p <crate>` names.
///
/// The package name and the directory name are the same throughout this
/// workspace, and a step naming a package this cannot locate would otherwise be
/// judged against an empty tree and pass.
fn crate_dir(root: &Path, krate: &str) -> PathBuf {
    let dir = root.join("crates").join(krate);
    assert!(
        dir.join("Cargo.toml").is_file(),
        "`-p {krate}` in {WORKFLOW} names no crate directory under crates/; this contract \
         resolves a package to its directory to read the tests the step runs, and one it \
         cannot resolve is one it would judge against nothing"
    );
    dir
}

/// Every `.rs` file under `dir`, with its path relative to `dir` split into
/// segments. `target/` is skipped: it holds copies, not sources.
///
/// A directory that cannot be listed fails rather than being skipped: a subtree
/// this cannot read is a subtree whose ignored tests are missing from the
/// census, and the census is the only thing that says a test is unread.
fn collect_rs(dir: &Path, rel: &mut Vec<String>, out: &mut Vec<(PathBuf, Vec<String>)>) {
    let entries =
        std::fs::read_dir(dir).unwrap_or_else(|e| panic!("could not list {}: {e}", dir.display()));
    for entry in entries {
        let entry =
            entry.unwrap_or_else(|e| panic!("could not read an entry of {}: {e}", dir.display()));
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "target" {
            continue;
        }
        let path = entry.path();
        rel.push(name.clone());
        if path.is_dir() {
            collect_rs(&path, rel, out);
        } else if name.ends_with(".rs") {
            out.push((path, rel.clone()));
        }
        rel.pop();
    }
}

/// Whether a file that was just found by a directory walk carries the marker.
///
/// Unreadable is a failure, not a "no": a file this cannot read is a file whose
/// ignored tests are silently absent from the census.
fn contains_marker(path: &Path) -> bool {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", path.display()))
        .contains(IGNORE_MARKER)
}

/// The crate directories of the workspace, by directory name.
fn crate_dirs(root: &Path) -> Vec<String> {
    let crates = root.join("crates");
    let entries = std::fs::read_dir(&crates)
        .unwrap_or_else(|e| panic!("could not read {}: {e}", crates.display()));
    let mut names: Vec<String> = entries
        .map(|entry| {
            entry.unwrap_or_else(|e| panic!("could not read an entry of {}: {e}", crates.display()))
        })
        .filter(|entry| entry.path().join("Cargo.toml").is_file())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect();
    names.sort();
    names
}

/// Every ignore-marked site in the workspace, with what could name it.
///
/// A site in a location this contract cannot classify fails rather than being
/// skipped: a scan that silently under-reads is a scan whose census is wrong in
/// the direction that looks like a pass.
fn ignored_sites() -> Vec<Ignored> {
    let root = workspace_root();
    let mut sites = Vec::new();
    let mut unreadable = Vec::new();
    for krate in crate_dirs(&root) {
        let dir = root.join("crates").join(&krate);
        let mut files = Vec::new();
        collect_rs(&dir, &mut Vec::new(), &mut files);
        for (path, rel) in files {
            if !contains_marker(&path) {
                continue;
            }
            // Relative to the workspace, because that is how the excused list
            // below spells a site and how a failure message has to read: the
            // crate directory is reached through the workspace root, so the
            // unresolved form carries the walk's own `crates/<crate>/../..`.
            let path = path.strip_prefix(&root).unwrap_or(&path);
            let path = path.to_string_lossy().to_string();
            let site = match rel.as_slice() {
                [tests, file] if tests == "tests" && file.ends_with(".rs") => {
                    Site::Binary(file.trim_end_matches(".rs").to_string())
                }
                [first, ..] if first == "src" => Site::Library,
                // A file under `tests/` that is not a top-level one is a module
                // reached through whichever binary includes it, and a marker in
                // a `benches/`, `examples/` or build-script file is reached by
                // something this contract does not model. Either way it is a
                // test nothing here knows how to name.
                _ => {
                    unreadable.push(path);
                    continue;
                }
            };
            sites.push(Ignored {
                path,
                krate: krate.clone(),
                site,
            });
        }
    }
    assert!(
        unreadable.is_empty(),
        "{unreadable:?} carry the ignore marker outside the two shapes this contract reads \
         (a top-level file under `tests/`, or a file under `src/`). Nothing here knows which \
         step runs them, so they are a hole in the census; teach this contract that shape, or \
         move the test to one it reads"
    );
    sites
}

/// The ignore-marked sites the workflow's steps name.
struct Named {
    binaries: BTreeSet<(String, String)>,
    libraries: BTreeSet<String>,
}

impl Named {
    fn from(steps: &[Step]) -> Self {
        let mut named = Named {
            binaries: BTreeSet::new(),
            libraries: BTreeSet::new(),
        };
        for step in steps {
            match &step.target {
                Target::Binary(name) => {
                    named.binaries.insert((step.krate.clone(), name.clone()));
                }
                Target::Library | Target::Every => {
                    named.libraries.insert(step.krate.clone());
                }
            }
        }
        named
    }

    fn names(&self, site: &Ignored) -> bool {
        match &site.site {
            Site::Binary(stem) => self.binaries.contains(&(site.krate.clone(), stem.clone())),
            Site::Library => self.libraries.contains(&site.krate),
        }
    }
}

/// A step that names a binary which is gone, or which no longer has an ignored
/// test in it, is a step that passes having run nothing.
#[test]
fn every_ignored_step_runs_something_that_is_still_ignored() {
    let root = workspace_root();
    let steps = ignored_steps(&workflow());
    assert!(
        !steps.is_empty(),
        "read no `--ignored` step out of {WORKFLOW}. Either the workflow stopped asking for the \
         ignored corpus -- in which case the tests it used to run are all unread now -- or this \
         contract lost the shape it reads, and judging an empty set of steps would pass while \
         covering nothing"
    );

    for step in &steps {
        let dir = crate_dir(&root, &step.krate);
        let spelled = match &step.target {
            Target::Binary(name) => format!("--test {name}"),
            Target::Library => "--lib".to_string(),
            Target::Every => "no target selector".to_string(),
        };

        if let Target::Binary(name) = &step.target {
            let file = dir.join("tests").join(format!("{name}.rs"));
            assert!(
                file.is_file(),
                "{WORKFLOW}:{} runs `cargo test -p {} {spelled} -- --ignored`, and {} does not \
                 exist. (cargo itself fails on this, so the step is not quiet -- but the \
                 message here names the pairing rather than a missing target.)",
                step.line,
                step.krate,
                file.display()
            );
            assert!(
                contains_marker(&file),
                "{WORKFLOW}:{} runs `cargo test -p {} {spelled} -- --ignored`, and {} holds no \
                 test with the ignore marker. The step still exits 0: `--test <name> -- \
                 --ignored` with nothing to select is a pass that ran no test at all, which is \
                 the same as the step not being there, except that it looks like coverage.",
                step.line,
                step.krate,
                file.display()
            );
            continue;
        }

        let scope = match &step.target {
            Target::Library => dir.join("src"),
            _ => dir.clone(),
        };
        let mut files = Vec::new();
        collect_rs(&scope, &mut Vec::new(), &mut files);
        let held: Vec<PathBuf> = files
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| contains_marker(path))
            .collect();
        assert!(
            !held.is_empty(),
            "{WORKFLOW}:{} runs `cargo test -p {} {spelled} -- --ignored`, and {} holds no test \
             with the ignore marker. The step passes having run nothing.",
            step.line,
            step.krate,
            scope.display()
        );
    }
}

/// A test the workflow never names is a test nobody runs.
#[test]
fn every_ignored_test_is_named_by_a_step_or_excused() {
    let sites = ignored_sites();
    assert!(
        !sites.is_empty(),
        "the scan found no ignore-marked test anywhere under crates/, which cannot be true of \
         this tree. A census that reads nothing reports no gaps"
    );

    let named = Named::from(&ignored_steps(&workflow()));
    let unnamed: Vec<&str> = sites
        .iter()
        .filter(|site| !named.names(site))
        .map(|site| site.path.as_str())
        .filter(|path| !UNNAMED.iter().any(|(excused, _)| excused == path))
        .collect();
    assert!(
        unnamed.is_empty(),
        "{unnamed:?} hold an ignored test and no step in {WORKFLOW} names them, so nothing runs \
         them -- not the local stage, which runs no `--ignored` step, and not CI. Marked-ignored \
         reads in every output as a deliberate choice, which is why this is invisible from both \
         ends. Add the step that runs each one (see the live-DB steps for the shape), or add it \
         to UNNAMED with the reason it is left out: the second is a decision a reviewer can \
         check, the first is the hole."
    );
}

/// An excuse that outlived its reason reads as a decision and is a stale answer.
#[test]
fn an_excuse_survives_only_while_its_site_is_unnamed_and_still_ignored() {
    let sites = ignored_sites();
    let named = Named::from(&ignored_steps(&workflow()));
    for (path, reason) in UNNAMED {
        assert!(
            !reason.trim().is_empty(),
            "{path} is excused in this file with no reason. \"Left out\" with no reason is \
             indistinguishable from \"forgotten\"."
        );
        let site = sites
            .iter()
            .find(|site| site.path == *path)
            .unwrap_or_else(|| {
                panic!(
                    "{path} is excused here but holds no ignored test any more. The excuse now \
                 licenses nothing, and the next reader takes it for a live decision: drop the \
                 entry."
                )
            });
        assert!(
            !named.names(site),
            "{path} is excused here as one no step names, and a step in {WORKFLOW} names it now. \
             The excuse is no longer true, and an excuse that is no longer true is where the \
             next unnamed test will hide: drop the entry."
        );
    }
}
