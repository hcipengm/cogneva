//! Gate: every `.rs` the repository carries at or above 50 KiB has been judged, once,
//! as one axis, several axes, or exempt — and the table is the only place that judgement
//! lives.
//!
//! Why a census and not a rule. A size threshold is a *prohibitive* gate: it names a shape
//! that must not appear, and every red it produces is an instruction to split a file. A
//! prohibitive gate written before the legitimate uses of its shape have been counted reds
//! on the first legitimate use it meets, and a gate that cries wolf costs more than the
//! defect it was aimed at, because the next real finding gets read as another false alarm.
//! So the population is measured and judged first, and the rule — if one is ever written —
//! is written against the axis this table found rather than against bytes. The owner's
//! 2026-10-14 ruling is the axis: split a file when it holds two groups of readers or two
//! change cadences, not when it is large. The hard form of that is two runtime entities
//! that do not share a process — one side's failure is invisible from the other, and no
//! single build, test or reader sees both. A file with one such entity can be a megabyte
//! and still be one file. The separation has to be structural, too: the other side runs
//! under its own process entry point, not merely at another time. Two loops in one
//! resident process are one subsystem, and splitting one buys no separation between them.
//!
//! What is checked here, and what is not. The population and the sizes are read from the
//! tree on every run, so a file that crosses the threshold with no row fails, and a row
//! whose file fell below it — or left the tree — fails too. The verdicts are judgements and
//! cannot be recomputed; what can be checked is that each one is a member of the closed set
//! and that its witness is real. A `multi-axis` row has to name both sides: the file's own
//! item, which the file must declare, and the other side's process entry point, which has
//! to be a subcommand in the binary's dispatch table. That is what `does not share a
//! process` means in a form a machine can read — without it the verdict is a sentence
//! about a run that once happened rather than a structure, and that reading is what this
//! column was rewritten to catch. What the entry-point check still cannot say is that the
//! command it finds runs *that* side: it says the command is real. Naming the right one is
//! a judgement the column carries and the checker does not, the same ceiling the file's own
//! item has.
//!
//! Not a size gate. Nothing here refuses a file for being large, and nothing here asks for
//! one to be split. It refuses a *large file nobody has judged* — which is the state this
//! tree was in for all sixty of them.
//!
//! principle: feedback_a_prohibitive_gate_needs_a_counterexample_census_first
//! principle: feedback_split_a_file_by_change_axis_not_by_byte_size
//! principle: feedback_two_running_entities_means_a_second_process_entrypoint_not_a_scenario

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

/// The one carrier of the census: one row per file, judged.
const CENSUS_FILE: &str = "debt/file-size-census.tsv";

/// Where the population starts.
///
/// 50 KiB is the threshold the ruling counted against, not a bound anyone derived. It is a
/// screening line: the point is to have looked at every file a size rule would plausibly
/// name. The smallest file here is 51,223 bytes, 23 above the line — so the line is a
/// screen and not a bound: a file just under it is not obviously a different kind of file,
/// and the next crossing is expected to arrive as a row to judge rather than as a debate
/// about bytes.
const THRESHOLD_BYTES: u64 = 50 * 1024;

/// The closed set of verdicts.
const VERDICTS: &[&str] = &["multi-axis", "single-axis", "exempt"];

/// The closed set of reasons a file is out of a production-code size rule's reach.
///
/// `registry` is a table whose size is the number of entries it holds: splitting it would
/// give one closed set two carriers, which is the failure the metric names and their help
/// text exist to prevent. `test-harness` is a file that only a test compiles.
const EXEMPT_REASONS: &[&str] = &["registry", "test-harness"];

/// One row of the census.
#[derive(Debug, Clone)]
struct Row {
    file: String,
    verdict: String,
    /// `;`-separated item names for a judged file, or the single reason token when exempt.
    witness: String,
}

impl Row {
    fn witnesses(&self) -> Vec<&str> {
        self.witness
            .split(';')
            .map(str::trim)
            .filter(|w| !w.is_empty())
            .collect()
    }
}

/// The rows, parsed once.
fn rows() -> &'static [Row] {
    static ROWS: OnceLock<Vec<Row>> = OnceLock::new();
    ROWS.get_or_init(|| {
        read_repo_file(CENSUS_FILE)
            .lines()
            .filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
            .filter_map(|line| {
                let mut field = line.split('\t');
                let file = field.next()?.trim();
                // The column-name line is the table's header, not a row.
                if file == "file" {
                    return None;
                }
                Some(Row {
                    file: file.to_string(),
                    verdict: field.next().unwrap_or("").trim().to_string(),
                    witness: field.next().unwrap_or("").trim().to_string(),
                })
            })
            .collect()
    })
}

/// Read a repo file named relative to this crate's manifest, failing loudly: a census that
/// read nothing would pass every assertion by having no rows.
fn read_repo_file(relative: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

/// The repository root, derived from this crate's own manifest so the test does not depend
/// on the working directory it is run from.
fn workspace_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::canonicalize(&root).unwrap_or(root)
}

/// Every `.rs` the repository carries under `crates/`, at or above the threshold, with the
/// size the working tree holds right now.
///
/// The universe is what the repository carries — `git ls-files`, not a walk — for the same
/// reason the module-tree gate reads it that way: an untracked file is a scratch file or a
/// probe, and nothing is owed for it. Reading the size from the working tree rather than
/// the index is deliberate: the question is how large the file a person would have to read
/// is, and that is the file on disk.
fn population() -> BTreeMap<String, u64> {
    let root = workspace_root();
    let out = Command::new("git")
        .arg("-C")
        .arg(&root)
        .args(["ls-files", "-z", "--", "crates"])
        .output()
        .expect("git must be runnable from a test binary");
    assert!(
        out.status.success(),
        "git ls-files failed, so the set of files the repository carries is unknown: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = std::str::from_utf8(&out.stdout).expect("this repository's paths are UTF-8");
    text.split('\0')
        .filter(|rel| !rel.is_empty() && rel.ends_with(".rs"))
        .filter_map(|rel| {
            let size = std::fs::metadata(root.join(rel)).ok()?.len();
            (size >= THRESHOLD_BYTES).then(|| (rel.to_string(), size))
        })
        .collect()
}

/// Whether `file` declares `name` as an item — `fn`, `struct`, `enum`, `trait`, `impl` or
/// `mod` — so that a witness points at something the file actually holds.
///
/// The name has to end where the item's name ends, so `land` is not satisfied by
/// `landing_dir`, and `Config` is not satisfied by `ConfigLoader`.
fn declares(file: &str, name: &str) -> bool {
    let Ok(text) = std::fs::read_to_string(workspace_root().join(file)) else {
        return false;
    };
    ["fn", "struct", "enum", "trait", "impl", "mod"]
        .iter()
        .any(|keyword| {
            let needle = format!("{keyword} {name}");
            text.match_indices(&needle).any(|(at, _)| {
                let after = text[at + needle.len()..].chars().next();
                !after.is_some_and(|c| c.is_alphanumeric() || c == '_')
            })
        })
}

/// The process entry points the `cogneva` binary publishes: the subcommand names in
/// `crates/cogneva/src/cli.rs`'s dispatch table, which is also what a Job's container
/// command runs.
///
/// Read out of that table rather than by searching the crate for the string: a name sitting
/// in a comment or a test is not an entry point, and the table is the one place a
/// subcommand is declared. Reading it is what separates "the other side runs in a process
/// of its own" from "the other side once ran in another process".
fn process_entry_points() -> &'static BTreeSet<String> {
    static POINTS: OnceLock<BTreeSet<String>> = OnceLock::new();
    POINTS.get_or_init(|| {
        let text = read_repo_file("src/cli.rs");
        let rest = text
            .split_once("const SUBCOMMANDS")
            .expect("crates/cogneva/src/cli.rs no longer declares a SUBCOMMANDS table")
            .1;
        let body = rest
            .split_once("= [")
            .expect("the SUBCOMMANDS table is no longer a table of subcommand names")
            .1;
        let body = body
            .split_once("];")
            .expect("the SUBCOMMANDS table has no end, so its entries cannot be read")
            .0;
        // Every odd-numbered chunk of a split on the quote is inside a pair of them.
        body.split('"')
            .skip(1)
            .step_by(2)
            .map(str::to_string)
            .collect()
    })
}

/// Every row's verdict is one of the closed set, its witness is the shape that verdict
/// calls for, and the file's own side of that witness is an item the file declares.
///
/// The witness is asserted in both directions because that is what makes the column a
/// claim rather than a label: a `multi-axis` row names two sides that must both be real —
/// the file's item, and the process entry point that runs only the other side — a
/// `single-axis` row names exactly one item, and an `exempt` row names a reason from the
/// closed set instead of a file's contents.
#[test]
fn every_row_carries_a_verdict_and_a_witness_of_that_shape() {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for row in rows() {
        assert!(
            VERDICTS.contains(&row.verdict.as_str()),
            "{CENSUS_FILE} gives `{}` the verdict `{}`, which is not one of {VERDICTS:?} — a \
             verdict outside the set is a sentence wearing a label, and the label is the half \
             this gate can check",
            row.file,
            row.verdict
        );
        assert!(
            seen.insert(row.file.as_str()),
            "{CENSUS_FILE} carries `{}` twice",
            row.file
        );

        let witnesses = row.witnesses();
        match row.verdict.as_str() {
            "multi-axis" => {
                assert_eq!(
                    witnesses.len(),
                    2,
                    "{CENSUS_FILE} calls `{}` multi-axis but names {} witness(es) — the verdict \
                     is a claim about two entities that do not share a process, and it takes \
                     exactly two names: the file's own item and the entry point that runs the \
                     other side",
                    row.file,
                    witnesses.len()
                );
                assert!(
                    process_entry_points().contains(witnesses[1]),
                    "{CENSUS_FILE} calls `{}` multi-axis and names `{}` as the other side's \
                     process entry point, but no such subcommand is in \
                     crates/cogneva/src/cli.rs's dispatch table — two sides that do not share \
                     a process means the other side has a process of its own, and a name that \
                     runs nothing is a story about a process rather than a structure",
                    row.file,
                    witnesses[1]
                );
            }
            "single-axis" => {
                assert_eq!(
                    witnesses.len(),
                    1,
                    "{CENSUS_FILE} gives `{}` the verdict single-axis and {} witness(es) — the \
                     witness is the one item carrying the file's one cadence",
                    row.file,
                    witnesses.len()
                );
            }
            "exempt" => {
                assert_eq!(
                    witnesses.len(),
                    1,
                    "{CENSUS_FILE} gives `{}` the verdict exempt and {} witness(es) — an exempt \
                     row names its reason, not a file's contents",
                    row.file,
                    witnesses.len()
                );
                assert!(
                    EXEMPT_REASONS.contains(&witnesses[0]),
                    "{CENSUS_FILE} exempts `{}` for `{}`, which is not one of {EXEMPT_REASONS:?}",
                    row.file,
                    witnesses[0]
                );
                continue;
            }
            other => unreachable!("verdict `{other}` passed the closed-set check"),
        }

        // The file's own side of the witness, which is what an item can be checked
        // against. A `multi-axis` row's second name is the other side's entry point and is
        // deliberately not in this file: it is checked against the dispatch table above.
        let missing: Vec<&str> = witnesses
            .iter()
            .copied()
            .take(1)
            .filter(|name| !declares(&row.file, name))
            .collect();
        assert!(
            missing.is_empty(),
            "{CENSUS_FILE} judges `{}` {} and points at {missing:?}, which the file does not \
             declare as an `fn`, `struct`, `enum`, `trait`, `impl` or `mod` — the witness is \
             what separates a judgement about this file from a word in a column",
            row.file,
            row.verdict
        );
    }
}

/// Every `.rs` the repository carries under `crates/` at or above the threshold has a row,
/// and every row's file is one — asserted from both sides, which is what makes the table a
/// census rather than a list of files someone happened to look at.
#[test]
fn the_table_covers_the_population_it_measures() {
    let population = population();
    assert!(
        !population.is_empty(),
        "no `.rs` the repository carries under crates/ is at or above {} bytes, so this gate \
         would pass on nothing — that is a broken measurement, not a small tree",
        THRESHOLD_BYTES
    );

    let registered: BTreeSet<&str> = rows().iter().map(|row| row.file.as_str()).collect();
    let unjudged: Vec<(&str, u64)> = population
        .iter()
        .filter(|(file, _)| !registered.contains(file.as_str()))
        .map(|(file, size)| (file.as_str(), *size))
        .collect();
    assert!(
        unjudged.is_empty(),
        "these files are at or above {} bytes and {CENSUS_FILE} does not judge them: \
         {unjudged:?} — a size rule cannot be written against a population nobody has counted, \
         so each one needs a verdict and a witness",
        THRESHOLD_BYTES
    );

    let stale: Vec<String> = rows()
        .iter()
        .filter(|row| !population.contains_key(&row.file))
        .map(|row| {
            let path = workspace_root().join(&row.file);
            if !path.is_file() {
                format!("`{}` — the tree carries no such file", row.file)
            } else {
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                format!("`{}` — it is {size} bytes now", row.file)
            }
        })
        .collect();
    assert!(
        stale.is_empty(),
        "these rows describe a tree that is no longer here: {stale:?} — the table is a \
         description of the population at the threshold, and only the population's members \
         belong in it"
    );
}

/// The judgements the census can be held to, so that they are pinned rather than prose.
///
/// Three of these are the reviewer's falsifiable reading: the deployer holds two runtime
/// entities in different pods and is the walk through the axes; the two metric tables are
/// registries whose size is the number of series they name, and splitting either would give
/// one closed set two carriers. A census that reads the same way about all three would have
/// found no axis at all.
///
/// The fourth came out of checking the third: `crates/cog-github/src/landing.rs` was judged
/// multi-axis when this table landed, and reading it against a process entry point moved it
/// to one axis — its two sides are two loops in the same resident process, and the story
/// that made it look otherwise (a landing that has to survive the process that made it) is
/// a reason to persist state, not a second runtime entity. It is pinned here because the
/// two ways of being wrong do not cost the same: one axis too few is a file nobody touches,
/// and one axis too many sends someone to split a cohesive subsystem.
#[test]
fn the_census_reads_the_files_the_judgements_are_about() {
    let verdict_of = |file: &str| -> String {
        rows()
            .iter()
            .find(|row| row.file == file)
            .unwrap_or_else(|| panic!("{CENSUS_FILE} carries no row for `{file}`"))
            .verdict
            .clone()
    };

    assert_eq!(
        verdict_of("crates/cog-reflection/src/mainline_deployer.rs"),
        "multi-axis",
        "the one file the ruling names as a split is judged at the axis level: its build side \
         runs in the evolution pod and its rollout side in a job, and neither is present when \
         the other runs. A census that reads it as one axis has stopped distinguishing the \
         shape the ruling is about"
    );

    assert_eq!(
        verdict_of("crates/cog-github/src/landing.rs"),
        "single-axis",
        "the landing side and the watch loop are two loops in one resident process — the \
         landing is called in place by this crate's plugin and the watch is a loop that same \
         plugin starts, and neither has a subcommand running it alone. A verdict of \
         `multi-axis` here is a file sent to be split for a separation its two sides do not \
         have"
    );

    for registry in [
        "crates/cog-core/src/contract/metric_names.rs",
        "crates/cog-core/src/contract/metric_help.rs",
    ] {
        let verdict = verdict_of(registry);
        assert!(
            verdict == "single-axis" || verdict == "exempt",
            "`{registry}` is judged `{verdict}` — it is a registry whose size is the number of \
             entries it holds, and the two halves (the name and what it means) are one closed \
             set split across two files on purpose. A verdict of `multi-axis` here is the exact \
             false positive a size rule written without this census would produce"
        );
    }
}
