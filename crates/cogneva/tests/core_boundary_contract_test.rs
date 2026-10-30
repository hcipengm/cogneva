//! Guards the contract layer boundary.
//!
//! `cog-core` defines traits and cross-crate types; concrete storage drivers
//! belong to `cog-storage`. Depending on `sqlx`/`redis` there would force every
//! consumer of the contracts to link a driver, so the boundary is asserted
//! rather than left to review.
//!
//! The driver ban is the sharp end of a larger claim: the base crate is where
//! the *shape* of what crosses between crates lives, and everything else -- the
//! loop that runs, the file that is written, the row that is read -- belongs to
//! the crate that owns that job. That claim cannot be read off the source (what
//! counts as business is a judgement), but the first step toward breaking it
//! can: a new dependency on the base, or a new module in it. So both are frozen
//! the same way the edge graph is frozen -- a name the table does not carry
//! fails the build, and a name whose entry is gone fails too -- and the reason
//! beside each row is what makes "it is only a small helper" a sentence
//! somebody had to write down.
//!
//! Two dependencies were removed rather than listed when this table was
//! written: `toml` and `zstd` were declared and never named in a single source
//! file. That is the other direction of the same rule -- the base crate is
//! supposed to get smaller, and a dependency nobody uses is weight every
//! consumer of the contracts links for nothing.
//!
//! This gate lives here rather than under `crates/cog-core/tests/` because its
//! subject is the workspace's layering, not anything inside the base crate: the
//! base is the crate that is supposed to stop changing, and adding a test
//! directory to it makes every future boundary question a commit to the crate
//! nobody is supposed to touch. The workspace-level gates already live in this
//! crate -- `crate_dependency_contract_test` walks the same graph from the same
//! side -- so the two read as one picture here, and the base crate keeps no
//! test target of its own.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

mod common;

/// Where the base crate's sources sit relative to the workspace root.
const BASE_SRC: &str = "crates/cog-core/src";
/// The base crate's manifest, workspace-relative.
const BASE_MANIFEST: &str = "crates/cog-core/Cargo.toml";

/// The dependencies `cog-core` declares for the artifact it ships, and what each
/// one is for.
///
/// The second column is not checked against anything -- it is here because a
/// frozen list without reasons is a list somebody appends to. A row that cannot
/// fill it in is a row that has not been thought about.
const DEPENDENCIES: &[(&str, &str)] = &[
    (
        "async-trait",
        "trait objects across an await point in the contracts",
    ),
    (
        "base64",
        "the payload encoding the stream and platform-sign contracts speak",
    ),
    ("blake3", "the digest both sides of an audit entry compute"),
    ("bytes", "the frames the stream and storage contracts carry"),
    ("chrono", "timestamps on the cross-crate types"),
    ("futures", "the combinators the stream contracts hand back"),
    (
        "hmac",
        "platform-sign: one rule for two processes, keys held apart",
    ),
    (
        "jsonschema",
        "validating against a schema the contracts carry themselves",
    ),
    (
        "libc",
        "the host's own numbers, which the admission and build-cache contracts name",
    ),
    ("regex", "the shapes the secret redactor recognises"),
    ("schemars", "the JSON schemas the contract types derive"),
    ("serde", "the derives on every cross-crate type"),
    (
        "serde_json",
        "the wire form of those types and of the config body",
    ),
    ("sha2", "the digests governance acts and agent state carry"),
    ("thiserror", "the contract error enums"),
    (
        "tokio",
        "the async traits the contracts are defined against",
    ),
    (
        "tracing",
        "the spans the loop-health and observability contracts publish",
    ),
    ("uuid", "identifiers on the cross-crate types"),
];

/// What a top-level module in the base crate is allowed to be.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    /// A contract: traits and types other crates describe their work with.
    Contract,
    /// A name, a constant or a pure reading of one -- vocabulary several crates
    /// spell the same way. Nothing is read, written or decided here.
    Vocabulary,
    /// An act: it opens a file, spawns, queries a store, or keeps state. These
    /// are the rows a boundary argument is about.
    Mechanism,
}

/// Every top-level module the base crate carries, what kind it is, and why it
/// is there rather than in the crate whose work it is.
///
/// The boundary rule is "contracts, not business". That sentence cannot be
/// checked directly -- what counts as business is a judgement -- but the thing
/// that precedes every violation of it can: a module appears here. So the list
/// is frozen the same way the dependency set is, and the third column is the
/// question a `Mechanism` row has to answer out loud: *which crate owns this
/// work, and why can it not live there?*
///
/// The honest answers today are two. Some mechanisms are read by most of the
/// workspace (`loop_health` by thirteen crates, `paths` by sixteen); a crate
/// other than `cogneva` may depend on `cog-core` and nothing else, so a shared
/// act has no other home that does not become a sideways edge. The rest are
/// waiting to move, and the rows say so.
const MODULES: &[(&str, Kind, &str)] = &[
    ("admission", Kind::Mechanism, "reads the host's own pressure; three crates read it and none of them owns the host"),
    ("atomic_write", Kind::Mechanism, "replaces a file whole; four crates write files and they are not each other's dependents"),
    ("audit", Kind::Contract, "the audit entry's shape and the chain both sides verify"),
    ("build_cache", Kind::Contract, "the reading a build cache's size travels under"),
    ("build_cache_readings", Kind::Mechanism, "reads the cache's layers; cog-reflection and cog-extension both do"),
    ("build_cache_reclaim", Kind::Mechanism, "deletes what the plan in `contract` names; the only caller is this crate's own cache readings, so the seam between planning and deleting is inside one mechanism rather than across crates"),
    ("claim_footprint", Kind::Mechanism, "walks a claim-backed directory; five crates read it under different names"),
    ("config", Kind::Contract, "the configuration document every crate is handed"),
    ("config_sections", Kind::Contract, "which parts of that document reach a process"),
    ("contract", Kind::Contract, "the contracts themselves"),
    ("day_key", Kind::Vocabulary, "the date form a day goes into a name with"),
    ("env", Kind::Vocabulary, "the environment names the platform sets"),
    ("error", Kind::Contract, "the error enum the contracts hand back"),
    ("fs_size", Kind::Mechanism, "walks a directory tree for its bytes; three crates ask, one owns the tree"),
    ("git_budget", Kind::Vocabulary, "how long a git command may run"),
    ("loop_health", Kind::Mechanism, "the registry of loops; thirteen crates report into it"),
    ("observability_text", Kind::Mechanism, "renders a reading as text; five crates publish that text"),
    ("paths", Kind::Vocabulary, "where the image puts the process's own files"),
    ("reading_cadence", Kind::Vocabulary, "how often an unchanged reading is written again"),
    ("retention", Kind::Vocabulary, "how a 'how many days' knob is read"),
    ("roles", Kind::Vocabulary, "the names the platform gives its workers"),
    ("secrets", Kind::Mechanism, "secret providers and redaction; every crate logs through it"),
    ("sql", Kind::Vocabulary, "quoting a name into SQL, and the two rulers for a relation"),
    ("token_estimate", Kind::Vocabulary, "the chars-per-token ruler cog-agent and cog-memory both price text with; pure arithmetic over the text it is handed, and two copies would be two units"),
    ("topology", Kind::Vocabulary, "the names a deployment gives its workloads"),
    ("types", Kind::Contract, "the cross-crate types"),
    ("vocabulary", Kind::Vocabulary, "filling a query template's slots"),
];

/// The top-level modules the base crate's `lib.rs` declares.
fn modules() -> BTreeSet<String> {
    let lib = common::routes::read_repo(&format!("{BASE_SRC}/lib.rs"));
    lib.lines()
        .filter_map(|line| {
            let line = line.trim();
            // A declaration ends in `;`. The alias modules below are written
            // `pub mod agent {` and re-export a contract that is already above
            // them, so they are not modules of their own.
            let rest = line.strip_prefix("pub mod ")?.strip_suffix(';')?;
            Some(rest.trim().to_owned())
        })
        .collect()
}

/// The section names in a manifest whose entries are dependencies, and the
/// table each one is frozen against.
///
/// Only `[dependencies]` has a table today: a dev dependency is not linked into
/// anything shipped, so it cannot hand another crate's consumer a driver.
const FROZEN_SECTIONS: &[&str] = &["dependencies"];

/// The keys declared in `[section]` of a manifest body.
///
/// Deliberately a line reader over the section rather than a TOML parser: this
/// gate has no business adding a parser to the workspace to read four sections,
/// and the question -- which names are declared here -- does not need one.
fn declared(manifest: &str, section: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut inside = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            // Headings carry their brackets: `[dependencies]` is the section
            // named `dependencies`.
            inside = trimmed.trim_start_matches('[').trim_end_matches(']').trim() == section;
            continue;
        }
        if !inside || trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if let Some((key, _)) = trimmed.split_once('=') {
            out.insert(key.trim().to_owned());
        }
    }
    out
}

/// Every `.rs` the base crate compiles from `src/`.
fn sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&common::root::repo_root().join(BASE_SRC), &mut out);
    out
}

#[test]
fn core_does_not_depend_on_storage_drivers() {
    let manifest = common::routes::read_repo(BASE_MANIFEST);
    let declared = declared(&manifest, "dependencies");
    for banned in ["sqlx", "redis"] {
        assert!(
            !declared.contains(banned),
            "cog-core must not depend on `{banned}`; storage drivers live in cog-storage"
        );
    }
}

#[test]
fn core_sources_do_not_name_storage_drivers() {
    let files = sources();
    assert!(
        !files.is_empty(),
        "no sources found under {BASE_SRC} -- this gate is measuring nothing"
    );
    for path in files {
        let src = std::fs::read_to_string(&path).expect("source file is readable");
        for banned in ["sqlx::", "redis::"] {
            assert!(
                !src.contains(banned),
                "cog-core sources must not reference `{banned}` ({})",
                path.display()
            );
        }
    }
}

/// A dependency the table does not name fails, so the base cannot grow quietly.
#[test]
fn a_new_dependency_is_not_in_the_table() {
    let manifest = common::routes::read_repo(BASE_MANIFEST);
    let table: BTreeSet<&str> = DEPENDENCIES.iter().map(|(name, _)| *name).collect();
    for section in FROZEN_SECTIONS {
        let declared = declared(&manifest, section);
        let fresh: Vec<&String> = declared
            .iter()
            .filter(|name| !table.contains(name.as_str()))
            .collect();
        assert!(
            fresh.is_empty(),
            "[{section}] grew dependencies the table does not name: {:?}\n\
             Everything else in the workspace links what the base links, so a new \
             dependency here is a cost paid by every crate and a step away from the \
             base being contracts only. If it is right, add a row with the reason \
             it cannot live in the crate that needs it; if it is not, the code that \
             wants it belongs in that crate.",
            fresh
        );
    }
}

/// A row whose dependency is gone fails, so the table cannot drift above the manifest.
#[test]
fn a_dependency_the_table_names_is_still_declared() {
    let manifest = common::routes::read_repo(BASE_MANIFEST);
    let declared = declared(&manifest, "dependencies");
    let stale: Vec<&&str> = DEPENDENCIES
        .iter()
        .map(|(name, _)| name)
        .filter(|name| !declared.contains(**name))
        .collect();
    assert!(
        stale.is_empty(),
        "the table names dependencies the manifest no longer has: {:?}\n\
         Delete the row in the commit that removed the dependency.",
        stale
    );
}

/// Every row says why, so the table cannot become a list somebody appends to.
#[test]
fn every_dependency_row_says_what_it_is_for() {
    for (name, why) in DEPENDENCIES {
        assert!(
            !why.trim().is_empty(),
            "{name} is frozen without a reason; the column is the difference \
             between a decision and a row"
        );
    }
}

/// A module the table does not name fails, so the base cannot grow a mechanism
/// without a row that argues for it.
#[test]
fn a_new_module_is_not_in_the_table() {
    let declared = modules();
    let table: BTreeSet<&str> = MODULES.iter().map(|(name, _, _)| *name).collect();
    let fresh: Vec<&String> = declared
        .iter()
        .filter(|name| !table.contains(name.as_str()))
        .collect();
    assert!(
        fresh.is_empty(),
        "cog-core grew modules the table does not name: {:?}\n\
         The base crate is where what crosses between crates is written down. A \
         new module here is either a contract (say so, and put the shape under \
         `contract`), a name several crates spell (a `Vocabulary` row), or work \
         that belongs to the crate whose job it is -- and if it really is shared \
         work, the `Mechanism` row has to name the crates that read it and why \
         none of them can own it.",
        fresh
    );
}

/// A row whose module is gone fails, so the table cannot drift above the crate.
#[test]
fn a_module_the_table_names_is_still_declared() {
    let declared = modules();
    let stale: Vec<&&str> = MODULES
        .iter()
        .map(|(name, _, _)| name)
        .filter(|name| !declared.contains(**name))
        .collect();
    assert!(
        stale.is_empty(),
        "the table names modules `lib.rs` no longer declares: {:?}\n\
         Delete the row in the commit that removed the module.",
        stale
    );
}

/// Every row says why it is here, so the table cannot become a list of names.
#[test]
fn every_module_row_says_why_it_is_here() {
    for (name, kind, why) in MODULES {
        assert!(
            !why.trim().is_empty(),
            "{name} is listed as {kind:?} without a reason; the column is what \
             separates a decision from a row"
        );
    }
    // The mechanism count is the number this rule is trying to bring down. It is
    // printed rather than asserted against a constant: a ceiling written here
    // would be a number to raise, and the rows are the argument.
    let mechanisms = MODULES
        .iter()
        .filter(|(_, k, _)| *k == Kind::Mechanism)
        .count();
    println!(
        "{mechanisms} of {} modules in cog-core are mechanisms; the rest are \
         contracts and vocabulary",
        MODULES.len()
    );
}
