//! Gate: `cog-core`'s root entries are the ones agreed to be contract.
//!
//! Every crate in the workspace depends on `cog-core`, so whatever sits at its
//! root is available to all of them. That makes the root the right home for a
//! definition two publishers have to spell the same way -- a metric family's
//! labels, what "how big is this directory" means, the error type every
//! signature carries -- and equally the right place for a mechanism to hide: a
//! slot file held with `flock`, a process-global registry of spawned loops, a
//! walker that reclaims a cache volume. Those compile, they get reused, and
//! nothing in the diff reads as "a process-local implementation now lives in
//! the shared crate". The cost arrives later and sideways: a change to that
//! implementation rebuilds every crate at once, and the reason `cog-core`
//! exists stops being readable from its contents.
//!
//! The rule is not "core may not be reused" -- it is that a root entry is
//! contract or mechanism, and only contract stays. This gate does not decide
//! that question; it makes the answer a reviewed line rather than an accident.
//! A file or directory at the root that the table does not name fails, and a
//! table row whose entry is gone fails too, so the table cannot outlive the
//! tree it describes.
//!
//! Two things this gate does not see, said here so its green is read for what
//! it is. It matches on entry boundaries, so a contract and an implementation
//! sharing one file pass. And no token separates the two classes in text:
//! contract modules in this crate read `/proc` and walk directories on purpose
//! -- one definition of "how big is this tree" has to do the reading itself --
//! while a mechanism can act through plain `std::fs` without ever naming
//! anything unusual. What stands in for that missing token is the `moves_to`
//! column: a row that says the crate must not keep this module also says which
//! crate takes it, and that crate has to exist. The debt is then a live claim
//! with a reader, not a sentence that only ever grows.

use std::collections::BTreeMap;
use std::path::PathBuf;

struct Entry {
    /// The name at the root of `crates/cog-core/src`, without the `.rs` suffix
    /// for a file.
    name: &'static str,
    /// Why this belongs in the shared crate -- or, when it does not, what it
    /// does that a contract would not.
    why: &'static str,
    /// `None` for a contract, which stays. The workspace crate that is owed
    /// the mechanism otherwise.
    moves_to: Option<&'static str>,
}

const ROOT_ENTRIES: &[Entry] = &[
    Entry {
        name: "admission.rs",
        why: "the admission controller: it holds a slot file with flock and reads \
              /proc/pressure, and it owns the control law that moves the bound. \
              State and actions, not a definition.",
        moves_to: Some("cog-reflection"),
    },
    Entry {
        name: "audit.rs",
        why: "the shape of a tamper-evident chain and the verifier that walks it, \
              which the side that appends and the side that checks both depend on \
              producing the same answer.",
        moves_to: None,
    },
    Entry {
        name: "build_cache.rs",
        why: "the metric family a build cache's size is published under, and what \
              its labels mean. Two processes publish this family, so it is stated \
              once.",
        moves_to: None,
    },
    Entry {
        name: "build_cache_readings.rs",
        why: "walks the cache volume through statvfs and assembles the layer \
              totals. A reading of the host, with the syscall in it.",
        moves_to: Some("cog-reflection"),
    },
    Entry {
        name: "build_cache_reclaim.rs",
        why: "performs the deletions the contract's plan describes. The plan is a \
              definition; this is the act.",
        moves_to: Some("cog-reflection"),
    },
    Entry {
        name: "claim_footprint.rs",
        why: "the one definition of what a claim-backed directory holds and the \
              series it is published under. Reads mountinfo so two readers cannot \
              report different sizes for one volume.",
        moves_to: None,
    },
    Entry {
        name: "config.rs",
        why: "the configuration document's types, read by every process.",
        moves_to: None,
    },
    Entry {
        name: "config_sections.rs",
        why: "which sections of the configuration reach a running process and when \
              -- the list the watcher and the appliers have to share.",
        moves_to: None,
    },
    Entry {
        name: "error.rs",
        why: "the error vocabulary every crate's signatures carry.",
        moves_to: None,
    },
    Entry {
        name: "fs_size.rs",
        why: "the one definition of how many bytes a tree holds and how those bytes \
              fall into layers. A second walker would report a different size for \
              one tree with nothing in either reading to explain the difference.",
        moves_to: None,
    },
    Entry {
        name: "lib.rs",
        why: "the crate root: module declarations and the re-export face the rest of \
              the workspace names.",
        moves_to: None,
    },
    Entry {
        name: "loop_health.rs",
        why: "keeps the process-global registry of the background loops this \
              process started, their cadence and restart settings, and decides \
              whether each is still alive.",
        moves_to: Some("cog-observability"),
    },
    Entry {
        name: "observability_text.rs",
        why: "Prometheus text rendering for the readings a process publishes \
              itself. More than one process serves /metrics, so the rendering is \
              one function.",
        moves_to: None,
    },
    Entry {
        name: "secrets.rs",
        why: "the SecretProvider trait and the stateless providers behind it, plus \
              the redaction both the logger and the responder apply.",
        moves_to: None,
    },
    Entry {
        name: "token_estimate.rs",
        why: "one token estimate used as a bound on both sides, so a window is \
              never measured two ways.",
        moves_to: None,
    },
    Entry {
        name: "contract",
        why: "the contract layer itself: interfaces, names and types two crates \
              must agree on.",
        moves_to: None,
    },
    Entry {
        name: "types",
        why: "the shared types. A value crossing a crate boundary has one type.",
        moves_to: None,
    },
];

/// The repository root, derived from this crate's own manifest so the test does
/// not depend on the working directory it is run from.
fn workspace_root() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::canonicalize(&root).unwrap_or(root)
}

fn core_source_root() -> PathBuf {
    workspace_root().join("crates/cog-core/src")
}

/// The names at the root of `cog-core/src`: `.rs` files without their suffix,
/// and directories. Everything else there is a file this gate has no opinion
/// about.
fn entries_at_root() -> BTreeMap<String, bool> {
    let dir = core_source_root();
    let mut found = BTreeMap::new();
    let listing =
        std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()));
    for entry in listing.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        // `file_type` does not follow symlinks: a link out of the tree is not a
        // module this crate carries.
        match entry.file_type() {
            Ok(t) if t.is_dir() => {
                found.insert(name, true);
            }
            Ok(t) if t.is_file() && name.ends_with(".rs") => {
                found.insert(name, false);
            }
            _ => {}
        }
    }
    found
}

fn registry() -> BTreeMap<&'static str, &'static Entry> {
    ROOT_ENTRIES.iter().map(|e| (e.name, e)).collect()
}

#[test]
fn every_root_entry_is_registered_and_every_row_still_exists() {
    let found = entries_at_root();

    // The crate root is what makes this the directory the gate is about. A walk
    // that lands somewhere else -- a moved crate, a path built from the wrong
    // base -- finds no `lib.rs`, and the set comparison below would then be
    // comparing the table against a listing of something that is not this
    // crate's source root.
    assert!(
        found.get("lib.rs").is_some_and(|is_dir| !is_dir),
        "{} has no lib.rs, so this is not the crate root the table describes",
        core_source_root().display()
    );

    let table = registry();
    let unregistered: Vec<&String> = found
        .keys()
        .filter(|name| !table.contains_key(name.as_str()))
        .collect();
    assert!(
        unregistered.is_empty(),
        "root entries of crates/cog-core/src with no row in the table: {unregistered:?} -- \
         every crate depends on this one, so an entry here is reachable from all of them. \
         Add a row saying whether it is a definition two publishers must share, or a \
         mechanism that belongs in the crate that owns it"
    );

    let missing: Vec<&str> = table
        .keys()
        .filter(|name| !found.contains_key(**name))
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "rows in the table whose entry is gone from crates/cog-core/src: {missing:?} -- a \
         row that outlives its module keeps reading as an allowance for something the tree \
         no longer has, and the next module to take that name inherits the exemption"
    );
}

#[test]
fn a_row_that_owes_a_move_names_a_crate_that_exists() {
    let crates_dir = workspace_root().join("crates");

    for entry in ROOT_ENTRIES {
        let Some(dest) = entry.moves_to else {
            continue;
        };
        let manifest = crates_dir.join(dest).join("Cargo.toml");
        assert!(
            manifest.is_file(),
            "{} is registered as owed to {dest}, and {dest} is not a crate in this \
             workspace ({} is missing). An owed move whose destination does not exist is \
             not a plan -- it is a label that reads as decided while nothing can receive \
             the module. Name the crate that will, or drop the destination and say why \
             the crate keeps this module",
            entry.name,
            manifest.display()
        );
    }
}

/// Every row carries a reason.
///
/// This is the whole of what can be checked about the reason: whether it argues
/// the right thing is a reading a person has to make. There is no word that
/// separates the two classes -- "contract" contains "act", a definition can be
/// described with "keeps", and a mechanism can be described without either --
/// so a keyword test here would pass on every row and say nothing.
#[test]
fn the_table_states_a_reason_for_every_row() {
    for entry in ROOT_ENTRIES {
        assert!(
            !entry.why.trim().is_empty(),
            "{} is registered with no reason. A whitelist entry without a reason is \
             indistinguishable from an exemption nobody had to argue for",
            entry.name
        );
    }
}

/// No two rows name the same entry.
#[test]
fn the_table_names_each_root_entry_once() {
    let mut seen = std::collections::BTreeSet::new();
    let mut duplicates = Vec::new();
    for entry in ROOT_ENTRIES {
        if !seen.insert(entry.name) {
            duplicates.push(entry.name);
        }
    }
    assert!(
        duplicates.is_empty(),
        "root entries registered twice: {duplicates:?} -- the fold that reads this table \
         keeps one row per name, so a second row is either dead text or the one that was \
         meant, and nothing says which"
    );
}
