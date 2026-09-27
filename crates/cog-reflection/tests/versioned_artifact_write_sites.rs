//! Gate: every path that writes a versioned artifact is a declared path.
//!
//! The failure this exists for: an artifact had a live *read* side — the
//! recommendation path reads the active version on every call — while nothing
//! in the process ever wrote a version, so the whole channel was a half loop
//! (read side running, write side only reachable by a human endpoint). No
//! runtime check reports that shape: nothing crashes, no log complains that a
//! producer is missing, and the reader keeps falling back to its default. It is
//! visible only by asking, statically, who writes this thing.
//!
//! So: collect the source files that call the artifact write API, and require
//! the set to equal a declared set in which every entry says what class of
//! trigger it is. A new writer fails the gate until someone classifies it; a
//! deleted autonomous producer fails it because the class is then missing while
//! the driver is still enabled by default.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// How a versioned artifact gets written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteTrigger {
    /// A periodic loop the process runs on its own. An unattended deployment
    /// needs at least one of these, otherwise the channel is a half loop.
    Autonomous,
    /// Only reachable through the admin HTTP surface: a person has to ask.
    HumanOnly,
    /// The store's own implementation, invoked by one of the above. Not a
    /// trigger in itself.
    Internal,
}

struct WriteSite {
    file: &'static str,
    trigger: WriteTrigger,
    /// The symbol that makes this file the class it claims to be. Checked to
    /// still exist: a table entry outliving its code would keep the gate green
    /// while the thing it describes is gone.
    evidence: &'static str,
    reason: &'static str,
}

/// The calls that reach `PolicyStore::save_new_version`, the one thing that
/// actually appends a version. `ArtifactEvolution::approve` and `::evolve` are
/// the two doors in front of it, so calling either is writing.
///
/// Split in two because the three tokens are not equally specific. Only one API
/// in the workspace is called `save_new_version`, so matching it is the call.
/// `.approve(` and `.evolve(` are ordinary verbs: any other subsystem may name a
/// method the same way, and then this gate reads that subsystem's call as a
/// version write and asks its author to declare a trigger that does not exist.
/// The disambiguation is the type: reaching those two doors requires holding an
/// `ArtifactEvolution` (which holds the `PolicyStore`), so a real write path
/// always names one of them, while a same-named method on another type does not.
const WRITE_API_UNIQUE: [&str; 1] = ["save_new_version("];
const WRITE_API_VERBS: [&str; 2] = [".evolve(", ".approve("];
const ARTIFACT_TYPES: [&str; 2] = ["ArtifactEvolution", "PolicyStore"];

/// Does this production source (not tests) write a versioned artifact?
///
/// A pure function so both directions can be pinned by test: a lone `.approve(`
/// must not be read as a write, and a real door call must be.
fn calls_the_write_api(production: &str) -> bool {
    WRITE_API_UNIQUE.iter().any(|api| production.contains(api))
        || (WRITE_API_VERBS.iter().any(|api| production.contains(api))
            && ARTIFACT_TYPES.iter().any(|t| production.contains(t)))
}

const WRITE_SITES: &[WriteSite] = &[
    WriteSite {
        file: "crates/cog-reflection/src/policy_evolution.rs",
        trigger: WriteTrigger::Autonomous,
        evidence: "run_policy_evolution_loop",
        reason: "product-level evolution driver: replays recorded decisions on an interval and \
                 hot-swaps the policy version when a candidate parameter set wins",
    },
    WriteSite {
        file: "crates/cog-reflection/src/evolution_admin.rs",
        trigger: WriteTrigger::HumanOnly,
        evidence: "approve_change",
        reason: "admin endpoints; a person approves a staged candidate",
    },
    WriteSite {
        file: "crates/cog-reflection/src/policy_store.rs",
        trigger: WriteTrigger::Internal,
        evidence: "pub async fn save_new_version",
        reason: "the store itself plus approve/evolve, which are the two triggers' landing step",
    },
];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/cog-reflection")
        .to_path_buf()
}

/// Everything up to the first test-only item. Occurrences inside test modules
/// are fixtures, not production write paths, so the gate stops reading there.
fn production_source(text: &str) -> &str {
    let cut = text
        .match_indices("#[cfg(test)]")
        .chain(text.match_indices("mod tests"))
        .map(|(i, _)| i)
        .min()
        .unwrap_or(text.len());
    &text[..cut]
}

fn rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|n| n == "target") {
                continue;
            }
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn files_calling_the_write_api() -> BTreeSet<String> {
    let root = workspace_root();
    let mut found = BTreeSet::new();
    for crate_dir in std::fs::read_dir(root.join("crates"))
        .expect("workspace has a crates directory")
        .flatten()
    {
        let src = crate_dir.path().join("src");
        if !src.is_dir() {
            continue;
        }
        let mut sources = Vec::new();
        rust_sources(&src, &mut sources);
        for path in sources {
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            if calls_the_write_api(production_source(&text)) {
                let relative = path
                    .strip_prefix(&root)
                    .expect("scanned path is under the workspace root")
                    .to_string_lossy()
                    .replace('\\', "/");
                found.insert(relative);
            }
        }
    }
    found
}

#[test]
fn every_versioned_artifact_write_site_is_classified() {
    let found = files_calling_the_write_api();
    let declared: BTreeSet<String> = WRITE_SITES.iter().map(|s| s.file.to_string()).collect();

    let undeclared: Vec<&String> = found.difference(&declared).collect();
    let gone: Vec<(&str, &str)> = WRITE_SITES
        .iter()
        .filter(|s| !found.contains(s.file))
        .map(|s| (s.file, s.reason))
        .collect();

    assert!(
        undeclared.is_empty() && gone.is_empty(),
        "a versioned-artifact write path is not classified. Undeclared: {undeclared:?}; \
         declared but gone (with what the entry claimed it was for): {gone:?}. \
         Add a WriteSite entry saying what triggers this write — or delete the write path — \
         rather than letting it land unremarked.",
    );

    // Each classification has to still be backed by its evidence symbol.
    let root = workspace_root();
    for site in WRITE_SITES {
        let text = std::fs::read_to_string(root.join(site.file))
            .unwrap_or_else(|e| panic!("{}: {e}", site.file));
        assert!(
            production_source(&text).contains(site.evidence),
            "{} is classified {:?} because of `{}`, which is no longer there",
            site.file,
            site.trigger,
            site.evidence,
        );
    }
}

#[test]
fn a_same_named_method_on_another_type_is_not_a_write() {
    // The false positive this narrowing exists for: a document-approval handler
    // calls `docs.approve(id, who)`. It writes a staged plan, not a version, and
    // it names no artifact type — so it must not be collected.
    assert!(!calls_the_write_api(
        "fn approve_document(&self) { self.docs.approve(&id, who) }"
    ));
    // The other direction, or the narrowing above would be a blindfold: a real
    // door call names the type it goes through.
    assert!(calls_the_write_api(
        "fn stage(&self) { self.evolution.approve(candidate, &store) } // ArtifactEvolution"
    ));
    // And the unique token stands on its own: a file that only calls the store
    // may hold its type through an alias.
    assert!(calls_the_write_api(
        "fn save(&self) { self.store.save_new_version(next) }"
    ));
}

#[test]
fn the_autonomous_classification_agrees_with_the_driver_default() {
    let enabled = cog_reflection::policy_evolution::PolicyEvolutionConfig::default().enabled;
    let autonomous: Vec<&str> = WRITE_SITES
        .iter()
        .filter(|s| s.trigger == WriteTrigger::Autonomous)
        .map(|s| s.file)
        .collect();

    if enabled {
        assert!(
            autonomous.contains(&"crates/cog-reflection/src/policy_evolution.rs"),
            "the artifact-evolution driver runs by default, so it writes in every deployment; \
             the file that runs it has to be declared Autonomous. Deleting the loop while \
             leaving the driver enabled is the half loop this gate is for. Declared: {autonomous:?}"
        );
    } else {
        assert!(
            autonomous.is_empty(),
            "the driver is off by default, so no unattended deployment writes a version; \
             calling a writer Autonomous would claim coverage the default configuration does \
             not deliver. Declared: {autonomous:?}"
        );
    }
}
