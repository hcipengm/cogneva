//! Gate: the contract layer's boundary, read off the crate instead of a table.
//!
//! `cog-core` is where the *shape* of what crosses between crates lives: the
//! traits and the types. Everything else -- the loop that runs, the file that is
//! written, the row that is read -- belongs to the crate that owns that job.
//! "Contracts, not business" cannot be read off the source directly, because
//! what counts as business is a judgement. Two of its consequences can, and they
//! are what this file asserts.
//!
//! First, the base is supposed to get smaller: every dependency it declares has
//! to be named in its own sources. A dependency nobody names is weight every
//! crate that links the base pays for, and it is the shape a "just in case"
//! addition takes. This rule found four (`toml` and `zstd` on the shipped side,
//! `tokio-test` and `filetime` on the dev side), declared and never written.
//!
//! Second, an *act* in the base has to be earning its place. A module that opens
//! files, spawns, opens a socket or keeps shared mutable state does work, and
//! work belongs to the crate that performs it -- with one structural exception:
//! a crate other than the assembly may depend on the base and nothing else, so a
//! mechanism that two crates need has no other home that does not become a
//! sideways edge. So the count is the argument. A mechanism reached by two or
//! more crates outside the base is shared by construction; one reached by fewer
//! belongs in the crate that reaches it, and the failure names that crate.
//!
//! Both were tables before this, one row per dependency and one per module, each
//! with a reason beside it. The reasons were worth writing -- that is where the
//! rule got sharpened -- but a copy of the crate kept by hand drifts from the
//! crate, and the drift is silent in the one direction that matters: the table
//! says what a human believed, the source says what is there. These rules read
//! the source.
//!
//! The scanner is the part that has to be trusted, so the things that make a
//! naive search lie are removed before anything is counted: comments and string
//! literals are blanked (`lex::blank`), and the `#[cfg(test)]` half of a file is
//! dropped (`production_text`) -- a marker in a doc comment or in a test module
//! is not a call, and a module that only *mentions* `std::fs::` in prose is not
//! doing anything. `the_marker_scan_separates_a_call_from_a_mention` pins that
//! distinction on both polarities.
//!
//! Where this gate lives, and why not in the crate it measures: its subject is
//! the workspace's layering, and the base is the crate that is supposed to stop
//! changing. A test directory inside it would make every future boundary
//! question a commit to that crate, and would add a test target to the one crate
//! whose cost is paid by all twenty-six others.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::common;

use common::lex::{blank, Dialect};
use common::rust_production::{
    all_crate_files, production_sources, production_text, relative_name,
};

/// Where the base crate's sources sit relative to the workspace root.
const BASE_SRC: &str = "crates/cog-core/src";
/// The base crate's manifest, workspace-relative.
const BASE_MANIFEST: &str = "crates/cog-core/Cargo.toml";
/// The directory the base crate lives in, workspace-relative.
const BASE_CRATE: &str = "crates/cog-core";

/// The storage drivers that must not appear in the base, in either section.
///
/// This is a rule rather than a derivation: nothing about the tree says a driver
/// is different from any other dependency. It is here because the consequence is
/// structural -- a driver in the base is linked into every consumer of the
/// contracts, so a crate that only wanted a trait gets the driver's whole world
/// (a connection pool, a runtime, a TLS stack) whether it asked for it or not.
const DRIVERS: &[&str] = &["sqlx", "redis"];

/// How many crates outside the base have to reach a mechanism before it earns
/// its way into the base.
///
/// Two, and the number comes from the layering rather than from taste: a crate
/// other than the assembly can reach the base and nothing else, so sharing an
/// act between two components means putting it here. One consumer has a home --
/// its own crate -- and moving a one-crate mechanism out is exactly the
/// direction the base is supposed to shrink in.
const SHARED: usize = 2;

/// A census that read fewer modules than this has gone wrong, and the rule below
/// would pass over the empty set.
const MIN_MODULES: usize = 20;

/// What makes a module an act.
///
/// Each name is something a trait or a type cannot do: read or write a file,
/// start a process, open a socket, spawn a task, keep state that outlives the
/// call. The list is short on purpose -- every entry on it is a decision that a
/// module containing it is doing work, and an entry that stops being decisive is
/// a false failure somebody has to argue with.
const ACTS: &[&str] = &[
    "std::fs::",
    "std::process::",
    "std::net::",
    "std::thread::spawn",
    "tokio::spawn",
    "Command::new",
    "OnceLock",
    "std::sync::Mutex",
    "std::sync::RwLock",
    "std::sync::atomic",
];

/// The top-level modules the base's `lib.rs` declares, and the sources that make
/// each one up.
///
/// A module is a declaration ending in `;`; the `pub mod agent { .. }` form
/// re-exports a contract that is already above it and is not a module of its
/// own. A module's sources are its `X.rs` or `X/mod.rs` plus everything under
/// `X/`, read from the tree: the rule below is about what the compiler actually
/// builds.
fn modules(root: &Path) -> BTreeMap<String, Vec<PathBuf>> {
    let lib = common::routes::read_repo(&format!("{BASE_SRC}/lib.rs"));
    let src = root.join(BASE_SRC);
    let mut out = BTreeMap::new();
    for line in lib.lines() {
        let line = line.trim();
        let Some(rest) = line
            .strip_prefix("pub mod ")
            .and_then(|rest| rest.strip_suffix(';'))
        else {
            continue;
        };
        let name = rest.trim();
        let mut files = Vec::new();
        for candidate in [
            src.join(format!("{name}.rs")),
            src.join(name).join("mod.rs"),
        ] {
            if candidate.is_file() {
                files.push(candidate);
            }
        }
        let nested = src.join(name);
        if nested.is_dir() {
            common::routes::walk(&nested, &["rs"], &mut files);
        }
        files.sort();
        files.dedup();
        out.insert(name.to_owned(), files);
    }
    out
}

/// One module's code, with the two things that make a naive search lie removed.
///
/// Comments and string literals are blanked rather than deleted, because the
/// question here is a presence question and offsets do not matter; the
/// `#[cfg(test)]` half is dropped, because a test module's call is not the
/// crate's behaviour.
fn code_of(files: &[PathBuf]) -> String {
    files
        .iter()
        .map(|path| {
            let raw =
                std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            blank(&production_text(&raw), Dialect::Rust)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// No identifier character immediately before `at`.
fn starts_a_word(text: &str, at: usize) -> bool {
    text[..at]
        .chars()
        .next_back()
        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
}

/// No identifier character immediately after `at`.
fn ends_a_word(text: &str, at: usize) -> bool {
    text[at..]
        .chars()
        .next()
        .is_none_or(|c| !(c.is_alphanumeric() || c == '_'))
}

/// Whether `text` names `path` as a whole path.
///
/// `crate::types` must not be found inside `crate::types_extra` or in
/// `my_crate::types`: every count in this file is a count of crates, and a
/// prefix or suffix match would let one module's consumers pay for another's.
fn names_path(text: &str, path: &str) -> bool {
    let mut from = 0;
    while let Some(at) = text[from..].find(path) {
        let start = from + at;
        if starts_a_word(text, start) && ends_a_word(text, start + path.len()) {
            return true;
        }
        from = start + 1;
    }
    false
}

/// Whether `text` writes `<crate>::` -- a dependency earning its place.
///
/// Only the boundary in front is checked: the needle ends at a `::`, so what
/// follows it is the item the dependency is being used for, and that is always
/// an identifier.
fn uses_crate(text: &str, dependency: &str) -> bool {
    let needle = format!("{}::", dependency.replace('-', "_"));
    let mut from = 0;
    while let Some(at) = text[from..].find(&needle) {
        let start = from + at;
        if starts_a_word(text, start) {
            return true;
        }
        from = start + 1;
    }
    false
}

/// The public names a module declares, so a crate that uses
/// `cog_core::<name>` can be attributed to the module the name lives in.
///
/// Needed because half the base is reached through `lib.rs`'s re-exports
/// (`pub use secrets::redact_secrets`) rather than through the module path, and
/// a gate that only understood `cog_core::<module>` would read those consumers
/// as zero and fail a module that is in fact used everywhere.
fn declared_names(code: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for line in code.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pub ") else {
            continue;
        };
        if let Some(body) = rest.strip_prefix("use ") {
            for part in body.trim_end_matches(';').split([',', '{', '}']) {
                let part = part.trim();
                let part = part.rsplit("::").next().unwrap_or_default();
                let name = part.split(" as ").last().unwrap_or_default().trim();
                if is_ident(name) {
                    out.insert(name.to_owned());
                }
            }
            continue;
        }
        for keyword in [
            "fn ", "struct ", "enum ", "trait ", "type ", "const ", "static ", "mod ",
        ] {
            if let Some(rest) = rest.strip_prefix(keyword) {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if is_ident(&name) {
                    out.insert(name);
                }
                break;
            }
        }
    }
    out
}

fn is_ident(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_alphabetic() || c == '_')
        && chars.all(|c| c.is_alphanumeric() || c == '_')
}

/// The workspace's crate directories, and the production code of each.
///
/// Production only: a `cog_core::` call in a test module does not make the base
/// shared -- the crate that ships without the test does not reach it.
fn consumer_code(root: &Path) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for path in production_sources(root) {
        let name = relative_name(root, &path);
        let parts: Vec<&str> = name.split('/').collect();
        let (Some("crates"), Some(dir)) = (parts.first().copied(), parts.get(1).copied()) else {
            continue;
        };
        if dir == "cog-core" {
            continue;
        }
        let raw =
            std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let code = blank(&production_text(&raw), Dialect::Rust);
        out.entry(dir.to_owned()).or_default().push_str(&code);
    }
    out
}

/// The modules a piece of code reaches through `cog_core`, by module name or by
/// the name of something one of them declares.
///
/// Three spellings have to be read, and they are all live: `cog_core::error`
/// (the module), `cog_core::SFError::Config(..)` (an item), and
/// `use cog_core::{SFError, SFResult}` (a brace list, which is how half the
/// workspace writes it). Missing the third would read a module that every crate
/// uses as one nothing reaches, and the failure would point at the module
/// instead of the scan.
fn referenced_modules(
    code: &str,
    modules: &BTreeSet<String>,
    owners: &BTreeMap<String, BTreeSet<String>>,
) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let attribute = |name: &str, out: &mut BTreeSet<String>| {
        if modules.contains(name) {
            out.insert(name.to_owned());
        }
        if let Some(declared_by) = owners.get(name) {
            out.extend(declared_by.iter().cloned());
        }
    };
    let mut from = 0;
    while let Some(at) = code[from..].find("cog_core::") {
        let start = from + at + "cog_core::".len();
        if code[start..].starts_with('{') {
            // A brace list: `{SFError, SFResult}`, `{error::SFError}`, or a
            // nested `{contract::{types, ids}}`. Each entry is attributed to the
            // module that declares its last segment; `self` and `*` name no
            // item, and a glob is read as no evidence rather than as everything
            // -- undercounting a consumer makes this rule quieter, overcounting
            // would make it lie about a module being shared.
            let body_end =
                common::routes::matching_brace(code, start, Dialect::Rust).unwrap_or(code.len());
            for entry in code[start + 1..body_end].split(',') {
                let last = entry.trim().rsplit("::").next().unwrap_or_default().trim();
                if is_ident(last) {
                    attribute(last, &mut out);
                }
            }
            from = body_end.max(start);
            continue;
        }
        let name: String = code[start..]
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if !name.is_empty() {
            attribute(&name, &mut out);
        }
        from = start.max(from + at + 1);
    }
    out
}

/// The section names in a manifest whose entries are dependencies.
///
/// Deliberately a line reader over the manifest rather than a TOML parser: this
/// gate has no business adding a parser to the workspace to read two sections,
/// and the question -- which names are declared here -- does not need one.
fn declared(manifest: &str, section: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut inside = false;
    for line in manifest.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
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

/// The base crate's `[dependencies]` and `[dev-dependencies]`, and the code each
/// one is checked against.
///
/// The shipped side is checked against the production half only: that is the
/// code the dependency is linked for. The dev side is checked against everything
/// under the crate, because that is where a dev dependency is used -- a name in
/// a test module is the dependency earning its place.
fn code_for(root: &Path, shipped: bool) -> String {
    if shipped {
        let mut files = Vec::new();
        common::routes::walk(&root.join(BASE_SRC), &["rs"], &mut files);
        files.sort();
        code_of(&files)
    } else {
        let mut code = String::new();
        for path in all_crate_files(root) {
            if !relative_name(root, &path).starts_with(&format!("{BASE_CRATE}/")) {
                continue;
            }
            let raw = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            code.push_str(&blank(&raw, Dialect::Rust));
            code.push('\n');
        }
        code
    }
}

/// The census read the crate, so the rules below are not passing over nothing.
#[test]
fn the_census_reads_the_crate() {
    let root = common::root::repo_root();
    let modules = modules(&root);
    assert!(
        modules.len() >= MIN_MODULES,
        "read {} modules out of `{BASE_SRC}/lib.rs` (floor {MIN_MODULES}): the \
         declaration scan is lying, and every rule below would be measuring a \
         module set that does not exist",
        modules.len()
    );
    for (name, files) in &modules {
        assert!(
            !files.is_empty(),
            "`lib.rs` declares `{name}` but no source was found for it: a module \
             whose code could not be read is classified as doing nothing, which \
             is the reading a broken scan and a clean module share"
        );
    }
    assert!(
        !consumer_code(&root).is_empty(),
        "no crate outside the base was read: every mechanism would the count \
         zero consumers and the rule would pass by measuring nothing"
    );
}

#[test]
fn core_does_not_depend_on_storage_drivers() {
    let manifest = common::routes::read_repo(BASE_MANIFEST);
    for section in ["dependencies", "dev-dependencies"] {
        let declared = declared(&manifest, section);
        for banned in DRIVERS {
            assert!(
                !declared.contains(*banned),
                "cog-core must not depend on `{banned}` in [{section}]; storage \
                 drivers live in cog-storage, and a driver in the base is linked \
                 into every crate that only wanted a trait"
            );
        }
    }
}

#[test]
fn core_sources_do_not_name_storage_drivers() {
    let root = common::root::repo_root();
    let mut files = Vec::new();
    common::routes::walk(&root.join(BASE_SRC), &["rs"], &mut files);
    assert!(
        !files.is_empty(),
        "no sources found under {BASE_SRC} -- this gate is measuring nothing"
    );
    for path in files {
        let src = std::fs::read_to_string(&path).expect("source file is readable");
        for banned in DRIVERS {
            let needle = format!("{banned}::");
            assert!(
                !src.contains(&needle),
                "cog-core sources must not reference `{needle}` ({})",
                path.display()
            );
        }
    }
}

/// Every declared dependency is a name the crate writes.
///
/// This is the rule that keeps the base shrinking: a dependency nobody names is
/// weight every consumer links for nothing, and adding one is a one-line diff
/// nobody reviews. The failure asks for one of two things -- name it in the code
/// that needs it, or drop it -- and both are visible in the same commit.
#[test]
fn every_declared_dependency_is_named_in_the_crate() {
    let root = common::root::repo_root();
    let manifest = common::routes::read_repo(BASE_MANIFEST);
    let mut unnamed = Vec::new();
    for (section, shipped) in [("dependencies", true), ("dev-dependencies", false)] {
        let code = code_for(&root, shipped);
        for dependency in declared(&manifest, section) {
            if !uses_crate(&code, &dependency) {
                unnamed.push(format!("{dependency} (in [{section}])"));
            }
        }
    }
    assert!(
        unnamed.is_empty(),
        "the base declares dependencies its own sources never name: {}\n\
         Everything in the workspace links what the base links, so a dependency \
         here is a cost paid twenty-six times. A declared-and-unused dependency \
         is the shape a 'just in case' addition takes: it compiles, it costs, and \
         nothing ever breaks. Either the code that wants it is missing -- which is \
         a feature nobody shipped -- or the line is, and it belongs in this \
         commit's diff.",
        unnamed.join(", ")
    );
}

/// Which acting modules are reached by fewer than [`SHARED`] crates outside the
/// base, and how many modules act at all.
///
/// Split out of the test that runs it on the tree so the judgement can be fed
/// data no tree currently has: the shape of a module nobody outside the base
/// calls is one the tree is supposed not to contain, so the only way to see the
/// rule fire is to hand it that shape.
fn lonely_mechanisms(
    code: &BTreeMap<String, String>,
    reached_by: &BTreeMap<String, BTreeSet<String>>,
) -> (Vec<String>, usize) {
    // What each module reaches inside the base: a `crate::<other>` path.
    let references: BTreeMap<String, BTreeSet<String>> = code
        .iter()
        .map(|(name, text)| {
            let reached = code
                .keys()
                .filter(|other| *other != name && names_path(text, &format!("crate::{other}")))
                .cloned()
                .collect();
            (name.clone(), reached)
        })
        .collect();

    // A module acts if it names one of the markers, or if it reaches a module
    // that does: the act is inherited by whatever calls it. `build_cache_reclaim`
    // is the live example -- it opens files, and the only thing that calls it is
    // this crate's own readings, which three crates call.
    let mut acts: BTreeSet<String> = code
        .keys()
        .filter(|name| ACTS.iter().any(|marker| code[*name].contains(marker)))
        .cloned()
        .collect();
    loop {
        let grew: Vec<String> = code
            .keys()
            .filter(|name| !acts.contains(*name))
            .filter(|name| references[*name].iter().any(|other| acts.contains(other)))
            .cloned()
            .collect();
        if grew.is_empty() {
            break;
        }
        acts.extend(grew);
    }

    // A module's reach is its own consumers plus the consumers of everything
    // that reaches it: a mechanism only this crate's other modules call is still
    // reached by whoever calls them. The edges point from a caller to what it
    // calls, so the reach flows the other way -- a caller's consumers are its
    // dependencies' consumers too.
    let mut shared: BTreeMap<String, BTreeSet<String>> = code
        .keys()
        .map(|name| {
            let own = reached_by.get(name).cloned().unwrap_or_default();
            (name.clone(), own)
        })
        .collect();
    loop {
        let mut grew = false;
        for (module, dependencies) in &references {
            let own = shared[module].clone();
            for dependency in dependencies {
                let entry = shared
                    .get_mut(dependency)
                    .expect("every module is measured");
                let before = entry.len();
                entry.extend(own.iter().cloned());
                grew |= entry.len() != before;
            }
        }
        if !grew {
            break;
        }
    }

    let lonely: Vec<String> = acts
        .iter()
        .filter(|module| shared[*module].len() < SHARED)
        .map(|module| {
            let who: Vec<&str> = shared[module].iter().map(String::as_str).collect();
            if who.is_empty() {
                format!("{module}: no crate outside the base reaches it")
            } else {
                format!("{module}: reached by {}", who.join(", "))
            }
        })
        .collect();
    (lonely, acts.len())
}

/// A mechanism in the base has to be reached by more than one crate.
#[test]
fn a_module_that_touches_the_machine_is_reached_by_more_than_one_crate() {
    let root = common::root::repo_root();
    let modules = modules(&root);
    let code: BTreeMap<String, String> = modules
        .iter()
        .map(|(name, files)| (name.clone(), code_of(files)))
        .collect();
    // Item name -> the modules that declare it, so a crate that names a
    // re-exported item (`cog_core::SFError`) is attributed to the module the
    // item lives in rather than to nobody.
    let mut owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (module, text) in &code {
        for name in declared_names(text) {
            owners.entry(name).or_default().insert(module.clone());
        }
    }
    let module_names: BTreeSet<String> = modules.keys().cloned().collect();

    // Who reaches each module, by crate directory outside the base.
    let mut reached_by: BTreeMap<String, BTreeSet<String>> = modules
        .keys()
        .map(|name| (name.clone(), BTreeSet::new()))
        .collect();
    for (crate_dir, text) in consumer_code(&root) {
        for module in referenced_modules(&text, &module_names, &owners) {
            if let Some(set) = reached_by.get_mut(&module) {
                set.insert(crate_dir.clone());
            }
        }
    }

    let (lonely, acting) = lonely_mechanisms(&code, &reached_by);
    assert!(
        lonely.is_empty(),
        "modules in the base that do work nothing outside it shares:\n  {}\n\
         A module that opens files, starts processes, opens sockets or keeps \
         state is an act, and an act belongs to the crate that performs it. The \
         one thing that can force an act into the base is being shared: a crate \
         other than the assembly may depend on the base and nothing else, so a \
         mechanism two or more crates need has no other home that does not become \
         a sideways edge. These have fewer. Either move the code into the crate \
         that reaches it -- the base is supposed to get smaller, and this is the \
         direction it shrinks in -- or, if it really is shared, the consumer is \
         not reaching it yet and that is the thing to fix.",
        lonely.join("\n  ")
    );
    println!(
        "{acting} of {} modules in the base do work; the rest are shapes and names",
        modules.len()
    );
}

/// The judgement counts reach, not names.
///
/// The tree is supposed not to contain a mechanism reached by fewer than two
/// crates, so the rule above can only ever pass on it -- which is the same
/// reading as a rule that cannot fail. These four shapes pin both polarities:
/// the act nobody reaches, the act one crate reaches, the act two reach, and the
/// act reached only through another module (which is the shape the cache reclaim
/// has today).
#[test]
fn the_judgement_counts_reach_and_not_names() {
    let code: BTreeMap<String, String> = [
        ("actor", "pub fn f() { std::fs::read(\"/p\"); }"),
        ("caller", "pub fn g() { crate::actor::f(); }"),
        ("local", "pub fn h() { std::fs::write(\"/p\", \"\"); }"),
        ("shape", "pub struct S;"),
    ]
    .into_iter()
    .map(|(name, text)| (name.to_owned(), text.to_owned()))
    .collect();

    let reached = |pairs: &[(&str, &[&str])]| {
        code.keys()
            .map(|name| {
                let crates = pairs
                    .iter()
                    .find(|(module, _)| module == name)
                    .map(|(_, crates)| crates.iter().map(|c| (*c).to_owned()).collect())
                    .unwrap_or_default();
                (name.clone(), crates)
            })
            .collect::<BTreeMap<String, BTreeSet<String>>>()
    };

    // Two crates reach the caller, which is the only thing that reaches the
    // actor; nothing reaches `local`; `shape` does not act and is not judged.
    let (lonely, acting) = lonely_mechanisms(
        &code,
        &reached(&[("actor", &["cog-one"]), ("caller", &["cog-one", "cog-two"])]),
    );
    assert_eq!(
        acting, 3,
        "an act reached through another module was not counted as one"
    );
    assert_eq!(
        lonely,
        vec!["local: no crate outside the base reaches it".to_owned()],
        "the judgement flagged the wrong modules: {lonely:?}"
    );

    // The same tree with the caller reached by one crate only: the actor now
    // inherits the count of a single consumer, and both acts are flagged.
    let (lonely, _) = lonely_mechanisms(
        &code,
        &reached(&[("actor", &["cog-one"]), ("caller", &["cog-one"])]),
    );
    assert_eq!(
        lonely,
        vec![
            "actor: reached by cog-one".to_owned(),
            "caller: reached by cog-one".to_owned(),
            "local: no crate outside the base reaches it".to_owned(),
        ],
        "an act reached by one crate was not flagged: {lonely:?}"
    );
}

/// The marker scan tells a call from a mention.
///
/// The rule above is a presence question over a projection of the source, so the
/// projection is the thing that can be wrong: a scan that read comments would
/// call a documented `std::fs::read` an act, and a scan that read string
/// literals would call a logged one an act. Both polarities are pinned here,
/// because a scanner that answered "no" to everything would make the rule above
/// pass over every module in the crate.
#[test]
fn the_marker_scan_separates_a_call_from_a_mention() {
    let call = "fn read_it() -> std::io::Result<()> { std::fs::read(\"/p\")?; Ok(()) }";
    assert!(
        ACTS.iter()
            .any(|marker| blank(call, Dialect::Rust).contains(marker)),
        "a real call was not read as an act: the scan is answering no to \
         everything, and the boundary rule above is vacuous"
    );
    let comment = "/// Reads `std::fs::read` for the caller.\nfn read_it() {}";
    assert!(
        !ACTS
            .iter()
            .any(|marker| blank(comment, Dialect::Rust).contains(marker)),
        "a path inside a doc comment was read as an act"
    );
    let literal = "fn name() -> &'static str { \"std::fs::read\" }";
    assert!(
        !ACTS
            .iter()
            .any(|marker| blank(literal, Dialect::Rust).contains(marker)),
        "a path inside a string literal was read as an act"
    );
    let fixture =
        "fn f() {}\n#[cfg(test)]\nmod tests {\n    fn t() { std::fs::read(\"/p\").ok(); }\n}\n";
    assert!(
        !ACTS
            .iter()
            .any(|marker| code_of_text(fixture).contains(marker)),
        "a call in a test module was read as the crate's behaviour"
    );
}

/// [`code_of`]'s two projections, over text instead of a file.
fn code_of_text(text: &str) -> String {
    blank(&production_text(text), Dialect::Rust)
}
