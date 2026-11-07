//! principle: feedback_no_glue_layers
//! Gate: the workspace carries components, not a layer between them.
//!
//! 用户明确反对 bridge/胶水层代码，要求架构干净整洁。
//!
//! `core_boundary_contract_test` and `crate_dependency_contract_test` already
//! forbid one component reaching into another, so a layer cannot enter this
//! workspace as an edge. The one shape left to it is a crate: a member whose
//! purpose is to be a second name for code that already exists. That is glue at
//! crate granularity -- every call site goes through it, it owns nothing, and a
//! change to the thing behind it is a change to it as well.
//!
//! Three sentences, each read off the tree:
//!
//! 1. no member directory carries a layer word as one of its name segments;
//! 2. every member declares items of its own in its production sources -- one
//!    of `fn`, `struct`, `enum`, `trait`, `type`, `const`, `static`, `impl`,
//!    `macro_rules!`, `union` -- and not only `use` / `pub use` lines;
//! 3. the census read every member the manifest named, and at least
//!    `MIN_MEMBERS` of them, so neither rule above passes over the empty set.
//!
//! What this does not judge: whether a given function forwards its arguments.
//! That is a judgement about code, and a decorator that forwards deliberately is
//! not this rule's target. Only the crate-shaped layer is, because a crate is
//! the shape a layer is actually added in.

use crate::common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use common::lex::{blank, Dialect};
use common::routes::{read, read_repo, repo_root};
use common::rust_production::{production_sources, production_text, relative_name};

/// A census that named fewer members than this has read the wrong thing, and
/// both rules below would then be judging an empty set.
const MIN_MEMBERS: usize = 20;

/// The words no member may be named after. A layer sits between two pieces and
/// owns neither, so a crate carrying one of these as a name segment is
/// announcing that shape.
const LAYER_WORDS: [&str; 6] = [
    "bridge",
    "glue",
    "shim",
    "facade",
    "middleman",
    "passthrough",
];

/// The keywords that make a crate own something, as opposed to re-exporting
/// names it did not define. Deliberately generous: over-counting makes this
/// rule quieter, and a gate that reads too little is the failure it cannot
/// afford.
const ITEM_KEYWORDS: [&str; 10] = [
    "fn ",
    "struct ",
    "enum ",
    "trait ",
    "type ",
    "const ",
    "static ",
    "impl ",
    "macro_rules!",
    "union ",
];

/// What the workspace looks like from the manifest and from the sources.
struct Census {
    /// The member directories the root manifest names.
    named: BTreeSet<String>,
    /// The members the source walk actually read a file for.
    reached: BTreeSet<String>,
    /// The members whose whole production surface is `use` / `pub use` lines.
    forwarded: Vec<String>,
}

/// The members the root manifest names, as `(directory, directory name)`.
///
/// A line reader rather than a TOML parser: this gate reads one array of quoted
/// paths, and the workspace carries no TOML dependency for its tests to borrow.
fn members_from_manifest(manifest: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    let mut inside = false;
    for line in manifest.lines() {
        let text = line.split('#').next().unwrap_or_default().trim();
        if !inside {
            inside = text.starts_with("members") && text.contains('[');
            continue;
        }
        if text.starts_with(']') {
            break;
        }
        if let Some(path) = text.split('"').nth(1) {
            let name = path.rsplit('/').next().unwrap_or(path);
            out.push((path.to_string(), name.to_string()));
        }
    }
    out
}

/// The layer word a member's directory name carries, if it carries one.
///
/// Segments, not substrings: `cog-gateway` and `abridged` stay green, while a
/// crate that does mean to be the layer has to spell the word out.
fn layer_word_in(name: &str) -> Option<&'static str> {
    let segments: Vec<&str> = name
        .split(['-', '_'])
        .filter(|segment| !segment.is_empty())
        .collect();
    LAYER_WORDS.into_iter().find(|&word| {
        segments
            .iter()
            .any(|segment| segment.eq_ignore_ascii_case(word))
    })
}

/// Whether a member's production surface declares anything of its own.
///
/// The `#[cfg(test)]` half is dropped first, because a fixture is not the crate
/// carrying production code; comments and string literals are blanked next,
/// because a `struct` written in a doc comment is not an item.
fn declares_its_own_code(text: &str) -> bool {
    let code = blank(&production_text(text), Dialect::Rust);
    ITEM_KEYWORDS.iter().any(|keyword| code.contains(keyword))
}

/// Read every member's production sources into one text per member.
fn census(root: &Path, members: &[(String, String)]) -> Census {
    let mut per_member: BTreeMap<String, String> = BTreeMap::new();
    for path in production_sources(root) {
        let name = relative_name(root, path.as_path());
        let found = members.iter().find(|(dir, _)| {
            name.strip_prefix(dir.as_str())
                .is_some_and(|rest| rest.starts_with('/'))
        });
        let Some((dir, _)) = found else {
            continue;
        };
        per_member
            .entry(dir.clone())
            .or_default()
            .push_str(read(path.as_path()).as_str());
    }
    let named: BTreeSet<String> = members.iter().map(|(dir, _)| dir.clone()).collect();
    let reached: BTreeSet<String> = per_member.keys().cloned().collect();
    let mut forwarded: Vec<String> = per_member
        .iter()
        .filter(|(_, text)| !declares_its_own_code(text))
        .map(|(dir, _)| dir.clone())
        .collect();
    forwarded.sort();
    Census {
        named,
        reached,
        forwarded,
    }
}

#[test]
fn no_member_is_named_after_a_layer() {
    let members = members_from_manifest(&read_repo("Cargo.toml"));
    let count = members.len();
    assert!(
        count >= MIN_MEMBERS,
        "the manifest named {count} members (floor {MIN_MEMBERS}): the reader did not reach the \
         list, and the rule below would be judging nothing"
    );
    let mut named: Vec<String> = Vec::new();
    for (dir, name) in &members {
        if let Some(word) = layer_word_in(name) {
            named.push(format!("{dir} ({word})"));
        }
    }
    assert!(
        named.is_empty(),
        "these members are named after a layer: {named:?}. A bridge or glue crate is a second \
         name for code that already exists: every call site goes through it, it owns nothing, \
         and the component on either side of it stops being the component a reader can follow."
    );
}

#[test]
fn a_layer_word_is_a_segment_and_not_a_substring() {
    for red in [
        "cog-bridge",
        "cog-glue",
        "cog-shim",
        "cog-facade",
        "cog-middleman",
        "cog-passthrough",
        "bridge_water",
        "glue-layer",
    ] {
        assert!(
            layer_word_in(red).is_some(),
            "{red} carries a layer word as a name segment and has to be reported"
        );
    }
    for green in ["cog-gateway", "abridged", "cog-core", "cog-bridging"] {
        assert!(
            layer_word_in(green).is_none(),
            "{green} carries a layer word only as part of a segment, which is not this rule"
        );
    }
}

#[test]
fn no_member_is_only_a_name_for_another_crates_code() {
    let root = repo_root();
    let members = members_from_manifest(&read_repo("Cargo.toml"));
    let found = census(&root, &members);
    let count = found.named.len();
    assert!(
        count >= MIN_MEMBERS,
        "the manifest named {count} members (floor {MIN_MEMBERS}): the census did not reach the \
         list"
    );
    let unreached: Vec<&String> = found.named.difference(&found.reached).collect();
    assert!(
        unreached.is_empty(),
        "these members were named by the manifest and no production source was read for them: \
         {unreached:?}. A member whose directory the walk skipped and a clean crate read the \
         same way, so the rule below cannot claim to have judged them."
    );
    assert!(
        found.forwarded.is_empty(),
        "these members declare nothing of their own -- their whole production surface is `use` \
         and `pub use` lines: {:?}. That is a glue layer at crate granularity: a second name for \
         code that already exists, with nothing inside it that a change could be about.",
        found.forwarded
    );
}

#[test]
fn a_crate_of_reexports_is_reported_and_a_crate_with_an_item_is_not() {
    let forwarder = "pub use cog_core::error::*;\n\
                     pub use cog_core::AgentId;\n\
                     use std::path::Path;\n";
    let owner = "pub use cog_core::error::*;\n\
                 pub fn route() {}\n";
    assert!(
        !declares_its_own_code(forwarder),
        "a crate of re-exports read as a crate with code in it"
    );
    assert!(
        declares_its_own_code(owner),
        "a crate with a `fn` of its own has to count as owning something"
    );
}
