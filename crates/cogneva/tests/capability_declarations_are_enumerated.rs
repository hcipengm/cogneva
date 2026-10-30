//! principle: feedback_an_advertised_capability_is_a_row_in_the_serving_table
//!
//! Every place in the tree that builds a `"capabilities"` object is classified
//! here, and the classification is an equality.
//!
//! Two kinds of place write that key, and they are not the same kind of
//! statement:
//!
//! - A **server** reply says what this process can be asked to do. It is a
//!   promise to a caller, and it has to be built from the table that serves the
//!   promise rather than spelled out next to it -- otherwise the reply keeps
//!   naming a namespace whose handlers were deleted, and the caller acts on a
//!   list that does not describe the server it is talking to.
//! - A **client**'s handshake params say what the client itself can do. `{}` is
//!   the honest answer there, and it is not a promise about this tree at all.
//!
//! Which of the two a given line is cannot be read off the line: an empty object
//! is the right shape for a client with nothing to declare and the wrong shape
//! for a server. So this file does not try to tell them apart by shape. It
//! pins *the set of places that write the key at all*, with a reason for each,
//! and goes red when a place arrives that nobody has classified. The arrival is
//! the thing being caught: a third server declaring its capabilities as a
//! literal is exactly how the first two drifted.
//!
//! **Counts, not just paths.** A path-only census cannot see a second literal
//! added inside a file that already has one -- which is how a client's params
//! file grows a server reply. The count per file is what makes that an arrival.
//!
//! **Ceiling.** This says nothing about capabilities declared under some other
//! key or spelled into a struct field rather than a JSON object (`AgentCard`,
//! `PluginManifest`, an agent registry row). Those are separate declarations
//! with their own constructors and their own readers; this census covers the
//! JSON-key form only, because that is the form the two drifting servers used.
//! It also cannot tell whether a server-side entry it has been told to allow is
//! actually derived -- that is what each server's own test does, and this file
//! pins only that the two exist.

use std::collections::BTreeMap;

use crate::common;

use common::routes::repo_root;

use common::rust_production::all_crate_files;

/// The JSON key whose construction points are being enumerated.
const KEY: &str = "\"capabilities\":";

/// Every file that writes that key, how many lines of it do, and which kind of
/// statement it is.
///
/// Adding a row here is the fix for a red run, and the reason column is the
/// point of the row: "the client's own handshake params" and "a test fixture"
/// are answers, and so is "a server reply, which now has to be built from its
/// serving table". What is not an answer is adding the row without reading the
/// new line.
const KNOWN: &[(&str, usize, &str)] = &[
    (
        "crates/cog-agent/src/tools.rs",
        1,
        "a roster entry, not an advertisement: `list_agents` echoes each \
         registered agent's own `capabilities` exactly as the registry holds \
         them, which is the same field the send path checks before publishing. \
         Nothing in it claims what this process can be asked to do, and there \
         is no serving table to build it from -- the value belongs to the agent \
         it describes, not to the server returning it",
    ),
    (
        "crates/cog-gateway/src/a2a.rs",
        2,
        "two server replies, one per version served (the 0.3 card and the 1.0 \
         card). Both take the object from `capabilities_wire()`, which derives \
         every flag from `capabilities()` and so from `ADVERTISED`; neither card \
         spells a flag out. The count is two because the versions are two \
         distinct documents, not because there are two authors of the promise \
         -- a third one would have to go through the same constructor",
    ),
    (
        "crates/cog-gateway/src/search_admin.rs",
        1,
        "the settings page's own option list, not a promise about what this \
         process can be asked to do: it names which response shapes and which \
         capabilities the write side will accept for a fallback upstream. Both \
         columns are mapped from the enums' own tables (`ResponseShape::\
         CONFIGURABLE`, `Capability::ALL`) rather than spelled out, so the menu \
         and the parser cannot drift apart -- an option the writer rejects, or a \
         value the writer accepts that no page offers, are the two failures this \
         shares a table to avoid",
    ),
    (
        "crates/cog-protocol/src/a2a.rs",
        2,
        "test fixtures: the wire shape a client parses back into an agent card -- \
         one 0.3 card and one 1.0 card, in the same test, because the two \
         versions differ in where the endpoint and the version live. Neither is \
         a reply this tree sends",
    ),
    (
        "crates/cog-protocol/src/mcp.rs",
        2,
        "one comment describing the defect, and the server reply, which is built \
         from `capabilities_json()` rather than spelled out",
    ),
    (
        "crates/cog-protocol/src/transport.rs",
        1,
        "the client's own handshake params -- what cogneva, as a client, can do, \
         not a promise about this tree",
    ),
    (
        "crates/cog-protocol/tests/mcp_sse.rs",
        1,
        "test fixture: a stand-in server's handshake reply",
    ),
    (
        "crates/cog-protocol/tests/mcp_stdio.rs",
        1,
        "test fixture: a stand-in server's handshake reply",
    ),
];

/// The two servers that advertise capabilities, and the constructor their reply
/// has to come from. A stale row here is a red run of its own: it would mean the
/// census is naming a constructor that no longer exists.
const SERVER_CONSTRUCTORS: &[(&str, &str)] = &[
    ("crates/cog-protocol/src/mcp.rs", "capabilities_json()"),
    ("crates/cog-gateway/src/a2a.rs", "pub const ADVERTISED"),
];

/// Every `.rs` file under `dir`, recursively. Test helpers and build scripts are
/// `.rs` too, and a declaration written in one of those is still a declaration.
/// How many lines of each file contain the key, as repo-relative paths.
fn census() -> BTreeMap<String, usize> {
    let root = repo_root();
    let files = all_crate_files(&root);

    let mut found = BTreeMap::new();
    for path in files {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        let count = text.lines().filter(|line| line.contains(KEY)).count();
        if count == 0 {
            continue;
        }
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        found.insert(rel, count);
    }
    found
}

fn known_map() -> BTreeMap<String, usize> {
    KNOWN
        .iter()
        .map(|(path, count, _)| ((*path).to_string(), *count))
        .collect()
}

/// The whole point: nothing may write that key without being classified.
#[test]
fn every_place_that_builds_a_capabilities_object_is_enumerated() {
    let found = census();
    let known = known_map();

    assert!(
        !known.is_empty(),
        "an empty registry would make the comparison below vacuous"
    );

    let arrived: Vec<&String> = found.keys().filter(|k| !known.contains_key(*k)).collect();
    assert!(
        arrived.is_empty(),
        "these files write a `{KEY}` object and are not classified here: {arrived:?}. Read \
         each one and add a row with a reason. If it is a server reply, it has to be built \
         from the table that serves the promise -- `capabilities_json()` in \
         crates/cog-protocol/src/mcp.rs and `ADVERTISED` in crates/cog-gateway/src/a2a.rs are \
         the two that are. If it is prose or a client's own handshake params, say so in the \
         reason column"
    );

    let gone: Vec<&String> = known.keys().filter(|k| !found.contains_key(*k)).collect();
    assert!(
        gone.is_empty(),
        "the registry names files that no longer write a `{KEY}` object: {gone:?}. A census \
         that keeps a row for something that left is a census nobody can trust about what \
         arrived"
    );

    let grown: Vec<(String, usize, usize)> = known
        .iter()
        .filter_map(|(path, was)| {
            let now = found.get(path)?;
            (*now != *was).then(|| (path.clone(), *was, *now))
        })
        .collect();
    assert!(
        grown.is_empty(),
        "the count of lines writing a `{KEY}` object changed (file, was, now): {grown:?}. A \
         second one inside a file that already had one is an arrival too -- that is how a \
         client's params file grows a server reply. Classify the new line and update its \
         count"
    );
}

/// The other half of the census: the two servers it names have to still exist,
/// both built from one construction point rather than a literal in the reply.
///
/// Without this, a census could stay green while the constructors it points at
/// were deleted and the replies went back to being spelled out -- the counts
/// would not move, because a literal and a call write the same key.
#[test]
fn the_two_servers_still_build_their_reply_from_one_construction_point() {
    let root = repo_root();
    for (path, constructor) in SERVER_CONSTRUCTORS {
        let text = std::fs::read_to_string(root.join(path))
            .unwrap_or_else(|e| panic!("{path} has to be readable: {e}"));
        assert!(
            text.contains(constructor),
            "{path} has to build its advertised capabilities from `{constructor}`, or the \
             reply is a literal again: an object built next to the dispatch agrees with \
             itself no matter what the dispatch answers"
        );
    }
}
