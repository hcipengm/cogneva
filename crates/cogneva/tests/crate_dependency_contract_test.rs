//! Gate: the workspace's crate graph may only shrink, never grow.
//!
//! The dependency direction between crates is the one rule nothing else can
//! check. `cargo` accepts any acyclic edge, so a crate can reach sideways or
//! upward and the build stays green -- until someone reads it and finds the
//! layering gone. That failure is invisible in exactly the way a gate is meant
//! to catch: the code compiles, the tests pass, and the edge is one line in a
//! `Cargo.toml` that no reviewer reads, added while solving a problem in a
//! different crate entirely.
//!
//! Freezing is the whole mechanism. `ALLOWED` is the graph as it stands: every
//! workspace-to-workspace edge is a row, and a build that has an edge the table
//! does not name fails. So a new edge is not forbidden by a rule about *which*
//! edges are bad -- it is forbidden because it is new, and adding it means
//! writing the row that says so. The same table is a ceiling in the other
//! direction: a row whose edge is gone fails too, so the table cannot quietly
//! grow stale while the graph underneath it shrinks. Removing an edge and
//! removing its row are one act, and the diff proves it.
//!
//! Dev dependencies are a second table rather than a column, because they are a
//! different claim: a dev dependency is not linked into anything shipped, so
//! folding the two would let an edge move from dev to normal without changing
//! the table -- which is the one direction that matters.
//!
//! `replace_with` is the route out. A row that carries it names what the
//! consumer is expected to reach instead; an empty one says the edge is accepted
//! as it stands and nobody has planned a replacement. It is not checked against
//! the code, because the shape of the replacement is a design decision -- but a
//! row cannot be deleted without the edge going first, so the column has a
//! reader every time somebody shrinks the graph.

use std::collections::BTreeSet;
use std::process::Command;

/// One accepted edge: `(consumer, dependency, route out)`.
///
/// Names are package names, so a rename in `Cargo.toml` fails the table rather
/// than silently matching a different crate.
type Row = (&'static str, &'static str, &'static str);

/// Every workspace-to-workspace edge linked into something shipped.
const ALLOWED: &[Row] = &[
    ("cog-agent", "cog-core", ""),
    ("cog-auth", "cog-core", ""),
    ("cog-collaboration", "cog-core", ""),
    ("cog-eval", "cog-core", ""),
    ("cog-extension", "cog-core", ""),
    ("cog-gateway", "cog-core", ""),
    ("cog-gateway", "cog-llm", ""),
    ("cog-gateway", "cog-observability", ""),
    ("cog-gateway", "cog-storage", ""),
    ("cog-github", "cog-core", ""),
    ("cog-guardrail", "cog-core", ""),
    ("cog-llm", "cog-core", ""),
    ("cog-memory", "cog-core", ""),
    // The boot-footprint marks are published through the metrics exporter that
    // `cog-observability` owns; the route out of this edge is that contract.
    ("cog-memory", "cog-observability", ""),
    ("cog-memory", "cog-storage", ""),
    ("cog-net", "cog-core", ""),
    ("cog-notification", "cog-core", ""),
    ("cog-observability", "cog-core", ""),
    // The bounded lock wait is a property of the runner `cog-storage` owns, so a
    // store that writes DDL has to hand it to that runner instead of keeping its
    // own `raw_sql`. The edge does not deepen the graph: every crate that already
    // depends on `cog-observability` depends on `cog-storage` too, and storage
    // reaches only core and redis.
    ("cog-observability", "cog-storage", ""),
    ("cog-orchestrator", "cog-core", ""),
    ("cog-prompt", "cog-core", ""),
    ("cog-protocol", "cog-core", ""),
    ("cog-quota", "cog-core", ""),
    ("cog-reflection", "cog-core", ""),
    ("cog-skill", "cog-core", ""),
    ("cog-storage", "cog-core", ""),
    ("cog-storage", "cog-redis", ""),
    ("cog-stream", "cog-core", ""),
    ("cog-stream", "cog-redis", ""),
    ("cog-supervisor", "cog-core", ""),
    ("cog-wiki", "cog-core", ""),
    ("cogneva", "cog-agent", ""),
    ("cogneva", "cog-auth", ""),
    ("cogneva", "cog-collaboration", ""),
    ("cogneva", "cog-core", ""),
    ("cogneva", "cog-eval", ""),
    ("cogneva", "cog-extension", ""),
    ("cogneva", "cog-gateway", ""),
    ("cogneva", "cog-github", ""),
    ("cogneva", "cog-guardrail", ""),
    ("cogneva", "cog-llm", ""),
    ("cogneva", "cog-memory", ""),
    ("cogneva", "cog-net", ""),
    ("cogneva", "cog-notification", ""),
    ("cogneva", "cog-observability", ""),
    ("cogneva", "cog-orchestrator", ""),
    ("cogneva", "cog-prompt", ""),
    ("cogneva", "cog-protocol", ""),
    ("cogneva", "cog-quota", ""),
    ("cogneva", "cog-reflection", ""),
    ("cogneva", "cog-skill", ""),
    ("cogneva", "cog-storage", ""),
    ("cogneva", "cog-stream", ""),
    ("cogneva", "cog-supervisor", ""),
    ("cogneva", "cog-wiki", ""),
    ("cogneva-bootstrap", "cog-core", ""),
];

/// The same, for dependencies only tests link.
const ALLOWED_DEV: &[Row] = &[
    ("cog-agent", "cog-stream", ""),
    ("cog-github", "cog-storage", ""),
    ("cog-memory", "cog-stream", ""),
    // Fixture support: four crates' `tests/` need the same facts (which
    // PostgreSQL, and whether an unset one stops or skips). Nothing shipped
    // links it, so the edges are dev-only; keeping the rule inside one crate's
    // `tests/common` just means the other three write it again.
    ("cog-memory", "cog-test-support", ""),
    ("cog-observability", "cog-test-support", ""),
    ("cog-orchestrator", "cog-storage", ""),
    ("cog-reflection", "cog-prompt", ""),
    ("cog-reflection", "cog-storage", ""),
    ("cog-storage", "cog-test-support", ""),
    ("cogneva", "cog-redis", ""),
    ("cogneva", "cog-test-support", ""),
];

/// The crate everything else may depend on, and which depends on nothing here.
const BASE: &str = "cog-core";

/// What `cargo metadata` says the graph is, right now.
struct Graph {
    /// Workspace crate names, so a dependency on something outside the
    /// workspace is not mistaken for an edge this table governs.
    members: BTreeSet<String>,
    normal: BTreeSet<(String, String)>,
    dev: BTreeSet<(String, String)>,
}

fn graph() -> Graph {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .output()
        .expect("cargo metadata should be runnable from a test");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let meta: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON");
    let packages = meta["packages"].as_array().expect("packages is a list");
    let members: BTreeSet<String> = packages
        .iter()
        .filter_map(|p| p["name"].as_str().map(str::to_owned))
        .collect();
    let mut normal = BTreeSet::new();
    let mut dev = BTreeSet::new();
    for package in packages {
        // Only workspace members are governed. A dependency `cog-*` crate
        // published outside the workspace would share the prefix and not the
        // rule, and this table would then be asserting about something it does
        // not own.
        let Some(consumer) = package["name"].as_str() else {
            continue;
        };
        if !members.contains(consumer) {
            continue;
        }
        for dep in package["dependencies"].as_array().into_iter().flatten() {
            let Some(name) = dep["name"].as_str() else {
                continue;
            };
            if !members.contains(name) {
                continue;
            }
            // `kind` is null for a normal dependency and a string otherwise, so
            // reading it as a string and comparing is what keeps a build
            // dependency on the shipped side, where it belongs: it is linked
            // into the artifact's build script, not into a test binary.
            let edge = (consumer.to_owned(), name.to_owned());
            if dep["kind"].as_str() == Some("dev") {
                dev.insert(edge);
            } else {
                normal.insert(edge);
            }
        }
    }
    Graph {
        members,
        normal,
        dev,
    }
}

fn rows(table: &[Row]) -> BTreeSet<(String, String)> {
    table
        .iter()
        .map(|(consumer, dep, _)| ((*consumer).to_owned(), (*dep).to_owned()))
        .collect()
}

fn name(edge: &(String, String)) -> String {
    format!("{} -> {}", edge.0, edge.1)
}

/// A: an edge the table does not name fails, so the graph cannot grow.
#[test]
fn a_new_edge_is_not_in_the_table() {
    let g = graph();
    let table = rows(ALLOWED);
    let fresh: Vec<String> = g.normal.difference(&table).map(name).collect();
    assert!(
        fresh.is_empty(),
        "the crate graph grew: {}\n\
         An edge this table does not name is a design decision, not a build \
         detail. If the edge is right, add `(\"<consumer>\", \"<dep>\", \"\")` to \
         ALLOWED and say in the commit why; if it is not, reach the same code \
         through the crate that already owns it.",
        fresh.join(", ")
    );
}

/// B: a row whose edge is gone fails, so the table cannot drift above the graph.
#[test]
fn a_row_whose_edge_is_gone_is_stale() {
    let g = graph();
    let table = rows(ALLOWED);
    let stale: Vec<String> = table.difference(&g.normal).map(name).collect();
    assert!(
        stale.is_empty(),
        "the table names edges the graph no longer has: {}\n\
         Delete those rows in the same commit that removed the edge. A ceiling \
         nobody lowers is not a ceiling, it is a record of what used to be true.",
        stale.join(", ")
    );
}

/// C: the two tables stay separate, so an edge cannot move between them unseen.
#[test]
fn dev_edges_have_their_own_table() {
    let g = graph();
    let table = rows(ALLOWED_DEV);
    let fresh: Vec<String> = g.dev.difference(&table).map(name).collect();
    let stale: Vec<String> = table.difference(&g.dev).map(name).collect();
    let promoted: Vec<String> = g
        .normal
        .intersection(&rows(ALLOWED_DEV))
        .map(name)
        .collect();
    assert!(
        fresh.is_empty(),
        "new dev-only edges: {}\n\
         Dev dependencies are in ALLOWED_DEV, not ALLOWED: an edge that moves \
         from one table to the other is the change this split exists to make \
         visible.",
        fresh.join(", ")
    );
    assert!(
        stale.is_empty(),
        "ALLOWED_DEV names dev edges the graph no longer has: {}\n\
         Delete those rows with the edge.",
        stale.join(", ")
    );
    assert!(
        promoted.is_empty(),
        "linked into the artifact and also listed as dev-only: {}\n\
         A dependency declared in both sections is carried by the normal one; \
         the dev row says the wrong thing about it.",
        promoted.join(", ")
    );
}

/// D: the bottom of the graph stays the bottom.
#[test]
fn the_base_crate_depends_on_nothing_here() {
    let g = graph();
    assert!(
        g.members.contains(BASE),
        "{BASE} is not a workspace member any more; this gate is measuring a \
         name that no longer exists"
    );
    let grown: Vec<String> = g
        .normal
        .iter()
        .chain(g.dev.iter())
        .filter(|(consumer, _)| consumer == BASE)
        .map(name)
        .collect();
    assert!(
        grown.is_empty(),
        "{BASE} grew dependencies on other crates: {}\n\
         Everything else may depend on the base; the base depending on anything \
         makes the direction circular the moment that crate reaches back. A type \
         the base needs from another crate belongs in the base, not beside it.",
        grown.join(", ")
    );
}
