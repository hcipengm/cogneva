//! Gate: the shape of the workspace's crate graph, derived rather than listed.
//!
//! The dependency direction between crates is the one rule nothing else can
//! check. `cargo` accepts any acyclic edge, so a crate can reach sideways or
//! upward and the build stays green -- until someone reads it and finds the
//! layering gone. That failure is invisible in exactly the way a gate is meant
//! to catch: the code compiles, the tests pass, and the edge is one line in a
//! `Cargo.toml` that no reviewer reads, added while solving a problem in a
//! different crate entirely.
//!
//! This gate used to freeze the graph: every edge it had was a row of a table,
//! every crate a row of another. A freeze is honest but it is also a second
//! copy of the graph, kept by hand, and its failure mode is the one every copy
//! has -- a row stops describing the thing it names while both look fine. The
//! table was replaced by the shape the rows were approximating, read out of
//! `cargo metadata` on every run: the whole graph, the crates that are programs,
//! and who links what into a shipped artifact. Nothing here lists a crate or an
//! edge; adding one without a change of shape is not possible, because there is
//! nothing to add it to.
//!
//! Three sentences, each a test:
//!
//! 1. a crate other than the assembly reaches the base and nothing else;
//! 2. a dev edge may only point at a crate nothing ships;
//! 3. a crate is either a program or something else in the workspace depends on.
//!
//! They are the same three the tables encoded, minus their upkeep. The second is
//! where the fixture crate's exemption lives, and it is worth being exact about
//! why that is not a list with one name on it: the property that admits a dev
//! edge is "nothing links this crate into a shipped artifact", which is derived
//! from the normal edges and can be re-checked against the graph by hand. A
//! crate that starts shipping stops qualifying in the same commit that makes it
//! ship, without anyone remembering to update a table.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

/// The crate every other crate may reach, and which reaches nothing here.
const BASE: &str = "cog-core";

/// The crate whose job is to put the others together.
///
/// One exception, named once. It is the assembly: the process that hands each
/// crate its work and the only place the pieces are allowed to meet. Everything
/// else is a component, and a component that reaches another component has
/// become a library with two names -- every change to one is a change to the
/// other, and every rebuild of one rebuilds both.
const ASSEMBLY: &str = "cogneva";

/// A census that read fewer crates than this has gone wrong, and every rule
/// below would pass over the empty set.
///
/// The tree has had twenty-seven since the skeleton settled. The floor is not
/// that number: it is low enough that removing crates never trips it and high
/// enough that a `cargo metadata` which resolved one package cannot pass.
const MIN_MEMBERS: usize = 20;

/// What `cargo metadata` says the graph is, right now.
struct Graph {
    /// Workspace crate names, so a dependency on something outside the
    /// workspace is not mistaken for an edge these rules govern.
    members: BTreeSet<String>,
    /// Edges linked into a shipped artifact.
    normal: BTreeSet<(String, String)>,
    /// Edges only tests link.
    dev: BTreeSet<(String, String)>,
    /// Crates that are programs: their package directory holds a `src/main.rs`.
    ///
    /// Read from the tree rather than from a list, because it is a fact about
    /// the crate and not a decision about it.
    programs: BTreeSet<String>,
}

impl Graph {
    /// The crates something links into a shipped artifact.
    ///
    /// This is the set a dev edge is measured against: it is what "ships"
    /// means, derived from the graph instead of asserted about a name.
    fn shipped(&self) -> BTreeSet<&String> {
        self.normal
            .iter()
            .map(|(_, dependency)| dependency)
            .collect()
    }

    /// The crates something depends on at all, dev edges included.
    fn depended_on(&self) -> BTreeSet<&String> {
        self.normal
            .iter()
            .chain(self.dev.iter())
            .map(|(_, dependency)| dependency)
            .collect()
    }
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
    let mut programs = BTreeSet::new();
    for package in packages {
        // Only workspace members are governed. A dependency `cog-*` crate
        // published outside the workspace would share the prefix and not the
        // rule, and these rules would then be asserting about something they do
        // not own.
        let Some(consumer) = package["name"].as_str() else {
            continue;
        };
        if !members.contains(consumer) {
            continue;
        }
        if let Some(dir) = package["manifest_path"].as_str() {
            let dir = Path::new(dir).parent().unwrap_or(Path::new(""));
            if dir.join("src/main.rs").is_file() {
                programs.insert(consumer.to_owned());
            }
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
        programs,
    }
}

fn name(edge: &(String, String)) -> String {
    format!("{} -> {}", edge.0, edge.1)
}

/// The census read the workspace, so the rules below are not passing over
/// nothing.
#[test]
fn the_graph_was_read() {
    let g = graph();
    assert!(
        g.members.len() >= MIN_MEMBERS,
        "the census read {} workspace crates (floor {MIN_MEMBERS}): every rule in \
         this file would hold over a graph that was never read",
        g.members.len()
    );
    assert!(
        g.members.contains(BASE),
        "{BASE} is not a workspace member any more; the rules below name a crate \
         that no longer exists and would pass by measuring nothing"
    );
    assert!(
        g.members.contains(ASSEMBLY),
        "{ASSEMBLY} is not a workspace member any more; the one exemption in this \
         file would then apply to no crate at all, and the direction rule would \
         silently become 'everything reaches only the base' with no assembly"
    );
    assert!(
        g.programs.contains(ASSEMBLY),
        "{ASSEMBLY} is exempt from the direction rule because it is the process \
         that assembles the components, but its package no longer has a \
         `src/main.rs` -- the exemption now covers a library, and the one place \
         the pieces meet is somewhere else"
    );
    println!(
        "{} crates, {} shipped edges, {} dev edges, {} programs",
        g.members.len(),
        g.normal.len(),
        g.dev.len(),
        g.programs.len()
    );
}

/// 1: the bottom of the graph stays the bottom.
#[test]
fn the_base_crate_depends_on_nothing_here() {
    let g = graph();
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

/// The component-to-component edges, as sentences.
fn sideways(g: &Graph) -> Vec<String> {
    g.normal
        .iter()
        .filter(|(consumer, dependency)| consumer != ASSEMBLY && dependency != BASE)
        .map(name)
        .collect()
}

/// The dev edges whose target is something an artifact already links, as
/// sentences.
fn dev_offenders(g: &Graph) -> Vec<String> {
    let shipped = g.shipped();
    let mut offenders = Vec::new();
    for (consumer, target) in &g.dev {
        let edge = (consumer.clone(), target.clone());
        if g.normal.contains(&edge) {
            offenders.push(format!(
                "{} (declared in both sections; the normal one wins and the dev \
                 section says the wrong thing about it)",
                name(&edge)
            ));
        } else if g.programs.contains(target) {
            offenders.push(format!("{} (its target is a program)", name(&edge)));
        } else if shipped.contains(target) {
            let linkers: Vec<&str> = g
                .normal
                .iter()
                .filter(|(_, dependency)| dependency == target)
                .map(|(linker, _)| linker.as_str())
                .collect();
            offenders.push(format!(
                "{} (its target is linked into the artifact by {})",
                name(&edge),
                linkers.join(", ")
            ));
        }
    }
    offenders
}

/// The crates that are neither programs nor depended on.
fn orphans(g: &Graph) -> Vec<String> {
    let depended = g.depended_on();
    g.members
        .iter()
        .filter(|member| !g.programs.contains(*member) && !depended.contains(*member))
        .cloned()
        .collect()
}

/// 2: a component reaches the base and nothing else.
#[test]
fn a_crate_that_is_not_the_assembly_reaches_only_the_base() {
    let sideways = sideways(&graph());
    assert!(
        sideways.is_empty(),
        "a component reached another component: {}\n\
         Every crate but the assembly may depend on the base and nothing else. \
         That is what makes the graph a star instead of a lattice: without it \
         every change to one crate becomes a rebuild and a possible breaking \
         change in every crate that reaches it, and 'which crate owns this' stops \
         having an answer. Three honest routes for the code that wants the edge: \
         the shape it needs moves into the base (a trait or a type, not the \
         work), the work moves into the crate that already owns that job, or the \
         assembly -- whose job is to put components together -- does the wiring.",
        sideways.join(", ")
    );
}

/// 3: a dev edge points at a crate nothing ships.
///
/// The double is the fixture crate's whole reason to exist, and its exemption is
/// this property rather than its name: nothing links it into an artifact, so it
/// cannot drag a rule of one crate into the test run of another. The failure it
/// prevents is specific -- a double owned by a shipping crate turns that crate's
/// changes into the failing tests of every crate that uses the double, reported
/// where nothing about it was being tested.
#[test]
fn a_dev_edge_points_at_a_crate_nothing_ships() {
    let offenders = dev_offenders(&graph());
    assert!(
        offenders.is_empty(),
        "a test-only edge does not point at a crate nothing ships:\n  {}\n\
         A dev dependency that names a shipping crate is a double a crate's \
         rules live in: the moment that crate changes, the test that fails is in \
         somebody else's suite, about something that was not being tested. A \
         double belongs in the crate whose tests use it, or in a crate nothing \
         links into an artifact -- which is a property of the graph here, not an \
         entry on a list.",
        offenders.join("\n  ")
    );
}

/// 4: a crate is a program, or something in the workspace depends on it.
///
/// The skeleton is settled. What this replaces from the frozen table is the
/// "just one small crate" shape: a manifest, a `src/`, and no edge either way. No
/// edge means no gate on the edge side can see it, and it is a whole-tree cost
/// paid by everything that rebuilds while being a split nobody made a decision
/// about. The rule does not forbid a crate; it says a crate has to be *for*
/// something -- wired into the crate that will use it, or a process of its own.
#[test]
fn no_crate_sits_in_the_tree_with_nobody_using_it() {
    let orphans = orphans(&graph());
    assert!(
        orphans.is_empty(),
        "crates nothing depends on and that are not programs: {orphans:?}\n\
         The skeleton is settled: a crate is not a build detail, it is a split \
         every consumer pays for. If the crate is right, the crate that uses it \
         depends on it -- a dev edge is enough, and a crate nothing ships (the \
         fixture crate is one) reaches the graph that way. If it is not, that \
         code belongs in the crate that already owns the job."
    );
}

/// The rules separate the shapes they are aimed at from the ones they allow.
///
/// The tree passes all of them, which is the same reading as rules that cannot
/// fail. Each judgement is fed a graph that has the offending shape and one that
/// has the permitted neighbour of it -- the assembly's sideways edges, the
/// fixture crate's dev-only reach -- so both polarities are pinned without
/// editing a manifest to find out.
#[test]
fn the_rules_separate_the_shapes_they_are_aimed_at() {
    let graph_of = |members: &[&str],
                    normal: &[(&str, &str)],
                    dev: &[(&str, &str)],
                    programs: &[&str]| Graph {
        members: members.iter().map(|m| (*m).to_owned()).collect(),
        normal: normal
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect(),
        dev: dev
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect(),
        programs: programs.iter().map(|p| (*p).to_owned()).collect(),
    };

    // A component reaching the base is the shape; the assembly reaching a
    // component is the neighbour that must stay silent.
    let reaching = graph_of(
        &[BASE, ASSEMBLY, "cog-one", "cog-two"],
        &[
            ("cog-one", BASE),
            ("cog-two", BASE),
            ("cog-two", "cog-one"),
            (ASSEMBLY, "cog-one"),
            (ASSEMBLY, BASE),
        ],
        &[],
        &[ASSEMBLY],
    );
    assert_eq!(
        sideways(&reaching),
        vec!["cog-two -> cog-one".to_owned()],
        "the direction rule flagged the wrong edges"
    );

    // A dev edge to a crate nothing ships is the shape the fixture crate has;
    // the same edge, once something links that crate, is the offender -- and so
    // is a pair declared in both sections.
    let doubling = graph_of(
        &[
            BASE,
            ASSEMBLY,
            "cog-one",
            "cog-user",
            "cog-test-support",
            "cog-shipping",
        ],
        &[
            ("cog-one", BASE),
            ("cog-shipping", BASE),
            ("cog-one", "cog-shipping"),
            (ASSEMBLY, "cog-shipping"),
        ],
        &[
            ("cog-user", "cog-test-support"),
            ("cog-user", "cog-shipping"),
            ("cog-one", "cog-shipping"),
        ],
        &[ASSEMBLY],
    );
    let offenders = dev_offenders(&doubling);
    assert_eq!(
        offenders.len(),
        2,
        "the dev rule did not separate the shipped target from the test-only \
         one: {offenders:?}"
    );
    assert!(
        offenders
            .iter()
            .any(|line| line.starts_with("cog-one -> cog-shipping")
                && line.contains("declared in both sections")),
        "a pair declared dev and normal at once was not reported as such: {offenders:?}"
    );
    assert!(
        offenders
            .iter()
            .any(|line| line.starts_with("cog-user -> cog-shipping")
                && line.contains("linked into the artifact")),
        "a dev edge to a crate an artifact links was not reported: {offenders:?}"
    );
    assert!(
        !offenders
            .iter()
            .any(|line| line.starts_with("cog-user -> cog-test-support")),
        "the test-only crate's edge was reported: {offenders:?}"
    );

    // A crate nobody depends on and that is not a program is the shape; the
    // fixture crate (dev-reached) and a program are the neighbours.
    let hanging = graph_of(
        &[BASE, ASSEMBLY, "cog-test-support", "cog-orphan"],
        &[(ASSEMBLY, BASE)],
        &[(ASSEMBLY, "cog-test-support")],
        &[ASSEMBLY],
    );
    assert_eq!(
        orphans(&hanging),
        vec!["cog-orphan".to_owned()],
        "the orphan rule flagged the wrong crates"
    );
}
