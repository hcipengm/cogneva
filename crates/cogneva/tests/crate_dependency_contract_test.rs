//! Gate: the business crates depend on `cog-core`, not on each other.
//!
//! The contract is stated as an architecture, not as a preference: `cog-core`
//! carries the types every crate shares, and a business crate reaches another
//! business crate's *implementation* only through a contract that is either in
//! `cog-core` or is itself a documented layer. What the tree does today is
//! another matter: two dozen internal edges run between business crates, and the
//! only check that existed covered `cog-core` alone — the rule was stated,
//! carried no registry, and nothing read it, which is how a rule is spelled
//! while remaining unenforced.
//!
//! Why it is worth a gate rather than a convention. An edge between two business
//! crates makes one of them a second hub: changing it rebuilds everything that
//! links it, and the cost is paid by whoever edits the *dependency* while being
//! blamed on whoever edits the dependent. It also decides what can be tested in
//! isolation: a crate that links a neighbour's concrete backend cannot be
//! exercised without that backend, so its tests reach for whatever in-memory
//! stand-in the neighbour happens to ship — which is what most of the dev rows
//! below record. Neither cost is visible at the point the edge is added, where
//! the editor sees one `use` statement that made their change compile.
//!
//! The registry is a **debt list, not an exemption list**. Every row names the
//! core-side contract that edge is to be carried by, so a row is a statement
//! about work still owed rather than a permission granted; the end state of both
//! tables is empty. There is deliberately no "allowed" code, no free-text reason
//! and no "needs a ruling" terminal state: a row that cannot name where the edge
//! is going is a row that has not been thought through, and prose explaining why
//! an edge is acceptable reads exactly the same whether or not the explanation is
//! true. The cost of the work is not a reason to skip the naming — an edge whose
//! target is hard to name is the edge worth naming.
//!
//! The assertions hold the line where the tree is today and take the teeth from
//! the other direction:
//!
//! - A new edge fails. Every normal internal dependency of a business crate
//!   must appear in [`ALLOWED`], and every dev one in [`ALLOWED_DEV`]; the
//!   failure names the edge.
//! - A stale row fails. Every row must still correspond to a real edge, so
//!   removing an edge and leaving its row behind is refused rather than quietly
//!   widening the freeze. The table therefore only shrinks: it takes one edit to
//!   remove an edge and one to remove its row, and the second edit is the one
//!   this assertion is for.
//! - A row whose target does not exist fails. Each `replace_with` is a path into
//!   `cog-core`'s contract module, and a module or trait that is not there is
//!   either a typo or a plan that was never carried out. The handful of targets
//!   that genuinely have to be written first are listed in
//!   [`TARGETS_TO_INTRODUCE`], which is asserted in both directions: a target
//!   there must be missing today, and one that is not there must be present.
//! - `cog-core` itself has no internal dependency at all. It is the bottom of
//!   the graph, so an edge out of it is a cycle about to happen.
//!
//! Why the numbers come from `cargo metadata` rather than from reading the
//! manifests. The edges this gate is about are the ones a manifest *resolves*
//! to, in three kinds (normal, dev, build) that a manifest spells differently
//! and can express more than once for the same pair. A text scan gets the shapes
//! it was written to recognise and silently misses the rest: a commented-out
//! line, a `[target.'cfg(...)'.dependencies]` table, a rename, a feature-gated
//! one. The half-gate this replaces scans two files for two driver names and
//! would pass a crate that reached the same driver through a renamed
//! dependency — the scan answers "does this text appear", and the question is
//! "does this crate link that one". `cargo metadata` answers the question, in
//! the vocabulary of the resolver rather than of the file format.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The internal dependencies the tree has today, frozen: `(consumer,
/// dependency, replace_with)`.
///
/// `replace_with` is the edge's primary target — the contract that covers the
/// bulk of what the consumer reaches for — and not a claim that the edge has
/// exactly one use. An edge whose other uses need contracts of their own is
/// really several edges that share one dependency, and the row cannot say so:
/// the row is keyed on the pair, and the dependency stays until the last use of
/// it is gone. The known remainders are named where the row is, so a reader does
/// not have to rediscover them; `cog-gateway -> cog-storage` is the worst of
/// them, with one use of a pooled-connection handle beside the metrics backends
/// the row names.
///
/// A business crate is any workspace member named `cog-*`. The crates that
/// assemble them are listed in [`ASSEMBLY_ROOTS`] and are the only members
/// allowed to name a business crate directly, because wiring the crates
/// together is the whole of what they do.
const ALLOWED: &[(&str, &str, &str)] = &[
    ("cog-agent", "cog-redis", "contract::stream::MessageBackend"),
    ("cog-auth", "cog-redis", "contract::kv::KeyValueStore"),
    ("cog-auth", "cog-storage", "contract::kv::KeyValueStore"),
    (
        "cog-collaboration",
        "cog-redis",
        "contract::kv::KeyValueStore",
    ),
    ("cog-gateway", "cog-llm", "contract::llm::LlmClient"),
    ("cog-gateway", "cog-net", "contract::net::HttpClient"),
    (
        "cog-gateway",
        "cog-observability",
        "contract::alerts::AlertStore",
    ),
    ("cog-gateway", "cog-redis", "contract::kv::KeyValueStore"),
    // Metrics backends cover all but two of this edge's uses. The remainders
    // are one direct client handle, which is [`contract::kv::KeyValueStore`],
    // and one pooled-connection handle for which no contract exists yet — that
    // one needs a target of its own when the edge is worked, and it is why the
    // row cannot come out on the metrics work alone.
    (
        "cog-gateway",
        "cog-storage",
        "contract::storage::MetricsBackend",
    ),
    (
        "cog-memory",
        "cog-storage",
        "contract::storage::VectorBackend",
    ),
    ("cog-quota", "cog-redis", "contract::kv::KeyValueStore"),
    ("cog-quota", "cog-storage", "contract::kv::KeyValueStore"),
    ("cog-reflection", "cog-eval", "contract::eval::EvalService"),
    (
        "cog-storage",
        "cog-protocol",
        "contract::codec::RawRecordCodec",
    ),
    // What the storage crate owns and what the redis crate is: the argument
    // that the driver belongs to storage is an argument about which crate the
    // implementation lands in, not about a capability every crate should be
    // able to reach. Either way the reach is a key-value contract; there is no
    // version of this edge that ends in a direct link.
    ("cog-storage", "cog-redis", "contract::kv::KeyValueStore"),
    (
        "cog-stream",
        "cog-redis",
        "contract::stream::MessageBackend",
    ),
    (
        "cog-supervisor",
        "cog-redis",
        "contract::supervisor::LlmPoolStatusSource",
    ),
];

/// The dev-only internal dependencies, frozen the same way and asserted with
/// the same severity.
///
/// A dev edge does not ship, but it does rebuild: linking a neighbour from
/// `#[cfg(test)]` puts that neighbour in the rebuild set of this crate's tests
/// all the same, and it couples the two crates' fixtures — the test cannot be
/// run without the neighbour's in-memory backend existing and behaving. The
/// stand-in is the symptom; a contract with one implementation is what leaves a
/// test with nowhere else to get a stand-in from.
const ALLOWED_DEV: &[(&str, &str, &str)] = &[
    (
        "cog-github",
        "cog-storage",
        "contract::storage::MetricsBackend",
    ),
    (
        "cog-memory",
        "cog-stream",
        "contract::stream::MessageBackend",
    ),
    (
        "cog-orchestrator",
        "cog-storage",
        "contract::storage::StateBackend",
    ),
    (
        "cog-reflection",
        "cog-prompt",
        "contract::prompt::PromptManager",
    ),
    (
        "cog-reflection",
        "cog-storage",
        "contract::storage::StateBackend",
    ),
    (
        "cog-skill",
        "cog-storage",
        "contract::storage::MetricsBackend",
    ),
    ("cog-wiki", "cog-memory", "contract::memory::MemoryBackend"),
];

/// The closed set of core-side targets a row may name.
///
/// Closed rather than an open path field so that a row cannot be pointed at
/// something that merely exists: every member is a contract the core publishes,
/// which is what makes "reach it through this instead" a statement the reviewer
/// can check. Adding a member is a deliberate act, and it should be the act of
/// writing that contract.
const REPLACEMENT_TARGETS: &[&str] = &[
    "contract::alerts::AlertStore",
    "contract::codec::RawRecordCodec",
    "contract::eval::EvalService",
    "contract::kv::KeyValueStore",
    "contract::llm::LlmClient",
    "contract::memory::MemoryBackend",
    "contract::net::HttpClient",
    "contract::prompt::PromptManager",
    "contract::storage::MetricsBackend",
    "contract::storage::StateBackend",
    "contract::storage::VectorBackend",
    "contract::stream::MessageBackend",
    "contract::supervisor::LlmPoolStatusSource",
];

/// Targets the core does not have yet, and which the work above has to write
/// before any row naming them can be removed.
///
/// This is a list of owed contracts, not a relaxation of [`REPLACEMENT_TARGETS`]
/// — a name here still has to be spelled exactly like the contract that will be
/// written. It is asserted in both directions so that it empties itself: a
/// target listed here that now exists is a stale entry, which is the same
/// failure as a stale edge row.
const TARGETS_TO_INTRODUCE: &[&str] = &[
    // Most of the edges that reach the redis driver directly — five of the
    // eight, the ones that are not already covered by a contract for what they
    // do — want the same thing, and so do the two that reach `cog-storage`'s
    // client: a handle to a key-value store with the operations they use. No
    // such contract exists. The core has one for streams, objects, vectors and
    // metrics, and the plain key-value operations are the gap.
    "contract::kv::KeyValueStore",
    // `cog-prompt` owns prompt templates; the core publishes the rendering
    // contract (`split_contract`, `render_varying_half`) but no way to *obtain*
    // a rendered prompt. What the reader side needs is the manager rather than a
    // one-shot source: it loads the declared templates, takes the rendering
    // parameters, and can say which keys are declared.
    "contract::prompt::PromptManager",
];

/// The members that assemble the business crates, and the only ones permitted
/// to name one directly.
///
/// Declared rather than inferred from the `cog-*` naming: a member that is
/// neither `cog-*` nor listed here fails, so a business crate cannot be brought
/// in under a name this gate would not recognise, and a new assembly root has to
/// be a decision.
const ASSEMBLY_ROOTS: &[&str] = &["cogneva", "cogneva-bootstrap"];

/// `cog-core` is the bottom of the graph: every crate may depend on it, and it
/// depends on no crate.
const CORE: &str = "cog-core";

/// Where the core's contract modules are, relative to this crate's manifest.
/// The registry spells a target as `contract::<module>::<trait>`, and the check
/// reads the module rather than trusting the spelling.
const CORE_CONTRACT_DIR: &str = "../cog-core/src/contract";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Normal,
    Dev,
    Build,
}

impl Kind {
    fn of(metadata: &serde_json::Value) -> Kind {
        match metadata.get("kind").and_then(|k| k.as_str()) {
            None => Kind::Normal,
            Some("dev") => Kind::Dev,
            Some("build") => Kind::Build,
            Some(other) => panic!("unrecognised dependency kind {other:?}"),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Kind::Normal => "normal",
            Kind::Dev => "dev",
            Kind::Build => "build",
        }
    }
}

/// One internal edge, as the resolver sees it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Edge {
    consumer: String,
    dep: String,
    kind: Kind,
}

impl Edge {
    fn label(&self) -> String {
        format!("{} -> {} ({})", self.consumer, self.dep, self.kind.label())
    }
}

/// The workspace as `cargo metadata` resolves it.
struct Workspace {
    members: BTreeMap<String, PathBuf>,
    edges: Vec<Edge>,
}

impl Workspace {
    fn read() -> Self {
        let out = Command::new(env!("CARGO"))
            .args([
                "metadata",
                "--format-version",
                "1",
                "--no-deps",
                "--manifest-path",
            ])
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
            .output()
            .expect("cargo metadata must be runnable from a test binary");
        assert!(
            out.status.success(),
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let doc: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("cargo metadata must emit JSON");
        let packages = doc["packages"]
            .as_array()
            .expect("cargo metadata carries packages");

        // Members first: an edge is one between members, and a path dependency
        // outside the workspace is not a member of it.
        let mut members = BTreeMap::new();
        for p in packages {
            let name = p["name"].as_str().expect("a package has a name");
            let path = PathBuf::from(
                p["manifest_path"]
                    .as_str()
                    .expect("a package has a manifest path"),
            );
            members.insert(name.to_string(), path);
        }

        let mut edges = Vec::new();
        for p in packages {
            let consumer = p["name"].as_str().expect("a package has a name");
            let Some(deps) = p["dependencies"].as_array() else {
                continue;
            };
            for d in deps {
                let dep = d["name"].as_str().expect("a dependency has a name");
                // Only members, and never the self-edge a package may spell out
                // to reach its own dev features.
                if !members.contains_key(dep) || dep == consumer || dep == CORE {
                    continue;
                }
                edges.push(Edge {
                    consumer: consumer.to_string(),
                    dep: dep.to_string(),
                    kind: Kind::of(d),
                });
            }
        }
        edges.sort();
        edges.dedup();
        Self { members, edges }
    }

    fn kind(&self, kind: Kind) -> Vec<&Edge> {
        self.edges.iter().filter(|e| e.kind == kind).collect()
    }
}

/// The registry rows, keyed for lookup, with the target set checked in both
/// directions: a target outside the closed set is a row pointed at something
/// that is not a contract, and a duplicated pair is two rows for one edge.
fn registry(rows: &[(&str, &str, &str)], table: &str) -> BTreeSet<(String, String)> {
    let mut out = BTreeSet::new();
    for (consumer, dep, target) in rows {
        assert!(
            REPLACEMENT_TARGETS.contains(target),
            "{table} says {consumer} -> {dep} should be carried by `{target}`, which is not one of \
             {REPLACEMENT_TARGETS:?}"
        );
        assert!(
            out.insert((consumer.to_string(), dep.to_string())),
            "{table} registers {consumer} -> {dep} twice"
        );
    }
    out
}

/// The set of edges as the same shape the registry uses.
fn pairs(edges: &[&Edge]) -> BTreeSet<(String, String)> {
    edges
        .iter()
        .map(|e| (e.consumer.clone(), e.dep.clone()))
        .collect()
}

/// Whether the contract module a target names exists, and whether the trait it
/// names is declared there.
///
/// The module file is the check that a target is not pointing at a directory
/// that was never created; the declaration inside it is the check that the
/// spelling still matches the contract. Only an actual `trait` declaration
/// counts: accepting a mention in backticks would make a doc comment enough to
/// satisfy the row, which is the failure mode of every text-shaped gate, and it
/// is available here for free because a target is always a trait.
///
/// This reads the core's own source rather than the tree being judged, which is
/// what makes text an acceptable input: a doc comment mentioning the name would
/// be the core lying about itself, and the question here is only whether the
/// registry points at something the core publishes.
fn core_contract_says(target: &str) -> (bool, bool) {
    let mut parts = target.split("::");
    assert_eq!(
        parts.next(),
        Some("contract"),
        "targets are `contract::...`"
    );
    let module = parts.next().expect("a target names a module");
    let item = parts.next().expect("a target names a trait");
    assert_eq!(parts.next(), None, "a target is `contract::m::Trait`");

    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(CORE_CONTRACT_DIR)
        .join(format!("{module}.rs"));
    let Ok(source) = std::fs::read_to_string(&path) else {
        return (false, false);
    };
    (true, declares_trait(&source, item))
}

/// Whether the source declares a trait by this name.
///
/// The identifier after `trait` has to end where the name ends, or `trait Foo`
/// would be satisfied by `trait Foobar` — a lookup by prefix that answers a
/// question nobody asked.
fn declares_trait(source: &str, item: &str) -> bool {
    let mut rest = source;
    while let Some(at) = rest.find("trait ") {
        let after = &rest[at + "trait ".len()..];
        let name: String = after
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        if name == item {
            return true;
        }
        rest = after;
    }
    false
}

/// A member is a business crate if it is named `cog-*`; the assembly roots are
/// the declared exceptions, and nothing else is.
#[test]
fn every_member_is_a_business_crate_or_a_declared_assembly_root() {
    let ws = Workspace::read();
    let undeclared: Vec<&String> = ws
        .members
        .keys()
        .filter(|name| !name.starts_with("cog-") && !ASSEMBLY_ROOTS.contains(&name.as_str()))
        .collect();
    assert!(
        undeclared.is_empty(),
        "workspace members that are neither a `cog-*` business crate nor one of the assembly \
         roots {ASSEMBLY_ROOTS:?}: {undeclared:?} — a business crate outside the naming this gate \
         recognises would be exempt from every assertion below without anyone deciding that"
    );
    for root in ASSEMBLY_ROOTS {
        assert!(
            ws.members.contains_key(*root),
            "assembly root {root} is declared but is not a workspace member"
        );
    }
}

/// The assembly roots are exempt from the registry because assembling is what
/// they do; every other member's normal dependencies have to be on the list.
#[test]
fn a_normal_dependency_between_business_crates_is_registered_or_rejected() {
    let ws = Workspace::read();
    let registered = registry(ALLOWED, "ALLOWED");
    let unregistered: Vec<String> = ws
        .kind(Kind::Normal)
        .iter()
        .filter(|e| !ASSEMBLY_ROOTS.contains(&e.consumer.as_str()))
        .filter(|e| !registered.contains(&(e.consumer.clone(), e.dep.clone())))
        .map(|e| e.label())
        .collect();
    assert!(
        unregistered.is_empty(),
        "business crates depend on each other without a registered target: {unregistered:?} — a \
         business crate reaches another's contract, not its implementation; if the edge is here to \
         stay it needs a row in ALLOWED naming the contract it should be carried by, and if it is \
         not, the dependency is the fix"
    );
}

/// The dev table is asserted with the same severity as the normal one: the edge
/// does not ship, but it is the same coupling at test time.
#[test]
fn a_dev_dependency_between_business_crates_is_registered_or_rejected() {
    let ws = Workspace::read();
    let registered = registry(ALLOWED_DEV, "ALLOWED_DEV");
    let unregistered: Vec<String> = ws
        .kind(Kind::Dev)
        .iter()
        .filter(|e| !ASSEMBLY_ROOTS.contains(&e.consumer.as_str()))
        .filter(|e| !registered.contains(&(e.consumer.clone(), e.dep.clone())))
        .map(|e| e.label())
        .collect();
    assert!(
        unregistered.is_empty(),
        "business crates reach each other from their tests without a registered target: \
         {unregistered:?} — a dev edge rebuilds the same as any other, and the contract that would \
         carry it is the same one the normal table names"
    );
}

/// The direction that gives the registry its teeth: an edge that is gone must
/// take its row with it, or the table stops being a description of the tree and
/// becomes a place for edges to accumulate.
#[test]
fn a_registered_edge_that_no_longer_exists_is_a_stale_row() {
    let ws = Workspace::read();
    let live = |kind: Kind, rows: &[(&str, &str, &str)], table: &str| {
        let registered = registry(rows, table);
        let present = pairs(&ws.kind(kind));
        let stale: Vec<String> = registered
            .difference(&present)
            .map(|(consumer, dep)| format!("{consumer} -> {dep}"))
            .collect();
        assert!(
            stale.is_empty(),
            "{table} registers edges the tree no longer has: {stale:?} — the table only shrinks; \
             removing an edge means removing its row"
        );
    };
    live(Kind::Normal, ALLOWED, "ALLOWED");
    live(Kind::Dev, ALLOWED_DEV, "ALLOWED_DEV");
}

/// Every row names a contract that is really there, or one that is on the list
/// of contracts still owed. A target that is neither is a typo, a rename that
/// nobody followed, or a plan that was quietly dropped — and all three look
/// exactly like a row that is fine.
#[test]
fn every_registered_target_is_a_core_contract_that_exists_or_is_owed() {
    for (table, rows) in [("ALLOWED", ALLOWED), ("ALLOWED_DEV", ALLOWED_DEV)] {
        for (consumer, dep, target) in rows {
            let (module_exists, trait_declared) = core_contract_says(target);
            let owed = TARGETS_TO_INTRODUCE.contains(target);
            if owed {
                assert!(
                    !trait_declared,
                    "{table} lists `{target}` (for {consumer} -> {dep}) as still to be written, \
                     but the core already declares it — the owed list has to empty itself, or it \
                     becomes a second place where a target can sit without being checked"
                );
            } else {
                assert!(
                    module_exists,
                    "{table} sends {consumer} -> {dep} to `{target}`, but the core has no \
                     {CORE_CONTRACT_DIR}/{}.rs",
                    target.split("::").nth(1).unwrap_or("?")
                );
                assert!(
                    trait_declared,
                    "{table} sends {consumer} -> {dep} to `{target}`, but that contract module \
                     does not declare it — a target naming something that is not there is a row \
                     nobody can act on"
                );
            }
        }
    }
    // And the owed list is a subset of the closed set: a target that can be
    // named is a target from [`REPLACEMENT_TARGETS`], owed or not.
    for target in TARGETS_TO_INTRODUCE {
        assert!(
            REPLACEMENT_TARGETS.contains(target),
            "TARGETS_TO_INTRODUCE carries `{target}`, which is not one of {REPLACEMENT_TARGETS:?}"
        );
    }
}

/// `cog-core` is the crate every other one may depend on, which makes its own
/// dependencies the one set that has to be empty.
#[test]
fn cog_core_depends_on_no_workspace_crate() {
    let ws = Workspace::read();
    let out: Vec<String> = ws
        .edges
        .iter()
        .filter(|e| e.consumer == CORE)
        .map(|e| e.label())
        .collect();
    assert!(
        out.is_empty(),
        "{CORE} depends on workspace crates: {out:?} — contracts do not depend on the crates that \
         implement them"
    );
}

/// A build-time dependency between business crates has no shape in this
/// architecture: the registry has no table for one because the tree has none,
/// and a build script that links a neighbour runs before the contract it would
/// be reaching for exists.
#[test]
fn no_business_crate_is_linked_from_anothers_build_script() {
    let ws = Workspace::read();
    let out: Vec<String> = ws.kind(Kind::Build).iter().map(|e| e.label()).collect();
    assert!(
        out.is_empty(),
        "business crates are linked as build dependencies of each other: {out:?}"
    );
}
