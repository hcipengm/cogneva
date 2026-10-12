//! Gate: `cog-core` carries shared types and contracts, not mechanisms.
//!
//! The rule is stated in the architecture and is easy to agree with: the core
//! holds what two crates must spell the same way — a type, a name, a unit, a
//! trait — and a capability that one crate owns lives in that crate. What the
//! tree does today is another matter. `cog-core` is the crate all of the others
//! depend on, which is exactly what makes it the tempting place to put anything:
//! a module dropped there is reachable from everywhere, so nothing ever has to
//! argue about where it belongs. Six of them are mechanisms by their own
//! description — a `flock` slot protocol, a process-level singleton, `/proc`
//! sampling, a lease, a reclaimer — and they sit next to the contracts because
//! being depended on by everyone was mistaken for being shared.
//!
//! Being called by several crates is not the test. A contract is an interface, a
//! name or a shape; a mechanism keeps state and does something. A mechanism that
//! happens to be called from nine crates is still a mechanism, and the nine call
//! sites are nine reasons for it to be *reachable as a contract*, which is a
//! different thing from nine reasons for it to live here.
//!
//! What this gate is for. The principle was stated and nothing read it: no
//! check in the CI configuration or the deploy scripts enforced it, so the way
//! to add a mechanism to the core was to add a file. A stated rule with no
//! registry and no gate is a rule that reads as enforced while being enforced by
//! nothing.
//!
//! Why the shapes below are the shapes that cannot be fooled. A symbol scan for
//! the mechanisms is not available: `unsafe` appears in a doc comment about
//! recovery being unsafe, `tokio::spawn` appears inside an example in a contract
//! module's documentation, and text cannot tell those from a real call site. So
//! the assertions read structure instead:
//!
//! - [`CORE_ROOT_MODULES`] is the exact set of modules in the core's source
//!   root, and the exact set of directories there too. A file that is not on the
//!   list fails, and so does a directory, so a mechanism cannot arrive by being
//!   added — only by being registered, which is a decision someone has to write
//!   down.
//! - The mechanisms the relocation owes are registered in [`RELOCATION_TARGET`]
//!   with the crate they are going to, so the entry that lets a mechanism stay
//!   is the entry that says where it is going. That list is asserted in both
//!   directions, so it empties itself as the work lands.
//! - The core's own dependencies are a closed set with no workspace crate in it,
//!   and [`DEPS_TO_DROP`] names what the relocation is going to make unnecessary.
//!
//! What this gate does not decide. A module that is part contract and part
//! implementation passes, because the gate is drawn around files: the contract
//! half is why the module is registered, and the implementation half sits inside
//! the same file where nothing here can see it. That is a real gap, and it is
//! the one the registration of such a module should be made to mention rather
//! than the one this test can close.
//!
//! The same honesty is owed about the reasons themselves: they are written by
//! whoever adds the module, and nothing here checks that they are true. What the
//! registry buys is that the claim has to be made out loud, in one of a fixed
//! handful of words, in a diff somebody reviews — the alternative was a rule
//! nothing read at all. A module registered as `shared-definition` that is
//! really a mechanism passes this gate and fails its reviewer, and that is the
//! division of labour the gate is built on rather than a hole in it.
//!
//! principle: feedback_prefer_not_to_touch_core

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

/// The modules in the core's source root, each with the reason it is a contract
/// rather than a capability: `(module, reason)`.
///
/// The reason is a member of [`MODULE_REASONS`] rather than prose. A row whose
/// reason is a sentence makes the same claim whether the claim is true, and the
/// whole point of registering a module is that the claim can be checked against
/// the list.
const CORE_ROOT_MODULES: &[(&str, &str)] = &[
    ("admission", "pending-relocation"),
    ("audit", "shared-definition"),
    ("build_cache", "shared-definition"),
    ("build_cache_readings", "pending-relocation"),
    ("build_cache_reclaim", "pending-relocation"),
    ("build_gate", "pending-relocation"),
    ("claim_footprint", "shared-definition"),
    ("config", "shared-config"),
    ("config_sections", "shared-config"),
    ("error", "shared-error"),
    ("fs_size", "shared-definition"),
    ("lib", "crate-root"),
    ("loop_health", "pending-relocation"),
    ("observability_text", "shared-definition"),
    ("owner_lease", "pending-relocation"),
    ("secrets", "shared-definition"),
    ("token_estimate", "shared-definition"),
];

/// The directories in the core's source root, registered the same way and for
/// the same reason: a module that arrives as a directory would otherwise pass an
/// assertion drawn around `.rs` files.
const CORE_ROOT_DIRS: &[&str] = &["contract", "types"];

/// The closed set of reasons a module may be registered with.
const MODULE_REASONS: &[&str] = &[
    // A definition that two publishers have to agree on — a name, a unit, a
    // size, a rendering — where a second copy would drift and the two readings
    // would differ with nothing in either one to explain the difference.
    "shared-definition",
    // Configuration types: every crate that reads the configuration reads the
    // same fields, and a second definition would be a second configuration.
    "shared-config",
    // The error type every crate's fallible boundary returns.
    "shared-error",
    // The crate root itself.
    "crate-root",
    // A mechanism that is in the core today and is owed a move to the crate
    // that owns it. See `RELOCATION_TARGET`.
    "pending-relocation",
];

/// The shape of the move a registered mechanism is owed.
///
/// A registered destination is not by itself a plan. Where a mechanism can go
/// depends on who reads it: a module whose consumers already reach the
/// destination can move as it stands, and one whose consumers reach a crate that
/// reaches *them* cannot move at all without inverting an edge. The two shapes
/// take the two different answers, so the row has to say which one it is rather
/// than leave a reader to work it out from the destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owed {
    /// The module moves whole, and every crate that reads it already depends on
    /// the destination. Nothing else has to change for the move to be legal, so
    /// the work is the move and nothing here can be satisfied by relabelling.
    Direct,
    /// The module is a contract and a mechanism in one file, and no existing
    /// crate both reads it and is reachable from the destination. It leaves in
    /// two halves: the contract half stays in the core as a module under
    /// `contract/`, where the assertions drawn around the core's source root
    /// cannot see it, and the half that keeps state leaves for the destination.
    /// The consumers are untouched, because they already reach `cog-core`.
    Split,
}

/// Where each mechanism is going: `(module, destination crate, owed)`.
///
/// The two directions are both asserted, so this is a debt list and not a
/// permission: a module registered as awaiting relocation has to name its
/// destination, and the destination has to be a module that is registered that
/// way. The list ends at empty, and until it does, each row says which crate the
/// work goes to rather than why the module may stay.
///
/// The destinations are existing crates rather than a new one: the direction of
/// the dependency graph decides who can hold what, and a crate the consumers
/// already depend on is the one whose relocation does not invent an edge. The
/// modules that reach `/proc` or take a `flock` are the same family and have to
/// land together, because they call each other.
///
/// What each destination means depends on [`Owed`]: for a `Direct` row it is
/// where the module ends up, and for a `Split` row it is where the half that
/// keeps state ends up. A `Split` row's destination is therefore *not* a crate
/// its readers depend on -- it is chosen by which crate already runs the
/// mechanism, and the readers never touch it.
const RELOCATION_TARGET: &[(&str, &str, Owed)] = &[
    // Runs the control law in `cog-eval`, the crate that drives the gate through
    // a whole cycle. `cog-reflection` was named here before the readers were
    // counted, and it cannot be the destination: `cog-eval` is a reader, so the
    // move would add `cog-eval -> cog-reflection`, and `cog-reflection` already
    // depends on `cog-eval` -- the edge would close a cycle rather than lengthen
    // a path.
    ("admission", "cog-eval", Owed::Split),
    // Read for the sizes of the layers it caches, by the crate whose workdir
    // owns that cache.
    ("build_cache_readings", "cog-extension", Owed::Split),
    // One reader, and it is the destination.
    ("build_cache_reclaim", "cog-reflection", Owed::Direct),
    // Twenty-eight call sites in the destination and five in the assembly root.
    ("build_gate", "cog-reflection", Owed::Direct),
    // The crate that publishes the health readings the loops are watched by.
    ("loop_health", "cog-observability", Owed::Split),
    // The lease is stored next to the tiers it guards.
    ("owner_lease", "cog-storage", Owed::Direct),
];

/// Where a split module's contract half lands: the core's `contract` module,
/// under the module's own name.
///
/// The name is derived rather than written down a second time. A row that named
/// its contract half could name a different module than the one it is registered
/// under, and the two spellings would agree with nothing; deriving it means the
/// half is either at the path the registry implies or it is not there.
const CONTRACT_DIR: &str = "../cog-core/src/contract";

/// The calls a contract half may not contain, because a contract that reaches
/// the kernel is a mechanism that was renamed rather than moved.
///
/// Read as code rather than as text. Each entry is a spelling that only occurs
/// where something is being called or opened: `/proc` needs the quote of a path
/// literal, a process spawn needs the module path, and `libc` needs the `::`.
/// A bare word would not do -- `cog-core/src/contract/supervisor.rs` has a doc
/// comment reading "restart the service/process", and matching `/proc` inside
/// that word would report a contract that reaches the kernel where there is a
/// sentence about restarting one.
const CONTRACT_FORBIDDEN: &[&str] = &["\"/proc", "std::process", "libc::", "flock("];

/// The crates the core may depend on: closed, and with no workspace crate in it.
///
/// The core is the bottom of the graph. An entry here is a decision that the
/// contract layer needs that crate's types; a workspace crate appearing here is
/// a cycle in the making, and appears nowhere because [`CORE`]'s own file
/// dependency check would then be reading a graph that no longer exists.
const CORE_DEPS: &[&str] = &[
    "async-trait",
    "base64",
    "blake3",
    "bytes",
    "chrono",
    "futures",
    "hmac",
    "jsonschema",
    "libc",
    "regex",
    "schemars",
    "serde",
    "serde_json",
    "sha2",
    "thiserror",
    "tokio",
    "toml",
    "tracing",
    "uuid",
    "zstd",
];

/// Dependencies the relocation is going to make unnecessary, listed so that
/// removing them is the same edit as removing the last module that needs them.
///
/// Asserted in the direction that forces the list to empty: while an entry is
/// here it has to still be a dependency, so the moment the modules move and the
/// dependency goes, the leftover entry fails and has to be deleted from both
/// this list and [`CORE_DEPS`]. After that the closed set does the banning on
/// its own — an unlisted dependency cannot come back.
const DEPS_TO_DROP: &[&str] = &[
    // Reached the kernel directly for `getuid`, `kill` and the file-size and
    // disk-space calls. Contracts do not talk to the kernel; the mechanisms
    // that do are the ones leaving.
    "libc",
];

/// `cog-core` is the bottom of the graph: every crate may depend on it, and it
/// depends on no crate.
const CORE: &str = "cog-core";

/// The core's source root, relative to this crate's manifest.
const CORE_SRC: &str = "../cog-core/src";

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

/// The workspace as `cargo metadata` resolves it, plus the core's source root as
/// the filesystem has it.
struct Workspace {
    /// Every workspace member, by name.
    members: BTreeSet<String>,
    /// The core's dependencies, by kind. Read from the resolver rather than the
    /// manifest: a dependency spelled under a target table, renamed, or behind a
    /// feature is still a dependency, and a text scan of the manifest answers a
    /// question about the file format instead of about the crate.
    core_deps: BTreeMap<Kind, BTreeSet<String>>,
    /// Every member's normal dependencies, by member name.
    ///
    /// The relocation check asks whether a crate a module is consumed from
    /// already reaches the crate the module is going to, and that is a question
    /// about the resolved graph for the same reason the core's own set is: an
    /// edge can be spelled under a target table or behind a feature, and a scan
    /// of the manifest would answer about the spelling instead of the edge.
    member_deps: BTreeMap<String, BTreeSet<String>>,
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

        let mut members = BTreeSet::new();
        let mut member_deps: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for p in packages {
            let name = p["name"]
                .as_str()
                .expect("a package has a name")
                .to_string();
            let mut normal = BTreeSet::new();
            for d in p["dependencies"]
                .as_array()
                .expect("a package carries its dependencies")
            {
                if Kind::of(d) == Kind::Normal {
                    normal.insert(
                        d["name"]
                            .as_str()
                            .expect("a dependency has a name")
                            .to_string(),
                    );
                }
            }
            member_deps.insert(name.clone(), normal);
            members.insert(name);
        }

        let core = packages
            .iter()
            .find(|p| p["name"].as_str() == Some(CORE))
            .expect("the workspace has a cog-core member");
        let mut core_deps: BTreeMap<Kind, BTreeSet<String>> = BTreeMap::new();
        for d in core["dependencies"]
            .as_array()
            .expect("a package carries its dependencies")
        {
            let name = d["name"].as_str().expect("a dependency has a name");
            core_deps
                .entry(Kind::of(d))
                .or_default()
                .insert(name.to_string());
        }
        Self {
            members,
            core_deps,
            member_deps,
        }
    }
}

/// The core's source root as the filesystem has it: the `.rs` modules and the
/// directories, split the way the registry is.
fn core_root_entries() -> (BTreeSet<String>, BTreeSet<String>) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(CORE_SRC);
    let mut modules = BTreeSet::new();
    let mut subdirs = BTreeSet::new();
    for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| {
        panic!(
            "the core's source root {} must be readable: {e}",
            dir.display()
        )
    }) {
        let entry = entry.expect("a directory entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let kind = entry.file_type().expect("a file type");
        if kind.is_dir() {
            subdirs.insert(name);
        } else if let Some(stem) = name.strip_suffix(".rs") {
            modules.insert(stem.to_string());
        }
    }
    (modules, subdirs)
}

/// The registry as a set, with the reason set checked in both directions: a
/// reason outside the closed set is a free-text exemption wearing a label, and a
/// module registered twice is two rows for one file.
fn registered_modules() -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (module, reason) in CORE_ROOT_MODULES {
        assert!(
            MODULE_REASONS.contains(reason),
            "CORE_ROOT_MODULES registers `{module}` with `{reason}`, which is not one of \
             {MODULE_REASONS:?}"
        );
        assert!(
            out.insert(module.to_string()),
            "CORE_ROOT_MODULES registers `{module}` twice"
        );
    }
    out
}

/// The modules the relocation owes, from the registry itself rather than from a
/// second list that could disagree with it.
fn pending_relocation() -> BTreeSet<String> {
    CORE_ROOT_MODULES
        .iter()
        .filter(|(_, reason)| *reason == "pending-relocation")
        .map(|(module, _)| module.to_string())
        .collect()
}

/// The core's root modules are exactly the registered ones, and so are its root
/// directories. A new file fails; a new directory fails; a registered module
/// that is gone fails. There is no way to add a mechanism without editing this
/// list, which is the whole of what the gate is for.
#[test]
fn the_cores_root_modules_are_exactly_the_registered_ones() {
    let (modules, subdirs) = core_root_entries();
    let registered = registered_modules();

    let unregistered: Vec<&String> = modules.difference(&registered).collect();
    assert!(
        unregistered.is_empty(),
        "cog-core's source root has modules that are not registered: {unregistered:?} — the core \
         holds types and contracts; a module that does something belongs in the crate that owns \
         the capability, and one that is genuinely shared is registered here with a reason from \
         {MODULE_REASONS:?}"
    );

    let missing: Vec<&String> = registered.difference(&modules).collect();
    assert!(
        missing.is_empty(),
        "CORE_ROOT_MODULES registers modules the core no longer has: {missing:?} — the list is a \
         description of the tree, so a module that moved takes its row with it"
    );

    let registered_dirs: BTreeSet<String> = CORE_ROOT_DIRS.iter().map(|d| d.to_string()).collect();
    let undeclared_dirs: Vec<&String> = subdirs.difference(&registered_dirs).collect();
    assert!(
        undeclared_dirs.is_empty(),
        "cog-core's source root has directories that are not registered: {undeclared_dirs:?} — a \
         mechanism that arrives as a directory rather than a file would pass an assertion drawn \
         around `.rs` files, so the directories are registered too"
    );
    let missing_dirs: Vec<&String> = registered_dirs.difference(&subdirs).collect();
    assert!(
        missing_dirs.is_empty(),
        "CORE_ROOT_DIRS names directories the core no longer has: {missing_dirs:?}"
    );
}

/// The entry that lets a mechanism stay in the core is the entry that says where
/// it is going, asserted from both sides: a mechanism owed a move has to name a
/// destination, and a destination has to belong to a mechanism owed a move. The
/// list is a debt list and it empties itself — when the last mechanism leaves,
/// both halves are empty and this test has nothing left to check.
#[test]
fn every_mechanism_owed_a_move_names_where_it_is_going() {
    let ws = Workspace::read();
    let owed = pending_relocation();

    let mut destinations: BTreeMap<String, String> = BTreeMap::new();
    for (module, destination, _owed) in RELOCATION_TARGET {
        assert!(
            ws.members.contains(*destination),
            "RELOCATION_TARGET sends `{module}` to `{destination}`, which is not a workspace member"
        );
        assert_ne!(
            *destination, CORE,
            "RELOCATION_TARGET sends `{module}` to the core, which is where it already is — a \
             relocation whose destination is its source is not a plan"
        );
        assert!(
            owed.contains(*module),
            "RELOCATION_TARGET names `{module}` as a module to move, but CORE_ROOT_MODULES does \
             not register it as awaiting relocation — either it has moved and the row here is \
             stale, or the two lists have stopped agreeing about what is a mechanism"
        );
        destinations.insert(module.to_string(), destination.to_string());
    }
    // The other direction: a module registered as awaiting relocation has to
    // name where it is going, or the registration is a description of a problem
    // with no work attached to it.
    let unaddressed: Vec<&String> = owed
        .iter()
        .filter(|m| !destinations.contains_key(m.as_str()))
        .collect();
    assert!(
        unaddressed.is_empty(),
        "these modules are registered as awaiting relocation but name no destination: \
         {unaddressed:?} — a mechanism marked as owing a move is marked as owning a plan; if the \
         destination is genuinely undecided the module is still a mechanism and the decision is \
         still owed, but it is not this list's job to hold it undecided"
    );
}

/// The core's dependencies are the closed set, and no workspace crate is in it.
#[test]
fn cog_core_depends_only_on_the_registered_crates() {
    let ws = Workspace::read();
    let declared: BTreeSet<String> = CORE_DEPS.iter().map(|d| d.to_string()).collect();
    let normal = ws
        .core_deps
        .get(&Kind::Normal)
        .expect("cog-core has normal dependencies");

    let unregistered: Vec<&String> = normal.difference(&declared).collect();
    assert!(
        unregistered.is_empty(),
        "cog-core depends on crates outside the registered set: {unregistered:?} — every \
         dependency of the contract layer is a decision about what the contracts need, and a new \
         one is a decision a reviewer should see rather than a line in a manifest"
    );

    let unused: Vec<&String> = declared.difference(normal).collect();
    assert!(
        unused.is_empty(),
        "CORE_DEPS registers dependencies cog-core does not have: {unused:?} — the set describes \
         the tree, so dropping a dependency means dropping its row"
    );

    let workspace_crates: Vec<&String> = normal.iter().filter(|d| d.starts_with("cog-")).collect();
    assert!(
        workspace_crates.is_empty(),
        "cog-core depends on workspace crates: {workspace_crates:?} — the core is the bottom of \
         the graph, so an edge out of it is a cycle about to happen, and the crates that implement \
         its contracts are exactly the ones it must not name"
    );

    for kind in [Kind::Dev, Kind::Build] {
        let deps = ws.core_deps.get(&kind).cloned().unwrap_or_default();
        let workspace_crates: Vec<&String> =
            deps.iter().filter(|d| d.starts_with("cog-")).collect();
        assert!(
            workspace_crates.is_empty(),
            "cog-core reaches workspace crates from its {} dependencies: {workspace_crates:?}",
            kind.label()
        );
    }
}

/// A dependency marked for removal has to still be there to remove. When the
/// modules it was for have moved and the dependency is gone, this fails and the
/// entry has to leave the list — which is when the closed set in
/// [`CORE_DEPS`] takes over the ban.
#[test]
fn every_dependency_marked_for_removal_is_still_a_dependency() {
    let ws = Workspace::read();
    let normal = ws
        .core_deps
        .get(&Kind::Normal)
        .expect("cog-core has normal dependencies");
    for dep in DEPS_TO_DROP {
        assert!(
            CORE_DEPS.contains(dep),
            "DEPS_TO_DROP names `{dep}`, which is not in CORE_DEPS — the two lists have to agree, \
             or the closed set stops describing what the removal is going to leave behind"
        );
        assert!(
            normal.contains(*dep),
            "`{dep}` is listed as going away, but cog-core no longer depends on it — remove it \
             from DEPS_TO_DROP and CORE_DEPS, and the closed set will refuse to let it back in"
        );
    }
}

/// Whether `needle` occurs in `hay` as a whole word.
///
/// The consumer reading is deliberately generous — a crate that names a module
/// anywhere counts as a reader. Being generous costs a gate that fails against a
/// real dependency to add; being stingy costs a relocation that invents the very
/// edge this check exists to catch, and the second mistake is the one nothing
/// downstream would report.
fn names_module(hay: &str, needle: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    hay.match_indices(needle).any(|(at, _)| {
        let before = hay[..at].chars().next_back();
        let after = hay[at + needle.len()..].chars().next();
        before.is_none_or(|c| !ident(c)) && after.is_none_or(|c| !ident(c))
    })
}

/// Whether any `.rs` file under `dir` names `module`.
fn any_source_names(dir: &Path, module: &str) -> bool {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return false,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(_) => continue,
        };
        if file_type.is_dir() {
            if any_source_names(&path, module) {
                return true;
            }
        } else if path.extension().is_some_and(|e| e == "rs") {
            if let Ok(text) = std::fs::read_to_string(&path) {
                if names_module(&text, module) {
                    return true;
                }
            }
        }
    }
    false
}

/// The crates that read `module`, the core excluded, read from the sources
/// rather than from the registry.
///
/// The question here is not who is *allowed* to reach a module but who does, and
/// no manifest records that: an edge can be resolved and unused. The scan is of
/// `crates/*/src`, so a use that only exists in a crate's tests does not count —
/// a test can reach a dev-dependency the crate itself does not link, and counting
/// that would report a consumer that does not need the destination in the graph
/// the move is about.
///
/// Only members are read. A directory under `crates/` that no manifest names is
/// not in the dependency graph at all — nothing compiles it, so it has no edges
/// to check and a use inside it reaches nothing. It is a real defect of its own
/// kind, but it is not a reader, and reporting it here would say a relocation is
/// blocked when what has happened is that a file was written where nothing
/// builds.
fn consumers_of(root: &Path, members: &BTreeSet<String>, module: &str) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let crates = match std::fs::read_dir(root.join("crates")) {
        Ok(crates) => crates,
        Err(e) => panic!("the workspace's `crates` directory must be readable: {e}"),
    };
    for entry in crates.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name == CORE || !members.contains(&name) {
            continue;
        }
        if any_source_names(&entry.path().join("src"), module) {
            out.insert(name);
        }
    }
    out
}

/// The workspace root, from this crate's manifest directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// A row that says the readers already reach the destination is making a claim
/// about the dependency graph, and this reads the graph.
///
/// This is the half of the relocation plan that a registry cannot state: naming
/// a destination says where a module should end up, not whether it can go there.
/// A module read by a crate that the destination reaches — or read by a crate
/// that reaches nothing the destination has — cannot move without an edge, and
/// the worst of those is a cycle, which no amount of moving a file closes. The
/// rows that make this claim are exactly the ones checked; a `Split` row makes
/// the opposite claim, which is why the two shapes are told apart rather than
/// both being sent down this path.
#[test]
fn a_relocation_the_readers_already_reach_names_an_edge_that_is_there() {
    let ws = Workspace::read();
    let root = workspace_root();
    let mut unreachable = Vec::new();
    let mut checked = 0usize;
    for (module, destination, owed) in RELOCATION_TARGET {
        if *owed != Owed::Direct {
            continue;
        }
        for consumer in consumers_of(&root, &ws.members, module) {
            // A crate does not depend on itself, so the destination reading its
            // own module says nothing about whether the move invents an edge --
            // that half of the module leaves by not leaving.
            if consumer == *destination {
                continue;
            }
            checked += 1;
            let reaches = ws
                .member_deps
                .get(&consumer)
                .is_some_and(|deps| deps.contains(*destination));
            if !reaches {
                unreachable.push(format!(
                    "`{consumer}` reads `{module}`, which this row sends to `{destination}`, and \
                     `{consumer}` does not depend on `{destination}`"
                ));
            }
        }
    }
    assert!(
        unreachable.is_empty(),
        "these relocations are registered as needing no new edge and would invent one: \
         {unreachable:#?} — either the destination is wrong for the module, or the module is not \
         `direct` and leaves in halves"
    );
    assert!(
        checked > 0,
        "no `direct` row had a consumer outside the core, so this check read nothing: a row whose \
         reading is empty is a row that is not describing the tree"
    );
}

/// A mechanism leaving the core in halves keeps a contract half under
/// `contract/`, where the assertions drawn around the core's source root cannot
/// see it — the root's module set is registered, and a file inside a registered
/// directory is not part of it.
///
/// So the kernel calls are read directly. A contract half that reaches `/proc`,
/// spawns a process, calls into `libc` or takes a `flock` is not a contract: it
/// is the same mechanism under a second name, and nothing else in this file
/// would report it. The scan covers the whole contract directory rather than
/// only the halves the current rows imply, so a half lands checked instead of
/// checked once someone remembers to register it.
#[test]
fn the_cores_contract_layer_holds_no_mechanism() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(CONTRACT_DIR);
    let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| {
        panic!(
            "the contract directory {} must be readable: {e}",
            dir.display()
        )
    });
    let mut found = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for token in CONTRACT_FORBIDDEN {
            if text.contains(token) {
                found.push(format!("{} holds `{token}`", path.display()));
            }
        }
    }
    assert!(
        found.is_empty(),
        "the core's contract layer reaches the kernel: {found:#?} — a contract is an interface or \
         a shape, and a file here that reads `/proc`, spawns a process or takes a lock is a \
         mechanism that was renamed rather than moved"
    );
}
