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

/// Where each mechanism is going: `(module, destination crate)`.
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
const RELOCATION_TARGET: &[(&str, &str)] = &[
    ("admission", "cog-reflection"),
    ("build_cache_readings", "cog-reflection"),
    ("build_cache_reclaim", "cog-reflection"),
    ("build_gate", "cog-reflection"),
    ("loop_health", "cog-observability"),
    ("owner_lease", "cog-storage"),
];

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
        for p in packages {
            members.insert(
                p["name"]
                    .as_str()
                    .expect("a package has a name")
                    .to_string(),
            );
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
        Self { members, core_deps }
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
    for (module, destination) in RELOCATION_TARGET {
        assert!(
            ws.members.contains(*destination),
            "RELOCATION_TARGET sends `{module}` to `{destination}`, which is not a workspace member"
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
