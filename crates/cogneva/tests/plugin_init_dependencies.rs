//! Plugins in the same topological layer are initialised concurrently, so a
//! plugin that reads another plugin's service during its own `init` must depend
//! on that plugin in `requires`. Otherwise the read races the producer's init
//! and silently degrades to a fallback — or, under strict_persistence, fails the
//! process at random.
//!
//! Whether a read pin exists at all is deliberately *not* asserted here. The
//! only thing such an assertion could read is a hand-maintained `provides` list,
//! which agrees with itself rather than with the runtime. That half lives in
//! `PluginRunner::audit_pins`, which reports the pins the process really
//! published and read, by whom. These tests keep the ordering half, because init
//! order is not something the runtime can reconstruct after the fact.

use std::collections::{HashMap, HashSet};

/// (consumer plugin, producer plugin) for every edge a plugin's own `init`
/// depends on, read off each consumer's `plugin.rs`.
///
/// Only the two plugin names are data: they are what the assertion compares.
/// Which pins are read is a note for whoever reads a failure, kept as a comment
/// so it cannot be mistaken for a second vocabulary the runtime would have to
/// agree with — a name written here that drifted would change nothing but the
/// wording of a message.
const INIT_CONSUMES: &[(&str, &str)] = &[
    // ExplainPool, VectorBackend, ObjectBackend
    ("memory", "storage"),
    // ObjectBackend
    ("wiki", "storage"),
    // The object backend is chosen from `providers.storage.provider` at init;
    // the S3 path needs an HttpClient then, not on first request.
    // HttpClient
    ("storage", "net"),
    // ExplainPool
    ("gateway", "storage"),
    // ActiveAlertSource
    ("gateway", "observability"),
    // The agent pool binds each role's skill — its iteration budget above all —
    // when it is built inside init. Losing this edge to a soft dependency would
    // not fail: every role would quietly run on its configured seed, which is
    // the "wired but ineffective" state the binding exists to end.
    // SkillRegistry
    ("agent", "skill"),
];

/// Every plugin `name` transitively depends on.
fn requires_closure(name: &str) -> HashSet<&'static str> {
    let descriptors = cogneva::plugin_registry::all_descriptors();
    let edges: HashMap<&str, &[&'static str]> =
        descriptors.iter().map(|d| (d.name, d.requires)).collect();
    let mut seen: HashSet<&'static str> = HashSet::new();
    let mut queue: Vec<&str> = vec![name];
    while let Some(current) = queue.pop() {
        for &dep in edges.get(current).copied().unwrap_or(&[]) {
            if seen.insert(dep) {
                queue.push(dep);
            }
        }
    }
    seen
}

#[test]
fn init_consumes_are_ordered() {
    for (plugin, producer) in INIT_CONSUMES {
        assert!(
            requires_closure(plugin).contains(producer),
            "{plugin} reads from {producer} during init but does not depend on {producer}; \
             same-layer plugins init in parallel, so the read can race the publisher"
        );
    }
}

/// A dependency name that matches no registered plugin is only ever noticed when
/// the process fails to start. Catch it here instead.
#[test]
fn every_declared_dependency_exists() {
    let descriptors = cogneva::plugin_registry::all_descriptors();
    assert!(!descriptors.is_empty(), "no descriptors registered");
    let names: HashSet<&str> = descriptors.iter().map(|d| d.name).collect();

    for desc in descriptors {
        for target in desc.requires.iter().chain(desc.optional_requires) {
            assert!(
                names.contains(target),
                "{} depends on '{target}', which no registered plugin provides",
                desc.name
            );
        }
    }
}

/// `all_descriptors()` is generated from `[dependencies]`, so a crate listed
/// twice — or a name reused by another crate — would silently shadow one plugin.
#[test]
fn plugin_names_are_unique() {
    let descriptors = cogneva::plugin_registry::all_descriptors();
    let mut seen = HashSet::new();
    for desc in descriptors {
        assert!(
            seen.insert(desc.name),
            "plugin name '{}' is registered more than once",
            desc.name
        );
    }
}
