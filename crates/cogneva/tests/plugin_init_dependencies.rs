//! Plugins in the same topological layer are initialised concurrently, so a
//! plugin that consumes another plugin's service during its own `init` must
//! declare that dependency in `requires`. Otherwise the consume races with the
//! producer's init and silently degrades to a fallback — or, under
//! strict_persistence, fails the process at random.
//!
//! These tests pin the declarations that make init-time consumes safe.

/// Services storage publishes in its own `init`, in time for consumers.
const STORAGE_PROVIDED: &[&str] = &[
    "ExplainPool",
    "VectorBackend",
    "ObjectBackend",
    "MetricsBackend",
    "RawLogger",
    "StateBackend",
    "RawLogIndexStore",
    "CheckpointStore",
    "ObservabilityGateway",
    "HookArchive",
    "MediaBackend",
    "UserStore",
    "PlatformIdentityStore",
];

/// Services each plugin consumes during `init`, read off its `plugin.rs`.
const INIT_CONSUMES: &[(&str, &[&str])] = &[
    ("memory", &["ExplainPool", "VectorBackend", "ObjectBackend"]),
    ("wiki", &["ObjectBackend"]),
];

fn assert_declared(plugin: &str, requires: &[&str]) {
    let (_, consumed) = INIT_CONSUMES
        .iter()
        .find(|(name, _)| *name == plugin)
        .unwrap_or_else(|| panic!("{plugin} is missing from INIT_CONSUMES"));

    for service in *consumed {
        assert!(
            STORAGE_PROVIDED.contains(service),
            "{service} is not listed as storage-provided; this test is stale"
        );
        assert!(
            requires.contains(&"storage"),
            "{plugin} consumes {service} during init but does not require storage; \
             same-layer plugins init in parallel, so this is a race"
        );
    }
}

#[test]
fn memory_plugin_declares_storage_dependency() {
    assert_declared("memory", cog_memory::plugin::DESCRIPTOR.requires);
}

#[test]
fn wiki_plugin_declares_storage_dependency() {
    assert_declared("wiki", cog_wiki::plugin::DESCRIPTOR.requires);
}
