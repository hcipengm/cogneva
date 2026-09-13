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

/// Services the net plugin publishes, in time for consumers.
const NET_PROVIDED: &[&str] = &["HttpClient"];

/// (consumer plugin, producer plugin, services consumed during init), read off
/// each consumer's `plugin.rs`.
const INIT_CONSUMES: &[(&str, &str, &[&str])] = &[
    (
        "memory",
        "storage",
        &["ExplainPool", "VectorBackend", "ObjectBackend"],
    ),
    ("wiki", "storage", &["ObjectBackend"]),
    // The object backend is chosen from `providers.storage.provider` at init;
    // the S3 path needs an HttpClient then, not on first request.
    ("storage", "net", &["HttpClient"]),
];

fn provided_by(producer: &str) -> &'static [&'static str] {
    match producer {
        "storage" => STORAGE_PROVIDED,
        "net" => NET_PROVIDED,
        other => panic!("{other} has no provided-service list in this test"),
    }
}

fn assert_declared(plugin: &str, requires: &[&str]) {
    let (_, producer, consumed) = INIT_CONSUMES
        .iter()
        .find(|(name, _, _)| *name == plugin)
        .unwrap_or_else(|| panic!("{plugin} is missing from INIT_CONSUMES"));

    for service in *consumed {
        assert!(
            provided_by(producer).contains(service),
            "{service} is not listed as provided by {producer}; this test is stale"
        );
        assert!(
            requires.contains(producer),
            "{plugin} consumes {service} during init but does not require {producer}; \
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

#[test]
fn storage_plugin_declares_net_dependency() {
    assert_declared("storage", cog_storage::plugin::DESCRIPTOR.requires);
}
