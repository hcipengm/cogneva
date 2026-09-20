//! Plugins in the same topological layer are initialised concurrently, so a
//! plugin that consumes another plugin's service during its own `init` must
//! declare that dependency in `requires`. Otherwise the consume races with the
//! producer's init and silently degrades to a fallback — or, under
//! strict_persistence, fails the process at random.
//!
//! These tests pin the declarations that make init-time consumes safe. The
//! provided-service lists are read from the producers' real descriptors rather
//! than copied here: a hand-written copy cannot fail when the descriptor it
//! mirrors drifts, which is the very failure this test exists to catch.

/// (consumer plugin, producer plugin, services the consumer reads during init),
/// read off each consumer's `plugin.rs`.
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
    ("gateway", "storage", &["ExplainPool"]),
    ("gateway", "observability", &["ActiveAlertSource"]),
];

fn provided_by(producer: &str) -> &'static [&'static str] {
    match producer {
        "storage" => cog_storage::plugin::DESCRIPTOR.provides,
        "net" => cog_net::plugin::DESCRIPTOR.provides,
        "observability" => cog_observability::plugin::DESCRIPTOR.provides,
        other => panic!("{other} has no descriptor in this test"),
    }
}

#[test]
fn init_consumes_are_declared_and_ordered() {
    for (plugin, producer, consumed) in INIT_CONSUMES {
        let requires = match *plugin {
            "memory" => cog_memory::plugin::DESCRIPTOR.requires,
            "wiki" => cog_wiki::plugin::DESCRIPTOR.requires,
            "storage" => cog_storage::plugin::DESCRIPTOR.requires,
            "gateway" => cog_gateway::plugin::DESCRIPTOR.requires,
            other => panic!("{other} has no descriptor in this test"),
        };

        for service in *consumed {
            assert!(
                provided_by(producer).contains(service),
                "{service} is not listed as provided by {producer}; \
                 {plugin} reads it during init, so the declaration has drifted"
            );
            assert!(
                requires.contains(producer),
                "{plugin} consumes {service} during init but does not require {producer}; \
                 same-layer plugins init in parallel, so this is a race"
            );
        }
    }
}

/// A `provides` entry is a promise that some consumer can look the pin up. A
/// plugin that lists a pin it only ever consumes makes that promise to itself,
/// so the connectivity check passes and the `expect(...)` at the consume site
/// panics at startup instead. `all_descriptors()` comes from `build.rs`, so
/// this covers every registered crate without a list to keep in sync.
#[test]
fn declared_pins_are_honest() {
    let descriptors = cogneva::plugin_registry::all_descriptors();
    assert!(!descriptors.is_empty(), "no descriptors registered");

    for desc in descriptors {
        let consumed: Vec<&str> = desc.consumes.iter().map(|c| c.type_name).collect();
        for provided in desc.provides {
            assert!(
                !consumed.contains(provided),
                "{} declares '{}' in both provides and consumes; it is a re-export \
                 of a pin it also reads, so nothing guarantees a publisher exists",
                desc.name,
                provided
            );
        }

        for consume in desc.consumes.iter().filter(|c| c.required) {
            let foreign = descriptors
                .iter()
                .any(|d| d.name != desc.name && d.provides.contains(&consume.type_name));
            assert!(
                foreign,
                "{} require-consumes '{}' but no other plugin publishes it",
                desc.name, consume.type_name
            );
        }
    }
}
