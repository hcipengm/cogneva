//! Every key the delivered document writes is read by something, and every
//! classified section has a reader.
//!
//! The section table (`cog_core::config_sections`) judges whole sections: a
//! section that arrives, one that is missing, one nobody classified. What it
//! cannot see is a key inside a section that no type has a field for — serde
//! passes such a key over without a word, so a knob the document sets reads as
//! applied while nothing uses it. `assert_sections_match` in cog-collaboration
//! decides that for the four sections that crate reads; every other section had
//! no such check, which is the gap `metrics` / `memory` / `system` drifted
//! through. This gate closes it for all of them, and it lives in the assembly
//! crate because that is the only place every section's type can be named.
//!
//! Two directions exist, and only one of them is asserted here:
//!
//! - A key the document writes must have a field behind it. Asserted: any
//!   occurrence is a defect, whichever section it is in.
//! - A field the type always produces must be written by the document. Not
//!   asserted for the sections outside cog-collaboration: `#[serde(default =
//!   "f")]` lets a field's value under omission differ from the same field in
//!   the type's `Default`, so writing every producible key into the templates
//!   is not a mechanical edit — it needs a per-field audit, and a template that
//!   carries a value the running process does not take is worse than one that
//!   lets the built-in default apply. The count is printed instead, so the
//!   backlog is a reading rather than an assumption.
//!
//! Neither direction reaches a key that has a field and is dropped after
//! parsing: `agent.heartbeat_interval_secs` had a field and was snapshotted
//! into a binding the code never used. That join is between the reader and its
//! consumer, and no type-level check can stand in for it.
//!
//! A section's reader is not always one type. `self_evolution`'s own struct has
//! no field for `artifact_evolution`; that key is read by cog-reflection, which
//! seeks it out by pointer. Such a key is declared here as a reader of its own,
//! joined to the pointer's spelling in the source, so "a key no type has a
//! field for" stays a finding rather than becoming an allowance.

use serde_json::{Map, Value};
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// One section's reader: the type that decides what the section may contain,
/// and where the code spells the section out.
struct Reader {
    section: &'static str,
    /// `(file, text)`: read back from disk, the file must contain the text.
    /// `None` for a section the process's own config struct reads — there the
    /// type is the join, and the name is checked against that struct's keys.
    join: Option<(&'static str, &'static str)>,
    /// Keys inside this section that a *different* reader takes out of the
    /// document by pointer. The section's own type has no field for them, so
    /// without writing them down here the gate would report a knob that is read
    /// as one that is not — and the declaration is joined to the source the
    /// same way a section's is.
    nested: Vec<Reader>,
    /// Read a section back through the type that reads it: `Ok` with what the
    /// type keeps, `Err` with why a value does not fit it at all. A value that
    /// does not fit is a defect of its own — the reader either fails or falls
    /// back to its defaults, and either way the document reads as applied.
    read_back: fn(&Value) -> Result<Value, String>,
}

impl Reader {
    fn of<T>(section: &'static str, join: Option<(&'static str, &'static str)>) -> Self
    where
        T: serde::de::DeserializeOwned + serde::Serialize,
    {
        Self {
            section,
            join,
            nested: Vec::new(),
            read_back: |value| {
                let typed: T = serde_json::from_value(value.clone()).map_err(|e| {
                    format!(
                        "the value does not fit {}, the type that reads it: {e}",
                        std::any::type_name::<T>()
                    )
                })?;
                Ok(serde_json::to_value(typed).expect("a section read back serializes"))
            },
        }
    }

    /// Declare a key inside this section that another reader takes by pointer.
    fn with_nested(mut self, reader: Reader) -> Self {
        self.nested.push(reader);
        self
    }

    /// Whether the type demonstrably has a field at `key`, proved by handing it
    /// a value and reading it back.
    ///
    /// A section may write an optional key as `null`, and a null is dropped on
    /// the way back out exactly like a key no field stands behind — so the
    /// round trip alone cannot tell an unset knob from a typo'd one. A probe
    /// value can: a real field keeps it, an unknown key drops every one. A
    /// probe whose shape the type does not take is not evidence either way, so
    /// it is skipped rather than read as absence.
    ///
    /// The list carries non-empty containers as well as empty ones: a field
    /// skipped when it is empty (`skip_serializing_if = "Vec::is_empty"`) keeps
    /// the key for `["probe"]` and drops it for `[]`, and the empty probe alone
    /// would report a field that is read as one that does not exist.
    fn has_field(&self, section: &Map<String, Value>, key: &str) -> bool {
        let probes = [
            serde_json::json!("probe"),
            serde_json::json!(1),
            serde_json::json!(true),
            serde_json::json!([]),
            serde_json::json!({}),
            serde_json::json!(["probe"]),
            serde_json::json!({"probe": 1}),
        ];
        probes.iter().any(|probe| {
            let mut probed = section.clone();
            probed.insert(key.to_string(), probe.clone());
            (self.read_back)(&Value::Object(probed))
                .map(|read_back| read_back.get(key).is_some())
                .unwrap_or(false)
        })
    }
}

/// Every classified section and the type that reads it.
///
/// The list is hand-written because no type can be named generically, but it is
/// not free to fall behind: the gate compares it against the section table in
/// both directions, so a section with no entry fails here, and the entry's join
/// is read back out of the source it names.
fn readers() -> Vec<Reader> {
    vec![
        // Carried by `cog_core::Config`, the struct the process loads the
        // document into and the plugins read (`AppConfig` flattens it), so the
        // field itself is the join and there is no second spelling to check.
        Reader::of::<cog_core::config::AppInfo>("app", None),
        Reader::of::<cog_core::config::AgentConfig>("agent", None),
        Reader::of::<cog_core::config::DagExecutorConfig>("dag_executor", None),
        Reader::of::<cog_core::config::GatewayConfig>("gateway", None),
        Reader::of::<cog_core::config::HookEngineConfig>("hook_engine", None),
        Reader::of::<cog_core::config::MetricsConfig>("metrics", None),
        Reader::of::<cog_core::config::MultiBackendConsumerConfig>("multi_backend_consumer", None),
        Reader::of::<cog_core::config::ProviderConfigs>("providers", None),
        Reader::of::<cog_core::contract::storage::RawLoggerConfig>("raw_logger", None),
        Reader::of::<cog_core::config::SelfEvolutionConfig>("self_evolution", None).with_nested(
            Reader::of::<cog_reflection::PolicyEvolutionConfig>(
                "artifact_evolution",
                Some((
                    "crates/cog-reflection/src/policy_evolution.rs",
                    "pointer(\"/self_evolution/artifact_evolution\")",
                )),
            ),
        ),
        Reader::of::<cog_core::config::SupervisorConfig>("supervisor", None),
        Reader::of::<cog_core::config::SystemConfig>("system", None),
        Reader::of::<cog_core::config::TierMigratorConfig>("tier_migrator", None),
        // The env supersede map: the loader merges each `PATH=dot.path` pair
        // over the document, so every value in it is moved as a string.
        Reader::of::<std::collections::HashMap<String, String>>("env", None),
        // Read by a crate's own loader, which spells the pointer as a literal.
        Reader::of::<cog_llm::config::LLMRoutingConfig>(
            "llm_routing",
            Some((
                "crates/cog-llm/src/config.rs",
                "load_section(\"/llm_routing\"",
            )),
        ),
        Reader::of::<cog_llm::config::TuningConfig>(
            "tuning",
            Some(("crates/cog-llm/src/config.rs", "load_section(\"/tuning\"")),
        ),
        Reader::of::<cog_agent::config::AgentLoopConfig>(
            "agent_loop",
            Some((
                "crates/cog-agent/src/config.rs",
                "load_section(\"/agent_loop\"",
            )),
        ),
        Reader::of::<cog_agent::config::AgentManagerConfig>(
            "agent_pool",
            Some((
                "crates/cog-agent/src/config.rs",
                "load_section(\"/agent_pool\"",
            )),
        ),
        Reader::of::<cog_collaboration::config::BoundaryConfig>(
            "boundary",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/boundary\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::PgeSettings>(
            "pge",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/pge\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::RalphSettings>(
            "ralph",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/ralph\")",
            )),
        ),
        Reader::of::<cog_collaboration::config::SelfReviewSettings>(
            "self_review",
            Some((
                "crates/cog-collaboration/src/config.rs",
                "load_section(\"/self_review\")",
            )),
        ),
        // Read by seeking the value out of the document by JSON pointer.
        Reader::of::<cog_github::config::GitHubIntegrationConfig>(
            "github_integration",
            Some((
                "crates/cog-github/src/config.rs",
                "pointer(\"/github_integration\")",
            )),
        ),
        Reader::of::<cog_github::config::GiteeIntegrationConfig>(
            "gitee_integration",
            Some((
                "crates/cog-github/src/config.rs",
                "pointer(\"/gitee_integration\")",
            )),
        ),
        Reader::of::<cog_memory::config::MemoryConfig>(
            "memory",
            Some(("crates/cog-memory/src/config.rs", "pointer(\"/memory\")")),
        ),
        Reader::of::<cog_prompt::config::PromptConfig>(
            "prompts",
            Some(("crates/cog-prompt/src/config.rs", "pointer(\"/prompts\")")),
        ),
        Reader::of::<cog_observability::config::ObservabilityExportersConfig>(
            "observability",
            Some((
                "crates/cog-observability/src/config.rs",
                "pointer(\"/observability\")",
            )),
        ),
        // Not read by anything, by design: the delivery comparator drops every
        // key starting with `_comment` at any depth, which is what makes a
        // template self-describing without changing what a process reads.
        Reader::of::<Map<String, Value>>(
            "_comment",
            Some((
                "crates/cog-core/src/config_sections.rs",
                "!key.starts_with(\"_comment\")",
            )),
        ),
    ]
}

/// The sections `cog_core::Config` carries, read out of the type rather than
/// listed here a second time.
fn sections_the_process_config_carries() -> Vec<String> {
    let value =
        serde_json::to_value(cog_core::Config::default()).expect("the process config serializes");
    let mut names: Vec<String> = value
        .as_object()
        .expect("the process config is an object")
        .keys()
        .cloned()
        .collect();
    names.sort();
    names
}

/// Every section the section table classifies.
fn classified_sections() -> Vec<String> {
    let mut names: Vec<String> = cog_core::config_sections::CONFIG_SECTIONS
        .iter()
        .map(|s| s.name.to_string())
        .collect();
    names.sort();
    names
}

/// A section's name is classified, and the reader for it is declared here.
///
/// This is the direction that catches a section nobody reads at all: the table
/// says how a section reaches a process, and a section whose reader was never
/// written reaches nothing while every check that only compares documents
/// passes.
#[test]
fn every_classified_section_has_a_reader() {
    let mut declared: Vec<String> = readers().iter().map(|r| r.section.to_string()).collect();
    declared.sort();
    let mut classified = classified_sections();
    classified.sort();
    assert_eq!(
        classified, declared,
        "the sections the document is classified into and the sections this gate \
         knows a reader for disagree: a section listed here with no reader is \
         written by every deployment and read by nothing"
    );

    // A reader that claims no source spelling has to be a field of the struct
    // the process loads, or the claim stands on nothing.
    let carried = sections_the_process_config_carries();
    for reader in readers() {
        if reader.join.is_none() && reader.section != "env" {
            assert!(
                carried.contains(&reader.section.to_string()),
                "`{}` is declared as a field of the process config, and that struct has no \
                 such key: the section reaches the process through nothing",
                reader.section
            );
        }
    }
}

/// Every reader still spells the section out where it says it does.
///
/// A join written down once can outlive the code it names — the pointer reader
/// deleted, the gate still reporting the section as read. Reading the spelling
/// back out of the source is what keeps the declaration tied to a reader that
/// exists.
#[test]
fn every_reader_spells_the_section_out_where_it_says_it_does() {
    let root = repo_root();
    for reader in readers() {
        let Some((file, text)) = reader.join else {
            continue;
        };
        let path = root.join(file);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        assert!(
            source.contains(text),
            "{} no longer spells `{}` out as `{text}`: the section is declared as read \
             by a reader that is not there",
            path.display(),
            reader.section
        );
    }
}

/// Every key a shipped document writes has a field behind it.
///
/// serde passes over a key with no field behind it without a word, so an entry
/// nothing reads looks exactly like one that was applied — that is the
/// direction a typo hides in, and it is the same defect one level down from a
/// section nobody classified.
#[test]
fn every_key_a_shipped_document_writes_is_read_by_something() {
    let root = repo_root();
    let mut readings = Readings::default();
    let mut nested_seen: Vec<&str> = Vec::new();
    let mut omitted: Vec<String> = Vec::new();
    let mut docs = 0usize;
    for (file, doc) in shipped_configs(&root) {
        docs += 1;
        // Sections the document leaves out. Not asserted: a document may be a
        // sample rather than a declaration, and writing every section into a
        // sample would make it a second carrier of every default. The documents
        // the cluster receives are held to completeness elsewhere — the binary
        // refuses to ship a declaration the table does not classify in full,
        // and the embedders are held identical to the chart's. Printed so a
        // section that goes missing from a sample is a reading rather than a
        // silence.
        let missing = cog_core::config_sections::sections_missing_from_document(&doc);
        if !missing.is_empty() {
            omitted.push(format!("{file} leaves out {}", missing.join(", ")));
        }
        for reader in readers() {
            let Some(section) = doc.get(reader.section) else {
                continue;
            };
            nested_seen.extend(
                reader
                    .nested
                    .iter()
                    .filter(|n| section.get(n.section).is_some())
                    .map(|n| n.section),
            );
            check_section(&file, &reader, section, &mut readings);
        }
    }
    assert!(docs >= 5, "only {docs} shipped configs were found");
    // A nested key declared as read by pointer has to be written by something,
    // or the declaration names a reader for a knob no document carries.
    for reader in readers() {
        for nested in &reader.nested {
            assert!(
                nested_seen.contains(&nested.section),
                "`{}` is declared as read by pointer from `{}`, and no shipped document \
                 writes it",
                nested.section,
                reader.section
            );
        }
    }
    println!(
        "config surface: {docs} documents, {} sections, {} keys a section type produces \
         and its document does not write (reading only — see the note at the top of this \
         file)",
        readings.sections, readings.undocumented
    );
    for line in &omitted {
        println!("config surface: {line}");
    }
}

#[derive(Default)]
struct Readings {
    sections: usize,
    undocumented: usize,
}

/// Check one section-shaped value against the reader that takes it, and every
/// key inside it — including a key a nested reader takes by pointer.
fn check_section(file: &str, reader: &Reader, value: &Value, readings: &mut Readings) {
    let Value::Object(object) = value else {
        // Only the top-level `_comment` is not an object: it is one line of
        // prose the comparator drops, so its shape is its own business.
        assert_eq!(
            reader.section, "_comment",
            "{file} `{}` is not an object, and no reader can read it",
            reader.section
        );
        return;
    };
    readings.sections += 1;
    let read_back =
        (reader.read_back)(value).unwrap_or_else(|e| panic!("{file} `{}`: {e}", reader.section));
    let read_back = read_back
        .as_object()
        .expect("a section read back is an object");
    for key in object.keys().filter(|k| !k.starts_with("_comment")) {
        if let Some(nested) = reader.nested.iter().find(|n| n.section == key.as_str()) {
            check_section(file, nested, &object[key], readings);
            continue;
        }
        if read_back.contains_key(key) || reader.has_field(object, key) {
            continue;
        }
        panic!(
            "{file} `{}.{key}` has no field behind it: serde passes it over in silence, \
             so it reads as applied while nothing uses it",
            reader.section
        );
    }
    // The other direction, counted rather than asserted: keys the type always
    // produces that this document does not write.
    readings.undocumented += read_back
        .keys()
        .filter(|k| !object.contains_key(*k))
        .count();
}

/// The config where it is shipped: the chart's file, the static manifest the
/// cluster pulls, the rendered profiles, and the example template.
fn shipped_configs(root: &Path) -> Vec<(String, Value)> {
    let mut out = vec![(
        "cogneva.example.json".to_string(),
        read_config(&root.join("cogneva.example.json")),
    )];
    for relative in [
        "deploy/helm/cogneva/files/cogneva.json",
        "deploy/k3s/cogneva-json-configmap.yaml",
    ] {
        out.push((relative.to_string(), read_config(&root.join(relative))));
    }
    let rendered = root.join("deploy/rendered");
    let mut profiles: Vec<_> = std::fs::read_dir(&rendered)
        .unwrap_or_else(|e| panic!("read {}: {e}", rendered.display()))
        .map(|e| e.expect("readable dir entry").path())
        .filter(|p| p.is_dir())
        .collect();
    profiles.sort();
    for dir in profiles {
        let file = dir.join("10-configmap-cogneva-json.yaml");
        out.push((file.display().to_string(), read_config(&file)));
    }
    out
}

/// Read the config wherever it is shipped: the chart and the example keep it as
/// a file of its own, a ConfigMap manifest keeps it as a block scalar.
fn read_config(path: &Path) -> Value {
    match path.extension().and_then(|e| e.to_str()) {
        Some("json") => read_json(path),
        Some("yaml") => read_config_manifest(path),
        other => panic!("{} has no known config format ({other:?})", path.display()),
    }
}

fn read_json(path: &Path) -> Value {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()))
}

/// Pull the config back out of a ConfigMap manifest.
///
/// The payload is a YAML block scalar indented under its key, so it is
/// de-indented and parsed as the JSON it is. A manifest that stops carrying the
/// key, or carries it empty, fails here rather than passing quietly.
fn read_config_manifest(path: &Path) -> Value {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let lines: Vec<&str> = raw.lines().collect();
    let start = lines
        .iter()
        .position(|l| l.starts_with("  ") && l.trim_start().starts_with("cogneva.json: |"))
        .unwrap_or_else(|| panic!("{} embeds no cogneva.json block", path.display()));
    let mut payload = String::new();
    for line in &lines[start + 1..] {
        match line.strip_prefix("    ") {
            Some(rest) => {
                payload.push_str(rest);
                payload.push('\n');
            }
            None if line.trim().is_empty() => payload.push('\n'),
            None => break,
        }
    }
    serde_json::from_str(&payload)
        .unwrap_or_else(|e| panic!("{} embeds an unparseable config: {e}", path.display()))
}
