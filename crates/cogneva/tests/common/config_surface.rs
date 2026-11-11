//! 配置面的唯一读法：进程把哪张表读成哪些节、声明文档在哪里、每个节被类型读回后
//! 还产生哪些键。
//!
//! 两份判据共用这一份实现：
//!
//! * `config_section_reader_contract_test` 判「文档写的键有字段接着」；
//! * `every_config_knob_is_named_by_a_delivered_document_test` 判「类型产生的键
//!   至少在某一份交付文档里被写出来」——一个只活在代码默认值里的旋钮，运维在交付
//!   文档里看不见它，也就没有发现它的路径。
//!
//! 节的读回类型、声明文档的位置、以及「这个键背后有没有字段」的探针都只写在这里。
//! 分开抄两份时，两份对同一份文档会给出不同答案，而两份都能各自绿。

// 每个集成测试二进制都编译这一整个 `common`，但只有用到配置面的那两个读它：其余
// 二进制里这些格子没人构造，而它们不是死代码，是同一份实现被别人编进来。
#![allow(dead_code)]

use serde_json::{Map, Value};
use std::path::Path;

/// One section's reader: the type that decides what the section may contain,
/// and where the code spells the section out.
pub struct Reader {
    pub section: &'static str,
    /// `(file, text)`: read back from disk, the file must contain the text.
    /// `None` for a section the process's own config struct reads — there the
    /// type is the join, and the name is checked against that struct's keys.
    pub join: Option<(&'static str, &'static str)>,
    /// Keys inside this section that a *different* reader takes out of the
    /// document by pointer. The section's own type has no field for them, so
    /// without writing them down here the gate would report a knob that is read
    /// as one that is not — and the declaration is joined to the source the
    /// same way a section's is.
    pub nested: Vec<Reader>,
    /// Read a section back through the type that reads it: `Ok` with what the
    /// type keeps, `Err` with why a value does not fit it at all. A value that
    /// does not fit is a defect of its own — the reader either fails or falls
    /// back to its defaults, and either way the document reads as applied.
    read_back: fn(&Value) -> Result<Value, String>,
}

impl Reader {
    pub fn of<T>(section: &'static str, join: Option<(&'static str, &'static str)>) -> Self
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
    pub fn with_nested(mut self, reader: Reader) -> Self {
        self.nested.push(reader);
        self
    }

    /// Read a section back through the type that reads it.
    pub fn read_back(&self, value: &Value) -> Result<Value, String> {
        (self.read_back)(value)
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
    pub fn has_field(&self, section: &Map<String, Value>, key: &str) -> bool {
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
            self.read_back(&Value::Object(probed))
                .map(|read_back| read_back.get(key).is_some())
                .unwrap_or(false)
        })
    }
}

/// Every classified section and the type that reads it.
///
/// The list is hand-written because no type can be named generically, but it is
/// not free to fall behind: the gate compares it against the section table in
/// both directions, so a section with no entry fails there, and the entry's join
/// is read back out of the source it names.
pub fn readers() -> Vec<Reader> {
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
pub fn sections_the_process_config_carries() -> Vec<String> {
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
pub fn classified_sections() -> Vec<String> {
    let mut names: Vec<String> = cog_core::config_sections::CONFIG_SECTIONS
        .iter()
        .map(|s| s.name.to_string())
        .collect();
    names.sort();
    names
}

/// Counts for one document: sections read, and keys the type produces but the
/// document does not write.
#[derive(Default)]
pub struct Readings {
    pub sections: usize,
    pub undocumented: usize,
}

/// What the document writes in one section, and what the type produces there:
/// the two sets a knob-level judgement is made from.
pub struct SectionShape {
    /// `system` or `self_evolution.artifact_evolution`.
    pub path: String,
    /// Keys the document writes (including a nested section's key).
    pub written: std::collections::BTreeSet<String>,
    /// Keys the type produces when it reads this document's section.
    pub produced: std::collections::BTreeSet<String>,
}

/// Check one section-shaped value against the reader that takes it, and every
/// key inside it — including a key a nested reader takes by pointer.
pub fn check_section(file: &str, reader: &Reader, value: &Value, readings: &mut Readings) {
    let mut shapes = Vec::new();
    section_shapes(file, reader, value, reader.section, &mut shapes);
    readings.sections += shapes.len();
    readings.undocumented += shapes
        .iter()
        .map(|shape| shape.produced.difference(&shape.written).count())
        .sum::<usize>();
}

/// The written and produced key sets of `value` and every nested section under
/// it, with `prefix` as the accumulated path.
///
/// A key the document writes must have a field behind it: serde passes an
/// unknown key over in silence, so it reads as applied while nothing uses it.
/// That assertion lives here, in the one walk both gates share.
pub fn section_shapes(
    file: &str,
    reader: &Reader,
    value: &Value,
    prefix: &str,
    out: &mut Vec<SectionShape>,
) {
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
    let read_back = reader
        .read_back(value)
        .unwrap_or_else(|e| panic!("{file} `{}`: {e}", reader.section));
    let read_back = read_back
        .as_object()
        .expect("a section read back is an object");
    let mut written = std::collections::BTreeSet::new();
    for key in object.keys() {
        written.insert(key.clone());
        // `_comment` is prose the delivery comparator drops; it is not a knob,
        // so it is written by design rather than read by a field.
        if key.starts_with("_comment") {
            continue;
        }
        if let Some(nested) = reader.nested.iter().find(|n| n.section == key.as_str()) {
            section_shapes(
                file,
                nested,
                &object[key],
                &format!("{prefix}.{}", nested.section),
                out,
            );
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
    out.push(SectionShape {
        path: prefix.to_string(),
        written,
        produced: read_back.keys().cloned().collect(),
    });
}

/// The config where it is shipped: the chart's file, the static manifest the
/// cluster pulls, the rendered profiles, and the example template.
pub fn shipped_configs(root: &Path) -> Vec<(String, Value)> {
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
pub fn read_config(path: &Path) -> Value {
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
