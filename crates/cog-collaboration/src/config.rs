//! Collaboration 自有配置段（self_review / pge / boundary）——core
//! config.rs 不聚合单 crate 配置。自读 cogneva.json
//! 对应段，无 env 映射的段保持 JSON 驱动。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use cog_core::{SFError, SFResult};

fn load_section<T: serde::de::DeserializeOwned + Default>(pointer: &str) -> SFResult<T> {
    let path =
        std::env::var("COGNEVA_CONFIG_PATH").unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
    load_section_from(Path::new(&path), pointer)
}

fn load_section_from<T: serde::de::DeserializeOwned + Default>(
    path: &Path,
    pointer: &str,
) -> SFResult<T> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let root: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
            match root.pointer(pointer) {
                Some(section) => serde_json::from_value(section.clone())
                    .map_err(|e| SFError::Config(format!("{} {pointer}: {e}", path.display()))),
                None => Ok(T::default()),
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(T::default()),
        Err(e) => Err(SFError::Config(format!("{}: {e}", path.display()))),
    }
}

/// Self-review quality gate configuration for PGE actors.
/// Disabled by default so existing behavior is unchanged unless opted in.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SelfReviewSettings {
    pub enabled: bool,
    pub max_iterations: u32,
    pub quality_threshold: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
    pub best_practices: Vec<String>,
}

impl Default for SelfReviewSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            max_iterations: 2,
            quality_threshold: 0.8,
            spec: None,
            best_practices: Vec::new(),
        }
    }
}

impl SelfReviewSettings {
    pub fn load() -> SFResult<Self> {
        load_section("/self_review")
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/self_review")
    }

    /// Convert to the runtime [`cog_core::SelfReviewConfig`] when enabled.
    pub fn to_config(&self) -> Option<cog_core::SelfReviewConfig> {
        if !self.enabled {
            return None;
        }
        Some(cog_core::SelfReviewConfig {
            max_iterations: self.max_iterations,
            quality_threshold: self.quality_threshold,
            spec: self.spec.clone(),
            best_practices: self.best_practices.clone(),
        })
    }
}

/// PGE pipeline configuration: optional JSON Schemas constraining actor
/// outputs, plus the local-repair budget.
///
/// When a schema is configured for an actor (keyed by actor name:
/// "planner", "generator", "evaluator", "moderator", "merger"), the actor
/// injects it into the prompt context as `output_schema` and validates the
/// raw LLM output against it. Validation failures are logged and the legacy
/// lenient parsing still applies, so a bad schema can never break the
/// pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PgeSettings {
    pub schemas: HashMap<String, serde_json::Value>,
    /// Local-repair budget: how many times, after the evaluator judges a
    /// generation a failure, its feedback is handed back to the generator
    /// with the plan held fixed. Only when this runs out does the run
    /// escalate to a global reset, which re-runs the planner. Zero turns the
    /// path off, and then every failure pays for a fresh plan even when the
    /// failure lies in a generation the plan already described.
    pub local_repair_max: u32,
}

/// The repair budget a run gets when nothing sets one. Two, not one and not
/// unbounded: the first repair is what lets feedback be acted on at all, and
/// the second absorbs a first rewrite that is itself rejected. Past that the
/// feedback is unchanged between iterations, so repeating it cannot buy a
/// different outcome — either the plan is the real defect, which is the global
/// reset's job, or nothing in the feedback is actionable.
pub const DEFAULT_LOCAL_REPAIR_MAX: u32 = 2;

/// A zero here would close the repair loop for every deployment that does not
/// set one, which is the state this constant exists to leave behind, so it is
/// refused where it is written rather than in a test.
const _: () = assert!(DEFAULT_LOCAL_REPAIR_MAX > 0);

impl Default for PgeSettings {
    fn default() -> Self {
        Self {
            schemas: HashMap::new(),
            local_repair_max: DEFAULT_LOCAL_REPAIR_MAX,
        }
    }
}

impl PgeSettings {
    pub fn load() -> SFResult<Self> {
        load_section("/pge")
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/pge")
    }

    /// Return the configured schema for `actor`, if any.
    pub fn schema_for(&self, actor: &str) -> Option<serde_json::Value> {
        self.schemas.get(actor).cloned()
    }
}

/// Ralph Loop 预算与停滞窗口配置。不收敛的链必须在预算内终止——
/// 无人值守场景没有操作者盯流调 prompt，"视为无限"等于无限烧 token。
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct RalphSettings {
    /// 单次执行的迭代预算硬上限。预算按次计，不跨执行累计：同一个目标被
    /// 重新调度时重新起步，已归档的历史只提供反馈与停滞证据。
    pub max_iterations: u32,
    /// 停滞窗口：最近这么多轮不买进展即终止——归一化反馈逐字相同（同一失败
    /// 原样重放），或分数未升且产物未增长的改写重放（读数不足两个不下结论）。
    /// 这个值同时决定归档历史保留的尾部长度。0 = 关闭停滞检测。
    pub stagnation_window: u32,
}

impl Default for RalphSettings {
    fn default() -> Self {
        Self {
            max_iterations: 50,
            stagnation_window: 5,
        }
    }
}

impl RalphSettings {
    pub fn load() -> SFResult<Self> {
        load_section("/ralph")
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/ralph")
    }

    /// Convert to the loop-level config struct.
    pub fn to_loop_config(&self) -> crate::squad::ralph::RalphLoopConfig {
        crate::squad::ralph::RalphLoopConfig {
            max_iterations: self.max_iterations,
            stagnation_window: self.stagnation_window,
        }
    }
}

/// Boundary rule configuration（cog-collaboration 注入 Evaluator 做动态
/// 边界维度评估）。规则元素类型 [`cog_core::BoundaryRule`] 是跨 crate
/// 数据契约，留在 core。
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct BoundaryConfig {
    #[serde(default)]
    pub rules: Vec<cog_core::BoundaryRule>,
}

impl BoundaryConfig {
    pub fn load() -> SFResult<Self> {
        load_section("/boundary")
    }

    pub fn load_from(path: &Path) -> SFResult<Self> {
        load_section_from(path, "/boundary")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boundary_config_default_has_no_rules() {
        assert!(BoundaryConfig::default().rules.is_empty());
    }

    #[test]
    fn missing_file_returns_defaults() {
        let p = Path::new("/nonexistent/cogneva.json");
        assert!(!SelfReviewSettings::load_from(p).unwrap().enabled);
        assert!(PgeSettings::load_from(p).unwrap().schemas.is_empty());
        assert!(BoundaryConfig::load_from(p).unwrap().rules.is_empty());
    }

    #[test]
    fn a_pge_section_without_the_repair_budget_gets_the_policy_default() {
        let dir = std::env::temp_dir().join(format!("cog-collab-pge-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        // The shipped deployments write the `pge` section for its schemas, so a
        // budget that is absent there must still leave the repair loop open:
        // reading a missing key as 0 is what made the loop unreachable.
        std::fs::write(&path, r#"{"pge": {"schemas": {}}}"#).unwrap();
        assert_eq!(
            PgeSettings::load_from(&path).unwrap().local_repair_max,
            DEFAULT_LOCAL_REPAIR_MAX
        );
        // An explicit 0 still means off — the default fills absences, it does
        // not overrule the deployment.
        std::fs::write(&path, r#"{"pge": {"local_repair_max": 0}}"#).unwrap();
        assert_eq!(PgeSettings::load_from(&path).unwrap().local_repair_max, 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A configuration section this crate reads: the pointer a template has to
    /// write it under, and the type that decides what the section may contain.
    struct Section {
        pointer: &'static str,
        /// Read a copy back through the type and serialize it again. The
        /// comparison is against *that* rather than against a field list
        /// written here, so a field added to the type joins the check without
        /// anyone remembering to write it down twice.
        read_back: fn(&serde_json::Value) -> serde_json::Value,
        /// What the type produces when nothing sets anything. Every key here is
        /// one a deployment has to be able to see and change.
        defaults: fn() -> serde_json::Value,
    }

    impl Section {
        fn of<T>(pointer: &'static str) -> Self
        where
            T: serde::de::DeserializeOwned + serde::Serialize + Default,
        {
            Self {
                pointer,
                read_back: |value| {
                    let typed: T = serde_json::from_value(value.clone())
                        .unwrap_or_else(|e| panic!("a section does not fit its type: {e}"));
                    serde_json::to_value(typed).expect("a section read back serializes")
                },
                defaults: || serde_json::to_value(T::default()).expect("a default serializes"),
            }
        }

        fn name(&self) -> &'static str {
            self.pointer.trim_start_matches('/')
        }

        /// Whether the type demonstrably has a field at `key`, proved by
        /// handing it a value and reading it back.
        ///
        /// Needed because a section may write an optional key as `null`, and a
        /// null is dropped on the way back out exactly like a key that no field
        /// stands behind — so the round trip alone cannot tell an unset knob
        /// from a typo'd one. A probe value can: a real field keeps it, an
        /// unknown key drops every one of them.
        fn has_field(
            &self,
            section: &serde_json::Map<String, serde_json::Value>,
            key: &str,
        ) -> bool {
            let probes = [
                serde_json::json!("probe"),
                serde_json::json!(1),
                serde_json::json!(true),
                serde_json::json!([]),
                serde_json::json!({}),
            ];
            probes.iter().any(|probe| {
                let mut probed = section.clone();
                probed.insert(key.to_string(), probe.clone());
                let read_back = (self.read_back)(&serde_json::Value::Object(probed));
                read_back.get(key).is_some()
            })
        }
    }

    /// Every section this crate reads.
    fn sections() -> Vec<Section> {
        vec![
            Section::of::<BoundaryConfig>("/boundary"),
            Section::of::<PgeSettings>("/pge"),
            Section::of::<RalphSettings>("/ralph"),
            Section::of::<SelfReviewSettings>("/self_review"),
        ]
    }

    /// The pointers the loader can actually be asked for, read out of this
    /// crate's source instead of listed here a second time.
    ///
    /// This function is the point of the gate. `ralph` was read by the code and
    /// written by no template, and nothing compared the two — so the list of
    /// things to compare cannot be another hand-written list, which would let
    /// the next section escape the same way.
    fn pointers_the_loader_can_be_asked_for() -> Vec<String> {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut out = Vec::new();
        let mut dirs = vec![src];
        while let Some(dir) = dirs.pop() {
            for entry in
                std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
            {
                let path = entry.expect("readable dir entry").path();
                if path.is_dir() {
                    dirs.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
                for line in text.lines() {
                    let call = line.trim_start();
                    let args = call
                        .strip_prefix("load_section_from(")
                        .or_else(|| call.strip_prefix("load_section("));
                    let Some(args) = args else { continue };
                    // Both call shapes put the pointer in the first quoted
                    // string: the plain one takes it as its only argument, the
                    // `_from` one as its second, after the path.
                    if let Some(pointer) = args.split('"').nth(1) {
                        out.push(pointer.to_string());
                    }
                }
            }
        }
        out.sort();
        out.dedup();
        out
    }

    /// The config a deployment ships, wherever it is embedded.
    ///
    /// Three places embed it: the chart's file, the static manifest the cluster
    /// pulls, and the rendered profiles. They have to agree, so the comparison
    /// is over all of them rather than over the one that happens to be edited.
    fn shipped_configs(root: &Path) -> Vec<(String, serde_json::Value)> {
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

    /// Read the config wherever it is shipped: the chart and the example keep it
    /// as a file of its own, a ConfigMap manifest keeps it as a block scalar.
    fn read_config(path: &Path) -> serde_json::Value {
        match path.extension().and_then(|e| e.to_str()) {
            Some("json") => read_json(path),
            Some("yaml") => read_config_manifest(path),
            other => panic!("{} has no known config format ({other:?})", path.display()),
        }
    }

    fn read_json(path: &Path) -> serde_json::Value {
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()))
    }

    /// Pull the config back out of a ConfigMap manifest.
    ///
    /// The payload is a YAML block scalar indented under its key, so it is
    /// de-indented and parsed as the JSON it is. A manifest that stops carrying
    /// the key, or carries it empty, fails here rather than passing quietly.
    fn read_config_manifest(path: &Path) -> serde_json::Value {
        let raw = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
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

    /// Every section the loader can be asked for is declared, and every
    /// declared section is asked for somewhere. A new section read by the code
    /// but missing from `sections()` would go unchecked, which is the state
    /// `ralph` was in.
    #[test]
    fn every_section_the_loader_reads_is_declared() {
        let mut declared: Vec<String> = sections().iter().map(|s| s.pointer.to_string()).collect();
        declared.sort();
        assert_eq!(
            pointers_the_loader_can_be_asked_for(),
            declared,
            "the sections this crate loads and the sections the template check covers disagree"
        );
    }

    /// A shipped template is a claim about each section's type, and both
    /// directions of that claim have to hold.
    ///
    /// serde passes over a key with no field behind it without a word, so an
    /// entry nothing reads looks exactly like one that was applied — that is
    /// the direction a typo hides in. A field read but never written ships a
    /// value the operator can neither see nor change, and a section the code
    /// reads but no template writes is the same defect one level up: the knob
    /// is invisible, and only its built-in default can apply.
    fn assert_sections_match(file: &str, doc: &serde_json::Value) {
        for section in sections() {
            let name = section.name();
            let value = doc.get(name).unwrap_or_else(|| {
                panic!(
                    "{file} writes no `{name}` section, which the code reads: every \
                     deployment silently takes the built-in default"
                )
            });
            let object = value
                .as_object()
                .unwrap_or_else(|| panic!("{file} `{name}` is not an object"));
            let written: Vec<&String> = object.keys().filter(|k| !k.starts_with('_')).collect();

            // Every key the type produces on its own has to be visible in the
            // template, or the deployment has a value no one can read or set.
            let defaults = (section.defaults)();
            for key in defaults.as_object().expect("defaults are an object").keys() {
                assert!(
                    written.contains(&key),
                    "{file} `{name}` does not write `{key}`, which the type always \
                     produces: the deployment cannot set what the code reads"
                );
            }

            // Every key the template writes has to be one the type knows. The
            // round trip answers it for anything with a value in it; a key
            // sitting at `null` is dropped either way, so it is decided by
            // probing instead.
            let read_back = (section.read_back)(value);
            let read_back_keys = read_back
                .as_object()
                .expect("a section read back is an object");
            for key in &written {
                if read_back_keys.contains_key(*key) || section.has_field(object, key) {
                    continue;
                }
                panic!(
                    "{file} `{name}.{key}` has no field behind it: serde passes it \
                     over in silence, so it reads as applied while nothing uses it"
                );
            }
        }
    }

    #[test]
    fn shipped_config_templates_track_every_section_surface() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut seen = 0;
        for (file, doc) in shipped_configs(&root) {
            assert_sections_match(&file, &doc);
            seen += 1;
        }
        assert!(seen >= 5, "only {seen} shipped configs were found");
    }

    /// The three embedders of the config ship one and the same document. The
    /// rendered profiles and the static manifest are separate files from the
    /// chart's, and only the chart's is edited by hand, so a change that misses
    /// one of them would otherwise reach the cluster as two different configs
    /// with no reading saying which one a pod got.
    #[test]
    fn the_embedders_ship_one_and_the_same_config() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut configs = shipped_configs(&root)
            .into_iter()
            .filter(|(file, _)| !file.ends_with("cogneva.example.json"));
        let (reference_file, reference) = configs.next().expect("a chart source config");
        for (file, doc) in configs {
            assert_eq!(
                doc, reference,
                "{file} ships a different config than {reference_file}"
            );
        }
    }

    #[test]
    fn ralph_settings_defaults_bound_the_loop() {
        let p = Path::new("/nonexistent/cogneva.json");
        let r = RalphSettings::load_from(p).unwrap();
        assert_eq!(r.max_iterations, 50);
        assert_eq!(r.stagnation_window, 5);
    }

    #[test]
    fn reads_sections() {
        let dir = std::env::temp_dir().join(format!("cog-collab-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"self_review": {"enabled": true, "max_iterations": 4},
                "pge": {"schemas": {"planner": {"type": "object"}}},
                "boundary": {"rules": [{"name": "r1", "rule_type": "soft", "description": "d"}]}}"#,
        )
        .unwrap();
        let sr = SelfReviewSettings::load_from(&path).unwrap();
        assert!(sr.enabled);
        assert_eq!(sr.max_iterations, 4);
        assert!(sr.to_config().is_some());
        assert!(PgeSettings::load_from(&path)
            .unwrap()
            .schema_for("planner")
            .is_some());
        assert_eq!(BoundaryConfig::load_from(&path).unwrap().rules.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }
}
