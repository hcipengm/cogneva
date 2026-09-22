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

    /// A shipped template is a claim about this section's type: serde passes
    /// over a key with no field behind it without a word, so an entry nothing
    /// reads looks exactly like one that was applied. The comparison is an
    /// equality because the other direction matters too — a field read but
    /// never written ships a value the operator can neither see nor change,
    /// which is how the repair budget stayed unreachable from every
    /// deployment while the code that read it was already there.
    fn assert_pge_surface_matches(file: &Path) {
        let raw = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let doc: serde_json::Value = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", file.display()));
        let section = doc
            .get("pge")
            .unwrap_or_else(|| panic!("{} has no pge section", file.display()));

        let mut expected: Vec<String> = serde_json::to_value(PgeSettings::default())
            .expect("PgeSettings serializes")
            .as_object()
            .expect("PgeSettings serializes to an object")
            .keys()
            .cloned()
            .collect();
        let mut actual: Vec<String> = section
            .as_object()
            .unwrap()
            .keys()
            .filter(|k| !k.starts_with('_'))
            .cloned()
            .collect();
        expected.sort();
        actual.sort();
        assert_eq!(
            expected,
            actual,
            "{} pge section drifted from PgeSettings",
            file.display()
        );
    }

    #[test]
    fn shipped_config_templates_track_the_pge_field_surface() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        assert_pge_surface_matches(&root.join("cogneva.example.json"));
        assert_pge_surface_matches(&root.join("deploy/helm/cogneva/files/cogneva.json"));
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
