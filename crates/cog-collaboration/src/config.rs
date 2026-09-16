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
/// outputs. Empty by default so existing behavior is unchanged.
///
/// When a schema is configured for an actor (keyed by actor name:
/// "planner", "generator", "evaluator", "moderator", "merger"), the actor
/// injects it into the prompt context as `output_schema` and validates the
/// raw LLM output against it. Validation failures are logged and the legacy
/// lenient parsing still applies, so a bad schema can never break the
/// pipeline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PgeSettings {
    pub schemas: HashMap<String, serde_json::Value>,
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
