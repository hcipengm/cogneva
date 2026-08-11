//! LLM 路由与调优配置——cog-llm 自有配置段（core config.rs 不聚合单
//! crate 配置）。自读 cogneva.json `llm_routing` /
//! `tuning` 段；`tuning` 叠加 `COGNEVA_STREAM_CAPACITY` 等 env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

fn load_section<T: serde::de::DeserializeOwned + Default>(
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let path =
        std::env::var("COGNEVA_CONFIG_PATH").unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
    load_section_from(std::path::Path::new(&path), pointer, env_map)
}

fn load_section_from<T: serde::de::DeserializeOwned + Default>(
    path: &std::path::Path,
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let mut section = match std::fs::read_to_string(path) {
        Ok(text) => {
            let root: serde_json::Value = serde_json::from_str(&text)
                .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
            root.pointer(pointer)
                .cloned()
                .unwrap_or(serde_json::json!({}))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
        Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
    };
    cog_core::config::apply_env_paths(&mut section, env_map);
    serde_json::from_value(section)
        .map_err(|e| SFError::Config(format!("{} {pointer}: {e}", path.display())))
}

const TUNING_ENV: &[(&str, &str)] = &[
    ("COGNEVA_STREAM_CAPACITY", "stream_capacity"),
    ("COGNEVA_HIGH_WATERMARK", "high_watermark"),
    ("COGNEVA_LOW_WATERMARK", "low_watermark"),
    ("COGNEVA_MAX_SUMMARIES", "max_summaries"),
];

/// LLM 后端配置。`api_key` 在 Debug 输出中脱敏，防止泄漏到日志。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LLMBackendConfig {
    pub provider: String,
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub model: String,
    /// API 兼容风格：`openai` 或 `anthropic`。默认 `openai`。
    #[serde(default = "default_api_style")]
    pub api_style: String,
    pub weight: u32,
    pub enabled: bool,
}

fn default_api_style() -> String {
    "openai".into()
}

impl std::fmt::Debug for LLMBackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LLMBackendConfig")
            .field("provider", &self.provider)
            .field("api_key", &cog_core::config::redacted(&self.api_key))
            .field("base_url", &self.base_url)
            .field("model", &self.model)
            .field("api_style", &self.api_style)
            .field("weight", &self.weight)
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl Default for LLMBackendConfig {
    fn default() -> Self {
        Self {
            provider: String::new(),
            api_key: String::new(),
            base_url: None,
            model: String::new(),
            api_style: default_api_style(),
            weight: 1,
            enabled: true,
        }
    }
}

/// LLM routing / failover configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct LLMRoutingConfig {
    pub strategy: String,
    pub backends: Vec<LLMBackendConfig>,
    pub retry_on_429: bool,
    pub retry_on_402: bool,
    pub max_failover_attempts: u32,
}

impl LLMRoutingConfig {
    /// 自读 cogneva.json `llm_routing` 段；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        load_section("/llm_routing", &[])
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        load_section_from(path, "/llm_routing", &[])
    }
}

/// Domain-level tuning constants used across the system.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct TuningConfig {
    pub stream_capacity: usize,
    pub high_watermark: usize,
    pub low_watermark: usize,
    pub max_summaries: usize,
}

impl TuningConfig {
    /// 自读 cogneva.json `tuning` 段 + env 覆盖。
    pub fn load() -> SFResult<Self> {
        load_section("/tuning", TUNING_ENV)
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        load_section_from(path, "/tuning", TUNING_ENV)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_returns_defaults() {
        let p = std::path::Path::new("/nonexistent/x.json");
        assert!(LLMRoutingConfig::load_from(p).unwrap().backends.is_empty());
        assert_eq!(TuningConfig::load_from(p).unwrap().stream_capacity, 0);
    }

    #[test]
    fn reads_sections() {
        let dir = std::env::temp_dir().join(format!("cog-llm-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"llm_routing": {"strategy": "failover", "backends": [{"provider": "anthropic", "api_key": "sk-secret", "model": "claude"}]},
                "tuning": {"stream_capacity": 64}}"#,
        )
        .unwrap();
        let r = LLMRoutingConfig::load_from(&path).unwrap();
        assert_eq!(r.strategy, "failover");
        assert_eq!(r.backends.len(), 1);
        // api_key 脱敏
        let dbg = format!("{:?}", r.backends[0]);
        assert!(!dbg.contains("sk-secret"));
        assert_eq!(TuningConfig::load_from(&path).unwrap().stream_capacity, 64);
        std::fs::remove_dir_all(&dir).ok();
    }
}
