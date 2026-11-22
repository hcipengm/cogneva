//! LLM 路由与调优配置——cog-llm 自有配置段（core config.rs 不聚合单
//! crate 配置）。自读 cogneva.json `llm_routing` /
//! `tuning` 段；`tuning` 叠加 `COGNEVA_STREAM_CAPACITY` 等 env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

// 容量 0 建不出有界通道：这条兜底自己把 panic 藏起来了，所以要在编译期当场红。
// 判的是常量自己的性质，写成运行期断言只会被 clippy 判成恒真。
const _: () = assert!(crate::DEFAULT_STREAM_CAPACITY > 0);

fn load_section<T: serde::de::DeserializeOwned + Default>(
    pointer: &str,
    env_map: &[(&str, &str)],
) -> SFResult<T> {
    let path = cog_core::paths::config_file();
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

/// The env-var names this section answers to. Published so the deploy-config
/// gate can tell "honored, but not by the core schema" from "honored by
/// nobody".
pub const TUNING_ENV: &[(&str, &str)] = &[("COGNEVA_STREAM_CAPACITY", "stream_capacity")];

/// LLM 后端配置。`api_key` 在 Debug 输出中脱敏，防止泄漏到日志。
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct LLMBackendConfig {
    pub provider: String,
    pub api_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    pub model: String,
    /// API 兼容风格：`openai`（chat-completions 形状）、`openai-responses`
    /// （Responses 形状，同一家的较新那个面）或 `anthropic`。默认 `openai`。
    #[serde(default = "default_api_style")]
    pub api_style: String,
    pub weight: u32,
    pub enabled: bool,
}

fn default_api_style() -> String {
    crate::openai::API_STYLE.into()
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

/// 一个后端的一份权重：均分。
///
/// 权重是份额，不是可以调的档位——一份就是「和其他后端一样多」，只有运维给了
/// 不同份额时它才不是一。
// cap: fixed — 份额的单位：一份就是均分
pub const EQUAL_WEIGHT: u32 = 1;

impl Default for LLMBackendConfig {
    fn default() -> Self {
        Self {
            provider: String::new(),
            api_key: String::new(),
            base_url: None,
            model: String::new(),
            api_style: default_api_style(),
            weight: EQUAL_WEIGHT,
            enabled: true,
        }
    }
}

/// LLM routing / failover configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct LLMRoutingConfig {
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

    /// 流式事件的通道容量：`0` 不是一个可用值——事件流是有界通道，容量为 0
    /// 建不出来——所以 0 在这里读作「没写」，由 [`DEFAULT_STREAM_CAPACITY`] 给。
    ///
    /// 这一条同时也是「只声明一次」的落点：容量写进交付文档就会与这个常量变成
    /// 同一事实的两处声明，改一处不改另一处时，两边都各自绿。文档里不写它。
    pub fn stream_capacity_or_default(&self) -> usize {
        match self.stream_capacity {
            0 => crate::DEFAULT_STREAM_CAPACITY,
            capacity => capacity,
        }
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

    /// 没写容量时读到的是常量本身；写了就按写的来；常量必须是一个能建通道的数。
    #[test]
    fn an_absent_capacity_reads_the_constant_instead_of_zero() {
        let unset = TuningConfig::default();
        assert_eq!(unset.stream_capacity, 0, "默认仍是 0：0 是「没写」的哨兵");
        assert_eq!(
            unset.stream_capacity_or_default(),
            crate::DEFAULT_STREAM_CAPACITY
        );
        let written = TuningConfig {
            stream_capacity: 64,
        };
        assert_eq!(written.stream_capacity_or_default(), 64);
    }

    #[test]
    fn reads_sections() {
        let dir = std::env::temp_dir().join(format!("cog-llm-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"llm_routing": {"backends": [{"provider": "anthropic", "api_key": "sk-secret", "model": "claude"}]},
                "tuning": {"stream_capacity": 64}}"#,
        )
        .unwrap();
        let r = LLMRoutingConfig::load_from(&path).unwrap();
        assert_eq!(r.backends.len(), 1);
        // api_key 脱敏
        let dbg = format!("{:?}", r.backends[0]);
        assert!(!dbg.contains("sk-secret"));
        assert_eq!(TuningConfig::load_from(&path).unwrap().stream_capacity, 64);
        std::fs::remove_dir_all(&dir).ok();
    }
}
