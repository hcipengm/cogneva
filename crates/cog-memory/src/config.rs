//! Memory 配置——cog-memory 自有配置段（core config.rs 不聚合单 crate
//! 配置，审计文档 §7.3）。自读 cogneva.json `memory` 段并叠加
//! `COGNEVA_MEMORY_*` env 覆盖。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

const MEMORY_ENV: &[(&str, &str)] = &[
    ("COGNEVA_MEMORY_ENABLED", "enabled"),
    ("COGNEVA_MEMORY_BACKEND_TYPE", "backend_type"),
    ("COGNEVA_MEMORY_BASE_DIR", "base_dir"),
    ("COGNEVA_MEMORY_EMBEDDING_DIMENSION", "embedding_dimension"),
    ("COGNEVA_MEMORY_AUTO_INGEST", "auto_ingest"),
];

/// Memory 子系统配置。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MemoryConfig {
    pub enabled: bool,
    /// Backend type: `memory`, `file`, or `composite`.
    pub backend_type: String,
    /// Base directory for file-backed memory layers (raw/schema/summary).
    pub base_dir: String,
    /// Embedding dimension for summary vectors.
    pub embedding_dimension: usize,
    /// Auto-ingest AgentEnd events into memory.
    pub auto_ingest: bool,
}

impl MemoryConfig {
    /// 自读 cogneva.json `memory` 段 + env 覆盖；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        let mut section = match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                root.pointer("/memory")
                    .cloned()
                    .unwrap_or(serde_json::json!({}))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => serde_json::json!({}),
            Err(e) => return Err(SFError::Config(format!("{}: {e}", path.display()))),
        };
        cog_core::config::apply_env_paths(&mut section, MEMORY_ENV);
        serde_json::from_value(section)
            .map_err(|e| SFError::Config(format!("{} memory: {e}", path.display())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_returns_default() {
        let cfg = MemoryConfig::load_from(std::path::Path::new("/nonexistent/x.json")).unwrap();
        assert!(!cfg.enabled);
    }

    #[test]
    fn reads_section() {
        let dir = std::env::temp_dir().join(format!("cog-mem-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"memory": {"enabled": true, "backend_type": "composite", "embedding_dimension": 1024}}"#,
        )
        .unwrap();
        let cfg = MemoryConfig::load_from(&path).unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.backend_type, "composite");
        assert_eq!(cfg.embedding_dimension, 1024);
        std::fs::remove_dir_all(&dir).ok();
    }
}
