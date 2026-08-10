//! Prompt 配置——cog-prompt 自有配置段（core config.rs 不聚合单 crate
//! 配置，审计文档 §7.3）。自读 cogneva.json `prompts` 段。

use serde::{Deserialize, Serialize};

use cog_core::{SFError, SFResult};

/// Prompt configuration — directory for external prompt templates.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct PromptConfig {
    pub dir: String,
    #[serde(default)]
    pub hot_reload: bool,
}

impl PromptConfig {
    /// 自读 cogneva.json `prompts` 段；文件/段缺失回退默认，
    /// 段存在但解析失败响亮报错。
    pub fn load() -> SFResult<Self> {
        let path = std::env::var("COGNEVA_CONFIG_PATH")
            .unwrap_or_else(|_| "/etc/cogneva/cogneva.json".into());
        Self::load_from(std::path::Path::new(&path))
    }

    /// 从指定文件加载（测试与自定义路径用）。
    pub fn load_from(path: &std::path::Path) -> SFResult<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let root: serde_json::Value = serde_json::from_str(&text)
                    .map_err(|e| SFError::Config(format!("{}: {e}", path.display())))?;
                match root.pointer("/prompts") {
                    Some(section) => serde_json::from_value(section.clone())
                        .map_err(|e| SFError::Config(format!("{} prompts: {e}", path.display()))),
                    None => Ok(Self::default()),
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(SFError::Config(format!("{}: {e}", path.display()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_section_and_missing_defaults() {
        let p = std::path::Path::new("/nonexistent/x.json");
        assert_eq!(PromptConfig::load_from(p).unwrap().dir, "");

        let dir = std::env::temp_dir().join(format!("cog-prompt-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("cogneva.json");
        std::fs::write(
            &path,
            r#"{"prompts": {"dir": "/etc/cogneva/prompts", "hot_reload": true}}"#,
        )
        .unwrap();
        let cfg = PromptConfig::load_from(&path).unwrap();
        assert_eq!(cfg.dir, "/etc/cogneva/prompts");
        assert!(cfg.hot_reload);
        std::fs::remove_dir_all(&dir).ok();
    }
}
