//! `cog-prompt` — Prompt 工程化 crate。
//! 彻底消灭代码里的硬编码 prompt。所有 system prompt、user prompt template、
//! instruction 均通过 `PromptRegistry` 从外部文件/数据库/远程 URL 加载，支持：
//! - 运行时热加载（文件修改后自动生效，无需重启）
//! - 版本管理（Git 风格 diff + 回滚）
//! - A/B 测试（流量分割 + 效果对比 + 自动择优）
//! - Jinja2 风格模板变量替换
//! ## 核心原则
//! **代码里不允许出现任何 `"You are a helpful assistant..."` 字符串常量。**
//! 所有 prompt 必须通过 `registry.get("domain:purpose")` 获取。

pub mod ab_test;
pub mod builder;
pub mod config;
pub mod loader;
pub mod plugin;
pub mod registry;
pub mod template;
pub mod version;

pub use ab_test::{AbTestConfig, AbTestGroup, PromptVariant};
pub use builder::PromptBuilder;
pub use config::PromptConfig;
pub use loader::{FileSystemLoader, PromptLoader, WatchMode};
pub use registry::{PromptEntry, PromptRegistry, PromptSource};
pub use template::{TemplateEngine, TemplateVars};
pub use version::{PromptVersion, VersionHistory};

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::RwLock;

static GLOBAL_PROMPT_MANAGER: OnceLock<Arc<PromptManager>> = OnceLock::new();

/// Initialize the global prompt manager singleton.
/// Should be called once at application startup after [`PromptManager::from_dir`]
/// has successfully loaded prompts.
pub fn init_global(manager: Arc<PromptManager>) {
    let _ = GLOBAL_PROMPT_MANAGER.set(manager);
}

/// Get a raw prompt string from the global manager.
/// Returns `None` if the global manager has not been initialized or the key
/// does not exist.
pub fn global_prompt(key: &str) -> Option<String> {
    GLOBAL_PROMPT_MANAGER.get()?.get(key)
}

/// Render a prompt from the global manager with template variables.
/// Returns an error if the global manager has not been initialized,
/// the key does not exist, or template rendering fails.
pub fn global_render(key: &str, vars: &TemplateVars) -> anyhow::Result<String> {
    let manager = GLOBAL_PROMPT_MANAGER
        .get()
        .ok_or_else(|| anyhow::anyhow!("Global prompt manager not initialized"))?;
    manager.render(key, vars)
}

/// Convenience builder for production use.
pub struct PromptManager {
    pub registry: Arc<RwLock<PromptRegistry>>,
    pub loader: Arc<dyn PromptLoader>,
    pub template_engine: TemplateEngine,
}

impl PromptManager {
    /// Load prompts from a directory and start hot-reload watcher.
    pub async fn from_dir<P: AsRef<std::path::Path>>(
        dir: P,
        watch: WatchMode,
    ) -> anyhow::Result<Self> {
        let loader = Arc::new(FileSystemLoader::new(dir.as_ref().to_path_buf()));
        let mut registry = PromptRegistry::new();
        loader.load_all(&mut registry).await?;

        let registry = Arc::new(RwLock::new(registry));

        if watch == WatchMode::HotReload {
            let reg_clone = registry.clone();
            let loader_clone = loader.clone();
            tokio::spawn(async move {
                if let Err(e) = loader_clone.watch(reg_clone).await {
                    tracing::error!("Prompt hot-reload watcher failed: {}", e);
                }
            });
        }

        Ok(Self {
            registry,
            loader,
            template_engine: TemplateEngine::new(),
        })
    }

    /// Get a raw prompt string by key (synchronous).
    pub fn get(&self, key: &str) -> Option<String> {
        let reg = self.registry.read().ok()?;
        reg.get(key).map(|e| e.content.clone())
    }

    /// Get a prompt and render it with template variables (synchronous).
    pub fn render(&self, key: &str, vars: &TemplateVars) -> anyhow::Result<String> {
        let raw = self
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Prompt not found: {}", key))?;
        self.template_engine.render(&raw, vars)
    }

    /// Get a system message ready for LLM consumption (synchronous).
    pub fn system_message(
        &self,
        key: &str,
        vars: &TemplateVars,
    ) -> anyhow::Result<cog_core::Message> {
        let content = self.render(key, vars)?;
        Ok(cog_core::Message::system(content))
    }

    /// List all versions of a prompt.
    pub fn list_versions(&self, key: &str) -> Vec<String> {
        let reg = self.registry.read().ok();
        reg.map(|r| r.list_versions(key)).unwrap_or_default()
    }

    /// Get a specific historical version of a prompt.
    pub fn get_version(&self, key: &str, version: &str) -> Option<PromptVersion> {
        let reg = self.registry.read().ok()?;
        reg.get_version(key, version)
    }

    /// Diff two versions of a prompt.
    pub fn diff_versions(&self, key: &str, old: &str, new: &str) -> Option<String> {
        let reg = self.registry.read().ok()?;
        reg.diff_versions(key, old, new)
    }

    /// Rollback a prompt to a specific version.
    pub fn rollback(&self, key: &str, version: &str) -> anyhow::Result<()> {
        let mut reg = self
            .registry
            .write()
            .map_err(|_| anyhow::anyhow!("registry poisoned"))?;
        if reg.rollback(key, version) {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "rollback failed: key={} version={}",
                key,
                version
            ))
        }
    }
}

impl cog_core::PromptProvider for PromptManager {
    fn get(&self, key: &str) -> Option<String> {
        self.get(key)
    }

    fn render(&self, key: &str, vars: &HashMap<String, String>) -> cog_core::SFResult<String> {
        let mut tv = crate::template::TemplateVars::new();
        for (k, v) in vars {
            tv.insert(k.clone(), v.clone());
        }
        self.render(key, &tv)
            .map_err(|e| cog_core::SFError::Agent(format!("prompt render failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_prompt_manager_loads_from_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let yaml = r#"
prompts:
  self_review:critique:
    content: "Critique the following output for errors."
    version: "1.0.0"
"#;
        std::fs::write(tmp.path().join("prompts.yaml"), yaml).unwrap();

        let mgr = PromptManager::from_dir(tmp.path(), WatchMode::None)
            .await
            .unwrap();

        let prompt = mgr.get("self_review:critique");
        assert!(prompt.is_some());
        assert!(prompt.unwrap().contains("Critique"));
    }
}
