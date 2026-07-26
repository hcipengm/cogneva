//! Skill plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Skill plugin that provides the skill registry and external skill registry.
pub struct SkillPlugin {
    registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    initialized: bool,
}

impl SkillPlugin {
    /// Create a plugin that will build a fresh registry during `init`.
    pub fn new() -> Self {
        Self {
            registry: None,
            initialized: false,
        }
    }

    /// Create a plugin that wraps an existing registry.
    pub fn from_registry(registry: Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>) -> Self {
        Self {
            registry: Some(registry),
            initialized: false,
        }
    }
}

impl Default for SkillPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for SkillPlugin {
    fn name(&self) -> &'static str {
        "skill"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let registry = if let Some(ref reg) = self.registry {
            reg.clone()
        } else {
            Arc::new(tokio::sync::RwLock::new(cog_core::SkillRegistry::new()))
        };
        self.registry = Some(registry.clone());
        ctx.publish(registry);
        info!("SkillPlugin skill registry published");

        // Build external skill registry from configuration.
        let skill_config = crate::SkillConfig {
            directories: vec![
                std::path::PathBuf::from("/opt/cogneva/skills"),
                std::path::PathBuf::from("/var/lib/cogneva/skills"),
                dirs::home_dir()
                    .map(|h| h.join(".cogneva/skills"))
                    .unwrap_or_else(|| std::path::PathBuf::from("~/.cogneva/skills")),
            ],
            hot_reload_interval_secs: ctx.config().system.skill_hot_reload_interval_secs,
        };

        let impl_registry = crate::SkillRegistryImpl::new(skill_config);
        if let Err(e) = impl_registry.load_all().await {
            warn!(error = %e, "Failed to load some skills");
        }
        let _watcher = impl_registry.spawn_watcher();
        let external_registry: Arc<dyn cog_core::ExternalSkillRegistry> = impl_registry;
        ctx.publish_service(external_registry);
        info!("SkillPlugin external skill registry published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("SkillPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "skill",
    requires: &[],
    optional_requires: &[],
    provides: &["SkillRegistry", "ExternalSkillRegistry"],
    consumes: &[],
    factory: || Box::new(SkillPlugin::new()),
};
