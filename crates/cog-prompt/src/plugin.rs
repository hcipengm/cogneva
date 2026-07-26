//! Prompt plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Prompt plugin that manages the global prompt manager.
pub struct PromptPlugin {
    #[allow(dead_code)]
    config: Option<cog_core::PromptConfig>,
    manager: Option<Arc<crate::PromptManager>>,
}

impl PromptPlugin {
    /// Create a plugin that will build the manager from config during `init`.
    pub fn new() -> Self {
        Self {
            config: None,
            manager: None,
        }
    }

    /// Create a plugin that wraps an already-built manager.
    pub fn from_manager(manager: Arc<crate::PromptManager>) -> Self {
        Self {
            config: None,
            manager: Some(manager),
        }
    }
}

impl Default for PromptPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for PromptPlugin {
    fn name(&self) -> &'static str {
        "prompt"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if let Some(ref pm) = self.manager {
            ctx.publish(pm.clone());
            let pm_dyn: Arc<dyn cog_core::PromptProvider> = pm.clone();
            ctx.publish_service(pm_dyn);
            info!("PromptPlugin re-published existing manager");
            return Ok(());
        }

        let (dir, hot_reload) = {
            let c = &ctx.config().prompts;
            (c.dir.clone(), c.hot_reload)
        };
        let pm = Arc::new(
            crate::PromptManager::from_dir(
                &dir,
                if hot_reload {
                    crate::WatchMode::HotReload
                } else {
                    crate::WatchMode::None
                },
            )
            .await
            .map_err(|e| cog_core::SFError::Config(e.to_string()))?,
        );
        crate::init_global(pm.clone());
        self.manager = Some(pm.clone());
        let pm_dyn: Arc<dyn cog_core::PromptProvider> = pm.clone();
        ctx.publish(pm);
        ctx.publish_service(pm_dyn);
        info!("PromptPlugin initialized from {}", dir);
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("PromptPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "prompt",
    requires: &[],
    optional_requires: &[],
    provides: &["PromptProvider"],
    consumes: &[],
    factory: || Box::new(PromptPlugin::new()),
};
