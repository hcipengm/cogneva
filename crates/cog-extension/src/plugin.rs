//! Extension plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Extension plugin that self-assembles and publishes sandbox backend,
/// plugin registry, and TaskSandboxExecutor.
pub struct ExtensionPlugin {
    initialized: bool,
    sandbox_backend: Option<Arc<dyn cog_core::SandboxBackend>>,
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
}

impl ExtensionPlugin {
    /// Create a plugin that will build extension services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            sandbox_backend: None,
            plugin_registry: None,
        }
    }
}

impl Default for ExtensionPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for ExtensionPlugin {
    fn name(&self) -> &'static str {
        "extension"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let app_dir = ctx.config().app.app_dir.clone();

        let sandbox_backend: Arc<dyn cog_core::SandboxBackend> =
            Arc::new(crate::WasmRuntime::new());
        ctx.publish_service(sandbox_backend.clone());
        info!("ExtensionPlugin sandbox backend published");

        let plugin_registry: Arc<dyn cog_core::PluginRegistry> =
            Arc::new(crate::registry::PluginRegistryImpl::new(
                format!("{}/plugins", app_dir),
                None,
                sandbox_backend.clone(),
            ));
        ctx.publish_service(plugin_registry.clone());
        info!("ExtensionPlugin plugin registry published");

        let executor = crate::TaskSandboxExecutor::new(sandbox_backend.clone())
            .with_plugin_registry(plugin_registry.clone());
        ctx.publish_service::<dyn cog_core::TaskExecutor>(Arc::new(executor));
        info!("ExtensionPlugin TaskSandboxExecutor published");

        self.sandbox_backend = Some(sandbox_backend);
        self.plugin_registry = Some(plugin_registry);
        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("ExtensionPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "extension",
    requires: &[],
    optional_requires: &[],
    provides: &["SandboxBackend", "PluginRegistry", "TaskExecutor"],
    consumes: &[],
    factory: || Box::new(ExtensionPlugin::new()),
};
