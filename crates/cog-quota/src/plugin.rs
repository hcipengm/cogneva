//! Quota plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Quota plugin that self-assembles and publishes quota and hierarchy managers.
pub struct QuotaPlugin {
    initialized: bool,
}

impl QuotaPlugin {
    /// Create a plugin that will build quota services during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for QuotaPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for QuotaPlugin {
    fn name(&self) -> &'static str {
        "quota"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let redis_client = ctx
            .consume::<cog_core::storage::RedisClient>()
            .expect("redis client")
            .0
            .clone();

        let quota_manager = {
            let conn = redis_client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| {
                    cog_core::SFError::Config(format!(
                        "Redis connection for quota manager failed: {}",
                        e
                    ))
                })?;
            Arc::new(crate::QuotaManager::new(conn, 50_000))
        };
        ctx.publish(quota_manager.clone());
        let quota_dyn: Arc<dyn cog_core::QuotaManager> = quota_manager.clone();
        ctx.publish_service(quota_dyn);
        let workspace_quota_dyn: Arc<dyn cog_core::WorkspaceQuotaSource> = quota_manager;
        ctx.publish_service(workspace_quota_dyn);
        info!("QuotaPlugin quota manager published");

        let hierarchy_manager = {
            let conn = redis_client.get_multiplexed_async_connection().await.ok();
            conn.map(|c| {
                let limits = crate::QuotaLimits::from_hard(50_000, 0.8);
                Arc::new(crate::HierarchyManager::new(c, limits))
            })
        };
        if let Some(ref hm) = hierarchy_manager {
            ctx.publish(hm.clone());
            info!("QuotaPlugin hierarchy manager published");
        }

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("QuotaPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "quota",
    requires: &["storage"],
    optional_requires: &[],
    provides: &["QuotaManager", "HierarchyManager", "WorkspaceQuotaSource"],
    consumes: &[cog_core::ConsumeSpec {
        type_name: "RedisClient",
        required: true,
    }],
    factory: || Box::new(QuotaPlugin::new()),
};
