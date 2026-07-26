//! Auth plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Auth plugin that self-assembles and publishes JWT manager and session manager.
pub struct AuthPlugin {
    initialized: bool,
}

impl AuthPlugin {
    /// Create a plugin that will build auth services during `init`.
    pub fn new() -> Self {
        Self { initialized: false }
    }
}

impl Default for AuthPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for AuthPlugin {
    fn name(&self) -> &'static str {
        "auth"
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

        let jwt_manager: Arc<dyn cog_core::AuthProvider> =
            Arc::new(crate::JwtManager::new(crate::jwt::JwtConfig::default()));
        ctx.publish_service(jwt_manager);
        info!("AuthPlugin JWT manager published");

        let session_manager: Arc<dyn cog_core::SessionManager> = {
            let conn = redis_client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| {
                    cog_core::SFError::Config(format!(
                        "Redis connection for session manager failed: {}",
                        e
                    ))
                })?;
            Arc::new(crate::SessionManager::new(conn))
        };
        ctx.publish_service(session_manager);
        info!("AuthPlugin session manager published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("AuthPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "auth",
    requires: &["storage"],
    optional_requires: &[],
    provides: &["AuthProvider", "SessionManager"],
    consumes: &[cog_core::ConsumeSpec {
        type_name: "RedisClient",
        required: true,
    }],
    factory: || Box::new(AuthPlugin::new()),
};
