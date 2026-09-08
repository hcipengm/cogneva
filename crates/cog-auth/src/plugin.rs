//! Auth plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

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
            Arc::new(crate::JwtManager::new(resolve_jwt_config(ctx.config())?));
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

/// Build the JWT config: TTL from the shared gateway config, HMAC secret from
/// the `COGNEVA_JWT_SECRET` env (injected from the install-time generated
/// cluster Secret). Unset env falls back to a per-boot random secret with a
/// loud warning — safe against forgery, but every token dies on restart, so
/// real deployments must inject the Secret. Short secrets and the known
/// placeholder are refused unless demo login is explicitly enabled; the same
/// rule is enforced again inside `JwtManager::new` as a last line of defense.
fn resolve_jwt_config(config: &cog_core::Config) -> cog_core::SFResult<crate::jwt::JwtConfig> {
    let mut cfg = crate::jwt::JwtConfig {
        access_token_ttl_minutes: config.gateway.effective_access_token_ttl_minutes() as i64,
        ..crate::jwt::JwtConfig::default()
    };
    match std::env::var("COGNEVA_JWT_SECRET") {
        Ok(secret) if !secret.is_empty() => {
            // Throwaway demo deployments (gateway.demo_login_enabled) grant an
            // admin token to any credentials, so the secret is meaningless
            // there; allow the weak secret only in that explicit case.
            let demo = config.gateway.demo_login_enabled;
            if !demo
                && (secret.len() < crate::jwt::MIN_HMAC_SECRET_LEN
                    || secret == crate::jwt::KNOWN_WEAK_SECRET)
            {
                return Err(cog_core::SFError::Config(format!(
                    "COGNEVA_JWT_SECRET must be a random value of at least {} bytes and must \
                     not be the public placeholder '{}'; set a random secret (the installer \
                     generates one) or enable gateway.demo_login_enabled for throwaway demos",
                    crate::jwt::MIN_HMAC_SECRET_LEN,
                    crate::jwt::KNOWN_WEAK_SECRET
                )));
            }
            cfg.allow_weak_secret = demo;
            cfg.secret = secret;
        }
        _ => {
            cfg.secret = format!(
                "ephemeral-{}-{}",
                uuid::Uuid::new_v4(),
                uuid::Uuid::new_v4()
            );
            warn!(
                "COGNEVA_JWT_SECRET not set; generated a per-boot random secret — \
                 all issued tokens are invalidated on restart"
            );
        }
    }
    Ok(cfg)
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
