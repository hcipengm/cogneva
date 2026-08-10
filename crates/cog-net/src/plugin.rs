//! Network plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Network plugin that provides HTTP and WebSocket clients.
pub struct NetPlugin {
    #[allow(dead_code)]
    http_config: Option<crate::HttpClientConfig>,
    http_client: Option<Arc<crate::ReqwestHttpClient>>,
    websocket_client: Option<Arc<crate::TungsteniteWebSocketClient>>,
}

impl NetPlugin {
    /// Create a plugin that will build clients from config during `init`.
    pub fn new() -> Self {
        Self {
            http_config: None,
            http_client: None,
            websocket_client: None,
        }
    }

    /// Create a plugin that wraps already-built clients.
    pub fn from_clients(
        http_client: Arc<crate::ReqwestHttpClient>,
        websocket_client: Arc<crate::TungsteniteWebSocketClient>,
    ) -> Self {
        Self {
            http_config: None,
            http_client: Some(http_client),
            websocket_client: Some(websocket_client),
        }
    }
}

impl Default for NetPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for NetPlugin {
    fn name(&self) -> &'static str {
        "net"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let http = if let Some(ref client) = self.http_client {
            client.clone()
        } else {
            // http_client 是 cog-net 自有配置段，自读 cogneva.json。
            let config = crate::HttpClientConfig::load()?;
            Arc::new(crate::ReqwestHttpClient::from_config(&config))
        };

        let ws = if let Some(ref client) = self.websocket_client {
            client.clone()
        } else {
            Arc::new(crate::TungsteniteWebSocketClient::new())
        };

        self.http_client = Some(http.clone());
        self.websocket_client = Some(ws.clone());

        ctx.publish(http.clone());
        let http_dyn: Arc<dyn cog_core::HttpClient> = http;
        ctx.publish_service(http_dyn);
        ctx.publish(ws.clone());
        let ws_dyn: Arc<dyn cog_core::WebSocketClient> = ws;
        ctx.publish_service(ws_dyn);
        info!("NetPlugin initialized");
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("NetPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "net",
    requires: &[],
    optional_requires: &[],
    provides: &["HttpClient", "WebSocketClient"],
    consumes: &[],
    factory: || Box::new(NetPlugin::new()),
};
