//! Network plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Network plugin that provides HTTP and WebSocket clients.
pub struct NetPlugin {
    http_client: Option<Arc<crate::ReqwestHttpClient>>,
    websocket_client: Option<Arc<crate::TungsteniteWebSocketClient>>,
}

impl NetPlugin {
    /// Create a plugin that will build clients from config during `init`.
    ///
    /// The config it builds from is cog-net's own `http_client` section of
    /// `cogneva.json`, read at init time; there is no constructor that takes
    /// one, so the file is the single source for it.
    pub fn new() -> Self {
        Self {
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
        // http_client 是 cog-net 自有配置段，自读 cogneva.json。配置在这里读一次，
        // 两条构造路径都用它：客户端要的是超时/连接池/代理，而消费者还要那个
        // 请求期限本身（见下面发布的 `HttpRequestTimeout`），后者不能靠在别处
        // 再读一遍配置得到。
        let config = crate::HttpClientConfig::load()?;
        let http = if let Some(ref client) = self.http_client {
            client.clone()
        } else {
            Arc::new(crate::ReqwestHttpClient::from_config(&config)?)
        };

        // 流式那一份：同一段配置，去掉整体请求期限。期限活在客户端上，而一次请求
        // 的超时只能把它调小、不能取消，所以「连接要不要活过短交互的期限」只能是
        // 持有哪一个客户端的选择——见 `StreamingHttpClient`。少了这一份，事件流会
        // 在短交互的期限上被切断，而读的人会把账记到服务端头上。
        let streaming = Arc::new(cog_core::StreamingHttpClient(Arc::new(
            crate::ReqwestHttpClient::new(crate::build_streaming_client(&config)?),
        )));

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
        ctx.publish(streaming);
        ctx.publish(Arc::new(cog_core::HttpRequestTimeout(
            std::time::Duration::from_secs(config.timeout_secs),
        )));
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
    factory: || Box::new(NetPlugin::new()),
};
