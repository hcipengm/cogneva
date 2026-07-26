//! Notification plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::info;

/// Notification plugin that assembles dispatchers and stores.
pub struct NotificationPlugin;

impl NotificationPlugin {
    /// Create the notification plugin.
    pub fn new() -> Self {
        Self
    }
}

impl Default for NotificationPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for NotificationPlugin {
    fn name(&self) -> &'static str {
        "notification"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let config = ctx.config();

        let broadcast =
            crate::BroadcastDispatcher::new(config.system.websocket_event_cache_capacity.max(16));
        let tx = broadcast.sender();

        let mut dispatcher = crate::MultiDispatcher::new()
            .add(Arc::new(broadcast) as Arc<dyn cog_core::NotificationDispatcher>);

        let http_client = ctx
            .consume_service::<dyn cog_core::HttpClient>()
            .expect("http client for webhook dispatchers");

        if let Some(ref url) = config.gateway.notification_webhook_url {
            if !url.is_empty() {
                let webhook = crate::WebhookDispatcher::new(http_client.clone(), url.clone());
                dispatcher = dispatcher.add(Arc::new(webhook));
                info!(webhook_url = %url, "Generic notification webhook dispatcher enabled");
            }
        }

        if let Some(ref cfg) = config.gateway.notification_dingtalk {
            if !cfg.webhook_url.is_empty() {
                let d = crate::DingTalkDispatcher::new(
                    http_client.clone(),
                    cfg.webhook_url.clone(),
                    cfg.secret.clone(),
                );
                dispatcher = dispatcher.add(Arc::new(d));
                info!("DingTalk notification dispatcher enabled");
            }
        }

        if let Some(ref cfg) = config.gateway.notification_feishu {
            if !cfg.webhook_url.is_empty() {
                let d = crate::FeishuDispatcher::new(
                    http_client.clone(),
                    cfg.webhook_url.clone(),
                    cfg.secret.clone(),
                );
                dispatcher = dispatcher.add(Arc::new(d));
                info!("Feishu notification dispatcher enabled");
            }
        }

        if let Some(ref cfg) = config.gateway.notification_wechat_work {
            if !cfg.webhook_url.is_empty() {
                let d =
                    crate::WeChatWorkDispatcher::new(http_client.clone(), cfg.webhook_url.clone());
                dispatcher = dispatcher.add(Arc::new(d));
                info!("WeChat Work notification dispatcher enabled");
            }
        }

        let dispatcher: Arc<dyn cog_core::NotificationDispatcher> = Arc::new(dispatcher);
        let store: Arc<dyn cog_core::NotificationStore> =
            Arc::new(crate::InMemoryNotificationStore::new());

        ctx.publish(Arc::new(tx.clone()));
        ctx.publish_service(dispatcher);
        ctx.publish_service(store);
        info!("NotificationPlugin initialized");
        Ok(())
    }

    async fn start(&self, _ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("NotificationPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "notification",
    requires: &["net"],
    optional_requires: &[],
    provides: &[
        "NotificationDispatcher",
        "NotificationStore",
        "Sender<Notification>",
    ],
    consumes: &[cog_core::ConsumeSpec {
        type_name: "HttpClient",
        required: true,
    }],
    factory: || Box::new(NotificationPlugin::new()),
};
