//! Notification plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// The configured address behind an optional string, if there is one.
///
/// An empty string counts as absent: the config surface renders an unset
/// address as `""` so that "the manifest has no such key" and "the operator
/// left it empty" stay distinguishable, and both have to mean "no outlet".
fn address(address: &Option<String>) -> Option<&str> {
    address.as_deref().filter(|a| !a.is_empty())
}

/// The address and secret behind a platform robot config, if it carries an
/// address. A robot config without a URL is not an outlet.
fn platform_parts(cfg: &Option<cog_core::PlatformWebhookConfig>) -> Option<(&str, Option<&str>)> {
    cfg.as_ref()
        .filter(|c| !c.webhook_url.is_empty())
        .map(|c| (c.webhook_url.as_str(), c.secret.as_deref()))
}

/// Names of the outlets this configuration enables, in registration order.
///
/// Built from the same two predicates the registration below uses, so the
/// announcement of what can leave this process cannot disagree with what was
/// actually registered — a second, hand-kept list would drift the moment a
/// channel is added.
fn enabled_outlets(config: &cog_core::Config) -> Vec<&'static str> {
    let gateway = &config.gateway;
    let mut outlets = Vec::new();
    if address(&gateway.notification_webhook_url).is_some() {
        outlets.push("webhook");
    }
    if platform_parts(&gateway.notification_dingtalk).is_some() {
        outlets.push("dingtalk");
    }
    if platform_parts(&gateway.notification_feishu).is_some() {
        outlets.push("feishu");
    }
    if platform_parts(&gateway.notification_wechat_work).is_some() {
        outlets.push("wechat-work");
    }
    outlets
}

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

        let http_client = ctx.require_service::<dyn cog_core::HttpClient>()?;

        if let Some(url) = address(&config.gateway.notification_webhook_url) {
            let webhook = crate::WebhookDispatcher::new(http_client.clone(), url.to_string());
            dispatcher = dispatcher.add(Arc::new(webhook));
            info!(webhook_url = %url, "Generic notification webhook dispatcher enabled");
        }

        if let Some((url, secret)) = platform_parts(&config.gateway.notification_dingtalk) {
            let d = crate::DingTalkDispatcher::new(
                http_client.clone(),
                url.to_string(),
                secret.map(str::to_string),
            );
            dispatcher = dispatcher.add(Arc::new(d));
            info!("DingTalk notification dispatcher enabled");
        }

        if let Some((url, secret)) = platform_parts(&config.gateway.notification_feishu) {
            let d = crate::FeishuDispatcher::new(
                http_client.clone(),
                url.to_string(),
                secret.map(str::to_string),
            );
            dispatcher = dispatcher.add(Arc::new(d));
            info!("Feishu notification dispatcher enabled");
        }

        if let Some((url, _)) = platform_parts(&config.gateway.notification_wechat_work) {
            let d = crate::WeChatWorkDispatcher::new(http_client.clone(), url.to_string());
            dispatcher = dispatcher.add(Arc::new(d));
            info!("WeChat Work notification dispatcher enabled");
        }

        // Announce the outlets on the same footing either way: "no outlet" is a
        // configured state that has to read as one, otherwise a silently
        // dropped alert is indistinguishable from no alert having fired.
        let outlets = enabled_outlets(config);
        if outlets.is_empty() {
            warn!(
                "No notification outlet configured: alerts and evolution downgrade notices \
                 will stay inside this process (WebSocket subscribers only) and never reach a \
                 human. Set gateway.notification_webhook_url (COGNEVA_NOTIFICATION_WEBHOOK_URL) \
                 or a platform robot webhook to give them a receiver"
            );
        } else {
            info!(outlets = ?outlets, "Notification outlets configured");
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
    factory: || Box::new(NotificationPlugin::new()),
};

#[cfg(test)]
mod tests {
    use super::*;

    fn webhook(url: &str) -> Option<String> {
        Some(url.to_string())
    }

    fn robot(url: &str, secret: Option<&str>) -> Option<cog_core::PlatformWebhookConfig> {
        Some(cog_core::PlatformWebhookConfig {
            webhook_url: url.to_string(),
            secret: secret.map(str::to_string),
        })
    }

    /// The default config has no address anywhere, so nothing leaves the
    /// process — and that is exactly the state the plugin now announces.
    #[test]
    fn default_config_has_no_outlets() {
        let config = cog_core::Config::default();
        assert!(enabled_outlets(&config).is_empty());
        assert_eq!(address(&config.gateway.notification_webhook_url), None);
    }

    /// An empty address is an absent address: manifests render unset keys as
    /// `""`, and treating that as an outlet would claim a receiver that no one
    /// ever configured.
    #[test]
    fn empty_addresses_are_not_outlets() {
        let mut config = cog_core::Config::default();
        config.gateway.notification_webhook_url = webhook("");
        config.gateway.notification_dingtalk = robot("", None);
        assert!(enabled_outlets(&config).is_empty());
    }

    #[test]
    fn generic_webhook_is_an_outlet() {
        let mut config = cog_core::Config::default();
        config.gateway.notification_webhook_url = webhook("https://example.invalid/hook");
        assert_eq!(enabled_outlets(&config), vec!["webhook"]);
    }

    /// Every platform robot the code can register is reported, in registration
    /// order, so the announcement covers the whole set rather than a subset.
    #[test]
    fn every_platform_robot_is_reported() {
        let mut config = cog_core::Config::default();
        config.gateway.notification_dingtalk = robot("https://example.invalid/dt", Some("s"));
        config.gateway.notification_feishu = robot("https://example.invalid/fs", None);
        config.gateway.notification_wechat_work = robot("https://example.invalid/wx", None);
        assert_eq!(
            enabled_outlets(&config),
            vec!["dingtalk", "feishu", "wechat-work"]
        );
    }

    /// A robot config present but without a URL contributes nothing — the
    /// predicate is the address, not the presence of the section.
    #[test]
    fn robot_without_address_is_not_an_outlet() {
        let mut config = cog_core::Config::default();
        config.gateway.notification_feishu = robot("", Some("s"));
        assert!(enabled_outlets(&config).is_empty());
        assert_eq!(platform_parts(&config.gateway.notification_feishu), None);
    }
}
