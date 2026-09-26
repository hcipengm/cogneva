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

/// The name of each outlet, in registration order.
///
/// Two readers need these names and they are the same names: the address table
/// below (where the outlet is configured) and the dispatcher's construction
/// site (where the outlet reports its delivery outcome). Written twice as
/// literals they would drift the moment one is renamed, and a dispatcher
/// reporting under a name nothing reads is a reading that does not exist.
pub const OUTLET_WEBHOOK: &str = "webhook";
pub const OUTLET_DINGTALK: &str = "dingtalk";
pub const OUTLET_FEISHU: &str = "feishu";
pub const OUTLET_WECHAT_WORK: &str = "wechat-work";

/// The config path each outlet reads its address from, in registration order.
///
/// This is the producer's own claim about where its addresses live, and it is
/// the only place that claim exists: a deployment writes these paths and finds
/// out nothing when one of them is wrong. An outlet whose path no deployment
/// can write is a dispatcher present in code that can never be registered, and
/// at runtime that is indistinguishable from "no address configured" — a
/// legitimate state. The test below pins each path to the predicate that reads
/// it, so the table cannot drift from the registration.
pub const OUTLET_ADDRESS_PATHS: [(&str, &str); 4] = [
    (OUTLET_WEBHOOK, "gateway.notification_webhook_url"),
    (OUTLET_DINGTALK, "gateway.notification_dingtalk.webhook_url"),
    (OUTLET_FEISHU, "gateway.notification_feishu.webhook_url"),
    (
        OUTLET_WECHAT_WORK,
        "gateway.notification_wechat_work.webhook_url",
    ),
];

/// The outlet names, for the delivery readings to render rows for.
///
/// Derived from the address table rather than kept as a second list: this name
/// set has one source or it drifts.
pub fn outlet_names() -> Vec<&'static str> {
    OUTLET_ADDRESS_PATHS
        .iter()
        .map(|(outlet, _)| *outlet)
        .collect()
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

        // 广播出口不进投递读数：它没有"投递失败"这回事，没有 WebSocket 订阅者
        // 是一个合法状态（进程内那半环），把它混进来会让"没人看着面板"报警。
        let mut dispatcher = crate::MultiDispatcher::new()
            .add(Arc::new(broadcast) as Arc<dyn cog_core::NotificationDispatcher>);

        let http_client = ctx.require_service::<dyn cog_core::HttpClient>()?;

        if let Some(url) = address(&config.gateway.notification_webhook_url) {
            let webhook =
                crate::WebhookDispatcher::new(http_client.clone(), url.to_string(), OUTLET_WEBHOOK);
            dispatcher = dispatcher.add(Arc::new(webhook));
            info!(webhook_url = %url, "Generic notification webhook dispatcher enabled");
        }

        if let Some((url, secret)) = platform_parts(&config.gateway.notification_dingtalk) {
            let d = crate::DingTalkDispatcher::new(
                http_client.clone(),
                url.to_string(),
                secret.map(str::to_string),
                OUTLET_DINGTALK,
            );
            dispatcher = dispatcher.add(Arc::new(d));
            info!("DingTalk notification dispatcher enabled");
        }

        if let Some((url, secret)) = platform_parts(&config.gateway.notification_feishu) {
            let d = crate::FeishuDispatcher::new(
                http_client.clone(),
                url.to_string(),
                secret.map(str::to_string),
                OUTLET_FEISHU,
            );
            dispatcher = dispatcher.add(Arc::new(d));
            info!("Feishu notification dispatcher enabled");
        }

        if let Some((url, _)) = platform_parts(&config.gateway.notification_wechat_work) {
            let d = crate::WeChatWorkDispatcher::new(
                http_client.clone(),
                url.to_string(),
                OUTLET_WECHAT_WORK,
            );
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

        // "通知发出去了"这件事从进程外读不出来：出口配错、被 egress 拦掉、被平台
        // 以报文里的错误码拒收，三种都只表现为"没人收到"。读数按出口分开报，
        // 所以判据能指出是哪一个出口、以及失败在哪一层。
        ctx.publish_observable(crate::delivery::observable());
        info!("notification delivery readings published");

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

    /// Each declared path, written through the loader's own setter on the
    /// default tree, has to enable the outlet it is declared for. This is the
    /// tie the table needs: the path in a deployment's env map and the field
    /// the predicate reads are two different names for one thing, and a table
    /// checked only against itself would keep agreeing with itself while the
    /// deployment wrote somewhere nobody reads. Note the default tree: the
    /// robot sections serialize as `null`, so this also pins that a write
    /// through an unset section lands.
    #[test]
    fn each_declared_path_enables_its_outlet() {
        for (outlet, path) in OUTLET_ADDRESS_PATHS {
            let mut value =
                serde_json::to_value(cog_core::Config::default()).expect("config serializes");
            cog_core::config::set_json_path(&mut value, path, "https://example.invalid/hook");
            let config: cog_core::Config =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("{path}: {e}"));
            assert!(
                enabled_outlets(&config).contains(&outlet),
                "writing {path} did not enable {outlet}"
            );
        }
    }
}
