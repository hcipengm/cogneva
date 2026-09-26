//! Notification business logic crate.
//! Provides [`InMemoryNotificationStore`] — a per-process notification
//! backend suitable for development and single-node deployments.
//! Persistent backends (PostgreSQL, Redis) can be added here later without
//! touching consumers.

use async_trait::async_trait;
use base64::Engine as _;
use cog_core::{Notification, NotificationFilter, NotificationList, NotificationStore, SFResult};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

pub mod delivery;

/// 一次 HTTP 投递的结果，除了报文里那层判断之外的全部。
///
/// 报文层的判断（`EnvelopeError`）由各平台自己的信封解析补上：那是厂商自己的
/// 契约，只有认它的那个 dispatcher 能判，放在这里会变成一个替所有平台猜的公共
/// 函数。
fn transport_outcome(resp: &cog_core::HttpResponse) -> delivery::DeliveryResult {
    if resp.is_success() {
        delivery::DeliveryResult::Ok
    } else {
        delivery::DeliveryResult::HttpError
    }
}

/// 投递失败的返回值。
///
/// `provider` 是出口名而不是平台域名：这层错误的消费者是"哪个出口发不出去"，
/// 而一个出口对应哪台主机是部署面的事。
fn delivery_error(
    outlet: &'static str,
    result: delivery::DeliveryResult,
    detail: impl std::fmt::Display,
) -> cog_core::SFError {
    cog_core::SFError::Adapter {
        provider: outlet.to_string(),
        message: format!("delivery {result}: {detail}"),
    }
}

/// 平台机器人信封对"这次消息收没收下"的表态。
#[derive(Debug, PartialEq, Eq)]
enum EnvelopeVerdict {
    /// 信封说收下了。
    Accepted,
    /// 信封说没收下，附带平台自己那句话（只进日志与错误文本，不进标签）。
    Rejected(String),
    /// 信封没表态：报文不是 JSON，或者没有那个字段。我们解析不了是我们的问题，
    /// 不是平台拒收——按没拿到证据处理，沿用 HTTP 层已经给出的判断。
    Silent,
}

/// 读平台机器人信封里那个错误码。
///
/// 钉钉与企业微信共用 `{"errcode":0,"errmsg":"ok"}`，飞书自定义机器人用
/// `{"code":0,"msg":"success"}`（旧版是 `StatusCode`/`StatusMessage`，故接受数字
/// 与字符串两种写法）。**字段在且非 0 才算拒收**——这是这套读数里最像"已送达"
/// 的一种失败：HTTP 全绿而消息根本没进群，原因通常是关键词、签名或机器人被停用。
fn envelope_verdict(body: &[u8], code_fields: &[&str], message_fields: &[&str]) -> EnvelopeVerdict {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return EnvelopeVerdict::Silent;
    };
    let non_zero = |v: &serde_json::Value| -> bool {
        match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) != 0,
            serde_json::Value::String(s) => !s.is_empty() && s != "0",
            _ => false,
        }
    };
    for field in code_fields {
        if let Some(code) = value.get(*field) {
            if !non_zero(code) {
                return EnvelopeVerdict::Accepted;
            }
            let message = message_fields
                .iter()
                .find_map(|f| value.get(*f).and_then(|m| m.as_str()))
                .unwrap_or("");
            return EnvelopeVerdict::Rejected(format!("{field}={code} {message}"));
        }
    }
    EnvelopeVerdict::Silent
}

/// In-memory notification store backed by a [`Vec`] inside a [`RwLock`].
#[derive(Debug, Clone, Default)]
pub struct InMemoryNotificationStore {
    inner: Arc<RwLock<Vec<Notification>>>,
}

impl InMemoryNotificationStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl NotificationStore for InMemoryNotificationStore {
    async fn list(&self, filter: NotificationFilter) -> SFResult<NotificationList> {
        let guard = self.inner.read().await;
        let mut items: Vec<Notification> = if filter.unread_only {
            guard.iter().filter(|n| !n.is_read).cloned().collect()
        } else {
            guard.iter().cloned().collect()
        };

        items.sort_by_key(|a| std::cmp::Reverse(a.created_at));

        let unread_count = guard.iter().filter(|n| !n.is_read).count() as u32;
        let limit = filter.limit.min(1000);
        let has_more = items.len() > limit;
        let items: Vec<Notification> = items.into_iter().take(limit).collect();

        let next_cursor = if has_more {
            items.last().map(|n| n.id.clone())
        } else {
            None
        };

        debug!(
            unread_only = filter.unread_only,
            limit = limit,
            returned = items.len(),
            unread_count = unread_count,
            "Listed notifications"
        );

        Ok(NotificationList {
            items,
            unread_count,
            next_cursor,
            has_more,
        })
    }

    async fn mark_read(&self, id: &str) -> SFResult<bool> {
        let mut guard = self.inner.write().await;
        match guard.iter_mut().find(|n| n.id == id) {
            Some(n) => {
                n.is_read = true;
                n.read_at = Some(chrono::Utc::now());
                debug!(notification_id = %id, "Marked notification as read");
                Ok(true)
            }
            None => {
                warn!(notification_id = %id, "Notification not found for mark_read");
                Ok(false)
            }
        }
    }

    async fn mark_all_read(&self) -> SFResult<usize> {
        let mut guard = self.inner.write().await;
        let now = chrono::Utc::now();
        let mut count = 0;
        for n in guard.iter_mut() {
            if !n.is_read {
                n.is_read = true;
                n.read_at = Some(now);
                count += 1;
            }
        }
        debug!(count = count, "Marked all notifications as read");
        Ok(count)
    }

    async fn create(&self, notification: Notification) -> SFResult<()> {
        let mut guard = self.inner.write().await;
        guard.push(notification);
        Ok(())
    }
}

/// Broadcast-based dispatcher — pushes notifications to all active
/// WebSocket subscribers via a [`tokio::sync::broadcast`] channel.
/// Gateway layer spawns receivers and forwards each [`Notification`] as a
/// `ServerMessage::Notification` over the WebSocket connection.
#[derive(Debug, Clone)]
pub struct BroadcastDispatcher {
    tx: tokio::sync::broadcast::Sender<cog_core::Notification>,
}

impl BroadcastDispatcher {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = tokio::sync::broadcast::channel(capacity);
        Self { tx }
    }

    /// Subscribe to notification broadcasts.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::Notification> {
        self.tx.subscribe()
    }

    /// Return a clone of the internal sender (useful when the sender needs to
    /// be stored separately in the gateway state).
    pub fn sender(&self) -> tokio::sync::broadcast::Sender<cog_core::Notification> {
        self.tx.clone()
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for BroadcastDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let _ = self.tx.send(notification.clone());
        Ok(())
    }
}

/// Webhook dispatcher — forwards notifications to an external HTTP endpoint.
/// Uses [`cog_core::HttpClient`] so it does **not** depend on `cog-net`
/// directly, preserving the star architecture.
#[derive(Debug, Clone)]
pub struct WebhookDispatcher {
    http_client: std::sync::Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
    headers: std::collections::HashMap<String, String>,
    /// 读数与错误都按出口归属，所以出口名随构造进来：它是这台 dispatcher 的
    /// 身份，不是调用时才知道的东西。
    outlet: &'static str,
}

impl WebhookDispatcher {
    pub fn new(
        http_client: std::sync::Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        outlet: &'static str,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            headers: std::collections::HashMap::new(),
            outlet,
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for WebhookDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let body = serde_json::to_vec(notification).map_err(cog_core::SFError::Serialization)?;
        let mut req = cog_core::HttpRequest::post(&self.webhook_url)
            .header("Content-Type", "application/json");
        req.body = Some(body);
        for (k, v) in &self.headers {
            req = req.header(k.clone(), v.clone());
        }
        // 接收方是任意的自定义端点，没有可断言的报文契约：2xx 就是收下了。
        dispatch_http(&*self.http_client, req, self.outlet, None).await
    }
}

/// 执行一次投递并把结果记下来，失败时返回错误。
///
/// 这是四种出口共用的那一段：结果按出口各记一格，然后**如实返回**——返回
/// `Ok(())` 而实际失败，就是让调用方再也无法知道通知没送到。错误只在这里返回、
/// 不在这里打日志，免得同一个失败被内外两层各记一行。
async fn dispatch_http(
    http_client: &dyn cog_core::HttpClient,
    req: cog_core::HttpRequest,
    outlet: &'static str,
    envelope: Option<(&[&str], &[&str])>,
) -> cog_core::SFResult<()> {
    let resp = match http_client.execute(req).await {
        Ok(resp) => resp,
        Err(e) => {
            delivery::record(outlet, delivery::DeliveryResult::Unreachable);
            return Err(delivery_error(
                outlet,
                delivery::DeliveryResult::Unreachable,
                &e,
            ));
        }
    };
    let mut result = transport_outcome(&resp);
    let mut detail = format!("HTTP {}", resp.status);
    if result == delivery::DeliveryResult::Ok {
        if let Some((code_fields, message_fields)) = envelope {
            if let EnvelopeVerdict::Rejected(why) =
                envelope_verdict(&resp.body, code_fields, message_fields)
            {
                result = delivery::DeliveryResult::EnvelopeError;
                detail = format!(
                    "HTTP {} but the receiver's own envelope refused it: {why}",
                    resp.status
                );
            }
        }
    }
    delivery::record(outlet, result);
    if result == delivery::DeliveryResult::Ok {
        Ok(())
    } else {
        Err(delivery_error(outlet, result, detail))
    }
}

/// Chains multiple dispatchers so that a single `dispatch` call fans out to
/// every configured channel (broadcast + webhook + future channels).
#[derive(Debug, Clone)]
pub struct MultiDispatcher {
    inner: Vec<std::sync::Arc<dyn cog_core::NotificationDispatcher>>,
}

impl MultiDispatcher {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add(mut self, dispatcher: std::sync::Arc<dyn cog_core::NotificationDispatcher>) -> Self {
        self.inner.push(dispatcher);
        self
    }
}

impl Default for MultiDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for MultiDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let mut failed = 0usize;
        let mut first: Option<cog_core::SFError> = None;
        for d in &self.inner {
            // 一个出口发不出去不影响下一个：每次投递都要走完整份名单。
            match d.dispatch(notification).await {
                Ok(()) => {}
                Err(e) => {
                    // 每个出口只在这里记一行。出口名在错误的 `provider` 里，不另
                    // 打一份 dispatcher 的 Debug——那里面带着投递地址本身。
                    tracing::warn!(error = %e, "Notification outlet did not receive this notification");
                    failed += 1;
                    first.get_or_insert(e);
                }
            }
        }
        match (failed, first) {
            (0, _) => Ok(()),
            (n, Some(e)) => Err(cog_core::SFError::Adapter {
                provider: format!("{n} outlet(s)"),
                message: e.to_string(),
            }),
            (_, None) => Ok(()),
        }
    }
}

// ─── Platform-specific webhook dispatchers ───

fn hmac_sha256_base64(secret: &str, data: &str) -> String {
    type HmacSha256 = Hmac<Sha256>;
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(data.as_bytes());
    let result = mac.finalize();
    let bytes = result.into_bytes();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// DingTalk (钉钉) robot webhook dispatcher.
#[derive(Debug, Clone)]
pub struct DingTalkDispatcher {
    http_client: Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
    secret: Option<String>,
    outlet: &'static str,
}

impl DingTalkDispatcher {
    pub fn new(
        http_client: Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        secret: Option<String>,
        outlet: &'static str,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            secret,
            outlet,
        }
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for DingTalkDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let mut url = self.webhook_url.clone();
        if let Some(ref secret) = self.secret {
            let timestamp = chrono::Utc::now().timestamp_millis();
            let sign = hmac_sha256_base64(secret, &format!("{}\n{}", timestamp, secret));
            let sign_encoded = sign
                .replace('+', "%2B")
                .replace('/', "%2F")
                .replace('=', "%3D");
            url.push_str(&format!("&timestamp={}&sign={}", timestamp, sign_encoded));
        }

        let payload = serde_json::json!({
            "msgtype": "markdown",
            "markdown": {
                "title": &notification.title,
                "text": format!("### {}\n\n{}", notification.title, notification.body),
            }
        });
        let body = serde_json::to_vec(&payload).map_err(cog_core::SFError::Serialization)?;
        let req = cog_core::HttpRequest::post(&url)
            .header("Content-Type", "application/json")
            .body(body);

        dispatch_http(
            &*self.http_client,
            req,
            self.outlet,
            Some((&["errcode"], &["errmsg"])),
        )
        .await
    }
}

/// Feishu (Lark / 飞书) robot webhook dispatcher.
#[derive(Debug, Clone)]
pub struct FeishuDispatcher {
    http_client: Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
    secret: Option<String>,
    outlet: &'static str,
}

impl FeishuDispatcher {
    pub fn new(
        http_client: Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        secret: Option<String>,
        outlet: &'static str,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            secret,
            outlet,
        }
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for FeishuDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let sign = self
            .secret
            .as_ref()
            .map(|secret| hmac_sha256_base64(secret, &format!("{}\n{}", timestamp, secret)));

        let payload = serde_json::json!({
            "timestamp": &timestamp,
            "sign": sign,
            "msg_type": "text",
            "content": {
                "text": format!("{}\n\n{}", notification.title, notification.body),
            }
        });
        let body = serde_json::to_vec(&payload).map_err(cog_core::SFError::Serialization)?;
        let req = cog_core::HttpRequest::post(&self.webhook_url)
            .header("Content-Type", "application/json")
            .body(body);

        dispatch_http(
            &*self.http_client,
            req,
            self.outlet,
            Some((&["code", "StatusCode"], &["msg", "StatusMessage"])),
        )
        .await
    }
}

/// WeChat Work (企业微信) robot webhook dispatcher.
#[derive(Debug, Clone)]
pub struct WeChatWorkDispatcher {
    http_client: Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
    outlet: &'static str,
}

impl WeChatWorkDispatcher {
    pub fn new(
        http_client: Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        outlet: &'static str,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            outlet,
        }
    }
}

#[async_trait]
impl cog_core::NotificationDispatcher for WeChatWorkDispatcher {
    async fn dispatch(&self, notification: &cog_core::Notification) -> cog_core::SFResult<()> {
        let payload = serde_json::json!({
            "msgtype": "markdown",
            "markdown": {
                "content": format!("**{}**\n\n{}", notification.title, notification.body),
            }
        });
        let body = serde_json::to_vec(&payload).map_err(cog_core::SFError::Serialization)?;
        let req = cog_core::HttpRequest::post(&self.webhook_url)
            .header("Content-Type", "application/json")
            .body(body);

        dispatch_http(
            &*self.http_client,
            req,
            self.outlet,
            Some((&["errcode"], &["errmsg"])),
        )
        .await
    }
}

pub mod plugin;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use cog_core::{Notification, NotificationDispatcher as _};

    fn make_notification(id: &str, is_read: bool) -> Notification {
        Notification {
            id: id.into(),
            title: format!("Title {}", id),
            body: format!("Body {}", id),
            is_read,
            created_at: Utc::now(),
            read_at: None,
        }
    }

    /// 固定的接收方：要么答一份写死的报文，要么在传输层就失败。
    ///
    /// 只读那份报文本身，不做任何额外判断——本文件里"什么算送达"的判据都在被测
    /// 代码里，替身只负责把它看到的东西交出去。
    #[derive(Debug)]
    struct StubUpstream {
        outcome: Result<(u16, &'static str), &'static str>,
    }

    #[async_trait]
    impl cog_core::HttpClient for StubUpstream {
        async fn execute(
            &self,
            _req: cog_core::HttpRequest,
        ) -> cog_core::SFResult<cog_core::HttpResponse> {
            match self.outcome {
                Ok((status, body)) => Ok(cog_core::HttpResponse {
                    status,
                    headers: std::collections::HashMap::new(),
                    body: body.as_bytes().to_vec(),
                }),
                Err(message) => Err(cog_core::SFError::IO(message.to_string())),
            }
        }
    }

    fn stub(outcome: Result<(u16, &'static str), &'static str>) -> Arc<dyn cog_core::HttpClient> {
        Arc::new(StubUpstream { outcome })
    }

    /// 某个出口某个结果当前的累计次数。
    ///
    /// 读的是进程表：投递动作记进去的就是这一张，替身换成局部表就测不到真相。
    fn counted(outlet: &str, result: delivery::DeliveryResult) -> u64 {
        delivery::registry().count(outlet, result)
    }

    /// 平台回了 2xx，报文却说这次没收下——这是最像"已送达"的一种失败：
    /// HTTP 全绿，消息根本没进群。它必须是失败，而且必须被单独计数。
    #[tokio::test]
    async fn a_robot_envelope_that_refuses_the_message_is_a_failure() {
        const OUTLET: &str = "test-robot-refused";
        let d = DingTalkDispatcher::new(
            stub(Ok((
                200,
                r#"{"errcode":310000,"errmsg":"keywords not in content"}"#,
            ))),
            "https://example.invalid/hook",
            None,
            OUTLET,
        );
        let before = counted(OUTLET, delivery::DeliveryResult::EnvelopeError);
        let err = d
            .dispatch(&make_notification("n1", false))
            .await
            .expect_err("报文说没收下，就不是成功");
        assert!(
            err.to_string().contains("envelope_error"),
            "错误要说出是哪一层失败的: {err}"
        );
        assert_eq!(
            counted(OUTLET, delivery::DeliveryResult::EnvelopeError),
            before + 1
        );
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::Ok), 0);
    }

    /// 对照组：信封说收下了，就是成功，且不落在任何失败格子里。
    #[tokio::test]
    async fn an_accepted_envelope_is_a_delivery() {
        const OUTLET: &str = "test-robot-accepted";
        let d = DingTalkDispatcher::new(
            stub(Ok((200, r#"{"errcode":0,"errmsg":"ok"}"#))),
            "https://example.invalid/hook",
            None,
            OUTLET,
        );
        d.dispatch(&make_notification("n2", false))
            .await
            .expect("收下了");
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::Ok), 1);
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::EnvelopeError), 0);
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::Unreachable), 0);
    }

    /// 三种失败互相分开：没得到答复、答复了但不是 2xx、报文自己拒收。它们的
    /// 成因与处置人不同，合成一格会让最该区分的那次失败变成四个字。
    #[tokio::test]
    async fn the_three_ways_to_fail_are_counted_apart() {
        for (name, outcome, expected) in [
            (
                "test-parent-unreachable",
                Err("connection refused"),
                delivery::DeliveryResult::Unreachable,
            ),
            (
                "test-parent-http-error",
                Ok((503, r#"{"errcode":0}"#)),
                delivery::DeliveryResult::HttpError,
            ),
        ] {
            let d =
                DingTalkDispatcher::new(stub(outcome), "https://example.invalid/hook", None, name);
            let before = counted(name, expected);
            assert!(
                d.dispatch(&make_notification("n3", false)).await.is_err(),
                "{name}: {expected} 必须是失败"
            );
            assert_eq!(counted(name, expected), before + 1, "{name}");
            assert_eq!(counted(name, delivery::DeliveryResult::Ok), 0, "{name}");
        }
    }

    /// 报文读不懂不算拒收：那是我们解析不了，不是平台说没收到。判错这个方向会
    /// 把一次已经送达的通知报成失败。
    #[tokio::test]
    async fn a_body_we_cannot_read_is_not_a_refusal() {
        const OUTLET: &str = "test-unreadable-body";
        let d = DingTalkDispatcher::new(
            stub(Ok((200, "<html>gateway</html>"))),
            "https://example.invalid/hook",
            None,
            OUTLET,
        );
        d.dispatch(&make_notification("n4", false))
            .await
            .expect("HTTP 2xx 而没有表态，按送达算");
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::Ok), 1);
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::EnvelopeError), 0);
    }

    /// 飞书的信封是它自己那套字段名（`code`/`msg`，旧版 `StatusCode`）。
    /// 只认一个平台的字段名会让另一个平台的拒收读成成功。
    #[tokio::test]
    async fn each_platform_envelope_is_read_by_its_own_field_names() {
        const OUTLET: &str = "test-feishu-envelope";
        let d = FeishuDispatcher::new(
            stub(Ok((200, r#"{"code":19021,"msg":"sign match fail"}"#))),
            "https://example.invalid/hook",
            None,
            OUTLET,
        );
        let err = d
            .dispatch(&make_notification("n5", false))
            .await
            .expect_err("code 非 0 是拒收");
        assert!(err.to_string().contains("19021"), "{err}");

        const LEGACY: &str = "test-feishu-legacy-envelope";
        let d = FeishuDispatcher::new(
            stub(Ok((
                200,
                r#"{"StatusCode":9499,"StatusMessage":"Bad Request"}"#,
            ))),
            "https://example.invalid/hook",
            None,
            LEGACY,
        );
        assert!(d.dispatch(&make_notification("n6", false)).await.is_err());
    }

    /// 通用 webhook 的接收方是任意的，没有可断言的报文契约：它回了 2xx 就是
    /// 收下了，哪怕体内写着某个平台自己的错误码——替它认那是替别人猜。
    #[tokio::test]
    async fn the_generic_webhook_has_no_envelope_to_read() {
        const OUTLET: &str = "test-generic-webhook";
        let d = WebhookDispatcher::new(
            stub(Ok((
                200,
                r#"{"errcode":310000,"errmsg":"keywords not in content"}"#,
            ))),
            "https://example.invalid/hook",
            OUTLET,
        );
        d.dispatch(&make_notification("n7", false))
            .await
            .expect("2xx 就是收下了");
        assert_eq!(counted(OUTLET, delivery::DeliveryResult::Ok), 1);
    }

    /// 一个出口发不出去不影响下一个，但整次投递要如实报成失败——过去这里对每
    /// 个子出口都吞掉错误、最后返 `Ok(())`，于是调用方永远不知道有人没收到。
    #[tokio::test]
    async fn one_failing_outlet_does_not_stop_the_others_and_the_fan_out_reports_it() {
        const BAD: &str = "test-fanout-bad";
        const GOOD: &str = "test-fanout-good";
        let multi = MultiDispatcher::new()
            .add(Arc::new(DingTalkDispatcher::new(
                stub(Err("connection refused")),
                "https://example.invalid/bad",
                None,
                BAD,
            )))
            .add(Arc::new(DingTalkDispatcher::new(
                stub(Ok((200, r#"{"errcode":0,"errmsg":"ok"}"#))),
                "https://example.invalid/good",
                None,
                GOOD,
            )));

        let err = multi
            .dispatch(&make_notification("n8", false))
            .await
            .expect_err("有一个出口没收到，整次投递就是失败");
        assert!(err.to_string().contains("1 outlet"), "{err}");
        // 后一个出口照样收到了：名单要走完，失败不许短路。
        assert_eq!(counted(GOOD, delivery::DeliveryResult::Ok), 1);
        assert_eq!(counted(BAD, delivery::DeliveryResult::Unreachable), 1);
    }

    #[tokio::test]
    async fn list_all_sorted_descending() {
        let store = InMemoryNotificationStore::new();
        store.create(make_notification("a", false)).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        store.create(make_notification("b", false)).await.unwrap();

        let list = store
            .list(NotificationFilter {
                unread_only: false,
                limit: 10,
                cursor: None,
            })
            .await
            .unwrap();

        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].id, "b");
        assert_eq!(list.items[1].id, "a");
    }

    #[tokio::test]
    async fn mark_read_existing() {
        let store = InMemoryNotificationStore::new();
        store.create(make_notification("x", false)).await.unwrap();

        let found = store.mark_read("x").await.unwrap();
        assert!(found);

        let list = store
            .list(NotificationFilter {
                unread_only: true,
                limit: 10,
                cursor: None,
            })
            .await
            .unwrap();
        assert_eq!(list.items.len(), 0);
    }

    #[tokio::test]
    async fn mark_read_missing() {
        let store = InMemoryNotificationStore::new();
        let found = store.mark_read("missing").await.unwrap();
        assert!(!found);
    }

    #[tokio::test]
    async fn mark_all_read_counts_correctly() {
        let store = InMemoryNotificationStore::new();
        store.create(make_notification("a", false)).await.unwrap();
        store.create(make_notification("b", false)).await.unwrap();
        store.create(make_notification("c", true)).await.unwrap();

        let count = store.mark_all_read().await.unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn pagination_limit_and_has_more() {
        let store = InMemoryNotificationStore::new();
        for i in 0..5 {
            store
                .create(make_notification(&format!("n{}", i), false))
                .await
                .unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        let list = store
            .list(NotificationFilter {
                unread_only: false,
                limit: 2,
                cursor: None,
            })
            .await
            .unwrap();

        assert_eq!(list.items.len(), 2);
        assert!(list.has_more);
        assert!(list.next_cursor.is_some());
    }
}
