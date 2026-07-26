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
    /// be stored separately in [`GatewayState`]).
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
}

impl WebhookDispatcher {
    pub fn new(
        http_client: std::sync::Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            headers: std::collections::HashMap::new(),
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
        match self.http_client.execute(req).await {
            Ok(resp) if resp.is_success() => Ok(()),
            Ok(resp) => {
                tracing::warn!(
                    webhook_url = %self.webhook_url,
                    status = resp.status,
                    "Notification webhook returned non-success status"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    webhook_url = %self.webhook_url,
                    error = %e,
                    "Notification webhook dispatch failed"
                );
                Ok(())
            }
        }
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
        for d in &self.inner {
            if let Err(e) = d.dispatch(notification).await {
                tracing::warn!(error = %e, "Notification sub-dispatcher failed");
            }
        }
        Ok(())
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
}

impl DingTalkDispatcher {
    pub fn new(
        http_client: Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        secret: Option<String>,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            secret,
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

        match self.http_client.execute(req).await {
            Ok(resp) if resp.is_success() => Ok(()),
            Ok(resp) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    status = resp.status,
                    "DingTalk webhook returned non-success status"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    error = %e,
                    "DingTalk webhook dispatch failed"
                );
                Ok(())
            }
        }
    }
}

/// Feishu (Lark / 飞书) robot webhook dispatcher.
#[derive(Debug, Clone)]
pub struct FeishuDispatcher {
    http_client: Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
    secret: Option<String>,
}

impl FeishuDispatcher {
    pub fn new(
        http_client: Arc<dyn cog_core::HttpClient>,
        webhook_url: impl Into<String>,
        secret: Option<String>,
    ) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
            secret,
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

        match self.http_client.execute(req).await {
            Ok(resp) if resp.is_success() => Ok(()),
            Ok(resp) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    status = resp.status,
                    "Feishu webhook returned non-success status"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    error = %e,
                    "Feishu webhook dispatch failed"
                );
                Ok(())
            }
        }
    }
}

/// WeChat Work (企业微信) robot webhook dispatcher.
#[derive(Debug, Clone)]
pub struct WeChatWorkDispatcher {
    http_client: Arc<dyn cog_core::HttpClient>,
    webhook_url: String,
}

impl WeChatWorkDispatcher {
    pub fn new(http_client: Arc<dyn cog_core::HttpClient>, webhook_url: impl Into<String>) -> Self {
        Self {
            http_client,
            webhook_url: webhook_url.into(),
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

        match self.http_client.execute(req).await {
            Ok(resp) if resp.is_success() => Ok(()),
            Ok(resp) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    status = resp.status,
                    "WeChat Work webhook returned non-success status"
                );
                Ok(())
            }
            Err(e) => {
                tracing::warn!(
                    url = %self.webhook_url,
                    error = %e,
                    "WeChat Work webhook dispatch failed"
                );
                Ok(())
            }
        }
    }
}

pub mod plugin;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use cog_core::Notification;

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
