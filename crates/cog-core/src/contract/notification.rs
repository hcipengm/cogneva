//!Notification domain types and store trait.
//!The [`NotificationStore`] trait lives in `cog-core` so that the gateway
//!(and any other crate) can depend on the abstraction without pulling in
//!concrete implementations.

use crate::SFResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A user-facing notification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Notification {
    pub id: String,
    pub title: String,
    pub body: String,
    pub is_read: bool,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

/// Filter arguments for listing notifications.
#[derive(Debug, Clone, Default)]
pub struct NotificationFilter {
    pub unread_only: bool,
    pub limit: usize,
    pub cursor: Option<String>,
}

/// Paginated result from [`NotificationStore::list`].
#[derive(Debug, Clone)]
pub struct NotificationList {
    pub items: Vec<Notification>,
    pub unread_count: u32,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

/// Abstraction over notification persistence.
#[async_trait]
pub trait NotificationStore: Send + Sync {
    /// List notifications matching the filter, sorted by `created_at` descending.
    async fn list(&self, filter: NotificationFilter) -> SFResult<NotificationList>;

    /// Mark a single notification as read. Returns `true` if the ID was found.
    async fn mark_read(&self, id: &str) -> SFResult<bool>;

    /// Mark all unread notifications as read. Returns the number affected.
    async fn mark_all_read(&self) -> SFResult<usize>;

    /// Create a new notification.
    async fn create(&self, notification: Notification) -> SFResult<()>;
}

/// Abstraction over notification delivery (WebSocket push, webhook, email, etc.).
/// Implementations live in `cog-notification` so that `cog-core` remains
/// dependency-free.  The dispatcher is invoked *after* the notification has
/// been persisted via [`NotificationStore::create`].
#[async_trait]
pub trait NotificationDispatcher: Send + Sync + std::fmt::Debug {
    /// Deliver the notification to external channels.
    async fn dispatch(&self, notification: &Notification) -> SFResult<()>;
}
