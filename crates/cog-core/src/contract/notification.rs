//!Notification domain types and store trait.
//!The [`NotificationStore`] trait lives in `cog-core` so that the gateway
//!(and any other crate) can depend on the abstraction without pulling in
//!concrete implementations.

use crate::SFResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What a notification is about.
///
/// The variants are the ones the `notifications` table accepts and
/// [`Self::as_str`] spells them the way its check constraint does. The set is
/// closed here rather than left to each producer because a kind with no
/// spelling there is a row no persistent store can write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationType {
    TaskCompleted,
    QuotaWarning,
    Mention,
    System,
}

impl NotificationType {
    /// Canonical storage representation (matches the `notifications.ntype` column).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TaskCompleted => "task_completed",
            Self::QuotaWarning => "quota_warning",
            Self::Mention => "mention",
            Self::System => "system",
        }
    }

    /// Inverse of [`Self::as_str`]. Unknown values fall back to [`Self::System`]:
    /// a row whose kind this build does not know is still a notification of
    /// some kind, and dropping it would make the list shorter than the table.
    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "task_completed" => Self::TaskCompleted,
            "quota_warning" => Self::QuotaWarning,
            "mention" => Self::Mention,
            _ => Self::System,
        }
    }
}

/// A user-facing notification.
///
/// A notification belongs to exactly one user. The `notifications` table makes
/// that column `NOT NULL` with no default, so "a notification for nobody" is
/// not a row that can exist — and a read that is not told which user to answer
/// for has no question to answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Notification {
    /// The row's identity.
    ///
    /// The table stores it as a UUID, so an id that is not one is a
    /// notification no persistent store can write. A store that assigns
    /// identity itself may ignore what it is handed.
    pub id: String,
    /// The user this is for, spelled as a UUID string.
    pub user_id: String,
    pub ntype: NotificationType,
    pub title: String,
    pub body: String,
    pub is_read: bool,
    pub created_at: DateTime<Utc>,
    pub read_at: Option<DateTime<Utc>>,
}

/// Filter arguments for listing notifications.
///
/// The owner is deliberately not a field here: it is a required argument of
/// [`NotificationStore::list`]. A filter can be defaulted and an owner cannot,
/// and the difference matters — see the trait.
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
///
/// Every read takes the **owner** as its own argument rather than trusting a
/// caller to put one in a filter. A store asked only for a notification id
/// answers for every user in the deployment, and the caller that forgot to
/// scope is not the one who can tell: an unscoped answer looks exactly like a
/// scoped one, only longer.
#[async_trait]
pub trait NotificationStore: Send + Sync {
    /// List `user_id`'s notifications matching the filter, sorted by
    /// `created_at` descending.
    async fn list(&self, user_id: &str, filter: NotificationFilter) -> SFResult<NotificationList>;

    /// Mark one of `user_id`'s notifications as read. Returns `true` if the ID
    /// was found **and** belongs to that user.
    async fn mark_read(&self, user_id: &str, id: &str) -> SFResult<bool>;

    /// Mark all of `user_id`'s unread notifications as read. Returns the number
    /// affected.
    async fn mark_all_read(&self, user_id: &str) -> SFResult<usize>;

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

/// Persist a notification, then hand it to the dispatcher.
///
/// The order is the contract's, not a caller's to arrange: what
/// [`NotificationDispatcher`] documents above is a rule about a sequence, and a
/// sequence written out again at every call site is a rule each of them can get
/// wrong on its own. The one that gets it wrong is not visibly broken — it
/// pushes a notification to a channel and never stores it, and the person who
/// opened that channel finds the list it points at does not contain what they
/// were just told.
///
/// A failed store stops the delivery rather than being logged past it: the
/// notification that would be pushed is one the reader cannot look up
/// afterwards, which is the state this function exists to prevent.
///
/// Either side may be absent — a store and no channel, or a channel and no
/// store — and absent means "skip", not "fail": the composition of what a
/// deployment has is the deployment's business. A caller that needs a backend
/// to exist checks for it itself.
pub async fn persist_then_dispatch(
    store: Option<&std::sync::Arc<dyn NotificationStore>>,
    dispatcher: Option<&std::sync::Arc<dyn NotificationDispatcher>>,
    notification: Notification,
) -> SFResult<()> {
    if let Some(store) = store {
        store.create(notification.clone()).await?;
    }
    if let Some(dispatcher) = dispatcher {
        dispatcher.dispatch(&notification).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// One party that is both the store and the channel, writing each call into
    /// a single log. One log rather than two is the point: the claim here is
    /// about the order between the two calls, and two separate records cannot
    /// show an order at all -- a body that dispatched first would still leave
    /// `store` holding one entry and `dispatch` holding one entry.
    #[derive(Default)]
    struct Recorder {
        log: Arc<Mutex<Vec<String>>>,
        fails: bool,
    }

    impl std::fmt::Debug for Recorder {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Recorder")
                .field("fails", &self.fails)
                .finish()
        }
    }

    impl Recorder {
        fn as_store(&self) -> Arc<dyn NotificationStore> {
            Arc::new(Self {
                log: Arc::clone(&self.log),
                fails: self.fails,
            })
        }

        fn as_dispatcher(&self) -> Arc<dyn NotificationDispatcher> {
            Arc::new(Self {
                log: Arc::clone(&self.log),
                fails: false,
            })
        }

        fn calls(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl NotificationStore for Recorder {
        async fn list(
            &self,
            _user_id: &str,
            _filter: NotificationFilter,
        ) -> SFResult<NotificationList> {
            unimplemented!("not exercised here")
        }
        async fn mark_read(&self, _user_id: &str, _id: &str) -> SFResult<bool> {
            unimplemented!("not exercised here")
        }
        async fn mark_all_read(&self, _user_id: &str) -> SFResult<usize> {
            unimplemented!("not exercised here")
        }
        async fn create(&self, _notification: Notification) -> SFResult<()> {
            self.log.lock().unwrap().push("store".into());
            if self.fails {
                return Err(crate::SFError::Agent("the store is down".into()));
            }
            Ok(())
        }
    }

    #[async_trait]
    impl NotificationDispatcher for Recorder {
        async fn dispatch(&self, _notification: &Notification) -> SFResult<()> {
            self.log.lock().unwrap().push("dispatch".into());
            Ok(())
        }
    }

    fn notification() -> Notification {
        Notification {
            id: "00000000-0000-4000-8000-000000000001".into(),
            user_id: "00000000-0000-4000-8000-0000000000ff".into(),
            ntype: NotificationType::System,
            title: "t".into(),
            body: "b".into(),
            is_read: false,
            created_at: Utc::now(),
            read_at: None,
        }
    }

    #[tokio::test]
    async fn the_store_is_written_before_the_channel_is_told() {
        let recorder = Recorder::default();

        persist_then_dispatch(
            Some(&recorder.as_store()),
            Some(&recorder.as_dispatcher()),
            notification(),
        )
        .await
        .expect("both sides accept");

        assert_eq!(
            recorder.calls(),
            vec!["store".to_string(), "dispatch".to_string()],
            "the channel is told once, and only after the notification is stored"
        );
    }

    /// The case the rule exists for: a notification that could not be stored is
    /// never pushed to a channel. One that was pushed anyway reads, to whoever
    /// saw it, exactly like one the list would show them.
    #[tokio::test]
    async fn a_notification_that_could_not_be_stored_is_not_dispatched() {
        let recorder = Recorder {
            fails: true,
            ..Default::default()
        };

        let outcome = persist_then_dispatch(
            Some(&recorder.as_store()),
            Some(&recorder.as_dispatcher()),
            notification(),
        )
        .await;

        assert!(outcome.is_err(), "the store refused, so this cannot be Ok");
        assert_eq!(
            recorder.calls(),
            vec!["store".to_string()],
            "nothing may reach a channel that the list will not show"
        );
    }

    /// A deployment with only one of the two backends is not an error: absent
    /// means skip. Checked in both directions, because a missing side is easy
    /// to read as "nothing to do" and drop the other side with it.
    #[tokio::test]
    async fn either_backend_alone_still_does_its_half() {
        let store = Recorder::default();
        persist_then_dispatch(Some(&store.as_store()), None, notification())
            .await
            .expect("a store with no channel is a deployment, not a failure");
        assert_eq!(store.calls(), vec!["store".to_string()]);

        let dispatcher = Recorder::default();
        persist_then_dispatch(None, Some(&dispatcher.as_dispatcher()), notification())
            .await
            .expect("a channel with no store is a deployment, not a failure");
        assert_eq!(dispatcher.calls(), vec!["dispatch".to_string()]);
    }
}
