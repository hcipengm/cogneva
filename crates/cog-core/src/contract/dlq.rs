//!Dead-letter queue (DLQ) trait and domain types.
//!Concrete implementations (Redis, in-memory) live in backend crates
//!(`cog-storage`, `cog-stream`) so that consumers only depend on this
//!abstraction.

use crate::SFResult;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Record of a single retry attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RetryAttempt {
    pub attempt: u32,
    pub error: String,
    pub timestamp: DateTime<Utc>,
}

/// An entry in the dead-letter queue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeadLetterEntry {
    pub original_task_id: String,
    pub task: crate::Task,
    pub final_error: String,
    pub retry_history: Vec<RetryAttempt>,
    pub enqueued_at: DateTime<Utc>,
    pub suggested_action: SuggestedAction,
}

/// Suggested remediation for a DLQ item.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum SuggestedAction {
    /// Manual retry by admin.
    #[default]
    ManualRetry,
    /// Skip this task and continue DAG.
    Skip,
    /// Escalate to higher-level supervisor.
    Escalate,
}

/// Abstract dead-letter queue.
#[async_trait]
pub trait DeadLetterQueue: Send + Sync {
    /// Push a failed task into the DLQ.
    async fn enqueue(&self, entry: DeadLetterEntry) -> SFResult<()>;

    /// Pop the oldest DLQ entry.
    async fn dequeue(&self) -> SFResult<Option<DeadLetterEntry>>;

    /// List entries (oldest first) up to `limit`.
    async fn list(&self, limit: usize) -> SFResult<Vec<DeadLetterEntry>>;

    /// Re-queue (remove and return) a specific DLQ entry by task ID.
    async fn replay(&self, task_id: &str) -> SFResult<Option<DeadLetterEntry>>;

    /// Count of items in the DLQ.
    async fn len(&self) -> SFResult<usize>;

    /// Whether the DLQ is empty.
    async fn is_empty(&self) -> SFResult<bool> {
        Ok(self.len().await? == 0)
    }

    /// Remove all entries older than the given duration (admin cleanup).
    async fn purge_older_than(&self, _max_age: Duration) -> SFResult<usize> {
        // Default no-op; concrete impls may override.
        Ok(0)
    }
}
