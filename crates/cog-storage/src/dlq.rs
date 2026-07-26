//! Dead-letter queue implementations.
//! - [`RedisDeadLetterQueue`] — persistent Redis list backend.
//! - [`MemoryDeadLetterQueue`] — in-memory backend for testing.

use async_trait::async_trait;
use chrono::Utc;
use cog_core::{DeadLetterEntry, DeadLetterQueue, SFError, SFResult};
use std::sync::Mutex;
use std::time::Duration;

#[cfg(feature = "redis")]
use redis::AsyncCommands;

// ─── Redis ──────────────────────────────────────────────────────────────────

/// Redis-backed dead-letter queue (uses a Redis list).
#[cfg(feature = "redis")]
pub struct RedisDeadLetterQueue {
    connection: redis::aio::MultiplexedConnection,
    queue_key: String,
}

#[cfg(feature = "redis")]
impl RedisDeadLetterQueue {
    pub async fn new(redis_url: &str, queue_key: impl Into<String>) -> SFResult<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| SFError::Redis(e.to_string()))?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| SFError::Redis(e.to_string()))?;
        Ok(Self {
            connection,
            queue_key: queue_key.into(),
        })
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl DeadLetterQueue for RedisDeadLetterQueue {
    async fn enqueue(&self, entry: DeadLetterEntry) -> SFResult<()> {
        let json = serde_json::to_string(&entry).map_err(SFError::Serialization)?;
        let _: () = self
            .connection
            .clone()
            .lpush(&self.queue_key, &json)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        Ok(())
    }

    async fn dequeue(&self) -> SFResult<Option<DeadLetterEntry>> {
        let json: Option<String> = self
            .connection
            .clone()
            .rpop(&self.queue_key, None)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        match json {
            Some(s) => {
                let entry: DeadLetterEntry =
                    serde_json::from_str(&s).map_err(SFError::Serialization)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<DeadLetterEntry>> {
        let items: Vec<String> = self
            .connection
            .clone()
            .lrange(&self.queue_key, -(limit as isize), -1)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        let mut entries = Vec::with_capacity(items.len());
        for s in items {
            let entry: DeadLetterEntry =
                serde_json::from_str(&s).map_err(SFError::Serialization)?;
            entries.push(entry);
        }
        Ok(entries)
    }

    async fn replay(&self, task_id: &str) -> SFResult<Option<DeadLetterEntry>> {
        let items: Vec<String> = self
            .connection
            .clone()
            .lrange(&self.queue_key, 0, -1)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        for s in items {
            let entry: DeadLetterEntry =
                serde_json::from_str(&s).map_err(SFError::Serialization)?;
            if entry.original_task_id == task_id {
                let _: () = self
                    .connection
                    .clone()
                    .lrem(&self.queue_key, 0, &s)
                    .await
                    .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    async fn len(&self) -> SFResult<usize> {
        let count: i64 = self
            .connection
            .clone()
            .llen(&self.queue_key)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        Ok(count as usize)
    }

    async fn purge_older_than(&self, max_age: Duration) -> SFResult<usize> {
        let items: Vec<String> = self
            .connection
            .clone()
            .lrange(&self.queue_key, 0, -1)
            .await
            .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
        let mut removed = 0usize;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(max_age).unwrap_or(chrono::Duration::seconds(0));
        for s in items {
            if let Ok(entry) = serde_json::from_str::<DeadLetterEntry>(&s) {
                if entry.enqueued_at < cutoff {
                    let _: () = self
                        .connection
                        .clone()
                        .lrem(&self.queue_key, 0, &s)
                        .await
                        .map_err(|e: redis::RedisError| SFError::Redis(e.to_string()))?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }
}

// ─── In-memory ──────────────────────────────────────────────────────────────

/// In-memory dead-letter queue for testing.
pub struct MemoryDeadLetterQueue {
    entries: Mutex<Vec<DeadLetterEntry>>,
}

impl MemoryDeadLetterQueue {
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl Default for MemoryDeadLetterQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DeadLetterQueue for MemoryDeadLetterQueue {
    async fn enqueue(&self, entry: DeadLetterEntry) -> SFResult<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        entries.push(entry);
        Ok(())
    }

    async fn dequeue(&self) -> SFResult<Option<DeadLetterEntry>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        if entries.is_empty() {
            Ok(None)
        } else {
            // Remove oldest (FIFO)
            let entry = entries.remove(0);
            Ok(Some(entry))
        }
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<DeadLetterEntry>> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        Ok(entries.iter().take(limit).cloned().collect())
    }

    async fn replay(&self, task_id: &str) -> SFResult<Option<DeadLetterEntry>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        if let Some(pos) = entries.iter().position(|e| e.original_task_id == task_id) {
            let entry = entries.remove(pos);
            Ok(Some(entry))
        } else {
            Ok(None)
        }
    }

    async fn len(&self) -> SFResult<usize> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        Ok(entries.len())
    }

    async fn purge_older_than(&self, max_age: Duration) -> SFResult<usize> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| SFError::Agent("dlq lock poisoned".into()))?;
        let cutoff = Utc::now()
            - chrono::Duration::from_std(max_age).unwrap_or(chrono::Duration::seconds(0));
        let before = entries.len();
        entries.retain(|e| e.enqueued_at >= cutoff);
        Ok(before - entries.len())
    }
}
