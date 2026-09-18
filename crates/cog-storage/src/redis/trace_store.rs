//! Redis-backed `TraceStore` — hot tier (0-7 days, TTL eviction).

use async_trait::async_trait;
use cog_core::{AgentTrace, SFError, SFResult, TraceMeta, TraceStore};

use super::backend::RedisBackend;

const INDEX_KEY: &str = "trace:index";

/// Redis-based trace store for the **hot tier**.
/// - Sub-millisecond reads/writes
/// - TTL auto-eviction (default 7 days)
/// - Listing via a secondary sorted-set index (`trace:index`) + lightweight
///   metadata keys (`trace:meta:{trace_id}`)
pub struct RedisTraceStore {
    backend: RedisBackend,
    ttl_seconds: u64,
}

impl RedisTraceStore {
    /// Create a new hot-tier trace store.
    /// `ttl_seconds` defaults to 7 days (604_800) when `None`.
    pub fn new(backend: RedisBackend, ttl_seconds: Option<u64>) -> Self {
        Self {
            backend,
            ttl_seconds: ttl_seconds.unwrap_or(604_800),
        }
    }

    fn key(trace_id: &str) -> String {
        format!("trace:{}", trace_id)
    }

    fn meta_key(trace_id: &str) -> String {
        format!("trace:meta:{}", trace_id)
    }

    /// Metadata for one trace, falling back to the full trace when the
    /// metadata key has expired but the trace itself has not.
    async fn meta_of(&self, trace_id: &str) -> SFResult<Option<TraceMeta>> {
        match self.backend.get_bytes(&Self::meta_key(trace_id)).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(self
                .load(trace_id)
                .await?
                .map(|t| TraceMeta::from_trace(&t))),
        }
    }
}

#[async_trait]
impl TraceStore for RedisTraceStore {
    async fn save(&self, trace: &AgentTrace) -> SFResult<String> {
        let key = Self::key(&trace.trace_id);
        let data = serde_json::to_vec(trace)?;

        self.backend
            .set_ex_bytes(&key, &data, self.ttl_seconds)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace save failed: {e}"),
            })?;

        // Maintain sorted-set index for listing (score = creation timestamp)
        let score = trace.created_at.timestamp() as f64;
        self.backend
            .zadd(INDEX_KEY, score, &trace.trace_id)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace index zadd failed: {e}"),
            })?;

        // Store lightweight metadata for list_meta() without loading full traces
        let meta = TraceMeta::from_trace(trace);
        let meta_bytes = serde_json::to_vec(&meta)?;
        self.backend
            .set_ex_bytes(
                &Self::meta_key(&trace.trace_id),
                &meta_bytes,
                self.ttl_seconds,
            )
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace meta save failed: {e}"),
            })?;

        Ok(trace.trace_id.clone())
    }

    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>> {
        let key = Self::key(trace_id);
        let data = self
            .backend
            .get_bytes(&key)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace load failed: {e}"),
            })?;

        match data {
            Some(bytes) => {
                let trace = serde_json::from_slice(&bytes)?;
                Ok(Some(trace))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, trace_id: &str) -> SFResult<()> {
        let key = Self::key(trace_id);
        self.backend.del(&key).await?;

        // Remove from index and delete metadata
        self.backend.zrem(INDEX_KEY, trace_id).await?;
        self.backend.del(&Self::meta_key(trace_id)).await?;

        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        let trace_ids = self
            .backend
            .zrevrange(INDEX_KEY, 0, limit.saturating_sub(1) as isize)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace list zrevrange failed: {e}"),
            })?;

        let mut traces = Vec::new();
        for id in trace_ids {
            if let Some(trace) = self.load(&id).await? {
                traces.push(trace);
            }
            // If load returns None, the trace expired concurrently — skip it.
        }
        Ok(traces)
    }

    async fn list_meta(&self, limit: usize) -> SFResult<Vec<TraceMeta>> {
        let trace_ids = self
            .backend
            .zrevrange(INDEX_KEY, 0, limit.saturating_sub(1) as isize)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "redis".into(),
                message: format!("trace list_meta zrevrange failed: {e}"),
            })?;

        let mut metas = Vec::new();
        for id in trace_ids {
            if let Some(meta) = self.meta_of(&id).await? {
                metas.push(meta);
            }
        }
        Ok(metas)
    }

    /// Tier-scoped, oldest first. Redis keeps every trace in one index, so the
    /// tier is only visible in each entry's metadata; the ascending walk is
    /// therefore bounded by the index, which is in turn bounded by the TTL —
    /// entries older than that no longer exist to be migrated.
    async fn list_meta_in_tier(
        &self,
        tier: cog_core::StorageTier,
        limit: usize,
    ) -> SFResult<Vec<TraceMeta>> {
        let trace_ids =
            self.backend
                .zrange(INDEX_KEY, 0, -1)
                .await
                .map_err(|e| SFError::Adapter {
                    provider: "redis".into(),
                    message: format!("trace list_meta_in_tier zrange failed: {e}"),
                })?;

        let mut metas = Vec::new();
        for id in trace_ids {
            if metas.len() >= limit {
                break;
            }
            if let Some(meta) = self.meta_of(&id).await? {
                if meta.tier == tier {
                    metas.push(meta);
                }
            }
        }
        Ok(metas)
    }
}
