//! Redis-backed `TraceStore` — hot tier (0-7 days, TTL eviction).

use async_trait::async_trait;
use cog_core::{AgentTrace, SFError, SFResult, TraceMeta, TraceStore};

use super::backend::RedisBackend;

const INDEX_KEY: &str = "trace:index";

/// 一条存进热层的 trace 读回来。
///
/// 解不出来时点名是哪一条：列表是逐条读出来的，报一句「expected value at
/// line 1 column 1」，读的人既不知道哪条坏了，也不知道该删哪条。
fn decode_trace(trace_id: &str, bytes: &[u8]) -> SFResult<AgentTrace> {
    serde_json::from_slice(bytes).map_err(|e| {
        SFError::Validation(format!("trace {trace_id} in the hot tier is not JSON: {e}"))
    })
}

/// 同上，索引里那条轻量元数据。
fn decode_meta(trace_id: &str, bytes: &[u8]) -> SFResult<TraceMeta> {
    serde_json::from_slice(bytes).map_err(|e| {
        SFError::Validation(format!(
            "trace {trace_id} in the hot tier has metadata that is not JSON: {e}"
        ))
    })
}

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
            Some(bytes) => Ok(Some(decode_meta(trace_id, &bytes)?)),
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
            Some(bytes) => Ok(Some(decode_trace(trace_id, &bytes)?)),
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn trace(id: &str) -> AgentTrace {
        AgentTrace {
            trace_id: id.to_string(),
            session_id: None,
            task_id: "t-1".to_string(),
            agent_id: "a-1".to_string(),
            created_at: Utc::now(),
            event_count: 0,
            byte_size: 0,
            version: "1".to_string(),
            tier: cog_core::StorageTier::Hot,
            compression: 0,
            checksum: String::new(),
            events: Vec::new(),
            llm_requests: Vec::new(),
            llm_responses: Vec::new(),
            tool_calls: Vec::new(),
        }
    }

    /// 列表是逐条读出来的。一条坏 trace 报出来时要点名是哪一条，否则读的人
    /// 只拿到「expected value」，不知道该删哪条、也不知道这条列表为什么短了。
    #[test]
    fn a_trace_that_will_not_decode_is_named() {
        let err = decode_trace("t-broken", b"{ half")
            .expect_err("a stored value that is not JSON must not decode");
        assert!(err.to_string().contains("t-broken"), "got: {err}");
    }

    #[test]
    fn a_trace_this_store_wrote_decodes_back() {
        let written = serde_json::to_vec(&trace("t-1")).unwrap();
        assert_eq!(decode_trace("t-1", &written).unwrap().trace_id, "t-1");
    }

    #[test]
    fn metadata_that_will_not_decode_is_named_too() {
        let err = decode_meta("t-meta", b"[]").expect_err("an array is not a trace's metadata");
        assert!(err.to_string().contains("t-meta"), "got: {err}");
    }
}
