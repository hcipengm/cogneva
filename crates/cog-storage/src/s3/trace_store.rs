//! S3-compatible `TraceStore` — cold tier (zstd:9 compression, long-term archive).

use async_trait::async_trait;
use cog_core::{AgentTrace, ObjectBackend, SFError, SFResult, TraceStore};
use std::sync::Arc;

use crate::backends::object_backends::S3ObjectBackend;

/// S3-based trace store for the **cold tier**.
/// - zstd level-9 compression for cost efficiency
/// - Compatible with AWS S3, MinIO, Tencent COS
/// - `list()` scans the prefix via `ListObjectsV2`
pub struct S3TraceStore {
    backend: Arc<S3ObjectBackend>,
    prefix: String,
}

impl S3TraceStore {
    pub fn new(backend: Arc<S3ObjectBackend>, prefix: impl Into<String>) -> Self {
        Self {
            backend,
            prefix: prefix.into(),
        }
    }

    fn key(&self, trace_id: &str) -> String {
        format!("{}/{}.json.zst", self.prefix, trace_id)
    }
}

#[async_trait]
impl TraceStore for S3TraceStore {
    async fn save(&self, trace: &AgentTrace) -> SFResult<String> {
        let json = serde_json::to_vec(trace)?;
        let compressed = zstd::encode_all(&json[..], 9)?;

        let key = self.key(&trace.trace_id);
        self.backend
            .put(&key, &compressed)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "s3".into(),
                message: format!("trace save failed: {e}"),
            })?;

        Ok(trace.trace_id.clone())
    }

    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>> {
        let key = self.key(trace_id);
        let data = self.backend.get(&key).await.map_err(|e| SFError::Adapter {
            provider: "s3".into(),
            message: format!("trace load failed: {e}"),
        })?;

        match data {
            Some(bytes) => {
                let decompressed = zstd::decode_all(&bytes[..])?;
                let trace = serde_json::from_slice(&decompressed)?;
                Ok(Some(trace))
            }
            None => Ok(None),
        }
    }

    async fn delete(&self, trace_id: &str) -> SFResult<()> {
        let key = self.key(trace_id);
        self.backend
            .delete(&key)
            .await
            .map_err(|e| SFError::Adapter {
                provider: "s3".into(),
                message: format!("trace delete failed: {e}"),
            })
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        let keys = self
            .backend
            .list(Some(&self.prefix))
            .await
            .map_err(|e| SFError::Adapter {
                provider: "s3".into(),
                message: format!("trace list failed: {e}"),
            })?;

        let mut traces: Vec<AgentTrace> = Vec::with_capacity(keys.len().min(limit));
        for key in keys.into_iter().take(limit) {
            match self.backend.get(&key).await {
                Ok(Some(bytes)) => {
                    if let Ok(decompressed) = zstd::decode_all(&bytes[..]) {
                        if let Ok(trace) = serde_json::from_slice(&decompressed) {
                            traces.push(trace);
                        }
                    }
                }
                _ => continue,
            }
        }
        traces.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(traces)
    }

    async fn list_meta(&self, limit: usize) -> SFResult<Vec<cog_core::TraceMeta>> {
        // S3 does not support metadata-only extraction for JSON objects.
        // We fetch the full object but discard the heavy event arrays to
        // reduce memory pressure on the caller.
        let keys = self
            .backend
            .list(Some(&self.prefix))
            .await
            .map_err(|e| SFError::Adapter {
                provider: "s3".into(),
                message: format!("trace list_meta failed: {e}"),
            })?;

        let mut metas: Vec<cog_core::TraceMeta> = Vec::with_capacity(keys.len().min(limit));
        for key in keys.into_iter().take(limit) {
            match self.backend.get(&key).await {
                Ok(Some(bytes)) => {
                    if let Ok(decompressed) = zstd::decode_all(&bytes[..]) {
                        if let Ok(trace) = serde_json::from_slice::<AgentTrace>(&decompressed) {
                            metas.push(cog_core::TraceMeta::from_trace(&trace));
                        }
                    }
                }
                _ => continue,
            }
        }
        metas.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(metas)
    }
}
