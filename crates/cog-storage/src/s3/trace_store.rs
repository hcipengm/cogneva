//! S3-compatible `TraceStore` — cold tier (zstd:9 compression, long-term archive).

use async_trait::async_trait;
use cog_core::{AgentTrace, ObjectBackend, SFError, SFResult, TraceStore};
use std::sync::Arc;

/// S3-based trace store for the **cold tier**.
/// - zstd level-9 compression for cost efficiency
/// - Compatible with AWS S3, MinIO, Tencent COS
/// - `list()` scans the prefix via `ListObjectsV2`
pub struct S3TraceStore {
    /// Only the trait's four calls are used, so the field is the trait: the
    /// store has no business knowing which object backend it sits on, and a
    /// test can hand it one that answers from memory.
    backend: Arc<dyn ObjectBackend>,
    prefix: String,
}

impl S3TraceStore {
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        Self {
            backend,
            prefix: prefix.into(),
        }
    }

    /// One object read back as the trace it was written from.
    ///
    /// `None` is a key that vanished after the listing — a delete in flight,
    /// which also removes it from the store's answer honestly. Anything else
    /// that stops the read is an error naming the key: an object that is
    /// there but does not decompress or parse is not a trace to leave out of
    /// a listing, it is the reason the listing cannot be answered.
    async fn read_trace(&self, key: &str) -> SFResult<Option<AgentTrace>> {
        let bytes = match self.backend.get(key).await.map_err(|e| SFError::Adapter {
            provider: "s3".into(),
            message: format!("trace read of {key} failed: {e}"),
        })? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let decompressed = zstd::decode_all(&bytes[..]).map_err(|e| SFError::Adapter {
            provider: "s3".into(),
            message: format!("trace {key} is not zstd: {e}"),
        })?;
        let trace = serde_json::from_slice(&decompressed).map_err(|e| SFError::Adapter {
            provider: "s3".into(),
            message: format!("trace {key} is not JSON: {e}"),
        })?;
        Ok(Some(trace))
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
        self.read_trace(&self.key(trace_id)).await
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
            if let Some(trace) = self.read_trace(&key).await? {
                traces.push(trace);
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
            if let Some(trace) = self.read_trace(&key).await? {
                metas.push(cog_core::TraceMeta::from_trace(&trace));
            }
        }
        metas.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        Ok(metas)
    }

    async fn list_meta_in_tier(
        &self,
        tier: cog_core::StorageTier,
        limit: usize,
    ) -> SFResult<Vec<cog_core::TraceMeta>> {
        // Object keys carry no timestamp — the prefix is the whole namespace —
        // so ordering by age means reading every object, as `list_meta` above
        // does. The tier lives inside each object's metadata.
        let keys = self
            .backend
            .list(Some(&self.prefix))
            .await
            .map_err(|e| SFError::Adapter {
                provider: "s3".into(),
                message: format!("trace list_meta_in_tier failed: {e}"),
            })?;

        let mut metas: Vec<cog_core::TraceMeta> = Vec::new();
        for key in keys {
            if let Some(trace) = self.read_trace(&key).await? {
                // A different tier is not an entry this listing dropped: the
                // scan reads every object because keys carry no tier.
                if trace.tier == tier {
                    metas.push(cog_core::TraceMeta::from_trace(&trace));
                }
            }
        }
        metas.sort_by_key(|a| a.created_at);
        metas.truncate(limit);
        Ok(metas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::StorageTier;
    use std::collections::{HashMap, HashSet};

    /// An object backend answering from memory. `vanished` keys are listed but
    /// answer `None`, which is what a delete in flight looks like.
    struct FakeObjects {
        keys: Vec<String>,
        objects: HashMap<String, Vec<u8>>,
        vanished: HashSet<String>,
        get_fails: bool,
    }

    impl FakeObjects {
        fn new(entries: Vec<(&str, Vec<u8>)>) -> Self {
            let keys = entries.iter().map(|(k, _)| (*k).to_string()).collect();
            let objects = entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            Self {
                keys,
                objects,
                vanished: HashSet::new(),
                get_fails: false,
            }
        }
    }

    #[async_trait]
    impl ObjectBackend for FakeObjects {
        async fn put(&self, _key: &str, _data: &[u8]) -> SFResult<String> {
            unimplemented!("this fake is read-only")
        }

        async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>> {
            if self.get_fails {
                return Err(SFError::Adapter {
                    provider: "s3".into(),
                    message: "connection reset".into(),
                });
            }
            if self.vanished.contains(key) {
                return Ok(None);
            }
            Ok(self.objects.get(key).cloned())
        }

        async fn delete(&self, _key: &str) -> SFResult<()> {
            unimplemented!("this fake is read-only")
        }

        async fn presign_url(&self, _key: &str, _expiry_secs: u64) -> SFResult<String> {
            unimplemented!("this fake is read-only")
        }

        async fn exists(&self, _key: &str) -> SFResult<bool> {
            unimplemented!("this fake is read-only")
        }

        async fn list(&self, _prefix: Option<&str>) -> SFResult<Vec<String>> {
            Ok(self.keys.clone())
        }
    }

    fn test_trace(id: &str, tier: StorageTier) -> AgentTrace {
        AgentTrace {
            trace_id: id.to_string(),
            session_id: None,
            task_id: "t".into(),
            agent_id: "a".into(),
            created_at: chrono::Utc::now(),
            event_count: 0,
            byte_size: 0,
            version: "test".into(),
            tier,
            compression: 9,
            checksum: String::new(),
            events: Vec::new(),
            llm_requests: Vec::new(),
            llm_responses: Vec::new(),
            tool_calls: Vec::new(),
        }
    }

    fn stored(trace: &AgentTrace) -> Vec<u8> {
        zstd::encode_all(&serde_json::to_vec(trace).unwrap()[..], 9).unwrap()
    }

    fn store(fake: FakeObjects) -> S3TraceStore {
        S3TraceStore::new(Arc::new(fake), "cold")
    }

    /// An object that is there but does not decompress is not a trace to leave
    /// out of the listing: the answer would be every trace except that one,
    /// served as if it were the whole set.
    #[tokio::test]
    async fn a_listing_reports_an_object_that_will_not_decode() {
        let s = store(FakeObjects::new(vec![(
            "cold/bad.json.zst",
            b"not zstd".to_vec(),
        )]));

        let err = s.list(10).await.expect_err("must not answer a short list");
        assert!(
            err.to_string().contains("cold/bad.json.zst"),
            "the error must name the object to inspect, got: {err}"
        );
        assert!(
            s.list_meta(10).await.is_err(),
            "the metadata listing reads the same objects and must fail too"
        );
    }

    /// A backend that refused the read is not a listing of the objects it did
    /// manage to return.
    #[tokio::test]
    async fn a_listing_does_not_fold_a_backend_error_into_a_shorter_list() {
        let mut fake = FakeObjects::new(vec![(
            "cold/t1.json.zst",
            stored(&test_trace("t1", StorageTier::Cold)),
        )]);
        fake.get_fails = true;
        let s = store(fake);

        let err = s.list(10).await.expect_err("must not answer a short list");
        assert!(err.to_string().contains("connection reset"), "{err}");
    }

    /// The one skip that stays a skip: the object was deleted between the
    /// listing and its read.
    #[tokio::test]
    async fn an_object_deleted_after_the_listing_is_not_a_listing_failure() {
        let mut fake = FakeObjects::new(vec![
            (
                "cold/gone.json.zst",
                stored(&test_trace("gone", StorageTier::Cold)),
            ),
            (
                "cold/here.json.zst",
                stored(&test_trace("here", StorageTier::Cold)),
            ),
        ]);
        fake.vanished.insert("cold/gone.json.zst".into());
        let s = store(fake);

        let listed = s.list(10).await.expect("a delete in flight is not failure");
        assert_eq!(
            listed
                .iter()
                .map(|t| t.trace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["here"]
        );
    }

    /// A tier listing skips the other tiers by design — that skip is the
    /// answer, not an omission — and still reports objects it cannot read.
    #[tokio::test]
    async fn a_tier_listing_returns_only_that_tier() {
        let s = store(FakeObjects::new(vec![
            (
                "cold/hot-1.json.zst",
                stored(&test_trace("hot-1", StorageTier::Hot)),
            ),
            (
                "cold/cold-1.json.zst",
                stored(&test_trace("cold-1", StorageTier::Cold)),
            ),
        ]));

        let hot = s.list_meta_in_tier(StorageTier::Hot, 10).await.unwrap();
        assert_eq!(
            hot.iter().map(|m| m.trace_id.as_str()).collect::<Vec<_>>(),
            vec!["hot-1"]
        );
    }
}
