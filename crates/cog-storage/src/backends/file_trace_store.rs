//! File-based `TraceStore` — local filesystem persistence for traces.
//! Implements the full tiered storage pattern from the design doc:
//! - Hot: local uncompressed, `{base}/hot/{trace_id}.trace` + `{trace_id}.meta.json`
//! - Warm: zstd:3 compressed, `{base}/warm/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//! - Cold: zstd:9 compressed, `{base}/cold/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//!   Physical migration between tiers is handled by the store itself when
//!   traces are saved with a different tier than their current location.

use async_trait::async_trait;
use cog_core::{AgentTrace, SFError, SFResult, StorageTier, TraceMeta, TraceStore};

/// Return the subdirectory name for a tier.
fn tier_subdir(tier: StorageTier) -> &'static str {
    match tier {
        StorageTier::Hot => "hot",
        StorageTier::Warm => "warm",
        StorageTier::Cold => "cold",
    }
}

/// Filesystem-safe file stem for a trace id. Trace ids embed free-form agent
/// ids that can be arbitrarily long or contain path separators; used directly
/// as a file name they break writes (ENAMETOOLONG) or escape the tier
/// directory. Short, safe ids pass through unchanged so existing files stay
/// loadable; anything else becomes `prefix-<hash>`, keeping distinct long ids
/// collision-free even when they share the retained prefix.
fn file_stem(trace_id: &str) -> String {
    const MAX_SAFE_LEN: usize = 180;
    let is_safe = !trace_id.is_empty()
        && trace_id.len() <= MAX_SAFE_LEN
        && !trace_id.starts_with('.')
        && trace_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'));
    if is_safe {
        return trace_id.to_string();
    }
    let hash = blake3::hash(trace_id.as_bytes()).to_hex();
    let prefix: String = trace_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .take(80)
        .collect();
    let prefix = prefix.trim_start_matches('.');
    if prefix.is_empty() {
        format!("trace-{}", &hash[..16])
    } else {
        format!("{}-{}", prefix, &hash[..16])
    }
}

/// File-based trace store with tiered storage and compression support.
pub struct FileTraceStore {
    base_dir: std::path::PathBuf,
}

impl FileTraceStore {
    pub fn new(base_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Get the tier directory path.
    fn tier_dir(&self, tier: StorageTier) -> std::path::PathBuf {
        self.base_dir.join(tier_subdir(tier))
    }

    /// Get the trace data file path for a given tier.
    fn trace_path(&self, tier: StorageTier, trace_id: &str) -> std::path::PathBuf {
        let dir = self.tier_dir(tier);
        let stem = file_stem(trace_id);
        match tier {
            StorageTier::Hot => dir.join(format!("{}.trace", stem)),
            StorageTier::Warm | StorageTier::Cold => dir.join(format!("{}.trace.zst", stem)),
        }
    }

    /// Get the metadata file path for a given tier.
    fn meta_path(&self, tier: StorageTier, trace_id: &str) -> std::path::PathBuf {
        self.tier_dir(tier)
            .join(format!("{}.meta.json", file_stem(trace_id)))
    }

    /// Search all tiers for a trace and return which tier contains it.
    async fn find_trace_tier(&self, trace_id: &str) -> Option<StorageTier> {
        for &tier in &[StorageTier::Hot, StorageTier::Warm, StorageTier::Cold] {
            let meta_path = self.meta_path(tier, trace_id);
            if tokio::fs::try_exists(&meta_path).await.unwrap_or(false) {
                return Some(tier);
            }
        }
        None
    }

    /// Serialize trace data with optional compression.
    fn serialize_trace(&self, trace: &AgentTrace) -> SFResult<Vec<u8>> {
        let json = serde_json::to_string(trace)?;
        if trace.compression > 0 {
            zstd::encode_all(json.as_bytes(), trace.compression)
                .map_err(|e| SFError::IO(format!("Compression failed: {}", e)))
        } else {
            Ok(json.into_bytes())
        }
    }

    /// Deserialize trace data with optional decompression.
    fn deserialize_trace(&self, bytes: &[u8], compression: i32) -> SFResult<AgentTrace> {
        let json_bytes = if compression > 0 {
            zstd::decode_all(bytes)
                .map_err(|e| SFError::IO(format!("Decompression failed: {}", e)))?
        } else {
            bytes.to_vec()
        };
        let json = String::from_utf8(json_bytes)
            .map_err(|e| SFError::IO(format!("Invalid UTF-8: {}", e)))?;
        serde_json::from_str(&json).map_err(SFError::Serialization)
    }

    /// Remove a trace from a specific tier (best effort).
    async fn remove_from_tier(&self, tier: StorageTier, trace_id: &str) {
        let _ = tokio::fs::remove_file(self.trace_path(tier, trace_id)).await;
        let _ = tokio::fs::remove_file(self.meta_path(tier, trace_id)).await;
    }

    /// Read every parseable metadata file in one tier directory.
    ///
    /// Entries that cannot be read or parsed are skipped rather than failing
    /// the listing: one unreadable file must not hide the tier's other
    /// thousands, in particular not from a migration looking for its backlog.
    async fn tier_metas(&self, tier: StorageTier) -> SFResult<Vec<TraceMeta>> {
        let tier_dir = self.tier_dir(tier);
        if !tokio::fs::try_exists(&tier_dir).await.unwrap_or(false) {
            return Ok(Vec::new());
        }

        let mut entries = match tokio::fs::read_dir(&tier_dir).await {
            Ok(e) => e,
            Err(_) => return Ok(Vec::new()),
        };

        let mut metas = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| SFError::IO(e.to_string()))?
        {
            let path = entry.path();
            if !path
                .file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|s| s.ends_with(".meta.json"))
            {
                continue;
            }
            if let Ok(meta_json) = tokio::fs::read_to_string(&path).await {
                if let Ok(meta) = serde_json::from_str::<TraceMeta>(&meta_json) {
                    metas.push(meta);
                }
            }
        }
        Ok(metas)
    }
}

#[async_trait]
impl TraceStore for FileTraceStore {
    async fn save(&self, trace: &AgentTrace) -> SFResult<String> {
        // Ensure destination tier directory exists
        let tier_dir = self.tier_dir(trace.tier);
        tokio::fs::create_dir_all(&tier_dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        // Check if trace exists in another tier and needs migration
        if let Some(current_tier) = self.find_trace_tier(&trace.trace_id).await {
            if current_tier != trace.tier {
                // Remove from old tier
                self.remove_from_tier(current_tier, &trace.trace_id).await;
            }
        }

        // Serialize and write trace data
        let trace_bytes = self.serialize_trace(trace)?;
        let trace_path = self.trace_path(trace.tier, &trace.trace_id);
        tokio::fs::write(&trace_path, trace_bytes)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        // Write metadata
        let meta = TraceMeta::from_trace(trace);
        let meta_path = self.meta_path(trace.tier, &trace.trace_id);
        let meta_json = serde_json::to_string_pretty(&meta)?;
        tokio::fs::write(&meta_path, meta_json)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        Ok(trace.trace_id.clone())
    }

    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>> {
        // Search for the trace in all tiers
        let Some(tier) = self.find_trace_tier(trace_id).await else {
            return Ok(None);
        };

        // Load trace data
        let trace_path = self.trace_path(tier, trace_id);
        let bytes = match tokio::fs::read(&trace_path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(SFError::IO(e.to_string())),
        };

        // Load metadata to get compression level
        let meta_path = self.meta_path(tier, trace_id);
        let meta_json = tokio::fs::read_to_string(&meta_path)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;
        let meta: TraceMeta = serde_json::from_str(&meta_json).map_err(SFError::Serialization)?;

        // Deserialize with compression from metadata
        let trace = self.deserialize_trace(&bytes, meta.compression)?;
        Ok(Some(trace))
    }

    async fn delete(&self, trace_id: &str) -> SFResult<()> {
        // Remove from all tiers (best effort)
        for &tier in &[StorageTier::Hot, StorageTier::Warm, StorageTier::Cold] {
            self.remove_from_tier(tier, trace_id).await;
        }
        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        // Metadata first: the newest `limit` are the only traces returned, so
        // reading the heavy payload of every trace before sorting is work
        // thrown away. A trace whose metadata is gone cannot be loaded anyway
        // — `load` needs it for the compression level.
        let mut traces = Vec::new();
        for meta in self.list_meta(limit).await? {
            if let Ok(Some(trace)) = self.load(&meta.trace_id).await {
                traces.push(trace);
            }
        }
        Ok(traces)
    }

    async fn list_meta(&self, limit: usize) -> SFResult<Vec<TraceMeta>> {
        let mut metas = Vec::new();
        for tier in [StorageTier::Hot, StorageTier::Warm, StorageTier::Cold] {
            metas.extend(self.tier_metas(tier).await?);
        }
        // Sort by creation time (newest first) and limit
        metas.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        metas.truncate(limit);
        Ok(metas)
    }

    async fn list_meta_in_tier(&self, tier: StorageTier, limit: usize) -> SFResult<Vec<TraceMeta>> {
        let mut metas = self.tier_metas(tier).await?;
        metas.sort_by_key(|a| a.created_at);
        metas.truncate(limit);
        Ok(metas)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_trace(trace_id: &str) -> AgentTrace {
        AgentTrace {
            trace_id: trace_id.to_string(),
            session_id: None,
            task_id: "t".into(),
            agent_id: "a".into(),
            created_at: chrono::Utc::now(),
            event_count: 0,
            byte_size: 0,
            version: "test".into(),
            tier: StorageTier::Hot,
            compression: 0,
            checksum: String::new(),
            events: Vec::new(),
            llm_requests: Vec::new(),
            llm_responses: Vec::new(),
            tool_calls: Vec::new(),
        }
    }

    fn aged_trace(trace_id: &str, tier: StorageTier, age_days: i64) -> AgentTrace {
        let mut trace = test_trace(trace_id);
        trace.tier = tier;
        trace.compression = if tier == StorageTier::Hot { 0 } else { 3 };
        trace.created_at = chrono::Utc::now() - chrono::Duration::days(age_days);
        trace
    }

    /// Migration works from the oldest end of a tier, so a listing that
    /// selects from the recent end returns nothing usable the moment the store
    /// outgrows the limit — which is exactly when there is a backlog to clear.
    #[tokio::test]
    async fn oldest_entries_stay_visible_when_the_limit_is_exceeded() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);

        // old-0 is the oldest of the warm entries, old-4 the newest.
        for i in 0..5 {
            store
                .save(&aged_trace(&format!("old-{i}"), StorageTier::Warm, 40 - i))
                .await
                .unwrap();
        }
        for i in 0..10 {
            store
                .save(&aged_trace(
                    &format!("new-{i}"),
                    StorageTier::Hot,
                    i as i64 % 3,
                ))
                .await
                .unwrap();
        }

        // The recent-end listing the migration used to read: the limit applies
        // across all tiers, so no warm entry survives it at all.
        let recent = store.list_meta(2).await.unwrap();
        assert!(
            recent.iter().all(|m| m.tier == StorageTier::Hot),
            "precondition: the newest entries are all hot, got {:?}",
            recent.iter().map(|m| m.tier).collect::<Vec<_>>()
        );

        let oldest_warm = store.list_meta_in_tier(StorageTier::Warm, 2).await.unwrap();
        assert_eq!(
            oldest_warm
                .iter()
                .map(|m| m.trace_id.as_str())
                .collect::<Vec<_>>(),
            vec!["old-0", "old-1"],
            "the oldest warm entries are the ones aging toward cold"
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[test]
    fn short_safe_ids_pass_through_unchanged() {
        assert_eq!(file_stem("agent-7f3c-abc123"), "agent-7f3c-abc123");
    }

    #[test]
    fn long_ids_are_truncated_with_distinguishing_hash() {
        let a = format!("{}-alpha", "x".repeat(300));
        let b = format!("{}-bravo", "x".repeat(300));
        let sa = file_stem(&a);
        let sb = file_stem(&b);
        assert!(sa.len() <= 100, "stem must fit NAME_MAX: {}", sa.len());
        assert_ne!(sa, sb, "shared 80-char prefix must not collide");
    }

    #[test]
    fn unsafe_characters_are_replaced_not_trusted() {
        let stem = file_stem("../escape/attempt");
        assert!(!stem.contains('/'));
        assert!(!stem.starts_with('.'));
    }

    #[tokio::test]
    async fn long_trace_id_roundtrips_through_save_and_load() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        let long_id = format!("agent-{}-{}", "x".repeat(300), uuid::Uuid::new_v4());
        store.save(&test_trace(&long_id)).await.unwrap();
        let loaded = store.load(&long_id).await.unwrap();
        assert_eq!(loaded.unwrap().trace_id, long_id);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
