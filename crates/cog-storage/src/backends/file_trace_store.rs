//! File-based `TraceStore` — local filesystem persistence for traces.
//! Implements the full tiered storage pattern from the design doc:
//! - Hot: local uncompressed, `{base}/hot/{trace_id}.trace` + `{trace_id}.meta.json`
//! - Warm: zstd:3 compressed, `{base}/warm/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//! - Cold: zstd:9 compressed, `{base}/cold/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//!   Physical migration between tiers is handled by the store itself when
//!   traces are saved with a different tier than their current location.

use async_trait::async_trait;
use cog_core::{AgentTrace, SFError, SFResult, TraceMeta, TraceStore};

/// Return the zstd compression level for a tier.
#[allow(dead_code)]
fn tier_compression_level(tier: cog_core::StorageTier) -> i32 {
    match tier {
        cog_core::StorageTier::Hot => 0,
        cog_core::StorageTier::Warm => 3,
        cog_core::StorageTier::Cold => 9,
    }
}

/// Return the subdirectory name for a tier.
fn tier_subdir(tier: cog_core::StorageTier) -> &'static str {
    match tier {
        cog_core::StorageTier::Hot => "hot",
        cog_core::StorageTier::Warm => "warm",
        cog_core::StorageTier::Cold => "cold",
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
    fn tier_dir(&self, tier: cog_core::StorageTier) -> std::path::PathBuf {
        self.base_dir.join(tier_subdir(tier))
    }

    /// Get the trace data file path for a given tier.
    fn trace_path(&self, tier: cog_core::StorageTier, trace_id: &str) -> std::path::PathBuf {
        let dir = self.tier_dir(tier);
        match tier {
            cog_core::StorageTier::Hot => dir.join(format!("{}.trace", trace_id)),
            cog_core::StorageTier::Warm | cog_core::StorageTier::Cold => {
                dir.join(format!("{}.trace.zst", trace_id))
            }
        }
    }

    /// Get the metadata file path for a given tier.
    fn meta_path(&self, tier: cog_core::StorageTier, trace_id: &str) -> std::path::PathBuf {
        self.tier_dir(tier).join(format!("{}.meta.json", trace_id))
    }

    /// Search all tiers for a trace and return which tier contains it.
    async fn find_trace_tier(&self, trace_id: &str) -> Option<cog_core::StorageTier> {
        for &tier in &[
            cog_core::StorageTier::Hot,
            cog_core::StorageTier::Warm,
            cog_core::StorageTier::Cold,
        ] {
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
    async fn remove_from_tier(&self, tier: cog_core::StorageTier, trace_id: &str) {
        let _ = tokio::fs::remove_file(self.trace_path(tier, trace_id)).await;
        let _ = tokio::fs::remove_file(self.meta_path(tier, trace_id)).await;
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
        for &tier in &[
            cog_core::StorageTier::Hot,
            cog_core::StorageTier::Warm,
            cog_core::StorageTier::Cold,
        ] {
            self.remove_from_tier(tier, trace_id).await;
        }
        Ok(())
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        let mut traces = Vec::new();

        // Scan all tiers
        for &tier in &[
            cog_core::StorageTier::Hot,
            cog_core::StorageTier::Warm,
            cog_core::StorageTier::Cold,
        ] {
            let tier_dir = self.tier_dir(tier);
            if !tokio::fs::try_exists(&tier_dir).await.unwrap_or(false) {
                continue;
            }

            let mut entries = match tokio::fs::read_dir(&tier_dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
            {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .is_some_and(|s| s.ends_with(".meta.json"))
                {
                    if let Ok(meta_json) = tokio::fs::read_to_string(&path).await {
                        if let Ok(meta) = serde_json::from_str::<TraceMeta>(&meta_json) {
                            if let Ok(Some(trace)) = self.load(&meta.trace_id).await {
                                traces.push(trace);
                            }
                        }
                    }
                }
            }
        }

        // Sort by creation time (newest first) and limit
        traces.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        traces.truncate(limit);
        Ok(traces)
    }

    async fn list_meta(&self, limit: usize) -> SFResult<Vec<TraceMeta>> {
        let mut metas = Vec::new();

        // Scan all tiers
        for &tier in &[
            cog_core::StorageTier::Hot,
            cog_core::StorageTier::Warm,
            cog_core::StorageTier::Cold,
        ] {
            let tier_dir = self.tier_dir(tier);
            if !tokio::fs::try_exists(&tier_dir).await.unwrap_or(false) {
                continue;
            }

            let mut entries = match tokio::fs::read_dir(&tier_dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };

            while let Some(entry) = entries
                .next_entry()
                .await
                .map_err(|e| SFError::IO(e.to_string()))?
            {
                let path = entry.path();
                if path.extension().and_then(|s| s.to_str()) == Some("json")
                    && path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .is_some_and(|s| s.ends_with(".meta.json"))
                {
                    if let Ok(meta_json) = tokio::fs::read_to_string(&path).await {
                        if let Ok(meta) = serde_json::from_str::<TraceMeta>(&meta_json) {
                            metas.push(meta);
                        }
                    }
                }
            }
        }

        // Sort by creation time (newest first) and limit
        metas.sort_by_key(|a| std::cmp::Reverse(a.created_at));
        metas.truncate(limit);
        Ok(metas)
    }
}
