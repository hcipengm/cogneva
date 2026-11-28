//! File-based `TraceStore` — local filesystem persistence for traces.
//! Implements the full tiered storage pattern from the design doc:
//! - Hot: local uncompressed, `{base}/hot/{trace_id}.trace` + `{trace_id}.meta.json`
//! - Warm: zstd:3 compressed, `{base}/warm/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//! - Cold: zstd:9 compressed, `{base}/cold/{trace_id}.trace.zst` + `{trace_id}.meta.json`
//!   Physical migration between tiers is handled by the store itself when
//!   traces are saved with a different tier than their current location.

use async_trait::async_trait;
use cog_core::{AgentTrace, SFError, SFResult, StorageTier, TraceMeta, TraceStore};

/// Filesystem-safe file stem for a trace id. Trace ids embed free-form agent
/// ids that can be arbitrarily long or contain path separators; used directly
/// as a file name they break writes (ENAMETOOLONG) or escape the tier
/// directory. Short, safe ids pass through unchanged so existing files stay
/// loadable; anything else becomes `prefix-<hash>`, keeping distinct long ids
/// collision-free even when they share the retained prefix.
fn file_stem(trace_id: &str) -> String {
    // cap: fixed — well under the filesystem's 255-byte name limit, leaving room for .trace and .meta.json
    // keep: 删掉后文件名加 .trace/.meta.json 撞上 255 字节上限，trace 写不进去；由「留出后缀的余量」兜住
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
        // The directory is named by the tier's own word: `StorageTier::as_str` is
        // the one place that spelling lives, and `from_str` reads it back. A
        // second match here would let the folders be named one thing while the
        // tier reported in an API or a label says another, with neither side
        // erroring -- the files simply would not be found.
        self.base_dir.join(tier.as_str())
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
            // state-file: store — 文件后端自己的介质：接上它时这里就是正本，不是第二份
            .join(format!("{}.meta.json", file_stem(trace_id)))
    }

    /// Search all tiers for a trace and return which tier contains it.
    ///
    /// A check that could not be answered is an error, not "no tier has it":
    /// `None` is read by `load` as "this trace was never recorded", so folding
    /// a permission or I/O failure into it would answer "no such trace" about
    /// one that is still on disk.
    async fn find_trace_tier(&self, trace_id: &str) -> SFResult<Option<StorageTier>> {
        for &tier in &[StorageTier::Hot, StorageTier::Warm, StorageTier::Cold] {
            let meta_path = self.meta_path(tier, trace_id);
            let exists = tokio::fs::try_exists(&meta_path)
                .await
                .map_err(|e| SFError::IO(format!("checking {}: {}", meta_path.display(), e)))?;
            if exists {
                return Ok(Some(tier));
            }
        }
        Ok(None)
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

    /// Remove a trace from a specific tier.
    ///
    /// An already-absent file is the goal rather than a failure. Anything else
    /// is reported: a copy that survives in a tier searched before the trace's
    /// new one keeps answering `load` with the pre-move trace, which reads as
    /// stale data rather than as a failed move.
    async fn remove_from_tier(&self, tier: StorageTier, trace_id: &str) -> SFResult<()> {
        for path in [
            self.trace_path(tier, trace_id),
            self.meta_path(tier, trace_id),
        ] {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(SFError::IO(format!(
                        "removing {} failed: {}",
                        path.display(),
                        e
                    )))
                }
            }
        }
        Ok(())
    }

    /// Read every metadata file in one tier directory.
    ///
    /// A tier directory that does not exist is an empty tier. Anything else
    /// that stops the read — the directory itself, or one of its sidecars — is
    /// an error naming the path. Skipping it would answer "these are the
    /// traces" about a set that was not fully seen; that answer reaches the
    /// traces listing and the migrator's backlog count alike. Only a file that
    /// vanished between the directory read and its own read is skipped: that
    /// is a delete in flight, not a listing that failed.
    async fn tier_metas(&self, tier: StorageTier) -> SFResult<Vec<TraceMeta>> {
        let tier_dir = self.tier_dir(tier);
        let dir_exists = tokio::fs::try_exists(&tier_dir)
            .await
            .map_err(|e| SFError::IO(format!("checking {}: {}", tier_dir.display(), e)))?;
        if !dir_exists {
            return Ok(Vec::new());
        }

        let mut entries = tokio::fs::read_dir(&tier_dir)
            .await
            .map_err(|e| SFError::IO(format!("reading {}: {}", tier_dir.display(), e)))?;

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
            let meta_json = match tokio::fs::read_to_string(&path).await {
                Ok(json) => json,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(SFError::IO(format!("reading {}: {}", path.display(), e))),
            };
            let meta: TraceMeta = serde_json::from_str(&meta_json)
                .map_err(|e| SFError::IO(format!("parsing {}: {}", path.display(), e)))?;
            metas.push(meta);
        }
        Ok(metas)
    }
}

#[async_trait]
impl TraceStore for FileTraceStore {
    async fn save(&self, trace: &AgentTrace) -> SFResult<String> {
        // Serialized before anything on disk is touched: a serialization
        // failure must not be preceded by the removal of the copy that is
        // still the only one.
        let trace_bytes = self.serialize_trace(trace)?;
        let meta_json = serde_json::to_string_pretty(&TraceMeta::from_trace(trace))?;

        // Which tier holds the copy this write replaces, read before the
        // destination exists: the search returns the first tier holding
        // metadata, so asked afterwards it would answer with the new one and
        // the old copy would be left behind to shadow it.
        let previous_tier = self.find_trace_tier(&trace.trace_id).await?;

        // Ensure destination tier directory exists
        let tier_dir = self.tier_dir(trace.tier);
        tokio::fs::create_dir_all(&tier_dir)
            .await
            .map_err(|e| SFError::IO(e.to_string()))?;

        // Destination first, metadata included. `load` reads a tier's metadata
        // to learn the compression level, so data without it is unreadable
        // while still looking like a trace that is present.
        //
        // Both files land through a temp-and-rename: a listing that reads a
        // payload while it is being replaced must see either the old trace or
        // the new one, never a truncated file. `SurvivesPowerLoss` because a
        // trace is the only copy of the events it records — there is nothing
        // to recompute it from.
        let trace_path = self.trace_path(trace.tier, &trace.trace_id);
        cog_core::atomic_write::write_bytes_async(
            &trace_path,
            &trace_bytes,
            cog_core::atomic_write::Durability::SurvivesPowerLoss,
        )
        .await?;

        let meta_path = self.meta_path(trace.tier, &trace.trace_id);
        cog_core::atomic_write::write_bytes_async(
            &meta_path,
            meta_json.as_bytes(),
            cog_core::atomic_write::Durability::SurvivesPowerLoss,
        )
        .await?;

        // Only now is the old copy redundant. Removing it is what makes the
        // move atomic in the direction that matters: a move that fails above
        // leaves the trace exactly where it was, rather than in neither tier.
        // A crash in between leaves it in both, which the next pass settles:
        // the source tier still lists it as overdue, so the move runs again.
        if let Some(previous_tier) = previous_tier {
            if previous_tier != trace.tier {
                self.remove_from_tier(previous_tier, &trace.trace_id)
                    .await?;
            }
        }

        Ok(trace.trace_id.clone())
    }

    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>> {
        // Search for the trace in all tiers
        let Some(tier) = self.find_trace_tier(trace_id).await? else {
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
        // Every tier is attempted even when one fails: stopping at the first
        // error would leave behind copies of the trace a delete was asked to
        // remove. The first failure is what is reported.
        let mut first_error = None;
        for &tier in &[StorageTier::Hot, StorageTier::Warm, StorageTier::Cold] {
            if let Err(e) = self.remove_from_tier(tier, trace_id).await {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>> {
        // Metadata first: the newest `limit` are the only traces returned, so
        // reading the heavy payload of every trace before sorting is work
        // thrown away. A trace whose metadata is gone cannot be loaded anyway
        // — `load` needs it for the compression level.
        // `None` is the entry vanishing between the listing and this load — a
        // delete in flight. An error is a payload that is on disk but cannot be
        // read, and dropping it would make the listing answer with every trace
        // except the damaged one, with nothing saying one is missing.
        let mut traces = Vec::new();
        for meta in self.list_meta(limit).await? {
            let loaded = self
                .load(&meta.trace_id)
                .await
                .map_err(|e| SFError::IO(format!("loading trace {}: {}", meta.trace_id, e)))?;
            if let Some(trace) = loaded {
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

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
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
        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// A tier move writes the destination before it removes the source, so a
    /// write that cannot land leaves the trace exactly where it was. Removing
    /// the source first loses it outright: no tier holds those events, and the
    /// migrator that counted the failing move has nothing left to retry from.
    #[tokio::test]
    async fn a_failed_tier_move_leaves_the_trace_where_it_was() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        let hot = aged_trace("t-fail", StorageTier::Hot, 40);
        store.save(&hot).await.unwrap();

        // The destination path is occupied by a directory, which fails the
        // write whatever the process's privileges — a full or unwritable disk
        // takes the same branch. Warm files carry the compression suffix.
        tokio::fs::create_dir_all(dir.join("warm/t-fail.trace.zst"))
            .await
            .unwrap();

        let mut demoted = hot.clone();
        demoted.tier = StorageTier::Warm;
        demoted.compression = 3;
        assert!(
            store.save(&demoted).await.is_err(),
            "a destination write that failed must be reported"
        );

        let survived = store
            .load("t-fail")
            .await
            .unwrap()
            .expect("a failed move must not destroy the only copy");
        assert_eq!(survived.tier, StorageTier::Hot);

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// The tier search answers with the first tier holding metadata. Asked
    /// after the destination has been written, a move upwards would report the
    /// destination as the previous tier and skip the removal, leaving the
    /// source copy behind to shadow the new one for every later `load`.
    #[tokio::test]
    async fn a_move_upwards_leaves_no_copy_behind() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        let warm = aged_trace("t-up", StorageTier::Warm, 40);
        store.save(&warm).await.unwrap();

        let mut promoted = warm.clone();
        promoted.tier = StorageTier::Hot;
        promoted.compression = 0;
        promoted.event_count = 7;
        store.save(&promoted).await.unwrap();

        assert!(
            !dir.join("warm/t-up.trace.zst").exists(),
            "the warm copy must be gone rather than left to shadow the hot one"
        );
        assert!(!dir.join("warm/t-up.meta.json").exists());
        assert_eq!(
            store.list_meta(10).await.unwrap().len(),
            1,
            "one trace, whichever tier it moved between"
        );

        let loaded = store.load("t-up").await.unwrap().unwrap();
        assert_eq!(loaded.tier, StorageTier::Hot);
        assert_eq!(
            loaded.event_count, 7,
            "the hot copy is the one that answers"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// A tier directory that exists but cannot be listed is not an empty
    /// tier. `read_dir` on a regular file fails with ENOTDIR whatever the
    /// process's privileges; a volume that refuses the read takes the same
    /// branch. Answering `Ok(vec![])` there says "no traces in this tier"
    /// about entries nobody looked at.
    #[tokio::test]
    async fn a_listing_that_cannot_read_a_tier_directory_is_not_an_empty_list() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();
        // `hot` is a file, not a directory: it exists, so the existence check
        // passes, and the directory read then fails.
        tokio::fs::write(dir.join("hot"), b"not a directory")
            .await
            .unwrap();

        let store = FileTraceStore::new(&dir);
        assert!(
            store.list_meta(10).await.is_err(),
            "a tier that could not be read must not be reported as empty"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// A sidecar that is there but is not a trace's metadata makes the
    /// listing short by one entry. Reporting the path is the only way the
    /// caller can tell this from a tier that genuinely holds fewer traces.
    #[tokio::test]
    async fn a_listing_reports_a_sidecar_it_cannot_parse() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        tokio::fs::create_dir_all(dir.join("hot")).await.unwrap();
        tokio::fs::write(dir.join("hot/broken.meta.json"), b"{ not json")
            .await
            .unwrap();

        let store = FileTraceStore::new(&dir);
        let err = store
            .list_meta(10)
            .await
            .expect_err("an unparseable sidecar must fail the listing");
        let message = err.to_string();
        assert!(
            message.contains("broken.meta.json"),
            "the error must name the file to inspect, got: {message}"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// Metadata without a readable payload is a damaged entry, not an absent
    /// one: the trace is on the listing's books either way. Omitting it made
    /// the traces listing answer with every trace except the damaged one.
    #[tokio::test]
    async fn a_listing_reports_a_payload_it_cannot_deserialize() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        store.save(&test_trace("t-damaged")).await.unwrap();
        tokio::fs::write(dir.join("hot/t-damaged.trace"), b"not a trace")
            .await
            .unwrap();

        let err = store
            .list(10)
            .await
            .expect_err("a trace whose payload cannot be read must fail the listing");
        assert!(
            err.to_string().contains("t-damaged"),
            "the error must name the trace, got: {err}"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// The one skip that stays a skip: the payload disappeared between the
    /// listing and its load, which is a delete in flight. Erroring here would
    /// make every concurrent delete fail an unrelated reader's listing.
    #[tokio::test]
    async fn an_entry_deleted_between_the_listing_and_the_load_is_not_a_failed_listing() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        store.save(&test_trace("t-going")).await.unwrap();
        tokio::fs::remove_file(dir.join("hot/t-going.trace"))
            .await
            .unwrap();

        let listed = store
            .list(10)
            .await
            .expect("a delete in flight is not a listing failure");
        assert!(listed.is_empty(), "the entry is gone, not broken");

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }

    /// The atomic write publishes through a rename, so its temp sibling must
    /// not survive a successful save and must never be listed as a trace.
    #[tokio::test]
    async fn a_save_leaves_no_temporary_file_beside_the_trace() {
        let dir = std::env::temp_dir().join(format!("fts-{}", uuid::Uuid::new_v4()));
        let store = FileTraceStore::new(&dir);
        store.save(&test_trace("t-tmp")).await.unwrap();

        let mut names = Vec::new();
        let mut entries = tokio::fs::read_dir(dir.join("hot")).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            names.push(entry.file_name().to_string_lossy().into_owned());
        }
        names.sort();
        assert_eq!(
            names,
            vec!["t-tmp.meta.json".to_string(), "t-tmp.trace".to_string()],
            "a landed save leaves exactly its two files"
        );

        cog_core::ignore_lost!(tokio::fs::remove_dir_all(&dir).await);
    }
}
