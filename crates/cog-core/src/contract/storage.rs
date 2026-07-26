//!Systematic storage trait contracts for Cogneva.
//!All storage backend traits are centralized here so that consumers
//!can import them through a single module rather than scattering
//!`use cog_core::backend::*` across the workspace.
//!Design principle:
// - `cog-core` defines the contract (trait + types + error).
// - `cog-storage` implements the contract (Postgres, Redis, Qdrant, S3, ...).
// - `cog-memory` uses `cog-storage` clients to implement the permanent
//   memory domain layer (Raw → Schema → Summary).

use crate::{SFError, SFResult};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

/// PostgreSQL pool wrappers published by the storage plugin so that
/// downstream crates can consume them without depending on `cog-storage`.
#[derive(Debug, Clone)]
pub struct UsersPool(pub Option<sqlx::PgPool>);

#[derive(Debug, Clone)]
pub struct MessagesPool(pub Option<sqlx::PgPool>);

#[derive(Debug, Clone)]
pub struct ConfigPool(pub Option<sqlx::PgPool>);

#[derive(Debug, Clone)]
pub struct ExplainPool(pub Option<sqlx::PgPool>);

/// Redis client wrapper so it can be stored in [`cog_core::PluginContext`].
#[derive(Debug, Clone)]
pub struct RedisClient(pub redis::Client);

/// A single time-series sample returned by [`MetricsBackend::query_range`].
#[derive(Debug, Clone)]
pub struct MetricSample {
    pub timestamp: DateTime<Utc>,
    pub value: f64,
    pub labels: HashMap<String, String>,
}

/// Runtime metrics-collection abstraction for time-series data.
/// Infrastructure-layer metrics trait for runtime health recording.
/// Implementations: Memory (testing), Prometheus (production).
#[async_trait]
pub trait MetricsBackend: Send + Sync {
    /// Record a gauge value (point-in-time measurement).
    async fn record_gauge(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()>;

    /// Increment a counter by `value`.
    async fn record_counter(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()>;

    /// Record a histogram observation.
    async fn record_histogram(
        &self,
        name: &str,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()>;

    /// Query gauge samples for a metric over a time range.
    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>>;

    /// Query counter samples for a metric over a time range.
    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>>;

    /// Query histogram samples for a metric over a time range.
    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>>;

    /// Check whether the backend is healthy and accessible.
    async fn health_check(&self) -> SFResult<()>;
}

/// Runtime object-storage abstraction for large blobs (files, snapshots, raw sources).
/// Implementations: Memory (testing), FileSystem (local dev), S3/SeaweedFS (production).
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Store an object and return its URI.
    async fn put(&self, key: &str, data: &[u8]) -> SFResult<String>;

    /// Retrieve an object by key.
    async fn get(&self, key: &str) -> SFResult<Option<Vec<u8>>>;

    /// Delete an object.
    async fn delete(&self, key: &str) -> SFResult<()>;

    /// Generate a presigned URL for temporary access (no-op for memory/fs backends).
    async fn presign_url(&self, key: &str, expiry_secs: u64) -> SFResult<String>;

    /// Check whether an object exists.
    async fn exists(&self, key: &str) -> SFResult<bool>;

    /// List object keys matching an optional prefix.
    async fn list(&self, prefix: Option<&str>) -> SFResult<Vec<String>>;
}

/// Unified raw data envelope.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawRecord {
    pub meta: RawMeta,
    pub context: RawContext,
    pub payload: RawPayload,
}

/// Metadata for a raw record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawMeta {
    pub version: String,
    pub stream: String,
    pub recorded_at: DateTime<Utc>,
    pub recorded_by: String,
    pub sequence: u64,
    pub trace_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
}

/// Contextual identifiers for a raw record.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RawContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<Uuid>,
}

/// Payload of a raw record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawPayload {
    pub direction: String,
    pub transport: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    pub raw: Value,
}

/// Abstraction for durable raw-data logging.
/// Every crate that produces raw data (WebSocket frames, LLM requests,
/// task events, etc.) writes through a shared `Arc<dyn RawLogger>`.
#[async_trait]
pub trait RawLogger: Send + Sync {
    /// Write a single raw record.
    async fn write(&self, record: RawRecord) -> SFResult<()>;

    /// Force any buffered data to persistent storage.
    async fn flush(&self) -> SFResult<()>;

    /// Shut down the logger, flushing all buffered data.
    async fn shutdown(&self) -> SFResult<()>;

    /// Write a record from its already-encoded protobuf wire bytes.
    /// Use this when forwarding records between nodes to avoid the
    /// decode → re-encode roundtrip.
    async fn write_proto(&self, encoded: Bytes) -> SFResult<()>;

    /// Read every record persisted for `stream`, decoding from binary
    /// (Protobuf or Protobuf+zstd) or JSONL as needed.
    /// Default implementation returns an empty list — useful for loggers
    /// without persistent storage (e.g. `NoopRawLogger`).
    async fn read_proto(&self, _stream: &str) -> SFResult<Vec<RawRecord>> {
        Ok(Vec::new())
    }
}

/// Output format for `FileRawLogger`.
/// `Jsonl` is the legacy default (one JSON record per line). `Proto` and
/// `ProtoZstd` write length-delimited Protobuf records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawLoggerFormat {
    /// Backward-compatible default: existing deployments keep emitting JSONL
    /// until they explicitly opt into Protobuf via the config flag.
    #[default]
    Jsonl,
    Proto,
    ProtoZstd,
}

impl RawLoggerFormat {
    /// File extension (without the leading dot) used for new files.
    pub fn extension(self) -> &'static str {
        match self {
            Self::Jsonl => "jsonl",
            Self::Proto => "proto.bin",
            Self::ProtoZstd => "proto.bin.zst",
        }
    }
}

/// Configuration for `FileRawLogger`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RawLoggerConfig {
    pub enabled: bool,
    /// Base directory where stream subdirectories are created.
    pub base_dir: String,
    /// Maximum records to buffer in memory before forcing a file write.
    pub max_buffer_size: usize,
    /// On-disk format for newly opened files.
    #[serde(default)]
    pub format: RawLoggerFormat,
    /// Compression level used when `format == ProtoZstd`. Defaults to 3 if
    /// unset, matching the design doc's "warm tier" recommendation.
    #[serde(default)]
    pub zstd_level: Option<i32>,
}

impl Default for RawLoggerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_dir: String::new(),
            max_buffer_size: 1000,
            format: RawLoggerFormat::default(),
            zstd_level: None,
        }
    }
}

/// Trait for persistent state backends.
#[async_trait]
pub trait StateBackend: Send + Sync {
    async fn get_agent_state(&self, agent_id: &str) -> SFResult<Option<crate::AgentState>>;
    async fn set_agent_state(&self, agent_id: &str, state: &crate::AgentState) -> SFResult<()>;

    /// Compare-and-swap agent state.
    /// Atomically sets `agent_id` to `new` only if the current state
    /// equals `expected`. Returns `true` if the swap succeeded.
    async fn cas_agent_state(
        &self,
        agent_id: &str,
        expected: &crate::AgentState,
        new: &crate::AgentState,
    ) -> SFResult<bool>;

    async fn get_checkpoint(&self, task_id: &str) -> SFResult<Option<crate::TaskCheckpoint>>;
    async fn save_checkpoint(&self, checkpoint: &crate::TaskCheckpoint) -> SFResult<()>;
    async fn append_event(&self, task_id: &str, event: &crate::Event) -> SFResult<u64>;
    async fn get_events(
        &self,
        task_id: &str,
        offset: u64,
        limit: usize,
    ) -> SFResult<Vec<crate::Event>>;
    async fn get_board(&self, task_id: &str) -> SFResult<Option<crate::ContextBoard>>;
    async fn set_board_field(&self, task_id: &str, field: &str, value: &str) -> SFResult<()>;
    async fn delete_checkpoint(&self, task_id: &str) -> SFResult<()>;
    async fn delete_board(&self, task_id: &str) -> SFResult<()>;
    async fn remove_board_field(&self, task_id: &str, field: &str) -> SFResult<()>;

    /// Save the full DAG executor state for a workspace.
    async fn save_dag_state(&self, workspace_id: &str, state: &serde_json::Value) -> SFResult<()> {
        let _ = workspace_id;
        let _ = state;
        Ok(())
    }

    /// Load the full DAG executor state for a workspace.
    async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
        let _ = workspace_id;
        Ok(None)
    }

    // ─── Fine-grained DAG state operations (multi-instance support) ───

    /// Get a single task from the DAG state.
    async fn dag_get_task(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Option<crate::Task>> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_get_task".into()))
    }

    /// Save or update a single task in the DAG state.
    async fn dag_set_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        _task: &crate::Task,
    ) -> SFResult<()> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_set_task".into()))
    }

    /// Remove a task from the DAG state.
    async fn dag_remove_task(&self, workspace_id: &str, task_id: &str) -> SFResult<()> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_remove_task".into()))
    }

    /// List all task IDs in a workspace.
    async fn dag_list_tasks(&self, workspace_id: &str) -> SFResult<Vec<String>> {
        let _ = workspace_id;
        Err(SFError::NotImplemented("dag_list_tasks".into()))
    }

    /// Get dependency list (task IDs this task is blocked by).
    async fn dag_get_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
    ) -> SFResult<Vec<String>> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_get_dependencies".into()))
    }

    /// Set dependency list for a task.
    async fn dag_set_dependencies(
        &self,
        workspace_id: &str,
        task_id: &str,
        _deps: &[String],
    ) -> SFResult<()> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_set_dependencies".into()))
    }

    /// Get dependents list (task IDs blocked by this task).
    async fn dag_get_dependents(&self, workspace_id: &str, task_id: &str) -> SFResult<Vec<String>> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_get_dependents".into()))
    }

    /// Set dependents list for a task.
    async fn dag_set_dependents(
        &self,
        workspace_id: &str,
        task_id: &str,
        _dependents: &[String],
    ) -> SFResult<()> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_set_dependents".into()))
    }

    /// Atomically complete a task and compute ready dependents.
    /// Returns the list of task IDs that became Ready.
    async fn dag_complete_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        _result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_complete_task".into()))
    }

    /// Atomically fail a task.
    /// Returns (should_retry, cancelled_task_ids).
    async fn dag_fail_task(
        &self,
        workspace_id: &str,
        task_id: &str,
        _error: String,
        _max_retries: u32,
    ) -> SFResult<(bool, Vec<String>)> {
        let _ = workspace_id;
        let _ = task_id;
        Err(SFError::NotImplemented("dag_fail_task".into()))
    }

    /// Clear all DAG state for a workspace.
    async fn dag_clear_workspace(&self, workspace_id: &str) -> SFResult<()> {
        let _ = workspace_id;
        Err(SFError::NotImplemented("dag_clear_workspace".into()))
    }
}

/// Sparse vector representation for keyword-aware retrieval.
/// Each non-zero dimension corresponds to a vocabulary token (e.g. BGE-M3
/// sparse output).  Compatible with Qdrant sparse-vector indexes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Default)]
pub struct SparseEmbedding {
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

impl SparseEmbedding {
    pub fn new(indices: Vec<u32>, values: Vec<f32>) -> Self {
        Self { indices, values }
    }
}

/// A single search result returned by [`VectorBackend::search`].
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    pub id: String,
    pub score: f32,
    pub metadata: Value,
}

/// Runtime vector-storage abstraction for semantic search and long-term memory.
/// Implementations: Qdrant (default), Milvus, Memory (testing).
#[async_trait]
pub trait VectorBackend: Send + Sync {
    /// Create a new collection with the given dimension.
    async fn create_collection(&self, collection: &str, dimension: usize) -> SFResult<()>;

    /// Delete a collection and all its vectors.
    async fn delete_collection(&self, collection: &str) -> SFResult<()>;

    /// Insert dense vectors with associated metadata.
    async fn insert(
        &self,
        collection: &str,
        vectors: Vec<Vec<f32>>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>>;

    /// Insert sparse vectors with associated metadata.
    /// Backends that do not support sparse vectors may return an error or
    /// implement a no-op fallback.
    async fn insert_sparse(
        &self,
        collection: &str,
        sparse: Vec<SparseEmbedding>,
        metadata: Vec<Value>,
    ) -> SFResult<Vec<String>>;

    /// Search for the top-k most similar dense vectors.
    async fn search(
        &self,
        collection: &str,
        vector: &[f32],
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>>;

    /// Search for the top-k most similar sparse vectors.
    /// Default implementation returns an empty result set so callers can
    /// gracefully degrade to dense-only search.
    async fn search_sparse(
        &self,
        collection: &str,
        sparse: &SparseEmbedding,
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let _ = (collection, sparse, top_k);
        Ok(Vec::new())
    }

    /// Hybrid search combining dense and sparse vectors.
    /// The default implementation performs dense recall and sparse recall
    /// independently, then fuses the two result lists via Reciprocal Rank
    /// Fusion (RRF, k=60).  Production backends (e.g. Qdrant) should
    /// override this to perform native hybrid search when available.
    async fn search_hybrid(
        &self,
        collection: &str,
        dense: &[f32],
        sparse: Option<&SparseEmbedding>,
        top_k: usize,
    ) -> SFResult<Vec<VectorSearchResult>> {
        let dense_results = self.search(collection, dense, top_k * 2).await?;

        let sparse_results = if let Some(s) = sparse {
            self.search_sparse(collection, s, top_k * 2).await?
        } else {
            Vec::new()
        };

        if sparse_results.is_empty() {
            let mut out = dense_results;
            out.truncate(top_k);
            return Ok(out);
        }

        // RRF fusion (k=60).
        const RRF_K: f32 = 60.0;
        let mut fused_scores: std::collections::HashMap<String, f32> =
            std::collections::HashMap::new();

        for (rank, r) in dense_results.iter().enumerate() {
            let score = 1.0 / (RRF_K + (rank + 1) as f32);
            *fused_scores.entry(r.id.clone()).or_insert(0.0) += score;
        }

        for (rank, r) in sparse_results.iter().enumerate() {
            let score = 1.0 / (RRF_K + (rank + 1) as f32);
            *fused_scores.entry(r.id.clone()).or_insert(0.0) += score;
        }

        let mut ranked: Vec<(String, f32)> = fused_scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(top_k);

        // Build final results preserving metadata from dense results when
        // available, otherwise from sparse results.
        let dense_map: std::collections::HashMap<String, VectorSearchResult> = dense_results
            .into_iter()
            .map(|r| (r.id.clone(), r))
            .collect();
        let sparse_map: std::collections::HashMap<String, VectorSearchResult> = sparse_results
            .into_iter()
            .map(|r| (r.id.clone(), r))
            .collect();

        let mut out = Vec::with_capacity(ranked.len());
        for (id, score) in ranked {
            if let Some(mut r) = dense_map
                .get(&id)
                .cloned()
                .or_else(|| sparse_map.get(&id).cloned())
            {
                r.score = score;
                out.push(r);
            }
        }
        Ok(out)
    }

    /// Delete vectors by their IDs.
    async fn delete(&self, collection: &str, ids: &[String]) -> SFResult<()>;

    /// Check whether a collection exists.
    async fn collection_exists(&self, collection: &str) -> SFResult<bool>;
}

/// WAL (Write-Ahead Log) record for event persistence.
/// Each record is a self-contained, serializable unit that can be
/// appended to a durable log and replayed later for recovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    /// Monotonic sequence number within a session.
    pub seq: u64,
    /// Session identifier for grouping related records.
    pub session_id: String,
    /// Event type discriminator.
    pub event_type: WalEventType,
    /// JSON payload. Schema depends on event_type.
    pub payload: serde_json::Value,
    /// UTC timestamp of when the record was created.
    pub timestamp: DateTime<Utc>,
    /// Optional checksum for integrity verification (future).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

/// Event type classification for WAL records.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WalEventType {
    /// Agent lifecycle start.
    AgentStart,
    /// Agent lifecycle end.
    AgentEnd,
    /// A turn started.
    TurnStart,
    /// A turn ended.
    TurnEnd,
    /// LLM message started streaming.
    MessageStart,
    /// LLM message content delta.
    MessageDelta,
    /// LLM message finished.
    MessageEnd,
    /// Tool execution started.
    ToolExecutionStart,
    /// Tool execution progress.
    ToolExecutionDelta,
    /// Tool execution completed.
    ToolExecutionEnd,
    /// State machine transition.
    StateChange,
    /// Task status changed in orchestrator.
    TaskStatusChange,
    /// Self-review completed.
    SelfReview,
    /// A ReAct step started.
    ReActStepStart,
    /// A ReAct step ended.
    ReActStepEnd,
    /// Agent encountered an internal error.
    AgentError,
    /// Resource threshold breached.
    ResourceAlert,
    /// Heartbeat event.
    Heartbeat,
    /// Checkpoint saved successfully.
    CheckpointSaved,
    /// Custom application event.
    Custom { name: String },
}

/// WAL operation errors.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Record not found: session={session_id}, seq={seq}")]
    NotFound { session_id: String, seq: u64 },

    #[error("Corrupted record at seq={seq}: {reason}")]
    Corrupted { seq: u64, reason: String },
}

/// WAL backend trait.
/// Implementations provide durable storage for WAL records.
/// Higher-level logic (batching, compression, retention) lives in the consumer layer.
#[async_trait::async_trait]
pub trait WalBackend: Send + Sync + std::fmt::Debug {
    /// Append a single record to the log.
    /// Returns the sequence number assigned to the record.
    async fn append(&self, record: WalRecord) -> Result<u64, WalError>;

    /// Read records starting from the given sequence number (inclusive).
    async fn read_since(&self, session_id: &str, seq: u64) -> Result<Vec<WalRecord>, WalError>;

    /// Read the latest N records for a session.
    async fn read_latest(&self, session_id: &str, limit: usize)
        -> Result<Vec<WalRecord>, WalError>;

    /// Truncate records before the given sequence number for a session.
    async fn truncate_before(&self, session_id: &str, seq: u64) -> Result<(), WalError>;

    /// Get the next sequence number for a session.
    async fn next_seq(&self, session_id: &str) -> Result<u64, WalError>;
}

/// Helper to build WAL records with auto timestamp.
impl WalRecord {
    pub fn new(
        seq: u64,
        session_id: impl Into<String>,
        event_type: WalEventType,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            seq,
            session_id: session_id.into(),
            event_type,
            payload,
            timestamp: Utc::now(),
            checksum: None,
        }
    }

    /// Encode this record as a JSON line (backward-compatible format).
    pub fn encode_to_json_line(&self) -> Result<String, WalError> {
        serde_json::to_string(self).map_err(WalError::Serialization)
    }

    /// Decode a record from a JSON line (backward-compatible format).
    pub fn decode_from_json_line(line: &str) -> Result<Self, WalError> {
        serde_json::from_str(line).map_err(WalError::Serialization)
    }
}

// Hot/Warm/Cold tier migration for raw-data files.
// `FileRawLogger` writes append-only length-delimited Protobuf files into
// `{base_dir}/{stream}/{YYYY-MM-DD}.proto.bin (or .proto.bin.zst when
// compression is enabled)`. Those files start in the **hot**
// tier (local SSD, uncompressed). After `hot_duration` they are compressed
// with zstd and stay locally as the **warm** tier. After `warm_duration`
// they are uploaded to the configured [`ObjectBackend`] (S3/COS/MinIO) and
// become the **cold** tier; the local copy is removed once the upload is
// verified.
// Each migrated file produces a [`RawLogIndexEntry`] persisted via a
// [`RawLogIndexStore`] (PostgreSQL in production, memory in tests). The
// query API in `cog-gateway` reads the same store to locate raw files.

/// Storage tier for raw-data files and traces.
/// `StorageTier` is the canonical name; `Tier` is kept as a backward-compatible alias.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    Hot,
    Warm,
    Cold,
}

impl StorageTier {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageTier::Hot => "hot",
            StorageTier::Warm => "warm",
            StorageTier::Cold => "cold",
        }
    }
}

impl std::str::FromStr for StorageTier {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "hot" => Ok(StorageTier::Hot),
            "warm" => Ok(StorageTier::Warm),
            "cold" => Ok(StorageTier::Cold),
            _ => Err(()),
        }
    }
}

/// Tier-migration policy. Durations are measured from the file's modification
/// timestamp.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierPolicy {
    /// Files newer than this stay in the hot tier (local, uncompressed).
    pub hot_duration: Duration,
    /// Files between `hot_duration` and `hot_duration + warm_duration`
    /// remain on local disk, compressed with zstd.
    pub warm_duration: Duration,
    /// zstd compression level for the warm tier (typical: 3).
    pub warm_compression_level: i32,
    /// zstd compression level for the cold tier (typical: 9).
    pub cold_compression_level: i32,
    /// How often the migrator scans for eligible files.
    pub scan_interval: Duration,
    /// Object-storage key prefix for cold-tier uploads.
    pub cold_key_prefix: String,
}

impl Default for TierPolicy {
    fn default() -> Self {
        Self {
            hot_duration: Duration::from_secs(7 * 24 * 60 * 60), // 7 d
            warm_duration: Duration::from_secs(90 * 24 * 60 * 60), // 90 d
            warm_compression_level: 3,
            cold_compression_level: 9,
            scan_interval: Duration::from_secs(3600), // hourly
            cold_key_prefix: "raw".into(),
        }
    }
}

/// One row of the `raw_log_index` table.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RawLogIndexEntry {
    // Hour of day (0-23) for hourly rollup granularity.
    pub hour: u8,
    pub stream_name: String,
    pub log_date: NaiveDate,
    pub file_path: String,
    pub tier: StorageTier,
    pub size_bytes: u64,
    /// Number of events (records) in the file.
    pub event_count: u64,
    pub checksum: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

/// Filter used by [`RawLogIndexStore::query`].
#[derive(Debug, Clone, Default)]
pub struct RawLogQuery {
    pub hour: Option<u8>,
    pub stream: Option<String>,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub tier: Option<StorageTier>,
    pub limit: Option<usize>,
}

/// Persistence abstraction for `raw_log_index`. Implementations: in-memory
/// (testing), PostgreSQL (production — to be added when sqlx wiring lands).
#[async_trait]
pub trait RawLogIndexStore: Send + Sync {
    async fn upsert(&self, entry: RawLogIndexEntry) -> SFResult<()>;
    async fn query(&self, q: &RawLogQuery) -> SFResult<Vec<RawLogIndexEntry>>;
}

/// doc, the runtime, and the database stay in lock-step.
pub const RAW_LOG_INDEX_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS raw_log_index (
    id            BIGSERIAL,
    stream_name   VARCHAR(32)  NOT NULL,
    log_date      DATE         NOT NULL,
    file_path     TEXT         NOT NULL,
    tier          VARCHAR(8)   NOT NULL DEFAULT 'hot',
    size_bytes    BIGINT       NOT NULL DEFAULT 0,
    checksum      VARCHAR(128) NOT NULL,
    start_time    TIMESTAMPTZ  NOT NULL,
    end_time      TIMESTAMPTZ  NOT NULL,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_name, log_date)
) PARTITION BY RANGE (log_date);

CREATE INDEX IF NOT EXISTS raw_log_index_stream_time_idx
    ON raw_log_index (stream_name, start_time, end_time);
"#;
