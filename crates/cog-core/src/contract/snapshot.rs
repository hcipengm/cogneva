//!Unified snapshot types for Agent traces and checkpoints.
// - `AgentTrace`    — execution traces for deterministic replay (was `cog-observability::Snapshot`)
// - `AgentCheckpoint` — runtime state for hand-off / resume (was `cog-core::Snapshot`)
// - `TraceStore`    — trait for trace persistence
// - `CheckpointStore` — trait for checkpoint persistence (was `SnapshotStore`)

use crate::{AgentEvent, Message, SFResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ==========================================================================
// TraceMeta — lightweight metadata for list operations
// ==========================================================================

/// Lightweight metadata view of an [`AgentTrace`].
/// Used by [`TraceStore::list_meta`] to avoid loading heavy event arrays
/// (events, llm_requests, llm_responses, tool_calls) when only metadata
/// is needed (e.g. UI listing, tier-migration scanning).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceMeta {
    pub trace_id: String,
    pub session_id: Option<String>,
    pub task_id: String,
    pub agent_id: String,
    pub created_at: DateTime<Utc>,
    pub event_count: u64,
    pub byte_size: u64,
    pub version: String,
    pub tier: crate::StorageTier,
    pub compression: i32,
    pub checksum: String,
}

impl TraceMeta {
    /// Build a `TraceMeta` from a full `AgentTrace`.
    pub fn from_trace(trace: &AgentTrace) -> Self {
        Self {
            trace_id: trace.trace_id.clone(),
            session_id: trace.session_id.clone(),
            task_id: trace.task_id.clone(),
            agent_id: trace.agent_id.clone(),
            created_at: trace.created_at,
            event_count: trace.event_count,
            byte_size: trace.byte_size,
            version: trace.version.clone(),
            tier: trace.tier,
            compression: trace.compression,
            checksum: trace.checksum.clone(),
        }
    }
}

// ==========================================================================
// AgentTrace — execution trace for replay / observability
// ==========================================================================

/// Agent 执行轨迹（Execution Traces）。
/// 不是运行时状态冻结，而是旁路记录的可序列化交互数据。
/// 捕获内容：LLM 请求/响应、EventStream 事件、Tool 调用记录。
/// 不包含：Agent 内部内存、闭包变量、进程状态。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentTrace {
    pub trace_id: String,
    pub session_id: Option<String>,
    pub task_id: String,
    pub agent_id: String,
    pub created_at: DateTime<Utc>,
    pub event_count: u64,
    pub byte_size: u64,
    pub version: String,
    pub tier: crate::StorageTier,
    pub compression: i32,
    pub checksum: String,
    /// Execution events (AgentEvent stream).
    pub events: Vec<AgentEvent>,
    /// LLM request records (optional, for full replay).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub llm_requests: Vec<LlmRequest>,
    /// LLM response records (optional, for full replay).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub llm_responses: Vec<LlmResponse>,
    /// Tool call records (optional, for full replay).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCallRecord>,
}

// ==========================================================================
// AgentCheckpoint — runtime state for hand-off
// ==========================================================================

/// Agent 状态快照（用于任务 hand-off / 断点续传）。
/// 捕获 Agent 运行时的完整状态，用于快速恢复和迁移。
/// 这是 `cog-core::Snapshot` 的继任者，命名更准确。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentCheckpoint {
    pub checkpoint_id: String,
    pub task_id: String,
    pub agent_state: serde_json::Value,
    pub context_window: Vec<Message>,
    pub event_offset: u64,
    pub timestamp: DateTime<Utc>,
}

// ==========================================================================
// Storage traits
// ==========================================================================

/// 执行轨迹存储契约。
#[async_trait]
pub trait TraceStore: Send + Sync {
    /// Save a trace and return its `trace_id`.
    async fn save(&self, trace: &AgentTrace) -> SFResult<String>;

    /// Load a trace by id, or `None` if it does not exist.
    async fn load(&self, trace_id: &str) -> SFResult<Option<AgentTrace>>;

    /// Delete a trace by id.
    async fn delete(&self, trace_id: &str) -> SFResult<()>;

    /// List recent traces up to `limit`.
    async fn list(&self, limit: usize) -> SFResult<Vec<AgentTrace>>;

    /// List lightweight metadata for recent traces up to `limit`.
    /// Backends that cannot support metadata-only queries should fall back
    /// to loading full traces and extracting [`TraceMeta`] via [`TraceMeta::from_trace`].
    async fn list_meta(&self, limit: usize) -> SFResult<Vec<TraceMeta>>;
}

/// 状态快照存储契约（`SnapshotStore` 的继任者）。
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Save a checkpoint and return its `checkpoint_id`.
    async fn save(&self, checkpoint: &AgentCheckpoint) -> SFResult<String>;

    /// Load a checkpoint by id, or `None` if it does not exist.
    async fn load(&self, checkpoint_id: &str) -> SFResult<Option<AgentCheckpoint>>;

    /// Delete a checkpoint by id.
    async fn delete(&self, checkpoint_id: &str) -> SFResult<()>;

    /// List recent checkpoints up to `limit`.
    async fn list(&self, limit: usize) -> SFResult<Vec<AgentCheckpoint>>;
}

// ==========================================================================
// Auxiliary types (for full replay)
// ==========================================================================

/// LLM request record (for deterministic replay).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmRequest {
    pub model: String,
    pub messages: Vec<Message>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    pub extra: serde_json::Value,
}

/// LLM response record (for deterministic replay).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LlmResponse {
    pub content: String,
    pub tool_calls: Vec<crate::ToolCall>,
    pub finish_reason: Option<String>,
    pub usage: Option<serde_json::Value>,
}

/// Tool call record (for deterministic replay).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCallRecord {
    pub tool_name: String,
    pub arguments: serde_json::Value,
    pub result: serde_json::Value,
    pub error: Option<String>,
    pub latency_ms: u64,
}
