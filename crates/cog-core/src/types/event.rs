use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::message::{Message, ToolCall};

/// Stop reason for LLM response termination.
/// Aligns with pi-ai's StopReason.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    #[default]
    Stop,
    Length,
    ToolUse,
    Error,
    Aborted,
}

/// Raw LLM-level event protocol.
/// Aligns with pi-ai's AssistantMessageEvent.
/// Each incremental event carries `partial: Message` — a snapshot of the
/// accumulated message state at this point in the stream. This lets consumers
/// observe the current full state without maintaining their own accumulator.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantMessageEvent {
    Start {
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextStart {
        content_index: usize,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextDelta {
        content_index: usize,
        delta: String,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextEnd {
        content_index: usize,
        content: String,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingStart {
        content_index: usize,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingEnd {
        content_index: usize,
        content: String,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallStart {
        content_index: usize,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallDelta {
        content_index: usize,
        delta: String,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: ToolCall,
        partial: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Usage {
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Done {
        reason: StopReason,
        message: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Error {
        reason: StopReason,
        error: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
}

/// Agent lifecycle event protocol (semantic layer).
/// Built on top of AssistantMessageEvent.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum AgentEvent {
    /// Agent run started.
    AgentStart {
        agent_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Agent run ended.
    AgentEnd {
        agent_id: String,
        messages: Vec<Message>,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A new turn started.
    TurnStart {
        agent_id: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A turn ended.
    TurnEnd {
        agent_id: String,
        message: Message,
        tool_results: Vec<Message>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A message started streaming.
    MessageStart {
        agent_id: String,
        message: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Message content updated (carries raw LLM event).
    MessageUpdate {
        agent_id: String,
        assistant_event: AssistantMessageEvent,
        message: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A message finished streaming.
    MessageEnd {
        agent_id: String,
        message: Message,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Tool execution started.
    ToolExecutionStart {
        agent_id: String,
        tool_call_id: String,
        tool_name: String,
        args: serde_json::Value,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Tool execution progress update.
    ToolExecutionUpdate {
        agent_id: String,
        tool_call_id: String,
        partial_result: serde_json::Value,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Tool execution completed.
    ToolExecutionEnd {
        agent_id: String,
        tool_call_id: String,
        result: serde_json::Value,
        is_error: bool,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// State machine transition.
    StateChange {
        agent_id: String,
        from: String,
        to: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Task status changed in the orchestrator.
    TaskStatusChange {
        task_id: String,
        status: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        agent_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Self-review completed.
    SelfReview {
        agent_id: String,
        status: String, // "PASS" | "NEED_REVISION"
        score: f32,
        #[serde(skip_serializing_if = "Option::is_none")]
        summary: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        critique: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        suggestions: Option<Vec<String>>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A ReAct (Reasoning + Acting) step started.
    /// Emitted at the beginning of each Think->Act->Observe cycle.
    ReActStepStart {
        agent_id: String,
        iteration: u32,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// A ReAct (Reasoning + Acting) step completed.
    /// Emitted after the Observation phase of each cycle.
    ReActStepEnd {
        agent_id: String,
        iteration: u32,
        thought: String,
        tool_calls: Vec<ToolCall>,
        observations: Vec<serde_json::Value>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Agent encountered an internal error (panic, LLM API failure, tool exception).
    /// Maps to HookType::SystemAlert for automatic escalation.
    AgentError {
        agent_id: String,
        error_code: String,
        severity: ErrorSeverity,
        details: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Resource threshold breach (token, memory, context window).
    /// Emitted proactively by the Agent so Supervisor can react in real-time.
    ResourceAlert {
        agent_id: String,
        metric: String,
        threshold: f64,
        current: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Heartbeat event for continuous heartbeat sequence archiving.
    /// Emitted by HeartbeatDriver alongside registry renewal.
    Heartbeat {
        agent_id: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    /// Checkpoint saved successfully.
    /// Emitted after an agent persists its state snapshot.
    CheckpointSaved {
        agent_id: String,
        checkpoint_id: String,
        task_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        crew_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        squad_id: Option<String>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
}

/// Severity level for AgentError events.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSeverity {
    #[default]
    Warning,
    Critical,
    Fatal,
}

/// Task lifecycle event broadcast by the orchestrator.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TaskEvent {
    TaskCreated {
        task_id: String,
        timestamp: DateTime<Utc>,
    },
    TaskScheduled {
        task_id: String,
        timestamp: DateTime<Utc>,
    },
    TaskStarted {
        task_id: String,
        timestamp: DateTime<Utc>,
    },
    TaskCompleted {
        task_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<serde_json::Value>,
        scheduled_dependents: Vec<String>,
        timestamp: DateTime<Utc>,
    },
    TaskFailed {
        task_id: String,
        error: String,
        retried: bool,
        cancelled: Vec<String>,
        timestamp: DateTime<Utc>,
    },
    TaskCancelled {
        task_id: String,
        reason: String,
        timestamp: DateTime<Utc>,
    },
    TaskRetried {
        task_id: String,
        retry_count: u32,
        timestamp: DateTime<Utc>,
    },
    TaskTimeout {
        task_id: String,
        timeout_seconds: u64,
        timestamp: DateTime<Utc>,
    },
}

/// Legacy StreamEvent — retained for backward compatibility during migration.
/// Will be replaced by AssistantMessageEvent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StreamEvent {
    Start {
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextDelta {
        delta: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingDelta {
        delta: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCall {
        tool_call_id: String,
        name: String,
        arguments: serde_json::Value,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Usage {
        prompt_tokens: u32,
        completion_tokens: u32,
        total_tokens: u32,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Done {
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Error {
        error: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
}
