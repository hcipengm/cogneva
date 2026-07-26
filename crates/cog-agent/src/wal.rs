use cog_core::{AgentEvent, WalBackend, WalError, WalEventType, WalRecord};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Agent-level WAL wrapper.
/// Bridges `AgentEvent` to `WalRecord` and provides session-scoped
/// sequence number management.
#[derive(Debug)]
pub struct AgentWal {
    backend: Arc<dyn WalBackend>,
    session_id: String,
    next_seq: AtomicU64,
}

impl AgentWal {
    pub async fn new(
        backend: Arc<dyn WalBackend>,
        session_id: impl Into<String>,
    ) -> Result<Self, WalError> {
        let session_id = session_id.into();
        let next_seq = backend.next_seq(&session_id).await?;
        Ok(Self {
            backend,
            session_id,
            next_seq: AtomicU64::new(next_seq),
        })
    }

    /// Append an AgentEvent to the WAL.
    pub async fn append(&self, event: &AgentEvent) -> Result<u64, WalError> {
        let seq = self.next_seq.fetch_add(1, Ordering::SeqCst);
        let (event_type, payload) = agent_event_to_wal(event);
        let record = WalRecord::new(seq, &self.session_id, event_type, payload);
        self.backend.append(record).await
    }

    /// Read records since the given sequence number.
    pub async fn read_since(&self, seq: u64) -> Result<Vec<WalRecord>, WalError> {
        self.backend.read_since(&self.session_id, seq).await
    }

    /// Read the latest N records.
    pub async fn read_latest(&self, limit: usize) -> Result<Vec<WalRecord>, WalError> {
        self.backend.read_latest(&self.session_id, limit).await
    }

    /// Truncate records before the given sequence number.
    pub async fn truncate_before(&self, seq: u64) -> Result<(), WalError> {
        self.backend.truncate_before(&self.session_id, seq).await
    }

    /// Get the current next sequence number.
    pub fn current_seq(&self) -> u64 {
        self.next_seq.load(Ordering::SeqCst)
    }
}

/// Convert an AgentEvent to WAL event type and JSON payload.
fn agent_event_to_wal(event: &AgentEvent) -> (WalEventType, serde_json::Value) {
    match event {
        AgentEvent::AgentStart {
            agent_id,
            timestamp,
            ..
        } => (
            WalEventType::AgentStart,
            json!({ "agent_id": agent_id, "timestamp": timestamp }),
        ),
        AgentEvent::AgentEnd {
            agent_id,
            messages,
            timestamp,
            ..
        } => (
            WalEventType::AgentEnd,
            json!({ "agent_id": agent_id, "messages": messages, "timestamp": timestamp }),
        ),
        AgentEvent::TurnStart {
            agent_id,
            timestamp,
        } => (
            WalEventType::TurnStart,
            json!({ "agent_id": agent_id, "timestamp": timestamp }),
        ),
        AgentEvent::TurnEnd {
            agent_id,
            message,
            tool_results,
            timestamp,
        } => (
            WalEventType::TurnEnd,
            json!({
                "agent_id": agent_id,
                "message": message,
                "tool_results": tool_results,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::MessageStart {
            agent_id,
            message,
            timestamp,
        } => (
            WalEventType::MessageStart,
            json!({ "agent_id": agent_id, "message": message, "timestamp": timestamp }),
        ),
        AgentEvent::MessageUpdate {
            agent_id,
            assistant_event,
            message,
            timestamp,
        } => (
            WalEventType::MessageDelta,
            json!({
                "agent_id": agent_id,
                "assistant_event": assistant_event,
                "message": message,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::MessageEnd {
            agent_id,
            message,
            timestamp,
        } => (
            WalEventType::MessageEnd,
            json!({ "agent_id": agent_id, "message": message, "timestamp": timestamp }),
        ),
        AgentEvent::ToolExecutionStart {
            agent_id,
            tool_call_id,
            tool_name,
            args,
            timestamp,
        } => (
            WalEventType::ToolExecutionStart,
            json!({
                "agent_id": agent_id,
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "args": args,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::ToolExecutionUpdate {
            agent_id,
            tool_call_id,
            partial_result,
            timestamp,
        } => (
            WalEventType::ToolExecutionDelta,
            json!({
                "agent_id": agent_id,
                "tool_call_id": tool_call_id,
                "partial_result": partial_result,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::ToolExecutionEnd {
            agent_id,
            tool_call_id,
            result,
            is_error,
            timestamp,
        } => (
            WalEventType::ToolExecutionEnd,
            json!({
                "agent_id": agent_id,
                "tool_call_id": tool_call_id,
                "result": result,
                "is_error": is_error,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::StateChange {
            agent_id,
            from,
            to,
            timestamp,
            ..
        } => (
            WalEventType::StateChange,
            json!({
                "agent_id": agent_id,
                "from": from,
                "to": to,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::TaskStatusChange {
            task_id,
            status,
            agent_id,
            timestamp,
            ..
        } => (
            WalEventType::TaskStatusChange,
            json!({
                "task_id": task_id,
                "status": status,
                "agent_id": agent_id,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::SelfReview {
            agent_id,
            status: review_status,
            score,
            summary,
            critique,
            suggestions,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "self_review".into(),
            },
            json!({
                "agent_id": agent_id,
                "status": review_status,
                "score": score,
                "summary": summary,
                "critique": critique,
                "suggestions": suggestions,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::ReActStepStart {
            agent_id,
            iteration,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "react_step_start".into(),
            },
            json!({
                "agent_id": agent_id,
                "iteration": iteration,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::ReActStepEnd {
            agent_id,
            iteration,
            thought,
            tool_calls,
            observations,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "react_step_end".into(),
            },
            json!({
                "agent_id": agent_id,
                "iteration": iteration,
                "thought": thought,
                "tool_calls": tool_calls,
                "observations": observations,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::AgentError {
            agent_id,
            error_code,
            severity,
            details,
            crew_id,
            squad_id,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "agent_error".into(),
            },
            json!({
                "agent_id": agent_id,
                "error_code": error_code,
                "severity": severity,
                "details": details,
                "crew_id": crew_id,
                "squad_id": squad_id,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::ResourceAlert {
            agent_id,
            metric,
            threshold,
            current,
            crew_id,
            squad_id,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "resource_alert".into(),
            },
            json!({
                "agent_id": agent_id,
                "metric": metric,
                "threshold": threshold,
                "current": current,
                "crew_id": crew_id,
                "squad_id": squad_id,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::Heartbeat {
            agent_id,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "heartbeat".into(),
            },
            json!({
                "agent_id": agent_id,
                "timestamp": timestamp
            }),
        ),
        AgentEvent::CheckpointSaved {
            agent_id,
            checkpoint_id,
            task_id,
            crew_id,
            squad_id,
            timestamp,
        } => (
            WalEventType::Custom {
                name: "checkpoint_saved".into(),
            },
            json!({
                "agent_id": agent_id,
                "checkpoint_id": checkpoint_id,
                "task_id": task_id,
                "crew_id": crew_id,
                "squad_id": squad_id,
                "timestamp": timestamp
            }),
        ),
    }
}
