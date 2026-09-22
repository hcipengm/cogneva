use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::message::{Message, ToolCall};
use super::ContentBlock;

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
///
/// Each variant carries only what changed. The accumulated message is not
/// repeated per event: the provider already holds the accumulator it builds
/// these events from, and a snapshot per event made the persisted size of one
/// message grow with `message_length × delta_count` — a planner run of 422
/// stream updates weighed 16.7 MB while the deltas' own text weighed 6.4 KB.
/// Consumers fold the deltas with [`AssistantMessageEvent::apply`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AssistantMessageEvent {
    Start {
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextStart {
        content_index: usize,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextDelta {
        content_index: usize,
        delta: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    TextEnd {
        content_index: usize,
        content: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingStart {
        content_index: usize,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingDelta {
        content_index: usize,
        delta: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ThinkingEnd {
        content_index: usize,
        content: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallStart {
        content_index: usize,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallDelta {
        content_index: usize,
        delta: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolCallEnd {
        content_index: usize,
        tool_call: ToolCall,
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

impl AssistantMessageEvent {
    /// Fold one incremental event into the message being accumulated.
    ///
    /// This is the single accumulator for an assistant stream. The runtime uses
    /// it to build the message, and anything replaying a trace folds the same
    /// deltas the same way, so the two cannot drift.
    ///
    /// Total by construction: a block announced at an index past the current
    /// content pads the gap rather than panicking, so a malformed stream
    /// degrades to an extra empty block instead of taking the process down.
    pub fn apply(&self, message: &mut Message) {
        match self {
            // Carries the whole finalized message, so it supersedes anything
            // the deltas have built so far.
            AssistantMessageEvent::Done {
                message: done_message,
                ..
            } => *message = done_message.clone(),
            AssistantMessageEvent::Error { .. } => {}
            event => {
                let Message::Assistant { content, .. } = message else {
                    return;
                };
                match event {
                    AssistantMessageEvent::Start { .. } => content.clear(),
                    AssistantMessageEvent::TextStart { content_index, .. } => {
                        block_at(content, *content_index, || ContentBlock::text(""));
                    }
                    AssistantMessageEvent::TextDelta {
                        content_index,
                        delta,
                        ..
                    } => {
                        if let ContentBlock::Text { text, .. } =
                            block_at(content, *content_index, || ContentBlock::text(""))
                        {
                            text.push_str(delta);
                        }
                    }
                    AssistantMessageEvent::TextEnd {
                        content_index,
                        content: final_text,
                        ..
                    } => {
                        if let ContentBlock::Text { text, .. } =
                            block_at(content, *content_index, || ContentBlock::text(""))
                        {
                            text.clone_from(final_text);
                        }
                    }
                    AssistantMessageEvent::ThinkingStart { content_index, .. } => {
                        block_at(content, *content_index, || ContentBlock::thinking(""));
                    }
                    AssistantMessageEvent::ThinkingDelta {
                        content_index,
                        delta,
                        ..
                    } => {
                        if let ContentBlock::Thinking { thinking, .. } =
                            block_at(content, *content_index, || ContentBlock::thinking(""))
                        {
                            thinking.push_str(delta);
                        }
                    }
                    AssistantMessageEvent::ThinkingEnd {
                        content_index,
                        content: final_thinking,
                        ..
                    } => {
                        if let ContentBlock::Thinking { thinking, .. } =
                            block_at(content, *content_index, || ContentBlock::thinking(""))
                        {
                            thinking.clone_from(final_thinking);
                        }
                    }
                    AssistantMessageEvent::ToolCallStart { content_index, .. } => {
                        block_at(content, *content_index, || {
                            ContentBlock::tool_call(
                                "",
                                "",
                                serde_json::Value::Object(Default::default()),
                            )
                        });
                    }
                    // A tool call's arguments stream in as JSON fragments, which
                    // have no representation in the block's parsed `arguments`.
                    // The assembled call arrives with ToolCallEnd, so this event
                    // exists for token-level progress, not for the accumulator.
                    AssistantMessageEvent::ToolCallDelta { .. } => {}
                    AssistantMessageEvent::ToolCallEnd {
                        content_index,
                        tool_call,
                        ..
                    } => {
                        let assembled = ContentBlock::tool_call(
                            tool_call.id.clone(),
                            tool_call.name.clone(),
                            tool_call.arguments.clone(),
                        );
                        let slot = block_at(content, *content_index, || assembled.clone());
                        *slot = assembled;
                    }
                    // Token accounting rides on the terminal message, which
                    // Done carries; these two are handled before the borrow.
                    AssistantMessageEvent::Usage { .. }
                    | AssistantMessageEvent::Done { .. }
                    | AssistantMessageEvent::Error { .. } => {}
                }
            }
        }
    }
}

/// The content block at `index`, creating it if the announced index is at or
/// past the end. Gaps are padded with empty text blocks so the announced index
/// keeps meaning for the deltas that follow it.
fn block_at(
    content: &mut Vec<ContentBlock>,
    index: usize,
    make: impl FnOnce() -> ContentBlock,
) -> &mut ContentBlock {
    if index >= content.len() {
        while content.len() < index {
            content.push(ContentBlock::text(""));
        }
        content.push(make());
    }
    &mut content[index]
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
    ///
    /// The accumulated message travels in `assistant_event` — every
    /// snapshot-carrying variant carries it as `partial`. It is deliberately
    /// not repeated as a sibling field: the producer used to set that sibling
    /// from `partial`, so both halves of the struct held byte-identical copies
    /// of a message that grows with every delta. One of them then doubled the
    /// size of every persisted trace and of the trace collector's in-memory
    /// buffer accounting.
    MessageUpdate {
        agent_id: String,
        assistant_event: AssistantMessageEvent,
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
    /// A running task stopped being renewed by whoever held it. Distinct from
    /// [`Self::TaskTimeout`] because the two say opposite things to a reader:
    /// a timeout is a run that spent its whole budget, while this is a run
    /// nobody is holding any more, reclaimed long before its budget elapsed.
    /// Folding the two into `TaskTimeout` would make a restart read as a run
    /// that used everything it was given.
    TaskLeaseExpired {
        task_id: String,
        /// The process whose renewal stopped. Absent only for a row written
        /// before leases existed, which is the one way a running task can have
        /// no holder.
        #[serde(skip_serializing_if = "Option::is_none")]
        owner: Option<String>,
        expired_at: DateTime<Utc>,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A delta event must carry only what changed. Each one used to also carry
    /// the whole accumulated message as `partial`, so a persisted message grew
    /// with `message_length × delta_count`: one planner run of 422 deltas
    /// weighed 16.7 MB while its own delta text weighed 6.4 KB. Consumers fold
    /// the deltas with [`AssistantMessageEvent::apply`] instead, and this test
    /// fails the moment a snapshot field is reintroduced.
    #[test]
    fn delta_events_carry_no_message_snapshot() {
        let cases = [
            serde_json::to_value(AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "x".into(),
                timestamp: Utc::now(),
            })
            .unwrap(),
            serde_json::to_value(AssistantMessageEvent::ThinkingDelta {
                content_index: 0,
                delta: "x".into(),
                timestamp: Utc::now(),
            })
            .unwrap(),
            serde_json::to_value(AssistantMessageEvent::ToolCallDelta {
                content_index: 0,
                delta: "x".into(),
                timestamp: Utc::now(),
            })
            .unwrap(),
        ];

        for value in cases {
            let fields = value.as_object().unwrap();
            let mut names: Vec<&str> = fields.keys().map(String::as_str).collect();
            names.sort_unstable();
            assert_eq!(
                names,
                ["content_index", "delta", "timestamp", "type"],
                "a delta event grew beyond its own delta: {value}"
            );
        }
    }

    /// The runtime builds the assistant message by folding this stream, and
    /// `Done` carries the provider's own copy of the same message. If the fold
    /// ever disagrees with it, replaying a trace reconstructs something the live
    /// run never produced.
    #[test]
    fn folding_a_delta_stream_reproduces_the_done_message() {
        let expected = Message::assistant(vec![
            ContentBlock::text("Hello"),
            ContentBlock::thinking("why"),
            ContentBlock::tool_call("c1", "read", serde_json::json!({ "path": "a.rs" })),
        ]);

        let events = [
            AssistantMessageEvent::Start {
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::TextStart {
                content_index: 0,
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "Hel".into(),
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::TextDelta {
                content_index: 0,
                delta: "lo".into(),
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::ThinkingStart {
                content_index: 1,
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::ThinkingDelta {
                content_index: 1,
                delta: "why".into(),
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::ToolCallStart {
                content_index: 2,
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::ToolCallDelta {
                content_index: 2,
                delta: "{\"path\":".into(),
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::ToolCallEnd {
                content_index: 2,
                tool_call: ToolCall {
                    id: "c1".into(),
                    name: "read".into(),
                    arguments: serde_json::json!({ "path": "a.rs" }),
                },
                timestamp: Utc::now(),
            },
            AssistantMessageEvent::Usage {
                prompt_tokens: 3,
                completion_tokens: 4,
                total_tokens: 7,
                timestamp: Utc::now(),
            },
        ];

        let mut folded = Message::assistant(Vec::new());
        for event in &events {
            event.apply(&mut folded);
        }
        let Message::Assistant { content, .. } = &folded else {
            panic!("folding a delta stream did not produce an assistant message");
        };
        assert_eq!(
            content,
            expected.content_blocks().unwrap(),
            "the folded stream disagrees with the message the provider finalized"
        );

        // Done supersedes whatever the deltas built, and the fold must land on
        // exactly the message it carries.
        AssistantMessageEvent::Done {
            reason: StopReason::ToolUse,
            message: expected.clone(),
            timestamp: Utc::now(),
        }
        .apply(&mut folded);
        assert_eq!(folded, expected);
    }
}
