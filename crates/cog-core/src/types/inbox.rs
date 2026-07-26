use serde::{Deserialize, Serialize};

/// Message sent to an agent's inbox via Redis Streams.
/// This is the inter-agent communication protocol used by the DagExecutor
/// consumer layer to dispatch commands to individual agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InboxMessage {
    /// Start a new agent run with the given goal.
    Prompt {
        goal: serde_json::Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        reply_stream: Option<String>,
    },
    /// Continue the current conversation with additional instruction.
    Continue { instruction: String },
    /// Abort the current run.
    Abort,
    /// Reset the agent, clearing all context and state.
    Reset,
    /// Send a steering instruction (injected as a system message).
    Steering { instruction: String },
}
