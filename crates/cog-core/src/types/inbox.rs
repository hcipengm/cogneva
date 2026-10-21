use serde::{Deserialize, Serialize};

/// The stream one agent's commands are published to, and the one its consumer
/// group reads.
///
/// Producer and consumer sit in different crates that do not depend on each
/// other: the gateway addresses an agent it cannot see, and the agent's own
/// consumer loop reads what it is given. The name is therefore part of the
/// protocol rather than an implementation detail of either side, and it lives
/// beside the wire form it carries. Spelling it twice would let a producer
/// publish into a stream nobody reads and call the command delivered.
pub fn agent_inbox_stream(agent_id: &str) -> String {
    format!("orchestrator:agent:{agent_id}:inbox")
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// The name is a wire fact, not a formatting choice: consumer groups under
    /// this exact spelling are already live in the deployment, and a command
    /// published under any other name is read by nobody while the publisher
    /// still reports success. Both ends call this function, so the assertion
    /// is about the spelling, not about the two ends agreeing.
    #[test]
    fn the_inbox_stream_name_does_not_drift() {
        assert_eq!(
            agent_inbox_stream("cogneva-worker-0"),
            "orchestrator:agent:cogneva-worker-0:inbox"
        );
    }
}
