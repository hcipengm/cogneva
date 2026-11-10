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
        /// 这一条是谁发的（员工 id）。`None` = 发的人没留名，或这是平台命令。
        ///
        /// 随消息走而不是只留在发布方的回执里：收件人读到的是一条**同事**发
        /// 来的话，而他手里除了这条消息没有第二处能查到那个同事是谁。缺省、
        /// 不带这个键的老消息照旧解析——线上已有的条目不能因为加一列就读不回来。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_agent: Option<String>,
    },
    /// Continue the current conversation with additional instruction.
    Continue {
        instruction: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_agent: Option<String>,
    },
    /// Abort the current run.
    Abort,
    /// Reset the agent, clearing all context and state.
    Reset,
    /// Send a steering instruction (injected as a system message).
    Steering {
        instruction: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        from_agent: Option<String>,
    },
}

impl InboxMessage {
    /// 这一条是谁发的；`None` 是「没留名」，不是「不知道是谁还硬编一个」。
    pub fn from_agent(&self) -> Option<&str> {
        match self {
            InboxMessage::Prompt { from_agent, .. }
            | InboxMessage::Continue { from_agent, .. }
            | InboxMessage::Steering { from_agent, .. } => from_agent.as_deref(),
            // 撤停与重置是平台对正在跑的那一轮说话，没有同事可指。
            InboxMessage::Abort | InboxMessage::Reset => None,
        }
    }
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

    /// 发信人随消息走；加了这一列之后，线上已有的老条目仍读得回来。
    ///
    /// 收件人手里只有这一条消息：谁发的若只留在发布方的回执里，他看到的就
    /// 是一条没有出处的话。缺省值必须在读侧成立——消息一旦进了流，形状就不再
    /// 由这里决定，旧条目没有这一列是既成事实。
    #[test]
    fn a_message_carries_who_sent_it_and_an_older_one_still_parses() {
        let sent = InboxMessage::Steering {
            instruction: "hold off until the build is green".into(),
            from_agent: Some("E0412".into()),
        };
        let wire = serde_json::to_value(&sent).expect("serializes");
        assert_eq!(wire["from_agent"], serde_json::json!("E0412"));
        let read: InboxMessage = serde_json::from_value(wire).expect("reads back");
        assert_eq!(read.from_agent(), Some("E0412"));

        let older: InboxMessage = serde_json::from_value(
            serde_json::json!({"type": "continue", "instruction": "keep going"}),
        )
        .expect("a message written before the sender column existed still parses");
        assert_eq!(older.from_agent(), None);

        let nameless = InboxMessage::Prompt {
            goal: serde_json::json!("run the tests"),
            reply_stream: None,
            from_agent: None,
        };
        let wire = serde_json::to_value(&nameless).expect("serializes");
        assert!(
            wire.get("from_agent").is_none(),
            "没留名就不写这一列，读者不必区分 null 与缺省: {wire}"
        );
    }
}
