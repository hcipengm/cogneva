use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::ContentBlock;

// ─── Broadcast Scope ───────────────────────────────────────────────────────

/// The intended audience of a [`HierarchicalMessage`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BroadcastScope {
    /// All Squads / Agents that belong to a single Crew.
    Crew { crew_id: String },
    /// All Agents that belong to a single Squad (Roundtable participants etc.).
    Squad { squad_id: String },
    /// A specific Agent — point-to-point unicast on its inbox topic.
    Agent { agent_id: String },
    /// Workspace-wide alert (Supervisor + every Agent).
    Global,
}

impl BroadcastScope {
    /// Returns the formal target identifier for this scope.
    pub fn target_id(&self) -> &str {
        match self {
            BroadcastScope::Crew { crew_id } => crew_id,
            BroadcastScope::Squad { squad_id } => squad_id,
            BroadcastScope::Agent { agent_id } => agent_id,
            BroadcastScope::Global => "",
        }
    }

    /// Short label suitable for metrics / logging.
    pub fn kind(&self) -> &'static str {
        match self {
            BroadcastScope::Crew { .. } => "crew",
            BroadcastScope::Squad { .. } => "squad",
            BroadcastScope::Agent { .. } => "agent",
            BroadcastScope::Global => "global",
        }
    }
}

/// Unified message type for LLM provider input and agent context.
/// Aligns with pi-ai's Message design.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "role", rename_all = "snake_case")]
pub enum Message {
    System {
        content: String,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    User {
        /// Content blocks: text plus optional multimodal media. Accepts the
        /// legacy plain-string serialization for backward compatibility.
        #[serde(default, deserialize_with = "deserialize_user_content")]
        content: Vec<ContentBlock>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    Assistant {
        /// Content blocks: text, thinking, tool calls, images.
        /// Aligns with pi-ai's AssistantMessage.content.
        content: Vec<ContentBlock>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<ToolCall>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        usage: Option<TokenUsage>,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
    ToolResult {
        tool_call_id: String,
        tool_name: String,
        /// Content blocks: text and images.
        content: Vec<ContentBlock>,
        #[serde(default)]
        is_error: bool,
        #[serde(default = "Utc::now")]
        timestamp: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TokenUsage {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_read_tokens: u32,
    pub cache_write_tokens: u32,
    pub total_tokens: u32,
    /// Cost in USD, computed from model pricing.
    pub cost: Cost,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Cost {
    pub input: f64,
    pub output: f64,
    pub cache_read: f64,
    pub cache_write: f64,
    pub total: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: serde_json::Value, // JSON Schema
}

impl Message {
    pub fn system(content: impl Into<String>) -> Self {
        Message::System {
            content: content.into(),
            timestamp: Utc::now(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Message::User {
            content: vec![ContentBlock::text(content)],
            timestamp: Utc::now(),
        }
    }

    /// Build a user message from explicit content blocks (text + media).
    pub fn user_blocks(content: Vec<ContentBlock>) -> Self {
        Message::User {
            content,
            timestamp: Utc::now(),
        }
    }

    pub fn assistant(content: Vec<ContentBlock>) -> Self {
        Message::Assistant {
            content,
            tool_calls: None,
            usage: None,
            timestamp: Utc::now(),
        }
    }

    pub fn assistant_text(content: impl Into<String>) -> Self {
        Message::Assistant {
            content: vec![ContentBlock::text(content)],
            tool_calls: None,
            usage: None,
            timestamp: Utc::now(),
        }
    }

    pub fn tool_result(
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        content: Vec<ContentBlock>,
    ) -> Self {
        Message::ToolResult {
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            content,
            is_error: false,
            timestamp: Utc::now(),
        }
    }

    pub fn tool_result_text(
        tool_call_id: impl Into<String>,
        tool_name: impl Into<String>,
        text: impl Into<String>,
    ) -> Self {
        Message::ToolResult {
            tool_call_id: tool_call_id.into(),
            tool_name: tool_name.into(),
            content: vec![ContentBlock::text(text)],
            is_error: false,
            timestamp: Utc::now(),
        }
    }

    pub fn content(&self) -> String {
        match self {
            Message::System { content, .. } => content.clone(),
            Message::User { content, .. }
            | Message::Assistant { content, .. }
            | Message::ToolResult { content, .. } => content
                .iter()
                .filter_map(|b| b.as_text())
                .collect::<Vec<_>>()
                .join(""),
        }
    }

    pub fn role(&self) -> &'static str {
        match self {
            Message::System { .. } => "system",
            Message::User { .. } => "user",
            Message::Assistant { .. } => "assistant",
            Message::ToolResult { .. } => "tool",
        }
    }

    /// Extract all tool calls from an Assistant message.
    pub fn tool_calls(&self) -> Vec<ToolCall> {
        match self {
            Message::Assistant { content, .. } => content
                .iter()
                .filter_map(|b| match b {
                    ContentBlock::ToolCall {
                        id,
                        name,
                        arguments,
                        ..
                    } => Some(ToolCall {
                        id: id.clone(),
                        name: name.clone(),
                        arguments: arguments.clone(),
                    }),
                    _ => None,
                })
                .collect(),
            _ => vec![],
        }
    }

    /// Get content blocks for Assistant or ToolResult messages.
    pub fn content_blocks(&self) -> Option<&Vec<ContentBlock>> {
        match self {
            Message::Assistant { content, .. } | Message::ToolResult { content, .. } => {
                Some(content)
            }
            _ => None,
        }
    }

    /// Append a text delta to the last content block if it's Text,
    /// otherwise push a new Text block.
    pub fn append_text_delta(&mut self, delta: &str) {
        match self {
            Message::Assistant { content, .. } | Message::ToolResult { content, .. } => {
                if let Some(last) = content.last_mut() {
                    if last.is_text() {
                        last.append_text(delta);
                        return;
                    }
                }
                content.push(ContentBlock::text(delta));
            }
            _ => {}
        }
    }
}

/// Accept either the new block array or a legacy plain string for a user
/// message's `content`, so persisted messages from before multimodal blocks
/// keep deserializing (a string becomes a single text block).
fn deserialize_user_content<'de, D>(deserializer: D) -> Result<Vec<ContentBlock>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum UserContentRepr {
        Blocks(Vec<ContentBlock>),
        Text(String),
    }
    match UserContentRepr::deserialize(deserializer)? {
        UserContentRepr::Blocks(blocks) => Ok(blocks),
        UserContentRepr::Text(text) => Ok(vec![ContentBlock::text(text)]),
    }
}
