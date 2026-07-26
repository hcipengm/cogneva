use serde::{Deserialize, Serialize};

/// Content block for assistant and tool result messages.
/// Aligns with pi-ai's AssistantMessage.content: (TextContent | ThinkingContent | ToolCall | ImageContent)[].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
        /// Optional text signature (e.g., OpenAI responses message metadata)
        #[serde(skip_serializing_if = "Option::is_none")]
        text_signature: Option<String>,
    },
    Thinking {
        thinking: String,
        /// Opaque signature for replaying thinking context (e.g., Anthropic signature)
        #[serde(skip_serializing_if = "Option::is_none")]
        thinking_signature: Option<String>,
        /// When true, the thinking content was redacted by safety filters.
        /// The opaque encrypted payload is stored in thinking_signature for multi-turn continuity.
        #[serde(default)]
        redacted: bool,
    },
    ToolCall {
        id: String,
        name: String,
        arguments: serde_json::Value,
        /// Google-specific: opaque signature for reusing thought context
        #[serde(skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
    },
    Image {
        /// base64 encoded image data
        data: String,
        /// e.g., "image/jpeg", "image/png"
        mime_type: String,
    },
}

impl ContentBlock {
    pub fn text(text: impl Into<String>) -> Self {
        ContentBlock::Text {
            text: text.into(),
            text_signature: None,
        }
    }

    pub fn thinking(thinking: impl Into<String>) -> Self {
        ContentBlock::Thinking {
            thinking: thinking.into(),
            thinking_signature: None,
            redacted: false,
        }
    }

    pub fn tool_call(
        id: impl Into<String>,
        name: impl Into<String>,
        arguments: serde_json::Value,
    ) -> Self {
        ContentBlock::ToolCall {
            id: id.into(),
            name: name.into(),
            arguments,
            thought_signature: None,
        }
    }

    pub fn image(data: impl Into<String>, mime_type: impl Into<String>) -> Self {
        ContentBlock::Image {
            data: data.into(),
            mime_type: mime_type.into(),
        }
    }

    pub fn is_text(&self) -> bool {
        matches!(self, ContentBlock::Text { .. })
    }

    pub fn is_thinking(&self) -> bool {
        matches!(self, ContentBlock::Thinking { .. })
    }

    pub fn is_tool_call(&self) -> bool {
        matches!(self, ContentBlock::ToolCall { .. })
    }

    pub fn is_image(&self) -> bool {
        matches!(self, ContentBlock::Image { .. })
    }

    /// Returns the text content if this is a Text block.
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ContentBlock::Text { text, .. } => Some(text),
            _ => None,
        }
    }

    /// Returns the thinking content if this is a Thinking block.
    pub fn as_thinking(&self) -> Option<&str> {
        match self {
            ContentBlock::Thinking { thinking, .. } => Some(thinking),
            _ => None,
        }
    }

    /// Returns the tool call if this is a ToolCall block.
    pub fn as_tool_call(&self) -> Option<(&str, &str, &serde_json::Value)> {
        match self {
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
                ..
            } => Some((id, name, arguments)),
            _ => None,
        }
    }

    /// Append text to a Text block. Panics if not a Text block.
    pub fn append_text(&mut self, delta: &str) {
        match self {
            ContentBlock::Text { text, .. } => text.push_str(delta),
            _ => panic!("Cannot append_text to non-Text content block"),
        }
    }

    /// Append thinking to a Thinking block. Panics if not a Thinking block.
    pub fn append_thinking(&mut self, delta: &str) {
        match self {
            ContentBlock::Thinking { thinking, .. } => thinking.push_str(delta),
            _ => panic!("Cannot append_thinking to non-Thinking content block"),
        }
    }
}
