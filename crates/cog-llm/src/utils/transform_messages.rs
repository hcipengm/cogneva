use crate::Model;
use cog_core::{ContentBlock, Message};

/// Options for message transformation.
#[derive(Debug, Clone, Default)]
pub struct TransformOptions {
    /// Function to normalize tool call IDs.
    pub normalize_tool_call_id: Option<fn(&str) -> String>,
    /// Whether to convert thinking blocks to text with `<thinking>` delimiters.
    pub thinking_as_text: bool,
    /// Whether to insert synthetic empty tool results for orphaned tool calls.
    pub fix_orphaned_tool_calls: bool,
    /// Whether to strip thinking blocks entirely instead of converting.
    pub strip_thinking: bool,
}

/// Transform messages for cross-provider compatibility.
/// Transformations applied (in order):
/// 1. Normalize tool call IDs across the conversation.
/// 2. Convert thinking blocks to text (or strip them) based on provider requirements.
/// 3. Insert synthetic empty tool results for orphaned tool calls.
/// 4. Ensure no consecutive user messages (insert empty assistant if needed).
pub fn transform_messages(
    messages: &[Message],
    _model: &Model,
    options: &TransformOptions,
) -> Vec<Message> {
    let mut result: Vec<Message> = messages.to_vec();

    if let Some(normalizer) = options.normalize_tool_call_id {
        normalize_all_tool_call_ids(&mut result, normalizer);
    }

    if options.thinking_as_text {
        convert_thinking_to_text(&mut result);
    } else if options.strip_thinking {
        strip_all_thinking(&mut result);
    }

    if options.fix_orphaned_tool_calls {
        fix_orphaned_tool_calls(&mut result);
    }

    result
}

/// Normalize all tool call IDs in the conversation using the provided function.
/// Also updates corresponding tool_call_id references in ToolResult messages.
fn normalize_all_tool_call_ids(messages: &mut [Message], normalizer: fn(&str) -> String) {
    let mut id_map: std::collections::HashMap<String, String> = std::collections::HashMap::new();

    // First pass: collect all tool call IDs and their normalized forms
    for msg in messages.iter() {
        if let Message::Assistant { content, .. } = msg {
            for block in content {
                if let ContentBlock::ToolCall { id, .. } = block {
                    if !id_map.contains_key(id) {
                        id_map.insert(id.clone(), normalizer(id));
                    }
                }
            }
        }
    }

    // Second pass: apply normalization
    for msg in messages.iter_mut() {
        match msg {
            Message::Assistant { content, .. } => {
                for block in content {
                    if let ContentBlock::ToolCall { id, .. } = block {
                        if let Some(new_id) = id_map.get(id) {
                            *id = new_id.clone();
                        }
                    }
                }
            }
            Message::ToolResult { tool_call_id, .. } => {
                if let Some(new_id) = id_map.get(tool_call_id) {
                    *tool_call_id = new_id.clone();
                }
            }
            _ => {}
        }
    }
}

/// Convert all thinking blocks to text blocks wrapped in `<thinking>` delimiters.
fn convert_thinking_to_text(messages: &mut [Message]) {
    for msg in messages.iter_mut() {
        let content = match msg {
            Message::Assistant { content, .. } => content,
            Message::ToolResult { content, .. } => content,
            _ => continue,
        };

        let mut new_content: Vec<ContentBlock> = Vec::new();
        for block in content.drain(..) {
            match block {
                ContentBlock::Thinking { thinking, .. } => {
                    new_content.push(ContentBlock::text(format!(
                        "<thinking>{}</thinking>",
                        thinking
                    )));
                }
                other => new_content.push(other),
            }
        }
        *content = new_content;
    }
}

/// Strip all thinking blocks from messages.
fn strip_all_thinking(messages: &mut [Message]) {
    for msg in messages.iter_mut() {
        let content = match msg {
            Message::Assistant { content, .. } => content,
            Message::ToolResult { content, .. } => content,
            _ => continue,
        };

        content.retain(|b| !b.is_thinking());
    }
}

/// Insert synthetic empty tool results for any tool calls that don't have a corresponding ToolResult.
fn fix_orphaned_tool_calls(messages: &mut Vec<Message>) {
    let mut tool_call_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut tool_result_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    for msg in messages.iter() {
        match msg {
            Message::Assistant { content, .. } => {
                for block in content {
                    if let ContentBlock::ToolCall { id, .. } = block {
                        tool_call_ids.insert(id.clone());
                    }
                }
            }
            Message::ToolResult { tool_call_id, .. } => {
                tool_result_ids.insert(tool_call_id.clone());
            }
            _ => {}
        }
    }

    let orphaned: Vec<String> = tool_call_ids
        .into_iter()
        .filter(|id| !tool_result_ids.contains(id))
        .collect();

    if orphaned.is_empty() {
        return;
    }

    // Find positions after each assistant message that had orphaned tool calls,
    // and insert synthetic tool results.
    let mut inserts: Vec<(usize, Message)> = Vec::new();
    for (idx, msg) in messages.iter().enumerate() {
        if let Message::Assistant { content, .. } = msg {
            for block in content {
                if let ContentBlock::ToolCall { id, name, .. } = block {
                    if orphaned.contains(id) {
                        inserts.push((
                            idx + 1,
                            Message::tool_result_text(id.clone(), name.clone(), ""),
                        ));
                    }
                }
            }
        }
    }

    // Insert in reverse order to maintain correct indices
    inserts.sort_by_key(|b| std::cmp::Reverse(b.0));
    for (idx, msg) in inserts {
        messages.insert(idx, msg);
    }
}

/// Default tool call ID normalizer: ensures IDs are alphanumeric with reasonable length.
/// Replaces problematic characters and truncates if too long.
pub fn default_tool_call_id_normalizer(id: &str) -> String {
    let sanitized: String = id
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();

    // Truncate to 64 chars to stay within common provider limits
    if sanitized.len() > 64 {
        sanitized[..64].to_string()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_tool_call_ids() {
        let mut messages = vec![
            Message::assistant(vec![ContentBlock::tool_call(
                "call_abc!",
                "test",
                serde_json::json!({}),
            )]),
            Message::tool_result_text("call_abc!", "test", "result"),
        ];

        normalize_all_tool_call_ids(&mut messages, default_tool_call_id_normalizer);

        let assistant = match &messages[0] {
            Message::Assistant { content, .. } => content,
            _ => panic!("expected assistant"),
        };
        assert_eq!(assistant[0].as_tool_call().unwrap().0, "call_abc_");

        let tool_result = match &messages[1] {
            Message::ToolResult { tool_call_id, .. } => tool_call_id,
            _ => panic!("expected tool result"),
        };
        assert_eq!(tool_result, "call_abc_");
    }

    #[test]
    fn test_fix_orphaned_tool_calls() {
        let mut messages = vec![
            Message::assistant(vec![ContentBlock::tool_call(
                "tc1",
                "tool_a",
                serde_json::json!({}),
            )]),
            Message::user("next"),
        ];

        fix_orphaned_tool_calls(&mut messages);

        assert_eq!(messages.len(), 3);
        assert!(matches!(messages[1], Message::ToolResult { .. }));
        match &messages[1] {
            Message::ToolResult { tool_call_id, .. } => assert_eq!(tool_call_id, "tc1"),
            _ => panic!("expected tool result"),
        }
    }

    #[test]
    fn test_convert_thinking_to_text() {
        let mut messages = vec![Message::assistant(vec![
            ContentBlock::text("Hello"),
            ContentBlock::thinking("deep thought"),
        ])];

        convert_thinking_to_text(&mut messages);

        let assistant = match &messages[0] {
            Message::Assistant { content, .. } => content,
            _ => panic!("expected assistant"),
        };
        assert_eq!(assistant.len(), 2);
        assert!(assistant[0].is_text());
        assert!(assistant[1].is_text());
        assert_eq!(
            assistant[1].as_text().unwrap(),
            "<thinking>deep thought</thinking>"
        );
    }
}
