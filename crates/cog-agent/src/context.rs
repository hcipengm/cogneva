use cog_core::Message;

/// Agent 上下文窗口管理。
/// 参考 pi-agent-core 的 transcript 设计，但用 Rust Vec 替代 JS 数组。
#[derive(Clone)]
pub struct ContextWindow {
    messages: Vec<Message>,
    max_tokens: usize,
    current_tokens: usize,
}

impl ContextWindow {
    pub fn new(max_tokens: usize) -> Self {
        Self {
            messages: Vec::new(),
            max_tokens,
            current_tokens: 0,
        }
    }

    pub fn add_message(&mut self, message: Message) {
        let tokens = estimate_tokens(&message.content());
        self.current_tokens += tokens;
        self.messages.push(message);
        self.trim_if_needed();
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    pub fn clear(&mut self) {
        self.messages.clear();
        self.current_tokens = 0;
    }

    /// Replace all messages and recalculate token count.
    /// Used by snapshot restore to reconstruct context state.
    pub fn restore_messages(&mut self, messages: Vec<Message>) {
        self.messages.clear();
        self.current_tokens = 0;
        for msg in messages {
            let tokens = estimate_tokens(&msg.content());
            self.current_tokens += tokens;
            self.messages.push(msg);
        }
        self.trim_if_needed();
    }

    pub fn to_prompt(&self) -> String {
        self.messages
            .iter()
            .map(|m| format!("[{}] {}", m.role(), m.content()))
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn token_count(&self) -> usize {
        self.current_tokens
    }

    fn trim_if_needed(&mut self) {
        while self.current_tokens > self.max_tokens && self.messages.len() > 2 {
            // 保留 system message，从 oldest non-system 开始删除
            let remove_idx = self
                .messages
                .iter()
                .position(|m| !matches!(m, Message::System { .. }));

            if let Some(idx) = remove_idx {
                let removed = self.messages.remove(idx);
                self.current_tokens -= estimate_tokens(&removed.content());
            } else {
                break;
            }
        }
        // 裁剪可能把 assistant 删掉却留下它的 tool result；严格校验的供应商
        // （Kimi/OpenAI）会拒绝找不到对应 tool_calls 声明的 tool 消息。
        // 把失去上下文的头部 tool result 一并摘掉。
        while self.messages.len() > 1 {
            let orphan = self
                .messages
                .iter()
                .position(|m| !matches!(m, Message::System { .. }))
                .is_some_and(|idx| matches!(self.messages[idx], Message::ToolResult { .. }));
            if !orphan {
                break;
            }
            let idx = self
                .messages
                .iter()
                .position(|m| !matches!(m, Message::System { .. }))
                .unwrap();
            let removed = self.messages.remove(idx);
            self.current_tokens = self
                .current_tokens
                .saturating_sub(estimate_tokens(&removed.content()));
        }
    }
}

/// 简化的 token 估算。
/// CJK 字符每个算 2 token，英文单词算 4 token。
pub fn estimate_tokens(text: &str) -> usize {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }

    let mut tokens = 0;
    for part in trimmed.split_whitespace() {
        // CJK 字符检测
        let has_cjk = part.chars().any(|c| {
            ('\u{4e00}'..='\u{9fff}').contains(&c)
                || ('\u{3000}'..='\u{303f}').contains(&c)
                || ('\u{ff00}'..='\u{ffef}').contains(&c)
        });

        if has_cjk {
            tokens += part.len() * 2; // 每个字节约 2 token（保守估计）
        } else {
            tokens += 4; // 英文单词约 4 token
        }
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_never_leaves_orphan_tool_result_at_head() {
        let mut ctx = ContextWindow::new(50);
        ctx.add_message(Message::system("sys"));
        ctx.add_message(Message::user(
            "用户消息占额度 用户消息占额度 用户消息占额度 用户消息占额度",
        ));
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_1",
            "http_request",
            serde_json::json!({"url": "https://example.com"}),
        )]));
        ctx.add_message(Message::tool_result_text("call_1", "http_request", "ok"));

        let msgs = ctx.messages();
        let first_non_system = msgs.iter().find(|m| !matches!(m, Message::System { .. }));
        if let Some(m) = first_non_system {
            assert!(
                !matches!(m, Message::ToolResult { .. }),
                "oldest non-system message must never be an orphaned tool result"
            );
        }
    }
}
