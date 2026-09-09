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
            // 保留 system message 与首条 user（任务输入）：丢掉首条 user 后，
            // 后续轮次会变成 assistant/tool 起头的无目标对话，模型答非所问。
            // 也不拆掉 assistant(tool_calls) 与其紧随的 tool result 配对。
            let mut first_user_seen = false;
            let remove_idx = self.messages.iter().position(|m| match m {
                Message::System { .. } => false,
                Message::User { .. } => {
                    if first_user_seen {
                        true
                    } else {
                        first_user_seen = true;
                        false
                    }
                }
                _ => true,
            });

            let Some(mut idx) = remove_idx else { break };
            if let Message::Assistant { .. } = self.messages[idx] {
                if self
                    .messages
                    .get(idx + 1)
                    .is_some_and(|m| matches!(m, Message::ToolResult { .. }))
                {
                    // 改删它后面的 tool result；扫描下一个可删位置
                    let mut candidate = idx + 1;
                    loop {
                        let removable = self.messages.get(candidate).is_some_and(|m| {
                            !matches!(m, Message::System { .. }) && {
                                let is_first_user = matches!(m, Message::User { .. })
                                    && self.messages[..candidate]
                                        .iter()
                                        .all(|x| !matches!(x, Message::User { .. }));
                                !is_first_user
                            }
                        });
                        if removable {
                            idx = candidate;
                            break;
                        }
                        if self.messages.get(candidate).is_none() {
                            break;
                        }
                        candidate += 1;
                    }
                    if self.messages.get(idx).is_none() {
                        break;
                    }
                }
            }
            let removed = self.messages.remove(idx);
            self.current_tokens = self
                .current_tokens
                .saturating_sub(estimate_tokens(&removed.content()));
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

    #[test]
    fn trim_never_evicts_first_user_message() {
        let mut ctx = ContextWindow::new(60);
        let big = "任务 ".repeat(60);
        ctx.add_message(Message::user(big.clone()));
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_1",
            "run_command",
            serde_json::json!({"command": "ls"}),
        )]));
        ctx.add_message(Message::tool_result_text("call_1", "run_command", "done"));

        assert!(
            ctx.messages().iter().any(|m| m.content() == big),
            "the original task input must survive trimming"
        );
        assert!(
            !matches!(
                ctx.messages().first(),
                Some(Message::Assistant { .. }) | Some(Message::ToolResult { .. })
            ),
            "window must not start with an assistant/tool message after trimming"
        );
    }

    #[test]
    fn trim_does_not_split_tool_call_from_its_result() {
        let mut ctx = ContextWindow::new(30);
        ctx.add_message(Message::user("task input task input task input"));
        ctx.add_message(Message::user("filler filler filler filler filler filler"));
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_9",
            "run_command",
            serde_json::json!({"command": "ls"}),
        )]));
        ctx.add_message(Message::tool_result_text("call_9", "run_command", "ok"));

        let msgs = ctx.messages();
        if let Some(pos) = msgs
            .iter()
            .position(|m| m.tool_calls().iter().any(|c| c.id == "call_9"))
        {
            assert!(
                matches!(msgs.get(pos + 1), Some(Message::ToolResult { tool_call_id, .. }) if tool_call_id == "call_9"),
                "assistant tool_call must stay paired with its result"
            );
        }
        assert!(
            !matches!(msgs.first(), Some(Message::ToolResult { .. })),
            "head must never be an orphaned tool result"
        );
    }
}
