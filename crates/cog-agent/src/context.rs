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
            // 删除单位是「轮次组」：assistant(tool_calls) 与其紧随的 tool result
            // 必须同进同出——只删 result 会留下声明了 tool_calls 却无响应的
            // assistant，严格校验的供应商（Kimi/OpenAI）直接 400 拒绝整条请求，
            // 网关侧还会把这条 400 误记成上游故障。
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

            let Some(idx) = remove_idx else { break };
            let mut end = idx + 1;
            if matches!(self.messages[idx], Message::Assistant { .. }) {
                while self
                    .messages
                    .get(end)
                    .is_some_and(|m| matches!(m, Message::ToolResult { .. }))
                {
                    end += 1;
                }
            }
            let mut freed = 0usize;
            for m in self.messages.drain(idx..end) {
                freed += estimate_tokens(&m.content());
            }
            self.current_tokens = self.current_tokens.saturating_sub(freed);
        }
        // 裁剪可能把 assistant 删掉却留下它的 tool result；严格校验的供应商
        // （Kimi/OpenAI）会拒绝找不到对应 tool_calls 声明的 tool 消息。孤儿
        // 可能出现在头部之外（预算在 len<=2 时停止裁剪），所以扫整条链。
        // 反过来不剥「尚未应答」的 tool_call：结果是下一轮才追加的，这里剥离
        // 会把正常进行中的调用也删掉——那一步只在协议边缘做。
        let (repaired, dropped) = cog_core::drop_orphan_tool_results(&self.messages);
        if dropped > 0 {
            self.messages = repaired;
            self.current_tokens = self
                .messages
                .iter()
                .map(|m| estimate_tokens(&m.content()))
                .sum();
        }
    }
}

/// 简化的 token 估算。
/// CJK 字符每个算 2 token（保守）；其余按字符数 /4 粗估（英文约 4 字符
/// 1 token）。CJK 必须按字符计而非字节：UTF-8 一个汉字 3 字节，按字节
/// 会把中文上下文高估 3 倍，窗口提前触发裁剪。
pub fn estimate_tokens(text: &str) -> usize {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0;
    }

    let mut tokens = 0;
    for part in trimmed.split_whitespace() {
        // CJK 字符检测
        let is_cjk = |c: &char| {
            ('\u{4e00}'..='\u{9fff}').contains(c)
                || ('\u{3000}'..='\u{303f}').contains(c)
                || ('\u{ff00}'..='\u{ffef}').contains(c)
        };
        let cjk_chars = part.chars().filter(is_cjk).count();
        let other_chars = part.chars().count() - cjk_chars;
        if cjk_chars > 0 {
            tokens += cjk_chars * 2 + other_chars.div_ceil(4);
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

    /// 生产事故回归（2026-09-15）：planner 的 run_command 输出顶爆预算时，
    /// 旧裁剪只删 tool result、留下声明了 tool_calls 的 assistant，Kimi/ark
    /// 对这条孤儿链直接 400，网关再把 400 误记为上游故障导致全池熔断。
    /// 裁剪必须以「assistant + 其全部 tool result」为最小删除单位。
    #[test]
    fn trim_over_budget_never_leaves_assistant_tool_calls_unanswered() {
        let mut ctx = ContextWindow::new(120);
        ctx.add_message(Message::user("原始任务输入 原始任务输入 原始任务输入"));
        ctx.add_message(Message::assistant(vec![
            cog_core::ContentBlock::tool_call(
                "run_command:0",
                "run_command",
                serde_json::json!({"command": "cargo build"}),
            ),
            cog_core::ContentBlock::tool_call(
                "run_command:1",
                "run_command",
                serde_json::json!({"command": "cargo test"}),
            ),
        ]));
        // 两条大输出，逐条 add 时各自触发 trim
        let big_a = "构建日志 输出很多 ".repeat(40);
        let big_b = "测试日志 输出很多 ".repeat(40);
        ctx.add_message(Message::tool_result_text(
            "run_command:0",
            "run_command",
            &big_a,
        ));
        ctx.add_message(Message::tool_result_text(
            "run_command:1",
            "run_command",
            &big_b,
        ));

        let msgs = ctx.messages();
        for (i, m) in msgs.iter().enumerate() {
            // 每个被保留的 assistant tool_call 都必须在紧随其后的
            // ToolResult 里有响应
            for call in m.tool_calls() {
                let answered = msgs[i + 1..]
                    .iter()
                    .take_while(|n| matches!(n, Message::ToolResult { .. }))
                    .any(|n| matches!(n, Message::ToolResult { tool_call_id, .. } if *tool_call_id == call.id));
                assert!(
                    answered,
                    "assistant tool_call {} survived trim without its result: {:?}",
                    call.id,
                    msgs.iter().map(|m| m.role()).collect::<Vec<_>>()
                );
            }
            // 每个被保留的 ToolResult 都必须有在前 assistant 声明过
            if let Message::ToolResult { tool_call_id, .. } = m {
                let declared = msgs[..i]
                    .iter()
                    .any(|p| p.tool_calls().iter().any(|c| &c.id == tool_call_id));
                assert!(
                    declared,
                    "tool result {tool_call_id} has no declaring assistant"
                );
            }
        }
    }
}
