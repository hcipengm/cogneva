use cog_core::Message;

/// Agent 上下文窗口管理。
/// 参考 pi-agent-core 的 transcript 设计，但用 Rust Vec 替代 JS 数组。
///
/// 窗口有两个面。`messages` 是模型看到的那份，按预算裁剪；`history` 是本窗口
/// 见过的全部消息，只增不减。裁剪是长对话的正常行为，但被裁掉的轮次原先在
/// 系统里再没有第二份：快照存的是 `messages`，`AgentEnd` 发的也是 `messages`，
/// 于是「模型这一轮没看到」等于「这段对话没有发生过」。两面分开之后，模型看
/// 多少仍由预算决定，系统留多少不由预算决定——记全，投影按预算来。
#[derive(Clone)]
pub struct ContextWindow {
    messages: Vec<Message>,
    /// 与 `messages` 同一套插入位置，但**从不**因为预算被裁。
    history: Vec<Message>,
    max_tokens: usize,
    current_tokens: usize,
}

impl ContextWindow {
    pub fn new(max_tokens: usize) -> Self {
        Self {
            messages: Vec::new(),
            history: Vec::new(),
            max_tokens,
            current_tokens: 0,
        }
    }

    pub fn add_message(&mut self, message: Message) {
        // 工具结果是上下文里唯一由进程外决定大小的内容：一条超大结果可以
        // 独自超过整个窗口，而裁剪只能整条丢弃——它恰恰是最新的那条，丢不掉，
        // 于是窗口预算失效，之后每一轮都带着它，同一份字节还会被当作记忆
        // 原文再发给抽取器两次。边界卡在「进入上下文」这一步，且只作用于
        // 工具结果：user 是任务输入、assistant 是模型自己的话，都不该在这里被改写。
        //
        // 历史记的是**过界之前**的这条：下面这道界是通道上唯一一处不留引用
        // 就丢字节的地方（工具返回点那条会先归档再裁，模型手里还留着引用），
        // 记在前面，被这道界裁掉的字节就还在历史里。
        self.history.push(message.clone());
        let message = bound_tool_result(message, self.max_tokens);
        let tokens = estimate_tokens(&message.content());
        self.current_tokens += tokens;
        self.messages.push(message);
        self.trim_if_needed();
    }

    /// Put a message at the very front.
    ///
    /// The stable half of the prompt (the answer contract) has to sit where the
    /// model reads first, and only the head of the conversation is that place.
    /// Appending it instead makes the same text appear again in the middle of the
    /// conversation — paid for twice — and moves it out of the cacheable prefix.
    pub fn prepend_message(&mut self, message: Message) {
        // 历史跟着投影走位置：读者拿到的那份与模型读过的那份开头相同。
        self.history.insert(0, message.clone());
        let message = bound_tool_result(message, self.max_tokens);
        self.current_tokens += estimate_tokens(&message.content());
        self.messages.insert(0, message);
        self.trim_if_needed();
    }

    /// Whether the context begins with exactly this system message.
    ///
    /// One agent is reused by the same role across turns (one turn per attempt),
    /// so a contract inserted unconditionally becomes a second copy in the middle
    /// of the conversation. "Already there" is read from the message itself, not
    /// from a counter that could drift out of step with it.
    pub fn starts_with_system(&self, content: &str) -> bool {
        matches!(
            self.messages.first(),
            Some(Message::System { content: head, .. }) if head == content
        )
    }

    pub fn messages(&self) -> &[Message] {
        &self.messages
    }

    /// 本窗口见过的全部消息，包含被预算裁掉、因而模型这一轮没看到的那部分。
    ///
    /// 这一份是耐久面：它落进 raw 归档，抽取器读的也是它。裁剪后的
    /// [`Self::messages`] 只有两个读者——模型的请求与快照，都是「这一刻
    /// 的投影」，不是「这段对话是什么」。
    pub fn history(&self) -> &[Message] {
        &self.history
    }

    pub fn clear(&mut self) {
        self.messages.clear();
        self.history.clear();
        self.current_tokens = 0;
    }

    /// Replace all messages and recalculate token count.
    /// Used by snapshot restore to reconstruct context state.
    pub fn restore_messages(&mut self, messages: Vec<Message>) {
        self.messages.clear();
        self.history.clear();
        self.current_tokens = 0;
        for msg in messages {
            // 快照可能是旧版本写下的：进入上下文的这一步在恢复路径上也要重做，
            // 否则一份更早的、没有这条界的进程存下的快照会把巨块带回来。
            self.history.push(msg.clone());
            let msg = bound_tool_result(msg, self.max_tokens);
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

    /// The character budget one tool result may occupy in this window.
    ///
    /// The tool-return path uses it to decide whether a result is oversized
    /// before any message is built; the bound applied on `add_message` uses the
    /// same number, so a result that fits here will not be cut there.
    pub fn tool_result_budget_chars(&self) -> usize {
        tool_result_budget_chars(self.max_tokens)
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
            let mut dropped = 0usize;
            for m in self.messages.drain(idx..end) {
                freed += estimate_tokens(&m.content());
                dropped += 1;
            }
            self.current_tokens = self.current_tokens.saturating_sub(freed);
            // 裁剪只作用在投影上，所以这条报的是「模型这一轮少看到多少」，
            // 而不是「丢了什么」——被裁的轮次仍在 history 里，落进 raw 归档。
            // 不另铸指标：压力读数由耐久面自己给（transcript 的估算长度对窗口
            // 配置），铸一个只在这里 +1 的计数器，等于把同一个量记两遍。
            tracing::debug!(
                "Context window dropped {} messages (~{} tokens) from the prompt; the window budget is {}, the run history keeps {} messages",
                dropped,
                freed,
                self.max_tokens,
                self.history.len(),
            );
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
        // 裁到底仍然超预算：这一轮就带着超额内容出站了。循环只剩 `len() <= 2`
        // 一条出路，也就是剩下的全是地板（system 与首条 user，见上面那段 why），
        // 没有可裁的东西了。
        //
        // 为什么单列这一行：外面能读到的那个数是**上游回报的真值**
        // （账上的 `tokens_input`），而这里裁的是**估算**——两者不同尺，且估算
        // 刻意是下界。缺这一行时，「估算低估了多少」与「窗口真的带着超额内容
        // 出站」在读数上同形，而两种情况的处置相反：前者调估算，后者调预算或
        // 地板。上面那条逐次裁剪留在 debug（常态，按窗口预算反复发生），
        // 这一条是**例外**，所以报在被采集的那一级。
        if self.current_tokens > self.max_tokens {
            tracing::warn!(
                budget = self.max_tokens,
                estimated = self.current_tokens,
                excess = self.current_tokens - self.max_tokens,
                kept_messages = self.messages.len(),
                "the context window goes out over budget: the floor of system plus the first user message does not fit, so trimming ran out of messages to drop"
            );
        }
    }
}

/// 单条工具结果允许占用的窗口份额的分母：窗口的一半。
///
/// 留一半给任务输入、system 与后续轮次，超出的部分不是"内容多"而是
/// 这条结果根本挤不进这段对话。与窗口同源而不是另立一个绝对值：
/// 窗口配大了，允许的单条结果跟着变大，两者不会各自漂移。
const TOOL_RESULT_WINDOW_SHARE_DIVISOR: usize = 2;

/// 估算口径里一个 token 折算的字符数，与 `estimate_tokens` 的英文分支同源。
const CHARS_PER_ESTIMATED_TOKEN: usize = 4;

/// The number of characters one tool result may occupy before it is cut.
///
/// The tool-return path reads this to decide whether a result needs archiving,
/// and the same number bounds it on the way into the context. One function so
/// the two cannot disagree about where the boundary is: a result the return path
/// judged to fit and the window then cut would be truncated twice, and the second
/// cut would take the truncation marker with it.
pub fn tool_result_budget_chars(max_tokens: usize) -> usize {
    (max_tokens / TOOL_RESULT_WINDOW_SHARE_DIVISOR)
        .max(1)
        .saturating_mul(CHARS_PER_ESTIMATED_TOKEN)
}

/// 把超出窗口份额的单条工具结果截断到预算内，其余消息原样返回。
///
/// 超出的部分必须在这里切掉：裁剪只能整条丢弃，而刚回来的这条工具结果恰恰是
/// 最新的一条、丢不掉——它独自超过窗口，窗口预算就整个失效，之后每一轮都带着
/// 同一份字节。
///
/// 界数的是字符不是估算 token：这道界要在消息被记账**之前**生效，手里是原文，
/// 而工具返回点（决定要不要归档）与这里共用同一个 [`tool_result_budget_chars`]。
/// 两处不同源会让同一条结果按两个边界各裁一次，第二次连截断标记一起砍掉。
/// 折算用的是估算英文那一档（[`CHARS_PER_TOKEN`] 字符 1 token）。
///
/// 这里丢掉 [`Truncation::dropped_chars`] 而不发读数，是因为这条路径本就不该
/// 对工具结果生效：返回点用的是同一个预算，已经按它裁过一次并用带引用的标记
/// 补齐，这里再裁会连引用一起砍掉。它是一道兜底，服务的是别处构造进上下文、
/// 没经过返回点的工具结果；那条路径上也没有 metrics handle 可发。真在这里
/// 裁到工具结果，说明两处预算不再同源——那是 `tool_result_budget_chars` 的
/// 不变量破了，不是一条读数能报的。
fn bound_tool_result(message: Message, max_tokens: usize) -> Message {
    let Message::ToolResult {
        tool_call_id,
        tool_name,
        content,
        is_error,
        timestamp,
    } = &message
    else {
        return message;
    };
    let budget_chars = tool_result_budget_chars(max_tokens);
    let text: String = content.iter().filter_map(|b| b.as_text()).collect();
    if text.chars().count() <= budget_chars {
        return message;
    }
    Message::ToolResult {
        tool_call_id: tool_call_id.clone(),
        tool_name: tool_name.clone(),
        content: vec![cog_core::ContentBlock::text(
            truncate_to_chars(&text, budget_chars, None).text,
        )],
        is_error: *is_error,
        timestamp: *timestamp,
    }
}

/// 一次裁剪的结果：模型将看到的文本，以及原文里没被看到的字符数。
///
/// 两者同源。丢弃量就是「原文长度减去保留数」，而保留数是标记算法本身的
/// 产物（标记也算进预算）；在这里之外重算一遍就是第二份会漂的实现。返回点
/// 拿这个数去发字符量读数——"截了几条"之外的另一半，只有它说得出一条被砍的
/// 结果是丢了十个字还是十万个字。
pub struct Truncation {
    /// 模型将看到的部分：原文头部，尾部接上说明被丢了多少的标记。
    pub text: String,
    /// 原文里没有出现在 [`Self::text`] 中的字符数。没超预算时是 0。
    pub dropped_chars: usize,
}

/// 从头部保留到预算为止，并在尾部说明被丢掉了多少。
///
/// 静默的截断读起来像完整输出：读者必须能从文本本身看出"还有没看到的"，
/// 否则一份被砍过的日志会被当成跑完了的日志。标记里带上原长度，是为了
/// 让重跑命令时有据可依（收窄输出，而不是原样再来一次）。
///
/// `reference` 是这段输出归档后的 `artifact://` 引用，由工具返回点传进来
/// （只有那里同时知道全文和窗口预算）。带上它，模型看到的标记就是一条可取回
/// 的把手，而不是一句"有东西被丢了"。
///
/// 标记本身也算进预算：它比预算长时结果会溢出，进上下文时被第二次裁剪，
/// 而那一次会把标记连同里面的引用一起砍掉——归档了却没人拿得到引用，等于
/// 没归档。所以保留的字符数按「预算减去标记」算，标记里报的也就是这个数。
pub fn truncate_to_chars(text: &str, budget_chars: usize, reference: Option<&str>) -> Truncation {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() <= budget_chars {
        return Truncation {
            text: text.to_string(),
            dropped_chars: 0,
        };
    }
    let total = chars.len();
    let marker_for = |shown: usize| match reference {
        Some(uri) => format!(
            "\n[tool output truncated: showing {shown} of {total} characters; \
             full output archived as {uri}]"
        ),
        None => format!("\n[tool output truncated: showing {shown} of {total} characters]"),
    };
    // 两趟：先按「报满预算」估标记长度，再按实际的保留数报一次，末尾再收紧一格
    // 保证「保留字符数 + 标记长度」不超过预算（标记位数只会随保留数变小，不会
    // 反过来）。这样落在预算内的结果进上下文时不会再被裁剪。
    let keep = budget_chars
        .saturating_sub(marker_for(budget_chars).chars().count())
        .min(total);
    let keep = keep.min(budget_chars.saturating_sub(marker_for(keep).chars().count()));
    let kept: String = chars[..keep].iter().collect();
    Truncation {
        text: format!("{kept}{}", marker_for(keep)),
        // 标记不在原文里，所以「没被看到的」只算原文被砍掉的那一段：保留数
        // 是 `keep`，不是 `keep` 加上标记长度。
        dropped_chars: total - keep,
    }
}

/// 估算口径在整个 workspace 里只有一份（`cog_core::token_estimate`）：窗口
/// 按它决定裁不裁、抽取器按它决定一段 transcript 发多少，两处既然是拿同一个
/// 数互相比，就不能各有一份实现。这里保留这个名字，是为了让窗口这一侧的读法
/// 不变。
pub use cog_core::estimate_tokens;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::{Layer, SubscriberExt};

    /// 采集面会收到的那些字段，按值取出来——断言「打过日志」是空的，
    /// 一个有字段名写错的读数照样会让它绿。
    #[derive(Default, Clone, Debug)]
    struct OverBudgetFields {
        message: Option<String>,
        budget: Option<u64>,
        estimated: Option<u64>,
        excess: Option<u64>,
    }

    impl tracing::field::Visit for OverBudgetFields {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = Some(format!("{value:?}").trim_matches('"').to_string());
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "budget" => self.budget = Some(value),
                "estimated" => self.estimated = Some(value),
                "excess" => self.excess = Some(value),
                _ => {}
            }
        }
    }

    struct CaptureOverBudget(Arc<Mutex<Vec<OverBudgetFields>>>);

    impl<S: tracing::Subscriber> Layer<S> for CaptureOverBudget {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut seen = OverBudgetFields::default();
            event.record(&mut seen);
            if seen
                .message
                .as_deref()
                .is_some_and(|m| m.starts_with("the context window goes out over budget"))
            {
                self.0.lock().unwrap().push(seen);
            }
        }
    }

    /// 裁到底仍超预算时，这一轮必须**被自己报出来**，且报的是能拿去对账的三个数。
    ///
    /// 这是账侧缺的那一半：账上的 `tokens_input` 是上游回报的真值，窗口裁的是估算
    /// 且刻意是下界 ⇒ 没有这一行时，「估算低了多少」与「真的带着超额内容出站」
    /// 在外面同形。所以这条只判「读数在不在采集面、三个数对不对」，判不了
    /// 「预算该不该抬」——那是另一条。
    #[test]
    fn a_window_that_cannot_trim_to_budget_reports_its_excess() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        // 装一层**按级别过滤**的订阅者：采集面装的就是 `COGNEVA_LOG_LEVEL`
        // 那一级（部署里是 info），所以断言必须经过同一道闸——不然把它写回
        // `debug!`（今天那行就是这么被挡在采集面之外的）这条用例照样绿。
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(CaptureOverBudget(captured.clone()));
        let observed = tracing::subscriber::with_default(subscriber, || {
            // 地板本身就超预算：两条消息，循环的 `len() > 2` 立刻为假。
            let mut ctx = ContextWindow::new(10);
            ctx.add_message(Message::system(&"系统提示词 很长 ".repeat(4)));
            ctx.add_message(Message::user(&"任务输入 很长 ".repeat(4)));
            assert!(
                ctx.token_count() > 10,
                "地板必须真的超预算，否则这条用例没有题目：{}",
                ctx.token_count()
            );
            ctx.token_count()
        });

        let seen = captured.lock().unwrap().clone();
        // 一次 `add_message` 报一条（每条消息加进来都各裁一次），所以条数对得上
        // 「结束在预算之外的那几次调用」；这里两条都对得上，没多报也没漏报。
        assert_eq!(seen.len(), 2, "每条落在预算外的入队各报一条：{seen:?}");
        for ev in &seen {
            assert_eq!(ev.budget, Some(10), "报的必须是**这次生效**的预算：{ev:?}");
            assert_eq!(
                ev.excess,
                ev.estimated.map(|e| e - 10),
                "超额量必须是估算减预算，不是另算一遍：{ev:?}"
            );
        }
        let last = seen.last().unwrap();
        assert_eq!(
            last.estimated,
            Some(observed as u64),
            "最后一条报的必须是**最终出站**那一份估算"
        );
    }

    /// 另一侧：裁得下来的窗口**不许**报超额——不然这一行会退化成「每次调用都报」，
    /// 而它唯一的价值就是指出例外。
    #[test]
    fn a_window_that_trims_to_budget_reports_no_excess() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        // 装一层**按级别过滤**的订阅者：采集面装的就是 `COGNEVA_LOG_LEVEL`
        // 那一级（部署里是 info），所以断言必须经过同一道闸——不然把它写回
        // `debug!`（今天那行就是这么被挡在采集面之外的）这条用例照样绿。
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(CaptureOverBudget(captured.clone()));
        let over = tracing::subscriber::with_default(subscriber, || {
            let mut ctx = ContextWindow::new(200);
            ctx.add_message(Message::system("系统提示词"));
            ctx.add_message(Message::user("任务输入"));
            ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
                "c1",
                "run_command",
                serde_json::json!({"command": "ls"}),
            )]));
            ctx.add_message(Message::tool_result_text(
                "c1",
                "run_command",
                &"输出 ".repeat(400),
            ));
            assert!(
                ctx.token_count() <= 200,
                "裁完之后必须在预算内，否则这条用例的另一侧不成立：{}",
                ctx.token_count()
            );
            ctx.token_count()
        });
        assert!(over <= 200);
        assert!(
            captured.lock().unwrap().is_empty(),
            "裁得下来的窗口报了超额：{:?}",
            captured.lock().unwrap()
        );
    }

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

    #[test]
    fn one_oversized_tool_result_cannot_outgrow_the_window() {
        let mut ctx = ContextWindow::new(400);
        ctx.add_message(Message::user("原始任务输入"));
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_bin",
            "run_command",
            serde_json::json!({"command": "cat /opt/cogneva/cogneva"}),
        )]));
        // 一条命令把二进制倾倒进上下文：由进程外决定大小，比整个窗口还大，而且
        // 整块没有空白。它是最新的一条，裁剪丢不掉它——所以要在进入上下文这一步
        // 按字符卡住份额。
        ctx.add_message(Message::tool_result_text(
            "call_bin",
            "run_command",
            "ELF\u{2}\u{1}\u{0}".repeat(2000),
        ));

        let kept = ctx
            .messages()
            .last()
            .expect("结果还在，只是被截短")
            .content();
        assert!(
            kept.contains("tool output truncated"),
            "被砍过的输出必须自己说自己被砍过，否则读起来像跑完了的日志"
        );
        assert!(
            kept.starts_with(&"ELF\u{2}\u{1}\u{0}".repeat(100)),
            "保留的是头部内容"
        );
        assert!(
            kept.chars().count() < 1000,
            "8000 字符的结果被压到窗口份额换算出的字符预算 400/2*4 = 800 附近: {}",
            kept.chars().count()
        );
    }

    #[test]
    fn a_tool_result_that_fits_is_untouched() {
        let mut ctx = ContextWindow::new(400);
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_ok",
            "run_command",
            serde_json::json!({"command": "cargo test"}),
        )]));
        let text = "编译通过，3 个测试用例全部通过";
        ctx.add_message(Message::tool_result_text("call_ok", "run_command", text));
        assert_eq!(ctx.messages().last().unwrap().content(), text);
    }

    #[test]
    fn the_task_input_is_never_truncated_by_the_tool_result_bound() {
        // 首条 user 是任务输入，由调用方决定内容；这里的界只针对外部输出。
        let mut ctx = ContextWindow::new(200);
        let input = "目标 ".repeat(200);
        ctx.add_message(Message::user(input.clone()));
        assert_eq!(ctx.messages()[0].content(), input);
    }

    #[test]
    fn a_truncated_result_reports_its_archived_reference_within_budget() {
        let text = "x".repeat(5000);
        let out = truncate_to_chars(&text, 400, Some("artifact://default/t-0001-0002"));
        assert!(
            out.text.contains("tool output truncated"),
            "被砍过要说出来: {}",
            out.text
        );
        assert!(
            out.text.contains("artifact://default/t-0001-0002"),
            "归档了就要给得回引用，否则砍掉的字节没人拿得到: {}",
            out.text
        );
        assert!(
            out.text.chars().count() <= 400,
            "标记本身也算预算，否则进窗口会被二次裁剪: {}",
            out.text.chars().count()
        );
        // 丢掉的字符数要和文本里的标记讲同一件事：标记左边就是保留的头部，
        // 两者差一个字符就说明读数与模型看到的不是同一个量。
        let kept = out
            .text
            .find("\n[tool output truncated: ")
            .expect("标记在尾部")
            .min(out.text.len());
        let kept = out.text[..kept].chars().count();
        assert_eq!(
            out.dropped_chars,
            text.chars().count() - kept,
            "丢弃量必须等于原文长度减去标记左边保留的头部"
        );
    }

    /// 被裁掉的轮次留在历史里：模型这一轮看不到它，系统不必因此失去它。
    /// 这条界之前，被裁的轮次在本进程之外没有第二份——快照存投影、AgentEnd
    /// 发投影，于是「模型没看到」等于「这段对话没有发生过」。
    #[test]
    fn a_trimmed_turn_stays_in_the_history() {
        let mut ctx = ContextWindow::new(40);
        let turns = [
            "第一轮 filler filler filler filler",
            "第二轮 filler filler filler filler",
            "第三轮 filler filler filler filler",
        ];
        ctx.add_message(Message::user("目标：把网关的上游换掉"));
        for t in turns {
            ctx.add_message(Message::assistant_text(t));
        }

        let history: Vec<String> = ctx.history().iter().map(|m| m.content()).collect();
        for kept in ["目标：把网关的上游换掉"].into_iter().chain(turns) {
            assert!(
                history.iter().any(|c| c == kept),
                "历史里少了「{kept}」：{history:?}"
            );
        }
        assert!(
            ctx.messages().len() < ctx.history().len(),
            "这张网得真的被裁过，否则这条测试什么都没证明（投影 {} 条 / 历史 {} 条）",
            ctx.messages().len(),
            ctx.history().len()
        );
    }

    /// 进上下文时被这条界裁掉的那一份，历史里是**过界之前**的全文：这道界是
    /// 链路上唯一不留引用就丢字节的地方（工具返回点那条先归档再裁，模型手里
    /// 有引用），历史记在它前面，被裁的字节就还在。
    #[test]
    fn a_tool_result_cut_on_the_way_in_keeps_its_bytes_in_the_history() {
        let mut ctx = ContextWindow::new(400);
        let full = "z".repeat(5_000);
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_raw",
            "run_command",
            serde_json::json!({"command": "dump"}),
        )]));
        ctx.add_message(Message::tool_result_text("call_raw", "run_command", &full));

        let projected = ctx.messages().last().unwrap().content();
        assert!(
            projected.contains("tool output truncated"),
            "投影这一份是被裁过的"
        );
        assert_eq!(
            ctx.history().last().unwrap().content(),
            full,
            "耐久面记的是过界之前的那条"
        );
    }

    #[test]
    fn clearing_the_window_clears_both_faces() {
        let mut ctx = ContextWindow::new(40);
        ctx.add_message(Message::user("第一轮 filler filler filler filler"));
        ctx.add_message(Message::user("第二轮 filler filler filler filler"));
        ctx.clear();

        assert!(ctx.messages().is_empty());
        assert!(
            ctx.history().is_empty(),
            "新一轮从空开始，否则上一轮的对话会算进这一轮的账"
        );
    }

    /// 快照恢复喂的是投影，两个面一起从它长出：恢复之后历史与投影同长，
    /// 之后的新消息照旧记全。
    #[test]
    fn a_restored_snapshot_seeds_both_faces() {
        let mut ctx = ContextWindow::new(400);
        ctx.restore_messages(vec![
            Message::user("任务输入"),
            Message::assistant_text("上一轮的回答"),
        ]);

        assert_eq!(ctx.history().len(), ctx.messages().len());
        assert_eq!(ctx.history().len(), 2);
    }

    #[test]
    fn an_already_truncated_result_keeps_its_reference_through_the_window() {
        // 返回点已按预算裁过一次并带上引用；进上下文时同一条界不能再裁一次，
        // 那一次会把标记连同引用一起砍掉。两处必须用同一个预算。
        let mut ctx = ContextWindow::new(400);
        let budget = ctx.tool_result_budget_chars();
        ctx.add_message(Message::assistant(vec![cog_core::ContentBlock::tool_call(
            "call_ref",
            "run_command",
            serde_json::json!({"command": "dump"}),
        )]));
        let full = "y".repeat(5000);
        let truncated = truncate_to_chars(&full, budget, Some("artifact://default/ref-1")).text;
        ctx.add_message(Message::tool_result_text(
            "call_ref",
            "run_command",
            &truncated,
        ));
        let kept = ctx.messages().last().unwrap().content();
        assert!(
            kept.contains("artifact://default/ref-1"),
            "二次裁剪不得把引用砍掉: {kept}"
        );
    }
}
