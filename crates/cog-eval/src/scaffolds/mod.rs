//! 主表那四行外壳：一行一个 [`AgentScaffold`] 实现。
//!
//! 四行都只从 [`SolveContext`] 拿东西（backbone、工具、环境、预算、种子），谁也不
//! 认识判分器——判分在评测台那一侧做。所以这里加一行、删一行、换一行实现，都不动
//! 评测台；反过来，评测台里也不许出现这里任何一个名字（有检查守着）。

pub mod agentflow;
pub mod codeact;
pub mod gepa;
pub mod nql;

pub use agentflow::AgentFlow;
pub use codeact::CodeAct;
pub use gepa::Gepa;
pub use nql::{Nql, PlatformRequest, PlatformRunner};

use std::sync::Arc;

use cog_core::{ChatOptions, ContentBlock, Message};

use crate::dataset::EvalCase;
use crate::scaffold::{AgentScaffold, SolveContext};

/// 主表的四行，顺序与表一致。
///
/// 这张表是**实验的组成**，不是评测台的组成：谁来跑、跑哪几行由这里决定，评测台只
/// 认 `AgentScaffold`。NQL 那一行要一个平台端口，因为它的实现就是真平台。
pub fn table_scaffolds(platform: Arc<dyn PlatformRunner>) -> Vec<Arc<dyn AgentScaffold>> {
    vec![
        Arc::new(CodeAct::new()),
        Arc::new(Gepa::default()),
        Arc::new(AgentFlow::default()),
        Arc::new(Nql::new(platform)),
    ]
}

/// 代码执行工具的参数名。
///
/// 外壳与基准工具包之间**唯一**的约定：外壳把要跑的代码放这个键里。它得写下来，
/// 否则四个外壳会各自猜一个键名，而猜错的形状是「工具收到空代码、安静地跑了个空」——
/// 没有报错、分数只是低，没人能归因。
pub const CODE_ARG: &str = "code";

/// 一次模型调用之后，上下文里那一轮的样子。
///
/// 这是**协议边缘的一步**，不是任何一种外壳的循环：循环的形状（什么时候停、下一步
/// 做什么）由各个外壳自己定，四个外壳正是靠它互相区分。共用这一小步不会让两行变成
/// 同一行的实现——但把某个外壳的整条循环抽出来共享就会，所以共享到这里为止。
pub(crate) struct Turn {
    pub text: String,
    pub calls: Vec<(String, String, serde_json::Value)>,
    pub content: Vec<ContentBlock>,
    pub usage: cog_core::Usage,
}

/// 发一次请求并把答复拆开。
pub(crate) async fn take_turn(
    ctx: &SolveContext,
    messages: &[Message],
    options: &ChatOptions,
) -> anyhow::Result<Turn> {
    let response = ctx.llm.chat(messages, options).await?;
    Ok(Turn {
        text: text_of(&response.content),
        calls: tool_calls_of(&response.content),
        content: response.content,
        usage: response.usage,
    })
}

/// 执行一次工具调用，并把结果接回上下文。
///
/// 工具报错**不是**这一步的失败：它是一次观察结果，照样要喂回去（模型要能看见
/// 「这条路走不通」）。返回值里的 `ok` 只用来记 trace。
pub(crate) async fn observe_call(
    ctx: &SolveContext,
    messages: &mut Vec<Message>,
    call_id: &str,
    name: &str,
    arguments: &serde_json::Value,
) -> (String, bool) {
    match ctx.tools.call(name, arguments.clone()).await {
        Ok(value) => {
            let observation = json_text(&value);
            messages.push(Message::tool_result_text(
                call_id,
                name,
                observation.clone(),
            ));
            (observation, true)
        }
        Err(e) => {
            let observation = format!("tool error: {e}");
            messages.push(Message::tool_result_text(
                call_id,
                name,
                observation.clone(),
            ));
            (observation, false)
        }
    }
}

/// `Value` → 喂回给模型的一段文本。
pub(crate) fn json_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// 题面文本：外壳看到的就是它。结构化输入照样序列化成文本，外壳不解析题面。
pub(crate) fn task_text(case: &EvalCase) -> String {
    match &case.input {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// 请求参数。工具规格来自**基准**（外壳不能给自己加工具），温度来自预算
/// （四行同值，格子之间才可比）。
pub(crate) fn chat_options(ctx: &SolveContext) -> ChatOptions {
    ChatOptions {
        temperature: Some(ctx.budget.temperature),
        tools: if ctx.tools.is_empty() {
            None
        } else {
            Some(ctx.tools.definitions().to_vec())
        },
        ..Default::default()
    }
}

/// 答复里的正文。工具调用不算正文——把工具调用当答案交回去，判分器拿到的是
/// 一段协议文本，不是答案。
pub(crate) fn text_of(content: &[ContentBlock]) -> String {
    let mut out = String::new();
    for block in content {
        if let ContentBlock::Text { text, .. } = block {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(text);
        }
    }
    out
}

/// 答复里请求的工具调用。
pub(crate) fn tool_calls_of(content: &[ContentBlock]) -> Vec<(String, String, serde_json::Value)> {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall {
                id,
                name,
                arguments,
                ..
            } => Some((id.clone(), name.clone(), arguments.clone())),
            _ => None,
        })
        .collect()
}

/// 把一次答复的用量并进累计。
///
/// 单题的成本是**各步之和**，不是最后一步：只看最后一步会把中间那些步记成免费，
/// 而步数正是各外壳之间差别最大的地方。
pub(crate) fn accumulate(total: &mut cog_core::Usage, add: &cog_core::Usage) {
    total.input += add.input;
    total.output += add.output;
    total.cache_read += add.cache_read;
    total.cache_write += add.cache_write;
    total.total_tokens += add.total_tokens;
    total.cost.input += add.cost.input;
    total.cost.output += add.cost.output;
    total.cost.cache_read += add.cost.cache_read;
    total.cost.cache_write += add.cost.cache_write;
    total.cost.total += add.cost.total;
}

/// 从一段文本里取出第一个围栏代码块的内容。
///
/// 纯函数，放在协议边缘：外壳的动作空间是「代码」，而模型吐代码最常见的形式就是
/// 围栏块。解析成败直接影响这一行的分数，所以它自己能被单测。
pub(crate) fn first_code_block(text: &str) -> Option<String> {
    let mut lines = text.lines();
    let mut body: Vec<&str> = Vec::new();
    let mut inside = false;
    for line in lines.by_ref() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("```") {
            if inside {
                return Some(body.join("\n"));
            }
            inside = true;
            continue;
        }
        if inside {
            body.push(line);
        }
    }
    let _ = lines;
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fenced_block_is_extracted_without_its_fence() {
        let text = "先想一下\n```python\nprint(1)\nprint(2)\n```\n完事";
        assert_eq!(first_code_block(text).unwrap(), "print(1)\nprint(2)");
        assert!(first_code_block("没有代码").is_none());
        // 没闭合的块不算代码：半截代码执行不了，当成答案又会被判错。
        assert!(first_code_block("```python\nprint(1)").is_none());
    }

    #[test]
    fn tool_calls_are_not_mistaken_for_an_answer() {
        let content = vec![
            ContentBlock::text("让我查一下"),
            ContentBlock::ToolCall {
                id: "t1".into(),
                name: "search".into(),
                arguments: serde_json::json!({"q": "x"}),
                thought_signature: None,
            },
        ];
        assert_eq!(text_of(&content), "让我查一下");
        let calls = tool_calls_of(&content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, "search");
    }

    #[test]
    fn usage_accumulates_over_steps() {
        let mut total = cog_core::Usage::default();
        let step = cog_core::Usage {
            total_tokens: 7,
            cost: cog_core::message::Cost {
                total: 0.5,
                ..Default::default()
            },
            ..Default::default()
        };
        accumulate(&mut total, &step);
        accumulate(&mut total, &step);
        assert_eq!(total.total_tokens, 14);
        assert_eq!(total.cost.total, 1.0);
    }
}

/// 四个外壳的测试替身。
///
/// 替身只放在这里、只在这里被复用：它们是**外壳的**测试用件，评测台那边一条都不碰
/// （评测台的中立性由 `tests/scaffold_neutral_rig.rs` 守着，这个模块不在它的检查范围
/// 里，但它也不该长出被测对象的逻辑）。
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use cog_core::{ChatOptions, ChatResponse, ContentBlock, Message, SFResult, Usage};

    use crate::scaffold::ToolSet;

    /// 工具替身吐出的固定输出。测试用它当**探针**：它出现在下一次请求里，
    /// 就说明观察结果真的回到了上下文。
    pub const CANNED_STDOUT: &str = "canned-observation";

    /// 一次脚本化的答复。
    pub enum Step {
        Text(String),
        ToolCall {
            name: String,
            arguments: serde_json::Value,
        },
    }

    /// 按调用次序回话的 backbone 替身。
    ///
    /// 它按**第几次调用**取答复，并把每一次请求原样记下来。用次序而不是「按请求内容
    /// 匹配」是有意的：改流程时按内容匹配会继续把同一句话发到另一处，静默地把测试
    /// 变成同义反复；按次序取会让那次改动在 `requests()` 上看得见。脚本用尽时它
    /// 直接 panic——外壳多问一次是流程变了，不该被吞掉。
    pub struct ScriptedLlm {
        steps: Vec<Step>,
        seen: Mutex<Vec<Vec<Message>>>,
        served: AtomicUsize,
    }

    impl ScriptedLlm {
        pub fn new(steps: Vec<Step>) -> Self {
            Self {
                steps,
                seen: Mutex::new(Vec::new()),
                served: AtomicUsize::new(0),
            }
        }

        pub fn text(text: impl Into<String>) -> Step {
            Step::Text(text.into())
        }

        pub fn tool_call(name: impl Into<String>, arguments: serde_json::Value) -> Step {
            Step::ToolCall {
                name: name.into(),
                arguments,
            }
        }

        pub fn requests(&self) -> Vec<Vec<Message>> {
            self.seen.lock().unwrap().clone()
        }

        pub fn calls(&self) -> usize {
            self.served.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl cog_core::LlmClient for ScriptedLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!("the scripts never stream")
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unreachable!("the scripts never stream")
        }

        async fn chat(
            &self,
            messages: &[Message],
            _options: &ChatOptions,
        ) -> SFResult<ChatResponse> {
            self.seen.lock().unwrap().push(messages.to_vec());
            let index = self.served.fetch_add(1, Ordering::SeqCst);
            let step = self.steps.get(index).unwrap_or_else(|| {
                panic!(
                    "the script ran out after {} calls: the scaffold asked one more time than \
                     the test scripted",
                    self.steps.len()
                )
            });
            let content = match step {
                Step::Text(text) => vec![ContentBlock::text(text.clone())],
                Step::ToolCall { name, arguments } => vec![ContentBlock::ToolCall {
                    id: format!("call-{index}"),
                    name: name.clone(),
                    arguments: arguments.clone(),
                    thought_signature: None,
                }],
            };
            Ok(ChatResponse {
                content,
                api: "scripted".into(),
                provider: "scripted".into(),
                model: "scripted".into(),
                response_id: None,
                usage: Usage {
                    total_tokens: 10,
                    ..Default::default()
                },
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// 记下每次工具调用的工具面。
    pub struct ToolRecorder {
        calls: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
    }

    impl ToolRecorder {
        pub fn new() -> Self {
            Self {
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// 一束工具：每个名字一个 native 闭包，跑起来只记一笔并回固定的输出。
        pub fn toolset(&self, names: &[&str]) -> ToolSet {
            let tools = names
                .iter()
                .map(|name| {
                    let name = (*name).to_string();
                    let calls = self.calls.clone();
                    cog_core::Tool {
                        name: name.clone(),
                        description: format!("the `{name}` tool"),
                        parameters: serde_json::json!({"type": "object"}),
                        implementation: cog_core::ToolImplementation::Native(Arc::new(
                            move |args: serde_json::Value| {
                                let name = name.clone();
                                let calls = calls.clone();
                                Box::pin(async move {
                                    calls.lock().unwrap().push((name, args));
                                    Ok(serde_json::json!({ "stdout": CANNED_STDOUT }))
                                })
                            },
                        )),
                    }
                })
                .collect();
            ToolSet::new(tools).unwrap()
        }

        pub fn calls(&self) -> Vec<(String, serde_json::Value)> {
            self.calls.lock().unwrap().clone()
        }

        pub fn count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    /// 基准最常给的那束：bash + 一个搜索工具。
    pub fn code_tool(recorder: &ToolRecorder) -> ToolSet {
        recorder.toolset(&["python", "bash", "search"])
    }

    /// 一段请求里最后一条消息的文本。
    pub fn last_text(messages: &[Message]) -> String {
        messages.last().map(|m| m.content()).unwrap_or_default()
    }

    /// 一段请求里的系统提示词。用来钉「这一代跑的是不是那一份提示词」。
    pub fn system_of(messages: &[Message]) -> String {
        for message in messages {
            if let Message::System { content, .. } = message {
                return content.clone();
            }
        }
        String::new()
    }
}
