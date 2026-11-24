//! principle: feedback_the_unit_of_work_is_the_batch_not_the_turn
//!
//! **一次整批生成 = 一次请求 + N 次工具调用，而且每一笔都归得到这一批。**
//!
//! 这句话以前哪里都没有钉。两半各自能读成对的：入口那一侧把整批拼成一个请求，
//! 调用事件那一侧按请求头上的任务号写行——中间那一段（请求带没带批号、批号一路
//! 传到工具的每一次调用没有）没有任何东西判。漏掉它的时候，账上「这一批花了几次
//! 调用」永远查不到：行都在，只是没有一行挂得住这一批，而「没挂住」与「没花钱」
//! 在读数上同形。
//!
//! 判的是**分工**，不是模型：假模型先要 N 次工具再收尾，真 agent 跑真的循环。
//! 一条用例同时钉三件——请求只有一份（整批进去，不是一条一条地进）、工具调用真
//! 发生了 N 次、这 N+1 次模型调用的任务头与主体头都是这一批与这个员工。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cog_core::{
    AssistantMessageEvent, AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions,
    ContentBlock, LlmClient, Message, RuntimeConfig, SFResult, StopReason, LLM_ACTOR_HEADER,
    LLM_TASK_HEADER,
};

/// 进 generator 的那一份请求：整批几条，逐条写它缺什么。
const BATCH_REQUEST: &str = "## S1\n缺的是：把入口接上\n\n## S2\n缺的是：把归因带上\n";

/// 这一次运行是谁：员工身份与角色都由外面声明，用例不替运行时编。
const AGENT_ID: &str = "employee-generator";

/// 假模型：前 `tool_turns` 次各要一次工具，之后收尾。
struct ToolThenAnswer {
    /// 每一次模型调用时请求头上的两个身份（任务、主体）。
    identity: Mutex<Vec<(Option<String>, Option<String>)>>,
    calls: Mutex<u32>,
    tool_turns: u32,
}

impl ToolThenAnswer {
    fn new(tool_turns: u32) -> Self {
        Self {
            identity: Mutex::new(Vec::new()),
            calls: Mutex::new(0),
            tool_turns,
        }
    }
}

#[async_trait]
impl LlmClient for ToolThenAnswer {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Err(cog_core::SFError::LLM(
            "this case streams; a non-streaming call would hide the turn it belongs to".into(),
        ))
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.identity.lock().unwrap().push((
            options.headers.get(LLM_TASK_HEADER).cloned(),
            options.headers.get(LLM_ACTOR_HEADER).cloned(),
        ));
        let turn = {
            let mut calls = self.calls.lock().unwrap();
            *calls += 1;
            *calls
        };
        let wants_tool = turn <= self.tool_turns;
        let message = if wants_tool {
            Message::assistant(vec![ContentBlock::ToolCall {
                id: format!("call-{turn}"),
                name: "read_file".into(),
                arguments: serde_json::json!({ "path": "crates/x.rs" }),
                thought_signature: None,
            }])
        } else {
            Message::assistant_text("整批改完了")
        };
        let reason = if wants_tool {
            StopReason::ToolUse
        } else {
            StopReason::Stop
        };
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_core::DEFAULT_STREAM_CAPACITY);
        let done = message.clone();
        let text = if wants_tool {
            String::new()
        } else {
            "整批改完了".to_string()
        };
        let response = ChatResponse {
            content: vec![ContentBlock::text(text)],
            api: "fake".into(),
            provider: "fake".into(),
            model: "fake".into(),
            response_id: None,
            usage: cog_core::Usage::default(),
            stop_reason: reason,
            error_message: None,
            upstream_failure: None,
            retry_after_secs: None,
            timestamp: chrono::Utc::now(),
        };
        tokio::spawn(async move {
            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason,
                    message: done,
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(response);
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(cog_core::SFError::LLM(
            "this case streams; the identity-free complete path is not the entry under test".into(),
        ))
    }

    async fn health_check(&self) -> bool {
        true
    }
}

/// 一次请求进来，N 次工具调用发生，N+1 次模型调用每一笔都挂着这一批与这个员工。
#[tokio::test]
async fn one_batch_generation_is_one_request_and_n_tool_calls_all_attributed() {
    const TOOL_CALLS: u32 = 2;
    let llm = Arc::new(ToolThenAnswer::new(TOOL_CALLS));
    let agent = Arc::new(cog_agent::Agent::new(
        RuntimeConfig {
            agent_id: AGENT_ID.into(),
            role: cog_core::roles::GENERATOR.into(),
            ..Default::default()
        },
        llm.clone(),
    ));
    let generator = cog_orchestrator::batch_edit::BatchGenerator::from_agent(agent);
    let batch =
        cog_orchestrator::batch_edit::BatchId::from_ids(["S1".to_string(), "S2".to_string()]);

    generator
        .generate(&batch, BATCH_REQUEST)
        .await
        .expect("整批一次生成应当答得回来");

    let seen = llm.identity.lock().unwrap().clone();
    assert_eq!(
        seen.len() as u32,
        TOOL_CALLS + 1,
        "一次请求 + {TOOL_CALLS} 次工具调用应当是 {} 次模型调用，实得 {}",
        TOOL_CALLS + 1,
        seen.len()
    );
    for (turn, (task, actor)) in seen.iter().enumerate() {
        assert_eq!(
            task.as_deref(),
            Some(batch.as_str()),
            "第 {} 次模型调用没有挂着这一批（任务头 {task:?}）：按批号查花销时它不见了",
            turn + 1
        );
        assert_eq!(
            actor.as_deref(),
            Some(AGENT_ID),
            "第 {} 次模型调用没有挂着这个员工（主体头 {actor:?}）",
            turn + 1
        );
    }
}
