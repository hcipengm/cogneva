//! 属主闲聊的一句：落一条意图，不建任务，也不多买一次模型调用。
//!
//! 「这句是不是活」由回答那一轮的同一次调用顺手判（模型选择调不调
//! `start_task`），不是另起一次分类调用。判完无论有没有活，这一句都要记一条
//! 接手事件——否则下一批分诊会把已经答过的话再捡一遍，而那一次没有这一轮的
//! 对话，判出来的东西是另一个人看着半截材料做的决定。
//!
//! 这里拉起真的 `PlatformConversation::turn`，接一只记录请求的假模型与一本
//! 记录意图的假账：判的是三件事——
//!
//! 1. 这一句成为一条意图，原话不改写，收件人是在对话的那个员工；
//! 2. 它被标成接手过（`task_id` 为空，读作「这一句只是话」），所以批量分诊
//!    不会再捡它；
//! 3. 这一轮只花了一次模型调用，任务图上一行也没有多。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, IntentRow, LlmClient, Message, SFResult,
};
use cog_gateway::chat::{ConversationRequest, ConversationTurn};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use cogneva::conversation::{Ledgers, PlatformConversation};

use cog_core::OrchestratorControl;

/// 一本记在内存里的意图账：记下了什么、谁被标成接手过。
#[derive(Default)]
struct RecordingIntents {
    rows: Mutex<Vec<IntentRow>>,
    triaged: Mutex<Vec<(Vec<String>, String, String)>>,
}

#[async_trait]
impl cog_core::EventStore for RecordingIntents {
    fn writer(&self) -> &str {
        "test"
    }
    fn next_event_id(&self) -> String {
        format!("T{}", self.rows.lock().unwrap().len())
    }
    async fn append(&self, event: cog_core::FactEvent) -> SFResult<()> {
        match event.payload {
            cog_core::EventPayload::IntentRecorded(row) => {
                self.rows.lock().unwrap().push(row);
            }
            cog_core::EventPayload::IntentTriaged {
                identity,
                task_id,
                actor,
            } => {
                self.triaged
                    .lock()
                    .unwrap()
                    .push((vec![identity], task_id, actor));
            }
            _ => {}
        }
        Ok(())
    }
}

/// 一只只会答一句话、从不调工具的模型；记下它收到的每一发。
#[derive(Default)]
struct TalkingLlm {
    calls: Mutex<Vec<Vec<Message>>>,
}

#[async_trait]
impl LlmClient for TalkingLlm {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unreachable!("循环走的是流式那条")
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.calls.lock().unwrap().push(messages.to_vec());
        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_llm::DEFAULT_STREAM_CAPACITY);
        tokio::spawn(async move {
            let mut response = ChatResponse {
                content: Vec::new(),
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: Usage::default(),
                stop_reason: StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            };
            let _ = producer
                .push(AssistantMessageEvent::Start {
                    timestamp: chrono::Utc::now(),
                })
                .await;
            response.content.push(ContentBlock::text(""));
            let _ = producer
                .push(AssistantMessageEvent::TextStart {
                    content_index: 0,
                    timestamp: chrono::Utc::now(),
                })
                .await;
            let text = "今天挺好".to_string();
            response.content[0] = ContentBlock::text(text.clone());
            let _ = producer
                .push(AssistantMessageEvent::TextDelta {
                    content_index: 0,
                    delta: text.clone(),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            let _ = producer
                .push(AssistantMessageEvent::TextEnd {
                    content_index: 0,
                    content: text,
                    timestamp: chrono::Utc::now(),
                })
                .await;
            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: StopReason::Stop,
                    message: Message::assistant(response.content.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(response);
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.chat_stream(&[Message::user(prompt)], &ChatOptions::default())
            .await
    }

    async fn health_check(&self) -> bool {
        true
    }
}

fn conversation(
    intents: Arc<RecordingIntents>,
) -> (PlatformConversation, Arc<dyn OrchestratorControl>) {
    use crate::common::orchestrator::control;

    let orchestrator = control("test-workspace");
    let turns = PlatformConversation::new(
        Arc::clone(&orchestrator),
        None,
        None,
        None,
        None,
        None,
        None,
        Ledgers::default(),
        None,
        None,
        None,
        None,
        None,
        None,
        Some(intents as Arc<dyn cog_core::EventStore>),
    );
    (turns, orchestrator)
}

fn request(session: &str, text: &str, llm: Arc<TalkingLlm>) -> ConversationRequest {
    ConversationRequest {
        session_id: session.into(),
        owner: "owner".into(),
        messages: vec![Message::user(text)],
        original_text: None,
        arrival_id: None,
        llm,
        platform: Some("web".into()),
        device_id: None,
        model: None,
        permission_mode: "default".into(),
        permissions: None,
    }
}

/// 跑一轮，把事件出口在后台抽干：出口满了会把这一轮堵住。
async fn run_turn(turns: &PlatformConversation, request: ConversationRequest) -> serde_json::Value {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<AgentEvent>(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let answer = turns.turn(request, tx).await.expect("这一轮该跑完");
    drain.await.expect("事件出口的读者不该崩");
    answer
}

/// 闲聊一句：一条意图、一条接手事件、零任务、一次模型调用。
#[tokio::test]
async fn a_turn_that_is_only_talk_files_nothing_and_buys_no_second_call() {
    let _guard = cog_core::progress::async_test_lock().await;

    let intents = Arc::new(RecordingIntents::default());
    let llm = Arc::new(TalkingLlm::default());
    // control() 是新起的内存账；这一轮派没派活，读的就是它。
    let (turns, orchestrator) = conversation(Arc::clone(&intents));

    let answer = run_turn(
        &turns,
        request("session-talk", "今天怎么样", Arc::clone(&llm)),
    )
    .await;

    assert!(
        answer.get("tasks").is_none(),
        "闲聊的一句被派成了任务：{answer}"
    );
    assert!(
        orchestrator.get_all_tasks().await.is_empty(),
        "任务图上多了一行：闲聊不该建任务"
    );
    assert_eq!(
        llm.calls.lock().unwrap().len(),
        1,
        "这一轮买了不止一次模型调用：判断是不是活不该另起一次分类"
    );

    let rows = intents.rows.lock().unwrap();
    assert_eq!(rows.len(), 1, "这一句没有成为一条意图");
    assert_eq!(rows[0].source, "chat");
    assert_eq!(rows[0].text, "今天怎么样", "原话被改写了");
    assert_eq!(rows[0].said_by, "owner");
    assert!(
        !rows[0].addressed_to.is_empty(),
        "意图没有收件人：不知道这句话是说给哪个员工的"
    );
    drop(rows);

    let triaged = intents.triaged.lock().unwrap();
    assert_eq!(triaged.len(), 1, "这一句没有被标成接手过");
    assert_eq!(triaged[0].0.len(), 1);
    assert_eq!(
        triaged[0].1, "",
        "没有任务时写了一个编出来的任务号：读面会去反查一条不存在的行"
    );
    assert_eq!(triaged[0].2, "conversation:no-work");
}

/// `/chat` 是属主自己说的判词：这一轮交给模型的说明里带着它，不由模型再猜。
#[tokio::test]
async fn the_chat_switch_reaches_the_turn_as_a_directive() {
    let _guard = cog_core::progress::async_test_lock().await;

    let intents = Arc::new(RecordingIntents::default());
    let llm = Arc::new(TalkingLlm::default());
    let (turns, _orchestrator) = conversation(Arc::clone(&intents));

    run_turn(
        &turns,
        request("session-chat-switch", "/chat 聊两句", Arc::clone(&llm)),
    )
    .await;

    let calls = llm.calls.lock().unwrap();
    let system: Vec<&str> = calls
        .first()
        .expect("这一轮该有一次模型调用")
        .iter()
        .filter_map(|message| match message {
            Message::System { content, .. } => Some(content.as_str()),
            _ => None,
        })
        .collect();
    assert!(
        system.iter().any(|text| text.contains("打了 `/chat`")),
        "`/chat` 没有作为判词交给模型：{:?}",
        system
    );
    assert!(
        system
            .iter()
            .any(|text| text.contains("不要调用 start_task")),
        "`/chat` 这一轮没有说清不要建任务：{:?}",
        system
    );
}
