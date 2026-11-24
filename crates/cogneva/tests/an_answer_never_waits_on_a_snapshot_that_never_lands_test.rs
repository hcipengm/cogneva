//! 会话开头那张快照是尽力而为的，它不该是一句回答的门票。
//!
//! 快照为的是「退回到这一段对话之前」。拍不下来时，装配层已经有一条如实的出路：
//! 把「这个会话没有开头快照」记下来（`mark_unavailable`），退不回去时界面读到的
//! 是那一句实话。它从来不是这一轮能不能说话的前提。
//!
//! 但它的位置曾经是前提：开跑之前同步等这一趟。执行器没有就绪后端时，这一趟会
//! 在整个换版窗口里重试（`REPLACEMENT_WINDOW`），于是**一句话在屏幕上停在
//! `message.start` 上好几分钟**——2026-11-24 实测一轮 3 分 26 秒，其间没有任何
//! 可见信号，最后落的还是一句与快照毫无关系的模型侧拒绝。
//!
//! 这里判的就是那个次序：执行器的快照永远不回时，这一轮仍然按它自己的结局收尾，
//! 而且是**当场**收尾。把同步等待放回去，这一条当场红（十秒的界都过不去）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::kv::MemoryStore;
use crate::common::orchestrator::control;

use cog_core::{
    AssistantMessageEvent, ChatOptions, ChatResponse, LlmClient, Message, SFError, SFResult,
    SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult, StopReason,
};
use cog_gateway::chat::{ConversationRequest, ConversationTurn};
use cogneva::conversation::{Ledgers, PlatformConversation};

/// 执行器在换版：端口一直拒，快照那一趟永远不回。
///
/// 它数着自己被问了几次，好在断言失败时说清「等的是哪一趟」。
#[derive(Default)]
struct ExecutorThatNeverAnswers {
    snapshots_asked: AtomicUsize,
    commands_asked: AtomicUsize,
}

#[async_trait]
impl SandboxBackend for ExecutorThatNeverAnswers {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        match &req.payload {
            SandboxPayload::CaptureCheckpoint { .. } => {
                self.snapshots_asked.fetch_add(1, Ordering::SeqCst);
                // 换版窗口里的等待在真后端里就是这样：一直重试，直到窗口过去。
                // 这一条不模拟那个窗口，直接永不返回——等的人等多久，它就等多久。
                std::future::pending::<()>().await;
                unreachable!("pending never resolves")
            }
            other => {
                self.commands_asked.fetch_add(1, Ordering::SeqCst);
                Err(SFError::Agent(format!("这条测试不发命令：{other:?}")))
            }
        }
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent("这一条不编 wasm".into()))
    }
}

/// 一个按上游拒绝的那句话收尾的模型：它是这一轮真正的结局。
struct Refusing {
    wording: String,
}

#[async_trait]
impl LlmClient for Refusing {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::EventStream::with_capacity(4);
        let wording = self.wording.clone();
        tokio::spawn(async move {
            let _ = producer
                .push(AssistantMessageEvent::Error {
                    reason: StopReason::Error,
                    error: Message::assistant_text(wording.clone()),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(ChatResponse {
                error_message: Some(wording),
                stop_reason: StopReason::Error,
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                ..ChatResponse::default()
            });
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        Err(SFError::Agent("这一条不发补全".into()))
    }

    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Err(SFError::Agent("这一条不走非流式".into()))
    }

    async fn health_check(&self) -> bool {
        true
    }
}

fn conversation(
    sandbox: Arc<dyn SandboxBackend>,
    people: Arc<dyn cog_core::KeyValueStore>,
) -> PlatformConversation {
    PlatformConversation::new(
        control("test-workspace"),
        None,
        None,
        None,
        None,
        None,
        Some(people),
        Ledgers::default(),
        None,
        None,
        None,
        None,
        None,
        Some(sandbox),
    )
}

#[tokio::test]
async fn a_turn_ends_with_its_own_failure_while_the_opening_snapshot_never_lands() {
    let executor = Arc::new(ExecutorThatNeverAnswers::default());
    let asked = Arc::clone(&executor);
    let conversation = conversation(
        executor,
        Arc::new(MemoryStore::default()) as Arc<dyn cog_core::KeyValueStore>,
    );
    let (events, mut seen) = tokio::sync::mpsc::channel(64);
    let outcome = conversation.turn(
        ConversationRequest {
            session_id: "s-opening-snapshot".into(),
            owner: "alice".into(),
            messages: vec![Message::user("你好")],
            llm: Arc::new(Refusing {
                wording: "API error (HTTP 503): 网关未配置 LLM 上游".into(),
            }),
            platform: Some("web".into()),
            device_id: None,
            model: None,
            permission_mode: "default".into(),
            permissions: None,
        },
        events,
    );
    // 十秒是这个界的两倍余量：健康部署里这一轮是毫秒级的，等的是模型不是快照。
    let settled = tokio::time::timeout(Duration::from_secs(10), outcome).await;
    let asked_snapshots = asked.snapshots_asked.load(Ordering::SeqCst);
    let asked_commands = asked.commands_asked.load(Ordering::SeqCst);
    // 读空事件管道，把这一轮已经说出口的话收下来（发送端已随这一轮丢掉）。
    let mut said = Vec::new();
    while let Ok(event) = seen.try_recv() {
        said.push(format!("{event:?}"));
    }
    let settled = settled.unwrap_or_else(|_| {
        panic!(
            "这一轮在十秒内没有收尾：它等的是一张谁也拿不到的会话开头快照。\
             快照被问了 {asked_snapshots} 次、命令被问了 {asked_commands} 次；\
             这一轮已经说出口的：{said:?}"
        )
    });
    assert_eq!(
        asked_snapshots, 1,
        "这一轮没有去拍会话开头的快照：快照是它的附加能力，不是可以省掉的东西"
    );
    let error = settled.expect_err("上游拒绝了，这一轮应当按失败收尾");
    assert!(
        error.to_string().contains("503"),
        "收尾的那句话不是上游拒绝的原文：{error}"
    );
}
