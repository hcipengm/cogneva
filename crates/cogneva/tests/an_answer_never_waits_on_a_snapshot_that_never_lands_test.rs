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
//!
//! 另一半同样在这里判：回答不等它，**命令仍然等**。快照拍的是这一轮动手之前的
//! 树，命令赶在它前面就拍不到那张树了。两条合起来才是这次改动的那条次序。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::common::orchestrator::control;

use cog_core::{
    AssistantMessageEvent, ChatOptions, ChatResponse, ContentBlock, KeyValueStore, LlmClient,
    Message, SFError, SFResult, SandboxBackend, SandboxPayload, SandboxRequest, SandboxResult,
    StopReason, ToolRegistry as _,
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

/// 这一段对话的「说过话的人 / 检查点」那一格：标量与哈希都收。
///
/// 这里不用套件里那个只做配额算术的替身：它在哈希字段上 `unreachable!()`，
/// 而检查点正是按哈希存的——拿它当存储，失败的是夹具而不是被测的那一轮。
#[derive(Default)]
struct Store {
    scalars: std::sync::Mutex<std::collections::HashMap<String, String>>,
    hashes: std::sync::Mutex<
        std::collections::HashMap<String, std::collections::HashMap<String, String>>,
    >,
}

#[async_trait]
impl KeyValueStore for Store {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        Ok(self.scalars.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        self.scalars
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, key: &str) -> SFResult<()> {
        self.scalars.lock().unwrap().remove(key);
        Ok(())
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        Ok(())
    }

    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        Ok(-1)
    }

    async fn incr(&self, key: &str, by: i64) -> SFResult<i64> {
        let mut scalars = self.scalars.lock().unwrap();
        let next = scalars
            .get(key)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            + by;
        scalars.insert(key.to_string(), next.to_string());
        Ok(next)
    }

    async fn incr_ex(&self, key: &str, by: i64, _ttl_secs: u64) -> SFResult<i64> {
        self.incr(key, by).await
    }

    async fn decr(&self, key: &str, by: i64) -> SFResult<i64> {
        self.incr(key, -by).await
    }

    async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
        Ok(())
    }

    async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
        Ok(())
    }

    async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn hset(&self, key: &str, field: &str, value: &str) -> SFResult<()> {
        self.hashes
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_default()
            .insert(field.to_string(), value.to_string());
        Ok(())
    }

    async fn hget(&self, key: &str, field: &str) -> SFResult<Option<String>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .and_then(|hash| hash.get(field).cloned()))
    }

    async fn hdel(&self, key: &str, field: &str) -> SFResult<()> {
        if let Some(hash) = self.hashes.lock().unwrap().get_mut(key) {
            hash.remove(field);
        }
        Ok(())
    }

    async fn hvals(&self, key: &str) -> SFResult<Vec<String>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.values().cloned().collect())
            .unwrap_or_default())
    }

    async fn hgetall(&self, key: &str) -> SFResult<Vec<(String, String)>> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.iter().map(|(f, v)| (f.clone(), v.clone())).collect())
            .unwrap_or_default())
    }

    async fn hlen(&self, key: &str) -> SFResult<u64> {
        Ok(self
            .hashes
            .lock()
            .unwrap()
            .get(key)
            .map(|hash| hash.len() as u64)
            .unwrap_or(0))
    }
}

fn conversation(
    sandbox: Arc<dyn SandboxBackend>,
    people: Arc<dyn cog_core::KeyValueStore>,
    tools: Option<Arc<cog_agent::ToolRegistry>>,
) -> PlatformConversation {
    PlatformConversation::new(
        control("test-workspace"),
        None,
        None,
        tools,
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
        Arc::new(Store::default()) as Arc<dyn cog_core::KeyValueStore>,
        None,
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

/// 一张拍得很慢的快照：命令那一侧必须等它，它自己不等任何人。
///
/// 它把「命令是不是赶在快照前面动了手」记在自己身上——这个次序没有别处读得出。
#[derive(Default)]
struct SlowSnapshot {
    snapshot_done: AtomicBool,
    commands: AtomicUsize,
    command_came_first: AtomicBool,
}

fn empty_result() -> SandboxResult {
    SandboxResult {
        stdout: String::new(),
        stderr: String::new(),
        exit_code: 0,
        output: None,
        duration_ms: 0,
        resource_usage: Default::default(),
        workdir: None,
    }
}

#[async_trait]
impl SandboxBackend for SlowSnapshot {
    async fn execute(&self, req: &SandboxRequest) -> SFResult<SandboxResult> {
        match &req.payload {
            SandboxPayload::CaptureCheckpoint { .. } => {
                // 真后端拍一张也要走一趟换版的远端读盘：慢，但不是永远不回。
                tokio::time::sleep(Duration::from_millis(500)).await;
                self.snapshot_done.store(true, Ordering::SeqCst);
                Ok(empty_result())
            }
            _ => {
                self.commands.fetch_add(1, Ordering::SeqCst);
                if !self.snapshot_done.load(Ordering::SeqCst) {
                    self.command_came_first.store(true, Ordering::SeqCst);
                }
                Ok(empty_result())
            }
        }
    }

    async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
        Err(SFError::Agent("这一条不编 wasm".into()))
    }
}

/// 第一轮就要跑一条命令、第二轮收尾的模型。
struct WantsACommand {
    calls: AtomicUsize,
}

#[async_trait]
impl LlmClient for WantsACommand {
    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        let nth = self.calls.fetch_add(1, Ordering::SeqCst);
        let (content, stop_reason) = if nth == 0 {
            (
                vec![ContentBlock::ToolCall {
                    id: "call-1".into(),
                    name: "run_command".into(),
                    arguments: serde_json::json!({ "command": "echo hi" }),
                    thought_signature: None,
                }],
                StopReason::ToolUse,
            )
        } else {
            (vec![ContentBlock::text("命令跑完了。")], StopReason::Stop)
        };
        let (stream, mut producer) = cog_core::EventStream::with_capacity(4);
        tokio::spawn(async move {
            producer.end(ChatResponse {
                content,
                stop_reason,
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

#[tokio::test]
async fn the_first_command_still_waits_for_the_opening_snapshot() {
    let executor = Arc::new(SlowSnapshot::default());
    let counter = Arc::clone(&executor);
    let tools = cog_agent::ToolRegistry::new();
    cog_core::ToolRegistry::register(&tools, cog_agent::tools::builtins::run_command());
    let conversation = conversation(
        executor,
        Arc::new(Store::default()) as Arc<dyn cog_core::KeyValueStore>,
        Some(Arc::new(tools)),
    );
    let (events, mut seen) = tokio::sync::mpsc::channel(64);
    // 这一轮发的事件比一轮纯回答多，有人要在场把它读掉，不然发送端会顶住。
    tokio::spawn(async move { while seen.recv().await.is_some() {} });
    let outcome = conversation.turn(
        ConversationRequest {
            session_id: "s-command-after-snapshot".into(),
            owner: "alice".into(),
            messages: vec![Message::user("跑一条命令")],
            llm: Arc::new(WantsACommand {
                calls: AtomicUsize::new(0),
            }),
            platform: Some("web".into()),
            device_id: None,
            model: None,
            // 跑命令在默认模式下要问人；这条判的是次序，模式取不问人的那一档。
            permission_mode: "bypass".into(),
            permissions: None,
        },
        events,
    );
    let settled = tokio::time::timeout(Duration::from_secs(10), outcome)
        .await
        .expect("这一轮在十秒内没有收尾");
    settled.expect("这一轮应当跑完并交出答案");
    assert_eq!(
        counter.commands.load(Ordering::SeqCst),
        1,
        "这一轮没有真的跑过那条命令：命令那一侧的次序判不了"
    );
    assert!(
        !counter.command_came_first.load(Ordering::SeqCst),
        "命令赶在会话开头那张快照之前动了手：这张快照不再是「会话开头的树」"
    );
}
