//! 断点续跑：一次尝试被杀掉之后，下一次派发从「最后一条有结果的工具调用之后」
//! 接着跑，而不是把整件活重买一遍。
//!
//! 两个面各判一次，缺一个这套东西就不成立：
//!
//! - **写**：内核把对话按模型看到的样子记下来——助手那条在工具执行**之前**落盘，
//!   结果在工具返回之后落盘，交付后关闭。少了那条顺序，续跑时「没执行」与
//!   「执行到一半」不可分辨。
//! - **读**：拿着一条开着的日志启动一个新进程，它不重问已经问过的、不留下一个
//!   没有结果的调用，并且按副作用等级处置那个「不确定」的调用：只读的重跑，
//!   有外部效果的不重跑、把事实交给模型。
use async_trait::async_trait;
use cog_agent::AgentRuntime;
use cog_core::attempt_journal::{AttemptJournal, JournalEntry, JournalKind};
use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, LlmClient, Message, RuntimeConfig, SFResult,
    ToolRegistry,
};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

// ── 一份内存里的日志 ───────────────────────────────────────────────────────

#[derive(Default)]
struct MemoryJournal {
    rows: Mutex<Vec<(String, JournalEntry)>>,
}

#[async_trait]
impl AttemptJournal for MemoryJournal {
    async fn append(&self, agent_id: &str, entry: &JournalEntry) -> Result<(), String> {
        let mut rows = self.rows.lock().unwrap();
        let seq = rows.iter().filter(|(t, _)| t == agent_id).count() as u64 + 1;
        let mut entry = entry.clone();
        entry.seq = seq;
        rows.push((agent_id.to_string(), entry));
        Ok(())
    }

    async fn load(&self, agent_id: &str) -> Result<Vec<JournalEntry>, String> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == agent_id)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl MemoryJournal {
    fn kinds(&self, agent_id: &str) -> Vec<JournalKind> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| t == agent_id)
            .map(|(_, e)| e.kind)
            .collect()
    }
}

// ── 一只记住「模型看到了什么」的上游 ───────────────────────────────────────

struct RecordingLlm {
    /// 每一次 `chat_stream` 收到的消息，按调用顺序。
    seen: Mutex<Vec<Vec<Message>>>,
    /// 前 `turns` 次调用带这个工具调用，之后只说话。
    tool_call: Option<(cog_core::ToolCall, usize)>,
    calls: AtomicUsize,
}

#[async_trait]
impl LlmClient for RecordingLlm {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unreachable!("the kernel streams; a non-streaming call would be a different path")
    }

    async fn chat_stream(
        &self,
        messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        let call_index = self.calls.fetch_add(1, Ordering::Relaxed);
        self.seen.lock().unwrap().push(messages.to_vec());

        let (stream, mut producer) =
            AssistantMessageEventStream::with_capacity(cog_llm::DEFAULT_STREAM_CAPACITY);
        let tool_call = self
            .tool_call
            .as_ref()
            .filter(|(_, turns)| call_index < *turns)
            .map(|(tc, _)| tc.clone());

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
            let text = "{\"result\": \"done\"}".to_string();
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
            if let Some(tc) = tool_call {
                response.content.push(ContentBlock::tool_call(
                    &tc.id,
                    &tc.name,
                    tc.arguments.clone(),
                ));
                let _ = producer
                    .push(AssistantMessageEvent::ToolCallEnd {
                        content_index: 1,
                        tool_call: tc,
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
            }
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

// ── 工具 ───────────────────────────────────────────────────────────────────

fn counting_tool(name: &str, runs: Arc<AtomicUsize>) -> cog_core::Tool {
    cog_core::Tool {
        name: name.to_string(),
        description: "counts how often it ran".into(),
        parameters: serde_json::json!({"type": "object"}),
        implementation: cog_core::ToolImplementation::Native(Arc::new(move |_args| {
            let runs = runs.clone();
            Box::pin(async move {
                runs.fetch_add(1, Ordering::Relaxed);
                Ok(serde_json::json!({"ok": true}))
            })
        })),
    }
}

/// 造一个运行体，连同它的事件出口一起交回。
///
/// 出口必须由调用方活着持有：内核在每次运行的序章里发 `AgentStart`，发不出去
/// 整次运行就当场失败——那是内核对自己的判据，不是测试的设置。丢掉接收端与
/// 「忘了跑」看起来一模一样，所以这里把两样绑成一对。
fn runtime(
    agent_id: &str,
    task_id: &str,
    runs: &[(&str, Arc<AtomicUsize>)],
) -> (AgentRuntime, mpsc::Receiver<AgentEvent>) {
    let registry = cog_agent::ToolRegistry::new();
    for (name, counter) in runs {
        registry.register(counting_tool(name, counter.clone()));
    }
    let (tx, rx) = mpsc::channel::<AgentEvent>(256);
    let agent = AgentRuntime::new(
        RuntimeConfig {
            stated_directions: Vec::new(),
            agent_id: agent_id.into(),
            role: "worker".to_string(),
            max_iterations: 4,
            context_window_size: 60_000,
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
            skill_config: None,
            crew_id: None,
            squad_id: None,
            owner_task_id: Some(task_id.to_string()),
            deliverable: cog_core::Deliverable::Unstated,
            deliverable_state: cog_core::DeclarationState::Unstated,
        },
        tx,
    )
    .with_tools(registry);
    (agent, rx)
}

fn llm_with_call(call: cog_core::ToolCall, turns: usize) -> RecordingLlm {
    RecordingLlm {
        seen: Mutex::new(Vec::new()),
        tool_call: Some((call, turns)),
        calls: AtomicUsize::new(0),
    }
}

#[tokio::test]
async fn a_run_records_the_conversation_in_the_order_that_makes_resume_possible() {
    let _guard = cog_core::progress::async_test_lock().await;
    cog_core::attempt_journal::reset_for_tests();

    let journal = Arc::new(MemoryJournal::default());
    cog_core::attempt_journal::install(journal.clone());

    let runs = Arc::new(AtomicUsize::new(0));
    let call = cog_core::ToolCall {
        id: "call-1".into(),
        name: "read_file".into(),
        arguments: serde_json::json!({"path": "a.rs"}),
    };
    let llm = llm_with_call(call, 1);
    let (mut rt, _events) = runtime(
        "squad-task-a-worker",
        "task-a",
        &[("read_file", runs.clone())],
    );
    rt.run(serde_json::json!({"task": "read it"}), &llm)
        .await
        .expect("the run finished");

    assert_eq!(runs.load(Ordering::Relaxed), 1, "the tool ran once");
    // 用户 → 助手（带调用）→ 工具结果 → 助手（交付）→ 关闭。
    assert_eq!(
        journal.kinds("squad-task-a-worker"),
        vec![
            JournalKind::User,
            JournalKind::Assistant,
            JournalKind::ToolResult,
            JournalKind::Assistant,
            JournalKind::Closed,
        ],
        "the journal is the conversation, in order, closed once it is delivered"
    );

    // 交付之后再派发一次：不续跑，从零开始——那条对话已经交出去了。
    let llm2 = llm_with_call(
        cog_core::ToolCall {
            id: "call-2".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path": "a.rs"}),
        },
        1,
    );
    let (mut rt2, _events2) = runtime(
        "squad-task-a-worker",
        "task-a",
        &[("read_file", runs.clone())],
    );
    rt2.run(serde_json::json!({"task": "read it"}), &llm2)
        .await
        .expect("the second run finished");
    let first_request = llm2
        .seen
        .lock()
        .unwrap()
        .first()
        .cloned()
        .unwrap_or_default();
    assert!(
        !first_request.iter().any(|m| matches!(
            m,
            Message::ToolResult { tool_call_id, .. } if tool_call_id == "call-1"
        )),
        "a delivered attempt must not be replayed into the next one: {first_request:?}"
    );
}

#[tokio::test]
async fn a_new_process_picks_up_where_the_interrupted_one_stopped() {
    let _guard = cog_core::progress::async_test_lock().await;
    cog_core::attempt_journal::reset_for_tests();

    let journal = Arc::new(MemoryJournal::default());
    cog_core::attempt_journal::install(journal.clone());

    // 上一次尝试死在工具里：助手那条已经落盘，结果没有。
    let agent_id = "squad-task-b-worker";
    journal
        .append(
            agent_id,
            &JournalEntry::message(
                JournalKind::User,
                Message::user("{\"task\":\"read a.rs\"}"),
                None,
                None,
            ),
        )
        .await
        .unwrap();
    journal
        .append(
            agent_id,
            &JournalEntry::message(
                JournalKind::Assistant,
                Message::assistant(vec![ContentBlock::ToolCall {
                    id: "call-1".into(),
                    name: "read_file".into(),
                    arguments: serde_json::json!({"path": "a.rs"}),
                    thought_signature: None,
                }]),
                None,
                None,
            ),
        )
        .await
        .unwrap();

    let runs = Arc::new(AtomicUsize::new(0));
    let llm = llm_with_call(
        cog_core::ToolCall {
            id: "never".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({}),
        },
        0,
    );
    let (mut rt, _events) = runtime(agent_id, "task-b", &[("read_file", runs.clone())]);
    rt.run(serde_json::json!({"task": "read a.rs"}), &llm)
        .await
        .expect("the resumed run finished");

    // 只读的调用被重跑了一次，而不是被跳过或交给模型。
    assert_eq!(
        runs.load(Ordering::Relaxed),
        1,
        "a read that had no recorded result is run again"
    );

    let seen = llm.seen.lock().unwrap().clone();
    let first = seen.first().expect("the model was asked").clone();
    assert!(
        first.iter().any(|m| matches!(
            m,
            Message::ToolResult { tool_call_id, .. } if tool_call_id == "call-1"
        )),
        "the interrupted call has a result before the model is asked again: {first:?}"
    );
    assert_eq!(
        llm.calls.load(Ordering::Relaxed),
        1,
        "the earlier turn is not bought again: the resumed run asks the model once"
    );
}

#[tokio::test]
async fn an_interrupted_call_with_side_effects_is_never_replayed() {
    let _guard = cog_core::progress::async_test_lock().await;
    cog_core::attempt_journal::reset_for_tests();

    let journal = Arc::new(MemoryJournal::default());
    cog_core::attempt_journal::install(journal.clone());

    let agent_id = "squad-task-c-worker";
    journal
        .append(
            agent_id,
            &JournalEntry::message(
                JournalKind::User,
                Message::user("{\"task\":\"commit\"}"),
                None,
                None,
            ),
        )
        .await
        .unwrap();
    journal
        .append(
            agent_id,
            &JournalEntry::message(
                JournalKind::Assistant,
                Message::assistant(vec![ContentBlock::ToolCall {
                    id: "call-1".into(),
                    name: "run_command".into(),
                    arguments: serde_json::json!({"command": "git commit -m x"}),
                    thought_signature: None,
                }]),
                None,
                None,
            ),
        )
        .await
        .unwrap();

    let runs = Arc::new(AtomicUsize::new(0));
    let llm = llm_with_call(
        cog_core::ToolCall {
            id: "never".into(),
            name: "run_command".into(),
            arguments: serde_json::json!({}),
        },
        0,
    );
    let (mut rt, _events) = runtime(agent_id, "task-c", &[("run_command", runs.clone())]);
    rt.run(serde_json::json!({"task": "commit"}), &llm)
        .await
        .expect("the resumed run finished");

    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "a call that may already have taken effect is not run a second time"
    );

    let seen = llm.seen.lock().unwrap().clone();
    let first = seen.first().expect("the model was asked").clone();
    let told = first.iter().any(|m| match m {
        Message::ToolResult {
            tool_call_id,
            content,
            ..
        } if tool_call_id == "call-1" => content
            .iter()
            .any(|b| matches!(b, ContentBlock::Text { text, .. } if text.contains("interrupted"))),
        _ => false,
    });
    assert!(
        told,
        "the model is told the call is in doubt instead of being left with a dangling call: {first:?}"
    );
}

#[tokio::test]
async fn a_run_told_to_stop_ends_at_the_turn_boundary_and_picks_up_there() {
    let _guard = cog_core::progress::async_test_lock().await;
    cog_core::attempt_journal::reset_for_tests();
    cog_agent::drain::reset_for_tests();

    let journal = Arc::new(MemoryJournal::default());
    cog_core::attempt_journal::install(journal.clone());

    // 上一轮已经完整落地：用户 → 助手（带调用）→ 结果。
    let agent_id = "squad-task-e-worker";
    for (kind, message, tool_call_id, tool) in [
        (
            JournalKind::User,
            Message::user("{\"task\":\"read a.rs\"}"),
            None,
            None,
        ),
        (
            JournalKind::Assistant,
            Message::assistant(vec![ContentBlock::ToolCall {
                id: "call-1".into(),
                name: "read_file".into(),
                arguments: serde_json::json!({"path": "a.rs"}),
                thought_signature: None,
            }]),
            None,
            None,
        ),
        (
            JournalKind::ToolResult,
            Message::tool_result_text("call-1", "read_file", "{\"text\":\"fn main\"}"),
            Some("call-1".to_string()),
            Some("read_file".to_string()),
        ),
    ] {
        journal
            .append(
                agent_id,
                &JournalEntry::message(kind, message, tool_call_id, tool),
            )
            .await
            .unwrap();
    }

    // 进程被要求停下。
    let signal = cog_core::ShutdownSignal::new();
    signal.trigger();
    cog_agent::drain::install(signal);

    let runs = Arc::new(AtomicUsize::new(0));
    let llm = llm_with_call(
        cog_core::ToolCall {
            id: "never".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({}),
        },
        0,
    );
    let (mut rt, _events) = runtime(agent_id, "task-e", &[("read_file", runs.clone())]);
    let stopped = rt.run(serde_json::json!({"task": "read a.rs"}), &llm).await;

    assert!(
        stopped.is_err(),
        "a run told to stop reports that it did not deliver"
    );
    assert_eq!(
        llm.calls.load(Ordering::Relaxed),
        0,
        "no model call is bought after the signal"
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "nothing is executed at the boundary"
    );
    assert_eq!(
        journal.kinds(agent_id).last(),
        Some(&JournalKind::ToolResult),
        "the attempt stays open at the last recorded step, with no duplicate request: {:?}",
        journal.kinds(agent_id)
    );

    // 闸放开：下一次派发从那里接上，不重跑已经记过的调用。
    cog_agent::drain::reset_for_tests();
    let llm2 = llm_with_call(
        cog_core::ToolCall {
            id: "never".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({}),
        },
        0,
    );
    let (mut rt2, _events2) = runtime(agent_id, "task-e", &[("read_file", runs.clone())]);
    rt2.run(serde_json::json!({"task": "read a.rs"}), &llm2)
        .await
        .expect("the resumed run finished");

    assert_eq!(
        llm2.calls.load(Ordering::Relaxed),
        1,
        "the resumed run asks the model once"
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "the call that already had a result is not run again"
    );
    let first = llm2
        .seen
        .lock()
        .unwrap()
        .first()
        .cloned()
        .expect("the model was asked");
    assert!(
        first.iter().any(|m| matches!(
            m,
            Message::ToolResult { tool_call_id, .. } if tool_call_id == "call-1"
        )),
        "the resumed conversation carries the recorded result: {first:?}"
    );
    assert_eq!(
        journal.kinds(agent_id).last(),
        Some(&JournalKind::Closed),
        "a delivered attempt closes its journal"
    );
}

#[tokio::test]
async fn a_journal_that_cannot_be_written_does_not_stop_the_run() {
    let _guard = cog_core::progress::async_test_lock().await;
    cog_core::attempt_journal::reset_for_tests();
    cog_agent::drain::reset_for_tests();

    struct Broken;
    #[async_trait]
    impl AttemptJournal for Broken {
        async fn append(&self, _agent_id: &str, _entry: &JournalEntry) -> Result<(), String> {
            Err("the database is down".into())
        }
        async fn load(&self, _agent_id: &str) -> Result<Vec<JournalEntry>, String> {
            Err("the database is down".into())
        }
    }
    cog_core::attempt_journal::install(Arc::new(Broken));

    let runs = Arc::new(AtomicUsize::new(0));
    let llm = llm_with_call(
        cog_core::ToolCall {
            id: "call-1".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path": "a.rs"}),
        },
        1,
    );
    let (mut rt, _events) = runtime(
        "squad-task-d-worker",
        "task-d",
        &[("read_file", runs.clone())],
    );
    rt.run(serde_json::json!({"task": "read it"}), &llm)
        .await
        .expect("the run finished");

    assert_eq!(
        runs.load(Ordering::Relaxed),
        1,
        "the run still did its work"
    );
    assert!(
        cog_core::attempt_journal::write_failures() > 0,
        "a journal that never writes is a reading, not a silence"
    );
}
