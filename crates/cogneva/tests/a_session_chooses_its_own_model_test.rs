//! 一段对话换模型：选的是这一段，下一轮真的用上它。
//!
//! 体验要的是「同一个页面里，这一段的模型由我说了算」——不是整台部署换上游（那是
//! 设置页的事），也不是每句话带一个模型名（那会把一段对话拆成几种口径）。落点是
//! **这一段对话**：选择按「这个人 + 这一段」记下来，下一轮发请求时带上它。
//!
//! 判据四处，缺一处这条链就断在别的地方：
//!
//! 1. **记得住、只属于这一段**：选了读得回来，换一段 / 换一个人读不到，改回默认
//!    之后读作「没选过」；
//! 2. **没地方记就说出来**：部署没接键值存储时，换模型是 `Err`（界面要说「没换成」），
//!    读是「没选过」（这一轮走上游配置的那一只）；
//! 3. **真的用上**：循环拿到的每一发请求上带着这一轮选的模型；
//! 4. **清单说了算**：能选的那几只来自平台已配置的那一份；清单之外的一律不收，
//!    空清单是「还没配」而不是一只默认型号。
//!
//! 第 3 条在本文件里拉起真的循环、读模型收到的请求；第 4 条读协议与 handler 的
//! 接线（套接字这条路没有在测试里起服务器，判的是那一支写了什么）。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::common::kv::MemoryStore;

use cog_agent::AgentRuntime;
use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, KeyValueStore, LlmClient, Message,
    RuntimeConfig, SFResult,
};
use cog_gateway::chat::ConversationTurn;
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use cogneva::conversation::{Ledgers, PlatformConversation};

/// 装配层那一段对话：只接上「说过话的人」这一格（选择存在那里），其余照实缺席。
fn conversation(people: Option<Arc<dyn KeyValueStore>>) -> PlatformConversation {
    use crate::common::orchestrator::control;

    PlatformConversation::new(
        control("test-workspace"),
        None,
        None,
        None,
        None,
        None,
        people,
        Ledgers::default(),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

/// 选了就记住，读得回来的是同一只，而且只属于这一段、这个人。
#[tokio::test]
async fn a_choice_is_remembered_by_the_conversation_it_was_made_in() {
    let kv: Arc<dyn KeyValueStore> = Arc::new(MemoryStore::default());
    let turns = conversation(Some(kv));

    assert_eq!(
        turns.chosen_model("alice", "s-1").await,
        Ok(None),
        "没选过时读回的是「没选过」，不是一只被替她挑好的模型"
    );
    assert_eq!(
        turns.choose_model("alice", "s-1", Some("deep-model")).await,
        Ok(Some("deep-model".into())),
        "换完之后要立刻读得到这一格"
    );
    assert_eq!(
        turns.chosen_model("alice", "s-1").await,
        Ok(Some("deep-model".into())),
        "下一轮读到的不是换过的那一只：选了个寂寞"
    );
    assert_eq!(
        turns.chosen_model("alice", "s-2").await,
        Ok(None),
        "同一个人另一段对话也跟着换了：换的是整台部署，不是这一段"
    );
    assert_eq!(
        turns.chosen_model("bob", "s-1").await,
        Ok(None),
        "别人拿同一个会话号读到了她选的模型"
    );
    assert_eq!(
        turns.choose_model("alice", "s-1", None).await,
        Ok(None),
        "改回默认之后这一格应当读作没选过"
    );
    assert_eq!(turns.chosen_model("alice", "s-1").await, Ok(None));
}

/// 没有地方记的时候说「没换成」，不静默成功——否则界面显示一次没发生的切换。
#[tokio::test]
async fn a_deployment_with_no_store_says_the_choice_cannot_be_kept() {
    let turns = conversation(None);
    assert_eq!(
        turns.chosen_model("alice", "s-1").await,
        Ok(None),
        "没接存储时这一轮走上游配置的那一只，不是一条读失败"
    );
    let refused = turns.choose_model("alice", "s-1", Some("deep-model")).await;
    assert!(
        refused.is_err(),
        "没接存储却答「换好了」：界面会显示一次下一轮就失效的切换"
    );
}

// ── 一只记住「请求上带的模型」的上游 ───────────────────────────────────────

struct RecordingLlm {
    seen: Mutex<Vec<Option<String>>>,
}

#[async_trait]
impl LlmClient for RecordingLlm {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unreachable!("循环走的是流式那条")
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        self.seen.lock().unwrap().push(options.model.clone());
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
            let text = "好了".to_string();
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

/// 造一个循环，连同它的事件出口一起交回（出口要活着，序章发不出去整次运行就失败）。
fn runtime(name: &str) -> (AgentRuntime, tokio::sync::mpsc::Receiver<AgentEvent>) {
    let (tx, rx) = tokio::sync::mpsc::channel::<AgentEvent>(256);
    let agent = AgentRuntime::new(
        RuntimeConfig {
            stated_directions: Vec::new(),
            agent_id: name.into(),
            role: "worker".to_string(),
            max_iterations: 2,
            context_window_size: Some(60_000),
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
            skill_config: None,
            crew_id: None,
            squad_id: None,
            owner_task_id: None,
            deliverable: cog_core::Deliverable::Unstated,
            deliverable_state: cog_core::DeclarationState::Unstated,
        },
        tx,
    )
    .with_tools(cog_agent::ToolRegistry::new());
    (agent, rx)
}

/// 这一段选的模型挂在这一轮的每一发请求上；没选过时不替它编一只。
#[tokio::test]
async fn the_model_chosen_for_the_session_is_what_the_request_carries() {
    let _guard = cog_core::progress::async_test_lock().await;

    let llm = RecordingLlm {
        seen: Mutex::new(Vec::new()),
    };
    let (mut agent, _events) = runtime("session-model-chooser");
    agent = agent.with_model(Some("deep-model".into()));
    agent
        .run(serde_json::json!({"task": "说一句"}), &llm)
        .await
        .expect("这一轮跑完");
    assert_eq!(
        llm.seen.lock().unwrap().first().cloned().flatten(),
        Some("deep-model".to_string()),
        "选了模型却没挂到请求上：上游按自己配置的那一只答，界面上的选择是假的"
    );

    let unset = RecordingLlm {
        seen: Mutex::new(Vec::new()),
    };
    let (mut agent, _events) = runtime("session-model-default");
    agent = agent.with_model(None);
    agent
        .run(serde_json::json!({"task": "说一句"}), &unset)
        .await
        .expect("这一轮跑完");
    assert_eq!(
        unset.seen.lock().unwrap().first().cloned().flatten(),
        None,
        "没选过时替人挑了一只：下一轮用的就不再是平台配置的那一只"
    );

    // 空串也是「没选过」：界面上清空下拉框与「改回默认」是同一个动作。
    let (mut agent, _events) = runtime("session-model-blank");
    let blank = RecordingLlm {
        seen: Mutex::new(Vec::new()),
    };
    agent = agent.with_model(Some("   ".into()));
    agent
        .run(serde_json::json!({"task": "说一句"}), &blank)
        .await
        .expect("这一轮跑完");
    assert_eq!(blank.seen.lock().unwrap().first().cloned().flatten(), None);
}

/// 一次往返办两件事：不带 `model` 是读，空串是改回默认。
#[test]
fn one_frame_reads_the_choice_or_changes_it() {
    use cog_gateway::websocket_protocol::{ClientMessage, ServerMessage};

    let read: ClientMessage =
        serde_json::from_str(r#"{"type":"chat_model","session_id":"s-1"}"#).expect("读的帧");
    match read {
        ClientMessage::ChatModel {
            session_id, model, ..
        } => {
            assert_eq!(session_id.as_deref(), Some("s-1"));
            assert_eq!(model, None, "不带 model 就是只读");
        }
        other => panic!("`chat_model` 解析成了别的：{other:?}"),
    }

    let revert: ClientMessage =
        serde_json::from_str(r#"{"type":"chat_model","session_id":"s-1","model":""}"#)
            .expect("改回默认的帧");
    match revert {
        ClientMessage::ChatModel { model, .. } => {
            assert_eq!(model.as_deref(), Some(""), "空串是改回默认，不是「没说话」");
        }
        other => panic!("`chat_model` 解析成了别的：{other:?}"),
    }

    let answer = serde_json::to_value(ServerMessage::ChatModel {
        session_id: "s-1".into(),
        model: Some("deep-model".into()),
        models: vec!["deep-model".into(), "fast-model".into()],
        configured: true,
        reason: None,
    })
    .expect("回执是 JSON");
    assert_eq!(answer["type"], "chat_model");
    assert_eq!(answer["model"], "deep-model");
    assert_eq!(answer["models"][1], "fast-model");
    assert_eq!(answer["configured"], true);

    // 空清单要说得出来：`configured: false` + 一句为什么，而不是一只默认型号。
    let empty = serde_json::to_value(ServerMessage::ChatModel {
        session_id: "s-1".into(),
        model: None,
        models: Vec::new(),
        configured: false,
        reason: Some("平台还没有配置任何模型".into()),
    })
    .expect("回执是 JSON");
    assert_eq!(empty["configured"], false);
    assert!(empty["reason"].as_str().unwrap().contains("还没有配置"));
    assert!(
        empty.get("model").is_none(),
        "没选过时不该有一只被挑好的模型"
    );
}

/// 接线：这一轮开跑前先读这一段选的那只，清单外的名字不收。
#[test]
fn the_turn_reads_the_choice_before_it_starts() {
    use crate::common::routes::read_repo;

    let chat = read_repo("crates/cog-gateway/src/chat.rs");
    let turn = chat
        .split_once("pub async fn run_chat_turn(")
        .unwrap_or_else(|| panic!("chat.rs 里找不到 run_chat_turn"))
        .1;
    let read = turn
        .find("conversation.chosen_model(")
        .unwrap_or_else(|| panic!("这一轮没有读这一段选的模型：界面上的选择到不了上游"));
    // 拿上游客户端之前先读好：读失败也要让这一轮跑下去（走上游配置的那一只），
    // 而不是在读不到选择的这一轮里说「模型不可用」。
    let client = turn
        .find(".llm_client")
        .unwrap_or_else(|| panic!("这一轮没有拿上游客户端？这条判据读的那一段变了"));
    assert!(read < client, "选模型要在拿上游客户端之前读出来");
    assert!(
        turn.contains("model: chosen_model.clone()"),
        "读到了却没交给这一轮：上游仍按自己配置的那一只答"
    );

    let selected = read_repo("crates/cogneva/src/conversation.rs");
    assert!(
        selected.contains("let mut runtime = runtime.with_model(request.model.clone());"),
        "这一轮选的模型没有挂到循环的请求上"
    );

    let socket = read_repo("crates/cog-gateway/src/websocket.rs");
    let arm = socket
        .split_once("ClientMessage::ChatModel {")
        .unwrap_or_else(|| panic!("websocket.rs 里没有 chat_model 这一支"))
        .1;
    let arm = arm
        .split_once("ClientMessage::ChatResume {")
        .map(|(arm, _)| arm)
        .unwrap_or(arm);
    let gate = arm
        .find("crate::sessions::speaks_here(")
        .unwrap_or_else(|| panic!("换模型没判归属：猜一个会话号就能改别人那一段的模型"));
    let grep = arm
        .find("catalogue.offers(target)")
        .unwrap_or_else(|| panic!("清单之外的模型也收：下一轮会用一只平台没配的模型发请求"));
    let write = arm
        .find(".choose_model(")
        .unwrap_or_else(|| panic!("这一支从来没有写下去过"));
    assert!(gate < write, "先判这一段能不能由这个人改，再写");
    assert!(grep < write, "先对清单，再写");
    assert!(
        arm.contains("catalogue.models.join"),
        "拒绝的时候要念出能选的那几只：只说「不行」等于让人自己猜"
    );

    let models = read_repo("crates/cog-gateway/src/chat_models.rs");
    assert!(
        models.contains("crate::llm_admin::forwarded_llm_status()"),
        "清单不是从平台已配置的那一份读的：内置型号表会在配置换掉之后继续列出调不通的模型"
    );
    assert!(
        models.contains("fn empty(reason"),
        "空清单没有一句「还没配」：界面只能画一个空下拉框"
    );
}

/// 界面：下拉框的选项来自服务端那份清单，空清单画的是原因而不是一个空框。
#[test]
fn the_page_draws_the_choice_from_the_configured_catalogue() {
    use crate::common::routes::read_repo;

    let pane = read_repo("web/src/conversation/ConversationPane.tsx");
    assert!(
        pane.contains("type: 'chat_model'"),
        "对话头没有问过/换过模型：这一条的后端接好了，人却没有入口"
    );
    assert!(
        pane.contains("modelCatalogue.models.map("),
        "下拉框的选项不是那份清单：写一张内置型号表会在配置换掉之后列出调不通的模型"
    );
    assert!(
        pane.contains("modelCatalogue.reason"),
        "清单为空时没写为什么：一个空下拉框里看不出是「还没配」还是「读不到」"
    );
    assert!(
        pane.contains("modelCatalogue !== null"),
        "还没问过就画了「没有可选的模型」：连上之前先说平台没配模型"
    );

    let hook = read_repo("web/src/hooks/useEventStream.ts");
    assert!(
        hook.contains("onChatModel: ingestChatModel"),
        "服务端答回来的清单没有落到 store 里"
    );

    let transport = read_repo("web/src/transport/websocket.ts");
    assert!(
        transport.contains("message.type === 'chat_model'"),
        "套接字收到了 `chat_model` 却没有交给页面"
    );
}
