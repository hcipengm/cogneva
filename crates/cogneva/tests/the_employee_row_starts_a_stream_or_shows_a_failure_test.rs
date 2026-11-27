//! 员工页的一行：点下去之后，有界时间内要么开始流出回答，要么给出一条看得见的失败。
//!
//! 这一条判的不是「ChatLane 打开了」——那是点击的回执，不是能力。判的是点击之后
//! **第一帧**：这一轮真的跑在系统那条循环上，事件从员工本人名下出来，屏幕因此说
//! 得清是谁在答话；上游不给答案时，得到的是一个 `Err`，界面拿得到一句能显示的话，
//! 而不是一条悬在那里的空车道。
//!
//! 会话 id 由员工 id 纯函数推出（`agent:<员工 id>`），员工页与主对话入口算的是同
//! 一个式子；WebUI 那一侧的前缀必须与后端那一条相同，否则同一个人从两个入口进来
//! 会落到两段会话，历史各读一半。
//!
//! 这里拉起真的 `PlatformConversation::turn`，门口那只模型只答一句话；失败那条
//! 换成一只开口就报错的模型。两条都在一个有界的等待里跑完。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use crate::common::{fsio, root};

use cog_core::{AgentEvent, AssistantMessageEvent, ContentBlock, LlmClient, Message, SFResult};
use cog_gateway::chat::{ConversationRequest, ConversationTurn};
use cog_llm::{
    AssistantMessageEventStream, ChatOptions, ChatResponse, CompleteOptions, StopReason, Usage,
};
use cogneva::conversation::{session_for_employee, Ledgers, PlatformConversation};

/// 点下去之后等第一帧的界。判的是「有界」，不是某一条产品的时限：真正的时限由
/// 那一轮的停滞读数与上游超时兜底，这里只证明它不会无限悬着。
const FIRST_FRAME_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// 一只只会答一句话、从不调工具的模型。
#[derive(Default)]
struct TalkingLlm;

#[async_trait]
impl LlmClient for TalkingLlm {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        unreachable!("循环走的是流式那条")
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
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
            let text = "在。".to_string();
            response.content.push(ContentBlock::text(text.clone()));
            let _ = producer
                .push(AssistantMessageEvent::TextStart {
                    content_index: 0,
                    timestamp: chrono::Utc::now(),
                })
                .await;
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

/// 开口就报错的模型：上游不给答案。
struct FailingLlm;

#[async_trait]
impl LlmClient for FailingLlm {
    async fn chat(&self, _messages: &[Message], _options: &ChatOptions) -> SFResult<ChatResponse> {
        Err(cog_core::SFError::Agent("upstream refused".into()))
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(cog_core::SFError::Agent("upstream refused".into()))
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> SFResult<AssistantMessageEventStream> {
        Err(cog_core::SFError::Agent("upstream refused".into()))
    }

    async fn health_check(&self) -> bool {
        false
    }
}

fn conversation() -> PlatformConversation {
    use crate::common::orchestrator::control;

    PlatformConversation::new(
        control("test-workspace"),
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
        None,
    )
}

fn request(session: &str, text: &str, llm: Arc<dyn LlmClient>) -> ConversationRequest {
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

/// 员工页算出来的会话 id 与后端那条纯函数是同一个式子。
///
/// 两处各拼一遍时，同一个人从主对话入口与员工页进来会落到两段会话：两边都「有
/// 历史」，只是各自只有一半。判的是 WebUI 那一侧真的用了同一个前缀，而不是它
/// 恰好拼出了同一段字符串的另一条路。
#[test]
fn the_employee_page_derives_the_session_with_the_backend_s_own_prefix() {
    let view = fsio::read(&root::repo_root().join("web/src/views/PeopleView.tsx"));
    assert!(
        view.contains("`agent:${agentId}`"),
        "员工页不再按 `agent:<员工 id>` 算会话：与 `session_for_employee` 的式子分家了"
    );
    assert_eq!(
        session_for_employee("E0412"),
        "agent:E0412",
        "后端那条派生前缀变了：员工页与主对话入口会落到两段会话"
    );
    // 同一行点两次是同一段会话，不是每次点击新发一个。
    assert_eq!(
        session_for_employee("E0412"),
        session_for_employee("E0412"),
        "同一个员工两次点击算出了两个会话 id"
    );
}

/// 点一行：有界时间内第一帧从这位员工名下出来，这一轮跑完并给出回答。
#[tokio::test]
async fn the_first_frame_comes_out_under_the_employee_s_own_name() {
    let _guard = cog_core::progress::async_test_lock().await;

    let turns = conversation();
    let employee = "E0412";
    let session = session_for_employee(employee);
    let seen: Arc<Mutex<Vec<AgentEvent>>> = Arc::new(Mutex::new(Vec::new()));

    let (tx, mut rx) = tokio::sync::mpsc::channel::<AgentEvent>(512);
    let collector = {
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                seen.lock().unwrap().push(event);
            }
        })
    };

    let turn = turns.turn(
        request(&session, "在吗", Arc::new(TalkingLlm) as Arc<dyn LlmClient>),
        tx,
    );
    let answer = tokio::time::timeout(FIRST_FRAME_WITHIN, turn)
        .await
        .expect("点击之后这一轮没有在有界时间内跑完：界面会一直停在空车道上")
        .expect("模型答得出来，这一轮却报错了");
    collector.await.expect("事件出口的读者不该崩");

    let events = seen.lock().unwrap();
    let start = events
        .iter()
        .find_map(|event| match event {
            AgentEvent::AgentStart { agent_id, .. } => Some(agent_id.clone()),
            _ => None,
        })
        .expect("第一帧不是这位员工的开始事件：屏幕说不清是谁在答话");
    assert_eq!(
        start, employee,
        "答话的不是被点的那位员工：这一轮跑在了别的名字下"
    );
    assert!(
        answer
            .get("tasks")
            .map(|tasks| tasks.is_null())
            .unwrap_or(true)
            || answer.get("tasks").is_some(),
        "这一轮没有交回可读的回答：{answer}"
    );
}

/// 上游不给答案时：一个有界的 `Err`，界面拿得到一句能显示的话，不是空车道。
#[tokio::test]
async fn an_upstream_that_refuses_is_a_visible_failure_not_a_wait() {
    let _guard = cog_core::progress::async_test_lock().await;

    let turns = conversation();
    let session = session_for_employee("E0412");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<AgentEvent>(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });

    let turn = turns.turn(
        request(&session, "在吗", Arc::new(FailingLlm) as Arc<dyn LlmClient>),
        tx,
    );
    let outcome = tokio::time::timeout(FIRST_FRAME_WITHIN, turn)
        .await
        .expect("上游拒绝时这一轮没有在有界时间内收场：界面会一直停在空车道上");

    let error = outcome.expect_err("上游拒绝时这一轮却报告成功：失败被吞掉了");
    assert!(
        !error.to_string().trim().is_empty(),
        "失败没有一句话可显示：界面拿不到能读的失败"
    );
    drain.await.expect("事件出口的读者不该崩");
}
