//! 说一句话不按角色挑人：名册上任意一行都收得到。
//!
//! 属主 2026-11-20 看员工页时问：「employee、chat 为啥会出现？哪个不是 employee
//! 那个不能 chat，这是智能体通用的」。查证的结果是：实现里根本没有按角色拦消息
//! 的判据，名册上任何 id 都能收消息——但也没有一条判据钉住「不许把它加回来」。
//! 名册上的角色是这一段活里的职务分工，不是一张准发名单；一旦某条路由按角色分岔，
//! 「给 planner 发得出去、给 external-worker 发不出去」在屏幕上与投递失败同形，
//! 而修的人只会去查网络。
//!
//! 判据：名册上每种角色的行各挑一行，戴上 `?wait_secs=` 发一句话，每一行都要把
//! 回答带回来。今天只有一条投递路径，明天有人加第二条（工具、内部通道）时，同样
//! 的问题仍要在这里先变成红。

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{
    AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, ResourceInfo, SFResult,
};
use cog_gateway::{create_router, GatewayState};

use crate::router_state;

fn every_roster_row() -> Vec<AgentRegistration> {
    // 人的身份词加小队／带外单元的角色名，各一行：这就是名册上可能出现的全部
    // 角色。加一个新的角色名却没有在这里出现，说明有人绕开了 `roles::ALL`。
    let roles: Vec<&str> = std::iter::once(cog_gateway::agents::EMPLOYEE_ROLE)
        .chain(cog_core::roles::ALL.iter().copied())
        .collect();
    roles
        .into_iter()
        .map(|role| {
            let id = format!("{role}-probe");
            AgentRegistration::new(
                id,
                "probe-host",
                "127.0.0.1",
                role,
                "default",
                ResourceInfo::default(),
            )
        })
        .collect()
}

/// 一份装着每种角色各一行的名册。
struct EveryRole {
    rows: Vec<AgentRegistration>,
}

#[async_trait::async_trait]
impl AgentRegistry for EveryRole {
    async fn register(&self, _registration: &AgentRegistration) -> SFResult<()> {
        Ok(())
    }
    async fn heartbeat(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }
    async fn deregister(&self, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }
    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>> {
        Ok(self
            .rows
            .iter()
            .find(|row| row.agent_id == agent_id)
            .cloned())
    }
    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(self.rows.clone())
    }
    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .rows
            .iter()
            .filter(|row| row.role == role)
            .cloned()
            .collect())
    }
}

fn state(backend: Arc<dyn MessageBackend>) -> Arc<GatewayState> {
    router_state::with_messaging(
        backend,
        Arc::new(EveryRole {
            rows: every_roster_row(),
        }),
    )
}

/// 站在那个员工的位置读一条命令：要回答的，就把回答写到它点名的流上。
async fn answer_the_next_command(backend: Arc<dyn MessageBackend>, agent_id: String) {
    let inbox = cog_core::agent_inbox_stream(&agent_id);
    backend
        .create_consumer_group(&inbox, "role-probe-consumer")
        .await
        .expect("the stand-in can open its own group");
    let mut stream = backend
        .subscribe(&inbox, "role-probe-consumer")
        .await
        .expect("the stand-in reads the inbox");
    let (id, bytes) = futures::StreamExt::next(&mut stream)
        .await
        .expect("one command arrives")
        .expect("the command is readable");
    backend
        .ack(&inbox, "role-probe-consumer", std::slice::from_ref(&id))
        .await
        .expect("the command is acked");
    match serde_json::from_slice::<InboxMessage>(&bytes).expect("the command is the wire form") {
        InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => {
            backend
                .publish(&stream, br#"{"answer":"heard"}"#)
                .await
                .expect("the answer goes onto the stream the command named");
        }
        other => panic!("expected a prompt naming a reply stream, got {other:?}"),
    }
}

#[tokio::test]
async fn every_row_on_the_roster_answers_a_prompt_whatever_its_role() {
    let rows = every_roster_row();
    assert!(
        rows.len() >= 2,
        "闭集里至少要有「一个人」与「一个座位」两种角色，否则这条判据什么都没量到"
    );
    for row in &rows {
        let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
        let state = state(backend.clone());
        let app = create_router(state.clone());
        let token = router_state::admin_bearer(&state, "role-probe").await;

        tokio::spawn(answer_the_next_command(
            backend.clone(),
            row.agent_id.clone(),
        ));

        let request = Request::builder()
            .method("POST")
            .uri(format!(
                "/api/v1/agents/{}/messages?wait_secs=5",
                row.agent_id
            ))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::from(
                r#"{"type":"prompt","goal":{"objective":"are you there"}}"#.to_string(),
            ))
            .expect("the request is well formed");
        let response = app
            .oneshot(request)
            .await
            .expect("the router answers a request");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("the body comes back");
        let body = String::from_utf8_lossy(&bytes).to_string();
        assert_eq!(
            status,
            StatusCode::OK,
            "角色「{}」的那一行没收到话：任何路径都不许按 role 拦消息（{body}）",
            row.role
        );
        let reply: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
        assert_eq!(
            reply["reply"]["answer"],
            serde_json::json!("heard"),
            "角色「{}」那一条拿到的是它自己的回答（{body}）",
            row.role
        );
    }
}
