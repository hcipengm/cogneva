//! The inbox route can be asked for the answer, not only for the delivery.
//!
//! `POST /api/v1/agents/{id}/messages` answers `202` and nothing else: the
//! agent's own consumer runs the command and the answer, for a prompt sent
//! without `reply_stream`, reaches only the event plane. A human talking to a
//! named agent therefore has a one-way channel -- and the route refuses a
//! caller-named `reply_stream` for a reason it does not give up: the platform
//! cannot end a stream whose owner it does not know.
//!
//! `?wait_secs=N` is that reason turned into a feature. The route opens the
//! stream itself, puts its name on the prompt it delivers, reads the answer,
//! ends the stream, and answers with what came back. What is judged here is the
//! route's own behaviour at both ends of the wait:
//!
//! - an agent that answers inside the window gets its answer into the response;
//! - an agent that never answers gets a timeout, not a hang and not a 202;
//! - the same body without the parameter still answers `202`, so the wait is
//!   what produces the answer rather than something the route always does;
//! - a command that carries no answer is refused when a wait is asked for,
//!   rather than delaying an abort for seconds and then timing out.
//!
//! The agent is a stand-in reading the same stream the real one reads: the point
//! is the route's contract, and a real agent would add a model call in the
//! middle of it.

use crate::common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, SFResult};
use cog_gateway::{create_router, GatewayState};

use crate::router_state;

const AGENT: &str = "probe-agent";
const ROUTE: &str = "/api/v1/agents/probe-agent/messages";

/// A roster that holds exactly one name.
struct OneAgent;

#[async_trait::async_trait]
impl AgentRegistry for OneAgent {
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
        Ok((agent_id == AGENT).then(|| common::mocks::registration(AGENT)))
    }
    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(vec![common::mocks::registration(AGENT)])
    }
    async fn list_by_role(&self, _role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(vec![common::mocks::registration(AGENT)])
    }
    async fn list_by_capability(&self, _capability: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(vec![common::mocks::registration(AGENT)])
    }
}

fn state(backend: Arc<dyn MessageBackend>) -> Arc<GatewayState> {
    router_state::with_messaging(backend, Arc::new(OneAgent))
}

async fn post(app: &axum::Router, token: &str, uri: &str, body: &str) -> (StatusCode, String) {
    let request = Request::builder()
        .method("POST")
        .uri(uri)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body comes back");
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Stand in for the agent's consumer: read one command off the inbox, and if it
/// asks for a reply, publish `answer` where it says.
async fn answer_the_next_command(backend: Arc<dyn MessageBackend>, answer: serde_json::Value) {
    let inbox = cog_core::agent_inbox_stream(AGENT);
    backend
        .create_consumer_group(&inbox, "probe-consumer")
        .await
        .expect("the stand-in can open its own group");
    let mut stream = backend
        .subscribe(&inbox, "probe-consumer")
        .await
        .expect("the stand-in reads the inbox");
    let (id, bytes) = futures::StreamExt::next(&mut stream)
        .await
        .expect("one command arrives")
        .expect("the command is readable");
    backend
        .ack(&inbox, "probe-consumer", std::slice::from_ref(&id))
        .await
        .expect("the command is acked");
    match serde_json::from_slice::<InboxMessage>(&bytes).expect("the command is the wire form") {
        InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => {
            backend
                .publish(&stream, &serde_json::to_vec(&answer).unwrap())
                .await
                .expect("the answer goes onto the stream the command named");
        }
        other => panic!("expected a prompt naming a reply stream, got {other:?}"),
    }
}

#[tokio::test]
async fn an_agent_that_answers_inside_the_window_answers_the_request() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "ask-probe").await;

    tokio::spawn(answer_the_next_command(
        backend.clone(),
        serde_json::json!({"answer": 42}),
    ));

    let (status, body) = post(
        &app,
        &token,
        &format!("{ROUTE}?wait_secs=5"),
        r#"{"type":"prompt","goal":{"objective":"what is the answer"}}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the request waited and answered: {body}"
    );
    let reply: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
    assert_eq!(reply["replied"], serde_json::json!(true), "{body}");
    assert_eq!(
        reply["reply"]["answer"],
        serde_json::json!(42),
        "the agent's answer is what comes back, not a summary of it: {body}"
    );
}

#[tokio::test]
async fn an_agent_that_never_answers_times_out_instead_of_hanging() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend);
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "ask-timeout-probe").await;

    let started = std::time::Instant::now();
    let (status, body) = post(
        &app,
        &token,
        &format!("{ROUTE}?wait_secs=1"),
        r#"{"type":"prompt","goal":{"objective":"nobody listens"}}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::GATEWAY_TIMEOUT,
        "no answer inside the window is a timeout the caller can see: {body}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the wait ends at its own deadline, not later: {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn without_the_parameter_the_route_still_only_queues() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend);
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "ask-control").await;

    let (status, body) = post(
        &app,
        &token,
        ROUTE,
        r#"{"type":"prompt","goal":{"objective":"fire and forget"}}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "the wait is what produces an answer; without it the route delivers and returns: {body}"
    );
}

#[tokio::test]
async fn a_command_with_no_answer_is_refused_a_wait() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend);
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "ask-shape-probe").await;

    let (status, body) = post(
        &app,
        &token,
        &format!("{ROUTE}?wait_secs=5"),
        r#"{"type":"abort"}"#,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an abort carries no answer, so waiting for one would only delay it: {body}"
    );
    assert!(
        body.contains("prompt"),
        "the refusal names the commands that do answer: {body}"
    );
}

/// 说话的人是平台的属主或成员，不是管理员。
///
/// 这条路一度只在管理员组：属主（Owner）读得到名册，却对册上任何一个人都说不上
/// 一句话——工作台里那些对话框于是只对超级管理员开着。人与智能体是平的，说一句话
/// 不改变在岗名单，不需要管理员；注册、杀掉、重启才需要，它们仍在管理员组。
///
/// 判据两头都要有：成员能进到投递那一步（这里是 `202`，话送出去了），访客仍在门外
/// （`403`）。只断言前一半，一条谁都能过的路由也能通过。
#[tokio::test]
async fn a_member_may_speak_to_an_agent_and_a_visitor_may_not() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend);
    let app = create_router(state.clone());
    let body = r#"{"type":"prompt","goal":{"objective":"what are you working on"}}"#;

    let member = router_state::member_bearer(&state, "member-probe").await;
    let (status, detail) = post(&app, &member, ROUTE, body).await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "a signed-in member may hand a prompt to a live agent: {detail}"
    );

    let visitor = router_state::visitor_bearer(&state, "visitor-probe").await;
    let (status, detail) = post(&app, &visitor, ROUTE, body).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a visitor stays below the routes that act on the platform: {detail}"
    );
}

/// 招人这条路由：属主与成员能用，访客不能，而且「这里招不了」要说出原因。
///
/// 判据取的是 503 而不是 201：这个夹具的进程里没有员工池，也还没有配模型，
/// 所以一个真的能建的部署与这里的差别本来就是这一格。要判的是**这条路对谁说
/// 话**——成员穿过了鉴权层走到处理函数，访客被挡在门外；把两者都断言一遍，一条
/// 谁都能过的路由过不了这个测试。
#[tokio::test]
async fn a_member_may_hire_an_employee_and_a_visitor_may_not() {
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = state(backend);
    let app = create_router(state.clone());

    let member = router_state::member_bearer(&state, "hirer-probe").await;
    let (status, detail) = post(&app, &member, "/api/v1/agents", "{}").await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "a member reaches the hiring route; this process has no pool, which is a sentence of its own: {detail}"
    );
    assert!(
        detail.contains("pool"),
        "the refusal says why nobody can be hired here: {detail}"
    );

    let visitor = router_state::visitor_bearer(&state, "visitor-probe").await;
    let (status, detail) = post(&app, &visitor, "/api/v1/agents", "{}").await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a visitor stays below the routes that add to the roster: {detail}"
    );
}
