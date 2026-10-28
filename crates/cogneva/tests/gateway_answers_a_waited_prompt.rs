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

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, SFResult};
use cog_gateway::{create_router, GatewayState};

mod router_state;

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
