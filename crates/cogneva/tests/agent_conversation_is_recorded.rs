//! What is said to an agent survives the response that carried it.
//!
//! Sending a command to a named agent used to leave two artifacts: bytes on the
//! agent's inbox stream, and an HTTP response in whatever terminal the caller
//! happened to be reading. Neither is a conversation. The stream is read and
//! acknowledged by the consumer, and the response scrolls away, so the same
//! person asking "what did I tell this agent, and did it answer" has no place
//! to look -- and the panel that is supposed to show it has nothing to read.
//!
//! The route now writes each settled send down and serves the conversation back
//! at the same path it is sent to. What is judged here is the pair:
//!
//! - a send that asks for no answer is recorded as queued, with the words that
//!   were said and the name they were said to;
//! - a send whose wait expires is recorded as timed out, not as an answer;
//! - the conversation route returns what the send route wrote, so the two are
//!   one feature rather than two halves that could disagree.
//!
//! The transcript is an in-memory stand-in: the SQL is judged against a real
//! PostgreSQL in `cog-storage`'s own suite, and what this suite is about is
//! whether the route writes at all and reads back what it wrote.

use crate::common;

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use cog_core::{AgentRegistration, AgentRegistry, SFResult};
use cog_gateway::{create_router, GatewayState};
use cog_storage::{AgentMessage, AgentMessageStore, NewAgentMessage, SendOutcome};

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

/// The transcript, kept in the test's own memory so it can be read back whole.
#[derive(Default)]
struct RecordingTranscript {
    lines: Mutex<Vec<AgentMessage>>,
}

#[async_trait::async_trait]
impl AgentMessageStore for RecordingTranscript {
    async fn record(&self, message: &NewAgentMessage<'_>) -> SFResult<()> {
        let mut lines = self
            .lines
            .lock()
            .expect("the transcript lock is not poisoned");
        let id = lines.len() as i64 + 1;
        lines.push(AgentMessage {
            id,
            from_unit: message.from_unit.to_string(),
            to_unit: message.to_unit.to_string(),
            kind: message.kind.to_string(),
            body: message.body.to_string(),
            outcome: message.outcome,
            reply: message.reply.map(str::to_string),
            outcome_detail: message.outcome_detail.map(str::to_string),
            created_at: chrono::Utc::now(),
        });
        Ok(())
    }

    async fn conversation(&self, peer: &str, limit: usize) -> SFResult<Vec<AgentMessage>> {
        let lines = self
            .lines
            .lock()
            .expect("the transcript lock is not poisoned");
        Ok(lines
            .iter()
            .filter(|line| line.to_unit == peer || line.from_unit == peer)
            .take(limit)
            .cloned()
            .collect())
    }
}

fn state(transcript: Arc<RecordingTranscript>) -> Arc<GatewayState> {
    let backend: Arc<dyn cog_core::MessageBackend> =
        Arc::new(cog_stream::MemoryMessageBackend::new());
    router_state::with_messaging_and_transcript(backend, Arc::new(OneAgent), transcript)
}

async fn call(
    app: &axum::Router,
    method: &str,
    token: &str,
    uri: &str,
    body: Option<&str>,
) -> (StatusCode, String) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"));
    if body.is_some() {
        request = request.header("Content-Type", "application/json");
    }
    let request = request
        .body(Body::from(body.unwrap_or("").to_string()))
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

/// A delivered command that asks for nothing back is still something that was
/// said, and the record has to name both ends and the words.
#[tokio::test]
async fn a_queued_command_is_written_down_with_what_was_said() {
    let transcript = Arc::new(RecordingTranscript::default());
    let state = state(transcript.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-queue-probe").await;

    let (status, body) = call(
        &app,
        "POST",
        &token,
        ROUTE,
        Some(r#"{"type":"prompt","goal":{"objective":"run the tests"}}"#),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::ACCEPTED,
        "delivery is unchanged: {body}"
    );

    let lines = transcript.lines.lock().expect("lock").clone();
    assert_eq!(
        lines.len(),
        1,
        "a delivered command leaves exactly one line: {lines:?}"
    );
    assert_eq!(lines[0].to_unit, AGENT);
    assert_eq!(lines[0].kind, "prompt");
    assert_eq!(lines[0].outcome, SendOutcome::Queued);
    assert!(
        lines[0].body.contains("run the tests"),
        "the words that were said are the point of the transcript: {:?}",
        lines[0].body
    );
    assert!(
        !lines[0].from_unit.trim().is_empty(),
        "a line with no sender cannot be read back as a conversation: {lines:?}"
    );
}

/// A wait that expires is an outcome of the send, not a lost send: the line is
/// written, and it says no answer came rather than pretending one did.
#[tokio::test]
async fn a_command_that_was_never_answered_is_recorded_as_timed_out() {
    let transcript = Arc::new(RecordingTranscript::default());
    let state = state(transcript.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-timeout-probe").await;

    let (status, body) = call(
        &app,
        "POST",
        &token,
        &format!("{ROUTE}?wait_secs=1"),
        Some(r#"{"type":"prompt","goal":{"objective":"nobody listens"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{body}");

    let lines = transcript.lines.lock().expect("lock").clone();
    assert_eq!(lines.len(), 1, "the send is recorded once: {lines:?}");
    assert_eq!(lines[0].outcome, SendOutcome::TimedOut);
    assert!(
        lines[0].reply.is_none(),
        "an answer that never came must not be recorded as one: {lines:?}"
    );
    assert!(
        lines[0].outcome_detail.is_some(),
        "the reader has to know why there is no answer: {lines:?}"
    );
}

/// The read side is the same feature: what the send route wrote is what the
/// conversation route returns.
#[tokio::test]
async fn the_conversation_route_returns_what_the_send_route_wrote() {
    let transcript = Arc::new(RecordingTranscript::default());
    let state = state(transcript.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-read-probe").await;

    call(
        &app,
        "POST",
        &token,
        ROUTE,
        Some(r#"{"type":"prompt","goal":{"objective":"say something quiet"}}"#),
    )
    .await;

    let (status, body) = call(&app, "GET", &token, ROUTE, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let page: serde_json::Value = serde_json::from_str(&body).expect("the body is JSON");
    assert_eq!(page["agent_id"], serde_json::json!(AGENT));
    let messages = page["messages"]
        .as_array()
        .expect("the conversation is a list");
    assert_eq!(
        messages.len(),
        1,
        "the reader sees the line that was written: {body}"
    );
    assert_eq!(messages[0]["outcome"], serde_json::json!("queued"));
    assert!(
        messages[0]["body"]
            .as_str()
            .is_some_and(|b| b.contains("say something quiet")),
        "{body}"
    );
}
