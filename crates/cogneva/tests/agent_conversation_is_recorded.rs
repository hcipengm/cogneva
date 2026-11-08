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

use cog_core::{AgentRegistration, AgentRegistry, MessageBackend, SFResult};
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
    async fn record(&self, message: &NewAgentMessage<'_>) -> SFResult<i64> {
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
        Ok(id)
    }

    async fn answer(&self, id: i64, reply: &str) -> SFResult<bool> {
        let mut lines = self
            .lines
            .lock()
            .expect("the transcript lock is not poisoned");
        let Some(line) = lines.iter_mut().find(|line| line.id == id) else {
            return Ok(false);
        };
        if !matches!(line.outcome, SendOutcome::Queued | SendOutcome::TimedOut) {
            return Ok(false);
        }
        line.outcome = SendOutcome::Replied;
        line.reply = Some(reply.to_string());
        line.outcome_detail = None;
        Ok(true)
    }

    async fn no_answer(&self, id: i64, outcome: SendOutcome, detail: &str) -> SFResult<bool> {
        let mut lines = self
            .lines
            .lock()
            .expect("the transcript lock is not poisoned");
        let Some(line) = lines.iter_mut().find(|line| line.id == id) else {
            return Ok(false);
        };
        if line.outcome != SendOutcome::Queued {
            return Ok(false);
        }
        line.outcome = outcome;
        line.outcome_detail = Some(detail.to_string());
        Ok(true)
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

/// A prompt nobody answers cannot spend the rest of its life saying it will be
/// answered.
///
/// `queued` is written for a prompt sent without a wait, and the panel reads it
/// as "delivered, it answers on its own". When the employee is running another
/// task, that sentence queues behind the whole run; the stream the answer would
/// arrive on closes at the end of the late window, and from then on no answer
/// can reach that row. Left as it is, the record promises an answer nothing will
/// deliver -- on 2026-11-07 a person asked a squad member a question, was told
/// the words were delivered, and heard nothing for ten minutes while the row
/// kept reading "waiting for it to answer".
///
/// The clock is paused and advanced over the window rather than the window
/// shortened for the test: what is judged is the bound the deployment actually
/// waits, not a number invented here.
#[tokio::test(start_paused = true)]
async fn a_prompt_nobody_answers_does_not_stay_queued_forever() {
    let transcript = Arc::new(RecordingTranscript::default());
    let state = state(transcript.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-silence-probe").await;

    let (status, body) = call(
        &app,
        "POST",
        &token,
        ROUTE,
        Some(r#"{"type":"prompt","goal":{"objective":"nobody listens"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");

    // The window is held by the collector task the route spawns, and a task
    // starts holding it on its first poll -- not at spawn. Advancing the clock
    // before that poll would leave the timer registering 600s into the future
    // of the new "now", so the row would still read queued and the test would
    // be judging the scheduler rather than the route.
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(
        cog_gateway::agents::LATE_ANSWER_WINDOW + std::time::Duration::from_secs(1),
    )
    .await;
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }

    let lines = transcript.lines.lock().expect("lock").clone();
    assert_eq!(lines.len(), 1, "the send is recorded once: {lines:?}");
    assert_eq!(
        lines[0].outcome,
        SendOutcome::TimedOut,
        "a send nobody answered has to say so instead of waiting forever: {lines:?}"
    );
    assert!(
        lines[0].reply.is_none(),
        "an answer that never came must not be recorded as one: {lines:?}"
    );
    assert!(
        lines[0].outcome_detail.is_some(),
        "the reader has to be told it went unanswered, not left reading a promise: {lines:?}"
    );
}

/// A wait longer than the deployment's per-request budget is refused at the
/// door, not cut off mid-flight.
///
/// Every route here sits under one request budget. A wait that outlives it is
/// cut off by that budget before the conversation line is written, and what the
/// caller gets is a bare 408: the words delivered, nothing recorded, and no way
/// to tell that from the agent's silence. The refusal names the bound so a
/// caller can see what this deployment can hold.
#[tokio::test]
async fn a_wait_longer_than_the_request_budget_is_refused_at_the_door() {
    let transcript = Arc::new(RecordingTranscript::default());
    let state = state(transcript.clone());
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-budget-probe").await;

    // This state answers a request within 30s and keeps 5s of that for writing
    // the line down, so the longest honest wait is 25s.
    let (status, body) = call(
        &app,
        "POST",
        &token,
        &format!("{ROUTE}?wait_secs=60"),
        Some(r#"{"type":"prompt","goal":{"objective":"wait far too long"}}"#),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a wait the deployment cannot hold is refused, not left to be cut off: {body}"
    );
    assert!(
        body.contains("above 25"),
        "the refusal has to name the bound this deployment can hold: {body}"
    );

    let lines = transcript.lines.lock().expect("lock").clone();
    assert_eq!(
        lines.len(),
        1,
        "a refused wait is still a send that was turned away, and it is written down: {lines:?}"
    );
    assert_eq!(lines[0].outcome, SendOutcome::Refused);
}

/// An answer that arrives after the request has been answered lands on the line
/// that request wrote.
///
/// The request is not the conversation: an agent's turn outlives the caller's
/// patience, and the platform's own request budget is shorter than many turns.
/// What keeps a late answer is that the reply stream stays open under a
/// collector, and that the line the send wrote is still waiting to be settled.
#[tokio::test]
async fn an_answer_that_arrives_later_settles_the_line_its_send_wrote() {
    let transcript = Arc::new(RecordingTranscript::default());
    let backend: Arc<dyn MessageBackend> = Arc::new(cog_stream::MemoryMessageBackend::new());
    let state = router_state::with_messaging_and_transcript(
        backend.clone(),
        Arc::new(OneAgent),
        transcript.clone(),
    );
    let app = create_router(state.clone());
    let token = router_state::admin_bearer(&state, "transcript-late-probe").await;

    // The test plays the worker: stand on the inbox before anything is sent, so
    // the command is read rather than missed behind the reader's position.
    let inbox = cog_core::agent_inbox_stream(AGENT);
    backend
        .create_consumer_group(&inbox, "probe-worker")
        .await
        .expect("the worker opens its inbox");
    let mut reader = backend
        .subscribe(&inbox, "probe-worker")
        .await
        .expect("the worker reads its inbox");

    let (status, body) = call(
        &app,
        "POST",
        &token,
        ROUTE,
        Some(r#"{"type":"prompt","goal":{"objective":"answer me later"}}"#),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");

    use futures::StreamExt;
    let (_, bytes) = tokio::time::timeout(std::time::Duration::from_secs(10), reader.next())
        .await
        .expect("the command reaches the worker")
        .expect("the inbox stream yields one frame")
        .expect("the frame reads back");
    let command: cog_core::InboxMessage =
        serde_json::from_slice(&bytes).expect("the command is an inbox message");
    let reply_stream = match &command {
        cog_core::InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => stream.clone(),
        other => panic!("a prompt must carry the reply stream the route opened: {other:?}"),
    };

    let answer = serde_json::json!("I read you, a little late");
    let payload = serde_json::to_vec(&answer).expect("the answer serialises");
    backend
        .publish(&reply_stream, &payload)
        .await
        .expect("the answer goes onto the stream the route opened");

    // The collector is a background task, so the line settles a moment later.
    let mut settled = Vec::new();
    for _ in 0..100 {
        settled = transcript.lines.lock().expect("lock").clone();
        if settled
            .iter()
            .any(|line| line.outcome == SendOutcome::Replied)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        settled.len(),
        1,
        "the answer belongs to the line the send wrote, not to a new one: {settled:?}"
    );
    assert_eq!(
        settled[0].outcome,
        SendOutcome::Replied,
        "a late answer is still an answer: {settled:?}"
    );
    assert_eq!(
        settled[0].reply.as_deref(),
        Some("I read you, a little late")
    );
    assert!(
        settled[0].outcome_detail.is_none(),
        "an answered line carries no failure detail: {settled:?}"
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
