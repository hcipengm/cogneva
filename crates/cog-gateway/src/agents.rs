//! HTTP handlers for the Agent registration / heartbeat endpoints.
//! Wires the [`cog_core::AgentRegistry`] trait to the gateway, exposing:
//! - `POST /api/v1/agents/register` — register a new Agent
//! - `POST /api/v1/agents/{id}/heartbeat` — renew TTL
//! - `DELETE /api/v1/agents/{id}` — graceful deregister
//! - `GET /api/v1/agents/{id}` — fetch a registration
//! - `GET /api/v1/agents` — list live Agents

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::GatewayState;
use cog_core::{AgentRegistration, ResourceInfo};
use cog_storage::{NewAgentMessage, SendOutcome};

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub hostname: String,
    pub pod_ip: String,
    pub role: String,
    pub workspace_id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub resources: ResourceInfo,
    /// Optional client-supplied uuid for deterministic agent_id.  When `None`
    /// we generate a fresh `uuid_v4` so multiple instances of the same pod
    /// don't collide on `blake3(hostname|ip|role)`.
    #[serde(default)]
    pub uuid: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResponse {
    pub agent_id: String,
    pub registration: AgentRegistration,
}

pub async fn register_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<RegisterRequest>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };

    let uuid = req.uuid.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let agent_id = cog_core::generate_agent_id(&req.hostname, &req.pod_ip, &req.role, &uuid);
    let now = chrono::Utc::now();
    let registration = AgentRegistration {
        agent_id: agent_id.clone(),
        role: req.role,
        workspace_id: req.workspace_id,
        capabilities: req.capabilities,
        resources: req.resources,
        hostname: req.hostname,
        pod_ip: req.pod_ip,
        registered_at: now,
        last_heartbeat: now,
    };

    match registry.register(&registration).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(RegisterResponse {
                agent_id,
                registration,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn heartbeat_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.heartbeat(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn deregister_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.deregister(&agent_id).await {
        Ok(()) => (StatusCode::NO_CONTENT, "").into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn get_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.get(&agent_id).await {
        Ok(Some(reg)) => (StatusCode::OK, Json(reg)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "agent not registered"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// Put one command on a named agent's inbox.
///
/// The inbox subscription and the command semantics existed, but nothing in
/// the platform ever produced a message for them, so an agent could be
/// addressed only from inside its own process. This is the producer that
/// closes the path: the body is the wire form of [`cog_core::InboxMessage`].
///
/// A `reply_stream` named by the caller is refused here. The platform ends a
/// reply stream once the ask it belongs to is over -- that ending is the only
/// thing that keeps one from outliving its question -- and a name supplied
/// from outside belongs to a reader this process cannot see, so nothing here
/// may delete it. The stream is also not something an outside caller can read:
/// the store is not reachable from the client side. Accepting the field would
/// therefore answer 202 for a key that can stay in the store for good, with no
/// way to drain it. An agent asking another agent gets an answer through the
/// `send_to_agent` tool with `expect_reply`, whose stream the platform opens
/// and ends by construction.
///
/// Two delivery paths, tried in that order:
///
/// - an agent running in *this* process, addressed directly, so the command
///   reaches its execution path without waiting for a poll;
/// - a name held by a process elsewhere, which this one cannot see and cannot
///   call into. The registry is the only thing that knows it exists, and it
///   answers with evidence of life rather than with a row that merely exists,
///   so a name it returns belongs to a running consumer group. The command
///   goes onto that agent's inbox stream, which is the same stream the
///   consumer reads by construction -- the name comes from
///   [`cog_core::agent_inbox_stream`], not from a second copy of the format.
///
/// Accepted, not processed: the agent's consumer owns execution, and the
/// answer goes to `reply_stream` rather than to this response. A name that
/// resolves to no live agent anywhere is a 404, because queueing a command for
/// a process that will never read it is indistinguishable from losing it.
pub async fn send_message_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<MessageQuery>,
    Json(mut msg): Json<cog_core::InboxMessage>,
) -> Response {
    // Who said it, taken from the request rather than invented: this route has
    // no authenticated caller, so the default names the channel the owner
    // speaks through instead of guessing at a unit they may not be.
    let from_unit = query
        .from
        .as_deref()
        .map(str::trim)
        .filter(|from| !from.is_empty())
        .unwrap_or(WEBUI_SENDER)
        .to_string();
    // Read before delivery: opening the ask rewrites the reply stream on the
    // message, and the transcript is of what was said, not of what was
    // rewritten on the way out.
    let kind = command_name(&msg);
    let body = command_body(&msg);
    let sent = Sent {
        from: &from_unit,
        to: &agent_id,
        kind,
        body: &body,
    };

    // Checked before either delivery path: both of them hand the command to a
    // handler that would publish the answer into that stream and leave it
    // there, because ending it is the asker's job and this route has no asker
    // it knows.
    if let Some(stream) = caller_named_reply_stream(&msg) {
        record_send(
            &state,
            &sent,
            SendOutcome::Refused,
            None,
            Some("the request named its own reply_stream, which this route refuses"),
        )
        .await;
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": "this route does not take a caller-named reply_stream: \
                          the platform cannot read or reclaim a stream whose owner \
                          it does not know. Pass `?wait_secs=N` instead and this \
                          route will open the stream itself, read the answer and \
                          end it before it answers; an agent asking another agent \
                          gets the same thing through the send_to_agent tool with \
                          expect_reply",
                "reply_stream": stream,
                "agent_id": agent_id,
            })),
        )
            .into_response();
    }

    let wait = match ask_window(&msg, query.wait_secs) {
        Ok(wait) => wait,
        Err(e) => {
            record_send(&state, &sent, SendOutcome::Refused, None, Some(&e)).await;
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e, "agent_id": agent_id})),
            )
                .into_response();
        }
    };

    // Opened before the command goes out, whichever path delivers it: a
    // consumer group starts at the stream's current end, so an answer
    // published between the publish and the subscribe would sit behind the
    // reader and the wait would run to its deadline with the answer already
    // on the wire.
    let ask = match wait {
        Some(window) => {
            // The answer is read off the queue, so a deployment with no queue
            // cannot serve this one request even when the agent is in this
            // process: the reply has nowhere to land.
            let Some(backend) = state.message_backend.clone() else {
                record_send(
                    &state,
                    &sent,
                    SendOutcome::Refused,
                    None,
                    Some("message_backend not configured: an answer has nowhere to land"),
                )
                .await;
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({
                        "error": "message_backend not configured: an answer has \
                                  nowhere to land without one",
                        "agent_id": agent_id,
                    })),
                )
                    .into_response();
            };
            match Ask::open(backend, &mut msg, window).await {
                Ok(ask) => Some(ask),
                Err(e) => {
                    record_send(&state, &sent, SendOutcome::Refused, None, Some(&e)).await;
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e, "agent_id": agent_id})),
                    )
                        .into_response();
                }
            }
        }
        None => None,
    };

    // A dropped `ask` on any of the returns below ends its stream, so a
    // refused delivery does not leave one behind.
    let delivered = match deliver(&state, &agent_id, msg).await {
        Ok(delivered) => delivered,
        Err(response) => {
            let detail = format!(
                "delivery was refused before the command reached the agent: HTTP {}",
                response.status().as_u16()
            );
            record_send(&state, &sent, SendOutcome::Refused, None, Some(&detail)).await;
            return response;
        }
    };

    match ask {
        None => {
            record_send(&state, &sent, SendOutcome::Queued, None, None).await;
            (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "queued": true,
                    "agent_id": agent_id,
                    "via": delivered.label(),
                })),
            )
                .into_response()
        }
        Some(ask) => {
            let answer = answer_with_reply(agent_id.clone(), delivered, ask).await;
            record_send(
                &state,
                &sent,
                answer.outcome,
                answer.reply.as_deref(),
                answer.detail.as_deref(),
            )
            .await;
            answer.response
        }
    }
}

/// The name the transcript uses for a command sent through this route.
///
/// The route is how a person reaches an agent; it deliberately does not invent
/// a unit name for them, because a transcript that attributes a person's words
/// to an agent they may not hold would read as something an agent said.
const WEBUI_SENDER: &str = "webui";

/// Read the conversation with one agent, both directions, oldest first.
///
/// The counterpart of the send route: what a person said to an agent and what
/// it answered, readable after the response has scrolled away. A deployment
/// with no database says so rather than answering an empty conversation, which
/// a reader would take as "nothing was ever said".
pub async fn conversation_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<ConversationQuery>,
) -> Response {
    let Some(store) = state.agent_messages.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "error": "no database is configured for agent conversations: this \
                          deployment keeps no transcript",
                "agent_id": agent_id,
            })),
        )
            .into_response();
    };
    let limit = query
        .limit
        .unwrap_or(CONVERSATION_DEFAULT_LIMIT)
        .min(cog_storage::ROWS_KEPT_PER_PEER as usize);
    match store.conversation(&agent_id, limit).await {
        Ok(messages) => {
            let lines: Vec<MessageView> = messages.iter().map(MessageView::from).collect();
            (
                StatusCode::OK,
                Json(serde_json::json!({ "agent_id": agent_id, "messages": lines })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string(), "agent_id": agent_id})),
        )
            .into_response(),
    }
}

/// What a reader may ask a conversation for.
#[derive(Debug, Deserialize, Default)]
pub struct ConversationQuery {
    /// How many lines, newest window first, returned oldest first. Absent means
    /// the default; above the store's bound is clamped to the bound, because
    /// asking for more than is kept is not an error -- it is the whole window.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// One line of a conversation as the reader gets it.
#[derive(Debug, Serialize)]
pub struct MessageView {
    pub id: i64,
    pub from_unit: String,
    pub to_unit: String,
    pub kind: String,
    pub body: String,
    pub outcome: String,
    pub reply: Option<String>,
    pub outcome_detail: Option<String>,
    /// RFC 3339, so a reader in another timezone can tell when it was said
    /// without parsing the server's formatting choice.
    pub created_at: String,
}

impl From<&cog_storage::AgentMessage> for MessageView {
    fn from(message: &cog_storage::AgentMessage) -> Self {
        Self {
            id: message.id,
            from_unit: message.from_unit.clone(),
            to_unit: message.to_unit.clone(),
            kind: message.kind.clone(),
            body: message.body.clone(),
            outcome: message.outcome.as_str().to_string(),
            reply: message.reply.clone(),
            outcome_detail: message.outcome_detail.clone(),
            created_at: message.created_at.to_rfc3339(),
        }
    }
}

/// How long the route will hold a request open waiting for an answer.
///
/// A route that waits is a connection held by the caller, and every proxy in
/// front of it has its own idle bound; a request told to wait longer than this
/// is refused rather than silently cut down to it, because a caller that asked
/// for five minutes and was given two would read the timeout as the agent's
/// silence rather than as this bound.
pub const MAX_WAIT_SECS: u64 = 120;

/// The consumer group the route reads its answers with.
///
/// One name for every ask because each ask owns its own stream: the group is
/// the reader, and a stream with exactly one reader does not need a second
/// identity.
const ASK_GROUP: &str = "asker";

/// What the request asked for beyond delivery.
#[derive(Debug, Deserialize, Default)]
pub struct MessageQuery {
    /// Seconds to wait for the agent's answer. Absent means do not wait: the
    /// reply is queued and this route answers `202`, which is what it has
    /// always done.
    #[serde(default)]
    pub wait_secs: Option<u64>,
    /// Who is speaking, for the transcript. Absent means a person through the
    /// WebUI; an agent that sends through this route names itself.
    #[serde(default)]
    pub from: Option<String>,
}

/// How many lines of a conversation a read returns when the caller does not say.
const CONVERSATION_DEFAULT_LIMIT: usize = 100;

/// Whether this request wants an answer, and why not when it cannot have one.
///
/// Split out from the handler so the decision is a function of the request
/// alone: which commands have an answer at all is a property of the command,
/// not of the deployment it is sent to.
fn ask_window(
    msg: &cog_core::InboxMessage,
    wait_secs: Option<u64>,
) -> Result<Option<std::time::Duration>, String> {
    let Some(secs) = wait_secs else {
        return Ok(None);
    };
    if !matches!(msg, cog_core::InboxMessage::Prompt { .. }) {
        return Err(format!(
            "wait_secs applies to `prompt` only: the other commands carry no answer \
             (an abort, a reset and a steering change what the agent is doing, and \
             a continue resumes a run that answers on its own stream); got {}",
            command_name(msg)
        ));
    }
    if secs == 0 {
        return Err("wait_secs must be at least 1".into());
    }
    if secs > MAX_WAIT_SECS {
        return Err(format!(
            "wait_secs above {MAX_WAIT_SECS} is refused rather than clamped: a caller \
             that asked for {secs}s and was given less would read the timeout as the \
             agent's silence"
        ));
    }
    Ok(Some(std::time::Duration::from_secs(secs)))
}

/// The command's own name, for error text.
fn command_name(msg: &cog_core::InboxMessage) -> &'static str {
    match msg {
        cog_core::InboxMessage::Prompt { .. } => "prompt",
        cog_core::InboxMessage::Steering { .. } => "steering",
        cog_core::InboxMessage::Continue { .. } => "continue",
        cog_core::InboxMessage::Abort => "abort",
        cog_core::InboxMessage::Reset => "reset",
    }
}

/// The words a command carries, for the transcript.
///
/// A prompt's payload is a JSON goal, which is the whole of what was said; the
/// two commands that carry no text answer with their own name so a reader sees
/// that something happened rather than an empty line.
fn command_body(msg: &cog_core::InboxMessage) -> String {
    match msg {
        cog_core::InboxMessage::Prompt { goal, .. } => match goal {
            serde_json::Value::String(text) => text.clone(),
            other => other.to_string(),
        },
        cog_core::InboxMessage::Continue { instruction } => instruction.clone(),
        cog_core::InboxMessage::Steering { instruction } => instruction.clone(),
        cog_core::InboxMessage::Abort => "(abort)".into(),
        cog_core::InboxMessage::Reset => "(reset)".into(),
    }
}

/// What was said, before the outcome is known: the four fields every written
/// line carries whichever way the send ends.
struct Sent<'a> {
    from: &'a str,
    to: &'a str,
    kind: &'a str,
    body: &'a str,
}

/// Write one settled send down, if this deployment has somewhere to write it.
///
/// A store that fails does not change the answer the caller is about to get:
/// the command was delivered or refused already, and a lost transcript line is
/// not a reason to report the delivery as failed.
async fn record_send(
    state: &GatewayState,
    sent: &Sent<'_>,
    outcome: SendOutcome,
    reply: Option<&str>,
    detail: Option<&str>,
) {
    let Some(store) = state.agent_messages.as_ref() else {
        return;
    };
    let message = NewAgentMessage {
        from_unit: sent.from,
        to_unit: sent.to,
        kind: sent.kind,
        body: sent.body,
        outcome,
        reply,
        outcome_detail: detail,
    };
    if let Err(e) = store.record(&message).await {
        tracing::warn!(
            to = %sent.to,
            outcome = outcome.as_str(),
            error = %e,
            "could not record a message sent to an agent"
        );
    }
}

/// Which of the two delivery paths took the command.
#[derive(Clone, Copy)]
enum Delivered {
    /// An agent in this process, handed the command directly.
    Pool,
    /// A name held elsewhere, reached through the shared inbox stream.
    Inbox,
}

impl Delivered {
    fn label(self) -> &'static str {
        match self {
            Self::Pool => "agent_pool",
            Self::Inbox => "inbox",
        }
    }
}

/// Put the command on the named agent's queue, or say why it could not go.
// The refusal is an HTTP response the route hands straight back to axum, so
// the large variant is the type the caller actually wants; boxing it would
// only move the same bytes onto the heap for the length of one return.
#[allow(clippy::result_large_err)]
async fn deliver(
    state: &GatewayState,
    agent_id: &str,
    msg: cog_core::InboxMessage,
) -> Result<Delivered, Response> {
    // Local first. A miss is not an answer -- the same name may be held by
    // another process, which is the whole point of the registry.
    if let Some(pool) = state.agent_pool.as_ref() {
        match pool.get_agent(agent_id).await {
            Ok(Some(agent)) => {
                return match agent.receive_message(msg).await {
                    Ok(()) => Ok(Delivered::Pool),
                    Err(e) => Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response()),
                }
            }
            Ok(None) => {}
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response())
            }
        }
    }

    let Some(registry) = state.agent_registry.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response());
    };
    match registry.get(agent_id).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "no live agent under this name"})),
            )
                .into_response())
        }
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response())
        }
    }

    // A name with a live holder but no queue to put the command on is a
    // deployment fault, not a delivery: say so instead of answering 202 for a
    // message that was never written anywhere.
    let Some(backend) = state.message_backend.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "message_backend not configured"})),
        )
            .into_response());
    };
    let payload = match serde_json::to_vec(&msg) {
        Ok(payload) => payload,
        Err(e) => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response())
        }
    };
    let stream = cog_core::agent_inbox_stream(agent_id);
    match backend.publish(&stream, &payload).await {
        Ok(()) => Ok(Delivered::Inbox),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string(), "stream": stream})),
        )
            .into_response()),
    }
}

/// One asked exchange: the response to return, and what to write down.
struct Answer {
    response: Response,
    outcome: SendOutcome,
    reply: Option<String>,
    detail: Option<String>,
}

/// Wait for the answer to a delivered prompt and turn it into the response.
async fn answer_with_reply(agent_id: String, delivered: Delivered, ask: Ask) -> Answer {
    let started = std::time::Instant::now();
    let outcome = ask.await_reply().await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    // One line per ask, beside the queue: the queue keeps no record of who
    // asked, so this is the only place the two ends meet.
    tracing::info!(
        to = %agent_id,
        via = delivered.label(),
        outcome = outcome.label(),
        elapsed_ms,
        "asked an agent and waited for its answer"
    );
    match outcome {
        AskOutcome::Replied(reply) => {
            // The answer arrives as parsed JSON; a string answer is what the
            // agent said, any other shape is kept as its JSON text so the
            // transcript never drops an answer it does not recognise.
            let text = match &reply {
                serde_json::Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            Answer {
                response: (
                    StatusCode::OK,
                    Json(serde_json::json!({
                        "replied": true,
                        "agent_id": agent_id,
                        "via": delivered.label(),
                        "reply": reply,
                    })),
                )
                    .into_response(),
                outcome: SendOutcome::Replied,
                reply: Some(text),
                detail: None,
            }
        }
        AskOutcome::TimedOut { waited_secs } => Answer {
            response: (
                StatusCode::GATEWAY_TIMEOUT,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "via": delivered.label(),
                    "waited_secs": waited_secs,
                    "error": "the agent did not answer within the wait this request \
                              asked for; the run itself was not cancelled",
                })),
            )
                .into_response(),
            outcome: SendOutcome::TimedOut,
            reply: None,
            detail: Some(format!("no answer arrived within {waited_secs}s")),
        },
        AskOutcome::Unreadable { error } => Answer {
            response: (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "error": format!("the agent answered with bytes that are not JSON: {error}"),
                })),
            )
                .into_response(),
            outcome: SendOutcome::Failed,
            reply: None,
            detail: Some(format!(
                "the agent answered with bytes that are not JSON: {error}"
            )),
        },
        AskOutcome::StreamError { error } => Answer {
            response: (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "replied": false,
                    "agent_id": agent_id,
                    "error": format!("the reply stream failed before an answer arrived: {error}"),
                })),
            )
                .into_response(),
            outcome: SendOutcome::Failed,
            reply: None,
            detail: Some(format!(
                "the reply stream failed before an answer arrived: {error}"
            )),
        },
    }
}

/// One ask's reply stream, from the moment the key exists until it is ended.
///
/// The stream is opened here rather than named by the caller for the reason
/// the refusal above gives: ending it is the asker's job, and this is the
/// asker. It is ended on every path the wait reaches, and the `Drop` below is
/// what covers the paths that reach none of them -- a cancelled request, or a
/// `?` return between the group's creation and the wait.
struct Ask {
    backend: Arc<dyn cog_core::MessageBackend>,
    stream: String,
    reader: cog_core::MessageStream,
    window: std::time::Duration,
    armed: bool,
}

impl Ask {
    /// Open a reply stream and put its name on `msg`.
    ///
    /// The group is created before the subscribe because that is what brings
    /// the key into existence, and the subscriber starts from the stream's
    /// current end; a group created afterwards would start behind the answer
    /// this call is about to ask for.
    async fn open(
        backend: Arc<dyn cog_core::MessageBackend>,
        msg: &mut cog_core::InboxMessage,
        window: std::time::Duration,
    ) -> Result<Self, String> {
        let stream = ask_stream();
        if let cog_core::InboxMessage::Prompt { reply_stream, .. } = msg {
            *reply_stream = Some(stream.clone());
        } else {
            return Err("only a prompt has an answer to wait for".into());
        }
        backend
            .create_consumer_group(&stream, ASK_GROUP)
            .await
            .map_err(|e| format!("cannot open a reply stream: {e}"))?;
        let reader = match backend.subscribe(&stream, ASK_GROUP).await {
            Ok(reader) => reader,
            // The key exists from the group's creation, and no guard owns it
            // yet, so this path ends it here.
            Err(e) => {
                backend.stop_stream(&stream, ASK_GROUP).await;
                if let Err(del) = backend.delete_stream(&stream).await {
                    tracing::warn!(
                        stream = %stream,
                        error = %del,
                        "a reply stream opened for an ask that could not read it was left behind"
                    );
                }
                return Err(format!("cannot read the reply stream: {e}"));
            }
        };
        Ok(Self {
            backend,
            stream,
            reader,
            window,
            armed: true,
        })
    }

    /// End the stream and stop counting this ask as outstanding.
    async fn close(&mut self) {
        self.backend.stop_stream(&self.stream, ASK_GROUP).await;
        if let Err(e) = self.backend.delete_stream(&self.stream).await {
            tracing::warn!(
                stream = %self.stream,
                error = %e,
                "the ask's reply stream outlived its answer: this backend cannot delete streams"
            );
        }
        self.armed = false;
    }

    async fn await_reply(mut self) -> AskOutcome {
        let window = self.window;
        let waited = tokio::time::timeout(window, futures::StreamExt::next(&mut self.reader)).await;
        let outcome = match waited {
            Ok(Some(Ok((id, bytes)))) => {
                if let Err(e) = self
                    .backend
                    .ack(&self.stream, ASK_GROUP, std::slice::from_ref(&id))
                    .await
                {
                    tracing::warn!(stream = %self.stream, error = %e, "cannot ack an answer");
                }
                serde_json::from_slice::<serde_json::Value>(&bytes)
                    .map(AskOutcome::Replied)
                    .unwrap_or_else(|e| AskOutcome::Unreadable {
                        error: e.to_string(),
                    })
            }
            Ok(Some(Err(e))) => AskOutcome::StreamError {
                error: e.to_string(),
            },
            Ok(None) => AskOutcome::StreamError {
                error: "the reply stream ended without a message".into(),
            },
            Err(_) => AskOutcome::TimedOut {
                waited_secs: window.as_secs(),
            },
        };
        self.close().await;
        outcome
    }
}

impl Drop for Ask {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let backend = self.backend.clone();
        let stream = self.stream.clone();
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    backend.stop_stream(&stream, ASK_GROUP).await;
                    if let Err(e) = backend.delete_stream(&stream).await {
                        tracing::warn!(
                            stream = %stream,
                            error = %e,
                            "an abandoned ask left its reply stream behind"
                        );
                    }
                });
            }
            Err(_) => tracing::warn!(
                stream = %self.stream,
                "an abandoned ask could not be reaped: no runtime to end its stream"
            ),
        }
    }
}

/// How one ask ended.
enum AskOutcome {
    Replied(serde_json::Value),
    TimedOut {
        waited_secs: u64,
    },
    /// The agent published something that is not JSON.
    Unreadable {
        error: String,
    },
    /// The queue failed, or the stream ended without a message.
    StreamError {
        error: String,
    },
}

impl AskOutcome {
    fn label(&self) -> &'static str {
        match self {
            Self::Replied(_) => "replied",
            Self::TimedOut { .. } => "timeout",
            Self::Unreadable { .. } => "unreadable",
            Self::StreamError { .. } => "stream_error",
        }
    }
}

/// The stream one ask waits on, named by the ask itself.
///
/// The name is the correlation: a reply carries nothing saying which question
/// it answers, so two questions sharing a stream would leave the reader unable
/// to tell the answers apart. Giving each ask its own stream makes the
/// correlation a property of the address.
///
/// Process id, wall clock and a counter together, because no one of them is
/// unique alone: the counter restarts at zero with the process, the clock
/// collides inside a tick, and the process id is reused.
fn ask_stream() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!(
        "gateway:ask:{}:{:x}:{}",
        std::process::id(),
        nanos,
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// The reply stream the caller put in the body, if any.
///
/// Split out so the refusal can be judged without a gateway state: what is
/// being decided is which bodies this route will take, and that depends on the
/// body alone.
fn caller_named_reply_stream(msg: &cog_core::InboxMessage) -> Option<&str> {
    match msg {
        cog_core::InboxMessage::Prompt {
            reply_stream: Some(stream),
            ..
        } => Some(stream),
        _ => None,
    }
}

/// One registry record as the roster prints it.
///
/// The routing id stays exactly as registered — it is derived from (task, role)
/// at both ends and cannot carry a name — and the readable name is derived from
/// that id here, at the one surface a person reads. Storing the name instead
/// would let the two drift: a record whose id was rewritten would keep a name
/// that no longer resolves to it.
fn agent_view(reg: &cog_core::AgentRegistration) -> serde_json::Value {
    let mut view = serde_json::to_value(reg).unwrap_or(serde_json::Value::Null);
    if let Some(obj) = view.as_object_mut() {
        obj.insert(
            "display_name".to_string(),
            serde_json::json!(cog_core::display_name_for(&reg.agent_id)),
        );
    }
    view
}

/// Split the roster into the two populations it actually holds.
///
/// A pool worker is capacity: it exists whether or not there is work. A squad
/// member is demand: one squad per task in flight, created when the task is
/// dispatched and retired when it ends. Summed into one number they answer no
/// question — "few agents" then reads alike whether the pool is idle or the
/// work has stopped coming.
fn roster_split(regs: &[cog_core::AgentRegistration]) -> (usize, usize) {
    let squads = regs
        .iter()
        .filter(|r| r.agent_id.starts_with(cog_core::SQUAD_ID_PREFIX))
        .count();
    (regs.len() - squads, squads)
}

pub async fn list_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let Some(registry) = state.agent_registry.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "agent_registry not configured"})),
        )
            .into_response();
    };
    match registry.list().await {
        Ok(regs) => {
            let agents: Vec<serde_json::Value> = regs.iter().map(agent_view).collect();
            let (workers, squads) = roster_split(&regs);
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "agents": agents,
                    "count": regs.len(),
                    "worker_agents": workers,
                    "squad_agents": squads,
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Agent Lifecycle Control Commands ───
// These POST endpoints dispatch lifecycle commands through the Supervisor
// trait, which emits events on the Supervisor broadcast channel for
// downstream consumption (gRPC server, hook engine, audit log, etc.).

#[derive(Debug, Deserialize)]
pub struct CheckpointQuery {
    /// Optional task ID.  When omitted the caller should supply it in the
    /// request body of a future revision; for now we require it.
    pub task_id: String,
}

pub async fn kill_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.kill_agent(&agent_id, "user-requested").await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "kill",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "kill",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn restart_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.restart_agent(&agent_id, false).await {
            Ok(true) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "dispatched",
                })),
            )
                .into_response(),
            Ok(false) => (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "agent not found",
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "restart",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "restart",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

pub async fn checkpoint_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    Query(query): Query<CheckpointQuery>,
) -> Response {
    match state.supervisor {
        Some(ref sup) => match sup.checkpoint_agent(&agent_id, &query.task_id).await {
            Ok(checkpoint_id) => (
                StatusCode::ACCEPTED,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "dispatched",
                    "checkpoint_id": checkpoint_id,
                })),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "agent_id": agent_id,
                    "command": "checkpoint",
                    "status": "error",
                    "error": e.to_string(),
                })),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "agent_id": agent_id,
                "command": "checkpoint",
                "status": "supervisor unavailable",
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::{agent_view, caller_named_reply_stream, roster_split};
    use cog_core::InboxMessage;

    /// 名册上每一行都要读得成一个名字，而路由用的 id 一个字节都不能变：
    /// 有读 id 的调用方，动了它就断；名字只是给读的人看的那一列。
    /// 池子是按产能起的那一批，小队是按任务起的那一批。合成一个数就答不了
    /// 任何问题：「人少」既可能是池子闲着，也可能是活停了。
    #[test]
    fn the_roster_counts_capacity_apart_from_demand() {
        let pool = cog_core::AgentRegistration::new(
            "luna-1822ab0e",
            "cog-agent",
            "127.0.0.1",
            "planner",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        let squad = cog_core::AgentRegistration::new(
            cog_core::agent_id_for("task-1", "generator"),
            "cog-agent",
            "127.0.0.1",
            "generator",
            "default",
            vec![],
            cog_core::ResourceInfo::default(),
        );
        assert_eq!(roster_split(&[pool, squad]), (1, 1));
    }

    #[test]
    fn the_roster_prints_a_name_next_to_the_id_it_keeps() {
        let reg = cog_core::AgentRegistration::new(
            cog_core::agent_id_for("task-1", "planner"),
            "cog-agent",
            "127.0.0.1",
            "planner",
            "default",
            vec!["planner".into()],
            cog_core::ResourceInfo::default(),
        );
        let view = agent_view(&reg);
        assert_eq!(view["agent_id"], reg.agent_id);
        let name = view["display_name"].as_str().expect("display_name present");
        assert!(name.contains('#'), "name {name} is not a handle");
        assert_eq!(name, cog_core::display_name_for(&reg.agent_id));
    }

    /// A reply stream the caller names is taken out of the body rather than
    /// honoured, and the other two shapes -- a prompt with no reply stream and
    /// a command that is not a prompt at all -- go through. Reading only the
    /// refusal would not say the route still accepts anything.
    #[test]
    fn only_a_caller_named_reply_stream_is_refused() {
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: Some("caller:reply:1".to_string()),
            }),
            Some("caller:reply:1")
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Prompt {
                goal: serde_json::json!({"goal": "run the tests"}),
                reply_stream: None,
            }),
            None
        );
        assert_eq!(
            caller_named_reply_stream(&InboxMessage::Abort),
            None,
            "only a prompt can name a reply stream"
        );
    }
}
