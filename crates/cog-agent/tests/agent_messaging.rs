//! What the agent-addressing built-ins put on the wire, and what they refuse to
//! put there.
//!
//! The tools exist because agents had no way to address each other: the only
//! other route to an inbox is the gateway's HTTP face, which the processes that
//! run agent loops cannot reach — they hold no credentials on purpose. So the
//! queue is in-process and this is the one producer an agent can call. The
//! listing tool is the other half of the same problem: an agent id is derived
//! from the task and the role, and a squad member knows neither for its peers,
//! so a send tool with no directory has no address to send to.
//!
//! Two properties are decided here rather than in the cluster:
//!
//! - **The payload is the inbox's own wire form.** Producer and consumer are in
//!   crates that do not know about each other, so a message that serializes
//!   into anything other than what [`InboxMessage`] reads is a command that is
//!   delivered to nobody while the publisher reports success.
//! - **The roster is asked first.** An agent id no process holds still gets a
//!   stream key when published to, and a consumer group is created by the
//!   consumer that will never run; the entry then sits in a key nothing reads
//!   and no sweep may take (it is not empty). A miss is an error here, and
//!   nothing is published.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;

use cog_agent::tools::builtins::{list_agents, send_to_agent};
use cog_core::{
    AgentRegistration, AgentRegistry, InboxMessage, MessageBackend, MessageStream, PendingStats,
    ResourceInfo, SFResult, ToolImplementation,
};

/// A queue that keeps what it was given.
#[derive(Default)]
struct Recording {
    published: Mutex<Vec<(String, Vec<u8>)>>,
}

impl Recording {
    fn published(&self) -> Vec<(String, Vec<u8>)> {
        self.published.lock().expect("lock").clone()
    }
}

#[async_trait]
impl MessageBackend for Recording {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        self.published
            .lock()
            .expect("lock")
            .push((subject.to_string(), payload.to_vec()));
        Ok(())
    }

    async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
        Ok(None)
    }

    async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
        Ok(Box::pin(futures::stream::empty()))
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        _start_id: &str,
    ) -> SFResult<MessageStream> {
        self.subscribe(subject, group).await
    }

    async fn stop_stream(&self, _subject: &str, _group: &str) {}

    async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
        Ok(())
    }

    async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
        Ok(())
    }

    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        _count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        Ok(None)
    }

    async fn pending_stats(
        &self,
        _stream: &str,
        _group: &str,
        _idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>> {
        Ok(None)
    }

    async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
        Ok(None)
    }
}

/// One live entry in the hand-written roster.
type Entry = (&'static str, &'static str, &'static [&'static str]);

/// A roster holding the names it was built with.
///
/// Written by hand because what is being checked is that the tools ask the
/// roster at all, and a registry that records nothing could not show that.
struct Roster {
    live: Vec<Entry>,
}

impl Roster {
    /// Entries that only need to exist: role `probe`, no capabilities.
    fn new(live: &[&'static str]) -> Self {
        Self {
            live: live.iter().map(|id| (*id, "probe", &[][..])).collect(),
        }
    }

    fn with_roles(rows: &[Entry]) -> Self {
        Self {
            live: rows.to_vec(),
        }
    }
}

#[async_trait]
impl AgentRegistry for Roster {
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
            .live
            .iter()
            .find(|(id, _, _)| *id == agent_id)
            .map(|(id, role, caps)| registration(id, role, caps)))
    }

    async fn list(&self) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .live
            .iter()
            .map(|(id, role, caps)| registration(id, role, caps))
            .collect())
    }

    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .live
            .iter()
            .filter(|(_, r, _)| *r == role)
            .map(|(id, role, caps)| registration(id, role, caps))
            .collect())
    }

    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>> {
        Ok(self
            .live
            .iter()
            .filter(|(_, _, caps)| caps.contains(&capability))
            .map(|(id, role, caps)| registration(id, role, caps))
            .collect())
    }
}

fn registration(agent_id: &str, role: &str, capabilities: &[&str]) -> AgentRegistration {
    let now = chrono::Utc::now();
    AgentRegistration {
        agent_id: agent_id.to_string(),
        role: role.into(),
        workspace_id: "probe".into(),
        capabilities: capabilities.iter().map(|c| (*c).to_string()).collect(),
        resources: ResourceInfo::default(),
        hostname: "probe".into(),
        pod_ip: "127.0.0.1".into(),
        registered_at: now,
        last_heartbeat: now,
    }
}

/// Call the tool the way the registry does.
async fn call<B: MessageBackend + 'static>(
    backend: Arc<B>,
    roster: Arc<Roster>,
    args: serde_json::Value,
) -> SFResult<serde_json::Value> {
    let tool = send_to_agent(backend, roster);
    match tool.implementation {
        ToolImplementation::Native(handler) => (handler)(args).await,
        _ => panic!("send_to_agent is a native tool"),
    }
}

/// Call the listing tool the way the registry does.
async fn call_list(roster: Arc<Roster>, args: serde_json::Value) -> SFResult<serde_json::Value> {
    let tool = list_agents(roster);
    match tool.implementation {
        ToolImplementation::Native(handler) => (handler)(args).await,
        _ => panic!("list_agents is a native tool"),
    }
}

/// What the directory returns is exactly what the send path will accept.
///
/// The two tools read the same roster, and this is the property that makes the
/// pair usable: an id copied out of one result and handed to the other is
/// delivered, not refused. A directory built from a different source would look
/// right and address nobody.
#[tokio::test]
async fn an_id_the_directory_lists_is_an_id_the_send_tool_delivers_to() {
    let roster = Arc::new(Roster::with_roles(&[
        ("squad:task-1-reviewer", "reviewer", &[]),
        ("squad:task-1-coder", "coder", &[]),
    ]));
    let listed = call_list(roster.clone(), serde_json::json!({}))
        .await
        .expect("listed");

    assert_eq!(listed["count"], serde_json::json!(2));
    let ids: Vec<&str> = listed["agents"]
        .as_array()
        .expect("agents is a list")
        .iter()
        .map(|a| a["agent_id"].as_str().expect("agent_id"))
        .collect();
    assert_eq!(ids, vec!["squad:task-1-reviewer", "squad:task-1-coder"]);

    let backend = Arc::new(Recording::default());
    call(
        backend.clone(),
        roster,
        serde_json::json!({ "agent_id": ids[1], "message": "run the tests" }),
    )
    .await
    .expect("an id from the directory is deliverable");

    assert_eq!(
        backend.published()[0].0,
        cog_core::agent_inbox_stream("squad:task-1-coder"),
        "the delivered stream is the one named by the listed id"
    );
}

/// Narrowing the search is a filter over the same list, not a second source.
#[tokio::test]
async fn the_directory_can_be_narrowed_by_role_and_capability() {
    let roster = Arc::new(Roster::with_roles(&[
        ("squad:task-1-reviewer", "reviewer", &["code-review"]),
        ("squad:task-1-coder", "coder", &["code-review", "shell"]),
        ("squad:task-1-tester", "tester", &["shell"]),
    ]));

    let reviewers = call_list(roster.clone(), serde_json::json!({ "role": "reviewer" }))
        .await
        .expect("listed");
    assert_eq!(reviewers["count"], serde_json::json!(1));
    assert_eq!(
        reviewers["agents"][0]["agent_id"],
        serde_json::json!("squad:task-1-reviewer")
    );

    let shells = call_list(roster.clone(), serde_json::json!({ "capability": "shell" }))
        .await
        .expect("listed");
    let mut shell_ids: Vec<&str> = shells["agents"]
        .as_array()
        .expect("agents is a list")
        .iter()
        .map(|a| a["agent_id"].as_str().expect("agent_id"))
        .collect();
    shell_ids.sort();
    assert_eq!(shell_ids, vec!["squad:task-1-coder", "squad:task-1-tester"]);

    let both = call_list(
        roster,
        serde_json::json!({ "role": "coder", "capability": "shell" }),
    )
    .await
    .expect("listed");
    assert_eq!(
        both["count"],
        serde_json::json!(1),
        "both filters apply: {both}"
    );
}

/// The wire form the inbox consumer reads, on the stream it reads it from.
#[tokio::test]
async fn a_prompt_reaches_the_inbox_the_consumer_group_reads() {
    let backend = Arc::new(Recording::default());
    let result = call(
        backend.clone(),
        Arc::new(Roster::new(&["worker-a"])),
        serde_json::json!({ "agent_id": "worker-a", "message": "review the patch" }),
    )
    .await
    .expect("delivered");

    let published = backend.published();
    assert_eq!(published.len(), 1, "one call is one message");
    let (subject, payload) = &published[0];
    assert_eq!(
        subject,
        &cog_core::agent_inbox_stream("worker-a"),
        "the subject is built by the same function the consumer uses, not spelled here"
    );
    let decoded: InboxMessage = serde_json::from_slice(payload).expect("the consumer decodes this");
    match decoded {
        InboxMessage::Prompt { goal, reply_stream } => {
            assert_eq!(goal, serde_json::json!("review the patch"));
            assert_eq!(
                reply_stream, None,
                "a tool call cannot read a reply stream, so it does not ask for one"
            );
        }
        other => panic!("a default kind is a prompt, got {other:?}"),
    }
    assert_eq!(result["delivered"], serde_json::json!(true));
    assert_eq!(result["agent_id"], serde_json::json!("worker-a"));
    assert_eq!(result["kind"], serde_json::json!("prompt"));
}

/// Both other kinds keep the same stream and change only the variant.
#[tokio::test]
async fn steering_and_continue_keep_the_instruction_verbatim() {
    for (kind, expected) in [
        (
            "steering",
            InboxMessage::Steering {
                instruction: "stop refactoring".into(),
            },
        ),
        (
            "continue",
            InboxMessage::Continue {
                instruction: "now run the tests".into(),
            },
        ),
    ] {
        let backend = Arc::new(Recording::default());
        call(
            backend.clone(),
            Arc::new(Roster::new(&["worker-a"])),
            serde_json::json!({
                "agent_id": "worker-a",
                "message": match kind { "steering" => "stop refactoring", _ => "now run the tests" },
                "kind": kind
            }),
        )
        .await
        .unwrap_or_else(|e| panic!("{kind} delivered: {e}"));

        let published = backend.published();
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].0, cog_core::agent_inbox_stream("worker-a"));
        // Compared as JSON rather than as the enum: the variant equality is
        // what a consumer on the other side of the wire sees, and the enum
        // itself does not derive `PartialEq`.
        assert_eq!(
            serde_json::to_value(
                serde_json::from_slice::<InboxMessage>(&published[0].1).expect("decode")
            )
            .expect("encode"),
            serde_json::to_value(expected).expect("encode")
        );
    }
}

/// The roster gate: no registration means no publish.
///
/// This is the failure the tool exists to prevent. Publishing to an unowned id
/// succeeds, the entry is a non-empty stream the retirement pass must never
/// touch, and nothing will ever read it — a message that looks delivered and
/// is not.
#[tokio::test]
async fn a_message_to_an_unknown_agent_publishes_nothing() {
    let backend = Arc::new(Recording::default());
    let error = call(
        backend.clone(),
        Arc::new(Roster::new(&["worker-a"])),
        serde_json::json!({ "agent_id": "worker-gone", "message": "hello" }),
    )
    .await
    .expect_err("an unknown target is refused");

    assert!(
        error.to_string().contains("worker-gone"),
        "the error names the id that was not found, so the caller can tell a typo \
         from a dead target: {error}"
    );
    assert!(
        backend.published().is_empty(),
        "nothing may be published before the roster answers"
    );
}

/// A kind outside the closed set is refused before anything is written, rather
/// than falling back to the nearest variant.
#[tokio::test]
async fn an_unknown_kind_is_refused_and_publishes_nothing() {
    let backend = Arc::new(Recording::default());
    let error = call(
        backend.clone(),
        Arc::new(Roster::new(&["worker-a"])),
        serde_json::json!({
            "agent_id": "worker-a",
            "message": "hello",
            "kind": "urgent"
        }),
    )
    .await
    .expect_err("an unknown kind is refused");

    assert!(error.to_string().contains("urgent"), "{error}");
    assert!(backend.published().is_empty());
}

/// An empty message is a command that carries no instruction; it is refused at
/// the door rather than waking the target with nothing to do.
#[tokio::test]
async fn an_empty_message_is_refused() {
    let backend = Arc::new(Recording::default());
    let error = call(
        backend.clone(),
        Arc::new(Roster::new(&["worker-a"])),
        serde_json::json!({ "agent_id": "worker-a", "message": "   " }),
    )
    .await
    .expect_err("an empty message is refused");

    assert!(error.to_string().contains("message required"), "{error}");
    assert!(backend.published().is_empty());
}

/// The run's own identity reaches both tools, and reaches them as the same
/// answer.
///
/// A tool is shared by every agent in the process, so the handler cannot know
/// who is calling unless the run says so; what it says arrives here through the
/// task-local the dispatcher sets around the call. The mark matters for the
/// directory because an agent id is derived from a task and a role, and an
/// agent cannot reconstruct either for itself: "which of these peers is me" has
/// no other answer inside the process.
///
/// The two results are asserted together because they are one fact seen twice.
/// A receipt naming a sender the directory does not mark would mean the two
/// tools read identity from different places.
#[tokio::test]
async fn the_run_identity_reaches_the_receipt_and_the_directory_alike() {
    let roster = Arc::new(Roster::with_roles(&[
        ("squad:task-1-coder", "coder", &[]),
        ("squad:task-1-reviewer", "reviewer", &[]),
    ]));
    let backend = Arc::new(Recording::default());

    let (sent, listed) = cog_agent::tools::CALLER_AGENT_ID
        .scope("squad:task-1-reviewer".to_string(), async {
            let sent = call(
                backend.clone(),
                roster.clone(),
                serde_json::json!({ "agent_id": "squad:task-1-coder", "message": "run the tests" }),
            )
            .await
            .expect("sent");
            let listed = call_list(roster.clone(), serde_json::json!({}))
                .await
                .expect("listed");
            (sent, listed)
        })
        .await;

    assert_eq!(sent["from"], serde_json::json!("squad:task-1-reviewer"));
    assert_eq!(listed["caller"], serde_json::json!("squad:task-1-reviewer"));
    let marked: Vec<&str> = listed["agents"]
        .as_array()
        .expect("agents is a list")
        .iter()
        .filter(|a| a["is_me"] == serde_json::json!(true))
        .map(|a| a["agent_id"].as_str().expect("agent_id"))
        .collect();
    assert_eq!(
        marked,
        vec!["squad:task-1-reviewer"],
        "exactly the caller is marked: {listed}"
    );
}

/// A run that announced no identity says so, rather than being given a name.
///
/// Both tools are reachable from callers that were never told who they are —
/// a runtime built without an agent config, a tool invoked straight from a
/// test. The honest answer is `null`: a made-up id in the receipt would read as
/// "this message came from X", which is a claim about a sender that does not
/// exist, and in the directory it would mark a row at random.
#[tokio::test]
async fn a_run_with_no_identity_reports_null_and_marks_nobody() {
    let roster = Arc::new(Roster::with_roles(&[
        ("squad:task-1-coder", "coder", &[]),
        ("squad:task-1-reviewer", "reviewer", &[]),
    ]));
    let backend = Arc::new(Recording::default());

    let sent = call(
        backend.clone(),
        roster.clone(),
        serde_json::json!({ "agent_id": "squad:task-1-coder", "message": "run the tests" }),
    )
    .await
    .expect("sent");
    assert_eq!(sent["from"], serde_json::Value::Null);

    let listed = call_list(roster, serde_json::json!({}))
        .await
        .expect("listed");
    assert_eq!(listed["caller"], serde_json::Value::Null);
    assert!(
        listed["agents"]
            .as_array()
            .expect("agents is a list")
            .iter()
            .all(|a| a["is_me"] == serde_json::json!(false)),
        "no row is marked when the caller is unknown: {listed}"
    );
}

/// A queue with a target on the other end: a prompt written to an inbox is
/// treated as if that agent had run it, and the result goes to the stream the
/// request named.
///
/// The target is simulated, deliberately. What is being decided here is the
/// caller's half of the exchange — that it names a stream, opens its reader
/// before the request goes out, hands back what arrives, and releases the
/// stream afterwards. The target's half (running the prompt and publishing the
/// result) is covered where the inbox handler runs.
struct Answering {
    inner: cog_stream::MemoryMessageBackend,
    answer: Option<serde_json::Value>,
    asked: Mutex<Option<String>>,
    deleted: Mutex<Vec<String>>,
    /// What a reader on the reply stream sees: only what is published after it
    /// subscribed.
    ///
    /// A broadcast channel rather than the memory backend underneath, on
    /// purpose. That backend replays its buffer to every new subscriber, which
    /// would make the ordering bug this test exists for invisible: a caller
    /// that published first and subscribed second would still find the answer
    /// waiting in the buffer. Redis does not replay, so the fake must not
    /// either.
    replies: tokio::sync::broadcast::Sender<(String, Vec<u8>)>,
}

impl Answering {
    fn new(answer: Option<serde_json::Value>) -> Self {
        Self {
            inner: cog_stream::MemoryMessageBackend::new(),
            answer,
            asked: Mutex::new(None),
            deleted: Mutex::new(Vec::new()),
            replies: tokio::sync::broadcast::channel(16).0,
        }
    }

    fn reply_stream_asked(&self) -> Option<String> {
        self.asked.lock().expect("lock").clone()
    }

    fn deleted(&self) -> Vec<String> {
        self.deleted.lock().expect("lock").clone()
    }
}

#[async_trait]
impl MessageBackend for Answering {
    async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
        if subject.starts_with("orchestrator:agent:") {
            if let Ok(InboxMessage::Prompt {
                reply_stream: Some(stream),
                ..
            }) = serde_json::from_slice::<InboxMessage>(payload)
            {
                *self.asked.lock().expect("lock") = Some(stream.clone());
                if let Some(answer) = self.answer.as_ref() {
                    let bytes = serde_json::to_vec(answer).expect("encode answer");
                    // A subscriber that is not attached yet receives nothing:
                    // that is the property under test.
                    let _ = self.replies.send(("1-0".to_string(), bytes));
                }
            }
        }
        self.inner.publish(subject, payload).await
    }

    async fn payload_limit(&self, subject: &str) -> SFResult<Option<usize>> {
        self.inner.payload_limit(subject).await
    }

    async fn subscribe(&self, subject: &str, group: &str) -> SFResult<MessageStream> {
        if subject.starts_with("orchestrator:reply:") {
            let rx = self.replies.subscribe();
            let live = futures::stream::unfold(rx, |mut rx| async move {
                rx.recv().await.ok().map(|msg| (Ok(msg), rx))
            });
            return Ok(Box::pin(live));
        }
        self.inner.subscribe(subject, group).await
    }

    async fn subscribe_from(
        &self,
        subject: &str,
        group: &str,
        start_id: &str,
    ) -> SFResult<MessageStream> {
        self.inner.subscribe_from(subject, group, start_id).await
    }

    async fn stop_stream(&self, subject: &str, group: &str) {
        self.inner.stop_stream(subject, group).await
    }

    async fn create_consumer_group(&self, stream: &str, group: &str) -> SFResult<()> {
        self.inner.create_consumer_group(stream, group).await
    }

    async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
        self.inner.ack(stream, group, ids).await
    }

    async fn claim_pending(
        &self,
        _stream: &str,
        _group: &str,
        _min_idle_ms: u64,
        _count: usize,
    ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
        Ok(None)
    }

    async fn pending_stats(
        &self,
        _stream: &str,
        _group: &str,
        _idle_threshold_ms: u64,
    ) -> SFResult<Option<PendingStats>> {
        Ok(None)
    }

    async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
        Ok(None)
    }

    async fn delete_stream(&self, subject: &str) -> SFResult<()> {
        self.deleted.lock().expect("lock").push(subject.to_string());
        self.inner.delete_stream(subject).await
    }
}

/// An ask comes back with the target's answer.
///
/// Before the tool could wait, the one producer an agent can reach returned as
/// soon as the instruction was queued: an agent could address a peer but never
/// hear from it, so a question could only be asked by a caller that had no way
/// to use the answer. The receipt is where the answer arrives, and the stream
/// it arrived on is gone afterwards — a request-scoped stream that outlived its
/// request would be a key per question for the life of the deployment.
#[tokio::test]
async fn an_ask_returns_the_answer_and_releases_the_stream_it_arrived_on() {
    let backend = Arc::new(Answering::new(Some(
        serde_json::json!({ "ok": true, "text": "all tests pass" }),
    )));
    let roster = Arc::new(Roster::new(&["squad:task-1-tester"]));

    let receipt = call(
        backend.clone(),
        roster,
        serde_json::json!({
            "agent_id": "squad:task-1-tester",
            "message": "run the tests",
            "expect_reply": true,
            "timeout_ms": 5_000,
        }),
    )
    .await
    .expect("an answer is not an error");

    assert_eq!(receipt["delivered"], serde_json::json!(true));
    assert_eq!(receipt["replied"], serde_json::json!(true));
    assert_eq!(
        receipt["reply"]["text"],
        serde_json::json!("all tests pass")
    );

    let stream = backend
        .reply_stream_asked()
        .expect("the request told the target where to answer");
    assert!(
        stream.starts_with("orchestrator:reply:"),
        "the reply stream is named by the ask, not by the target: {stream}"
    );
    assert_eq!(
        backend.deleted(),
        vec![stream],
        "the stream the answer arrived on is removed once the ask is done"
    );
}

/// A peer that never answers is a wait that ended, not a send that failed.
///
/// The instruction was published, so nothing was lost at the sending end: the
/// target may still be running it. Reporting an error would tell the caller its
/// send failed, which is the opposite of what happened, and it would leave the
/// caller no way to tell a slow peer from a broken channel.
#[tokio::test]
async fn a_peer_that_never_answers_ends_the_wait_without_calling_the_send_a_failure() {
    let backend = Arc::new(Answering::new(None));
    let roster = Arc::new(Roster::new(&["squad:task-1-tester"]));

    let receipt = call(
        backend.clone(),
        roster,
        serde_json::json!({
            "agent_id": "squad:task-1-tester",
            "message": "run the tests",
            "expect_reply": true,
            "timeout_ms": 50,
        }),
    )
    .await
    .expect("a silent peer is not an error");

    assert_eq!(
        receipt["delivered"],
        serde_json::json!(true),
        "the instruction reached the inbox"
    );
    assert_eq!(receipt["replied"], serde_json::json!(false));
    assert_eq!(receipt["reason"], serde_json::json!("timeout"));
    assert!(
        receipt["hint"]
            .as_str()
            .expect("a hint")
            .contains("still be running"),
        "the receipt says the target may still be working on it: {receipt}"
    );
    assert_eq!(
        backend.deleted().len(),
        1,
        "a wait that gave up still releases its stream"
    );
}

/// Only a run can answer, so a command that starts none is refused before
/// anything is written.
///
/// `steering` injects into a run already in progress and `continue` appends to
/// a conversation; neither produces a result of its own. Accepting the flag
/// would spend the caller's whole timeout to learn what the command kind
/// already said.
#[tokio::test]
async fn a_command_that_starts_no_run_cannot_be_asked_for_an_answer() {
    let backend = Arc::new(Answering::new(Some(serde_json::json!({"ok": true}))));
    let roster = Arc::new(Roster::new(&["squad:task-1-tester"]));

    let err = call(
        backend.clone(),
        roster,
        serde_json::json!({
            "agent_id": "squad:task-1-tester",
            "message": "focus on the flaky one",
            "kind": "steering",
            "expect_reply": true,
        }),
    )
    .await
    .expect_err("a steering command has no answer");

    assert!(
        err.to_string().contains("kind=prompt"),
        "the error names the kind that can answer: {err}"
    );
}
