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
async fn call(
    backend: Arc<Recording>,
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
