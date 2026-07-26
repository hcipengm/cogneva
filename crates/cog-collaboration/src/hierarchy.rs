//! Tier 8 — Hierarchical Communication (Squad / Agent topology).
//! Implements the two-tier communication model defined in
//! ```text
//!   Squad (squad:{task_id}:{idx})       Sub-task PGE/Ralph instance
//!     │
//!     └── Agent (agent:{host}:{ip}:{role})  Individual reasoning unit
//! ```
//! ## Components
//! * [`BroadcastScope`] — declares the audience of a message
//!   (Squad / Agent / Global).
//! * [`HierarchicalMessage`] — wire format carrying scope, sender, payload, and
//!   trace metadata.
//! * [`SquadId`], [`AgentIdent`] — formal-identifier helpers.
//! * [`HierarchicalCommunication`] — high-level send/subscribe API backed by a
//!   pluggable [`MessageBackend`] (Redis Streams / NATS / in-memory).
//! * [`BroadcastRouter`] — chooses between the network backend and the
//!   filesystem IPC fast-path based on the message's scope.
//! * [`cross_squad_notify`] — DagExecutor-Orchestrator-mediated relay for the
//!   "Squad-A and Squad-B never talk directly" rule.
//!   Cross-Squad coordination is intentionally **not** routed through this module
//!   directly: peer Squads publish [`InterSquadMessage`] envelopes to a shared
//!   relay topic and the [`DagExecutor`](../../sf_dag_executor/index.html)
//!   consumes them, evaluates the DAG, and decides whether to schedule the next
//!   Squad.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use cog_core::{BroadcastScope, MessageBackend, MessageStream, SFError, SFResult};

use crate::ipc::{FileSystemIpc, IpcChannel, IpcMessage};

// ──────────────────────────────────────────────────────────────────────────
// Identifier helpers
// ──────────────────────────────────────────────────────────────────────────

/// Canonical formatter for `squad:{task_id}:{index}` identifiers.
#[derive(Debug, Clone, Copy)]
pub struct SquadId;

impl SquadId {
    /// Format a Squad identifier from its owning task id.
    pub fn format(task_id: &str, index: usize) -> String {
        format!("squad:{task_id}:{index}")
    }
}

/// Canonical formatter for `agent:{hostname}:{pod_ip}:{role}` identifiers.
#[derive(Debug, Clone, Copy)]
pub struct AgentIdent;

impl AgentIdent {
    /// Format an Agent identifier from host metadata + role.
    pub fn format(hostname: &str, pod_ip: &str, role: &str) -> String {
        format!("agent:{hostname}:{pod_ip}:{role}")
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Topic naming
// ──────────────────────────────────────────────────────────────────────────

/// Canonical topic generators (Redis Streams / NATS subjects).
#[derive(Debug, Clone, Copy)]
pub struct TopicName;

impl TopicName {
    /// Workspace-wide event stream subscribed by every Agent + Supervisor.
    pub fn global_events(workspace_id: &str) -> String {
        format!("orchestrator:events:{workspace_id}")
    }

    /// Workspace-wide task queue (Consumer Group consumed by Agents).
    pub fn task_queue(workspace_id: &str) -> String {
        format!("orchestrator:tasks:{workspace_id}")
    }

    /// Heartbeat stream consumed by the Supervisor only.
    pub fn heartbeat(workspace_id: &str) -> String {
        format!("orchestrator:heartbeat:{workspace_id}")
    }

    /// Per-Task event stream — visible only to members of the named Task scope.
    pub fn task_events(task_id: &str) -> String {
        format!("orchestrator:task:{task_id}:events")
    }

    /// Per-Squad shared "context board" (Roundtable shared state).
    pub fn squad_board(squad_id: &str) -> String {
        format!("orchestrator:squad:{squad_id}:board")
    }

    /// Workspace-level dead-letter queue.
    pub fn dlq(workspace_id: &str) -> String {
        format!("orchestrator:dlq:{workspace_id}")
    }

    /// Per-Agent direct inbox (point-to-point, not broadcast).
    pub fn agent_inbox(agent_id: &str) -> String {
        format!("orchestrator:agent:{agent_id}:inbox")
    }

    /// Cross-Squad relay topic consumed by the DagExecutor Orchestrator.
    pub fn cross_squad(workspace_id: &str) -> String {
        format!("orchestrator:crosssquad:{workspace_id}")
    }

    /// Per-Agent state Hash key.
    pub fn agent_state(agent_id: &str) -> String {
        format!("orchestrator:agent:{agent_id}:state")
    }

    /// Per-Agent metadata Hash key.
    pub fn agent_metadata(agent_id: &str) -> String {
        format!("orchestrator:agent:{agent_id}:metadata")
    }

    /// Task checkpoint Hash key (Snapshot ID + event offset).
    pub fn checkpoint(task_id: &str) -> String {
        format!("orchestrator:checkpoint:{task_id}")
    }

    /// DAG serialized storage key per workspace.
    pub fn dag(workspace_id: &str) -> String {
        format!("orchestrator:dag:{workspace_id}")
    }

    /// Per-Agent token quota usage counter.
    pub fn quota_token_used(agent_id: &str) -> String {
        format!("orchestrator:quota:{agent_id}:token_used")
    }

    /// Resolve the canonical topic for a [`BroadcastScope`].
    pub fn for_scope(scope: &BroadcastScope, workspace_id: &str) -> String {
        match scope {
            BroadcastScope::Crew { crew_id } => Self::task_events(crew_id),
            BroadcastScope::Squad { squad_id } => Self::squad_board(squad_id),
            BroadcastScope::Agent { agent_id } => Self::agent_inbox(agent_id),
            BroadcastScope::Global => Self::global_events(workspace_id),
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Scope and message types
// ──────────────────────────────────────────────────────────────────────────

/// Wire format for hierarchical broadcasts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HierarchicalMessage {
    /// Idempotency key.
    pub message_id: String,
    /// Where this message should be delivered.
    pub scope: BroadcastScope,
    /// Application-defined message kind (e.g. `task_start`, `pge_round_end`).
    pub message_type: String,
    /// Formal id of the sender (Agent / Squad / Supervisor).
    pub sender_id: String,
    /// Free-form payload — serialised as JSON.
    pub payload: serde_json::Value,
    /// Producer wall-clock timestamp.
    pub timestamp: DateTime<Utc>,
    /// Optional cross-component trace id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl HierarchicalMessage {
    /// Build a message with the current UTC timestamp.
    pub fn new(
        message_id: impl Into<String>,
        scope: BroadcastScope,
        message_type: impl Into<String>,
        sender_id: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            scope,
            message_type: message_type.into(),
            sender_id: sender_id.into(),
            payload,
            timestamp: Utc::now(),
            trace_id: None,
        }
    }

    /// Attach a trace id and return `self`.
    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }
}

/// Cross-Squad envelope used by the DagExecutor Orchestrator relay.
/// [`InterSquadMessage`] to the shared cross-Squad topic and the Orchestrator
/// inspects the DAG before forwarding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InterSquadMessage {
    pub message_id: String,
    pub from_squad_id: String,
    /// Intended recipient. `None` means broadcast.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to_squad_id: Option<String>,
    pub message_type: String,
    pub payload: serde_json::Value,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl InterSquadMessage {
    /// Build a directed cross-Squad message.
    pub fn directed(
        message_id: impl Into<String>,
        from_squad_id: impl Into<String>,
        to_squad_id: impl Into<String>,
        message_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            from_squad_id: from_squad_id.into(),
            to_squad_id: Some(to_squad_id.into()),
            message_type: message_type.into(),
            payload,
            timestamp: Utc::now(),
            trace_id: None,
        }
    }

    /// Build a broadcast cross-Squad message (`to_squad_id == None`).
    pub fn broadcast(
        message_id: impl Into<String>,
        from_squad_id: impl Into<String>,
        message_type: impl Into<String>,
        payload: serde_json::Value,
    ) -> Self {
        Self {
            message_id: message_id.into(),
            from_squad_id: from_squad_id.into(),
            to_squad_id: None,
            message_type: message_type.into(),
            payload,
            timestamp: Utc::now(),
            trace_id: None,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Communication facade
// ──────────────────────────────────────────────────────────────────────────

/// High-level send / subscribe API for the three-tier topology.
/// Wraps a [`MessageBackend`] (Redis Streams / NATS / in-memory) and an
/// optional [`FileSystemIpc`] for Squad-local fast-path delivery.
pub struct HierarchicalCommunication {
    backend: Arc<dyn MessageBackend>,
    workspace_id: String,
    ipc: Option<FileSystemIpc>,
}

impl HierarchicalCommunication {
    /// Build a new communication layer over the given message backend.
    pub fn new(backend: Arc<dyn MessageBackend>, workspace_id: impl Into<String>) -> Self {
        Self {
            backend,
            workspace_id: workspace_id.into(),
            ipc: None,
        }
    }

    /// Attach a filesystem-IPC fast-path for Squad-scoped messages.
    pub fn with_filesystem_ipc(mut self, ipc: FileSystemIpc) -> Self {
        self.ipc = Some(ipc);
        self
    }

    /// Workspace this layer is bound to.
    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// Underlying message backend handle (shared, refcounted).
    pub fn backend(&self) -> Arc<dyn MessageBackend> {
        Arc::clone(&self.backend)
    }

    /// Filesystem IPC handle (if configured).
    pub fn ipc(&self) -> Option<&FileSystemIpc> {
        self.ipc.as_ref()
    }

    /// Resolve the canonical topic for a scope under this workspace.
    pub fn topic_for(&self, scope: &BroadcastScope) -> String {
        TopicName::for_scope(scope, &self.workspace_id)
    }

    /// Publish a hierarchical message via the network backend.
    /// Uses the topic dictated by the message's [`BroadcastScope`].  This is
    /// the durable / multi-host path; use [`Self::squad_local_send`] for
    /// same-host Squads when an IPC fast-path is configured.
    pub async fn broadcast(&self, message: &HierarchicalMessage) -> SFResult<()> {
        let topic = self.topic_for(&message.scope);
        let payload = serde_json::to_vec(message)?;
        self.backend.publish(&topic, &payload).await
    }

    /// Subscribe to messages targeting a particular scope.
    pub async fn subscribe(
        &self,
        scope: &BroadcastScope,
        consumer_group: &str,
    ) -> SFResult<MessageStream> {
        let topic = self.topic_for(scope);
        self.backend.subscribe(&topic, consumer_group).await
    }

    /// Squad-local fast-path: write an `IpcMessage` to the Squad's inbox.
    /// Falls back to a graceful error when no IPC has been configured.
    pub async fn squad_local_send(
        &self,
        squad_id: &str,
        message: &HierarchicalMessage,
    ) -> SFResult<()> {
        let ipc = self.ipc.as_ref().ok_or_else(|| {
            SFError::Validation("HierarchicalCommunication: filesystem IPC not configured".into())
        })?;
        let ipc_msg = IpcMessage {
            id: message.message_id.clone(),
            sender: message.sender_id.clone(),
            recipient: squad_id.to_string(),
            payload: serde_json::to_value(message).map_err(SFError::Serialization)?,
            timestamp: message.timestamp,
        };
        ipc.write_message(squad_id, IpcChannel::Inbox, &ipc_msg)
            .await
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Routing
// ──────────────────────────────────────────────────────────────────────────

/// Strategy for picking between the network backend and Squad-local IPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RoutingStrategy {
    /// Always go through the [`MessageBackend`] (default).
    #[default]
    BackendOnly,
    /// Squad-scope messages go through filesystem IPC (when configured); all
    /// others use the backend.
    PreferLocalForSquad,
    /// Squad-scope messages go to **both** the IPC fast-path and the
    /// backend — useful when remote observers also need the event.
    DualSquad,
}

/// Dispatches [`HierarchicalMessage`]s through the right transport based on
/// scope.
pub struct BroadcastRouter {
    comm: HierarchicalCommunication,
    strategy: RoutingStrategy,
}

impl BroadcastRouter {
    /// Build a router that always uses the network backend.
    pub fn new(comm: HierarchicalCommunication) -> Self {
        Self {
            comm,
            strategy: RoutingStrategy::default(),
        }
    }

    /// Override the routing strategy.
    pub fn with_strategy(mut self, strategy: RoutingStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Currently-active strategy.
    pub fn strategy(&self) -> RoutingStrategy {
        self.strategy
    }

    /// Underlying communication layer (read-only access).
    pub fn comm(&self) -> &HierarchicalCommunication {
        &self.comm
    }

    /// Route a message according to the strategy + scope.
    pub async fn route(&self, message: &HierarchicalMessage) -> SFResult<()> {
        match (self.strategy, &message.scope) {
            (RoutingStrategy::PreferLocalForSquad, BroadcastScope::Squad { squad_id })
                if self.comm.ipc.is_some() =>
            {
                self.comm.squad_local_send(squad_id, message).await
            }
            (RoutingStrategy::DualSquad, BroadcastScope::Squad { squad_id })
                if self.comm.ipc.is_some() =>
            {
                // Best-effort dual delivery: IPC first, then backend.  Any
                // error from the local fast-path bubbles up; backend errors
                // are also propagated so the caller can decide on retry.
                self.comm.squad_local_send(squad_id, message).await?;
                self.comm.broadcast(message).await
            }
            _ => self.comm.broadcast(message).await,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Cross-Squad helper
// ──────────────────────────────────────────────────────────────────────────

/// Publish an [`InterSquadMessage`] onto the workspace's cross-Squad relay
/// topic.  The DagExecutor Orchestrator subscribes to this topic and is responsible
/// for forwarding (or rejecting) the message based on the DAG.
pub async fn cross_squad_notify(
    backend: &dyn MessageBackend,
    workspace_id: &str,
    message: &InterSquadMessage,
) -> SFResult<()> {
    let topic = TopicName::cross_squad(workspace_id);
    let payload = serde_json::to_vec(message)?;
    backend.publish(&topic, &payload).await
}

// ──────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::sync::broadcast;

    /// Minimal in-memory `MessageBackend` used purely for unit tests.
    /// [`cog_stream::MemoryMessageBackend`] is now available, but this local
    /// implementation avoids pulling in the full `cog-core` test surface.
    #[derive(Debug)]
    struct TestBackend {
        channels: Mutex<HashMap<String, broadcast::Sender<Vec<u8>>>>,
        buffers: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    }

    impl TestBackend {
        fn new() -> Self {
            Self {
                channels: Mutex::new(HashMap::new()),
                buffers: Mutex::new(HashMap::new()),
            }
        }

        fn sender_for(&self, subject: &str) -> broadcast::Sender<Vec<u8>> {
            let mut channels = self.channels.lock().unwrap();
            channels
                .entry(subject.to_string())
                .or_insert_with(|| broadcast::channel(64).0)
                .clone()
        }

        fn buffer_for(&self, subject: &str) -> Vec<Vec<u8>> {
            let mut buffers = self.buffers.lock().unwrap();
            buffers.entry(subject.to_string()).or_default().clone()
        }
    }

    #[async_trait]
    impl MessageBackend for TestBackend {
        async fn publish(&self, subject: &str, payload: &[u8]) -> SFResult<()> {
            let sender = self.sender_for(subject);
            let mut buffers = self.buffers.lock().unwrap();
            buffers
                .entry(subject.to_string())
                .or_default()
                .push(payload.to_vec());
            // Drop result: zero subscribers is OK in unit tests.
            let _ = sender.send(payload.to_vec());
            Ok(())
        }

        async fn subscribe(&self, subject: &str, _group: &str) -> SFResult<MessageStream> {
            let sender = self.sender_for(subject);
            let rx = sender.subscribe();
            let stream = futures::stream::unfold(rx, move |mut rx| async move {
                match rx.recv().await {
                    Ok(bytes) => Some((Ok((String::new(), bytes)), rx)),
                    Err(broadcast::error::RecvError::Closed) => None,
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        Some((Err(SFError::Backpressure), rx))
                    }
                }
            });
            Ok(Box::pin(stream))
        }

        async fn subscribe_from(
            &self,
            subject: &str,
            _group: &str,
            start_id: &str,
        ) -> SFResult<MessageStream> {
            let sender = self.sender_for(subject);
            let rx = sender.subscribe();
            let buffer = self.buffer_for(subject);

            // For TestBackend, start_id "0" or empty replays the full buffer.
            let start_offset = if start_id == "0" || start_id.is_empty() {
                0usize
            } else {
                start_id.parse::<usize>().unwrap_or(0)
            };

            let pending: Vec<(String, Vec<u8>)> = buffer
                .into_iter()
                .enumerate()
                .filter(|(idx, _)| *idx >= start_offset)
                .map(|(_, b)| (String::new(), b))
                .collect();

            let stream = futures::stream::iter(pending.into_iter().map(Ok)).chain(
                futures::stream::unfold(rx, move |mut rx| async move {
                    match rx.recv().await {
                        Ok(bytes) => Some((Ok((String::new(), bytes)), rx)),
                        Err(broadcast::error::RecvError::Closed) => None,
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            Some((Err(SFError::Backpressure), rx))
                        }
                    }
                }),
            );
            Ok(Box::pin(stream))
        }

        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
    }

    // --- identifier helpers --------------------------------------------------

    #[test]
    fn test_squad_agent_idents() {
        let squad = SquadId::format("task-001", 0);
        assert_eq!(squad, "squad:task-001:0");

        let agent = AgentIdent::format("worker-3", "10.0.0.7", "planner");
        assert_eq!(agent, "agent:worker-3:10.0.0.7:planner");
    }

    // --- topic naming --------------------------------------------------------

    #[test]
    fn test_topic_name_format() {
        assert_eq!(TopicName::global_events("ws-1"), "orchestrator:events:ws-1");
        assert_eq!(TopicName::task_queue("ws-1"), "orchestrator:tasks:ws-1");
        assert_eq!(TopicName::heartbeat("ws-1"), "orchestrator:heartbeat:ws-1");
        assert_eq!(
            TopicName::task_events("task-1"),
            "orchestrator:task:task-1:events"
        );
        assert_eq!(
            TopicName::squad_board("squad:task-1:0"),
            "orchestrator:squad:squad:task-1:0:board"
        );
        assert_eq!(TopicName::dlq("ws-1"), "orchestrator:dlq:ws-1");
        assert_eq!(
            TopicName::agent_inbox("agent:host:ip:role"),
            "orchestrator:agent:agent:host:ip:role:inbox"
        );
        assert_eq!(
            TopicName::cross_squad("ws-1"),
            "orchestrator:crosssquad:ws-1"
        );
        assert_eq!(
            TopicName::agent_state("agent:host:ip:role"),
            "orchestrator:agent:agent:host:ip:role:state"
        );
        assert_eq!(
            TopicName::agent_metadata("agent:host:ip:role"),
            "orchestrator:agent:agent:host:ip:role:metadata"
        );
        assert_eq!(
            TopicName::checkpoint("task-001"),
            "orchestrator:checkpoint:task-001"
        );
        assert_eq!(TopicName::dag("ws-1"), "orchestrator:dag:ws-1");
        assert_eq!(
            TopicName::quota_token_used("agent:host:ip:role"),
            "orchestrator:quota:agent:host:ip:role:token_used"
        );
    }

    #[test]
    fn test_topic_for_scope() {
        let task = BroadcastScope::Crew {
            crew_id: "task-1".into(),
        };
        assert_eq!(
            TopicName::for_scope(&task, "ws-1"),
            "orchestrator:task:task-1:events"
        );

        let squad = BroadcastScope::Squad {
            squad_id: "squad:task-1:0".into(),
        };
        assert_eq!(
            TopicName::for_scope(&squad, "ws-1"),
            "orchestrator:squad:squad:task-1:0:board"
        );

        let agent = BroadcastScope::Agent {
            agent_id: "agent:h:i:r".into(),
        };
        assert_eq!(
            TopicName::for_scope(&agent, "ws-1"),
            "orchestrator:agent:agent:h:i:r:inbox"
        );

        let global = BroadcastScope::Global;
        assert_eq!(
            TopicName::for_scope(&global, "ws-1"),
            "orchestrator:events:ws-1"
        );
    }

    #[test]
    fn test_scope_target_id_and_kind() {
        let task = BroadcastScope::Crew {
            crew_id: "task-1".into(),
        };
        assert_eq!(task.target_id(), "task-1");
        assert_eq!(task.kind(), "crew");

        let squad = BroadcastScope::Squad {
            squad_id: "squad:task-1:0".into(),
        };
        assert_eq!(squad.target_id(), "squad:task-1:0");
        assert_eq!(squad.kind(), "squad");

        let agent = BroadcastScope::Agent {
            agent_id: "agent:h:i:r".into(),
        };
        assert_eq!(agent.target_id(), "agent:h:i:r");
        assert_eq!(agent.kind(), "agent");

        let global = BroadcastScope::Global;
        assert_eq!(global.target_id(), "");
        assert_eq!(global.kind(), "global");
    }

    // --- message construction -----------------------------------------------

    #[test]
    fn test_hierarchical_message_with_trace_id() {
        let msg = HierarchicalMessage::new(
            "msg-1",
            BroadcastScope::Global,
            "system_alert",
            "supervisor",
            serde_json::json!({"text": "hello"}),
        )
        .with_trace_id("trace-xyz");

        assert_eq!(msg.message_id, "msg-1");
        assert_eq!(msg.message_type, "system_alert");
        assert_eq!(msg.sender_id, "supervisor");
        assert_eq!(msg.scope, BroadcastScope::Global);
        assert_eq!(msg.trace_id.as_deref(), Some("trace-xyz"));
    }

    #[test]
    fn test_inter_squad_message_directed_vs_broadcast() {
        let directed = InterSquadMessage::directed(
            "im-1",
            "squad:a",
            "squad:b",
            "task_complete",
            serde_json::json!({"task_id": "t-1"}),
        );
        assert_eq!(directed.from_squad_id, "squad:a");
        assert_eq!(directed.to_squad_id.as_deref(), Some("squad:b"));

        let broadcast = InterSquadMessage::broadcast(
            "im-2",
            "squad:a",
            "artifact_ready",
            serde_json::json!({"path": "/tmp/x"}),
        );
        assert_eq!(broadcast.from_squad_id, "squad:a");
        assert!(broadcast.to_squad_id.is_none());
    }

    // --- broadcast routing ---------------------------------------------------

    #[tokio::test]
    async fn test_broadcast_uses_scope_topic() {
        let backend = Arc::new(TestBackend::new());
        let comm = HierarchicalCommunication::new(backend.clone(), "ws-1");

        let scope = BroadcastScope::Crew {
            crew_id: "task-1".into(),
        };
        let mut sub = comm.subscribe(&scope, "consumer-1").await.unwrap();

        let msg = HierarchicalMessage::new(
            "msg-1",
            scope.clone(),
            "task_start",
            "agent:h:ip:planner",
            serde_json::json!({"task_id": "task-1"}),
        );
        comm.broadcast(&msg).await.unwrap();

        let (_, bytes) = sub.next().await.unwrap().unwrap();
        let received: HierarchicalMessage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn test_scope_isolation() {
        let backend = Arc::new(TestBackend::new());
        let comm = HierarchicalCommunication::new(backend.clone(), "ws-1");

        // Subscriber A only listens to Task A.
        let scope_a = BroadcastScope::Crew {
            crew_id: "task:A".into(),
        };
        let mut sub_a = comm.subscribe(&scope_a, "g").await.unwrap();

        // Subscriber B only listens to Task B.
        let scope_b = BroadcastScope::Crew {
            crew_id: "task:B".into(),
        };
        let _sub_b = comm.subscribe(&scope_b, "g").await.unwrap();

        // Publish only to Task B.
        let msg_b = HierarchicalMessage::new(
            "msg-b",
            scope_b.clone(),
            "task_start",
            "supervisor",
            serde_json::json!({}),
        );
        comm.broadcast(&msg_b).await.unwrap();

        // Subscriber A should *not* receive anything within a short timeout.
        let timeout =
            tokio::time::timeout(std::time::Duration::from_millis(80), sub_a.next()).await;
        assert!(
            timeout.is_err(),
            "Task A subscriber must not receive Task B messages"
        );
    }

    #[tokio::test]
    async fn test_global_scope_uses_global_topic() {
        let backend = Arc::new(TestBackend::new());
        let comm = HierarchicalCommunication::new(backend.clone(), "ws-42");

        let mut sub = comm.subscribe(&BroadcastScope::Global, "g").await.unwrap();

        let msg = HierarchicalMessage::new(
            "alert-1",
            BroadcastScope::Global,
            "system_alert",
            "supervisor",
            serde_json::json!({"severity": "high"}),
        );
        comm.broadcast(&msg).await.unwrap();

        let (_, bytes) = sub.next().await.unwrap().unwrap();
        let got: HierarchicalMessage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got.message_type, "system_alert");
    }

    // --- BroadcastRouter -----------------------------------------------------

    #[tokio::test]
    async fn test_router_default_strategy_uses_backend() {
        let backend = Arc::new(TestBackend::new());
        let comm = HierarchicalCommunication::new(backend.clone(), "ws-1");

        let scope = BroadcastScope::Squad {
            squad_id: "squad:1:0".into(),
        };
        let mut sub = comm.subscribe(&scope, "g").await.unwrap();

        let router = BroadcastRouter::new(comm);
        let msg = HierarchicalMessage::new(
            "m-1",
            scope.clone(),
            "context_board_update",
            "agent:h:ip:planner",
            serde_json::json!({"k": "v"}),
        );
        router.route(&msg).await.unwrap();

        let (_, bytes) = sub.next().await.unwrap().unwrap();
        let got: HierarchicalMessage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn test_router_prefers_ipc_for_squad_when_configured() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = Arc::new(TestBackend::new());
        let ipc = FileSystemIpc::new(tmp.path());

        let comm = HierarchicalCommunication::new(backend.clone(), "ws-1").with_filesystem_ipc(ipc);

        let scope = BroadcastScope::Squad {
            squad_id: "squad:1:0".into(),
        };

        // Subscribe on the network backend — it should *not* see this message
        // because the router takes the IPC fast-path.
        let mut net_sub = backend
            .subscribe(&TopicName::for_scope(&scope, "ws-1"), "g")
            .await
            .unwrap();

        let router = BroadcastRouter::new(comm).with_strategy(RoutingStrategy::PreferLocalForSquad);
        let msg = HierarchicalMessage::new(
            "m-2",
            scope,
            "context_board_update",
            "agent:h:ip:planner",
            serde_json::json!({"k": "v"}),
        );
        router.route(&msg).await.unwrap();

        // IPC should have the message.
        let on_disk = router
            .comm()
            .ipc()
            .unwrap()
            .read_messages("squad:1:0", IpcChannel::Inbox, None)
            .await
            .unwrap();
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].id, "m-2");

        // Network backend stayed silent.
        let timeout =
            tokio::time::timeout(std::time::Duration::from_millis(80), net_sub.next()).await;
        assert!(
            timeout.is_err(),
            "PreferLocalForSquad must not duplicate to backend"
        );
    }

    #[tokio::test]
    async fn test_router_dual_squad_writes_both() {
        let tmp = tempfile::tempdir().unwrap();
        let backend = Arc::new(TestBackend::new());
        let ipc = FileSystemIpc::new(tmp.path());

        let comm = HierarchicalCommunication::new(backend.clone(), "ws-1").with_filesystem_ipc(ipc);

        let scope = BroadcastScope::Squad {
            squad_id: "squad:1:0".into(),
        };
        let mut net_sub = backend
            .subscribe(&TopicName::for_scope(&scope, "ws-1"), "g")
            .await
            .unwrap();

        let router = BroadcastRouter::new(comm).with_strategy(RoutingStrategy::DualSquad);
        let msg = HierarchicalMessage::new(
            "m-3",
            scope,
            "context_board_update",
            "agent:h:ip:planner",
            serde_json::json!({"k": "v"}),
        );
        router.route(&msg).await.unwrap();

        // Backend side received the broadcast.
        let (_, bytes) = net_sub.next().await.unwrap().unwrap();
        let got: HierarchicalMessage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got.message_id, "m-3");

        // IPC side got the same message.
        let on_disk = router
            .comm()
            .ipc()
            .unwrap()
            .read_messages("squad:1:0", IpcChannel::Inbox, None)
            .await
            .unwrap();
        assert_eq!(on_disk.len(), 1);
        assert_eq!(on_disk[0].id, "m-3");
    }

    #[tokio::test]
    async fn test_squad_local_send_without_ipc_errors() {
        let backend = Arc::new(TestBackend::new());
        let comm = HierarchicalCommunication::new(backend, "ws-1");
        let msg = HierarchicalMessage::new(
            "m-x",
            BroadcastScope::Squad {
                squad_id: "squad:1:0".into(),
            },
            "context_board_update",
            "agent",
            serde_json::json!({}),
        );
        let err = comm.squad_local_send("squad:1:0", &msg).await.unwrap_err();
        match err {
            SFError::Validation(_) => {}
            other => panic!("expected Validation error, got {other:?}"),
        }
    }

    // --- cross-Crew relay ----------------------------------------------------

    #[tokio::test]
    async fn test_cross_squad_notify_publishes_to_relay_topic() {
        let backend: Arc<dyn MessageBackend> = Arc::new(TestBackend::new());
        // Subscriber acts as the DagExecutor Orchestrator listening on the relay
        // topic.
        let mut orch_sub = backend
            .subscribe(&TopicName::cross_squad("ws-1"), "orchestrator")
            .await
            .unwrap();

        let msg = InterSquadMessage::directed(
            "im-1",
            "squad:task-1:0",
            "squad:task-2:0",
            "task_complete",
            serde_json::json!({"artifact": "/tmp/out.json"}),
        );
        cross_squad_notify(backend.as_ref(), "ws-1", &msg)
            .await
            .unwrap();

        let (_, bytes) = orch_sub.next().await.unwrap().unwrap();
        let got: InterSquadMessage = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn test_cross_squad_messages_do_not_leak_into_task_topics() {
        let backend: Arc<dyn MessageBackend> = Arc::new(TestBackend::new());

        // A Task-B subscriber watches *only* its own task topic.  It must NOT
        // see cross-Squad envelopes — those go to the relay topic.
        let task_b_topic = TopicName::task_events("task-2");
        let mut task_b_sub = backend.subscribe(&task_b_topic, "g").await.unwrap();

        let msg = InterSquadMessage::directed(
            "im-2",
            "squad:task-1:0",
            "squad:task-2:0",
            "task_complete",
            serde_json::json!({}),
        );
        cross_squad_notify(backend.as_ref(), "ws-1", &msg)
            .await
            .unwrap();

        let timeout =
            tokio::time::timeout(std::time::Duration::from_millis(80), task_b_sub.next()).await;
        assert!(
            timeout.is_err(),
            "Task B subscriber must not receive cross-Squad envelopes directly"
        );
    }
}
