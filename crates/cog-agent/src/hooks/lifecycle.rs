//! Hierarchical lifecycle hook engine — routes [`AgentEvent`]s into scope-aware
//! 与广播规则").
//! ## Why a second engine?
//! [`super::engine::HookEngine`] is a *publisher fan-out* engine: it matches a
//! generic [`super::types::HookEvent`] against YAML-loaded
//! [`super::types::HookDef`]s and dispatches static actions like webhooks,
//! Redis Streams, or notifications.
//! [`LifecycleHookEngine`] is a complementary *runtime event router*:
//! 1. It owns an async input channel.  Producers (most importantly the
//!    [`crate::control::AgentRuntime`]) `send` lifecycle events without caring who
//!    consumes them — the design's "zero-perception" property.
//! 2. A spawned background task drains the channel.  For every event it
//!    classifies the [`HookType`] (`TaskReceived`, `ToolBefore`, …), computes
//!    every registered [`HookHandler`].
//! 3. After dispatch the engine forwards the underlying [`AgentEvent`] to a
//!    downstream `mpsc::Sender<AgentEvent>` (the "existing broadcast channel"),
//!    so consumers that already attached to the loop's event stream keep
//!    working unchanged.
//! ## Routing rules
//! ```text
//! HookType                                            BroadcastScope
//! ─────────────────────────────────────────────────── ──────────────
//! TaskReceived / TaskComplete / TaskFailed            Crew    (crew_id required)
//! PgeRoundStart / PgeRoundEnd                         Squad   (squad_id required)
//! SystemAlert                                         Global
//! ToolBefore / ToolAfter / StateTransition / ShuttingDown   (none — agent-local)
//! ```
//! The "none" rows are still delivered to handlers that opt in via
//! [`HookHandler::type_filter`] — they simply have no broadcast target.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cog_core::AgentEvent;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;

use cog_core::BroadcastScope;

/// Default capacity of the engine's input channel.
pub const DEFAULT_LIFECYCLE_CHANNEL_BUFFER: usize = 256;

/// The first seven variants are the canonical "key Hooks" called out in the
/// design.  [`PgeRoundStart`](Self::PgeRoundStart) /
/// [`PgeRoundEnd`](Self::PgeRoundEnd) and [`SystemAlert`](Self::SystemAlert)
/// and Task #1 explicitly requires those routing rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HookType {
    TaskReceived,
    TaskComplete,
    TaskFailed,
    ToolBefore,
    ToolAfter,
    StateTransition,
    ShuttingDown,
    /// PGE round opened on a Squad.
    PgeRoundStart,
    /// PGE round closed on a Squad.
    PgeRoundEnd,
    /// Workspace-wide alert (Supervisor / every Agent).
    SystemAlert,
    /// Agent checkpoint persisted.
    CheckpointSaved,
}

impl HookType {
    /// * `Crew` scope requires `crew_id`.
    /// * `Squad` scope requires `squad_id`.
    /// * Other types return `None` — they are agent-local and have no
    ///   broadcast target (handlers can still opt-in via filters).
    pub fn broadcast_scope(
        &self,
        crew_id: Option<&str>,
        squad_id: Option<&str>,
    ) -> Option<BroadcastScope> {
        match self {
            HookType::TaskReceived | HookType::TaskComplete | HookType::TaskFailed => {
                crew_id.map(|c| BroadcastScope::Crew {
                    crew_id: c.to_string(),
                })
            }
            HookType::PgeRoundStart | HookType::PgeRoundEnd => {
                squad_id.map(|s| BroadcastScope::Squad {
                    squad_id: s.to_string(),
                })
            }
            HookType::SystemAlert => Some(BroadcastScope::Global),
            HookType::CheckpointSaved => crew_id.map(|c| BroadcastScope::Crew {
                crew_id: c.to_string(),
            }),
            HookType::ToolBefore
            | HookType::ToolAfter
            | HookType::StateTransition
            | HookType::ShuttingDown => None,
        }
    }

    /// Best-effort classification of an [`AgentEvent`] into a [`HookType`].
    /// Used by [`LifecycleHookEvent::from_agent_event`] when callers don't
    /// know the precise lifecycle phase.  Mappings:
    /// * `AgentStart` → `TaskReceived`
    /// * `AgentEnd` → `TaskComplete`
    /// * `ToolExecutionStart` → `ToolBefore`
    /// * `ToolExecutionEnd` → `ToolAfter`
    /// * `StateChange` → `StateTransition`
    /// * `TaskStatusChange { status }` → `TaskComplete` / `TaskFailed` /
    ///   `TaskReceived` based on `status`.
    /// * everything else → `StateTransition` (catch-all)
    pub fn from_agent_event(event: &AgentEvent) -> Self {
        match event {
            AgentEvent::AgentStart { .. } => HookType::TaskReceived,
            AgentEvent::AgentEnd { .. } => HookType::TaskComplete,
            AgentEvent::ToolExecutionStart { .. } => HookType::ToolBefore,
            AgentEvent::ToolExecutionEnd { .. } | AgentEvent::ToolExecutionUpdate { .. } => {
                HookType::ToolAfter
            }
            AgentEvent::StateChange { .. } => HookType::StateTransition,
            AgentEvent::TaskStatusChange { status, .. } => match status.as_str() {
                "completed" | "complete" | "done" => HookType::TaskComplete,
                "failed" | "error" | "unrecoverable" => HookType::TaskFailed,
                _ => HookType::TaskReceived,
            },
            AgentEvent::AgentError { .. } => HookType::SystemAlert,
            AgentEvent::ResourceAlert { .. } => HookType::SystemAlert,
            AgentEvent::CheckpointSaved { .. } => HookType::CheckpointSaved,
            _ => HookType::StateTransition,
        }
    }
}

/// A hierarchical lifecycle event — wraps an [`AgentEvent`] together with the
/// metadata needed to compute its [`BroadcastScope`].
#[derive(Debug, Clone)]
pub struct LifecycleHookEvent {
    pub hook_type: HookType,
    pub agent_id: String,
    pub crew_id: Option<String>,
    pub squad_id: Option<String>,
    pub event: AgentEvent,
    pub timestamp: DateTime<Utc>,
}

impl LifecycleHookEvent {
    /// Construct an event with a caller-supplied [`HookType`].
    pub fn new(hook_type: HookType, agent_id: impl Into<String>, event: AgentEvent) -> Self {
        Self {
            hook_type,
            agent_id: agent_id.into(),
            crew_id: None,
            squad_id: None,
            event,
            timestamp: Utc::now(),
        }
    }

    /// Construct an event by classifying the [`AgentEvent`] with
    /// [`HookType::from_agent_event`].
    pub fn from_agent_event(agent_id: impl Into<String>, event: AgentEvent) -> Self {
        let hook_type = HookType::from_agent_event(&event);
        Self::new(hook_type, agent_id, event)
    }

    /// Builder-style: attach a Crew identifier.
    pub fn with_crew_id(mut self, crew_id: impl Into<String>) -> Self {
        self.crew_id = Some(crew_id.into());
        self
    }

    /// Builder-style: attach a Squad identifier.
    pub fn with_squad_id(mut self, squad_id: impl Into<String>) -> Self {
        self.squad_id = Some(squad_id.into());
        self
    }

    /// Compute the broadcast scope for this event (delegates to
    /// [`HookType::broadcast_scope`] using the attached `crew_id` /
    /// `squad_id`).
    pub fn scope(&self) -> Option<BroadcastScope> {
        self.hook_type
            .broadcast_scope(self.crew_id.as_deref(), self.squad_id.as_deref())
    }
}

/// Trait implemented by lifecycle hook handlers.
/// Handlers receive every event for which **both** filters match:
/// * [`type_filter`](Self::type_filter): if non-empty, only listed types pass.
/// * [`scope_filter`](Self::scope_filter): if `Some`, only events whose
///   computed scope equals it pass; if the event has no broadcast scope it is
///   skipped.
/// Handlers run sequentially inside the engine's spawned task, so they should
/// avoid long-running work; spawn additional tasks for heavy I/O.
#[async_trait]
pub trait HookHandler: Send + Sync {
    /// Optional list of [`HookType`]s this handler subscribes to.  An empty
    /// slice means "all".
    fn type_filter(&self) -> &[HookType] {
        &[]
    }

    /// Optional [`BroadcastScope`] filter.  `None` means "all events";
    /// `Some(scope)` means only events that resolve to that exact scope.
    fn scope_filter(&self) -> Option<BroadcastScope> {
        None
    }

    /// Process an event.
    async fn handle(&self, event: &LifecycleHookEvent);
}

/// Hierarchical lifecycle hook engine.
/// See the module-level documentation for the full architecture overview.
pub struct LifecycleHookEngine {
    handlers: Arc<RwLock<Vec<Arc<dyn HookHandler>>>>,
    forward_tx: Option<mpsc::Sender<AgentEvent>>,
    buffer: usize,
}

impl Default for LifecycleHookEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl LifecycleHookEngine {
    /// Build an engine with no handlers, no forward channel, and the default
    /// input buffer.
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(Vec::new())),
            forward_tx: None,
            buffer: DEFAULT_LIFECYCLE_CHANNEL_BUFFER,
        }
    }

    /// Forward the underlying [`AgentEvent`] to this channel after each
    /// dispatch.  Setting a forwarder is what lets existing consumers
    /// (broadcast subscribers / WAL replayers / metrics) keep working.
    pub fn with_forward(mut self, tx: mpsc::Sender<AgentEvent>) -> Self {
        self.forward_tx = Some(tx);
        self
    }

    /// Override the input channel buffer (default
    /// [`DEFAULT_LIFECYCLE_CHANNEL_BUFFER`]).
    pub fn with_buffer(mut self, buffer: usize) -> Self {
        self.buffer = buffer.max(1);
        self
    }

    /// Append a handler.  Safe to call before or after [`spawn`](Self::spawn).
    pub async fn register(&self, handler: Arc<dyn HookHandler>) {
        self.handlers.write().await.push(handler);
    }

    /// Number of currently registered handlers.
    pub async fn handler_count(&self) -> usize {
        self.handlers.read().await.len()
    }

    /// Snapshot the handler list.  Mostly useful for debugging.
    pub async fn list_handlers(&self) -> Vec<Arc<dyn HookHandler>> {
        self.handlers.read().await.clone()
    }

    /// Spawn the dispatch loop and return its input sender + JoinHandle.
    /// The returned [`JoinHandle`] resolves once the input channel is closed
    /// (i.e. all senders are dropped).  Callers can `abort()` it to force a
    /// shutdown.
    /// The engine itself is consumed; if you need to register more handlers
    /// after spawning, register them on the [`Arc<LifecycleHookEngine>`]
    /// before calling [`spawn_arc`](Self::spawn_arc) instead.
    pub fn spawn(self) -> (mpsc::Sender<LifecycleHookEvent>, JoinHandle<()>) {
        Arc::new(self).spawn_arc()
    }

    /// Spawn the dispatch loop using an existing [`Arc`].  Returns the input
    /// sender and a JoinHandle.  The Arc remains valid for further handler
    /// registration after the loop has started.
    pub fn spawn_arc(self: Arc<Self>) -> (mpsc::Sender<LifecycleHookEvent>, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<LifecycleHookEvent>(self.buffer);
        let engine = Arc::clone(&self);
        let handle = tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                engine.dispatch(&event).await;
                if let Some(ref forward) = engine.forward_tx {
                    let _ = forward.send(event.event).await;
                }
            }
        });
        (tx, handle)
    }

    /// In-process emission used by tests and synchronous callers — dispatches
    /// to handlers and forwards to the downstream channel without going
    /// through the engine's own input channel.
    pub async fn emit(&self, event: LifecycleHookEvent) {
        self.dispatch(&event).await;
        if let Some(ref forward) = self.forward_tx {
            let _ = forward.send(event.event).await;
        }
    }

    async fn dispatch(&self, event: &LifecycleHookEvent) {
        // Snapshot handlers so calling `handle()` doesn't hold the read lock.
        let handlers = self.handlers.read().await.clone();
        if handlers.is_empty() {
            return;
        }
        let scope = event.scope();
        for handler in handlers {
            // Type filter
            let types = handler.type_filter();
            if !types.is_empty() && !types.contains(&event.hook_type) {
                continue;
            }
            // Scope filter
            if let Some(filter) = handler.scope_filter() {
                match scope.as_ref() {
                    Some(s) if *s == filter => {}
                    _ => continue,
                }
            }
            handler.handle(event).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::Message;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    /// Recording handler — captures every event it sees.
    struct RecordingHandler {
        types: Vec<HookType>,
        scope: Option<BroadcastScope>,
        count: AtomicUsize,
        seen: StdMutex<Vec<LifecycleHookEvent>>,
    }

    impl RecordingHandler {
        fn new() -> Self {
            Self {
                types: Vec::new(),
                scope: None,
                count: AtomicUsize::new(0),
                seen: StdMutex::new(Vec::new()),
            }
        }

        fn with_types(mut self, types: Vec<HookType>) -> Self {
            self.types = types;
            self
        }

        fn with_scope(mut self, scope: BroadcastScope) -> Self {
            self.scope = Some(scope);
            self
        }
    }

    #[async_trait]
    impl HookHandler for RecordingHandler {
        fn type_filter(&self) -> &[HookType] {
            &self.types
        }

        fn scope_filter(&self) -> Option<BroadcastScope> {
            self.scope.clone()
        }

        async fn handle(&self, event: &LifecycleHookEvent) {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.seen.lock().unwrap().push(event.clone());
        }
    }

    fn agent_start_event() -> AgentEvent {
        AgentEvent::AgentStart {
            agent_id: "a-1".into(),
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    fn agent_end_event() -> AgentEvent {
        AgentEvent::AgentEnd {
            agent_id: "a-1".into(),
            messages: vec![Message::user("ok")],
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    fn tool_start_event() -> AgentEvent {
        AgentEvent::ToolExecutionStart {
            agent_id: "a-1".into(),
            tool_call_id: "tc-1".into(),
            tool_name: "noop".into(),
            args: serde_json::json!({}),
            timestamp: Utc::now(),
        }
    }

    fn state_change_event() -> AgentEvent {
        AgentEvent::StateChange {
            agent_id: "a-1".into(),
            from: "idle".into(),
            to: "thinking".into(),
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        }
    }

    // ─────────────────────────────────────────────────────────────────────
    // Routing rule tests
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn routing_task_events_go_to_crew_scope() {
        for ht in [
            HookType::TaskReceived,
            HookType::TaskComplete,
            HookType::TaskFailed,
        ] {
            let scope = ht.broadcast_scope(Some("crew:t1"), None);
            assert_eq!(
                scope,
                Some(BroadcastScope::Crew {
                    crew_id: "crew:t1".into()
                }),
                "{:?} must route to Crew scope",
                ht
            );
        }
    }

    #[test]
    fn routing_pge_rounds_go_to_squad_scope() {
        for ht in [HookType::PgeRoundStart, HookType::PgeRoundEnd] {
            let scope = ht.broadcast_scope(None, Some("squad:c:0"));
            assert_eq!(
                scope,
                Some(BroadcastScope::Squad {
                    squad_id: "squad:c:0".into()
                }),
                "{:?} must route to Squad scope",
                ht
            );
        }
    }

    #[test]
    fn routing_checkpoint_saved_goes_to_crew_scope() {
        let scope = HookType::CheckpointSaved.broadcast_scope(Some("crew:c1"), None);
        assert_eq!(
            scope,
            Some(BroadcastScope::Crew {
                crew_id: "crew:c1".into()
            }),
            "CheckpointSaved must route to Crew scope"
        );
    }

    #[test]
    fn routing_checkpoint_saved_without_crew_id_has_no_scope() {
        assert_eq!(HookType::CheckpointSaved.broadcast_scope(None, None), None);
    }

    #[test]
    fn routing_system_alert_goes_to_global_scope() {
        let scope = HookType::SystemAlert.broadcast_scope(None, None);
        assert_eq!(scope, Some(BroadcastScope::Global));
    }

    #[test]
    fn routing_local_lifecycle_hooks_have_no_scope() {
        for ht in [
            HookType::ToolBefore,
            HookType::ToolAfter,
            HookType::StateTransition,
            HookType::ShuttingDown,
        ] {
            assert_eq!(
                ht.broadcast_scope(Some("crew:x"), Some("squad:x")),
                None,
                "{:?} must not have a broadcast scope",
                ht
            );
        }
    }

    #[test]
    fn routing_task_events_without_crew_id_have_no_scope() {
        // Agent-local lifecycle (no crew_id attached) → not broadcast.
        assert_eq!(HookType::TaskReceived.broadcast_scope(None, None), None);
    }

    #[test]
    fn routing_pge_events_without_squad_id_have_no_scope() {
        assert_eq!(HookType::PgeRoundStart.broadcast_scope(None, None), None);
    }

    // ─────────────────────────────────────────────────────────────────────
    // AgentEvent → HookType classification
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn classify_checkpoint_saved_as_checkpoint_saved() {
        let evt = AgentEvent::CheckpointSaved {
            agent_id: "a-1".into(),
            checkpoint_id: "snap-1".into(),
            task_id: "t-1".into(),
            crew_id: Some("crew:c1".into()),
            squad_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(HookType::from_agent_event(&evt), HookType::CheckpointSaved);
    }

    #[test]
    fn classify_agent_start_as_task_received() {
        assert_eq!(
            HookType::from_agent_event(&agent_start_event()),
            HookType::TaskReceived
        );
    }

    #[test]
    fn classify_agent_end_as_task_complete() {
        assert_eq!(
            HookType::from_agent_event(&agent_end_event()),
            HookType::TaskComplete
        );
    }

    #[test]
    fn classify_tool_execution_events() {
        assert_eq!(
            HookType::from_agent_event(&tool_start_event()),
            HookType::ToolBefore
        );
        let tool_end = AgentEvent::ToolExecutionEnd {
            agent_id: "a-1".into(),
            tool_call_id: "tc-1".into(),
            result: serde_json::Value::Null,
            is_error: false,
            timestamp: Utc::now(),
        };
        assert_eq!(HookType::from_agent_event(&tool_end), HookType::ToolAfter);
    }

    #[test]
    fn classify_state_change_as_state_transition() {
        assert_eq!(
            HookType::from_agent_event(&state_change_event()),
            HookType::StateTransition
        );
    }

    #[test]
    fn classify_task_status_change() {
        let completed = AgentEvent::TaskStatusChange {
            task_id: "t-1".into(),
            status: "completed".into(),
            agent_id: Some("a-1".into()),
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(
            HookType::from_agent_event(&completed),
            HookType::TaskComplete
        );

        let failed = AgentEvent::TaskStatusChange {
            task_id: "t-1".into(),
            status: "failed".into(),
            agent_id: None,
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(HookType::from_agent_event(&failed), HookType::TaskFailed);

        let other = AgentEvent::TaskStatusChange {
            task_id: "t-1".into(),
            status: "pending".into(),
            agent_id: None,
            crew_id: None,
            squad_id: None,
            timestamp: Utc::now(),
        };
        assert_eq!(HookType::from_agent_event(&other), HookType::TaskReceived);
    }

    // ─────────────────────────────────────────────────────────────────────
    // LifecycleHookEvent constructors and scope resolution
    // ─────────────────────────────────────────────────────────────────────

    #[test]
    fn lifecycle_event_scope_combines_metadata() {
        let evt = LifecycleHookEvent::new(HookType::TaskComplete, "a-1", agent_end_event())
            .with_crew_id("crew:t1");
        assert_eq!(
            evt.scope(),
            Some(BroadcastScope::Crew {
                crew_id: "crew:t1".into()
            })
        );
    }

    #[test]
    fn lifecycle_event_from_agent_event_classifies_type() {
        let evt = LifecycleHookEvent::from_agent_event("a-1", agent_start_event());
        assert_eq!(evt.hook_type, HookType::TaskReceived);
        assert_eq!(evt.agent_id, "a-1");
        assert!(evt.crew_id.is_none());
        assert!(evt.squad_id.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Engine dispatch tests
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn dispatch_calls_all_unfiltered_handlers() {
        let engine = LifecycleHookEngine::new();
        let h1 = Arc::new(RecordingHandler::new());
        let h2 = Arc::new(RecordingHandler::new());
        engine.register(h1.clone()).await;
        engine.register(h2.clone()).await;
        assert_eq!(engine.handler_count().await, 2);

        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                agent_start_event(),
            ))
            .await;

        assert_eq!(h1.count.load(Ordering::SeqCst), 1);
        assert_eq!(h2.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_respects_type_filter() {
        let engine = LifecycleHookEngine::new();
        let only_tasks = Arc::new(
            RecordingHandler::new()
                .with_types(vec![HookType::TaskReceived, HookType::TaskComplete]),
        );
        engine.register(only_tasks.clone()).await;

        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                agent_start_event(),
            ))
            .await;
        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                state_change_event(),
            ))
            .await;
        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                tool_start_event(),
            ))
            .await;

        // Only the AgentStart event matches the type filter.
        assert_eq!(only_tasks.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_respects_scope_filter_for_crew() {
        let engine = LifecycleHookEngine::new();
        let crew_handler = Arc::new(RecordingHandler::new().with_scope(BroadcastScope::Crew {
            crew_id: "crew:wanted".into(),
        }));
        engine.register(crew_handler.clone()).await;

        // Matching crew → delivered.
        engine
            .emit(
                LifecycleHookEvent::from_agent_event("a-1", agent_end_event())
                    .with_crew_id("crew:wanted"),
            )
            .await;
        // Different crew → filtered out.
        engine
            .emit(
                LifecycleHookEvent::from_agent_event("a-1", agent_end_event())
                    .with_crew_id("crew:other"),
            )
            .await;
        // No crew metadata → filtered out (no scope to compare).
        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                agent_end_event(),
            ))
            .await;

        assert_eq!(crew_handler.count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn dispatch_respects_scope_filter_for_global() {
        let engine = LifecycleHookEngine::new();
        let global_handler = Arc::new(RecordingHandler::new().with_scope(BroadcastScope::Global));
        engine.register(global_handler.clone()).await;

        engine
            .emit(LifecycleHookEvent::new(
                HookType::SystemAlert,
                "supervisor",
                state_change_event(),
            ))
            .await;
        // A non-Global event doesn't match the Global filter.
        engine
            .emit(
                LifecycleHookEvent::new(HookType::TaskComplete, "a-1", agent_end_event())
                    .with_crew_id("crew:1"),
            )
            .await;

        assert_eq!(global_handler.count.load(Ordering::SeqCst), 1);
    }

    // ─────────────────────────────────────────────────────────────────────
    // Spawned-task + forwarding tests
    // ─────────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn spawn_dispatches_via_channel_and_forwards() {
        let (forward_tx, mut forward_rx) = mpsc::channel::<AgentEvent>(8);
        let engine = LifecycleHookEngine::new().with_forward(forward_tx);
        let handler = Arc::new(RecordingHandler::new());

        // Register before spawn so the dispatch loop already sees the handler
        // when it processes the first event.
        let engine = Arc::new(engine);
        engine.register(handler.clone()).await;

        let (tx, _join) = engine.clone().spawn_arc();

        tx.send(LifecycleHookEvent::from_agent_event(
            "a-1",
            agent_start_event(),
        ))
        .await
        .unwrap();
        tx.send(LifecycleHookEvent::from_agent_event(
            "a-1",
            agent_end_event(),
        ))
        .await
        .unwrap();

        // Forward channel should receive both AgentEvents in order.
        let first = forward_rx.recv().await.unwrap();
        assert!(matches!(first, AgentEvent::AgentStart { .. }));
        let second = forward_rx.recv().await.unwrap();
        assert!(matches!(second, AgentEvent::AgentEnd { .. }));

        // Handler should have observed both events too.
        // Wait briefly for handler dispatch to settle.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(handler.count.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn spawn_join_handle_completes_when_input_dropped() {
        let engine = LifecycleHookEngine::new();
        let (tx, join) = engine.spawn();
        drop(tx);
        // Should resolve once the input channel closes.
        join.await.unwrap();
    }

    #[tokio::test]
    async fn emit_without_handlers_is_noop_but_still_forwards() {
        let (forward_tx, mut forward_rx) = mpsc::channel::<AgentEvent>(2);
        let engine = LifecycleHookEngine::new().with_forward(forward_tx);
        engine
            .emit(LifecycleHookEvent::from_agent_event(
                "a-1",
                agent_start_event(),
            ))
            .await;
        let got = forward_rx.recv().await.unwrap();
        assert!(matches!(got, AgentEvent::AgentStart { .. }));
    }
}
