use cog_core::{AgentEvent, AgentState, InboxMessage, SFError, SFResult};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

use crate::agent_kernel::runtime::{AgentHooks, AgentRuntime};
use crate::lifecycle::LifecycleManager;
use crate::tools::ToolRegistry;
use cog_core::RuntimeConfig;

use futures::StreamExt;

pub enum AgentCommand {
    Prompt {
        input: serde_json::Value,
        result_tx: oneshot::Sender<SFResult<serde_json::Value>>,
    },
    Continue {
        input: serde_json::Value,
        result_tx: oneshot::Sender<SFResult<serde_json::Value>>,
    },
    Steer {
        instruction: String,
    },
    #[allow(dead_code)]
    Reset,
    Snapshot {
        task_id: String,
        result_tx: oneshot::Sender<SFResult<cog_core::AgentCheckpoint>>,
    },
}

/// High-level Agent wrapper with consumer-facing API.
/// Wraps `AgentRuntime` and manages its lifecycle via a background task.
/// Events are broadcast so multiple consumers can subscribe.
/// # Example
/// ```ignore
/// use cog_agent::{Agent, RuntimeConfig, AgentRole};
/// use std::sync::Arc;
/// # async fn example() -> cog_core::SFResult<()> {
/// let agent = Agent::new(
///     RuntimeConfig {
///         agent_id: "my-agent".into(),
///         role: AgentRole::Planner,
///         max_iterations: 10,
///         context_window_size: 4000,
///     },
///     Arc::new(my_llm_provider),
/// );
/// agent.start().await;
/// let mut events = agent.subscribe();
/// let result = agent.prompt(serde_json::json!({"goal": "plan something"})).await?;
/// # Ok(())
/// # }
/// ```
pub struct Agent {
    config: RuntimeConfig,
    tools: ToolRegistry,
    hooks: AgentHooks,
    llm: Arc<dyn cog_core::LlmClient>,
    event_tx: broadcast::Sender<AgentEvent>,
    inner: Arc<Mutex<AgentInner>>,
    wal: Option<Arc<crate::wal::AgentWal>>,
    lifecycle: Option<Arc<LifecycleManager>>,
    raw_logger: Option<Arc<dyn cog_core::RawLogger>>,
    /// Global agent registry for multi-agent discovery.
    registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    /// Registration payload used when enrolling with the registry on startup.
    registration: Option<cog_core::AgentRegistration>,
    /// Message backend for inter-agent communication (Redis Streams).
    message_backend: Option<Arc<dyn cog_core::MessageBackend>>,
    /// State backend for shared ContextBoard read/write.
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    /// Working memory for anti-loop context management.
    pub working_memory: Option<crate::working_memory::AgentWorkingMemory>,
    /// Cross-session reflection engine for learning detection and promotion.
    reflection_engine: Option<Arc<dyn cog_core::ReflectionEngine>>,
    /// Checkpoint store for agent state persistence.
    checkpoint_store: Option<Arc<dyn cog_core::CheckpointStore>>,
    /// Sandbox backend for WASM tool execution.
    sandbox_backend: Option<Arc<dyn cog_core::SandboxBackend>>,
    /// Plugin registry for fetching WASM tool bytes.
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
    /// External skill registry for injecting available_skills into system prompt.
    external_skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    /// Guardrail for automated safety checks on inputs/outputs/tool calls.
    guardrail: Option<Arc<dyn cog_core::Guardrail>>,
    /// Broadcast channel capacity for agent events.
    event_channel_capacity: usize,
    /// mpsc channel capacity for agent commands.
    cmd_channel_capacity: usize,
    /// mpsc channel capacity for loop-internal events.
    loop_event_channel_capacity: usize,
    /// Heartbeat interval in seconds.
    heartbeat_interval_secs: u64,
    /// Poll interval for wait_for_idle in milliseconds.
    wait_for_idle_poll_ms: u64,
}

struct AgentInner {
    state: AgentState,
    cmd_tx: Option<mpsc::Sender<AgentCommand>>,
    task_handle: Option<tokio::task::JoinHandle<()>>,
    consumer_handle: Option<tokio::task::JoinHandle<()>>,
    registry_heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Agent {
    pub fn new(config: RuntimeConfig, llm: Arc<dyn cog_core::LlmClient>) -> Self {
        Self::with_channel_capacities(config, llm, 256, 16, 128)
    }

    /// Create an Agent with explicit channel capacities.
    pub fn with_channel_capacities(
        config: RuntimeConfig,
        llm: Arc<dyn cog_core::LlmClient>,
        event_channel_capacity: usize,
        cmd_channel_capacity: usize,
        loop_event_channel_capacity: usize,
    ) -> Self {
        let (event_tx, _) = broadcast::channel::<AgentEvent>(event_channel_capacity);
        Self {
            config,
            tools: ToolRegistry::new(),
            hooks: AgentHooks::default(),
            llm,
            event_tx,
            inner: Arc::new(Mutex::new(AgentInner {
                state: AgentState::Idle,
                cmd_tx: None,
                task_handle: None,
                consumer_handle: None,
                registry_heartbeat_handle: None,
            })),
            wal: None,
            lifecycle: None,
            raw_logger: None,
            registry: None,
            registration: None,
            message_backend: None,
            state_backend: None,
            working_memory: None,
            reflection_engine: None,
            checkpoint_store: None,
            sandbox_backend: None,
            plugin_registry: None,
            external_skill_registry: None,
            guardrail: None,
            event_channel_capacity,
            cmd_channel_capacity,
            loop_event_channel_capacity,
            heartbeat_interval_secs: 10,
            wait_for_idle_poll_ms: 50,
        }
    }

    pub fn with_raw_logger(mut self, logger: Arc<dyn cog_core::RawLogger>) -> Self {
        self.raw_logger = Some(logger);
        self
    }

    /// Set broadcast channel capacity for agent events.
    pub fn with_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.event_channel_capacity = capacity;
        self
    }

    /// Publish onto the shared cluster-wide event bus instead of a private
    /// per-agent channel, so live observers (WebSocket clients) see this
    /// agent's turns, streaming output, and tool executions as they happen.
    /// Must be called before `start` — subscribers of the old channel would
    /// silently stop receiving events afterwards.
    pub fn with_event_bus(mut self, tx: broadcast::Sender<AgentEvent>) -> Self {
        self.event_tx = tx;
        self
    }

    /// Set mpsc channel capacity for agent commands.
    pub fn with_cmd_channel_capacity(mut self, capacity: usize) -> Self {
        self.cmd_channel_capacity = capacity;
        self
    }

    /// Set mpsc channel capacity for loop-internal events.
    pub fn with_loop_event_channel_capacity(mut self, capacity: usize) -> Self {
        self.loop_event_channel_capacity = capacity;
        self
    }

    /// Set heartbeat interval in seconds.
    pub fn with_heartbeat_interval(mut self, secs: u64) -> Self {
        self.heartbeat_interval_secs = secs;
        self
    }

    /// Set poll interval for wait_for_idle in milliseconds.
    pub fn with_wait_for_idle_poll_ms(mut self, ms: u64) -> Self {
        self.wait_for_idle_poll_ms = ms;
        self
    }

    pub fn with_lifecycle(mut self, lifecycle: Arc<LifecycleManager>) -> Self {
        self.lifecycle = Some(lifecycle);
        self
    }

    pub fn with_tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
        self
    }

    pub fn with_hooks(mut self, hooks: AgentHooks) -> Self {
        self.hooks = hooks;
        self
    }

    /// Configure the checkpoint store for agent state persistence.
    pub fn with_checkpoint_store(mut self, store: Arc<dyn cog_core::CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    pub fn with_sandbox_backend(mut self, backend: Arc<dyn cog_core::SandboxBackend>) -> Self {
        self.sandbox_backend = Some(backend);
        self
    }

    pub fn with_plugin_registry(mut self, registry: Arc<dyn cog_core::PluginRegistry>) -> Self {
        self.plugin_registry = Some(registry);
        self
    }

    pub fn with_external_skill_registry(
        mut self,
        registry: Arc<dyn cog_core::ExternalSkillRegistry>,
    ) -> Self {
        self.external_skill_registry = Some(registry);
        self
    }

    pub fn with_guardrail(mut self, guardrail: Arc<dyn cog_core::Guardrail>) -> Self {
        self.guardrail = Some(guardrail);
        self
    }

    pub fn with_wal(mut self, wal: Arc<crate::wal::AgentWal>) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Configure the global agent registry.
    /// When set, `start()` will automatically register this agent.
    pub fn with_registry(mut self, registry: Arc<dyn cog_core::AgentRegistry>) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Configure the registration payload for global registry enrollment.
    pub fn with_registration(mut self, registration: cog_core::AgentRegistration) -> Self {
        self.registration = Some(registration);
        self
    }

    /// Configure the message backend for inter-agent communication.
    pub fn with_message_backend(mut self, backend: Arc<dyn cog_core::MessageBackend>) -> Self {
        self.message_backend = Some(backend);
        self
    }

    /// Configure the state backend for shared ContextBoard access.
    pub fn with_state_backend(mut self, backend: Arc<dyn cog_core::StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Configure the working memory for anti-loop context management.
    pub fn with_working_memory(
        mut self,
        memory: crate::working_memory::AgentWorkingMemory,
    ) -> Self {
        self.working_memory = Some(memory);
        self
    }

    /// Attach a reflection engine for cross-session learning and
    /// self-improvement.  When configured, the agent automatically
    /// detects learnings from events, tool results, and context windows.
    pub fn with_reflection_engine(mut self, engine: Arc<dyn cog_core::ReflectionEngine>) -> Self {
        self.reflection_engine = Some(engine);
        self
    }

    /// Start the background agent task. Idempotent.
    /// When a [`LifecycleManager`] is configured this also drives the
    /// state machine: `Init → Registered → Idle`.
    pub async fn start(&self) {
        let mut inner = self.inner.lock().await;
        if inner.cmd_tx.is_some() {
            return;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(self.cmd_channel_capacity);
        let event_tx = self.event_tx.clone();
        let config = self.config.clone();
        let mut tools = self.tools.clone();
        if let Some(ref sb) = self.sandbox_backend {
            tools.set_sandbox_backend(sb.clone());
        }
        if let Some(ref gr) = self.guardrail {
            tools.set_guardrail(gr.clone());
        }
        if let Some(ref pr) = self.plugin_registry {
            tools.set_plugin_registry(pr.clone());
        }
        let hooks = self.hooks.clone();
        let llm = self.llm.clone();
        let wal = self.wal.clone();
        let raw_logger = self.raw_logger.clone();
        let reflection_engine = self.reflection_engine.clone();
        let checkpoint_store = self.checkpoint_store.clone();
        let sandbox_backend = self.sandbox_backend.clone();
        let plugin_registry = self.plugin_registry.clone();
        let external_skill_registry = self.external_skill_registry.clone();

        let loop_event_cap = self.loop_event_channel_capacity;
        let handle = tokio::spawn(async move {
            let (loop_event_tx, mut loop_event_rx) = mpsc::channel(loop_event_cap);
            let mut agent_loop = AgentRuntime::new(config, loop_event_tx)
                .with_tools(tools)
                .with_hooks(hooks);
            if let Some(ref sb) = sandbox_backend {
                agent_loop = agent_loop.with_sandbox_backend(sb.clone());
            }
            if let Some(ref pr) = plugin_registry {
                agent_loop = agent_loop.with_plugin_registry(pr.clone());
                if let Some(ref esr) = external_skill_registry {
                    agent_loop = agent_loop.with_external_skill_registry(esr.clone());
                }
            }
            if let Some(ref w) = wal {
                agent_loop = agent_loop.with_wal(w.clone());
            }
            if let Some(ref rl) = raw_logger {
                agent_loop = agent_loop.with_raw_logger(rl.clone());
            }
            if let Some(ref re) = reflection_engine {
                agent_loop = agent_loop.with_reflection_engine(re.clone());
                let _reviewer_handle = re.start_reviewer();
            }
            if let Some(ref cp) = checkpoint_store {
                agent_loop = agent_loop.with_checkpoint_store(cp.clone());
            }

            // Forward events from AgentRuntime mpsc to Agent broadcast
            let forward_event_tx = event_tx.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(event) = loop_event_rx.recv().await {
                    let _ = forward_event_tx.send(event);
                }
            });

            run_agent_task(&mut agent_loop, cmd_rx, event_tx, llm, loop_event_cap).await;

            forward_handle.abort();
        });

        inner.cmd_tx = Some(cmd_tx);
        inner.task_handle = Some(handle);
        inner.state = AgentState::Idle;

        // Lifecycle integration: Init → Registered → Idle + heartbeat
        if let Some(ref lifecycle) = self.lifecycle {
            let agent_id = &self.config.agent_id;
            let _ = lifecycle.register(agent_id).await;
            let _ = lifecycle.transition(agent_id, AgentState::Registered).await;
            let _ = lifecycle.transition(agent_id, AgentState::Idle).await;
            lifecycle.start_heartbeat(agent_id).await;
        }

        // Global registry integration: register this agent so supervisors
        // and orchestrators can discover it.
        if let Some(ref registry) = self.registry {
            if let Some(ref registration) = self.registration {
                let _ = registry.register(registration).await;
                // Spawn periodic heartbeat to keep the registration alive.
                let agent_id = registration.agent_id.clone();
                let registry = registry.clone();
                let interval = std::time::Duration::from_secs(self.heartbeat_interval_secs);
                let heartbeat_handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        ticker.tick().await;
                        if let Err(e) = registry.heartbeat(&agent_id).await {
                            tracing::warn!("Registry heartbeat failed for {}: {}", agent_id, e);
                        }
                    }
                });
                inner.registry_heartbeat_handle = Some(heartbeat_handle);
            }
        }
    }

    /// Subscribe to agent lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<AgentEvent> {
        self.event_tx.subscribe()
    }

    /// Send a new prompt and start a fresh agent run.
    /// State machine: `Idle → Active → Completing → Idle` on success,
    /// `Active → Dead` on failure.
    pub async fn prompt(&self, input: serde_json::Value) -> SFResult<serde_json::Value> {
        self.start().await;
        let (result_tx, result_rx) = oneshot::channel();

        {
            let mut inner = self.inner.lock().await;
            inner.state = AgentState::Active;
            let cmd_tx = inner
                .cmd_tx
                .as_ref()
                .ok_or_else(|| SFError::Agent("Agent not started".into()))?;
            cmd_tx
                .send(AgentCommand::Prompt { input, result_tx })
                .await
                .map_err(|_| SFError::Agent("Command channel closed".into()))?;
        }

        if let Some(ref lifecycle) = self.lifecycle {
            let _ = lifecycle
                .transition(&self.config.agent_id, AgentState::Active)
                .await;
        }

        match result_rx.await {
            Ok(result) => {
                let mut inner = self.inner.lock().await;
                if result.is_ok() {
                    inner.state = AgentState::Idle;
                    if let Some(ref lifecycle) = self.lifecycle {
                        let agent_id = &self.config.agent_id;
                        let _ = lifecycle.transition(agent_id, AgentState::Completing).await;
                        let _ = lifecycle.transition(agent_id, AgentState::Idle).await;
                    }
                } else {
                    inner.state = AgentState::Dead;
                    if let Some(ref lifecycle) = self.lifecycle {
                        let _ = lifecycle
                            .transition(&self.config.agent_id, AgentState::Dead)
                            .await;
                    }
                }
                result
            }
            Err(_) => {
                let mut inner = self.inner.lock().await;
                inner.state = AgentState::Dead;
                if let Some(ref lifecycle) = self.lifecycle {
                    let _ = lifecycle
                        .transition(&self.config.agent_id, AgentState::Dead)
                        .await;
                }
                Err(SFError::Agent("Task aborted or panicked".into()))
            }
        }
    }

    /// Continue the conversation with additional input.
    /// Context is preserved across calls.
    pub async fn continue_(&self, input: serde_json::Value) -> SFResult<serde_json::Value> {
        self.start().await;
        let (result_tx, result_rx) = oneshot::channel();

        {
            let mut inner = self.inner.lock().await;
            inner.state = AgentState::Active;
            let cmd_tx = inner
                .cmd_tx
                .as_ref()
                .ok_or_else(|| SFError::Agent("Agent not started".into()))?;
            cmd_tx
                .send(AgentCommand::Continue { input, result_tx })
                .await
                .map_err(|_| SFError::Agent("Command channel closed".into()))?;
        }

        if let Some(ref lifecycle) = self.lifecycle {
            let _ = lifecycle
                .transition(&self.config.agent_id, AgentState::Active)
                .await;
        }

        match result_rx.await {
            Ok(result) => {
                let mut inner = self.inner.lock().await;
                if result.is_ok() {
                    inner.state = AgentState::Idle;
                    if let Some(ref lifecycle) = self.lifecycle {
                        let agent_id = &self.config.agent_id;
                        let _ = lifecycle.transition(agent_id, AgentState::Completing).await;
                        let _ = lifecycle.transition(agent_id, AgentState::Idle).await;
                    }
                } else {
                    inner.state = AgentState::Dead;
                    if let Some(ref lifecycle) = self.lifecycle {
                        let _ = lifecycle
                            .transition(&self.config.agent_id, AgentState::Dead)
                            .await;
                    }
                }
                result
            }
            Err(_) => {
                let mut inner = self.inner.lock().await;
                inner.state = AgentState::Dead;
                if let Some(ref lifecycle) = self.lifecycle {
                    let _ = lifecycle
                        .transition(&self.config.agent_id, AgentState::Dead)
                        .await;
                }
                Err(SFError::Agent("Task aborted or panicked".into()))
            }
        }
    }

    /// Send a steering instruction (injected as a system message).
    pub async fn steer(&self, instruction: String) -> SFResult<()> {
        self.start().await;
        let inner = self.inner.lock().await;
        let cmd_tx = inner
            .cmd_tx
            .as_ref()
            .ok_or_else(|| SFError::Agent("Agent not started".into()))?;
        cmd_tx
            .send(AgentCommand::Steer { instruction })
            .await
            .map_err(|_| SFError::Agent("Command channel closed".into()))?;
        Ok(())
    }

    /// Abort the current run and reset to idle.
    /// State machine: `Active → Completing → Inactive → Init → Registered → Idle`.
    pub async fn abort(&self) -> SFResult<()> {
        if let Some(ref lifecycle) = self.lifecycle {
            let agent_id = &self.config.agent_id;
            let _ = lifecycle.stop_heartbeat(agent_id).await;
            let _ = lifecycle.transition(agent_id, AgentState::Completing).await;
            let _ = lifecycle.transition(agent_id, AgentState::Inactive).await;
        }

        {
            let mut inner = self.inner.lock().await;
            if let Some(handle) = inner.task_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.consumer_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.registry_heartbeat_handle.take() {
                handle.abort();
            }
            inner.cmd_tx = None;
            inner.state = AgentState::Inactive;
        }

        // Restart the background task
        self.start().await;
        Ok(())
    }

    /// Wait until the agent is no longer active or completing.
    pub async fn wait_for_idle(&self) -> SFResult<()> {
        loop {
            let state = {
                let inner = self.inner.lock().await;
                inner.state
            };
            if !matches!(state, AgentState::Active | AgentState::Completing) {
                return Ok(());
            }
            tokio::time::sleep(std::time::Duration::from_millis(self.wait_for_idle_poll_ms)).await;
        }
    }

    /// Capture a snapshot of the current agent state.
    /// Returns a [`cog_core::AgentCheckpoint`] that can be persisted and later
    /// restored via [`Self::restore`].
    pub async fn snapshot(
        &self,
        task_id: impl Into<String>,
    ) -> SFResult<cog_core::AgentCheckpoint> {
        self.start().await;
        let (result_tx, result_rx) = oneshot::channel();

        let inner = self.inner.lock().await;
        let cmd_tx = inner
            .cmd_tx
            .as_ref()
            .ok_or_else(|| SFError::Agent("Agent not started".into()))?;
        cmd_tx
            .send(AgentCommand::Snapshot {
                task_id: task_id.into(),
                result_tx,
            })
            .await
            .map_err(|_| SFError::Agent("Command channel closed".into()))?;
        drop(inner);

        result_rx
            .await
            .map_err(|_| SFError::Agent("Snapshot task aborted".into()))?
    }

    /// Restore agent state from a snapshot.
    /// Aborts any in-flight work, restores context and state from the
    /// snapshot, and restarts the background task so that execution
    /// can continue.
    pub async fn restore(&self, snapshot: &cog_core::AgentCheckpoint) -> SFResult<()> {
        // Abort current task and reset channels
        {
            let mut inner = self.inner.lock().await;
            if let Some(handle) = inner.task_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.registry_heartbeat_handle.take() {
                handle.abort();
            }
            inner.cmd_tx = None;
            inner.state = AgentState::Idle;
        }

        // Restart background task with restored snapshot
        self.start_with_snapshot(snapshot.clone()).await;
        Ok(())
    }

    /// Restore agent state from a persisted checkpoint by id.
    /// Loads the checkpoint from the configured store, then restores
    /// the agent state and replays events from the stored offset.
    pub async fn restore_from_id(&self, checkpoint_id: &str) -> SFResult<()> {
        let store = self
            .checkpoint_store
            .as_ref()
            .ok_or_else(|| SFError::Agent("No checkpoint store configured".into()))?;
        let checkpoint = store
            .load(checkpoint_id)
            .await
            .map_err(|e| SFError::Agent(format!("Checkpoint load failed: {e}")))?
            .ok_or_else(|| SFError::Agent(format!("Checkpoint not found: {checkpoint_id}")))?;
        self.restore(&checkpoint).await
    }

    /// Start the background agent task with a pre-restored snapshot.
    /// This is like [`start`](Self::start) but seeds the `AgentRuntime`
    /// with state from a snapshot instead of a blank slate.
    async fn start_with_snapshot(&self, snapshot: cog_core::AgentCheckpoint) {
        let mut inner = self.inner.lock().await;
        if inner.cmd_tx.is_some() {
            return;
        }

        let (cmd_tx, cmd_rx) = mpsc::channel(self.cmd_channel_capacity);
        let event_tx = self.event_tx.clone();
        let config = self.config.clone();
        let mut tools = self.tools.clone();
        if let Some(ref sb) = self.sandbox_backend {
            tools.set_sandbox_backend(sb.clone());
        }
        if let Some(ref gr) = self.guardrail {
            tools.set_guardrail(gr.clone());
        }
        if let Some(ref pr) = self.plugin_registry {
            tools.set_plugin_registry(pr.clone());
        }
        let hooks = self.hooks.clone();
        let llm = self.llm.clone();
        let wal = self.wal.clone();
        let raw_logger = self.raw_logger.clone();
        let reflection_engine = self.reflection_engine.clone();
        let checkpoint_store = self.checkpoint_store.clone();
        let sandbox_backend = self.sandbox_backend.clone();
        let plugin_registry = self.plugin_registry.clone();

        let loop_event_cap = self.loop_event_channel_capacity;
        let handle = tokio::spawn(async move {
            let (loop_event_tx, mut loop_event_rx) = mpsc::channel(loop_event_cap);
            let mut agent_loop = AgentRuntime::new(config, loop_event_tx)
                .with_tools(tools)
                .with_hooks(hooks);
            if let Some(ref sb) = sandbox_backend {
                agent_loop = agent_loop.with_sandbox_backend(sb.clone());
            }
            if let Some(ref pr) = plugin_registry {
                agent_loop = agent_loop.with_plugin_registry(pr.clone());
            }
            if let Some(ref w) = wal {
                agent_loop = agent_loop.with_wal(w.clone());
            }
            if let Some(ref rl) = raw_logger {
                agent_loop = agent_loop.with_raw_logger(rl.clone());
            }
            if let Some(ref re) = reflection_engine {
                agent_loop = agent_loop.with_reflection_engine(re.clone());
                let _reviewer_handle = re.start_reviewer();
            }
            if let Some(ref cp) = checkpoint_store {
                agent_loop = agent_loop.with_checkpoint_store(cp.clone());
            }

            // Restore state from snapshot before running
            if let Err(e) = agent_loop.restore(&snapshot) {
                tracing::error!("Failed to restore snapshot: {}", e);
            }

            // Replay events after snapshot offset if WAL is available
            if snapshot.event_offset > 0 {
                if let Err(e) = agent_loop.replay_events(snapshot.event_offset).await {
                    tracing::warn!("Event replay failed: {}", e);
                }
            }

            // Forward events from AgentRuntime mpsc to Agent broadcast
            let forward_event_tx = event_tx.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(event) = loop_event_rx.recv().await {
                    let _ = forward_event_tx.send(event);
                }
            });

            run_agent_task(&mut agent_loop, cmd_rx, event_tx, llm, loop_event_cap).await;

            forward_handle.abort();
        });

        inner.cmd_tx = Some(cmd_tx);
        inner.task_handle = Some(handle);
    }

    /// Reset the agent, clearing all context and state.
    pub async fn reset(&self) -> SFResult<()> {
        if let Some(ref lifecycle) = self.lifecycle {
            let agent_id = &self.config.agent_id;
            let _ = lifecycle.stop_heartbeat(agent_id).await;
            let _ = lifecycle.transition(agent_id, AgentState::Inactive).await;
        }

        {
            let mut inner = self.inner.lock().await;
            if let Some(handle) = inner.task_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.consumer_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.registry_heartbeat_handle.take() {
                handle.abort();
            }
            inner.cmd_tx = None;
            inner.state = AgentState::Inactive;
        }
        self.start().await;
        Ok(())
    }

    /// Get the current agent state.
    pub async fn state(&self) -> AgentState {
        let inner = self.inner.lock().await;
        inner.state
    }

    /// Direct streaming access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    /// Use this when you need raw LLM streaming without agent behavior
    /// (e.g., simple one-shot completions, UI text streaming).
    pub async fn chat_stream(
        &self,
        messages: &[cog_core::Message],
        options: &cog_core::ChatOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        self.llm.chat_stream(messages, options).await
    }

    /// Direct streaming completion access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    pub async fn complete_stream(
        &self,
        prompt: &str,
        options: &cog_core::CompleteOptions,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        self.llm.complete_stream(prompt, options).await
    }

    /// Review an output string using the agent's self-review capability.
    /// Delegates to [`SelfReviewLoop`] internally.
    pub async fn review_output(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> SFResult<cog_core::SelfReviewResult> {
        let loop_ = crate::self_review::SelfReviewLoop::new(config.clone());
        loop_
            .review(output, self.llm.as_ref())
            .await
            .map(|(_, result)| result)
    }

    /// Review an output and return the (possibly revised) text along with the
    /// review result, preserving the SelfReviewLoop revision.
    pub async fn review_and_revise(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> SFResult<(String, cog_core::SelfReviewResult)> {
        let loop_ = crate::self_review::SelfReviewLoop::new(config.clone());
        loop_.review(output, self.llm.as_ref()).await
    }

    /// Start a background consumer that listens for [`InboxMessage`]s on
    /// this agent's Redis Streams inbox.
    /// Idempotent: if a consumer is already running this returns immediately.
    pub async fn start_consumer<F, Fut>(&self, mut handler: F) -> SFResult<()>
    where
        F: FnMut(InboxMessage) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = SFResult<()>> + Send,
    {
        let mut inner = self.inner.lock().await;
        if inner.consumer_handle.is_some() {
            return Ok(());
        }

        let backend = self
            .message_backend
            .clone()
            .ok_or_else(|| SFError::Agent("MessageBackend not configured".into()))?;
        let agent_id = self.config.agent_id.clone();

        let handle = tokio::spawn(async move {
            let consumer = crate::consumer::AgentConsumer::new(&agent_id, backend.clone());
            let group_name = format!("agent-{}", agent_id);
            if let Err(e) = backend
                .create_consumer_group(consumer.inbox_stream(), &group_name)
                .await
            {
                tracing::warn!("Consumer group create failed (may exist): {}", e);
            }
            let mut stream = match backend
                .subscribe(consumer.inbox_stream(), &group_name)
                .await
            {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Consumer subscribe failed: {}", e);
                    return;
                }
            };

            while let Some(result) = stream.next().await {
                match result {
                    Ok((_msg_id, bytes)) => match serde_json::from_slice::<InboxMessage>(&bytes) {
                        Ok(msg) => {
                            if let Err(e) = handler(msg).await {
                                tracing::warn!("Inbox handler error: {}", e);
                            }
                        }
                        Err(e) => tracing::warn!("Failed to deserialize InboxMessage: {}", e),
                    },
                    Err(e) => tracing::warn!("Stream error: {}", e),
                }
            }
        });

        inner.consumer_handle = Some(handle);
        Ok(())
    }

    /// Send an [`InboxMessage`] to another agent's inbox.
    pub async fn send_message(
        to_agent_id: &str,
        message: InboxMessage,
        backend: &dyn cog_core::MessageBackend,
    ) -> SFResult<()> {
        crate::consumer::AgentConsumer::send_message(to_agent_id, message, backend).await
    }

    /// Read a field from the shared [`ContextBoard`] for the given task.
    pub async fn read_board(&self, task_id: &str, field: &str) -> SFResult<Option<String>> {
        let backend = self
            .state_backend
            .clone()
            .ok_or_else(|| SFError::Agent("StateBackend not configured".into()))?;
        let board = backend.get_board(task_id).await?;
        Ok(board.and_then(|b| b.fields.get(field).cloned()))
    }

    /// Write a field to the shared [`ContextBoard`] for the given task.
    pub async fn write_board(&self, task_id: &str, field: &str, value: &str) -> SFResult<()> {
        let backend = self
            .state_backend
            .clone()
            .ok_or_else(|| SFError::Agent("StateBackend not configured".into()))?;
        backend.set_board_field(task_id, field, value).await
    }
}

#[async_trait::async_trait]
impl cog_core::Agent for Agent {
    async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.prompt(input).await
    }

    async fn start(&self) {
        self.start().await;
    }

    async fn snapshot(&self, task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        self.snapshot(task_id).await
    }

    async fn restore(&self, snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        self.restore(snapshot).await
    }

    async fn continue_(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        self.continue_(input).await
    }

    async fn steer(&self, instruction: String) -> cog_core::SFResult<()> {
        self.steer(instruction).await
    }

    async fn abort(&self) -> cog_core::SFResult<()> {
        self.abort().await
    }

    async fn reset(&self) -> cog_core::SFResult<()> {
        self.reset().await
    }

    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(self.state().await)
    }

    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        self.wait_for_idle().await
    }

    async fn restore_from_id(&self, checkpoint_id: &str) -> cog_core::SFResult<()> {
        self.restore_from_id(checkpoint_id).await
    }

    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        self.subscribe()
    }

    async fn chat_stream(
        &self,
        messages: &[cog_core::Message],
        options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(messages, options).await
    }

    async fn complete_stream(
        &self,
        prompt: &str,
        options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.complete_stream(prompt, options).await
    }

    async fn read_board(&self, task_id: &str, field: &str) -> cog_core::SFResult<Option<String>> {
        self.read_board(task_id, field).await
    }

    async fn write_board(&self, task_id: &str, field: &str, value: &str) -> cog_core::SFResult<()> {
        self.write_board(task_id, field, value).await
    }

    async fn receive_message(&self, msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        match self.message_backend {
            Some(ref backend) => {
                Agent::send_message(&self.config.agent_id, msg, backend.as_ref()).await
            }
            None => Err(cog_core::SFError::Agent(
                "No message backend configured for receive_message".into(),
            )),
        }
    }

    async fn review_output(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> cog_core::SFResult<cog_core::SelfReviewResult> {
        self.review_output(output, config).await
    }

    async fn review_and_revise(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> cog_core::SFResult<(String, cog_core::SelfReviewResult)> {
        Agent::review_and_revise(self, output, config).await
    }
}

impl Drop for Agent {
    fn drop(&mut self) {
        // Abort consumer and heartbeat tasks if present.
        if let Ok(inner) = self.inner.try_lock() {
            if let Some(ref handle) = inner.consumer_handle {
                handle.abort();
            }
            if let Some(ref handle) = inner.consumer_handle {
                handle.abort();
            }
            if let Some(ref handle) = inner.registry_heartbeat_handle {
                handle.abort();
            }
        }

        if let Some(ref lifecycle) = self.lifecycle {
            let agent_id = self.config.agent_id.clone();
            let lifecycle = lifecycle.clone();
            // Best-effort async cleanup: spawn a blocking task to stop heartbeat.
            // In a tokio runtime this will run; outside it silently does nothing.
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new();
                if let Ok(rt) = rt {
                    rt.block_on(async move {
                        lifecycle.stop_heartbeat(&agent_id).await;
                        let _ = lifecycle.transition(&agent_id, AgentState::Inactive).await;
                    });
                }
            });
        }

        // Best-effort deregister from global registry.
        if let Some(ref registry) = self.registry {
            let agent_id = self.config.agent_id.clone();
            let registry = registry.clone();
            let _ = std::thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new();
                if let Ok(rt) = rt {
                    rt.block_on(async move {
                        let _ = registry.deregister(&agent_id).await;
                    });
                }
            });
        }
    }
}

async fn run_agent_task(
    agent_loop: &mut AgentRuntime,
    mut cmd_rx: mpsc::Receiver<AgentCommand>,
    _event_tx: broadcast::Sender<AgentEvent>,
    llm: Arc<dyn cog_core::LlmClient>,
    loop_event_channel_capacity: usize,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            AgentCommand::Prompt { input, result_tx } => {
                let result = agent_loop.run(input, llm.as_ref()).await;
                let _ = result_tx.send(result);
            }
            AgentCommand::Continue { input, result_tx } => {
                // AgentRuntime::run() appends the user message to existing context,
                // so continuing is the same as prompting on the same loop instance.
                let result = agent_loop.run(input, llm.as_ref()).await;
                let _ = result_tx.send(result);
            }
            AgentCommand::Steer { instruction } => {
                let context = agent_loop.get_context();
                // We can't modify context directly because get_context returns &ContextWindow
                // Steering is best-effort for now; future AgentRuntime revision can support this.
                let _ = instruction;
                let _ = context;
            }
            AgentCommand::Reset => {
                // Recreate the agent loop with fresh context
                let (loop_event_tx, mut loop_event_rx) = mpsc::channel(loop_event_channel_capacity);
                let forward_handle =
                    tokio::spawn(
                        async move { while let Some(_event) = loop_event_rx.recv().await {} },
                    );

                let cfg = agent_loop.config();
                *agent_loop = AgentRuntime::new(
                    RuntimeConfig {
                        agent_id: agent_loop
                            .get_context()
                            .messages()
                            .first()
                            .and_then(|m| match m {
                                cog_core::Message::System { content, .. } => {
                                    Some(content.split_whitespace().next()?.to_string())
                                }
                                _ => None,
                            })
                            .unwrap_or_else(|| "agent".into()),
                        role: cfg.role.clone(),
                        max_iterations: cfg.max_iterations,
                        context_window_size: cfg.context_window_size,
                        skill_cache_ttl_secs: cfg.skill_cache_ttl_secs,
                        skill_config: cfg.skill_config.clone(),
                        crew_id: cfg.crew_id.clone(),
                        squad_id: cfg.squad_id.clone(),
                    },
                    loop_event_tx,
                );
                forward_handle.abort();
            }
            AgentCommand::Snapshot { task_id, result_tx } => {
                let snap = agent_loop.checkpoint(&task_id);
                if let Ok(ref checkpoint) = snap {
                    let cfg = agent_loop.config();
                    let _ = _event_tx.send(AgentEvent::CheckpointSaved {
                        agent_id: cfg.agent_id.clone(),
                        checkpoint_id: checkpoint.checkpoint_id.clone(),
                        task_id: task_id.clone(),
                        crew_id: cfg.crew_id.clone(),
                        squad_id: cfg.squad_id.clone(),
                        timestamp: chrono::Utc::now(),
                    });
                    // Persist to checkpoint store if configured
                    if let Some(ref store) = agent_loop.checkpoint_store() {
                        if let Err(e) = store.save(checkpoint).await {
                            tracing::warn!("Checkpoint save failed: {}", e);
                        }
                    }
                }
                let _ = result_tx.send(snap);
            }
        }
    }
}
