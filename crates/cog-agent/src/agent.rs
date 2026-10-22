use cog_core::{AgentEvent, AgentState, InboxMessage, SFError, SFResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

use crate::agent_kernel::runtime::{AgentHooks, AgentRuntime};
use crate::lifecycle::LifecycleManager;
use crate::tools::ToolRegistry;
use cog_core::RuntimeConfig;

use futures::StreamExt;

/// How long a retirement waits for the consumer to end.
///
/// Two things set the floor: a backend read blocks for at most one block period
/// (five seconds by default) and the stop flag is only seen between reads, and
/// then the message the handler is inside has to finish. Ten seconds is where
/// an idle consumer always makes it and a busy one is always worth a line — it
/// is a judgement about the wait, not a percentile of task duration.
const AGENT_RETIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// What one call to [`Agent::retire`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetireReport {
    /// Whether this call is the one that retired the object. False on a repeat,
    /// and the other two fields then describe nothing that happened.
    pub performed: bool,
    /// Whether the consumer task ended inside the budget.
    pub consumer_ended: bool,
    /// Whether the agent was running a turn when it was asked to leave.
    ///
    /// Kept apart from `consumer_ended` because the two misses are different
    /// faults: a busy agent that does not stop means the budget is smaller than
    /// its turns, an idle one that does not stop means the read loop never saw
    /// the signal.
    pub was_busy: bool,
}

impl RetireReport {
    fn already_retired() -> Self {
        Self {
            performed: false,
            consumer_ended: true,
            was_busy: false,
        }
    }
}

pub enum AgentCommand {
    Prompt {
        /// DAG task identity to stamp onto tool/sandbox calls. None = legacy
        /// unscoped prompt.
        task_id: Option<String>,
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
///         context_window_size: 32000,
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
    /// Where each finished run's token census is written, keyed by task.
    observability: Option<Arc<dyn cog_core::ObservabilityGateway>>,
    /// Where the loop sends a tool output too large to fit the context window,
    /// so the bytes it drops stay reachable by the reference in the marker.
    tool_output_archive: Option<Arc<dyn crate::archive::ToolOutputArchive>>,
    /// Broadcast channel capacity for agent events.
    event_channel_capacity: usize,
    /// 持久事件总线投递口。装上之后 AgentEnd 只写总线（broadcast 由
    /// MultiBackendEventConsumer 回灌一次），其余变体仍只走 broadcast。
    event_bus_sink: Option<crate::EventBusSink>,
    /// mpsc channel capacity for agent commands.
    cmd_channel_capacity: usize,
    /// mpsc channel capacity for loop-internal events.
    loop_event_channel_capacity: usize,
    /// Heartbeat interval in seconds.
    heartbeat_interval_secs: u64,
    /// Poll interval for wait_for_idle in milliseconds.
    wait_for_idle_poll_ms: u64,
    /// Set once this object has been retired, and never cleared.
    ///
    /// It is what keeps a retired object out of circulation: the manager's
    /// create path checks it under the lock it inserts into, so a name whose
    /// object was retired is given a new one instead of the dead consumer that
    /// would accept messages and answer none of them.
    retired: AtomicBool,
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
            reflection_engine: None,
            checkpoint_store: None,
            sandbox_backend: None,
            plugin_registry: None,
            external_skill_registry: None,
            guardrail: None,
            observability: None,
            tool_output_archive: None,
            event_channel_capacity,
            event_bus_sink: None,
            cmd_channel_capacity,
            loop_event_channel_capacity,
            heartbeat_interval_secs: 10,
            wait_for_idle_poll_ms: 50,
            retired: AtomicBool::new(false),
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

    /// Attach the durable event bus sink. With it, `AgentEnd` events go to the
    /// bus only (broadcast receives them once via the multi-backend consumer
    /// re-injection); every other variant stays broadcast-only.
    /// Must be called before `start`, like [`Self::with_event_bus`].
    pub fn with_event_bus_sink(mut self, sink: crate::EventBusSink) -> Self {
        self.event_bus_sink = Some(sink);
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

    /// Attach the observability gateway that receives this agent's per-task
    /// token census. Without it the runs still complete; their token spend is
    /// simply never recorded per task.
    pub fn with_observability(mut self, gateway: Arc<dyn cog_core::ObservabilityGateway>) -> Self {
        self.observability = Some(gateway);
        self
    }

    /// Hand the loop the surface that archives a tool output too large for the
    /// context window. Without it the output is still cut to fit; what is lost
    /// is the reference to the dropped bytes, so a reader of the transcript sees
    /// a truncation with no way back to the whole.
    pub fn with_tool_output_archive(
        mut self,
        archive: Arc<dyn crate::archive::ToolOutputArchive>,
    ) -> Self {
        self.tool_output_archive = Some(archive);
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
        let event_bus_sink = self.event_bus_sink.clone();
        let observability = self.observability.clone();
        let tool_output_archive = self.tool_output_archive.clone();

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
            if let Some(ref ob) = observability {
                agent_loop = agent_loop.with_observability(ob.clone());
            }
            if let Some(ref ar) = tool_output_archive {
                agent_loop = agent_loop.with_tool_output_archive(ar.clone());
            }

            // Forward events from AgentRuntime mpsc to Agent broadcast
            let forward_event_tx = event_tx.clone();
            let forward_sink = event_bus_sink.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(event) = loop_event_rx.recv().await {
                    forward_event(event, &forward_event_tx, forward_sink.as_ref());
                }
            });

            run_agent_task(&mut agent_loop, cmd_rx, event_tx, llm).await;

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
                // A lost key (TTL expiry, Redis flush, another replica's
                // shutdown) must not silence this agent forever: re-register
                // on the next beat instead of warning until restart.
                let agent_id = registration.agent_id.clone();
                let registration = registration.clone();
                let registry = registry.clone();
                let interval = std::time::Duration::from_secs(self.heartbeat_interval_secs);
                let heartbeat_handle = tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        ticker.tick().await;
                        if let Err(e) = registry.heartbeat(&agent_id).await {
                            if let Err(re) = registry.register(&registration).await {
                                tracing::warn!(
                                    "Registry heartbeat+re-register failed for {}: {} / {}",
                                    agent_id,
                                    e,
                                    re
                                );
                            }
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
        self.dispatch_prompt(None, input).await
    }

    /// Inherent counterpart of [`cog_core::Agent::prompt_for_task`] for callers
    /// holding the concrete type.
    pub async fn prompt_for_task(
        &self,
        task_id: &str,
        input: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
        self.dispatch_prompt(Some(task_id.to_string()), input).await
    }

    /// The change this task's checkout holds, as git prints it.
    ///
    /// The tree a run was given is the one it edited, so a diff taken from it
    /// is what the run did rather than what it said it did: git derives the
    /// path headers, the context and the hunk counts from the files themselves.
    /// `git add -N` first, so a file the run created is part of the diff rather
    /// than an untracked leftover the diff would silently omit.
    ///
    /// `exclude` names files to leave out. A run that writes its own answer into
    /// the tree — which is what a run told to take its answer from `git diff`
    /// may well do — leaves a file that is a copy of a change, and a diff that
    /// carried it would be applied as a change that creates it. Excluding it
    /// here is the only place the file can be left out: downstream, it is
    /// indistinguishable from a file the round meant to add.
    ///
    /// No backend, a failed command, an empty answer or a name that is not a
    /// plain file name is `Ok(None)`, not an error: a change that cannot be read
    /// from a tree is the caller's typed artifact to keep, and failing here
    /// would turn "no checkout" into "the round produced nothing". What the text
    /// means is the caller's to judge — this crate runs commands, it does not
    /// know what a change artifact is.
    pub async fn workspace_change(
        &self,
        task_id: &str,
        exclude: &[String],
    ) -> SFResult<Option<String>> {
        let Some(backend) = self.sandbox_backend.as_ref() else {
            return Ok(None);
        };
        let Some(command) = workspace_change_command(exclude) else {
            tracing::warn!(
                task_id,
                "a name to leave out of the checkout's diff is not a plain file name; keeping \
                 the generated artifact rather than running a command that would carry it"
            );
            return Ok(None);
        };
        let req = cog_core::SandboxRequest {
            task_id: task_id.to_string(),
            agent_id: self.config.agent_id.clone(),
            payload: cog_core::SandboxPayload::Command { command },
            input: serde_json::Value::Null,
            timeout: std::time::Duration::from_secs(120),
            limits: Default::default(),
        };
        let result = backend.execute(&req).await?;
        if result.exit_code != 0 {
            tracing::warn!(
                task_id,
                exit_code = result.exit_code,
                stderr = %result.stderr,
                "could not read the run's change from its checkout; keeping the generated artifact"
            );
        }
        Ok(workspace_change_from(&result))
    }

    async fn dispatch_prompt(
        &self,
        task_id: Option<String>,
        input: serde_json::Value,
    ) -> SFResult<serde_json::Value> {
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
                .send(AgentCommand::Prompt {
                    task_id,
                    input,
                    result_tx,
                })
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

    /// Whether [`Self::retire`] has been called on this object.
    pub fn is_retired(&self) -> bool {
        self.retired.load(Ordering::SeqCst)
    }

    /// Leave for good: stop the consumer, stop the heartbeat, deregister, and
    /// mark the object so no caller is ever handed it again.
    ///
    /// This is not [`Self::abort`]. `abort` cancels the current run and then
    /// starts the agent again — it resets, it does not leave. Everything that
    /// makes an agent cost something (a consumer, a socket, a registry entry)
    /// survives it. `retire` is the one-way door.
    ///
    /// The consumer is stopped by telling its read loop to finish, not by
    /// dropping the loop's future. A cancelled `XREADGROUP` may already have
    /// been answered — the server put the message in the group's pending list
    /// while the client threw the answer away without acking it — and nothing
    /// in this tree reclaims a pending entry. The flag is read between reads,
    /// so leaving costs at most one block period and loses nothing.
    ///
    /// Idempotent: the second call reports `performed: false` and touches
    /// nothing, so a task that reaches its terminal state twice does not pay
    /// for the same retirement twice.
    ///
    /// A consumer that outlasts the budget is reported, not raised: the object
    /// has left the roster by then either way, and an error would leave the
    /// caller unable to tell "the message outlived the wait" from "the
    /// retirement did not happen".
    pub async fn retire(&self) -> RetireReport {
        if self.retired.swap(true, Ordering::SeqCst) {
            return RetireReport::already_retired();
        }

        // Read before anything is torn down: afterwards the state says Inactive
        // no matter what the agent was doing, and "it was busy" is the
        // difference between a retirement that waited for a turn and one that
        // gave up on it.
        let was_busy = {
            let inner = self.inner.lock().await;
            matches!(inner.state, AgentState::Active | AgentState::Completing)
        };

        let consumer_stopped = self.stop_consumer().await;

        if let Some(ref lifecycle) = self.lifecycle {
            let agent_id = &self.config.agent_id;
            let _ = lifecycle.stop_heartbeat(agent_id).await;
            let _ = lifecycle.transition(agent_id, AgentState::Inactive).await;
        }
        if let Some(ref registry) = self.registry {
            if let Err(e) = registry.deregister(&self.config.agent_id).await {
                // A registry that cannot drop the entry is worth a line, but it
                // does not make the object live again: the local roster is what
                // decides who is handed out, and it is about to lose this one.
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    "retire could not deregister the agent: {e}"
                );
            }
        }

        {
            let mut inner = self.inner.lock().await;
            if let Some(handle) = inner.registry_heartbeat_handle.take() {
                handle.abort();
            }
            if let Some(handle) = inner.task_handle.take() {
                handle.abort();
            }
            inner.cmd_tx = None;
            inner.state = AgentState::Inactive;
        }

        if !consumer_stopped {
            // The loop is told either way; what timed out is only the wait for
            // it to finish, which means the handler it is inside is still
            // running. This is a reading, not a failure of the retirement: the
            // object is out of the roster either way, and a caller that turned
            // it into an error would have no way to say which of "the message
            // outlived the budget" and "the retirement did not happen" it saw.
            tracing::warn!(
                agent_id = %self.config.agent_id,
                busy = was_busy,
                budget_secs = AGENT_RETIRE_TIMEOUT.as_secs(),
                "agent retired, but its consumer was still running when the budget ran out"
            );
        }
        RetireReport {
            performed: true,
            consumer_ended: consumer_stopped,
            was_busy,
        }
    }

    /// Tell this agent's read loop to finish and wait for its task to end.
    ///
    /// Returns whether the loop ended inside the retirement budget. A backend
    /// the agent never attached has no loop to stop, which counts as stopped:
    /// there is nothing left running behind the object.
    async fn stop_consumer(&self) -> bool {
        let Some(backend) = self.message_backend.clone() else {
            return true;
        };
        let consumer = crate::consumer::AgentConsumer::new(&self.config.agent_id, backend.clone());
        let inbox = consumer.inbox_stream().to_string();
        let group = format!("agent-{}", self.config.agent_id);

        let handle = {
            let mut inner = self.inner.lock().await;
            inner.consumer_handle.take()
        };
        backend.stop_stream(&inbox, &group).await;
        let Some(handle) = handle else {
            return true;
        };
        tokio::time::timeout(AGENT_RETIRE_TIMEOUT, handle)
            .await
            .is_ok()
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
        let event_bus_sink = self.event_bus_sink.clone();
        let observability = self.observability.clone();
        let tool_output_archive = self.tool_output_archive.clone();

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
            if let Some(ref ob) = observability {
                agent_loop = agent_loop.with_observability(ob.clone());
            }
            if let Some(ref ar) = tool_output_archive {
                agent_loop = agent_loop.with_tool_output_archive(ar.clone());
            }

            // Restore state from snapshot before running
            if let Err(e) = agent_loop.restore(&snapshot) {
                tracing::error!("Failed to restore snapshot: {}", e);
            }

            // Replay events after snapshot offset if WAL is available.
            // 重放直发 broadcast 做本地状态重建，不过运行时 mpsc——mpsc 下游
            // 的总线投递只认活体流量，重放事件再进总线会被摄取侧当成新事件
            // 重复建档。
            if snapshot.event_offset > 0 {
                if let Err(e) = agent_loop
                    .replay_events(snapshot.event_offset, &event_tx)
                    .await
                {
                    tracing::warn!("Event replay failed: {}", e);
                }
            }

            // Forward events from AgentRuntime mpsc to Agent broadcast
            let forward_event_tx = event_tx.clone();
            let forward_sink = event_bus_sink.clone();
            let forward_handle = tokio::spawn(async move {
                while let Some(event) = loop_event_rx.recv().await {
                    forward_event(event, &forward_event_tx, forward_sink.as_ref());
                }
            });

            run_agent_task(&mut agent_loop, cmd_rx, event_tx, llm).await;

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
    /// Delegates to [`crate::SelfReviewLoop`] internally.
    pub async fn review_output(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> SFResult<cog_core::SelfReviewResult> {
        let loop_ =
            crate::self_review::SelfReviewLoop::new(config.clone()).with_actor(self.review_actor());
        loop_
            .review(output, self.llm.as_ref())
            .await
            .map(|(_, result)| result)
    }

    /// Token-attribution label for reviews this agent runs on its own output.
    /// The role is part of the label so review spend lands in the same
    /// per-agent bucket as the rest of the agent's calls.
    fn review_actor(&self) -> String {
        crate::self_review::self_review_actor(&self.config.role)
    }

    /// Review an output and return the (possibly revised) text along with the
    /// review result, preserving the SelfReviewLoop revision.
    pub async fn review_and_revise(
        &self,
        output: &str,
        config: &cog_core::SelfReviewConfig,
    ) -> SFResult<(String, cog_core::SelfReviewResult)> {
        let loop_ =
            crate::self_review::SelfReviewLoop::new(config.clone()).with_actor(self.review_actor());
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

            let inbox_stream = consumer.inbox_stream().to_string();
            while let Some(result) = stream.next().await {
                match result {
                    Ok((msg_id, bytes)) => {
                        // The kind of the command this delivery runs, recorded
                        // only after its ack lands. A command whose ack fails
                        // stays in the pending list and will run again, so
                        // counting it here would book a delivery that the queue
                        // does not agree happened.
                        let mut acked_kind: Option<&'static str> = None;
                        match serde_json::from_slice::<InboxMessage>(&bytes) {
                            Ok(msg) => {
                                let kind = crate::observable::inbox_kind(&msg);
                                if let Err(e) = handler(msg).await {
                                    // Leave pending: redelivery retries the handler.
                                    // Counted for the same reason `AgentConsumer`
                                    // counts it: the retry waits for the next start
                                    // of this consumer, so the log line is gone long
                                    // before anything acts on the failure.
                                    crate::observable::global_observable()
                                        .record_inbox_undelivered(kind);
                                    tracing::warn!(
                                        msg_id = %msg_id,
                                        kind,
                                        "Inbox handler error, message kept pending: {}",
                                        e
                                    );
                                    continue;
                                }
                                // Counted only once the ack below lands: until then
                                // the queue still holds the command and the next
                                // consumer start runs it again.
                                acked_kind = Some(kind);
                            }
                            Err(e) => {
                                // Same poison path as in `consumer`: the bytes
                                // reach the dead-letter queue before the ack that
                                // makes them unrecoverable. A failed write is
                                // reported and still does not hold the message
                                // pending — this parse can never succeed.
                                tracing::warn!(
                                    msg_id = %msg_id,
                                    "Failed to deserialize InboxMessage: {}",
                                    e
                                );
                                if let Err(dlq_err) = backend
                                    .dlq(
                                        &inbox_stream,
                                        &msg_id,
                                        &bytes,
                                        &format!("unparsable InboxMessage: {e}"),
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        msg_id = %msg_id,
                                        "dead-letter write failed; this message's bytes are now lost: {dlq_err}"
                                    );
                                }
                                crate::observable::global_observable().record_inbox_poisoned();
                            }
                        }
                        if let Err(e) = backend
                            .ack(&inbox_stream, &group_name, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(msg_id = %msg_id, "Failed to ack inbox message: {}", e);
                        } else if let Some(kind) = acked_kind {
                            crate::observable::global_observable().record_inbox_delivered(kind);
                        }
                    }
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

    /// Execute one message delivered to this agent's inbox.
    ///
    /// The inbox subscription used to receive every message and drop it, so an
    /// agent could be addressed but never answered. Running the command here is
    /// what turns the subscription into a delivery channel.
    ///
    /// A `Prompt` that names a `reply_stream` publishes its result there, so
    /// the sender reads the answer with the same backend API it published
    /// with. A prompt without one still runs for its side effects: the run's
    /// events reach the broadcast and the event plane as any other run does.
    /// Errors propagate to the consumer, which leaves the message pending
    /// instead of acking a run that never happened.
    pub async fn handle_inbox(&self, msg: InboxMessage) -> SFResult<()> {
        match msg {
            InboxMessage::Prompt { goal, reply_stream } => {
                let out = self.prompt(goal).await?;
                if let Some(stream) = reply_stream {
                    publish_inbox_reply(self.message_backend.as_ref(), &stream, &out).await?;
                }
                Ok(())
            }
            InboxMessage::Continue { instruction } => {
                let input = serde_json::Value::String(instruction);
                self.continue_(input).await.map(|_| ())
            }
            InboxMessage::Abort => self.abort().await,
            InboxMessage::Reset => self.reset().await,
            InboxMessage::Steering { instruction } => self.steer(instruction).await,
        }
    }

    /// Read a field from the shared `ContextBoard` for the given task.
    pub async fn read_board(&self, task_id: &str, field: &str) -> SFResult<Option<String>> {
        let backend = self
            .state_backend
            .clone()
            .ok_or_else(|| SFError::Agent("StateBackend not configured".into()))?;
        let board = backend.get_board(task_id).await?;
        Ok(board.and_then(|b| b.fields.get(field).cloned()))
    }

    /// Write a field to the shared `ContextBoard` for the given task.
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

    async fn prompt_for_task(
        &self,
        task_id: &str,
        input: serde_json::Value,
    ) -> cog_core::SFResult<serde_json::Value> {
        self.prompt_for_task(task_id, input).await
    }

    async fn workspace_change(
        &self,
        task_id: &str,
        exclude: &[String],
    ) -> cog_core::SFResult<Option<String>> {
        self.workspace_change(task_id, exclude).await
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

/// forward 循环的分发规则：装上总线投递口时 `AgentEnd` 只写总线——broadcast
/// 由 MultiBackendEventConsumer 从总线回灌恰好一次，本地实时消费者经回灌看到；
/// 直发+回灌会让 broadcast 订阅者看到两次。其余变体只写 broadcast。
fn forward_event(
    event: AgentEvent,
    broadcast_tx: &broadcast::Sender<AgentEvent>,
    sink: Option<&crate::EventBusSink>,
) {
    if let Some(s) = sink {
        if matches!(event, AgentEvent::AgentEnd { .. }) {
            s.send(event);
            return;
        }
    }
    let _ = broadcast_tx.send(event);
}

async fn run_agent_task(
    agent_loop: &mut AgentRuntime,
    mut cmd_rx: mpsc::Receiver<AgentCommand>,
    _event_tx: broadcast::Sender<AgentEvent>,
    llm: Arc<dyn cog_core::LlmClient>,
) {
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            AgentCommand::Prompt {
                task_id,
                input,
                result_tx,
            } => {
                let result = agent_loop
                    .run_scoped(input, llm.as_ref(), task_id.as_deref())
                    .await;
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
                // Clear the loop in place. Rebuilding it from its own config was
                // the previous behaviour and silently dropped everything the
                // caller had attached after construction — the tools first of
                // all — so a reset loop answered with nothing to call while
                // still running its state machine normally.
                agent_loop.reset();
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

/// Publish an inbox prompt's result onto the stream its sender named.
///
/// A reply that cannot name a transport is an error rather than a silent
/// success: the consumer treats a handler error as "leave the message
/// pending", and a run whose answer was dropped must not be acked as done.
async fn publish_inbox_reply(
    backend: Option<&Arc<dyn cog_core::MessageBackend>>,
    stream: &str,
    result: &serde_json::Value,
) -> SFResult<()> {
    let backend =
        backend.ok_or_else(|| SFError::Agent("No message backend to reply with".into()))?;
    let payload = serde_json::to_vec(result).map_err(SFError::Serialization)?;
    backend.publish(stream, &payload).await
}

#[cfg(test)]
mod inbox_reply_tests {
    use super::*;

    /// The answer lands on the stream the sender named, in the shape the
    /// sender can parse back. Before the inbox handler executed commands,
    /// nothing ever published here.
    #[tokio::test]
    async fn a_prompt_reply_lands_on_the_stream_the_sender_named() {
        let backend: Arc<dyn cog_core::MessageBackend> =
            Arc::new(cog_stream::MemoryMessageBackend::new());
        backend
            .create_consumer_group("replies", "caller")
            .await
            .unwrap();
        let answer = serde_json::json!({"ok": true, "text": "done"});

        publish_inbox_reply(Some(&backend), "replies", &answer)
            .await
            .unwrap();

        let mut stream = backend.subscribe("replies", "caller").await.unwrap();
        let (_, bytes) = stream.next().await.unwrap().unwrap();
        let read: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(read, answer);
    }

    /// No transport is an error, so the consumer leaves the command pending
    /// instead of acking a run whose answer went nowhere.
    #[tokio::test]
    async fn a_reply_without_a_transport_is_an_error() {
        let err = publish_inbox_reply(None, "replies", &serde_json::json!("x"))
            .await
            .expect_err("dropping the reply silently would ack a run that answered nobody");
        assert!(err.to_string().contains("No message backend"));
    }
}

#[cfg(test)]
mod forward_event_tests {
    use super::*;
    use std::sync::Mutex;

    /// 记录投递的事件；EventBusSink 的排空任务是异步的，测试里轮询等待。
    #[derive(Default)]
    struct SpyPublisher {
        published: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl cog_core::EventPublisher for SpyPublisher {
        async fn publish(&self, event: &AgentEvent) -> cog_core::SFResult<()> {
            let agent_id = match event {
                AgentEvent::AgentEnd { agent_id, .. } => agent_id.clone(),
                _ => "other".into(),
            };
            self.published.lock().unwrap().push(agent_id);
            Ok(())
        }
    }

    fn agent_end(agent_id: &str) -> AgentEvent {
        AgentEvent::AgentEnd {
            agent_id: agent_id.into(),
            messages: vec![],
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        }
    }

    async fn wait_published(spy: &SpyPublisher, n: usize) -> bool {
        for _ in 0..100 {
            if spy.published.lock().unwrap().len() >= n {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        false
    }

    /// 总线模式的核心不变式：装了投递口后 AgentEnd 只上总线，绝不直发
    /// broadcast——broadcast 的那一份由回灌消费者补，直发+回灌=重复。
    #[tokio::test]
    async fn agent_end_goes_to_sink_not_broadcast_when_sink_set() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<AgentEvent>(8);
        let spy = Arc::new(SpyPublisher::default());
        let sink = crate::EventBusSink::spawn(spy.clone(), 8, 1, None);

        forward_event(agent_end("a-bus"), &broadcast_tx, Some(&sink));

        assert!(
            broadcast_rx.try_recv().is_err(),
            "AgentEnd must not be sent to broadcast directly when a sink is set"
        );
        assert!(
            wait_published(&spy, 1).await,
            "AgentEnd must reach the bus publisher"
        );
    }

    /// 其余事件变体不受总线开关影响：只写 broadcast，不进总线——总线契约
    /// 上只承载 AgentEnd，高频事件上总线会打爆持久层。
    #[tokio::test]
    async fn non_agent_end_stays_on_broadcast_even_with_sink() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<AgentEvent>(8);
        let spy = Arc::new(SpyPublisher::default());
        let sink = crate::EventBusSink::spawn(spy.clone(), 8, 1, None);

        forward_event(
            AgentEvent::Heartbeat {
                agent_id: "a-live".into(),
                timestamp: chrono::Utc::now(),
            },
            &broadcast_tx,
            Some(&sink),
        );

        assert!(
            broadcast_rx.try_recv().is_ok(),
            "non-AgentEnd events must stay on broadcast"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            spy.published.lock().unwrap().is_empty(),
            "non-AgentEnd events must never reach the bus"
        );
    }

    /// 无投递口（events_on_bus 关闭）时维持旧行为：AgentEnd 直发 broadcast。
    #[tokio::test]
    async fn agent_end_goes_to_broadcast_when_no_sink() {
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<AgentEvent>(8);
        forward_event(agent_end("a-local"), &broadcast_tx, None);
        assert!(
            matches!(broadcast_rx.try_recv(), Ok(AgentEvent::AgentEnd { .. })),
            "without a sink AgentEnd must stay on broadcast"
        );
    }
}

/// The command that reads a run's change back out of its checkout.
///
/// `git add -N` before the diff, so a file the run created is part of it rather
/// than an untracked leftover `git diff` would silently omit; `--no-pager`,
/// because the executor has no terminal and a pager would hang the read.
/// `--no-color` for the same reason the pager is named: the answer is bytes
/// another program compares, not something a human reads.
const WORKSPACE_CHANGE_COMMAND: &str =
    "git add -N . >/dev/null 2>&1; git --no-pager diff --no-color";

/// The command that reads a run's change back out of its checkout, with the
/// files the caller named left out.
///
/// Both halves carry the exclusion, so the index is not left with an
/// intent-to-add entry for a file the diff then refuses to show, and the diff
/// gets an explicit `.` because a pathspec list of excludes alone is a list
/// whose meaning depends on the command. `literal` because a name is a name,
/// not a glob: a run may not widen what it leaves out by naming a file `*`.
///
/// `None` when a name is not a plain file name. The command is handed to a
/// shell and these names come from a model's own output, so anything that could
/// be read as shell syntax ends the read rather than being quoted and hoped
/// for; the caller keeps the artifact it already had.
fn workspace_change_command(exclude: &[String]) -> Option<String> {
    if exclude.is_empty() {
        return Some(WORKSPACE_CHANGE_COMMAND.to_string());
    }
    let mut pathspecs = String::new();
    for name in exclude {
        if !is_plain_file_name(name) {
            return None;
        }
        pathspecs.push_str(&format!(" ':(exclude,literal){name}'"));
    }
    Some(format!(
        "git add -N .{pathspecs} >/dev/null 2>&1; git --no-pager diff --no-color -- .{pathspecs}"
    ))
}

/// Whether a name may be put in a command as a literal path.
///
/// A plain file name: no directory, no shell or glob metacharacter, no leading
/// dash that a command could read as an option. Everything here is a character
/// that cannot change what the shell does with the string.
fn is_plain_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.starts_with('-')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
}

/// Read a command's answer as "the change this checkout holds".
///
/// A command that failed and a command that printed nothing are both `None`:
/// neither is a change, and the caller keeps the artifact it already has. The
/// decision is kept out of the request path so the two ways this returns `None`
/// can be read without a sandbox.
fn workspace_change_from(result: &cog_core::SandboxResult) -> Option<String> {
    if result.exit_code != 0 {
        return None;
    }
    let diff = result.stdout.trim_end_matches(['\n', '\r']);
    if diff.trim().is_empty() {
        return None;
    }
    Some(format!("{diff}\n"))
}

#[cfg(test)]
mod workspace_change_tests {
    use super::*;

    fn result(exit_code: i32, stdout: &str) -> cog_core::SandboxResult {
        cog_core::SandboxResult {
            stdout: stdout.into(),
            stderr: String::new(),
            exit_code,
            output: None,
            duration_ms: 0,
            resource_usage: Default::default(),
        }
    }

    /// The tree's diff comes back with exactly one terminating newline: git
    /// prints one, and a reader that keeps whatever trailing blank lines the
    /// capture added would hand the gate a patch with a body the header does
    /// not count.
    #[test]
    fn a_checkout_that_holds_a_diff_answers_with_it_terminated_once() {
        let stdout = "diff --git a/a.rs b/a.rs\n@@ -1 +1 @@\n-old\n+new\n\n";
        assert_eq!(
            workspace_change_from(&result(0, stdout)),
            Some("diff --git a/a.rs b/a.rs\n@@ -1 +1 @@\n-old\n+new\n".to_string())
        );
    }

    /// A run told to take its answer from `git diff` may write that answer into
    /// the tree. Both halves of the command have to leave that file out: an
    /// `add -N` that marked it would put an intent-to-add entry in the index for
    /// a file the diff then refuses to show, and a diff without its explicit `.`
    /// is a pathspec list whose meaning depends on the git version.
    #[test]
    fn the_file_the_run_wrote_its_answer_into_is_left_out_of_both_halves() {
        let command = workspace_change_command(&["changes.diff".to_string()]).unwrap();
        let exclude = "':(exclude,literal)changes.diff'";
        assert_eq!(
            command,
            format!("git add -N . {exclude} >/dev/null 2>&1; git --no-pager diff --no-color -- . {exclude}")
        );
    }

    /// No name to leave out is the plain read: the command must not grow a
    /// pathspec that would change what "the tree's diff" means.
    #[test]
    fn a_read_with_nothing_to_leave_out_is_the_plain_diff() {
        assert_eq!(
            workspace_change_command(&[]),
            Some(WORKSPACE_CHANGE_COMMAND.to_string())
        );
    }

    /// These names come from a model's output and go into a shell command. One
    /// that could be read as syntax — a quote, a substitution, a glob, a
    /// directory, an option — ends the read instead of being passed through;
    /// the caller then keeps the diff it already had.
    #[test]
    fn a_name_that_is_not_a_plain_file_name_is_refused_rather_than_passed_on() {
        for name in [
            "changes.diff' ; rm -rf / #",
            "a b.diff",
            "$(whoami).diff",
            "*.diff",
            "/etc/passwd",
            "sub/changes.diff",
            "-n",
            "",
        ] {
            assert_eq!(
                workspace_change_command(&[name.to_string()]),
                None,
                "{name:?} must not reach the command"
            );
        }
    }

    /// A git that failed, or a tree with nothing changed, is not a change: both
    /// have to read as "no answer", or a failed command would be handed on as
    /// an empty patch and cost the round it was meant to save.
    #[test]
    fn a_failed_command_and_an_untouched_tree_both_read_as_no_answer() {
        assert_eq!(
            workspace_change_from(&result(128, "diff --git a/a.rs b/a.rs\n")),
            None
        );
        assert_eq!(workspace_change_from(&result(0, "\n  \n")), None);
    }
}
