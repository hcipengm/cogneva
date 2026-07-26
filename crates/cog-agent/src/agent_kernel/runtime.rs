use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, ToolCall,
    ToolDefinition,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::context::ContextWindow;
use crate::hooks::LifecycleHookEvent;
use crate::tools::ToolRegistry;

/// A single ReAct iteration: Thought -> Action -> Observation.
/// ReAct (Reasoning + Acting) is the fundamental cycle each Agent executes:
/// 1. **Think** — the LLM reasons about the problem and decides what to do
/// 2. **Act**  — the LLM emits tool calls (actions)
/// 3. **Observe** — tool results are fed back as observations
///
/// This struct captures one complete cycle for introspection and debugging.
/// It is derived from the recorded [`RuntimeStep`]s after a run completes.
#[derive(Debug, Clone)]
pub struct ReActStep {
    /// The reasoning text produced by the LLM during the Thinking phase.
    pub thought: String,
    /// The tool calls (actions) emitted by the LLM.
    pub actions: Vec<ToolCall>,
    /// The observations (tool results) returned after executing actions.
    pub observations: Vec<serde_json::Value>,
    /// Timestamp when this ReAct step started.
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// ReAct Loop — explicit wrapper around [`AgentRuntime`] that documents the
/// Think->Act->Observe cycle.
/// The design doc specifies ReAct as "Agent internal infrastructure" that
/// doesn't occupy an architecture layer number.  This type makes the pattern
/// explicit in the API while the actual execution still delegates to the
/// underlying [`AgentRuntime`] state machine.
/// # ReAct Cycle
/// ```text
///     Think (RuntimeState::Thinking)
///       |
///       v
///     Call (RuntimeState::Calling)  -- emit tool_calls
///       |
///       v
///     Act (RuntimeState::Acting)    -- execute each tool
///       |
///       v
///     Observe (RuntimeState::Observing) -- collect results
///       |
///       v
///     Update (RuntimeState::Updating)   -- add to context
///       |
///       +---> next iteration or Complete
/// ```
pub struct ReActLoop {
    inner: AgentRuntime,
}

impl ReActLoop {
    /// Wrap an existing [`AgentRuntime`] as a [`ReActLoop`].
    pub fn new(inner: AgentRuntime) -> Self {
        Self { inner }
    }

    /// Execute one full ReAct cycle (think -> act -> observe).
    /// This is a convenience wrapper around [`AgentRuntime::run`] that
    /// additionally emits ReAct-specific lifecycle events.
    pub async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        self.inner.run(input, llm).await
    }

    /// Extract the ReAct steps from the completed loop run.
    /// Returns one [`ReActStep`] per iteration that contained tool calls.
    /// Steps without tool calls (final completion) are excluded.
    pub fn react_steps(&self) -> Vec<ReActStep> {
        self.inner
            .steps()
            .iter()
            .filter(|s| !s.tool_calls.is_empty())
            .map(|s| ReActStep {
                thought: s.thought.clone().unwrap_or_default(),
                actions: s.tool_calls.clone(),
                observations: s.observations.clone(),
                timestamp: s.timestamp,
            })
            .collect()
    }

    /// Return the number of complete ReAct iterations executed.
    pub fn react_iteration_count(&self) -> usize {
        self.react_steps().len()
    }

    /// Return the underlying [`AgentRuntime`] state.
    pub fn state(&self) -> RuntimeState {
        self.inner.state()
    }

    /// Return the underlying [`AgentRuntime`] steps.
    pub fn steps(&self) -> &[RuntimeStep] {
        self.inner.steps()
    }

    /// Mutable access to the underlying [`AgentRuntime`].
    pub fn inner_mut(&mut self) -> &mut AgentRuntime {
        &mut self.inner
    }

    /// Immutable access to the underlying [`AgentRuntime`].
    pub fn inner(&self) -> &AgentRuntime {
        &self.inner
    }
}

/// Agent loop state machine.
/// Aligns with pi-agent-core's RuntimeState.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    Idle,
    Thinking,
    Calling,
    Acting,
    Observing,
    Updating,
    Complete,
    Error,
}

impl std::fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeState::Idle => write!(f, "idle"),
            RuntimeState::Thinking => write!(f, "thinking"),
            RuntimeState::Calling => write!(f, "calling"),
            RuntimeState::Acting => write!(f, "acting"),
            RuntimeState::Observing => write!(f, "observing"),
            RuntimeState::Updating => write!(f, "updating"),
            RuntimeState::Complete => write!(f, "complete"),
            RuntimeState::Error => write!(f, "error"),
        }
    }
}

use cog_core::RuntimeConfig;

/// Hooks for customizing agent behavior.
#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct AgentHooks {
    /// Called before each tool call. Return Err to skip the tool call.
    pub before_tool_call:
        Option<Arc<dyn Fn(&str, &serde_json::Value) -> SFResult<()> + Send + Sync>>,
    /// Called after each tool call with the result.
    pub after_tool_call:
        Option<Arc<dyn Fn(&str, &serde_json::Value, &SFResult<serde_json::Value>) + Send + Sync>>,
    /// Called before sending context to LLM. Use to modify/summarize context.
    pub transform_context: Option<Arc<dyn Fn(&mut ContextWindow) + Send + Sync>>,
}

/// A single step in the agent loop, for introspection.
#[derive(Debug, Clone)]
pub struct RuntimeStep {
    pub state: RuntimeState,
    pub thought: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub observations: Vec<serde_json::Value>,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Agent execution loop with streaming LLM integration.
/// Core design:
/// - Streaming first: uses `llm.chat_stream()` and consumes `AssistantMessageEvent`s
/// - State machine driven, each transition emits `AgentEvent`
/// - Tool calls extracted from `ContentBlock::ToolCall` in the streaming response
/// - Hooks allow customizing tool execution and context transformation
pub struct AgentRuntime {
    config: RuntimeConfig,
    state: RuntimeState,
    context: ContextWindow,
    tools: ToolRegistry,
    /// Original event channel — when no hook engine is configured events
    /// are sent here directly.  When a hook engine *is* configured events
    /// are routed through the engine first and the engine forwards here.
    event_tx: mpsc::Sender<AgentEvent>,
    /// Optional hook engine sender — when set, emit_event sends
    /// LifecycleHookEvents to this engine's input channel instead of
    /// to event_tx directly.  The engine's forward_tx must be wired to
    /// event_tx so existing consumers still receive events.
    hook_engine: Option<mpsc::Sender<LifecycleHookEvent>>,
    hooks: AgentHooks,
    steps: Vec<RuntimeStep>,
    wal: Option<Arc<crate::wal::AgentWal>>,
    raw_logger: Option<Arc<dyn cog_core::RawLogger>>,
    reflection_engine: Option<Arc<dyn cog_core::ReflectionEngine>>,
    /// Wall-clock start time for the current run, used for effectiveness tracking.
    start_time: Option<chrono::DateTime<chrono::Utc>>,
    /// Optional checkpoint store for persisting/restoring agent state.
    checkpoint_store: Option<Arc<dyn cog_core::CheckpointStore>>,
    /// Optional sandbox backend for WASM tool execution.
    sandbox_backend: Option<Arc<dyn cog_core::SandboxBackend>>,
    /// Optional plugin registry for fetching WASM tool bytes.
    plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
    /// Optional external skill registry for injecting available_skills into system prompt.
    external_skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    /// Cached skill list to avoid querying the registry on every LLM call.
    available_skills_cache: Option<Vec<cog_core::SkillMetadata>>,
    /// When the skill cache was last refreshed.
    skills_cache_instant: Option<std::time::Instant>,
}

/// Try to extract a JSON object or array from free-form text.
/// Handles both raw JSON and JSON embedded inside markdown fences or reasoning.
fn try_extract_json(text: &str) -> Option<serde_json::Value> {
    let trimmed = text.trim();

    // Direct JSON prefix.
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        if let Ok(v) = serde_json::from_str(trimmed) {
            return Some(v);
        }
    }

    // Look for a fenced JSON block.
    if let Some(start) = trimmed.find("```json") {
        let after_open = &trimmed[start + 7..];
        if let Some(end) = after_open.find("```") {
            let inner = after_open[..end].trim();
            if let Ok(v) = serde_json::from_str(inner) {
                return Some(v);
            }
        }
    }

    // Extract the outermost balanced object or array by scanning braces.
    let start_obj = trimmed.find('{');
    let start_arr = trimmed.find('[');
    let start = match (start_obj, start_arr) {
        (Some(o), Some(a)) => Some(o.min(a)),
        (Some(o), None) => Some(o),
        (None, Some(a)) => Some(a),
        (None, None) => None,
    }?;

    let substr = &trimmed[start..];
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escape = false;
    let mut end = None;
    for (i, ch) in substr.char_indices() {
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '{' | '[' => depth += 1,
            '}' | ']' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    serde_json::from_str(&substr[..end]).ok()
}

impl AgentRuntime {
    /// Access the loop configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn new(mut config: RuntimeConfig, event_tx: mpsc::Sender<AgentEvent>) -> Self {
        if let Some(ref skill) = config.skill_config {
            config.max_iterations = skill.max_iterations;
        }

        let context = ContextWindow::new(config.context_window_size);

        Self {
            config,
            state: RuntimeState::Idle,
            context,
            tools: ToolRegistry::new(),
            event_tx,
            hook_engine: None,
            hooks: AgentHooks::default(),
            steps: Vec::new(),
            wal: None,
            raw_logger: None,
            reflection_engine: None,
            start_time: None,
            checkpoint_store: None,
            sandbox_backend: None,
            plugin_registry: None,
            external_skill_registry: None,
            available_skills_cache: None,
            skills_cache_instant: None,
        }
    }

    /// Set the checkpoint store for persistence.
    pub fn with_checkpoint_store(mut self, store: Arc<dyn cog_core::CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    pub fn with_reflection_engine(mut self, engine: Arc<dyn cog_core::ReflectionEngine>) -> Self {
        self.reflection_engine = Some(engine);
        self
    }

    pub fn with_raw_logger(mut self, logger: Arc<dyn cog_core::RawLogger>) -> Self {
        self.raw_logger = Some(logger);
        self
    }

    pub fn with_tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
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

    pub fn with_hooks(mut self, hooks: AgentHooks) -> Self {
        self.hooks = hooks;
        self
    }

    pub fn with_hook_engine(mut self, tx: mpsc::Sender<LifecycleHookEvent>) -> Self {
        self.hook_engine = Some(tx);
        self
    }

    pub fn with_wal(mut self, wal: Arc<crate::wal::AgentWal>) -> Self {
        self.wal = Some(wal);
        self
    }

    pub fn state(&self) -> RuntimeState {
        self.state
    }

    pub fn steps(&self) -> &[RuntimeStep] {
        &self.steps
    }

    pub fn get_context(&self) -> &ContextWindow {
        &self.context
    }

    pub fn get_tools(&self) -> &ToolRegistry {
        &self.tools
    }

    pub fn role(&self) -> &str {
        self.config.role.as_str()
    }

    pub fn checkpoint_store(&self) -> &Option<Arc<dyn cog_core::CheckpointStore>> {
        &self.checkpoint_store
    }

    /// Capture a checkpoint of the current agent state.
    /// Returns a [`Snapshot`] that can be persisted via a [`SnapshotStore`]
    /// and later restored to resume execution.
    pub fn checkpoint(&self, task_id: impl Into<String>) -> SFResult<cog_core::AgentCheckpoint> {
        let snapshot_id = format!("snap-{}-{}", self.config.agent_id, uuid::Uuid::new_v4());
        tracing::info!(
            agent_id = %self.config.agent_id,
            snapshot_id = %snapshot_id,
            "Agent state checkpoint captured"
        );
        Ok(cog_core::AgentCheckpoint {
            checkpoint_id: snapshot_id,
            task_id: task_id.into(),
            agent_state: serde_json::json!({
                "agent_id": self.config.agent_id,
                "role": self.config.role.to_string(),
                "state": self.state.to_string(),
                "steps_len": self.steps.len(),
            }),
            context_window: self.context.messages().to_vec(),
            event_offset: self.wal.as_ref().map(|w| w.current_seq()).unwrap_or(0),
            timestamp: chrono::Utc::now(),
        })
    }

    /// Restore agent state from a [`Snapshot`].
    /// Reconstructs the context window and internal state so that
    /// [`run`](Self::run) can continue from where the snapshot was taken.
    /// Events after `snapshot.event_offset` can be replayed via
    /// [`replay_events`](Self::replay_events).
    pub fn restore(&mut self, snapshot: &cog_core::AgentCheckpoint) -> SFResult<()> {
        // Restore context window
        self.context
            .restore_messages(snapshot.context_window.clone());

        // Restore loop state from agent_state JSON
        if let Some(state_str) = snapshot.agent_state.get("state").and_then(|v| v.as_str()) {
            self.state = match state_str {
                "idle" => RuntimeState::Idle,
                "thinking" => RuntimeState::Thinking,
                "calling" => RuntimeState::Calling,
                "acting" => RuntimeState::Acting,
                "observing" => RuntimeState::Observing,
                "updating" => RuntimeState::Updating,
                "complete" => RuntimeState::Complete,
                "error" => RuntimeState::Error,
                _ => RuntimeState::Idle,
            };
        }

        // Optionally restore role if present and different
        if let Some(role_str) = snapshot.agent_state.get("role").and_then(|v| v.as_str()) {
            self.config.role = role_str.to_string();
        }

        // Optionally restore agent_id if present
        if let Some(agent_id) = snapshot
            .agent_state
            .get("agent_id")
            .and_then(|v| v.as_str())
        {
            self.config.agent_id = agent_id.to_string();
        }

        // Reset steps — they are ephemeral introspection data.
        // The context window already contains the full conversation history.
        self.steps.clear();

        tracing::info!(
            agent_id = %self.config.agent_id,
            checkpoint_id = %snapshot.checkpoint_id,
            "Agent state restored from checkpoint"
        );

        Ok(())
    }

    /// Capture a checkpoint and persist it to the configured store.
    pub async fn checkpoint_and_save(&self, task_id: impl Into<String>) -> SFResult<String> {
        let checkpoint = self.checkpoint(task_id)?;

        let store = self
            .checkpoint_store
            .as_ref()
            .ok_or_else(|| SFError::Agent("No checkpoint store configured".into()))?;
        let id = store
            .save(&checkpoint)
            .await
            .map_err(|e| SFError::Agent(format!("Checkpoint save failed: {e}")))?;
        tracing::info!(
            agent_id = %self.config.agent_id,
            checkpoint_id = %id,
            "Checkpoint persisted"
        );
        Ok(id)
    }

    /// Restore agent state from a persisted checkpoint by id.
    pub async fn restore_from_store(&mut self, checkpoint_id: &str) -> SFResult<()> {
        let store = self
            .checkpoint_store
            .as_ref()
            .ok_or_else(|| SFError::Agent("No checkpoint store configured".into()))?;
        let checkpoint = store
            .load(checkpoint_id)
            .await
            .map_err(|e| SFError::Agent(format!("Checkpoint load failed: {e}")))?
            .ok_or_else(|| SFError::Agent(format!("Checkpoint not found: {checkpoint_id}")))?;
        tracing::info!(
            agent_id = %self.config.agent_id,
            checkpoint_id = %checkpoint_id,
            "Checkpoint loaded, restoring state"
        );
        self.restore(&checkpoint)
    }

    /// Replay WAL events from the given offset onward.
    /// Re-emits each event through the loop's event channel so that
    /// downstream consumers (broadcast, metrics, etc.) see the full
    /// history after a snapshot restore.
    /// Returns the number of events replayed.
    pub async fn replay_events(&self, from_offset: u64) -> SFResult<usize> {
        let Some(ref wal) = self.wal else {
            return Ok(0);
        };

        let records = wal
            .read_since(from_offset)
            .await
            .map_err(|e| SFError::Agent(format!("WAL replay failed: {e}")))?;

        let mut count = 0;
        for record in records {
            // Reconstruct AgentEvent from WalRecord payload
            if let Ok(event) = wal_record_to_agent_event(&record) {
                if self.event_tx.send(event).await.is_err() {
                    return Err(SFError::Backpressure);
                }
                count += 1;
            }
        }
        Ok(count)
    }

    /// Stream the next turn using the loop's current context and tools,
    /// returning the raw `AssistantMessageEventStream` for external consumption.
    /// This bypasses the internal event consumption of `think_stream`, allowing
    /// callers to handle streaming events themselves while still benefiting from
    /// the loop's configured context, hooks, and tool definitions.
    pub async fn chat_stream(
        &mut self,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<cog_core::AssistantMessageEventStream> {
        let mut messages: Vec<Message> = self.context.messages().to_vec();

        // transform_context hook
        if let Some(ref hook) = self.hooks.transform_context {
            hook(&mut self.context);
            messages = self.context.messages().to_vec();
        }

        let tool_defs = if self.tools.is_empty() {
            None
        } else {
            Some(
                self.tools
                    .list()
                    .iter()
                    .map(|t| ToolDefinition {
                        name: t.name.clone(),
                        description: t.description.clone(),
                        parameters: t.parameters.clone(),
                    })
                    .collect(),
            )
        };

        let options = cog_core::ChatOptions {
            tools: tool_defs,
            ..Default::default()
        };

        llm.chat_stream(&messages, &options).await
    }

    /// Execute the agent loop with streaming LLM.
    /// Backpressure: event_tx is a bounded channel; slow consumers naturally block.
    pub async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        tracing::info!(agent_id = %self.config.agent_id, "AgentRuntime::run started");
        self.state = RuntimeState::Idle;
        self.steps.clear();

        self.start_time = Some(chrono::Utc::now());
        self.emit_event(AgentEvent::AgentStart {
            agent_id: self.config.agent_id.clone(),
            crew_id: None,
            squad_id: None,
            timestamp: self.start_time.unwrap(),
        })
        .await?;

        self.context.add_message(Message::user(
            serde_json::to_string(&input).unwrap_or_default(),
        ));

        for iteration in 0..self.config.max_iterations {
            tracing::info!(agent_id = %self.config.agent_id, iteration, "AgentRuntime::run iteration start");
            // --- Turn Start ---
            self.emit_event(AgentEvent::TurnStart {
                agent_id: self.config.agent_id.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // Step 1: Thinking (streaming)
            self.state = RuntimeState::Thinking;
            tracing::info!(agent_id = %self.config.agent_id, "AgentRuntime::run calling think_stream");
            let think_timeout = Duration::from_secs(240);
            let assistant_msg = match tokio::time::timeout(think_timeout, self.think_stream(llm))
                .await
            {
                Ok(Ok(msg)) => msg,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    let err = SFError::Agent(format!(
                        "AgentRuntime::think_stream timed out after {}s for agent {}",
                        think_timeout.as_secs(),
                        self.config.agent_id
                    ));
                    tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime think_stream timeout");
                    return Err(err);
                }
            };

            let thought_text: String = assistant_msg
                .content_blocks()
                .map(|blocks| {
                    let text: String = blocks
                        .iter()
                        .filter_map(|b| b.as_text())
                        .collect::<Vec<_>>()
                        .join("");
                    if !text.is_empty() {
                        text
                    } else {
                        // Reasoning-only models (e.g. kimi-k2.6) place the answer in
                        // thinking/reasoning blocks instead of regular text.
                        blocks
                            .iter()
                            .filter_map(|b| b.as_thinking())
                            .collect::<Vec<_>>()
                            .join("")
                    }
                })
                .unwrap_or_default();

            let tool_calls = assistant_msg.tool_calls();

            // Record step
            self.steps.push(RuntimeStep {
                state: RuntimeState::Thinking,
                thought: Some(thought_text.clone()),
                tool_calls: tool_calls.clone(),
                observations: Vec::new(),
                result: None,
                error: None,
                timestamp: chrono::Utc::now(),
            });

            // If no tool calls, complete normally
            if tool_calls.is_empty() {
                self.state = RuntimeState::Complete;
                let result_timeout = Duration::from_secs(180);
                let result = match tokio::time::timeout(
                    result_timeout,
                    self.build_result(&thought_text, llm),
                )
                .await
                {
                    Ok(Ok(r)) => r,
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        let err = SFError::Agent(format!(
                            "AgentRuntime::build_result timed out after {}s for agent {}",
                            result_timeout.as_secs(),
                            self.config.agent_id
                        ));
                        tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime build_result timeout");
                        return Err(err);
                    }
                };

                self.steps.push(RuntimeStep {
                    state: RuntimeState::Complete,
                    thought: Some(thought_text),
                    tool_calls: Vec::new(),
                    observations: Vec::new(),
                    result: Some(result.clone()),
                    error: None,
                    timestamp: chrono::Utc::now(),
                });

                self.context.add_message(assistant_msg);

                self.emit_event(AgentEvent::TurnEnd {
                    agent_id: self.config.agent_id.clone(),
                    message: Message::assistant_text(result.to_string()),
                    tool_results: Vec::new(),
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                self.emit_event(AgentEvent::AgentEnd {
                    agent_id: self.config.agent_id.clone(),
                    messages: self.context.messages().to_vec(),
                    crew_id: None,
                    squad_id: None,
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                let success = self.state == RuntimeState::Complete;
                let steps = self.steps.len();
                let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
                crate::observable::global_observable().record_run(success, steps, tool_calls);

                return Ok(result);
            }

            // Last iteration but still has tool calls: max iterations reached
            if iteration == self.config.max_iterations - 1 {
                self.state = RuntimeState::Complete;
                let result = serde_json::json!({
                    "status": "max_iterations_reached",
                    "iterations": self.config.max_iterations,
                    "pending_tool_calls": tool_calls.len(),
                });

                self.steps.push(RuntimeStep {
                    state: RuntimeState::Complete,
                    thought: Some(thought_text.clone()),
                    tool_calls,
                    observations: Vec::new(),
                    result: Some(result.clone()),
                    error: None,
                    timestamp: chrono::Utc::now(),
                });

                self.context.add_message(assistant_msg);

                self.emit_event(AgentEvent::TurnEnd {
                    agent_id: self.config.agent_id.clone(),
                    message: Message::assistant_text(thought_text),
                    tool_results: Vec::new(),
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                self.emit_event(AgentEvent::AgentEnd {
                    agent_id: self.config.agent_id.clone(),
                    messages: self.context.messages().to_vec(),
                    crew_id: None,
                    squad_id: None,
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                let success = self.state == RuntimeState::Complete;
                let steps = self.steps.len();
                let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
                crate::observable::global_observable().record_run(success, steps, tool_calls);

                return Ok(result);
            }

            // Emit ReAct step start before executing tool calls
            self.emit_event(AgentEvent::ReActStepStart {
                agent_id: self.config.agent_id.clone(),
                iteration,
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // Step 2-4: Execute all tool calls
            self.state = RuntimeState::Calling;
            let mut observations: Vec<serde_json::Value> = Vec::new();
            let mut tool_result_messages: Vec<Message> = Vec::new();

            for tc in &tool_calls {
                self.emit_event(AgentEvent::ToolExecutionStart {
                    agent_id: self.config.agent_id.clone(),
                    tool_call_id: tc.id.clone(),
                    tool_name: tc.name.clone(),
                    args: tc.arguments.clone(),
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                // before_tool_call hook
                if let Some(ref hook) = self.hooks.before_tool_call {
                    if let Err(e) = hook(&tc.name, &tc.arguments) {
                        let err_result = serde_json::json!({ "error": e.to_string() });
                        observations.push(err_result.clone());
                        tool_result_messages.push(Message::tool_result_text(
                            &tc.id,
                            &tc.name,
                            err_result.to_string(),
                        ));

                        self.emit_event(AgentEvent::ToolExecutionEnd {
                            agent_id: self.config.agent_id.clone(),
                            tool_call_id: tc.id.clone(),
                            result: err_result.clone(),
                            is_error: true,
                            timestamp: chrono::Utc::now(),
                        })
                        .await?;

                        if let Some(ref after) = self.hooks.after_tool_call {
                            after(&tc.name, &tc.arguments, &Err(e));
                        }
                        continue;
                    }
                }

                self.state = RuntimeState::Acting;
                let result = self.tools.execute(&tc.name, tc.arguments.clone()).await;

                self.state = RuntimeState::Observing;
                let is_error = result.is_err();
                let observation = match &result {
                    Ok(val) => val.clone(),
                    Err(e) => serde_json::json!({ "error": e.to_string() }),
                };

                observations.push(observation.clone());
                tool_result_messages.push(Message::tool_result_text(
                    &tc.id,
                    &tc.name,
                    observation.to_string(),
                ));

                self.emit_event(AgentEvent::ToolExecutionEnd {
                    agent_id: self.config.agent_id.clone(),
                    tool_call_id: tc.id.clone(),
                    result: observation.clone(),
                    is_error,
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                // Reflection: feed tool result into learning pipeline
                if let Some(ref reflection) = self.reflection_engine {
                    if let Err(e) = reflection
                        .process_tool_result(&tc.name, &observation, is_error)
                        .await
                    {
                        tracing::warn!("Reflection process_tool_result failed: {}", e);
                    }
                }

                if let Some(ref after) = self.hooks.after_tool_call {
                    after(&tc.name, &tc.arguments, &result);
                }
            }

            // Update the last RuntimeStep with observations
            if let Some(last_step) = self.steps.last_mut() {
                last_step.observations = observations.clone();
            }

            // Emit ReAct step end after all observations collected
            self.emit_event(AgentEvent::ReActStepEnd {
                agent_id: self.config.agent_id.clone(),
                iteration,
                thought: thought_text.clone(),
                tool_calls: tool_calls.clone(),
                observations: observations.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // Step 5: Update context
            self.state = RuntimeState::Updating;
            self.context.add_message(assistant_msg);
            for tr in &tool_result_messages {
                self.context.add_message(tr.clone());
            }

            self.emit_event(AgentEvent::TurnEnd {
                agent_id: self.config.agent_id.clone(),
                message: Message::assistant_text(thought_text.clone()),
                tool_results: tool_result_messages.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;
        }

        // Max iterations reached
        self.state = RuntimeState::Complete;
        let result = serde_json::json!({
            "status": "max_iterations_reached",
            "iterations": self.config.max_iterations
        });

        self.emit_event(AgentEvent::AgentEnd {
            agent_id: self.config.agent_id.clone(),
            messages: self.context.messages().to_vec(),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        })
        .await?;

        // Reflection: process full context window for semantic patterns
        if let Some(ref reflection) = self.reflection_engine {
            if let Err(e) = reflection.process_context(self.context.messages()).await {
                tracing::warn!("Reflection process_context failed: {}", e);
            }
        }

        // Record observable metrics for this run
        let success = self.state == RuntimeState::Complete;
        let steps = self.steps.len();
        let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
        crate::observable::global_observable().record_run(success, steps, tool_calls);

        Ok(result)
    }

    /// Stream LLM response and accumulate the assistant message.
    async fn think_stream(&mut self, llm: &dyn cog_core::LlmClient) -> SFResult<Message> {
        let mut messages: Vec<Message> = self.context.messages().to_vec();

        // OpenAI-compatible APIs (e.g. Kimi) reject assistant messages whose
        // content would serialize to an empty string. Reasoning-only turns
        // produce `Thinking` blocks that these APIs drop, so replace them with
        // a placeholder before sending. The original context is left untouched.
        messages = messages
            .into_iter()
            .map(|msg| match msg {
                Message::Assistant { content, .. }
                    if !content.iter().any(|b| {
                        matches!(b, ContentBlock::Text { text, .. } if !text.is_empty())
                            || b.is_tool_call()
                    }) =>
                {
                    Message::assistant_text("(reasoning-only assistant turn)")
                }
                msg => msg,
            })
            .collect();

        // Inject available_skills into system prompt if external skill registry is configured.
        // Cache the skill list to avoid querying the registry on every LLM call.
        if let Some(ref registry) = self.external_skill_registry {
            let need_refresh = self
                .skills_cache_instant
                .map(|t| t.elapsed().as_secs() > self.config.skill_cache_ttl_secs)
                .unwrap_or(true);
            let skills = if need_refresh || self.available_skills_cache.is_none() {
                match registry.list().await {
                    Ok(skills) => {
                        self.available_skills_cache = Some(skills.clone());
                        self.skills_cache_instant = Some(std::time::Instant::now());
                        skills
                    }
                    Err(e) => {
                        tracing::warn!("Failed to list available skills: {}", e);
                        self.available_skills_cache.clone().unwrap_or_default()
                    }
                }
            } else {
                self.available_skills_cache.clone().unwrap_or_default()
            };

            if !skills.is_empty() {
                let skills_text = skills
                    .iter()
                    .map(|s| format!("- {}: {} — {}", s.id, s.name, s.description))
                    .collect::<Vec<_>>()
                    .join("\n");
                let skill_prompt = format!(
                    "You have access to the following skills. When your task matches a skill's description, consult that skill by reading its instructions and following them:\n\n{}",
                    skills_text
                );
                // If there's already a system message, prepend skill info to it.
                if let Some(Message::System { content, .. }) = messages.first_mut() {
                    *content = format!("{}\n\n{}", skill_prompt, content);
                } else {
                    messages.insert(0, Message::system(skill_prompt));
                }
            }
        }

        // transform_context hook
        if let Some(ref hook) = self.hooks.transform_context {
            hook(&mut self.context);
            messages = self.context.messages().to_vec();
        }

        let tool_defs = if self.tools.is_empty() {
            None
        } else {
            Some(
                self.tools
                    .list()
                    .iter()
                    .map(|t| ToolDefinition {
                        name: t.name.clone(),
                        description: t.description.clone(),
                        parameters: t.parameters.clone(),
                    })
                    .collect(),
            )
        };

        let options = cog_core::ChatOptions {
            tools: tool_defs,
            ..Default::default()
        };

        tracing::info!(agent_id = %self.config.agent_id, message_count = messages.len(), "AgentRuntime::think_stream calling LLM chat_stream");
        let stream = llm.chat_stream(&messages, &options).await?;
        tracing::info!(agent_id = %self.config.agent_id, "AgentRuntime::think_stream LLM chat_stream returned");

        // Emit MessageStart
        self.emit_event(AgentEvent::MessageStart {
            agent_id: self.config.agent_id.clone(),
            message: Message::assistant(Vec::new()),
            timestamp: chrono::Utc::now(),
        })
        .await?;

        let mut final_message = Message::assistant(Vec::new());

        // Iterate over the streaming events
        let mut stream = stream;
        while let Some(event) = stream.next().await {
            match &event {
                AssistantMessageEvent::TextStart { partial, .. }
                | AssistantMessageEvent::TextDelta { partial, .. }
                | AssistantMessageEvent::ThinkingStart { partial, .. }
                | AssistantMessageEvent::ThinkingDelta { partial, .. }
                | AssistantMessageEvent::ToolCallStart { partial, .. }
                | AssistantMessageEvent::ToolCallDelta { partial, .. } => {
                    final_message = partial.clone();
                }
                AssistantMessageEvent::Done { message, .. } => {
                    final_message = message.clone();
                }
                AssistantMessageEvent::Error { error, .. } => {
                    return Err(SFError::Agent(format!(
                        "LLM stream error: {}",
                        error.content()
                    )));
                }
                _ => {}
            }

            self.emit_event(AgentEvent::MessageUpdate {
                agent_id: self.config.agent_id.clone(),
                assistant_event: event,
                message: final_message.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;
        }

        // Get the final response
        let response = stream.result().await;

        // Build the final message from response content
        let content = if !response.content.is_empty() {
            response.content.clone()
        } else if let Message::Assistant { content, .. } = final_message {
            content
        } else {
            Vec::new()
        };

        let assistant_msg = Message::Assistant {
            content,
            tool_calls: None,
            usage: None,
            timestamp: chrono::Utc::now(),
        };

        self.emit_event(AgentEvent::MessageEnd {
            agent_id: self.config.agent_id.clone(),
            message: assistant_msg.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await?;

        Ok(assistant_msg)
    }

    async fn build_result(
        &self,
        thought: &str,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        let role_name = if let Some(ref skill) = self.config.skill_config {
            skill.name.clone()
        } else {
            self.config.role.to_string()
        };

        // Many reasoning-first models (e.g. kimi-k2.6) already emit the answer
        // as JSON inside the thinking stream. Re-use it directly to avoid a
        // second `response_format: json_object` call that can stall or hang.
        tracing::info!(
            agent_id = %self.config.agent_id,
            thought_len = %thought.len(),
            thought_prefix = %thought.chars().take(120).collect::<String>(),
            "AgentRuntime build_result examining thought"
        );
        if let Some(parsed) = try_extract_json(thought) {
            if !parsed.is_null() && parsed != serde_json::Value::Object(Default::default()) {
                tracing::info!(
                    agent_id = %self.config.agent_id,
                    "AgentRuntime build_result extracted JSON from thought; skipping reformat"
                );
                return Ok(parsed);
            }
        }

        tracing::info!(
            agent_id = %self.config.agent_id,
            "AgentRuntime build_result could not extract JSON; falling back to LLM reformat"
        );

        // Fallback: ask the model to reformat the thought as JSON. Use plain
        // text mode with a tight timeout so a stuck provider cannot block the
        // whole squad execution.
        let user_msg = Message::user(format!(
            "Convert the following thought into a compact JSON object.\n\nRole: {}\nThought: {}\n\nReturn only a JSON object.",
            role_name, thought
        ));

        let options = cog_core::ChatOptions {
            response_format: cog_core::ResponseFormat::Text,
            max_tokens: Some(1024),
            temperature: Some(0.1),
            ..Default::default()
        };

        let reformat_timeout = Duration::from_secs(30);
        match tokio::time::timeout(reformat_timeout, llm.chat(&[user_msg], &options)).await {
            Ok(Ok(response)) => {
                let text: String = response
                    .content
                    .iter()
                    .filter_map(|b| b.as_text())
                    .collect::<Vec<_>>()
                    .join("");
                let text = if !text.is_empty() {
                    text
                } else {
                    response
                        .content
                        .iter()
                        .filter_map(|b| b.as_thinking())
                        .collect::<Vec<_>>()
                        .join("")
                };
                if let Some(parsed) = try_extract_json(&text) {
                    if !parsed.is_null() && parsed != serde_json::Value::Object(Default::default())
                    {
                        return Ok(parsed);
                    }
                }
                Ok(serde_json::json!({ "result": text }))
            }
            Ok(Err(e)) => {
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    error = %e,
                    "AgentRuntime build_result LLM reformat failed; falling back to raw thought"
                );
                Ok(serde_json::json!({ "result": thought }))
            }
            Err(_) => {
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    "AgentRuntime build_result LLM reformat timed out after {}s; falling back to raw thought",
                    reformat_timeout.as_secs()
                );
                Ok(serde_json::json!({ "result": thought }))
            }
        }
    }

    async fn emit_event(&self, event: AgentEvent) -> SFResult<()> {
        // Raw logging for agent/tool events
        if let Some(ref logger) = self.raw_logger {
            let stream = match &event {
                AgentEvent::AgentStart { .. }
                | AgentEvent::AgentEnd { .. }
                | AgentEvent::TurnStart { .. }
                | AgentEvent::TurnEnd { .. } => Some("agent_raw"),
                AgentEvent::ToolExecutionStart { .. } | AgentEvent::ToolExecutionEnd { .. } => {
                    Some("tool_raw")
                }
                _ => None,
            };
            if let Some(stream) = stream {
                let raw = serde_json::to_value(&event).unwrap_or_default();
                let record = cog_core::RawRecord {
                    meta: cog_core::RawMeta {
                        version: "1.0".into(),
                        stream: stream.into(),
                        recorded_at: chrono::Utc::now(),
                        recorded_by: "cog-agent".into(),
                        sequence: 0,
                        trace_id: uuid::Uuid::new_v4().to_string(),
                        span_id: None,
                    },
                    context: cog_core::RawContext {
                        agent_id: Some(self.config.agent_id.clone()),
                        ..Default::default()
                    },
                    payload: cog_core::RawPayload {
                        direction: "internal".into(),
                        transport: "agent_loop".into(),
                        format: Some("json".into()),
                        raw,
                    },
                };
                if let Err(e) = logger.write(record).await {
                    tracing::warn!("RawLogger write failed ({}): {}", stream, e);
                }
            }
        }

        if let Some(ref wal) = self.wal {
            if let Err(e) = wal.append(&event).await {
                tracing::warn!("WAL append failed: {}", e);
            }
        }
        if let Some(ref hook_tx) = self.hook_engine {
            let mut hook_event =
                LifecycleHookEvent::from_agent_event(self.config.agent_id.clone(), event.clone());
            if let Some(ref crew_id) = self.config.crew_id {
                hook_event = hook_event.with_crew_id(crew_id.clone());
            }
            if let Some(ref squad_id) = self.config.squad_id {
                hook_event = hook_event.with_squad_id(squad_id.clone());
            }
            let _ = hook_tx.send(hook_event).await;
            // The engine's forward_tx relays the raw AgentEvent to event_tx.
        } else {
            self.event_tx
                .send(event.clone())
                .await
                .map_err(|_| SFError::Backpressure)?;
        }

        // Reflection: feed the event into the learning pipeline
        if let Some(ref reflection) = self.reflection_engine {
            if let Err(e) = reflection.process_event(&event).await {
                tracing::warn!("Reflection process_event failed: {}", e);
            }
        }

        Ok(())
    }
}

/// Convert a [`WalRecord`] back into an [`AgentEvent`].
/// This is the inverse of [`crate::wal::agent_event_to_wal`].
fn wal_record_to_agent_event(
    record: &cog_core::WalRecord,
) -> Result<AgentEvent, serde_json::Error> {
    use cog_core::WalEventType;

    let type_str = match &record.event_type {
        WalEventType::AgentStart => "agent_start",
        WalEventType::AgentEnd => "agent_end",
        WalEventType::TurnStart => "turn_start",
        WalEventType::TurnEnd => "turn_end",
        WalEventType::MessageStart => "message_start",
        WalEventType::MessageDelta => "message_update",
        WalEventType::MessageEnd => "message_end",
        WalEventType::ToolExecutionStart => "tool_execution_start",
        WalEventType::ToolExecutionDelta => "tool_execution_update",
        WalEventType::ToolExecutionEnd => "tool_execution_end",
        WalEventType::StateChange => "state_change",
        WalEventType::TaskStatusChange => "task_status_change",
        WalEventType::SelfReview => "self_review",
        WalEventType::ReActStepStart => "react_step_start",
        WalEventType::ReActStepEnd => "react_step_end",
        WalEventType::AgentError => "agent_error",
        WalEventType::ResourceAlert => "resource_alert",
        WalEventType::Heartbeat => "heartbeat",
        WalEventType::CheckpointSaved => "checkpoint_saved",
        WalEventType::Custom { name } => {
            return Err(serde::de::Error::custom(format!(
                "cannot replay custom WAL event: {name}"
            )));
        }
    };

    let mut payload = record.payload.clone();
    payload["type"] = serde_json::Value::String(type_str.into());
    serde_json::from_value(payload)
}

#[async_trait::async_trait]
impl cog_core::AgentRuntime for AgentRuntime {
    async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
    ) -> cog_core::SFResult<serde_json::Value> {
        self.run(input, llm).await
    }

    fn agent_id(&self) -> &str {
        &self.config.agent_id
    }

    fn role(&self) -> &str {
        self.config.role.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SkillConfig;

    #[test]
    fn agent_loop_config_uses_skill_prompt() {
        let skill = SkillConfig {
            skill_id: "custom-planner".into(),
            name: "Custom Planner".into(),
            system_prompt: "You are a custom planning expert.".into(),
            tools: vec!["md".into()],
            max_iterations: 42,
            role_type: "planner".into(),
        };

        let config = RuntimeConfig {
            skill_config: Some(skill.clone()),
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new(config, tx);
        // max_iterations is still overridden by skill config inside AgentRuntime::new
        assert_eq!(agent_loop.config.max_iterations, 42);
        // No system message should be injected when using structured JSON input
        let messages = agent_loop.get_context().messages();
        assert!(
            messages
                .iter()
                .all(|m| !matches!(m, cog_core::Message::System { .. })),
            "Context window should not contain any system messages"
        );
    }

    #[test]
    fn agent_loop_config_skill_overrides_max_iterations() {
        let skill = SkillConfig {
            skill_id: "fast-evaluator".into(),
            name: "Fast Evaluator".into(),
            system_prompt: "Evaluate quickly.".into(),
            tools: vec![],
            max_iterations: 3,
            role_type: "evaluator".into(),
        };

        let config = RuntimeConfig {
            max_iterations: 100,
            skill_config: Some(skill),
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new(config, tx);
        // max_iterations should be overridden by skill config
        assert_eq!(agent_loop.config.max_iterations, 3);
    }

    // ─── Mock WAL backend for testing ───

    #[derive(Debug, Default)]
    struct MockWalBackend {
        records: std::sync::Mutex<Vec<cog_core::WalRecord>>,
    }

    #[async_trait::async_trait]
    impl cog_core::WalBackend for MockWalBackend {
        async fn append(&self, record: cog_core::WalRecord) -> Result<u64, cog_core::WalError> {
            let mut records = self.records.lock().unwrap();
            records.push(record.clone());
            Ok(record.seq)
        }

        async fn read_since(
            &self,
            _session_id: &str,
            seq: u64,
        ) -> Result<Vec<cog_core::WalRecord>, cog_core::WalError> {
            let records = self.records.lock().unwrap();
            Ok(records.iter().filter(|r| r.seq >= seq).cloned().collect())
        }

        async fn read_latest(
            &self,
            _session_id: &str,
            limit: usize,
        ) -> Result<Vec<cog_core::WalRecord>, cog_core::WalError> {
            let records = self.records.lock().unwrap();
            let start = records.len().saturating_sub(limit);
            Ok(records[start..].to_vec())
        }

        async fn truncate_before(
            &self,
            _session_id: &str,
            seq: u64,
        ) -> Result<(), cog_core::WalError> {
            let mut records = self.records.lock().unwrap();
            records.retain(|r| r.seq >= seq);
            Ok(())
        }

        async fn next_seq(&self, _session_id: &str) -> Result<u64, cog_core::WalError> {
            let records = self.records.lock().unwrap();
            Ok(records.last().map(|r| r.seq + 1).unwrap_or(0))
        }
    }

    /// End-to-end test: checkpoint → persist → restore → replay.
    /// Verifies that events written to WAL after a checkpoint can be
    /// replayed onto a fresh AgentRuntime, restoring the exact event stream.
    #[tokio::test]
    async fn wal_checkpoint_restore_replay_e2e() {
        let session_id = "test-session-42";
        let agent_id = "test-agent";

        // 1. Build runtime with WAL
        let backend = Arc::new(MockWalBackend::default());
        let agent_wal = crate::wal::AgentWal::new(backend.clone(), session_id)
            .await
            .expect("create AgentWal");

        let (event_tx, mut event_rx) = mpsc::channel::<AgentEvent>(100);
        let config = RuntimeConfig {
            agent_id: agent_id.into(),
            ..Default::default()
        };
        let runtime = AgentRuntime::new(config, event_tx).with_wal(Arc::new(agent_wal));

        // Spawn a drain task so emit_event never backpressures
        let drain_handle = tokio::spawn(async move {
            let mut count = 0usize;
            while event_rx.recv().await.is_some() {
                count += 1;
            }
            count
        });

        // 2. Emit pre-checkpoint events (seq 0, 1, 2)
        let pre_events = vec![
            AgentEvent::TurnStart {
                agent_id: agent_id.into(),
                timestamp: chrono::Utc::now(),
            },
            AgentEvent::StateChange {
                agent_id: agent_id.into(),
                from: "idle".into(),
                to: "thinking".into(),
                crew_id: None,
                squad_id: None,
                timestamp: chrono::Utc::now(),
            },
            AgentEvent::TurnStart {
                agent_id: agent_id.into(),
                timestamp: chrono::Utc::now(),
            },
        ];
        for ev in &pre_events {
            runtime
                .emit_event(ev.clone())
                .await
                .expect("emit pre-checkpoint event");
        }

        // 3. Take checkpoint — captures event_offset = current seq (should be 3)
        let checkpoint = runtime.checkpoint("task-1").expect("checkpoint");
        assert_eq!(
            checkpoint.event_offset, 3,
            "checkpoint should capture offset 3"
        );

        // 4. Emit post-checkpoint events (seq 3, 4)
        let post_events = vec![
            AgentEvent::StateChange {
                agent_id: agent_id.into(),
                from: "thinking".into(),
                to: "acting".into(),
                crew_id: None,
                squad_id: None,
                timestamp: chrono::Utc::now(),
            },
            AgentEvent::TurnStart {
                agent_id: agent_id.into(),
                timestamp: chrono::Utc::now(),
            },
        ];
        for ev in &post_events {
            runtime
                .emit_event(ev.clone())
                .await
                .expect("emit post-checkpoint event");
        }

        // Drop the original runtime so the drain task can finish
        drop(runtime);
        let original_received = drain_handle.await.expect("drain task");
        assert_eq!(
            original_received, 5,
            "original runtime should have emitted 5 events"
        );

        // 5. Simulate restart: new runtime + restore checkpoint + replay WAL
        let (new_event_tx, mut new_event_rx) = mpsc::channel::<AgentEvent>(100);
        let new_config = RuntimeConfig {
            agent_id: agent_id.into(),
            ..Default::default()
        };
        let new_wal = crate::wal::AgentWal::new(backend.clone(), session_id)
            .await
            .expect("create AgentWal for new runtime");
        let mut new_runtime =
            AgentRuntime::new(new_config, new_event_tx).with_wal(Arc::new(new_wal));

        new_runtime
            .restore(&checkpoint)
            .expect("restore checkpoint");
        let replayed = new_runtime
            .replay_events(checkpoint.event_offset)
            .await
            .expect("replay events");
        assert_eq!(replayed, 2, "should replay 2 post-checkpoint events");

        // 6. Collect replayed events from the new receiver
        let mut replayed_events = Vec::new();
        while let Ok(Some(ev)) =
            tokio::time::timeout(std::time::Duration::from_millis(50), new_event_rx.recv()).await
        {
            replayed_events.push(ev);
        }

        assert_eq!(
            replayed_events.len(),
            2,
            "new runtime should receive 2 replayed events"
        );
        assert!(
            matches!(replayed_events[0], AgentEvent::StateChange { .. }),
            "first replayed event should be StateChange"
        );
        assert!(
            matches!(replayed_events[1], AgentEvent::TurnStart { .. }),
            "second replayed event should be TurnStart"
        );
    }
}
