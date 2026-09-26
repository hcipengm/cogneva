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
    /// Where the per-task token census of a finished run is written. Optional
    /// like the other injected handles — the run does not depend on it — but a
    /// missing gateway is the difference between a per-task reading and none,
    /// so its absence is reported rather than assumed.
    observability: Option<Arc<dyn cog_core::ObservabilityGateway>>,
    /// What the run in flight has been billed for so far. Reset by every
    /// [`Self::run_scoped`]; read when the run ends.
    run_usage: RunUsage,
    /// Turns the run in flight has actually started.
    run_iterations: u32,
}

/// The token census of one run: every billed LLM call the run made, folded into
/// running totals.
///
/// The totals live here rather than in the events stream because the response
/// carrying them is consumed and dropped at the end of the streaming call — the
/// assistant message handed back up has no token fields, so a run's spend was
/// visible only to the process-wide counters, never to the task that paid it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RunUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    /// Billed calls, including the deliverable-reformat fallback.
    pub llm_calls: u32,
}

impl RunUsage {
    /// Fold one billed call into the totals.
    ///
    /// The total is the larger of what the upstream reported and the sum of the
    /// halves. Upstreams differ here: some omit the total while reporting both
    /// halves, others fold cached input into it, and a total smaller than its
    /// own parts would make the per-task reading contradict itself.
    fn add(&mut self, usage: &cog_core::Usage) {
        let prompt = u64::from(usage.input);
        let completion = u64::from(usage.output);
        self.prompt_tokens = self.prompt_tokens.saturating_add(prompt);
        self.completion_tokens = self.completion_tokens.saturating_add(completion);
        self.total_tokens = self
            .total_tokens
            .saturating_add(u64::from(usage.total_tokens).max(prompt.saturating_add(completion)));
        self.llm_calls = self.llm_calls.saturating_add(1);
    }
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

/// The deliverable held in a finished turn's text: a JSON object or array,
/// possibly inside a fence or surrounded by prose. `None` means the turn
/// produced words but no product.
///
/// This is the same predicate `build_result` uses to decide it has something
/// to hand over, and the two must stay one function: it is what separates
/// "the run wrote a deliverable" from "the run wrote a sentence about one".
fn deliverable_in(text: &str) -> Option<serde_json::Value> {
    try_extract_json(text)
        .filter(|v| !v.is_null() && *v != serde_json::Value::Object(Default::default()))
}

/// The text of an assistant turn, as the loop reads it.
/// Text blocks win; reasoning-only models (e.g. kimi-k2.6) put the answer in
/// thinking/reasoning blocks instead, so those are the fallback rather than a
/// separate reading path.
fn assistant_text(msg: &Message) -> String {
    msg.content_blocks()
        .map(|blocks| {
            let text = blocks
                .iter()
                .filter_map(|b| b.as_text())
                .collect::<Vec<_>>()
                .join("");
            if !text.is_empty() {
                text
            } else {
                blocks
                    .iter()
                    .filter_map(|b| b.as_thinking())
                    .collect::<Vec<_>>()
                    .join("")
            }
        })
        .unwrap_or_default()
}

/// The ask put to the model when the iteration budget ends with tool calls
/// still pending. It has to say that tools are gone — otherwise the model
/// answers with another tool call and the turn buys nothing — and it has to
/// say what shape the answer takes, because this is the only turn in the run
/// that is not part of the ReAct loop.
const FINAL_DRAFT_INSTRUCTION: &str = "Your iteration budget for this run is spent and no more tool calls are \
     available: any call you just issued was not executed. Answer now, from what is already in this \
     conversation. Write your final deliverable as a single compact JSON object matching the output \
     shape your role instructions specify, and nothing else — no preamble, no explanation, no \
     markdown fences. If something is missing, still deliver the object with the fields you have and \
     name what is missing inside it.";

impl AgentRuntime {
    /// Access the loop configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn new(mut config: RuntimeConfig, event_tx: mpsc::Sender<AgentEvent>) -> Self {
        if let Some(ref skill) = config.skill_config {
            config.max_iterations = skill.max_iterations;
        }
        // 预算由这个角色自己跑出来的读数推：种子（配置面或技能面）只在还没有
        // 观测时生效。写死一个数在这里就是把它交给一个永远不知道工作量的常量，
        // 而它要裁的恰恰是"这一轮还要不要接着干"。
        config.max_iterations = crate::observable::global_observable()
            .iteration_budget_for(&config.role, config.max_iterations);

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
            observability: None,
            run_usage: RunUsage::default(),
            run_iterations: 0,
        }
    }

    /// Begin the next run on a clean context, keeping everything this instance
    /// was assembled with.
    ///
    /// A reset is about the run, not about the runtime. Rebuilding the instance
    /// from its configuration would keep every field the configuration carries
    /// and drop every one the caller attached afterwards — the tool registry
    /// above all, but also the hook engine, the WAL, the checkpoint store, the
    /// sandbox backend and the registries. A loop rebuilt that way answers with
    /// nothing to call and still looks healthy: it starts, it thinks, it
    /// produces a final answer with no tool in it. Clearing in place cannot
    /// lose a field, including fields this type gains later.
    pub fn reset(&mut self) {
        self.context.clear();
        self.state = RuntimeState::Idle;
        self.steps.clear();
        self.start_time = None;
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

    /// Hand the loop the gateway its per-task token census is written to.
    pub fn with_observability(mut self, gateway: Arc<dyn cog_core::ObservabilityGateway>) -> Self {
        self.observability = Some(gateway);
        self
    }

    /// Give this run its tools, narrowed to the role's declared set.
    ///
    /// The registry handed in is the whole fleet's; which of it this role may
    /// reach is a property of the role, and the role's skill is where it is
    /// declared — the same file that already carries the role's iteration
    /// budget. Restricting here rather than at the registry means the shared
    /// registry keeps every tool and each run gets a view of it, so one role's
    /// boundary cannot become another's.
    ///
    /// A role with no skill, or a skill that declares no tools, gets the whole
    /// registry: an absent list is no evidence about what the role needs, and
    /// inventing a boundary from it would restrict a role nobody has described.
    ///
    /// A name the registry does not hold is reported here. Narrowing is silent
    /// by design — a stale entry must not take the role down — but a list that
    /// resolves to nothing at all leaves the role with no tools, and that reads
    /// at the model as a role that never had any. The report is the only thing
    /// standing between the two, so it names the tools and the count that
    /// survived.
    pub fn with_tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = match self.config.skill_config.as_ref() {
            Some(skill) if !skill.tools.is_empty() => {
                let missing = tools.missing_tools(&skill.tools);
                if !missing.is_empty() {
                    // Both sides of the count are deduplicated: `missing` already
                    // is, and a declared list that repeats a name would otherwise
                    // subtract a shorter missing list from a longer declared one
                    // and report a role as better off than it is.
                    let declared: std::collections::HashSet<&String> = skill.tools.iter().collect();
                    let kept = declared.len() - missing.len();
                    tracing::warn!(
                        skill = %skill.skill_id,
                        missing = ?missing,
                        kept,
                        declared = declared.len(),
                        "skill declares tools the registry does not hold; the role runs without them"
                    );
                }
                tools.restricted_to(&skill.tools)
            }
            _ => tools,
        };
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
    /// Returns a [`cog_core::AgentCheckpoint`] that can be persisted via a
    /// [`cog_core::CheckpointStore`] and later restored to resume execution.
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

    /// Restore agent state from a [`cog_core::AgentCheckpoint`].
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
    /// Re-emits each event directly through the broadcast sink so that
    /// downstream consumers (metrics, etc.) see the full history after a
    /// snapshot restore. Replay bypasses the runtime mpsc: the bus delivery
    /// path downstream of it only accepts live traffic.
    /// Returns the number of events replayed.
    pub async fn replay_events(
        &self,
        from_offset: u64,
        sink: &tokio::sync::broadcast::Sender<AgentEvent>,
    ) -> SFResult<usize> {
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
                // 重放事件直发 broadcast（本地状态重建），不进运行时 mpsc：
                // mpsc 下游的总线投递只认活体流量，重放再进总线会被摄取侧
                // 当成新事件重复建档。无订阅者时 send 报错属正常，不算失败。
                let _ = sink.send(event);
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
        }
        .with_actor(&format!("agent:{}", self.config.role));

        llm.chat_stream(&messages, &options).await
    }

    /// Execute the agent loop with streaming LLM.
    /// Backpressure: event_tx is a bounded channel; slow consumers naturally block.
    pub async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        self.run_scoped(input, llm, None).await
    }

    /// Run with an explicit DAG task identity. Tool calls inside this run
    /// carry `task_id` and the runtime's own agent id into sandbox requests.
    pub async fn run_scoped(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
        run_task_id: Option<&str>,
    ) -> SFResult<serde_json::Value> {
        // The census is a property of this run, so it starts here and is
        // reported from exactly one place — the run's only exit. Doing it at
        // the answer-recording site instead would leave every run that ended
        // any other way (budget spent, upstream error, stall) unmeasured, and
        // those are the runs a token investigation is about.
        self.run_usage = RunUsage::default();
        self.run_iterations = 0;
        let started = std::time::Instant::now();
        let outcome = self.run_turns(input, llm, run_task_id).await;
        self.report_run_census(run_task_id, started.elapsed()).await;
        outcome
    }

    /// Write this run's token census where the per-task readers look for it.
    ///
    /// Best-effort on purpose: a metrics write must not turn a delivered task
    /// into a failed one. The loss is named rather than swallowed, because a
    /// missing row is indistinguishable from a task that never ran.
    async fn report_run_census(&self, run_task_id: Option<&str>, elapsed: std::time::Duration) {
        let Some(task_id) = run_task_id else {
            return;
        };
        let Some(gateway) = self.observability.as_ref() else {
            tracing::warn!(
                agent_id = %self.config.agent_id,
                task_id,
                total_tokens = self.run_usage.total_tokens,
                "no observability gateway attached; this run's token census is not recorded"
            );
            return;
        };
        let metrics = cog_core::TaskMetrics {
            task_id: task_id.to_string(),
            total_tokens: self.run_usage.total_tokens,
            prompt_tokens: self.run_usage.prompt_tokens,
            completion_tokens: self.run_usage.completion_tokens,
            tool_calls: self.steps.iter().map(|s| s.tool_calls.len()).sum::<usize>() as u32,
            iterations: self.run_iterations,
            duration_ms: elapsed.as_millis() as u64,
            timestamp: chrono::Utc::now(),
        };
        if let Err(e) = gateway.record_task_metrics(metrics).await {
            tracing::warn!(
                agent_id = %self.config.agent_id,
                task_id,
                total_tokens = self.run_usage.total_tokens,
                error = %e,
                "run census write failed; this task's token metrics are missing"
            );
        }
    }

    /// The run itself, from the first turn to its answer.
    async fn run_turns(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
        run_task_id: Option<&str>,
    ) -> SFResult<serde_json::Value> {
        tracing::info!(agent_id = %self.config.agent_id, task_id = run_task_id.unwrap_or(""), "AgentRuntime::run started");
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
            self.run_iterations = iteration + 1;
            // --- Turn Start ---
            self.emit_event(AgentEvent::TurnStart {
                agent_id: self.config.agent_id.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // Step 1: Thinking (streaming).
            //
            // No whole-turn wall-clock cap here: slow models legitimately spend
            // many minutes emitting large generations, and killing a live
            // stream mid-flight wastes the tokens already produced and gets
            // misclassified as an environment failure. Hang protection lives
            // inside think_stream as a per-event stall timeout instead.
            self.state = RuntimeState::Thinking;
            tracing::info!(agent_id = %self.config.agent_id, "AgentRuntime::run calling think_stream");
            let assistant_msg = self.think_stream(llm).await?;

            let thought_text = assistant_text(&assistant_msg);

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
                return self
                    .deliver(llm, thought_text, assistant_msg, iteration + 1)
                    .await;
            }

            // Last iteration but still has tool calls: the loop's iteration
            // budget is spent. Before writing the run off, ask the model once
            // more for its answer with no tools attached: every turn so far was
            // reading and testing, and the model has never been asked to hand
            // anything over. Writing it off here records a question that was
            // never put as one that cannot be answered — and the budget it
            // would have answered out of is already paid for.
            if iteration == self.config.max_iterations - 1 {
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    max_iterations = self.config.max_iterations,
                    pending_tool_calls = tool_calls.len(),
                    "agent loop exhausted its iteration budget with tool calls still pending; \
                     asking for a final draft without tools before writing the run off"
                );
                // The ask below is another request on this same transcript, and
                // an assistant turn whose tool calls have no results is one no
                // provider will accept. Close it first: the calls are recorded
                // as never run (the truth — the budget ended before they did),
                // then the plain-text ask goes in as the next user turn.
                self.context.add_message(assistant_msg);
                let not_run = serde_json::json!({
                    "error": "not executed: the iteration budget ended before this tool call ran"
                });
                for tc in &tool_calls {
                    self.context.add_message(Message::tool_result_text(
                        &tc.id,
                        &tc.name,
                        not_run.to_string(),
                    ));
                }
                self.context
                    .add_message(Message::user(FINAL_DRAFT_INSTRUCTION));
                let forced = match self.think_stream_final(llm).await {
                    Ok(msg) => {
                        let text = assistant_text(&msg);
                        if text.trim().is_empty() {
                            // 追问过了，模型还是不交东西：这件事必须跟着哨兵走。
                            // 少了它，读的人分不出「还没被问过」与「被直接问过
                            // 还是没答」，而这两种的处理方式不一样。
                            (None, "empty")
                        } else if deliverable_in(&text).is_none() {
                            // 有字没产物。走正常路径的话，这段字会被包成
                            // {"result": <text>} 交给评估——把「被截断」读成
                            // 「交了东西」，还给了重试一个看上去合法的理由。
                            // 被工具拿走后仍写不出交付物的，就是这一轮的实话。
                            (None, "malformed")
                        } else {
                            (Some((text, msg)), "delivered")
                        }
                    }
                    // 追问本身失败（上游抖动、流断）不改写这一轮的定性：预算确实
                    // 用完了，照哨兵走终止性失败，比把一次追问的传输错误报成整轮
                    // 失败更接近事实。
                    Err(e) => {
                        tracing::warn!(
                            agent_id = %self.config.agent_id,
                            error = %e,
                            "the final-draft call failed; the run still ends on its spent budget"
                        );
                        (None, "unavailable")
                    }
                };
                if let Some((draft, msg)) = forced.0 {
                    tracing::info!(
                        agent_id = %self.config.agent_id,
                        "the final draft recovered an answer the loop's budget had no room for"
                    );
                    return self
                        .deliver(llm, draft, msg, self.config.max_iterations + 1)
                        .await;
                }
                self.state = RuntimeState::Complete;
                // The run stops here having bought nothing: the model was still
                // reading and testing when the budget ended, so no answer was
                // ever written. Without this line the exhaustion is invisible —
                // the sentinel below is the only trace, and every reader of the
                // result sees an empty output, not a spent budget.
                let result = serde_json::json!({
                    "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                    "iterations": self.config.max_iterations,
                    "pending_tool_calls": tool_calls.len(),
                    "final_draft": forced.1,
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

                let steps = self.steps.len();
                let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
                crate::observable::global_observable().record_run(
                    &self.config.role,
                    crate::observable::RunOutcome::BudgetExhausted,
                    iteration + 1,
                    steps,
                    tool_calls,
                );

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
                let scope = run_task_id.map(|tid| crate::tools::ToolScope {
                    task_id: tid.to_string(),
                    agent_id: self.config.agent_id.clone(),
                });
                let result = self
                    .tools
                    .execute_scoped(&tc.name, tc.arguments.clone(), scope.as_ref())
                    .await;

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
        tracing::warn!(
            agent_id = %self.config.agent_id,
            max_iterations = self.config.max_iterations,
            "agent loop exhausted its iteration budget; \
             the run stops mid-exploration and returns no deliverable"
        );
        let result = serde_json::json!({
            "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
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
        let steps = self.steps.len();
        let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
        crate::observable::global_observable().record_run(
            &self.config.role,
            crate::observable::RunOutcome::BudgetExhausted,
            self.config.max_iterations,
            steps,
            tool_calls,
        );

        Ok(result)
    }

    /// Close the run on an answer: extract the deliverable from `draft`, record
    /// the turn, and report the run as delivered.
    ///
    /// Both the ordinary path (the loop stopped because the model had nothing
    /// left to call) and the forced path (the budget ended and the model was
    /// asked directly) end here, so the extraction, event emission and run
    /// accounting cannot drift apart between them.
    async fn deliver(
        &mut self,
        llm: &dyn cog_core::LlmClient,
        draft: String,
        turn_message: Message,
        iterations: u32,
    ) -> SFResult<serde_json::Value> {
        self.state = RuntimeState::Complete;
        let result_timeout = Duration::from_secs(180);
        let result = match tokio::time::timeout(result_timeout, self.build_result(&draft, llm))
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
            thought: Some(draft),
            tool_calls: Vec::new(),
            observations: Vec::new(),
            result: Some(result.clone()),
            error: None,
            timestamp: chrono::Utc::now(),
        });

        self.context.add_message(turn_message);

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

        let steps = self.steps.len();
        let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
        crate::observable::global_observable().record_run(
            &self.config.role,
            crate::observable::RunOutcome::Delivered,
            iterations,
            steps,
            tool_calls,
        );

        Ok(result)
    }

    /// Stream LLM response and accumulate the assistant message.
    async fn think_stream(&mut self, llm: &dyn cog_core::LlmClient) -> SFResult<Message> {
        self.think_stream_with(llm, false).await
    }

    /// The same call with no tools offered: the one ask that has to produce an
    /// answer rather than another action.
    async fn think_stream_final(&mut self, llm: &dyn cog_core::LlmClient) -> SFResult<Message> {
        self.think_stream_with(llm, true).await
    }

    async fn think_stream_with(
        &mut self,
        llm: &dyn cog_core::LlmClient,
        without_tools: bool,
    ) -> SFResult<Message> {
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

        let tool_defs = if without_tools || self.tools.is_empty() {
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
        }
        .with_actor(&format!("agent:{}", self.config.role));

        // Stall-based hang protection: the clock resets on every stream event,
        // so a slow-but-alive generation is never aborted — only a connection
        // that stops making progress for the full window is treated as dead.
        let stall_timeout = Duration::from_secs(self.config.think_stall_timeout_secs.max(1));
        let stall_err = |phase: &str| {
            SFError::Agent(format!(
                "AgentRuntime::think_stream timed out: no LLM stream progress for {}s while {} for agent {}",
                stall_timeout.as_secs(),
                phase,
                self.config.agent_id
            ))
        };

        tracing::info!(agent_id = %self.config.agent_id, message_count = messages.len(), "AgentRuntime::think_stream calling LLM chat_stream");
        let mut stream = match tokio::time::timeout(
            stall_timeout,
            llm.chat_stream(&messages, &options),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                let err = stall_err("establishing the stream");
                tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime think_stream stall timeout");
                return Err(err);
            }
        };
        tracing::info!(agent_id = %self.config.agent_id, "AgentRuntime::think_stream LLM chat_stream returned");

        // Emit MessageStart
        self.emit_event(AgentEvent::MessageStart {
            agent_id: self.config.agent_id.clone(),
            message: Message::assistant(Vec::new()),
            timestamp: chrono::Utc::now(),
        })
        .await?;

        let mut final_message = Message::assistant(Vec::new());

        // Iterate over the streaming events. Each await is stall-guarded: an
        // event arriving resets the clock, so long generations survive while a
        // hung stream is aborted after one quiet window.
        loop {
            let event = match tokio::time::timeout(stall_timeout, stream.next()).await {
                Ok(Some(event)) => event,
                Ok(None) => break,
                Err(_) => {
                    let err = stall_err("streaming events");
                    tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime think_stream stall timeout");
                    return Err(err);
                }
            };
            if let AssistantMessageEvent::Error { error, .. } = &event {
                return Err(SFError::Agent(format!(
                    "LLM stream error: {}",
                    error.content()
                )));
            }
            event.apply(&mut final_message);

            self.emit_event(AgentEvent::MessageUpdate {
                agent_id: self.config.agent_id.clone(),
                assistant_event: event,
                timestamp: chrono::Utc::now(),
            })
            .await?;
        }

        // Get the final response. The stream has already ended, so this
        // normally resolves instantly; the stall guard only covers a producer
        // that closed the event channel without ever completing the result.
        let response = match tokio::time::timeout(stall_timeout, stream.result()).await {
            Ok(response) => response,
            Err(_) => {
                let err = stall_err("awaiting the final response");
                tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime think_stream stall timeout");
                return Err(err);
            }
        };
        // Bill this call to the run before anything can drop the response: the
        // assistant message built below carries no usage, so this line is the
        // only place the run's spend is still readable.
        self.run_usage.add(&response.usage);

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
        &mut self,
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
        if let Some(parsed) = deliverable_in(thought) {
            tracing::info!(
                agent_id = %self.config.agent_id,
                "AgentRuntime build_result extracted JSON from thought; skipping reformat"
            );
            return Ok(parsed);
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
        }
        .with_actor(&format!("agent:{}", self.config.role));

        let reformat_timeout = Duration::from_secs(30);
        match tokio::time::timeout(reformat_timeout, llm.chat(&[user_msg], &options)).await {
            Ok(Ok(response)) => {
                // The reformat is a billed call like any other turn; leaving it
                // out would understate the runs that needed it, which are the
                // ones whose answer came out of a fallback.
                self.run_usage.add(&response.usage);
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
                if let Some(parsed) = deliverable_in(&text) {
                    return Ok(parsed);
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

    /// A reset clears the run's context and keeps everything the instance was
    /// built with.
    ///
    /// The defect this replaces was not "reset forgot the tools" so much as
    /// "reset rebuilt the loop from a config": every attachment the caller had
    /// made — the tools, the hook engine, the WAL, the checkpoint store, the
    /// sandbox backend, the registries — was silently gone, and the rebuilt
    /// loop then started, thought and answered with no tool call in it. Nothing
    /// about that run looks broken; it looks like a model that chose not to use
    /// its tools.
    ///
    /// The tool registry is the reading taken here because it is the attachment
    /// whose absence is invisible. The context assertion is the other half: a
    /// reset that kept the tools and the old conversation would not be a reset.
    #[test]
    fn reset_keeps_the_attachments_and_clears_the_context() {
        let config = RuntimeConfig {
            role: "reset-attachment-test".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let registry = crate::tools::ToolRegistry::new();
        cog_core::ToolRegistry::register(&registry, crate::tools::builtins::read_file());
        let mut agent_loop = AgentRuntime::new(config, tx).with_tools(registry);
        assert!(
            !agent_loop.get_tools().is_empty(),
            "the fixture has to start with a tool, or the assertion below proves nothing"
        );

        // Seed the conversation through the restore path, which is the only
        // way in from outside: it exists precisely to put someone else's
        // messages into this window.
        let seeded = cog_core::AgentCheckpoint {
            checkpoint_id: "reset-fixture".into(),
            task_id: "reset-fixture".into(),
            agent_state: serde_json::json!({}),
            context_window: vec![cog_core::Message::user("a previous run's input")],
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        };
        agent_loop
            .restore(&seeded)
            .expect("fixture context restores");
        assert!(
            !agent_loop.get_context().messages().is_empty(),
            "the fixture's conversation has to be in the window before a reset can be said to clear it"
        );

        agent_loop.reset();

        assert!(
            !agent_loop.get_tools().is_empty(),
            "a reset must not take the loop's tools with it"
        );
        assert!(
            agent_loop.get_context().messages().is_empty(),
            "a reset that keeps the previous conversation is not a reset"
        );
    }

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

        // 角色取一个只有本用例用的名字：预算现在还跟着"这个角色跑出来的读数"走，
        // 借用别的用例会写进去的角色名，断言就变成了对执行顺序的断言。
        let config = RuntimeConfig {
            role: "skill-prompt-test".into(),
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
            role: "skill-budget-test".into(),
            max_iterations: 100,
            skill_config: Some(skill),
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new(config, tx);
        // max_iterations should be overridden by skill config
        assert_eq!(agent_loop.config.max_iterations, 3);
    }

    /// 这个角色自己跑出来的读数决定上限：种子（配置面 / 技能面）只在还没有
    /// 观测时生效，有了观测就不再是那个数。写死一个常量的坏处正在这里——它
    /// 永远不知道这一轮的工作量，而它要裁的恰恰是"还要不要接着干"。
    #[test]
    fn the_iteration_budget_follows_this_roles_own_runs() {
        let role = "budget-calibrated-test";
        for iterations in [6u32, 8] {
            crate::observable::global_observable().record_run(
                role,
                crate::observable::RunOutcome::Delivered,
                iterations,
                20,
                5,
            );
        }

        let config = RuntimeConfig {
            role: role.into(),
            max_iterations: 10,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new(config, tx);

        assert!(
            agent_loop.config.max_iterations > 10,
            "the seed was binding for this role's own runs, so it must not stay the ceiling: {}",
            agent_loop.config.max_iterations
        );
        assert!(
            agent_loop.config.max_iterations > 8,
            "the ceiling must admit the longest run this role has delivered"
        );
    }

    fn registry_of(names: &[&str]) -> ToolRegistry {
        let registry = ToolRegistry::new();
        for name in names {
            cog_core::ToolRegistry::register(
                &registry,
                cog_core::Tool {
                    name: (*name).into(),
                    description: String::new(),
                    parameters: serde_json::json!({}),
                    implementation: cog_core::ToolImplementation::Shell(cog_core::ShellOp::Command),
                },
            );
        }
        registry
    }

    fn runtime_with(skill: Option<SkillConfig>, role: &str) -> AgentRuntime {
        let config = RuntimeConfig {
            role: role.into(),
            skill_config: skill,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        AgentRuntime::new(config, tx)
    }

    fn skill_naming(role: &str, tools: &[&str]) -> SkillConfig {
        SkillConfig {
            skill_id: format!("{role}-boundary-test"),
            name: role.into(),
            system_prompt: String::new(),
            tools: tools.iter().map(|t| (*t).into()).collect(),
            max_iterations: 5,
            role_type: role.into(),
        }
    }

    /// 角色说自己能用什么，跑起来就只能用什么：边界落在运行时持有的那份工具上，
    /// 而不是只落在给模型看的定义列表上——后者是一句建议。
    #[test]
    fn a_run_holds_the_tools_its_skill_declares() {
        let whole = registry_of(&["read_file", "write_file", "run_command"]);
        let runtime = runtime_with(
            Some(skill_naming("boundary-narrow-test", &["read_file"])),
            "boundary-narrow-test",
        )
        .with_tools(whole.clone());

        let mut held = runtime.tools.names();
        held.sort();
        assert_eq!(held, vec!["read_file".to_string()]);

        // 收窄的是这一轮手上的那份，共享的那份不动：否则一个角色的边界会变成别人的。
        assert_eq!(
            whole.names().len(),
            3,
            "narrowing one run must not shrink the registry it was handed"
        );
    }

    /// 没声明边界就没有边界。空名单不是"什么都不许"，它是一个没人描述过这个角色
    /// 需要什么的证据；凭它把角色清空，是把缺失当成了限制。
    #[test]
    fn an_undeclared_boundary_leaves_the_run_the_whole_registry() {
        let whole = registry_of(&["read_file", "write_file", "run_command"]);

        for skill in [None, Some(skill_naming("boundary-absent-test", &[]))] {
            let runtime = runtime_with(skill, "boundary-absent-test").with_tools(whole.clone());
            assert_eq!(
                runtime.tools.names().len(),
                3,
                "an absent tool list is not a boundary"
            );
        }
    }

    /// 名单里写错一个名字，只该少那一个；剩下的照常收窄，构建不出错。
    /// 让一处笔误把整轮跑挂掉，是把整理遗漏变成了一次事故。
    #[test]
    fn a_name_with_no_tool_behind_it_narrows_without_failing() {
        let whole = registry_of(&["read_file", "run_command"]);
        let runtime = runtime_with(
            Some(skill_naming(
                "boundary-typo-test",
                &["read_file", "reed_file"],
            )),
            "boundary-typo-test",
        )
        .with_tools(whole);

        assert_eq!(runtime.tools.names(), vec!["read_file".to_string()]);
    }

    /// 没有观测就原样用种子：凭一个没看见过的事实改数字，只是把猜数换了个地方。
    #[test]
    fn a_role_without_observations_keeps_its_seed_budget() {
        let config = RuntimeConfig {
            role: "budget-unobserved-test".into(),
            max_iterations: 7,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new(config, tx);
        assert_eq!(agent_loop.config.max_iterations, 7);
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
        let (new_event_tx, _new_event_rx) = mpsc::channel::<AgentEvent>(100);
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
        let (replay_tx, mut replay_rx) = tokio::sync::broadcast::channel::<AgentEvent>(100);
        let replayed = new_runtime
            .replay_events(checkpoint.event_offset, &replay_tx)
            .await
            .expect("replay events");
        assert_eq!(replayed, 2, "should replay 2 post-checkpoint events");

        // 6. Collect replayed events from the broadcast receiver
        let mut replayed_events = Vec::new();
        while let Ok(Ok(ev)) =
            tokio::time::timeout(std::time::Duration::from_millis(50), replay_rx.recv()).await
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

    // ─── Timing-controlled mock LLM for stall detection tests ───

    struct Chunk {
        delay: Duration,
        text: &'static str,
    }

    /// Streams `chunks` with per-chunk delays. With `hang_after_chunks` the
    /// producer holds the stream open forever after the last chunk: the
    /// connection neither progresses nor terminates, exactly like a hung
    /// upstream.
    struct TimedStreamLlm {
        chunks: Vec<Chunk>,
        hang_after_chunks: bool,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for TimedStreamLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, producer) = cog_core::EventStream::with_capacity(16);
            let chunks: Vec<(Duration, String)> = self
                .chunks
                .iter()
                .map(|c| (c.delay, c.text.to_string()))
                .collect();
            let hang = self.hang_after_chunks;
            tokio::spawn(async move {
                let mut producer = producer;
                let mut text = String::new();
                for (delay, delta) in chunks {
                    tokio::time::sleep(delay).await;
                    text.push_str(&delta);
                    let _ = producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta,
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                }
                if hang {
                    // Hold the producer (and thus the stream) open forever.
                    std::future::pending::<()>().await;
                }
                producer.end(cog_core::ChatResponse {
                    content: vec![ContentBlock::Text {
                        text,
                        text_signature: None,
                    }],
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    response_id: None,
                    usage: cog_core::Usage::default(),
                    stop_reason: cog_core::StopReason::Stop,
                    error_message: None,
                    upstream_failure: None,
                    retry_after_secs: None,
                    timestamp: chrono::Utc::now(),
                });
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn chat(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse {
                content: vec![ContentBlock::Text {
                    text: r#"{"result":"reformatted"}"#.into(),
                    text_signature: None,
                }],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn stall_test_runtime(agent_id: &str) -> AgentRuntime {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: agent_id.into(),
            max_iterations: 1,
            think_stall_timeout_secs: 1,
            ..Default::default()
        };
        AgentRuntime::new(config, tx)
    }

    #[tokio::test]
    async fn think_stream_stall_timeout_aborts_hung_stream() {
        let llm = TimedStreamLlm {
            chunks: vec![Chunk {
                delay: Duration::from_millis(50),
                text: "partial",
            }],
            hang_after_chunks: true,
        };
        let mut runtime = stall_test_runtime("stall-hang");
        let started = std::time::Instant::now();
        let err = runtime
            .run(serde_json::json!({"task": "hang"}), &llm)
            .await
            .expect_err("hung stream must abort with a stall timeout");
        let msg = err.to_string();
        assert!(
            msg.contains("timed out") && msg.contains("no LLM stream progress"),
            "error should describe the stall timeout, got: {msg}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "stall window should fire promptly, took {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn think_stream_slow_but_live_stream_is_not_aborted() {
        // Every chunk arrives well within the 1s stall window, but the total
        // turn (6 × 400ms ≈ 2.4s) far exceeds it. A whole-turn wall-clock cap
        // would kill this stream mid-generation; stall detection must not.
        // The fragments assemble into a valid JSON object so run() completes
        // without the reformat fallback.
        let llm = TimedStreamLlm {
            chunks: [r#"{""#, r#""resu"#, r#"lt":""#, r#""o"#, r#"k""#, "}"]
                .iter()
                .map(|text| Chunk {
                    delay: Duration::from_millis(400),
                    text,
                })
                .collect(),
            hang_after_chunks: false,
        };
        let mut runtime = stall_test_runtime("stall-slow-alive");
        let started = std::time::Instant::now();
        runtime
            .run(serde_json::json!({"task": "slow"}), &llm)
            .await
            .expect("live stream slower than the stall window must still complete");
        assert!(
            started.elapsed() >= Duration::from_millis(2000),
            "test should actually outlast the stall window, took {:?}",
            started.elapsed()
        );
    }

    // ─── Per-task token census ───

    /// Keeps the census rows written to it. The read methods are only there to
    /// satisfy the contract — this path never reads, and stubbing them as
    /// `unimplemented!()` keeps that a fact rather than an assumption.
    #[derive(Default)]
    struct CensusSpy {
        rows: std::sync::Mutex<Vec<cog_core::TaskMetrics>>,
    }

    impl CensusSpy {
        fn rows(&self) -> Vec<cog_core::TaskMetrics> {
            self.rows.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::ObservabilityGateway for CensusSpy {
        async fn subscribe_events(
            &self,
            _filter: cog_core::EventFilter,
        ) -> SFResult<cog_core::observability::AgentEventStream> {
            unimplemented!("the census path never reads")
        }

        async fn get_agent_state(&self, _agent_id: &str) -> SFResult<cog_core::AgentState> {
            unimplemented!("the census path never reads")
        }

        async fn get_task_checkpoint(
            &self,
            _task_id: &str,
        ) -> SFResult<Option<cog_core::TaskCheckpoint>> {
            unimplemented!("the census path never reads")
        }

        async fn get_task_metrics(&self, _task_id: &str) -> SFResult<cog_core::TaskMetrics> {
            unimplemented!("the census path never reads")
        }

        async fn record_task_metrics(&self, metrics: cog_core::TaskMetrics) -> SFResult<()> {
            self.rows.lock().unwrap().push(metrics);
            Ok(())
        }

        async fn get_task_logs(
            &self,
            _task_id: &str,
            _limit: usize,
        ) -> SFResult<Vec<cog_core::LogEntry>> {
            unimplemented!("the census path never reads")
        }

        async fn get_snapshot_url(&self, _snapshot_id: &str) -> SFResult<String> {
            unimplemented!("the census path never reads")
        }

        async fn get_raw_log_index(
            &self,
            _stream: &str,
            _date: chrono::NaiveDate,
        ) -> SFResult<Vec<cog_core::RawLogIndex>> {
            unimplemented!("the census path never reads")
        }

        async fn get_cluster_overview(&self) -> SFResult<cog_core::ClusterOverview> {
            unimplemented!("the census path never reads")
        }

        async fn get_squad_state(&self, _squad_id: &str) -> SFResult<cog_core::SquadState> {
            unimplemented!("the census path never reads")
        }

        fn publish_event(&self, _event: AgentEvent) {}
    }

    fn usage(input: u32, output: u32, total_tokens: u32) -> cog_core::Usage {
        cog_core::Usage {
            input,
            output,
            total_tokens,
            ..Default::default()
        }
    }

    /// Answers every call with the same fixed usage figure. `text` is the
    /// streamed draft: text that is not JSON sends the run through the
    /// deliverable-reformat call, so both billed calls can be observed.
    struct CensusLlm {
        text: &'static str,
        stream_usage: cog_core::Usage,
        chat_usage: cog_core::Usage,
        error: bool,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for CensusLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            let text = self.text.to_string();
            let stream_usage = self.stream_usage.clone();
            let error = self.error;
            tokio::spawn(async move {
                let mut producer = producer;
                if error {
                    let _ = producer
                        .push(AssistantMessageEvent::Error {
                            reason: cog_core::StopReason::Error,
                            error: Message::assistant_text("upstream said no"),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                    producer.end(cog_core::ChatResponse::default());
                    return;
                }
                let _ = producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: text.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    content: vec![ContentBlock::Text {
                        text,
                        text_signature: None,
                    }],
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    response_id: None,
                    usage: stream_usage,
                    stop_reason: cog_core::StopReason::Stop,
                    error_message: None,
                    upstream_failure: None,
                    retry_after_secs: None,
                    timestamp: chrono::Utc::now(),
                });
            });
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn chat(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse {
                content: vec![ContentBlock::Text {
                    text: r#"{"result":"reformatted"}"#.into(),
                    text_signature: None,
                }],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: self.chat_usage.clone(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn census_runtime(agent_id: &str) -> AgentRuntime {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: agent_id.into(),
            max_iterations: 1,
            think_stall_timeout_secs: 5,
            ..Default::default()
        };
        AgentRuntime::new(config, tx)
    }

    /// Every billed call of a run lands in one row keyed by its task — both
    /// halves, the total, and the turn count.
    #[tokio::test]
    async fn a_finished_run_reports_its_token_census_against_its_task() {
        let spy = Arc::new(CensusSpy::default());
        let llm = CensusLlm {
            // Not JSON: the draft goes through the deliverable-reformat call,
            // so a second billed call lands in the same row.
            text: "the answer, in prose",
            stream_usage: usage(300, 200, 500),
            // No total reported, as some OpenAI-compatible upstreams do.
            chat_usage: usage(100, 50, 0),
            error: false,
        };
        let mut runtime = census_runtime("census-ok").with_observability(spy.clone());

        runtime
            .run_scoped(serde_json::json!({"task": "census"}), &llm, Some("task-1"))
            .await
            .expect("the run delivers");

        let rows = spy.rows();
        assert_eq!(rows.len(), 1, "exactly one row for one task");
        let row = &rows[0];
        assert_eq!(row.task_id, "task-1");
        assert_eq!(row.prompt_tokens, 400);
        assert_eq!(row.completion_tokens, 250);
        assert_eq!(
            row.total_tokens, 650,
            "a call that reports only its halves still contributes its spend"
        );
        assert_eq!(row.iterations, 1);
        assert_eq!(
            runtime.run_usage.llm_calls, 2,
            "the reformat fallback is a billed call and must be counted"
        );
    }

    /// A run that fails still reports what it spent before it failed: the runs
    /// worth investigating are the ones that burned tokens and produced
    /// nothing, and those are exactly the ones that end on an error.
    #[tokio::test]
    async fn a_failed_run_still_reports_its_census() {
        let spy = Arc::new(CensusSpy::default());
        let llm = CensusLlm {
            text: "",
            stream_usage: cog_core::Usage::default(),
            chat_usage: cog_core::Usage::default(),
            error: true,
        };
        let mut runtime = census_runtime("census-fail").with_observability(spy.clone());

        runtime
            .run_scoped(serde_json::json!({"task": "census"}), &llm, Some("task-2"))
            .await
            .expect_err("the stream error propagates");

        let rows = spy.rows();
        assert_eq!(rows.len(), 1, "a failed run is not a missing run");
        assert_eq!(rows[0].task_id, "task-2");
        assert_eq!(rows[0].iterations, 1);
    }

    /// An unscoped run has no task to key a row under, and inventing one would
    /// put a row in the per-task table that belongs to no task.
    #[tokio::test]
    async fn an_unscoped_run_writes_no_census_row() {
        let spy = Arc::new(CensusSpy::default());
        let llm = CensusLlm {
            text: r#"{"result":"ok"}"#,
            stream_usage: usage(10, 10, 20),
            chat_usage: cog_core::Usage::default(),
            error: false,
        };
        let mut runtime = census_runtime("census-unscoped").with_observability(spy.clone());

        runtime
            .run(serde_json::json!({"task": "census"}), &llm)
            .await
            .expect("the run delivers");

        assert!(
            spy.rows().is_empty(),
            "a run with no task id must not write a task row"
        );
    }

    /// The fold itself: the total never falls below its own parts, whichever
    /// half the upstream omitted.
    #[test]
    fn the_census_total_never_falls_below_its_halves() {
        let mut run_usage = RunUsage::default();
        // Total only.
        run_usage.add(&usage(0, 0, 1000));
        assert_eq!(run_usage.total_tokens, 1000);
        // Halves only: the total is derived, not left at zero beside them.
        run_usage.add(&usage(300, 200, 0));
        assert_eq!(run_usage.total_tokens, 1500);
        // A total smaller than its own halves is not allowed to shrink the sum.
        run_usage.add(&usage(100, 100, 50));
        assert_eq!(run_usage.total_tokens, 1700);
        assert_eq!(run_usage.prompt_tokens, 400);
        assert_eq!(run_usage.completion_tokens, 300);
        assert_eq!(run_usage.llm_calls, 3);
    }
}
