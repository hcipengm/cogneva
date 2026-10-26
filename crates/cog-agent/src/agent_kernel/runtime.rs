use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, ToolCall,
    ToolDefinition,
};
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::context::{truncate_to_chars, ContextWindow};
use crate::hooks::LifecycleHookEvent;
use crate::tools::{ToolRegistry, ToolRunError};

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

/// What a guardrail refusal does to the run that met it.
///
/// A refusal is a decision, not a failure: the rule that refused the call has
/// not moved, so the identical call cannot come out differently. Asking the
/// model again is therefore paid for out of the same context to learn the same
/// answer — unless the model changes the call, which is the one thing another
/// turn can produce.
///
/// `OneAdjustment` takes the recovery reading: one turn is bought for the model
/// to route around the rule (a different tool, different arguments), which is
/// the only thing another turn can produce that the refusal did not already
/// answer. `Stop` takes the zero-token reading and leaves the re-dispatch to
/// the caller.
///
/// The adjustment is counted per *run*, not per call. Counting per call would
/// let a model that keeps proposing a different blocked call — `a`, then `b`,
/// then `c` — buy a turn each and end the run on the iteration ceiling, which
/// is the same unbounded spend the refusal was supposed to stop. The first
/// refusal buys the turn; the next refusal of anything ends it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RefusalPolicy {
    /// End the run at the first refusal. No further model call is made.
    Stop,
    /// Buy one turn to propose a different call; any refusal after it — the
    /// same call or another one — ends the run.
    #[default]
    OneAdjustment,
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
    /// What a guardrail refusal does to the run. One adjustment turn by
    /// default: the run does not pay to learn the same answer twice, but it
    /// also does not abandon work a different call could still finish.
    refusal_policy: RefusalPolicy,
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
    /// Where an oversized tool output is written back so its tail stays
    /// reachable by reference. Optional like the other injected handles — the
    /// run completes without it — but its absence is not the same as a result
    /// that fit: with no archiver a truncated output is lost, and the handle is
    /// what carries the reading that says so.
    tool_output_archive: Option<Arc<dyn crate::archive::ToolOutputArchive>>,
    /// What the run in flight has been billed for so far. Reset by every
    /// [`Self::run_scoped`]; read when the run ends.
    run_usage: RunUsage,
    /// Turns the run in flight has actually started.
    run_iterations: u32,
    /// How the run in flight ended. Reset by every [`Self::run_scoped`] and set
    /// by [`Self::note_run_ending`], so the run's single exit can report the
    /// skill the role was bound to with the same verdict the observable counted
    /// — the ending is decided at three different places, and only the exit is
    /// common to all of them.
    run_outcome: Option<crate::observable::RunOutcome>,
    /// Whether this run has already reported a prompt that did not fit the
    /// configured context window.
    ///
    /// One alert per run: a condition that repeats on every call would turn a
    /// signal into a hum, and the reader would stop seeing the first one — which
    /// is the only one that says when it started.
    context_overrun_reported: bool,
    /// Identity of the run in flight, declared on every LLM call it makes.
    ///
    /// The same id the run reports its own token census under: without it the
    /// spend of one run is only readable as "this component, this environment",
    /// and "what did this task cost" cannot be answered from the ledger at all.
    /// Held on the runtime rather than threaded through every call site because
    /// it is a property of the run, exactly like the usage totals beside it.
    run_task_id: Option<String>,
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

/// What the model is told when the reply it just wrote is refused.
///
/// The instruction is the one a refusal buys: the same deliverable, rewritten
/// without the shape. Naming the shape is what makes the second attempt
/// different from the first — a model shown only "refused" tends to restate
/// the same reply.
const REPLY_REFUSED_INSTRUCTION: &str = "The reply you just wrote cannot be delivered: it \
     carries a shape the platform's own gate refuses. Rewrite the reply now with the same \
     deliverable — the same fields and the same content — without that shape. Do not mention \
     this instruction, the rule, or the refusal in the reply.";

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
            refusal_policy: RefusalPolicy::default(),
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
            tool_output_archive: None,
            run_usage: RunUsage::default(),
            run_task_id: None,
            run_iterations: 0,
            run_outcome: None,
            context_overrun_reported: false,
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

    /// Hand the loop the surface that archives an oversized tool output.
    ///
    /// The loop uses it at the tool-return point, where both the full output
    /// and the window budget are in hand: an output that does not fit is
    /// written back there and its truncation marker carries the reference, so
    /// the tail the model never saw can still be fetched.
    pub fn with_tool_output_archive(
        mut self,
        archive: Arc<dyn crate::archive::ToolOutputArchive>,
    ) -> Self {
        self.tool_output_archive = Some(archive);
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
    /// Choose what a guardrail refusal does to the run. See [`RefusalPolicy`];
    /// the default stops at the first refusal, because another turn spent on a
    /// decision that cannot change is exactly the spend this bounds.
    pub fn with_refusal_policy(mut self, policy: RefusalPolicy) -> Self {
        self.refusal_policy = policy;
        self
    }

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

    /// 一次 run 结束时随 `AgentEnd` 发布的消息：本窗口见过的全部，而不是
    /// 这一轮预算允许留下的那部分。
    ///
    /// 发布点有三处（正常收尾、迭代预算耗尽、别的提前结束路径），都从这里取。
    /// 让三处各写一遍 `.messages().to_vec()` 的话，「有的 run 记全、有的 run
    /// 只记被裁过的」这种分叉要靠人去比对三行才看得出来；这里只有一个定义，
    /// 加发布点的人也只会看到这一个名字。
    fn published_messages(&self) -> Vec<Message> {
        self.context.history().to_vec()
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
            let event = match wal_record_to_agent_event(&record) {
                Ok(event) => event,
                Err(e) => {
                    // 读不回来的记录过去被 `if let Ok` 悄悄跳过：恢复以后少了一段历史，
                    // 而盘上和日志里都没有痕迹说它丢过。留一行带序号与身份的日志，
                    // 「重放比 WAL 短」这件事才可归因。
                    tracing::warn!(
                        session = %record.session_id,
                        seq = record.seq,
                        error = %e,
                        "WAL replay skipped an undecodable record"
                    );
                    continue;
                }
            };
            // 重放事件直发 broadcast（本地状态重建），不进运行时 mpsc：
            // mpsc 下游的总线投递只认活体流量，重放再进总线会被摄取侧
            // 当成新事件重复建档。无订阅者时 send 报错属正常，不算失败。
            let _ = sink.send(event);
            count += 1;
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
        let options = self.stamp_task(options);

        llm.chat_stream(&messages, &options).await
    }

    /// 这笔花销该记在谁头上：所属的 DAG 任务优先，其次是这次运行的 id。
    ///
    /// 两个身份回答两个问题。运行 id 答「这一次跑了什么」（PGE 每轮一个新
    /// id，工具产物归档与沙箱工作目录按它命名）；所属任务答「这些 token 是
    /// 哪件活花的」——挣值的实际成本、预算闸、按任务的历史中位数都按后一个
    /// 查。2026-10-26 实测：账本里 13.7 万条带 id 的用量全是 `ralph-pipeline-*`，
    /// 与任务图 3831 个 id 交集为 0，于是成本恒读作 0、预算闸永远「读不到」
    /// 而放行。所以账与清点记所属任务，运行 id 只留在这条运行自己的地方。
    ///
    /// 不属于任何任务的 agent（常驻角色、临时评测）没有所属任务，按运行 id
    /// 记；两样都没有的调用不编一个身份，编出来的会读成一件真花过钱的任务。
    fn ledger_task_id<'a>(&'a self, run_task_id: Option<&'a str>) -> Option<&'a str> {
        self.config.owner_task_id.as_deref().or(run_task_id)
    }

    /// Put the run's identity on a call's headers, when there is one.
    ///
    /// The task identity is known only to whoever started the run; the
    /// transport cannot derive it, and a table mapping components to tasks
    /// would be wrong the moment one component serves two. A run started
    /// without an identity makes unlabelled calls rather than a made-up one,
    /// because a made-up id reads as a real job on the ledger.
    fn stamp_task(&self, options: cog_core::ChatOptions) -> cog_core::ChatOptions {
        match self.ledger_task_id(self.run_task_id.as_deref()) {
            Some(task) => options.with_task(task),
            None => options,
        }
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
        // 换了一次运行就是一个新的额度：上一轮报过的那次超大 prompt，不能替这一轮
        // 把嘴堵上，否则第二轮同样的问题会在任何地方都读不到。
        self.context_overrun_reported = false;
        self.run_outcome = None;
        // Set before the first call and cleared at the only exit, so a run that
        // was started without an identity cannot inherit the previous one.
        self.run_task_id = run_task_id.map(str::to_string);
        let started = std::time::Instant::now();
        let outcome = self.run_turns(input, llm, run_task_id).await;
        self.report_run_census(run_task_id, started.elapsed()).await;
        self.report_skill_outcome(started.elapsed()).await;
        self.run_task_id = None;
        outcome
    }

    /// Record this run's ending where both of its readers look for it.
    ///
    /// The per-role observable counts the ending on the spot, at each of the
    /// three places a run can stop; the skill report reads it from the exit,
    /// which is the only place common to all three. Writing both here keeps the
    /// two from disagreeing about what this run did.
    fn note_run_ending(&mut self, outcome: crate::observable::RunOutcome, iterations: u32) {
        let steps = self.steps.len();
        let tool_calls = self.steps.iter().map(|s| s.tool_calls.len()).sum();
        crate::observable::global_observable().record_run(
            &self.config.role,
            outcome,
            iterations,
            steps,
            tool_calls,
        );
        self.run_outcome = Some(outcome);
    }

    /// End the run because the reply it was about to send was refused.
    ///
    /// The refusal names what the answer did, not which tool it called: the
    /// message keeps the shape the tool refusal uses, so the caller that reads
    /// refusals out of an outcome reads this one the same way. It is counted as
    /// a refusal rather than a delivery, because nothing was delivered.
    fn reply_refusal(&mut self, rule: String, reason: String, iterations: u32) -> SFError {
        let err = SFError::Blocked {
            tool: "final_reply".to_string(),
            reason,
            rule,
        };
        tracing::warn!("{err}");
        self.note_run_ending(crate::observable::RunOutcome::Refused, iterations);
        err
    }

    /// Report this run's ending against the skill its role was bound to.
    ///
    /// The effectiveness tracker aggregates per skill, and nothing ever fed it
    /// one: the only outcomes recorded before this came from the squad and the
    /// change pipeline, which name themselves (`squad_execution`,
    /// `change_deployment`) — ids no skill registry holds. The binding and the
    /// ending are both only here: the role's skill arrives in the config, and
    /// the run's exit is the one place every ending reaches.
    ///
    /// A run that returned an error reports nothing. An upstream failure is no
    /// evidence about the skill, and the ending has no error case, so scoring
    /// it as a failure would charge the skill for someone else's outage.
    ///
    /// Best-effort: nothing about an unrecorded outcome may fail a run that
    /// already delivered.
    async fn report_skill_outcome(&self, elapsed: std::time::Duration) {
        let (Some(skill), Some(reflection), Some(ending)) = (
            self.config.skill_config.as_ref(),
            self.reflection_engine.as_ref(),
            self.run_outcome,
        ) else {
            return;
        };
        // A refusal is the rule's verdict on a call, not evidence about the
        // skill: the work may have been right and the tool simply not allowed
        // in this scope. Scoring it would charge the skill for someone else's
        // rule, the same way an upstream outage would.
        if ending == crate::observable::RunOutcome::Refused {
            return;
        }
        let delivered = ending.delivered();
        let outcome = cog_core::SkillOutcome {
            skill_id: skill.skill_id.clone(),
            task_signature: self.config.role.clone(),
            success: delivered,
            // The run carries no quality verdict of its own, so the score
            // encodes the ending — 1.0/0.0, the same thing every other caller
            // that has only the ending writes.
            score: Some(if delivered { 1.0 } else { 0.0 }),
            latency_ms: elapsed.as_millis() as u64,
            token_cost: self.run_usage.total_tokens,
            observed_at: chrono::Utc::now(),
        };
        if let Err(e) = reflection.process_skill_outcome(outcome).await {
            tracing::warn!(
                skill_id = %skill.skill_id,
                error = %e,
                "this run's skill outcome was not recorded"
            );
        }
    }

    /// Write this run's token census where the per-task readers look for it.
    ///
    /// Best-effort on purpose: a metrics write must not turn a delivered task
    /// into a failed one. The loss is named rather than swallowed, because a
    /// missing row is indistinguishable from a task that never ran.
    async fn report_run_census(&self, run_task_id: Option<&str>, elapsed: std::time::Duration) {
        // 与账本同一个身份（[`Self::ledger_task_id`]）：清点表按任务累加，读者的
        // 问题是「这件活一共花了多少」，与它分几轮跑无关。记在运行 id 上会让
        // 按任务查询一条也读不到，而那读起来和「这件活没花过钱」一模一样。
        let Some(task_id) = self.ledger_task_id(run_task_id) else {
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

    /// The tool-result message for one call, archiving the output when it will
    /// not fit the window.
    ///
    /// This is the only place that holds both the full text and the window
    /// budget, so it is the only place that can decide a result is oversized and
    /// still save the part the model will not see. A run with no archive surface
    /// (embedded use, tests) still cuts the output — it just cannot offer a
    /// reference to the tail, and the marker says only that bytes were dropped.
    async fn tool_result_message(
        &self,
        task_id: Option<&str>,
        turn: u32,
        frame: u32,
        tool_call_id: &str,
        tool_name: &str,
        text: &str,
    ) -> Message {
        let budget = self.context.tool_result_budget_chars();
        if text.chars().count() <= budget {
            return Message::tool_result_text(tool_call_id, tool_name, text);
        }
        // Oversized: the tail is about to be cut, and those bytes exist nowhere
        // else. Write the full output back where the read side can fetch it and
        // name it in the marker, so the loss is addressable rather than silent.
        //
        // The archive is attempted before the cut is made, because the reference
        // it returns changes the marker's length and the marker is charged to the
        // same budget: a cut made without it would keep characters the model will
        // not actually be shown. So the cut has to be made once, with whatever
        // reference came back, and counted from that one result -- two calls
        // would report a size the text does not carry.
        let reference = match &self.tool_output_archive {
            Some(archive) => match task_id {
                Some(task_id) => {
                    let id = cog_core::tool_output_raw_id(task_id, turn, frame);
                    match archive
                        .archive(tool_name, &id, "application/json", text)
                        .await
                    {
                        Ok(uri) => Some(uri),
                        Err(e) => {
                            tracing::warn!(
                                tool = tool_name,
                                id = %id,
                                error = %e,
                                "tool output does not fit the window and could not be archived; \
                                 the marker carries no reference"
                            );
                            None
                        }
                    }
                }
                // No task id to key a source by: nothing to fetch it back
                // under, so the marker stays reference-free. That is a fate of
                // this cut, not a reason to pass over it -- nothing counts the
                // tail as lost otherwise, and it is exactly as lost as one whose
                // write was refused.
                None => {
                    tracing::warn!(
                        tool = tool_name,
                        "tool output does not fit the window and the run carries no task id \
                         to archive it under; the marker carries no reference"
                    );
                    archive.record_unkeyed(tool_name).await;
                    None
                }
            },
            None => None,
        };
        let cut = truncate_to_chars(text, budget, reference.as_deref());
        // Counted after the archive is attempted, so the size is the size of the
        // text actually handed over. Every path above reaches here -- a failed
        // archive and an unkeyed run included -- so a run whose archive never
        // works is still a run that truncates, and its cuts are still counted.
        if let Some(archive) = &self.tool_output_archive {
            archive
                .record_truncation(tool_name, cut.dropped_chars)
                .await;
        }
        Message::tool_result_text(tool_call_id, tool_name, cut.text)
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

        // The stable half of the input document (the answer contract) is hoisted
        // to the front on its own. The whole document serializes in JSON key
        // order, and the fields that change every attempt (`context.attempt` and
        // friends) are exactly the ones that sort first — flattened together, the
        // first byte already differs and the upstream prefix cache loses the
        // entire prompt, while this contract is byte-identical throughout. See
        // `cog_core::contract::prompt`.
        let (contract, payload) = cog_core::contract::prompt::split_contract(input);
        if let Some(contract) = contract {
            // One agent is reused by the same role across turns (one turn per
            // attempt). A contract already in this position is not inserted a
            // second time: the same text appearing mid-conversation is paid for
            // twice and pushes the stable prefix off the head.
            if !self.context.starts_with_system(&contract) {
                self.context.prepend_message(Message::system(contract));
            }
        }
        // The varying half is rendered request-first, so the attempt counter
        // that used to open it does not sit in front of the one block every
        // attempt of a task shares. See `render_varying_half`.
        self.context.add_message(Message::user(
            cog_core::contract::prompt::render_varying_half(&payload),
        ));

        // Guardrail refusals, counted per identical call for this run. A refusal
        // is a decision about the call as written: the same name and the same
        // arguments will be refused again, so a second one is not a retry that
        // might work, it is the loop paying full price for an answer it already
        // has. Refusals of a *different* call are untouched -- changing the
        // approach is exactly what the first refusal asks for.
        // Refusals are counted for the whole run, not per call: the bound is
        // "how many turns may a refusal buy", and a model cycling through a
        // different blocked call each turn would otherwise buy one per turn.
        let mut refusals: u32 = 0;
        // Every call a rule refused in this run, in the order it happened. The
        // ending can name only one of them (the last), but the attempt this run
        // hands the work back to has never seen any of them — it starts with a
        // fresh context — so the earlier ones travel with the last one.
        let mut refused_calls: Vec<(String, String, String)> = Vec::new();

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
                    .deliver(
                        llm,
                        thought_text,
                        assistant_msg,
                        iteration + 1,
                        crate::observable::RunOutcome::Delivered,
                    )
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
                // 撞墙在这里就成立了，与这一轮最后交出什么都没关系。等终局再记，
                // 抢救回来的那几轮会读成从没撞过墙。
                crate::observable::global_observable().note_budget_exhausted(&self.config.role);
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
                // 这一问是整轮里唯一在预算花完之后发出的请求，所以必须自己有界：
                // 停顿超时只挡「不吭声」，一个一直吐字的流可以在它里面待到很久。
                let draft_timeout =
                    Duration::from_secs(self.config.final_draft_timeout_secs.max(1));
                let forced = match tokio::time::timeout(draft_timeout, self.think_stream_final(llm))
                    .await
                {
                    Ok(Ok(msg)) => {
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
                    Ok(Err(e)) => {
                        tracing::warn!(
                            agent_id = %self.config.agent_id,
                            error = %e,
                            "the final-draft call failed; the run still ends on its spent budget"
                        );
                        (None, "unavailable")
                    }
                    // 到点了还没给完。这一问是「最后再问一次」，不是「想聊多久都行」：
                    // 没有这条，一个卡住的上游能把一个已经花光预算的轮次再攥住十分钟。
                    Err(_) => {
                        tracing::warn!(
                            agent_id = %self.config.agent_id,
                            final_draft_timeout_secs = draft_timeout.as_secs(),
                            "the final-draft call ran past its bound; the run still ends on its spent budget"
                        );
                        (None, "timeout")
                    }
                };
                if let Some((draft, msg)) = forced.0 {
                    tracing::info!(
                        agent_id = %self.config.agent_id,
                        "the final draft recovered an answer the loop's budget had no room for"
                    );
                    return self
                        .deliver(
                            llm,
                            draft,
                            msg,
                            self.config.max_iterations + 1,
                            crate::observable::RunOutcome::DeliveredAfterExhaustion,
                        )
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
                    messages: self.published_messages(),
                    crew_id: None,
                    squad_id: None,
                    timestamp: chrono::Utc::now(),
                })
                .await?;

                self.note_run_ending(
                    crate::observable::RunOutcome::BudgetExhausted,
                    iteration + 1,
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

            for (frame, tc) in tool_calls.iter().enumerate() {
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
                // The tool is shared by every agent in this process, so the
                // handler cannot tell whose call this is unless the dispatcher
                // says so. It is named around the await rather than passed in:
                // the handler signature is the same for every native tool, and
                // only the addressing tools need a caller.
                let checked = crate::tools::CALLER_AGENT_ID
                    .scope(
                        self.config.agent_id.clone(),
                        self.tools.execute_scoped_checked(
                            &tc.name,
                            tc.arguments.clone(),
                            scope.as_ref(),
                        ),
                    )
                    .await;

                // A guardrail refusal is not a tool failure. A failed tool might
                // succeed on a retry; a refusal cannot, because the rule that
                // refused it is still there. Handing it back as an ordinary
                // error invites the loop to retry the identical call until the
                // iteration budget runs out, which is the most expensive way to
                // learn nothing. The observation says what a further turn would
                // buy — a different call — and the policy caps how many of
                // those turns the run pays for: none under `Stop`, one under
                // `OneAdjustment`.
                let mut blocked: Option<(String, String, String, u32)> = None;
                let result: Result<serde_json::Value, SFError> = match checked {
                    Ok(val) => Ok(val),
                    Err(ToolRunError::Blocked { tool, reason, rule }) => {
                        refusals += 1;
                        blocked = Some((tool.clone(), reason.clone(), rule.clone(), refusals));
                        refused_calls.push((tool.clone(), rule.clone(), reason.clone()));
                        Err(SFError::Blocked { tool, reason, rule })
                    }
                    Err(ToolRunError::Failed(e)) => Err(e),
                };

                self.state = RuntimeState::Observing;
                let is_error = result.is_err();
                let observation = match (&result, &blocked) {
                    (Ok(val), _) => val.clone(),
                    (Err(e), Some((_tool, _reason, rule, count))) => {
                        let ends = match self.refusal_policy {
                            RefusalPolicy::Stop => true,
                            RefusalPolicy::OneAdjustment => *count > 1,
                        };
                        serde_json::json!({
                            "error": e.to_string(),
                            "guardrail_blocked": true,
                            "rule": rule,
                            "run_ending": ends,
                            // When the run ends here, this text is the last thing
                            // the model sees, so it has to say why rather than
                            // invite one more attempt.
                            "note": if ends {
                                "A guardrail rule refused this call and the run is ending. \
                                 Report the rule as the blocker instead of re-proposing it."
                            } else {
                                "A guardrail rule refused this call. The identical call \
                                 will be refused the same way; propose a different call or \
                                 report this rule as the blocker."
                            },
                        })
                    }
                    (Err(e), None) => serde_json::json!({ "error": e.to_string() }),
                };

                observations.push(observation.clone());
                tool_result_messages.push(
                    self.tool_result_message(
                        run_task_id,
                        iteration,
                        frame as u32,
                        &tc.id,
                        &tc.name,
                        &observation.to_string(),
                    )
                    .await,
                );

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

                if let Some((tool, reason, rule, count)) = blocked {
                    let ends = match self.refusal_policy {
                        RefusalPolicy::Stop => true,
                        RefusalPolicy::OneAdjustment => count > 1,
                    };
                    if ends {
                        let note = match self.refusal_policy {
                            RefusalPolicy::Stop => {
                                "the run ended at the first refusal instead of paying for \
                                 another turn"
                                    .to_string()
                            }
                            RefusalPolicy::OneAdjustment => {
                                "a second refusal arrived after the one adjustment turn \
                                 allowed, so the run ended"
                                    .to_string()
                            }
                        };
                        // The rule's own message rides along. Whoever re-dispatches
                        // this work reads the reason and plants it into the next
                        // attempt's input, and "the run ended after a second
                        // refusal" says nothing about why this call was refused —
                        // the only part the next attempt can act on.
                        //
                        // The calls this run refused *before* the last one ride
                        // too: the next attempt has a fresh context, so a rule it
                        // already met here would otherwise be news to it again.
                        let earlier: Vec<String> = refused_calls
                            .iter()
                            .take(refused_calls.len().saturating_sub(1))
                            .map(|(t, r, why)| format!("{t} (rule: {r}): {why}"))
                            .collect();
                        let also = if earlier.is_empty() {
                            String::new()
                        } else {
                            format!("; also refused in this run: {}", earlier.join("; "))
                        };
                        let err = SFError::Blocked {
                            tool,
                            reason: format!("{reason} ({note}{also})"),
                            rule,
                        };
                        tracing::warn!("{err}");
                        // Count the ending where the per-role reader looks for it.
                        // Ending on a refusal used to leave the run uncounted
                        // altogether, so the runs a guardrail stopped were visible
                        // only as scattered tool events.
                        self.note_run_ending(crate::observable::RunOutcome::Refused, iteration);
                        return Err(err);
                    }
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
        // 同一条：预算在这里用完，就在这里记一次。
        crate::observable::global_observable().note_budget_exhausted(&self.config.role);
        let result = serde_json::json!({
            "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
            "iterations": self.config.max_iterations
        });

        self.emit_event(AgentEvent::AgentEnd {
            agent_id: self.config.agent_id.clone(),
            messages: self.published_messages(),
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
        self.note_run_ending(
            crate::observable::RunOutcome::BudgetExhausted,
            self.config.max_iterations,
        );

        Ok(result)
    }

    /// Close the run on an answer: extract the deliverable from `draft`, record
    /// the turn, and report the run as delivered.
    ///
    /// Both the ordinary path (the loop stopped because the model had nothing
    /// left to call) and the forced path (the budget ended and the model was
    /// asked directly) end here, so the extraction and the event emission cannot
    /// drift apart between them. The accounting deliberately is not shared:
    /// `outcome` is how the caller says which of the two this is, because the
    /// one thing the two paths must not agree on is whether the budget was spent.
    async fn deliver(
        &mut self,
        llm: &dyn cog_core::LlmClient,
        draft: String,
        turn_message: Message,
        iterations: u32,
        outcome: crate::observable::RunOutcome,
    ) -> SFResult<serde_json::Value> {
        // The finished reply meets the same gate the tool calls meet. The host
        // refuses a turn at the moment it would otherwise be sent, and a
        // platform that judged only tool calls would let the last word through
        // unguarded. One rewrite is what the refusal buys, which is the same
        // ask the host's own gate makes; a second refusal ends the run as a
        // refusal rather than delivering a reply that gate would have stopped.
        let (draft, turn_message) = match self.tools.check_output(&draft).await {
            cog_core::GuardResult::Pass => (draft, turn_message),
            cog_core::GuardResult::Warn { reason, rule } => {
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    rule = %rule,
                    "the finished reply met a guardrail warning: {reason}"
                );
                (draft, turn_message)
            }
            cog_core::GuardResult::Block { reason, rule } => {
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    rule = %rule,
                    reason = %reason,
                    "the finished reply was refused; asking the model to rewrite it once"
                );
                self.context.add_message(turn_message);
                self.context
                    .add_message(Message::user(REPLY_REFUSED_INSTRUCTION));
                // The rewrite is one more request on this transcript, so it is
                // bounded the same way the final-draft ask is: a stall timeout
                // only catches silence, and a stream that keeps emitting can
                // otherwise hold a finished run open indefinitely.
                let timeout = Duration::from_secs(self.config.final_draft_timeout_secs.max(1));
                let rewritten = tokio::time::timeout(timeout, self.think_stream_final(llm)).await;
                let (text, message) = match rewritten {
                    Ok(Ok(message)) => (assistant_text(&message), message),
                    Ok(Err(e)) => {
                        return Err(self.reply_refusal(
                            rule,
                            format!("{reason} (the rewrite could not be obtained: {e})"),
                            iterations,
                        ));
                    }
                    Err(_) => {
                        return Err(self.reply_refusal(
                            rule,
                            format!(
                                "{reason} (the rewrite did not finish within {}s)",
                                timeout.as_secs()
                            ),
                            iterations,
                        ));
                    }
                };
                match self.tools.check_output(&text).await {
                    cog_core::GuardResult::Block {
                        reason: second,
                        rule: second_rule,
                    } => {
                        return Err(self.reply_refusal(
                            second_rule,
                            format!(
                                "{second} (the reply had already been refused once by {rule}: {reason})"
                            ),
                            iterations,
                        ));
                    }
                    _ => (text, message),
                }
            }
        };
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
            messages: self.published_messages(),
            crew_id: None,
            squad_id: None,
            timestamp: chrono::Utc::now(),
        })
        .await?;

        self.note_run_ending(outcome, iterations);

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
        let options = self.stamp_task(options);

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
        let mut stream_error: Option<String> = None;

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
                // The reply is over, but keep reading instead of returning from
                // this arm. This event holds only the wording of the failure;
                // the *type* of it -- which refusal the upstream gave, and how
                // long that upstream said to wait -- is on the stream's final
                // response, and this is the one place every agent call passes
                // through. Returning here is how a quota outage looks to every
                // consumer above like a transient blip worth re-buying on the
                // next tick, since a sentence is all they get to read. Reading
                // on is also what drains the producer, so `result()` resolves.
                stream_error = Some(error.content());
                continue;
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
        // only place the run's spend is still readable. A refused call is
        // billed too -- it consumed the tokens it consumed, and the error path
        // returns from here before anything else could count them.
        self.run_usage.add(&response.usage);
        self.note_context_overrun(response.usage.input).await;

        if let Some(error) = stream_error {
            // A provider that classified the refusal (HTTP status -> a typed
            // refusal, plus the wait the upstream stated in its headers) already
            // said what this failure is, and the type is what the layer above
            // decides with: a terminal refusal must not be re-bought, and a
            // stated wait must reach the delay rather than be re-derived. The
            // sentence is kept as the message's text either way, so the wording
            // readers already have does not change.
            return Err(match response.upstream_failure {
                Some(cause) => SFError::upstream_refused_after(
                    cause,
                    format!("LLM stream error: {error}"),
                    response.retry_after_secs,
                ),
                None => SFError::Agent(format!("LLM stream error: {error}")),
            });
        }

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
        let options = self.stamp_task(options);

        let reformat_timeout = Duration::from_secs(30);
        match tokio::time::timeout(reformat_timeout, llm.chat(&[user_msg], &options)).await {
            Ok(Ok(response)) => {
                // The reformat is a billed call like any other turn; leaving it
                // out would understate the runs that needed it, which are the
                // ones whose answer came out of a fallback.
                self.run_usage.add(&response.usage);
                self.note_context_overrun(response.usage.input).await;
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

    /// 报一次「这一问的 prompt 比配置的上下文窗口还大」。
    ///
    /// 上下文在每次调用前都会裁到 `context_window_size` 以内，所以一个超过窗口的
    /// prompt 说明裁剪没有兑现。这件事不报出来就是隐形的：调用照样成功、答案照样
    /// 回来，只有账单知道这次送出去的比配置允许的多。它也是告警面上唯一有生产者的
    /// 那一种读数——没有它，那一面接得再对也永远是空的。
    ///
    /// 一次运行只报一次。每调用一次都报的话，读者很快就会把它当背景噪声，而真正
    /// 有用的恰恰是第一次：它说明从什么时候开始超的。窗口配成 0 当作没配，不判。
    async fn note_context_overrun(&mut self, prompt_tokens: u32) {
        if self.context_overrun_reported {
            return;
        }
        let window = self.config.context_window_size as u64;
        let prompt = u64::from(prompt_tokens);
        if window == 0 || prompt <= window {
            return;
        }
        self.context_overrun_reported = true;
        let event = AgentEvent::ResourceAlert {
            agent_id: self.config.agent_id.clone(),
            metric: "context_window".into(),
            threshold: window as f64,
            current: prompt as f64,
            crew_id: self.config.crew_id.clone(),
            squad_id: self.config.squad_id.clone(),
            timestamp: chrono::Utc::now(),
        };
        if let Err(e) = self.emit_event(event).await {
            // 没发出去就不算报过：下一问再试。这里不算「已报」，否则总线一断，
            // 这一次超窗就永远没人知道，而它其实是这一整轮里唯一的那次。
            self.context_overrun_reported = false;
            tracing::warn!(
                agent_id = %self.config.agent_id,
                error = %e,
                "context overrun alert could not be emitted"
            );
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
        // 写盘那一侧过去把这七种事件写成 `Custom { name }`，而 `WalEventType` 里
        // 它们各有专用变体。盘上已经躺着这样的记录，重启恢复时按 `Custom` 一律放弃，
        // 于是报错、心跳、检查点、自审这几类在重放里静默消失（`replay_events` 用
        // `if let Ok` 跳过，连日志都没有）。这七个名字照旧认回来，老记录才读得回。
        WalEventType::Custom { name } => match name.as_str() {
            "self_review" => "self_review",
            "react_step_start" => "react_step_start",
            "react_step_end" => "react_step_end",
            "agent_error" => "agent_error",
            "resource_alert" => "resource_alert",
            "heartbeat" => "heartbeat",
            "checkpoint_saved" => "checkpoint_saved",
            _ => {
                return Err(serde::de::Error::custom(format!(
                    "cannot replay custom WAL event: {name}"
                )));
            }
        },
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
mod ledger_identity_tests {
    use super::*;

    /// 一次运行的身份与它归属的任务是两件事，账要记在后者上。
    ///
    /// 2026-10-26 的现场：账本里 13.7 万条带 id 的用量全是 `ralph-pipeline-*`，
    /// 与任务图 3831 个 id 交集为 0，于是挣值的实际成本恒为 0、计划预算那道闸
    /// 永远「读不到花销」而放行。这条用例把两个身份的优先级钉在这里：有归属
    /// 就记归属，没有才退回运行 id，两样都没有就不编。
    #[test]
    fn a_run_belongs_to_its_task_and_the_ledger_records_the_task() {
        let config = RuntimeConfig {
            owner_task_id: Some("change-ledger-abc".into()),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let runtime = AgentRuntime::new(config, tx);
        let options = runtime.stamp_task(cog_core::ChatOptions::default());
        assert_eq!(
            options
                .headers
                .get(cog_core::LLM_TASK_HEADER)
                .map(String::as_str),
            Some("change-ledger-abc"),
            "账本上的任务身份必须是那件 DAG 任务，不是这一次运行的 id"
        );
    }

    /// 不属于任何任务的 agent（常驻角色、临时评测）按运行 id 记。
    #[test]
    fn a_run_without_an_owning_task_falls_back_to_its_run_id() {
        let (tx, _rx) = mpsc::channel(1);
        let mut runtime = AgentRuntime::new(RuntimeConfig::default(), tx);
        runtime.run_task_id = Some("ralph-pipeline-xyz".into());
        let options = runtime.stamp_task(cog_core::ChatOptions::default());
        assert_eq!(
            options
                .headers
                .get(cog_core::LLM_TASK_HEADER)
                .map(String::as_str),
            Some("ralph-pipeline-xyz")
        );
    }

    /// 两样都没有就不写身份头：编一个 id 会读成一件真花过钱的任务。
    #[test]
    fn a_run_with_no_identity_at_all_stamps_nothing() {
        let (tx, _rx) = mpsc::channel(1);
        let runtime = AgentRuntime::new(RuntimeConfig::default(), tx);
        let options = runtime.stamp_task(cog_core::ChatOptions::default());
        assert!(!options.headers.contains_key(cog_core::LLM_TASK_HEADER));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::SkillConfig;
    use serde_json::json;

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

    /// 每一种写进 WAL 的事件，都必须能原样读回来。
    ///
    /// 这里盯的是**写盘用了哪种记录类型**，不是读盘能不能兜住：过去七种事件被写成
    /// `Custom { name }`，而 `WalEventType` 里它们各有专用变体；重放那条路对认不出的
    /// 记录是 `if let Ok(...)`，于是那些记录重启后既不重放也不报错，盘上没有任何
    /// 痕迹说它们丢过。七种改成专用变体之后，同一个形状再出现一次（新事件接着写
    /// `Custom`）就靠这一条红。
    #[tokio::test]
    async fn every_written_event_reads_back() {
        let now = chrono::Utc::now();
        let agent = "agent-1".to_string();
        let cases = vec![
            AgentEvent::AgentStart {
                agent_id: agent.clone(),
                crew_id: Some("crew-1".into()),
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::AgentEnd {
                agent_id: agent.clone(),
                messages: vec![Message::user("done")],
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::TurnStart {
                agent_id: agent.clone(),
                timestamp: now,
            },
            AgentEvent::TurnEnd {
                agent_id: agent.clone(),
                message: Message::user("turn done"),
                tool_results: Vec::new(),
                timestamp: now,
            },
            AgentEvent::MessageStart {
                agent_id: agent.clone(),
                message: Message::user("hi"),
                timestamp: now,
            },
            AgentEvent::MessageUpdate {
                agent_id: agent.clone(),
                assistant_event: AssistantMessageEvent::TextDelta {
                    content_index: 0,
                    delta: "partial".into(),
                    timestamp: now,
                },
                timestamp: now,
            },
            AgentEvent::MessageEnd {
                agent_id: agent.clone(),
                message: Message::user("hi"),
                timestamp: now,
            },
            AgentEvent::ToolExecutionStart {
                agent_id: agent.clone(),
                tool_call_id: "call-1".into(),
                tool_name: "read_file".into(),
                args: json!({ "path": "/tmp/x" }),
                timestamp: now,
            },
            AgentEvent::ToolExecutionUpdate {
                agent_id: agent.clone(),
                tool_call_id: "call-1".into(),
                partial_result: json!({ "bytes": 12 }),
                timestamp: now,
            },
            AgentEvent::ToolExecutionEnd {
                agent_id: agent.clone(),
                tool_call_id: "call-1".into(),
                result: json!({ "ok": true }),
                is_error: false,
                timestamp: now,
            },
            AgentEvent::StateChange {
                agent_id: agent.clone(),
                from: "idle".into(),
                to: "thinking".into(),
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::TaskStatusChange {
                task_id: "task-1".into(),
                status: "running".into(),
                agent_id: Some(agent.clone()),
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::SelfReview {
                agent_id: agent.clone(),
                status: "PASS".into(),
                score: 0.9,
                summary: Some("looks right".into()),
                critique: None,
                suggestions: Some(vec!["none".into()]),
                timestamp: now,
            },
            AgentEvent::ReActStepStart {
                agent_id: agent.clone(),
                iteration: 2,
                timestamp: now,
            },
            AgentEvent::ReActStepEnd {
                agent_id: agent.clone(),
                iteration: 2,
                thought: "read the file".into(),
                tool_calls: vec![ToolCall {
                    id: "call-1".into(),
                    name: "read_file".into(),
                    arguments: json!({ "path": "/tmp/x" }),
                }],
                observations: vec![json!({ "ok": true })],
                timestamp: now,
            },
            AgentEvent::AgentError {
                agent_id: agent.clone(),
                error_code: "llm_upstream".into(),
                severity: cog_core::ErrorSeverity::Critical,
                details: "429".into(),
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::ResourceAlert {
                agent_id: agent.clone(),
                metric: "cpu".into(),
                threshold: 0.9,
                current: 0.95,
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
            AgentEvent::Heartbeat {
                agent_id: agent.clone(),
                timestamp: now,
            },
            AgentEvent::CheckpointSaved {
                agent_id: agent.clone(),
                checkpoint_id: "ckpt-1".into(),
                task_id: "task-1".into(),
                crew_id: None,
                squad_id: None,
                timestamp: now,
            },
        ];

        for event in cases {
            let what = serde_json::to_value(&event).unwrap()["type"]
                .as_str()
                .expect("每种事件都带 type 字段")
                .to_string();
            let backend = Arc::new(MockWalBackend::default());
            let wal = crate::wal::AgentWal::new(backend, "s").await.unwrap();
            wal.append(&event).await.expect("append");
            let records = wal.read_since(0).await.expect("read_since");
            assert_eq!(records.len(), 1, "{what} 应该写进一条记录");
            let record = &records[0];
            assert!(
                !matches!(record.event_type, cog_core::WalEventType::Custom { .. }),
                "{what} 被写成了兜底类型 Custom：读盘那头只认专用变体，\
                 这条记录会从重放里静默消失"
            );
            let decoded = wal_record_to_agent_event(record)
                .unwrap_or_else(|e| panic!("{what} 读不回来：{e}"));
            assert_eq!(decoded, event, "{what} 读回来的不是写进去的那条事件");
        }
    }

    /// 专用变体之前写下的 `Custom { name }` 记录仍旧读得回来，未知名字仍旧报错。
    #[test]
    fn legacy_custom_wal_records_still_decode() {
        let now = chrono::Utc::now();
        let cases = vec![
            (
                "self_review",
                json!({"agent_id":"a1","status":"PASS","score":0.9,"timestamp":now}),
            ),
            (
                "react_step_start",
                json!({"agent_id":"a1","iteration":1,"timestamp":now}),
            ),
            (
                "react_step_end",
                json!({"agent_id":"a1","iteration":1,"thought":"t","tool_calls":[],"observations":[],"timestamp":now}),
            ),
            (
                "agent_error",
                json!({"agent_id":"a1","error_code":"e","severity":"critical","details":"d","timestamp":now}),
            ),
            (
                "resource_alert",
                json!({"agent_id":"a1","metric":"cpu","threshold":0.9,"current":0.95,"timestamp":now}),
            ),
            ("heartbeat", json!({"agent_id":"a1","timestamp":now})),
            (
                "checkpoint_saved",
                json!({"agent_id":"a1","checkpoint_id":"c1","task_id":"t1","timestamp":now}),
            ),
        ];
        for (name, payload) in cases {
            let record = cog_core::WalRecord::new(
                0,
                "s",
                cog_core::WalEventType::Custom { name: name.into() },
                payload,
            );
            let decoded = wal_record_to_agent_event(&record)
                .unwrap_or_else(|e| panic!("盘上的老记录 {name} 读不回来：{e}"));
            assert_eq!(
                serde_json::to_value(&decoded).unwrap()["type"],
                json!(name),
                "老记录 {name} 读成了别的事件"
            );
        }

        let unknown = cog_core::WalRecord::new(
            0,
            "s",
            cog_core::WalEventType::Custom {
                name: "no_such_kind".into(),
            },
            json!({ "agent_id": "a1" }),
        );
        assert!(
            wal_record_to_agent_event(&unknown).is_err(),
            "未知的 Custom 名字必须报错——静默返回一个凑合的事件，等于把它记成了没发生过"
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
            final_draft_timeout_secs: 300,
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

    /// Refuses a call the way a real provider does: an error event carrying the
    /// wording, then a final response carrying the refusal the provider
    /// classified out of the HTTP status and the wait the upstream stated in
    /// its headers. The two halves are what the kernel has to join back up.
    struct RefusedStreamLlm {
        wording: String,
        cause: Option<cog_core::UpstreamFailure>,
        wait: Option<u64>,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for RefusedStreamLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, mut producer) = cog_core::EventStream::with_capacity(8);
            let wording = self.wording.clone();
            let (cause, wait) = (self.cause, self.wait);
            tokio::spawn(async move {
                let _ = producer
                    .push(AssistantMessageEvent::Error {
                        reason: cog_core::StopReason::Error,
                        error: Message::assistant_text(wording.clone()),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    error_message: Some(wording),
                    stop_reason: cog_core::StopReason::Error,
                    upstream_failure: cause,
                    retry_after_secs: wait,
                    api: "mock".into(),
                    provider: "mock".into(),
                    model: "mock".into(),
                    ..cog_core::ChatResponse::default()
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// A refusal's type and the wait its upstream stated are the provider's,
    /// not the sentence's, and the kernel is the only place they can be handed
    /// up: the event is what it walked past, the response is where they were
    /// written. Re-derived from the wording, a quota refusal arrives upstairs
    /// as an untyped failure and is re-bought next tick.
    #[tokio::test]
    async fn a_refused_stream_error_carries_its_type_and_the_stated_wait() {
        let llm = RefusedStreamLlm {
            wording: "API error (HTTP 503): upstreams unavailable".into(),
            cause: Some(cog_core::UpstreamFailure::QuotaExhausted),
            wait: Some(5578),
        };
        let mut runtime = stall_test_runtime("refused-typed");
        let err = runtime
            .think_stream(&llm)
            .await
            .expect_err("a refused stream must fail the call");
        assert_eq!(
            err.upstream_failure(),
            Some(cog_core::UpstreamFailure::QuotaExhausted)
        );
        assert_eq!(err.retry_after_secs(), Some(5578));
        assert!(err.is_terminal_upstream_failure());
        assert!(
            err.to_string().contains("LLM stream error")
                && err.to_string().contains("upstreams unavailable"),
            "the wording readers already have must survive, got: {err}"
        );
    }

    /// A stream that ends in an error the provider never classified stays
    /// untyped. Inventing a type here would turn every unclassified failure
    /// terminal, which is the opposite defect: a stream that dies mid-reply
    /// would stop being retried at all.
    #[tokio::test]
    async fn an_unclassified_stream_error_stays_untyped() {
        let llm = RefusedStreamLlm {
            wording: "stream ended without a reply".into(),
            cause: None,
            wait: None,
        };
        let mut runtime = stall_test_runtime("refused-untyped");
        let err = runtime
            .think_stream(&llm)
            .await
            .expect_err("an errored stream must fail the call");
        assert_eq!(err.upstream_failure(), None);
        assert_eq!(err.retry_after_secs(), None);
        assert!(!err.is_terminal_upstream_failure());
        assert!(err.to_string().contains("stream ended without a reply"));
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
            final_draft_timeout_secs: 300,
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

    /// A call whose prompt is wider than the configured window is announced once
    /// per run, and the announcement carries both the configured bound and what
    /// was actually sent.
    ///
    /// Without this the condition is invisible: the call succeeds, the answer
    /// arrives, and only the bill shows the context was bigger than configured.
    /// It is also the only reading on the alerts face that has a producer — that
    /// face's consumers were wired to a frame nothing ever emitted.
    #[tokio::test]
    async fn a_prompt_wider_than_the_window_is_reported_once_per_run() {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        let config = RuntimeConfig {
            agent_id: "overrun".into(),
            context_window_size: 100,
            max_iterations: 1,
            think_stall_timeout_secs: 5,
            final_draft_timeout_secs: 300,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);

        // 刚好等于窗口不算越界：窗口说的是「能送多少」，不是「必须少送一个」。
        runtime.note_context_overrun(100).await;
        assert!(
            rx.try_recv().is_err(),
            "a prompt that exactly fills the window is not an overrun"
        );

        runtime.note_context_overrun(101).await;
        match rx.try_recv().expect("an overrun has to be announced") {
            AgentEvent::ResourceAlert {
                agent_id,
                metric,
                threshold,
                current,
                ..
            } => {
                assert_eq!(agent_id, "overrun");
                assert_eq!(metric, "context_window");
                assert_eq!(threshold, 100.0, "the configured window, not a guess");
                assert_eq!(current, 101.0, "what was actually sent");
            }
            other => panic!("the overrun was announced as something else: {other:?}"),
        }

        // 同一轮再超一次不重复报：第二次是同一件事的更响的说法。
        runtime.note_context_overrun(5_000).await;
        assert!(
            rx.try_recv().is_err(),
            "one overrun per run: a second one would make the first unreadable"
        );

        // 换一轮重新有资格报。不重置的话，第一轮超一次，之后每一轮都静默。
        let llm = CensusLlm {
            text: r#"{"result":"ok"}"#,
            stream_usage: usage(400, 10, 410),
            chat_usage: cog_core::Usage::default(),
            error: false,
        };
        runtime
            .run_scoped(serde_json::json!({"task": "overrun"}), &llm, Some("task-9"))
            .await
            .expect("the run delivers");
        let mut announced = 0;
        while let Ok(event) = rx.try_recv() {
            if matches!(event, AgentEvent::ResourceAlert { .. }) {
                announced += 1;
            }
        }
        assert_eq!(
            announced, 1,
            "a new run over the same window reports it again; a carried-over flag would hide it"
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

    /// Records the messages each call was sent. It answers with JSON so that one
    /// run costs exactly one billed call (a non-JSON first draft would trigger a
    /// second, reformat call).
    #[derive(Default)]
    struct RecordingLlm {
        calls: std::sync::Mutex<Vec<Vec<Message>>>,
    }

    impl RecordingLlm {
        fn calls(&self) -> Vec<Vec<Message>> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for RecordingLlm {
        async fn chat_stream(
            &self,
            messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            self.calls.lock().unwrap().push(messages.to_vec());
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            let text = r#"{"content":"ok","artifacts":[]}"#.to_string();
            tokio::spawn(async move {
                let mut producer = producer;
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn contract_input(attempt: u32, contract_text: &str) -> serde_json::Value {
        serde_json::json!({
            cog_core::contract::prompt::PROMPT_CONTRACT_KEY: {"instructions": contract_text},
            "task": {"id": "t1"},
            "context": {"attempt": attempt},
        })
    }

    /// The stable half lands where the model reads first, and the user message
    /// carries no second copy of it.
    #[tokio::test]
    async fn the_contract_is_hoisted_to_the_front_of_the_prompt() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("contract-front");
        runtime
            .run(contract_input(1, "You are the Generator"), &llm)
            .await
            .expect("the run delivers");

        let calls = llm.calls();
        let first = &calls[0];
        let Message::System { content, .. } = &first[0] else {
            panic!(
                "the first message must be the stable half, got {:?}",
                first[0].role()
            );
        };
        assert!(
            content.contains("You are the Generator"),
            "the stable half itself: {content}"
        );
        // The varying half stays in the user message; the contract is not repeated.
        let user = first
            .iter()
            .find(|m| matches!(m, Message::User { .. }))
            .expect("a user message")
            .content();
        assert!(user.contains("\"attempt\":1"));
        assert!(
            !user.contains("You are the Generator"),
            "sending one contract twice is paying for it twice: {user}"
        );
    }

    /// Across two attempts of one agent, the bytes the model reads first must be
    /// identical: a prefix cache compares from the first byte, and every byte
    /// after the first changed one is bought again at full price. This is also
    /// this change's reading — obtainable without quota, because it measures the
    /// requests themselves rather than the upstream's ledger.
    #[tokio::test]
    async fn two_attempts_share_the_bytes_the_model_reads_first() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("contract-stable");
        for attempt in [1, 2] {
            runtime
                .run(contract_input(attempt, "Emit JSON only"), &llm)
                .await
                .expect("the run delivers");
        }

        let calls = llm.calls();
        let head_of = |call: &Vec<Message>| match &call[0] {
            Message::System { content, .. } => content.clone(),
            other => panic!(
                "the first message must be the stable half, got {:?}",
                other.role()
            ),
        };
        assert_eq!(
            head_of(&calls[0]),
            head_of(&calls[1]),
            "the first message of two attempts must be identical byte for byte"
        );
        // The varying half did vary — otherwise the assertion above is just
        // another way of saying nothing happened.
        let attempt_of = |call: &Vec<Message>| {
            call.iter()
                .rev()
                .find(|m| matches!(m, Message::User { .. }))
                .expect("a user message")
                .content()
        };
        assert_ne!(attempt_of(&calls[0]), attempt_of(&calls[1]));
        // The second attempt continues the conversation, with the contract still
        // at the head and present exactly once.
        assert_eq!(calls[1].len(), 4, "system message plus each round's own");
        let systems = calls[1]
            .iter()
            .filter(|m| matches!(m, Message::System { .. }))
            .count();
        assert_eq!(
            systems, 1,
            "the contract must not reappear mid-conversation"
        );
    }

    /// The user message opens on the request, so the block two attempts share
    /// reaches past the attempt counter that used to lead it.
    ///
    /// The system message is the same in both attempts either way; what differs
    /// is how much of *this* message the upstream cache can hold. Read as a
    /// prefix length rather than as a shape, because the length is the thing the
    /// cache acts on: the old order shared `{"context":{"attempt":`, and
    /// everything after it — the task, its input — was bought again at full
    /// price on every attempt of the same task.
    #[tokio::test]
    async fn two_attempts_share_the_body_of_the_user_message() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("contract-body");
        for attempt in [1, 2] {
            runtime
                .run(contract_input(attempt, "Emit JSON only"), &llm)
                .await
                .expect("the run delivers");
        }

        let calls = llm.calls();
        let body_of = |call: &Vec<Message>| {
            call.iter()
                .rev()
                .find(|m| matches!(m, Message::User { .. }))
                .expect("a user message")
                .content()
        };
        let (first, second) = (body_of(&calls[0]), body_of(&calls[1]));
        let shared = first
            .bytes()
            .zip(second.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        assert!(
            first.starts_with("{\"task\":"),
            "the request must open the message: {first}"
        );
        assert!(
            shared > "{\"context\":{\"attempt\":".len(),
            "the two attempts share only {shared} bytes of the user message"
        );
    }

    /// 每次调用都同时回一段可交付的 JSON 和一个工具调用：于是循环在预算内
    /// 永远停不下来，而钱花完之后那次「不带工具的追问」又确实拿得回东西。
    /// 这正是「被强制追问救回来的那一轮」的形状。
    struct ToolCallingLlm;

    #[async_trait::async_trait]
    impl cog_core::LlmClient for ToolCallingLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            tokio::spawn(async move {
                let mut producer = producer;
                let text = r#"{"result":"salvaged"}"#.to_string();
                let _ = producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: text.clone(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    content: vec![
                        ContentBlock::Text {
                            text,
                            text_signature: None,
                        },
                        ContentBlock::tool_call("call-salvage", "noop", serde_json::json!({})),
                    ],
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
                    text: r#"{"result":"salvaged"}"#.into(),
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

    /// 撞墙后被强制追问救回来的那一轮：读数必须两件都说——它交付了，也把预算
    /// 花光了；而它停下的那个位置不能反过来当「这个角色需要多长」的证据。
    /// 少了前半条，最贵的那种结尾读成健康的；少了后半条，每救一次墙就抬一次。
    #[tokio::test]
    async fn a_salvaged_run_counts_as_spent_without_raising_its_own_ceiling() {
        let role = "salvaged-run-test";
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "salvage".into(),
            role: role.into(),
            max_iterations: 1,
            think_stall_timeout_secs: 5,
            final_draft_timeout_secs: 300,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);
        let llm = ToolCallingLlm;

        let result = runtime
            .run_scoped(serde_json::json!({"task": "salvage"}), &llm, None)
            .await
            .expect("the forced final ask salvages an answer");

        assert_eq!(
            result["result"], "salvaged",
            "the run must end on what the forced ask produced, not on a sentinel: {result}"
        );

        let observer = crate::observable::global_observable();
        use cog_core::Observable as _;
        let metrics = observer.collect_metrics("").await.expect("collect_metrics");
        let spent: f64 = metrics
            .iter()
            .filter(|m| m.name == "agent_iteration_budget_exhausted")
            .filter(|m| m.labels.get("role").map(String::as_str) == Some(role))
            .map(|m| m.value)
            .sum();
        assert_eq!(
            spent, 1.0,
            "a run salvaged out of a budget it had already spent is still a spent budget"
        );
        assert_eq!(
            observer.iteration_budget_for(role, 7),
            7,
            "where it stopped is where the ceiling put it, not evidence the ceiling was low"
        );
    }

    /// 预算内照常给工具调用，撞墙后那次「不带工具的追问」既不吐字也不结束。
    /// 这正是上游卡住时的形状：停顿超时挡不住它，因为流并没有报错，只是不动。
    struct HangingFinalDraftLlm {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for HangingFinalDraftLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::spawn(async move {
                    let mut producer = producer;
                    producer.end(cog_core::ChatResponse {
                        content: vec![ContentBlock::tool_call(
                            "call-hang",
                            "noop",
                            serde_json::json!({}),
                        )],
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
            } else {
                // 生产者活着，但一直不推进：流既没有事件、也没有结束，
                // 于是停顿超时永远等不到「没有进展」，只有这道墙能拦住它。
                tokio::spawn(async move {
                    let _alive = producer;
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                });
            }
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// 追问挂住时：整轮必须按时收场，哨兵要写清是「到点没给完」而不是
    /// 「问过了没答」；而这次撞墙照样要记一次——它只在日志里，读的人
    /// 就永远看不见救不回来的那两次。
    #[tokio::test]
    async fn a_hung_final_draft_ends_the_run_on_its_bound_and_still_counts() {
        let role = "hung-draft-test";
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "hung-draft".into(),
            role: role.into(),
            max_iterations: 1,
            // 停顿窗口远大于这道墙，二者不会互相顶替：1s 后必须由墙收场。
            think_stall_timeout_secs: 300,
            final_draft_timeout_secs: 1,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);
        let llm = HangingFinalDraftLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let started = std::time::Instant::now();
        let result = runtime
            .run(serde_json::json!({"task": "hang"}), &llm)
            .await
            .expect("a hung final draft is not an error: the budget, not the ask, ends the run");

        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the bound must end the run, took {:?}",
            started.elapsed()
        );
        assert_eq!(
            result["status"],
            cog_core::contract::outcome::MAX_ITERATIONS_STATUS
        );
        assert_eq!(
            result["final_draft"], "timeout",
            "the sentinel must say the ask ran out of time, not that it answered nothing: {result}"
        );

        let observer = crate::observable::global_observable();
        use cog_core::Observable as _;
        let metrics = observer.collect_metrics("").await.expect("collect_metrics");
        let spent: f64 = metrics
            .iter()
            .filter(|m| m.name == "agent_iteration_budget_exhausted")
            .filter(|m| m.labels.get("role").map(String::as_str) == Some(role))
            .map(|m| m.value)
            .sum();
        assert_eq!(
            spent, 1.0,
            "a run whose recovery ask hung still hit the ceiling exactly once"
        );
    }

    /// Proposes the same call on every turn, the way a model does when it reads
    /// a refusal as something to work around.
    struct RepeatingCallLlm {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for RepeatingCallLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            tokio::spawn(async move {
                let mut producer = producer;
                producer.end(cog_core::ChatResponse {
                    content: vec![ContentBlock::tool_call(
                        "call-blocked",
                        "noop",
                        serde_json::json!({}),
                    )],
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// Proposes a *different* blocked call on every turn — `noop` with new
    /// arguments each time. Under a per-call bound every one of these is a
    /// first refusal, so the loop would keep buying turns until the iteration
    /// ceiling: the same unbounded spend the refusal was supposed to stop,
    /// reached by changing the arguments instead of repeating them.
    struct VaryingCallLlm {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for VaryingCallLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let attempt = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            tokio::spawn(async move {
                let mut producer = producer;
                producer.end(cog_core::ChatResponse {
                    content: vec![ContentBlock::tool_call(
                        format!("call-blocked-{attempt}"),
                        "noop",
                        serde_json::json!({ "attempt": attempt }),
                    )],
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    struct DenyEverything;

    #[async_trait::async_trait]
    impl cog_core::Guardrail for DenyEverything {
        async fn check_input(&self, _messages: &[Message]) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_output(&self, _response: &str) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_tool_call(&self, _tool: &cog_core::ToolCall) -> cog_core::GuardResult {
            cog_core::GuardResult::Block {
                reason: "not allowed here".into(),
                rule: "runtime-test-rule".into(),
            }
        }
    }

    /// A registry whose guardrail refuses everything, plus the count of times
    /// the tool body ran — which must stay zero on every refusal path.
    fn denied_registry() -> (ToolRegistry, Arc<std::sync::atomic::AtomicUsize>) {
        let mut registry = ToolRegistry::new();
        registry.set_guardrail(Arc::new(DenyEverything));
        let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hit = ran.clone();
        cog_core::ToolRegistry::register(
            &registry,
            cog_core::Tool {
                name: "noop".into(),
                description: "counts its own executions".into(),
                parameters: serde_json::json!({"type": "object"}),
                implementation: cog_core::ToolImplementation::Native(Arc::new(move |_args| {
                    let hit = hit.clone();
                    Box::pin(async move {
                        hit.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(serde_json::json!({ "ran": true }))
                    })
                })),
            },
        );
        (registry, ran)
    }

    /// Drives the scenario once under one policy and reports the three numbers
    /// that say what it cost: how the run ended, whether the tool body ran, and
    /// how many model turns were bought.
    async fn refusal_run(policy: RefusalPolicy) -> (String, usize, usize) {
        let (registry, ran) = denied_registry();
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "blocked-tool".into(),
            role: format!("blocked-tool-{policy:?}"),
            max_iterations: 10,
            ..Default::default()
        };

        let mut runtime = AgentRuntime::new(config, tx)
            .with_tools(registry)
            .with_refusal_policy(policy);
        let llm = RepeatingCallLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let err = runtime
            .run(serde_json::json!({ "task": "blocked" }), &llm)
            .await
            .expect_err("a refusal must end the run instead of being retried");
        (
            err.to_string(),
            ran.load(std::sync::atomic::Ordering::SeqCst),
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
        )
    }

    /// The zero-token policy, kept for roles where any refusal is fatal: the
    /// run ends before another turn is bought. The rule that refused the call
    /// has not moved, so the answer cannot either.
    #[tokio::test]
    async fn a_refusal_ends_the_run_before_another_turn() {
        let (err, ran, calls) = refusal_run(RefusalPolicy::Stop).await;
        assert!(
            err.contains("instead of paying for another turn"),
            "the end must name the refusal, not a budget: {err}"
        );
        assert!(
            err.contains("not allowed here"),
            "the rule's own message must survive the ending: whoever re-dispatches \
             this work plants it into the next attempt, and the policy prose alone \
             does not say why the call was refused: {err}"
        );
        assert_eq!(ran, 0, "the tool body must never run behind a refusal");
        assert_eq!(calls, 1, "one turn to propose the call, and no second");
    }

    /// The default is the recovering one: a refusal alone is not yet a reason
    /// to drop the work — one turn may produce a different call — but it is a
    /// reason not to pay for more than that.
    #[test]
    fn the_default_policy_buys_one_adjustment_turn() {
        assert_eq!(RefusalPolicy::default(), RefusalPolicy::OneAdjustment);
    }

    /// The policy that buys one turn to route around the rule: still bounded,
    /// and the second refusal is what ends it rather than the iteration
    /// ceiling.
    #[tokio::test]
    async fn one_adjustment_spends_exactly_one_turn_on_the_refusal() {
        let (err, ran, calls) = refusal_run(RefusalPolicy::OneAdjustment).await;
        assert!(
            err.contains("a second refusal arrived after the one adjustment turn"),
            "the end must name the repetition, not a budget: {err}"
        );
        assert_eq!(ran, 0, "the tool body must never run behind a refusal");
        assert_eq!(
            calls, 2,
            "one turn to propose the call, one to re-propose it, and no third"
        );

        // The ending has to land where the per-role reader looks, or the only
        // trace of a run a rule stopped is a scattering of tool events.
        use cog_core::Observable as _;
        let metrics = crate::observable::global_observable()
            .collect_metrics("")
            .await
            .expect("collect_metrics");
        let refused = metrics
            .iter()
            .find(|m| {
                m.name == "agent_guardrail_refused_runs"
                    && m.labels.get("role").map(String::as_str)
                        == Some("blocked-tool-OneAdjustment")
            })
            .map(|m| m.value)
            .unwrap_or(-1.0);
        assert_eq!(
            refused, 1.0,
            "a run that ended on a refusal must be counted against its role"
        );
    }

    /// Refuses every call under a rule named for the turn it happened on, so a
    /// single run can meet two different rules.
    struct DenyEachCallWithItsOwnRule {
        seen: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::Guardrail for DenyEachCallWithItsOwnRule {
        async fn check_input(&self, _messages: &[Message]) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_output(&self, _response: &str) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_tool_call(&self, _tool: &cog_core::ToolCall) -> cog_core::GuardResult {
            let n = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            cog_core::GuardResult::Block {
                reason: "not allowed here".into(),
                rule: format!("rule-{n}"),
            }
        }
    }

    /// The attempt this run hands its work back to starts with a fresh
    /// context, so a rule refused earlier in this run would be news to it all
    /// over again. The ending names the last refusal in its typed fields; every
    /// earlier one travels in the reason, which is what the dispatch layer
    /// plants into the next attempt's input.
    #[tokio::test]
    async fn an_earlier_refusal_travels_with_the_one_that_ended_the_run() {
        let mut registry = ToolRegistry::new();
        registry.set_guardrail(Arc::new(DenyEachCallWithItsOwnRule {
            seen: std::sync::atomic::AtomicUsize::new(0),
        }));
        cog_core::ToolRegistry::register(
            &registry,
            cog_core::Tool {
                name: "noop".into(),
                description: "never runs behind a refusal".into(),
                parameters: serde_json::json!({"type": "object"}),
                implementation: cog_core::ToolImplementation::Native(Arc::new(|_args| {
                    Box::pin(async move { Ok(serde_json::json!({ "ran": true })) })
                })),
            },
        );
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "blocked-two-rules".into(),
            role: "blocked-two-rules".into(),
            max_iterations: 10,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx).with_tools(registry);
        let llm = VaryingCallLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let err = runtime
            .run(serde_json::json!({ "task": "blocked" }), &llm)
            .await
            .expect_err("two refusals end the run");
        let text = err.to_string();
        assert!(
            text.contains("also refused in this run") && text.contains("rule-1"),
            "the first rule must travel with the second: {text}"
        );
        let refusal = cog_core::contract::outcome::guardrail_refusal_in(&text)
            .expect("the ending still parses as a refusal");
        assert_eq!(
            refusal.rule, "rule-2",
            "the typed field must stay the rule that ended the run, with the \
             earlier one only in the prose: {text}"
        );
    }

    /// The adjustment is one per run, not one per distinct call. A model that
    /// changes the arguments to get past the bound is proposing a call that is
    /// still refused, so each such turn buys nothing; counting per call would
    /// let it spend the whole iteration budget one refusal at a time.
    #[tokio::test]
    async fn a_new_blocked_call_each_turn_does_not_renew_the_adjustment() {
        let (registry, ran) = denied_registry();
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "blocked-tool".into(),
            role: "blocked-tool-varying".into(),
            max_iterations: 10,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx).with_tools(registry);
        let llm = VaryingCallLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let err = runtime
            .run(serde_json::json!({ "task": "blocked" }), &llm)
            .await
            .expect_err("a different refused call must not renew the adjustment turn");
        assert!(
            err.to_string()
                .contains("a second refusal arrived after the one adjustment turn"),
            "{err}"
        );
        assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the ceiling is two turns, not the iteration budget"
        );
    }

    /// Hands out the scripted replies in order, so a run asked to rewrite can
    /// answer differently the second time. Running past the script repeats its
    /// last line, which is what a model restating itself looks like.
    struct ScriptedReplyLlm {
        replies: std::sync::Mutex<Vec<String>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl ScriptedReplyLlm {
        fn new(replies: &[&str]) -> Self {
            Self {
                replies: std::sync::Mutex::new(replies.iter().map(|r| r.to_string()).collect()),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for ScriptedReplyLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let turn = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let text = {
                let replies = self.replies.lock().unwrap();
                replies
                    .get(turn)
                    .or_else(|| replies.last())
                    .cloned()
                    .unwrap_or_default()
            };
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            tokio::spawn(async move {
                let mut producer = producer;
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    /// Refuses a finished reply that carries the banned word — a rule about how
    /// the answer is phrased, which is what the host's own output gate is.
    struct RefuseBannedWord;

    #[async_trait::async_trait]
    impl cog_core::Guardrail for RefuseBannedWord {
        async fn check_input(&self, _messages: &[Message]) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
        async fn check_output(&self, response: &str) -> cog_core::GuardResult {
            if response.contains("BANNED") {
                cog_core::GuardResult::Block {
                    reason: "the reply hands the decision to a person".into(),
                    rule: "reply-shape".into(),
                }
            } else {
                cog_core::GuardResult::Pass
            }
        }
        async fn check_tool_call(&self, _tool: &cog_core::ToolCall) -> cog_core::GuardResult {
            cog_core::GuardResult::Pass
        }
    }

    fn reply_gate_runtime(role: &str) -> AgentRuntime {
        let mut registry = ToolRegistry::new();
        registry.set_guardrail(Arc::new(RefuseBannedWord));
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "reply-gate".into(),
            role: role.into(),
            max_iterations: 10,
            ..Default::default()
        };
        AgentRuntime::new(config, tx).with_tools(registry)
    }

    /// The answer is judged by the same gate the calls meet. A reply the gate
    /// refuses is not delivered as-is: the model is asked once to rewrite it,
    /// and the rewrite is what leaves the runtime.
    #[tokio::test]
    async fn a_refused_reply_is_rewritten_once_and_then_delivered() {
        let llm = ScriptedReplyLlm::new(&[
            r#"{"content":"BANNED: the owner should decide","artifacts":[]}"#,
            r#"{"content":"the run recorded its own verdict","artifacts":[]}"#,
        ]);
        let mut runtime = reply_gate_runtime("reply-gate-rewrite");
        let result = runtime
            .run(serde_json::json!({ "task": "answer" }), &llm)
            .await
            .expect("the rewritten reply is a deliverable");
        assert_eq!(
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "one reply plus exactly one rewrite, with no reformat call behind it"
        );
        assert!(
            !result.to_string().contains("BANNED"),
            "the refused text must not reach the result: {result}"
        );
        assert!(
            result.to_string().contains("recorded its own verdict"),
            "the rewrite is the deliverable: {result}"
        );
    }

    /// The rewrite is one attempt, not a loop. A model that answers with the
    /// same refused shape twice ends the run as a refusal — the gate's verdict
    /// is not something a second draft can be trusted to have cleared, and the
    /// caller that reads refusals has to see this one like any other.
    #[tokio::test]
    async fn a_second_refusal_ends_the_run_instead_of_delivering() {
        let llm = ScriptedReplyLlm::new(&[
            r#"{"content":"BANNED one","artifacts":[]}"#,
            r#"{"content":"BANNED again","artifacts":[]}"#,
        ]);
        let mut runtime = reply_gate_runtime("reply-gate-refused");
        let err = runtime
            .run(serde_json::json!({ "task": "answer" }), &llm)
            .await
            .expect_err("a reply the gate keeps refusing must not be delivered");
        let text = err.to_string();
        let refusal = cog_core::contract::outcome::guardrail_refusal_in(&text)
            .unwrap_or_else(|| panic!("the ending must parse as a refusal: {text}"));
        assert_eq!(
            refusal.tool, "final_reply",
            "the refusal names the reply, not a tool it did not call: {text}"
        );
        assert_eq!(refusal.rule, "reply-shape");
        assert_eq!(
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the second draft is the last one bought"
        );

        use cog_core::Observable as _;
        let metrics = crate::observable::global_observable()
            .collect_metrics("")
            .await
            .expect("collect_metrics");
        let refused = metrics
            .iter()
            .find(|m| {
                m.name == "agent_guardrail_refused_runs"
                    && m.labels.get("role").map(String::as_str) == Some("reply-gate-refused")
            })
            .map(|m| m.value)
            .unwrap_or(-1.0);
        assert_eq!(
            refused, 1.0,
            "a run ended by the output gate must be counted like one ended by a call"
        );
    }

    /// A tool output too large for the window is archived at the return point and
    /// the marker carries the reference. Only that point holds both the full text
    /// and the budget, so a bound that did not fire here would leave the dropped
    /// tail with no second reader anywhere.
    #[derive(Debug, Default)]
    struct RecordingArchive {
        archived: std::sync::Mutex<Vec<(String, String, String)>>,
        truncations: std::sync::Mutex<Vec<(String, usize)>>,
        unkeyed: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl crate::archive::ToolOutputArchive for RecordingArchive {
        async fn archive(
            &self,
            tool: &str,
            id: &str,
            _content_type: &str,
            text: &str,
        ) -> SFResult<String> {
            self.archived
                .lock()
                .unwrap()
                .push((tool.into(), id.into(), text.into()));
            Ok(cog_core::artifact_uri(
                cog_core::DEFAULT_MEMORY_NAMESPACE,
                id,
            ))
        }

        async fn record_truncation(&self, tool: &str, dropped_chars: usize) {
            self.truncations
                .lock()
                .unwrap()
                .push((tool.into(), dropped_chars));
        }

        async fn record_unkeyed(&self, tool: &str) {
            self.unkeyed.lock().unwrap().push(tool.into());
        }
    }

    /// First turn asks for the tool, second turn delivers. The tool's output is
    /// far past the window share, so the return point must archive it.
    struct OneToolThenAnswerLlm {
        calls: std::sync::atomic::AtomicUsize,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for OneToolThenAnswerLlm {
        async fn chat_stream(
            &self,
            _messages: &[Message],
            _options: &cog_core::ChatOptions,
        ) -> SFResult<cog_core::AssistantMessageEventStream> {
            let first = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0;
            let (stream, producer) = cog_core::EventStream::with_capacity(4);
            tokio::spawn(async move {
                let mut producer = producer;
                let content = if first {
                    vec![ContentBlock::tool_call(
                        "call-big",
                        "bigdump",
                        serde_json::json!({}),
                    )]
                } else {
                    vec![ContentBlock::Text {
                        text: r#"{"done":true}"#.into(),
                        text_signature: None,
                    }]
                };
                let _ = producer
                    .push(AssistantMessageEvent::TextDelta {
                        content_index: 0,
                        delta: String::new(),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                producer.end(cog_core::ChatResponse {
                    content,
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
            unimplemented!()
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn an_oversized_tool_output_is_archived_and_referenced_in_the_marker() {
        let registry = ToolRegistry::new();
        let payload = serde_json::json!({ "blob": "z".repeat(5000) });
        let expected = payload.to_string();
        let blob = payload.clone();
        cog_core::ToolRegistry::register(
            &registry,
            cog_core::Tool {
                name: "bigdump".into(),
                description: String::new(),
                parameters: serde_json::json!({}),
                implementation: cog_core::ToolImplementation::Native(Arc::new(move |_args| {
                    let v = blob.clone();
                    Box::pin(async move { Ok(v) })
                })),
            },
        );
        let archive = Arc::new(RecordingArchive::default());
        let config = RuntimeConfig {
            context_window_size: 400,
            max_iterations: 4,
            ..Default::default()
        };
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut runtime = AgentRuntime::new(config, tx)
            .with_tools(registry)
            .with_tool_output_archive(archive.clone());
        let llm = OneToolThenAnswerLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };
        let _ = runtime
            .run_scoped(serde_json::json!({"goal": "dump"}), &llm, Some("taskseam"))
            .await;

        let archived = archive.archived.lock().unwrap().clone();
        assert_eq!(archived.len(), 1, "一次超大输出，正好归档一次");
        assert_eq!(archived[0].0, "bigdump");
        assert_eq!(
            archived[0].1, "taskseam-0000-0000",
            "id 把任务与 (turn, frame) 编进去，零填充"
        );
        assert_eq!(archived[0].2, expected, "归档的是全文，不是截断后的那段");

        let joined = runtime
            .get_context()
            .messages()
            .iter()
            .map(|m| m.content())
            .collect::<Vec<_>>()
            .join("\n---\n");
        assert!(
            joined.contains("artifact://default/taskseam-0000-0000"),
            "标记里必须带得回引用，否则砍掉的尾巴没人拿得到: {joined}"
        );
        assert!(
            joined.contains("tool output truncated"),
            "被砍过必须在文本里自己说出来: {joined}"
        );

        let truncations = archive.truncations.lock().unwrap().clone();
        assert_eq!(truncations.len(), 1, "一次超大输出，正好记一次截断");
        assert_eq!(truncations[0].0, "bigdump");
        // 记下的字符量要和标记里报的是同一件事：标记左边就是保留下来的头部。
        let shown: usize = joined
            .split("[tool output truncated: showing ")
            .nth(1)
            .and_then(|rest| rest.split(" of ").next())
            .and_then(|n| n.parse().ok())
            .expect("标记里写着保留了多少字符");
        assert_eq!(
            truncations[0].1,
            expected.chars().count() - shown,
            "记录的丢弃量必须是原文长度减去标记里报的保留数；带引用时标记更长，\
             保留数因此更小，这个数不能按不带引用的标记算"
        );
        assert!(
            archive.unkeyed.lock().unwrap().is_empty(),
            "有 task id 的运行不该走无键那条结局"
        );
    }

    /// 只收下 `SkillOutcome` 的反射引擎替身。这条判据问的是「谁报、报了什么」，
    /// 其余七条方法都不在它要证的范围内，一律空实现。
    #[derive(Debug, Default)]
    struct SkillOutcomeRecorder {
        seen: std::sync::Mutex<Vec<cog_core::SkillOutcome>>,
    }

    #[async_trait::async_trait]
    impl cog_core::ReflectionEngine for SkillOutcomeRecorder {
        async fn process_skill_outcome(
            &self,
            outcome: cog_core::SkillOutcome,
        ) -> cog_core::SFResult<()> {
            self.seen.lock().unwrap().push(outcome);
            Ok(())
        }

        async fn process_tool_result(
            &self,
            _tool_name: &str,
            _result: &serde_json::Value,
            _is_error: bool,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn process_context(&self, _messages: &[Message]) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn process_event(&self, _event: &AgentEvent) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn extract_and_insert(
            &self,
            _pattern: &cog_core::Pattern,
        ) -> cog_core::SFResult<Option<String>> {
            Ok(None)
        }

        fn start_reviewer(&self) -> Option<tokio::task::JoinHandle<()>> {
            None
        }

        async fn record_squad_result(
            &self,
            _task_id: &str,
            _goal: &str,
            _success: bool,
            _pge_mode: &str,
            _score: Option<f32>,
            _latency_ms: u64,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn record_change_outcome(
            &self,
            _change_id: &str,
            _success: bool,
            _test_output: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 这一趟跑的是哪个技能、成没成，要报到有效性追踪器上。
    ///
    /// 接上之前，报进追踪器的只有 squad 与变更流水那两条自报家门的结果
    /// （`squad_execution` / `change_deployment`）——注册表里没有这两个名字
    /// （技能名来自 skills 目录加载的那份），于是由它们推出来的动作改不动任何
    /// 注册表，还会写下关于不存在技能的学习。角色绑定的那个技能与这一趟的结局
    /// 只在这里拿得到，所以判据落在这里：跑一趟，报上来的技能名与结局就是这一趟的。
    #[tokio::test]
    async fn a_run_reports_its_ending_against_the_skill_its_role_was_bound_to() {
        let llm = RecordingLlm::default();
        let reflection = Arc::new(SkillOutcomeRecorder::default());
        let skill = SkillConfig {
            skill_id: "skill-outcome-skill".into(),
            name: "Skill Outcome Test".into(),
            system_prompt: String::new(),
            tools: Vec::new(),
            max_iterations: 1,
            role_type: "skill-outcome-role".into(),
        };
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            role: "skill-outcome-role".into(),
            skill_config: Some(skill),
            max_iterations: 1,
            think_stall_timeout_secs: 5,
            final_draft_timeout_secs: 300,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx).with_reflection_engine(reflection.clone());

        runtime
            .run(contract_input(1, "Emit JSON only"), &llm)
            .await
            .expect("the run delivers");

        let seen = reflection.seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "一趟运行报一条结局，不重不漏");
        assert_eq!(
            seen[0].skill_id, "skill-outcome-skill",
            "报的必须是角色真正绑上的那个技能名"
        );
        assert!(seen[0].success, "交付了就是成功");
    }
}
