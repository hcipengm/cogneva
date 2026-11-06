use cog_core::{
    AgentEvent, AssistantMessageEvent, ContentBlock, Message, SFError, SFResult, ToolCall,
    ToolDefinition,
};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::context::{truncate_to_chars, ContextWindow};
use crate::hooks::LifecycleHookEvent;
use crate::tools::{ToolRegistry, ToolRunError};

/// Smallest repeated tool result that is worth replacing with a pointer.
///
/// Below this a repeated result is a tool answering a different question with
/// the same few words, and the pointer would not be shorter than the answer.
// cap: bound — 指针本身是一段固定的话；结果比它长不了多少时，换成指针既省不下字节，又让模型多跳一次去读上面那份。这个界是「省下的字节」与「多一跳」相抵的位置，不是从机器上量出来的
const REPEATED_TOOL_RESULT_MIN_CHARS: usize = 2_000;

/// How many calls a run may spend without changing the checkout before it is
/// told that nothing has changed.
///
/// Measured over the delivered generation runs of 2026-11-06 (24 h, 215 runs
/// segmented by gaps of five minutes): the median run was 5 calls and the
/// ninetieth percentile 32, so a run that has spent forty calls without writing
/// anything has already outlived nine runs in ten. A lower bound would
/// interrupt ordinary reading — a run that plans before it writes reads for a
/// while first, and that stretch is not the one this note is for.
// cap: bound — 四十是「九成的已交付运行此刻已经跑完」的位置；再低会把正常的先读后写当成迷路
const CALLS_WITHOUT_CHANGE: u32 = 40;

/// How many times one run may be told that the checkout has not moved.
// cap: bound — 一条提示本身也要付一次回合，且落地会作废它之后的缓存前缀；两次之后不再打断
const NOTHING_CHANGED_NOTES: u32 = 2;

/// What the model is told when the checkout has not moved for a long stretch.
///
/// It says what the reading is and what the two ways out are, without asking
/// for a report: the run has one artifact to produce, and a note that invites
/// prose invites a turn spent on prose.
const NOTHING_CHANGED_NOTE: &str = "The checkout has not changed for the last 40 tool calls: every one of them \
     read, searched or printed something, and none of them wrote. Searching cannot finish this — it can only \
     tell you where the change goes. If you already know where it goes, make the edit now. If you do not, say \
     which single fact is still missing instead of searching for it again.";

/// The command forms that leave the checkout different from what it was.
const MUTATING_COMMAND_FORMS: &[&str] = &[
    "sed -i",
    "tee ",
    ">>",
    "git add",
    "git commit",
    "git apply",
    "git checkout",
    "git restore",
    "git stash",
    "cargo fmt",
    "rustfmt ",
    "mkdir ",
    "touch ",
    "mv ",
    "cp ",
    "rm ",
    "ln ",
    "patch ",
    "> ",
];

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

impl RuntimeState {
    /// Every variant, so the word table and the lookup that reads it back are
    /// checked against the enum instead of against a hand-copied list.
    pub const ALL: [RuntimeState; 8] = [
        RuntimeState::Idle,
        RuntimeState::Thinking,
        RuntimeState::Calling,
        RuntimeState::Acting,
        RuntimeState::Observing,
        RuntimeState::Updating,
        RuntimeState::Complete,
        RuntimeState::Error,
    ];

    /// The word this state is checkpointed under.
    ///
    /// One spelling, in one place. `Display` prints it and `parse` accepts it, so
    /// the word `save` writes into a checkpoint and the word `restore` reads back
    /// out of one cannot come to disagree: the checkpoint says "observing", the
    /// restore matches none of its arms, and the agent comes back idle with
    /// nothing anywhere saying a word was dropped.
    pub fn as_str(self) -> &'static str {
        match self {
            RuntimeState::Idle => "idle",
            RuntimeState::Thinking => "thinking",
            RuntimeState::Calling => "calling",
            RuntimeState::Acting => "acting",
            RuntimeState::Observing => "observing",
            RuntimeState::Updating => "updating",
            RuntimeState::Complete => "complete",
            RuntimeState::Error => "error",
        }
    }

    /// The state a checkpointed word names; `None` for a word this build does not
    /// know.
    pub fn parse(word: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|state| state.as_str() == word)
    }
}

impl std::fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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
    /// Whether this run's deliverable is the reply text itself.
    ///
    /// A task run answers with a document the downstream steps read fields out
    /// of, so its last thought is turned into JSON (and, when the model did not
    /// write JSON, converted by one more model call). A conversation turn's
    /// deliverable is the sentence a person is reading on screen: asking a model
    /// to restate it as JSON would spend a call to change the answer's shape and
    /// hand back something the interface cannot print.
    reply_is_the_result: bool,
    /// The actor label this run's calls carry, when the role family is not the
    /// right grouping for them. `None` means `agent:<role>`.
    actor_label: Option<String>,
    /// Who these calls are spent for, when the caller knows a person.
    ///
    /// The task identity on the ledger answers "which job", the owner answers
    /// "whose quota" — a call made from an interface is charged to the person
    /// typing, and without this header it lands in the unattributed bucket.
    call_owner: Option<String>,
    /// The environment these calls are made from (interactive, pool, ...).
    /// `None` leaves the upstream default in place.
    call_origin: Option<String>,
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
/// A digest of the exact request a call would put on the wire.
///
/// The conversation and the tool list are the request. Options that only decide
/// how the answer is billed or labelled — cache retention, the actor stamp, the
/// task id — are left out on purpose: two attempts that differ in nothing the
/// model reads are the same question, and re-asking it inside a wait the upstream
/// named buys the same refusal at full price.
///
/// What the model reads is the message *content*, so every field that is only
/// bookkeeping stays out of the digest. The timestamp above all: it is stamped
/// when the message is built, so two sends of the same prompt differ in it on
/// every call, and hashing the serialized message would make every pair of
/// attempts look like different requests — which is exactly the case this guard
/// exists for.
fn request_digest(messages: &[Message], options: &cog_core::ChatOptions) -> String {
    let mut hasher = Sha256::new();
    let mut put = |part: &[u8]| {
        hasher.update(part);
        hasher.update([0u8]);
    };
    for message in messages {
        match message {
            Message::System { content, .. } => {
                put(b"system");
                put(content.as_bytes());
            }
            Message::User { content, .. } => {
                put(b"user");
                put(&serde_json::to_vec(content).unwrap_or_default());
            }
            Message::Assistant {
                content,
                tool_calls,
                ..
            } => {
                put(b"assistant");
                put(&serde_json::to_vec(content).unwrap_or_default());
                put(&serde_json::to_vec(tool_calls).unwrap_or_default());
            }
            Message::ToolResult {
                tool_call_id,
                tool_name,
                content,
                is_error,
                ..
            } => {
                put(b"tool");
                put(tool_call_id.as_bytes());
                put(tool_name.as_bytes());
                put(&[u8::from(*is_error)]);
                put(&serde_json::to_vec(content).unwrap_or_default());
            }
        }
    }
    if let Some(tools) = options.tools.as_ref() {
        for tool in tools {
            let parameters = tool.parameters.to_string();
            for part in [
                tool.name.as_str(),
                tool.description.as_str(),
                parameters.as_str(),
            ] {
                put(part.as_bytes());
            }
        }
    }
    hex::encode(hasher.finalize())
}

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

/// 这一句答案外面有没有壳，有就还它里面那句话。
///
/// 有的模型把整句回答包成一个对象发过来（实测 `{"answer":"…"}`、
/// `{"role":"employee","thought":"…"}`）。这一面交付的是说给人听的那一句，壳不是
/// 它的一部分：原样交出去，屏幕上读到的是花括号和转义，而收信的同事读到的还是
/// 第二层——`{"status":"completed","reply":"{\"answer\":…}"}`。
///
/// 只拆「整段就是一个对象、只有一个键、键名是已知的答案字段」的那种壳。别的
/// 一律原样交：一段真的以 JSON 为内容的回答不该被截掉一半，一个多键的对象也
/// 不该只留下其中一个键。
fn plain_answer(draft: &str) -> String {
    const ANSWER_FIELDS: [&str; 7] = [
        "answer", "reply", "text", "content", "message", "thought", "result",
    ];
    let Ok(serde_json::Value::Object(fields)) = serde_json::from_str(draft.trim()) else {
        return draft.to_string();
    };
    let Some((key, value)) = fields.iter().next() else {
        return draft.to_string();
    };
    match value {
        serde_json::Value::String(text)
            if fields.len() == 1 && ANSWER_FIELDS.contains(&key.as_str()) =>
        {
            text.clone()
        }
        _ => draft.to_string(),
    }
}

/// The request block exactly as `cog_core::contract::prompt::render_varying_half`
/// writes it, if the payload carries one.
///
/// The membership comes from
/// `cog_core::contract::prompt::REQUEST_KEYS` — the contract says which keys
/// state the request, this decides what to do about it. The comparison has to
/// be against the **rendered** spelling (`"task":{…}`), not the value alone:
/// the value also appears anywhere else the same text was quoted, and a
/// substring match on it would claim the request was already sent when only a
/// fragment of it was. A test pins these bytes to the renderer's.
fn request_members(payload: &serde_json::Value) -> Option<String> {
    let fields = payload.as_object()?;
    let key_text = |key: &str| serde_json::Value::String(key.to_string()).to_string();
    let members: Vec<String> = cog_core::contract::prompt::REQUEST_KEYS
        .iter()
        .filter_map(|key| {
            fields
                .get(*key)
                .map(|value| format!("{}:{}", key_text(key), value))
        })
        .collect();
    (!members.is_empty()).then(|| members.join(","))
}

/// Render what this attempt adds, leaving the request block out.
///
/// For a second attempt at the same task the request is byte-identical to the
/// one already in the conversation. Re-sending it adds nothing the model can
/// read twice and pays for every byte of it again: on the measured traffic a
/// fifth of the bytes added per turn were these repeated request blocks.
/// `None` when nothing would be left — a user message that says only `{}` is a
/// turn that says nothing.
fn render_varying_half_after_request(payload: &serde_json::Value) -> Option<String> {
    let fields = payload.as_object()?;
    let key_text = |key: &str| serde_json::Value::String(key.to_string()).to_string();
    let mut members: Vec<String> = Vec::with_capacity(fields.len());
    for (key, value) in fields {
        if cog_core::contract::prompt::REQUEST_KEYS.contains(&key.as_str()) {
            continue;
        }
        members.push(format!("{}:{}", key_text(key), value));
    }
    (!members.is_empty()).then(|| format!("{{{}}}", members.join(",")))
}

/// Take the attachments out of the document and hand them back as media blocks.
///
/// An attachment left in the document reaches the model as base64 prose: bytes
/// it cannot read as a picture, charged for by the token. So the key is lifted
/// out before the rest is rendered — the model sees the picture and the text,
/// not a picture's worth of text.
///
/// The shape is the one producers write: an array of `{"data_base64": …,
/// "mime_type": …}`. Entries without data are dropped rather than turned into
/// empty media blocks, and a missing MIME type reads as opaque bytes instead of
/// guessing a format the bytes may not be.
fn take_media(mut payload: serde_json::Value) -> (Vec<cog_core::ContentBlock>, serde_json::Value) {
    let taken = payload
        .as_object_mut()
        .and_then(|doc| doc.remove(cog_core::contract::prompt::MEDIA_KEY));
    let entries = match taken {
        None | Some(serde_json::Value::Null) => return (Vec::new(), payload),
        Some(serde_json::Value::Array(items)) => items,
        Some(one) => vec![one],
    };
    let blocks: Vec<cog_core::ContentBlock> = entries
        .into_iter()
        .filter_map(|entry| {
            let data = entry.get("data_base64")?.as_str()?.trim();
            if data.is_empty() {
                return None;
            }
            let mime_type = entry
                .get("mime_type")
                .and_then(|value| value.as_str())
                .map(str::trim)
                .filter(|mime| !mime.is_empty())
                .unwrap_or("application/octet-stream");
            Some(cog_core::ContentBlock::media(data, mime_type))
        })
        .collect();
    (blocks, payload)
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

/// Wall for assembling the run's final result at the end of the loop. This is
/// bookkeeping the run already has the material for, so it is generous only to
/// cover a large artifact being rendered — a run that spends this long has
/// wedged rather than worked.
// cap: bound — 要容得下渲染一件大产出；花掉这么久的运行是卡住了而不是在干活
const RESULT_BUILD_TIMEOUT: Duration = Duration::from_secs(180);

/// The status a conversation turn's result carries when it ends on an answer.
///
/// The other endings name themselves in the result already (the budget one is
/// [`cog_core::contract::outcome::MAX_ITERATIONS_STATUS`]); this is the one an
/// interface has to tell apart from them to know it has a sentence to print.
const CONVERSATION_REPLY_STATUS: &str = "completed";

/// How long it has been since the checkout was last different.
///
/// Every other exit presupposes that a run which keeps calling tools is
/// getting somewhere: it stops when the model stops calling tools, when a
/// rule refuses the same call, or when the iteration budget runs out. None
/// of them can see a run that keeps calling tools and never changes the
/// tree, because each call is a new one. Measured on one generation run
/// (2026-11-05, `change-ledger-539f5ef9d7db64cd`): 246 commands — 127
/// `grep`, 75 `sed`, 9 `git`, 3 compiles, and 2 `write_file` — over 2.3
/// hours, 234 of the 246 distinct, so the repeated-result stub below never
/// fired once. That run was not stuck on a hard step; it was searching and
/// never deciding it had searched enough, and nothing in its transcript
/// said how long it had been since anything moved.
///
/// So the reading kept here is of effect, not of effort: how many calls
/// have passed since the checkout changed. The note it produces is a
/// correction to that silence, not a ceiling on the run.
#[derive(Default)]
struct CheckoutProgress {
    calls_without_change: u32,
    notes_given: u32,
}

impl CheckoutProgress {
    fn note_call(&mut self, tool: &str, arguments: &serde_json::Value) {
        if changes_the_checkout(tool, arguments) {
            self.calls_without_change = 0;
        } else {
            self.calls_without_change = self.calls_without_change.saturating_add(1);
        }
    }

    /// The note to put in front of the model, once the count has run past
    /// the bound.
    ///
    /// Bounded per run rather than per crossing: the note costs a turn to
    /// answer and voids the cached prefix from where it lands, so it is a
    /// correction to give twice at most, not a running commentary. When the
    /// run writes again the count resets, and the next stretch earns its own
    /// note.
    fn take_note(&mut self) -> Option<&'static str> {
        if self.calls_without_change < CALLS_WITHOUT_CHANGE
            || self.notes_given >= NOTHING_CHANGED_NOTES
        {
            return None;
        }
        self.calls_without_change = 0;
        self.notes_given += 1;
        Some(NOTHING_CHANGED_NOTE)
    }
}

/// Whether a call leaves the checkout different from what it was.
///
/// `run_command` is one tool that both reads and writes, so for it the
/// effect has to be read off the command. The list of forms is short and
/// literal on purpose, and it is wrong in one direction only: a mutating
/// form it does not know about makes the note fire when something did
/// change, and the model can contradict a note by looking at what it just
/// wrote. A form wrongly counted as mutating would hide a run that is
/// circling, which is the failure the note exists to catch, so the list
/// errs towards calling too much of the shell a write.
fn changes_the_checkout(tool: &str, arguments: &serde_json::Value) -> bool {
    if tool != "run_command" {
        return tool == "write_file";
    }
    let command = arguments
        .get("command")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    // Bytes thrown away or pointed at a descriptor change no file: the whole
    // redirect goes, not just its target, or the bare `>` it leaves behind
    // reads as writing to something.
    let command = command
        .replace(">> /dev/null", "")
        .replace(">>/dev/null", "")
        .replace("> /dev/null", "")
        .replace(">/dev/null", "")
        .replace(">&1", "")
        .replace(">&2", "");
    // `--check` is the reporting form of the formatters whose plain form writes.
    let reports_only = command.contains("--check");
    MUTATING_COMMAND_FORMS.iter().any(|form| {
        let formats = form.starts_with("cargo fmt") || form.starts_with("rustfmt");
        !(reports_only && formats) && command.contains(form)
    })
}

/// 「这一次运行已经交出去的那份输出」的指纹：内容哈希与字节数。
///
/// 只认内容不认调用：同一个 `read_file` 换个区间就是另一份输出，不该被当成
/// 重复；反过来两条不同形状的命令打出同一份字节，那两份字节也确实只值付一次。
type OutputFingerprint = ([u8; 16], usize);

fn output_fingerprint(text: &str) -> OutputFingerprint {
    let digest = Sha256::digest(text.as_bytes());
    let mut head = [0u8; 16];
    head.copy_from_slice(&digest[..16]);
    (head, text.len())
}

/// 一趟运行已经交给模型的每一份工具输出。
///
/// 这份记录必须挂在运行上，不能从上下文窗口里读。窗口满了就从最前面丢轮次，
/// 而「窗口满」正是重复请求开始出现的那一刻——记录存在窗口里，等于在最需要
/// 记得「这一份已经给过」的时候把它忘掉，于是每填满一次窗口就要重新读一遍
/// 同样的字节，读完又填满。
///
/// 实测（2026-11-06，`change-ledger-0ea2e24e8f45341e`）：360 次工具调用里
/// 119 次是逐字重复已经交过的输出，其中一个 1,567 行的测试文件被读了 219
/// 次、其中 26 种区间被反复要；那一轮 286 次模型调用平均每次只产出 108 个
/// token——容量几乎全花在把刚被裁掉的上下文重新取回来上。
///
/// 指纹取自**原始输出**，不是进入窗口之后那一份。超预算的结果会被裁，裁出来
/// 的标记里带着轮次号，于是同一份原始输出每次被裁成的样子都不一样——按窗口里
/// 那份比对的门，对最大的那些结果永远不会响，而它们正是最贵的那一类。
#[derive(Default)]
struct DeliveredResults {
    by_output: std::collections::HashMap<OutputFingerprint, Delivered>,
}

struct Delivered {
    /// 第一次交出去的那一轮。
    turn: u32,
    /// 走到今天一共被要了几次，含第一次。
    asked: u32,
}

impl DeliveredResults {
    /// 这一份输出换来的那段话；没交过就记下并返回 `None`。
    ///
    /// 只对够长的输出说话：一条「0 errors」被问第二遍时，指针和原文一样短，
    /// 换过去只会让模型多跳一次。
    fn repeat_note(&mut self, tool: &str, text: &str, turn: u32) -> Option<String> {
        if text.chars().count() < REPEATED_TOOL_RESULT_MIN_CHARS {
            return None;
        }
        let key = output_fingerprint(text);
        let delivered = match self.by_output.get_mut(&key) {
            None => {
                self.by_output.insert(key, Delivered { turn, asked: 1 });
                return None;
            }
            Some(delivered) => {
                delivered.asked = delivered.asked.saturating_add(1);
                delivered
            }
        };
        let note = serde_json::json!({
            "deduplicated": true,
            "bytes": text.len(),
            "first_returned_at_turn": delivered.turn,
            "times_asked": delivered.asked,
            "note": format!(
                "This run already returned exactly these bytes of `{tool}` at turn {} (ask {}). \
                 They are not sent again: re-inserting bytes you already have evicts turns you \
                 have not read yet, and that is how a run re-reads the same file until it runs \
                 out. Read them above if they are still there; if the window dropped them, ask \
                 for the narrower slice you need.",
                delivered.turn, delivered.asked
            ),
        });
        Some(note.to_string())
    }
}

impl AgentRuntime {
    /// Access the loop configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    pub fn new(mut config: RuntimeConfig, event_tx: mpsc::Sender<AgentEvent>) -> Self {
        // 技能面写下的界只是这个角色的起点；没写就没有界。写成「没写就取 10」
        // 会让每一个角色都被同一个没人量过的数字裁着，而它裁的恰恰是「这一轮还
        // 要不要接着干」——2026-11-05 实测 generator 在第十轮带着一个没跑完的
        // 工具调用被砍，下一轮从头再来，同一条 CI 红修了六版。
        if let Some(iterations) = config.skill_config.as_ref().and_then(|s| s.max_iterations) {
            config.max_iterations = iterations;
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
            reply_is_the_result: false,
            actor_label: None,
            call_owner: None,
            call_origin: None,
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

    /// Declare this run's calls under a face of their own instead of the role
    /// family. The value must be one the actor label set admits.
    pub fn with_actor_label(mut self, label: impl Into<String>) -> Self {
        self.actor_label = Some(label.into());
        self
    }

    /// Charge this run's calls to a person, and declare the environment they
    /// are made from.
    pub fn with_call_identity(mut self, owner: impl Into<String>, origin: &str) -> Self {
        self.call_owner = Some(owner.into());
        self.call_origin = Some(origin.to_string());
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
    /// The keys a checkpoint's `agent_state` object carries.
    ///
    /// `checkpoint` writes them, `restore` reads them back, and the two are one
    /// object seen twice. A key renamed on one side alone leaves the other
    /// looking for a field that is not there -- and `restore` reads every one of
    /// them as optional, so the agent comes back with the value it was
    /// configured with and nothing anywhere says the snapshot was ignored.
    const CHECKPOINT_KEY_AGENT_ID: &'static str = "agent_id";
    const CHECKPOINT_KEY_ROLE: &'static str = "role";
    const CHECKPOINT_KEY_STATE: &'static str = "state";
    const CHECKPOINT_KEY_STEPS_LEN: &'static str = "steps_len";

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
                Self::CHECKPOINT_KEY_AGENT_ID: self.config.agent_id,
                Self::CHECKPOINT_KEY_ROLE: self.config.role.to_string(),
                Self::CHECKPOINT_KEY_STATE: self.state.to_string(),
                Self::CHECKPOINT_KEY_STEPS_LEN: self.steps.len(),
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
        if let Some(state_str) = snapshot
            .agent_state
            .get(Self::CHECKPOINT_KEY_STATE)
            .and_then(|v| v.as_str())
        {
            // A word this build does not know still lands on `Idle` -- restoring is
            // not a judgement. What changed is that the words are no longer spelled
            // here: `RuntimeState::parse` answers with the same table `Display`
            // printed from, so there is no second list that can fall behind it.
            self.state = RuntimeState::parse(state_str).unwrap_or(RuntimeState::Idle);
        }

        // Optionally restore role if present and different
        if let Some(role_str) = snapshot
            .agent_state
            .get(Self::CHECKPOINT_KEY_ROLE)
            .and_then(|v| v.as_str())
        {
            self.config.role = role_str.to_string();
        }

        // Optionally restore agent_id if present
        if let Some(agent_id) = snapshot
            .agent_state
            .get(Self::CHECKPOINT_KEY_AGENT_ID)
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

        // 这一路每一轮都重发整段历史：工具定义、system、以及上一次的每一步。前面
        // 那一大截逐字节没变，不向上游要缓存就等于按原价把它重买一遍——2026-10-26
        // 实测同一批提示词在不开缓存断点的协议面上只命中 3.6%，在同一天的兼容面上
        // 是 50%～67%。短窗就够：一轮之内连续重发，间隔是秒级，不是小时级。
        let options = cog_core::ChatOptions {
            tools: tool_defs,
            cache_retention: cog_core::CacheRetention::Short,
            ..Default::default()
        }
        .with_actor(&self.actor_label());
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

    /// The actor label this run's calls carry.
    ///
    /// A role's runs group under the role family (`agent:<role>`), which is what
    /// the cost views read. A run whose calls belong to a face of their own —
    /// the interface conversation is the one that exists — names it here, and
    /// the label still comes from the closed set the edge admits.
    fn actor_label(&self) -> String {
        self.actor_label
            .clone()
            .unwrap_or_else(|| format!("agent:{}", self.config.role))
    }

    /// Put the run's identity on a call's headers, when there is one.
    ///
    /// The task identity is known only to whoever started the run; the
    /// transport cannot derive it, and a table mapping components to tasks
    /// would be wrong the moment one component serves two. A run started
    /// without an identity makes unlabelled calls rather than a made-up one,
    /// because a made-up id reads as a real job on the ledger.
    fn stamp_task(&self, options: cog_core::ChatOptions) -> cog_core::ChatOptions {
        // Charged to a person, when there is one: the same call the interface's
        // own completion path used to stamp, now stamped where the call is made.
        let options = match self.call_owner.as_deref() {
            Some(owner) => options.with_owner(owner),
            None => options,
        };
        let options = match self.call_origin.as_deref() {
            Some(origin) => options.with_origin(origin),
            None => options,
        };
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
        let started = self.begin_scoped_run(run_task_id);
        let outcome = self.run_turns(input, llm, run_task_id).await;
        self.finish_scoped_run(run_task_id, started, outcome).await
    }

    /// Run one turn of a conversation the caller has already assembled.
    ///
    /// This is the entry the interface conversation uses. It exists because the
    /// capability that conversation needs — read the ledgers, dispatch work,
    /// watch it — is the loop this file already runs, and a second loop written
    /// beside it would answer the same questions a second way: its tool calls
    /// would not pass the guardrail, its tokens would not reach the census, and
    /// the events it produced would not be on the bus. What differs between a
    /// task attempt and a conversation turn is only where the input comes from:
    /// here the caller hands over the whole history (system prompt, earlier
    /// exchanges, this turn's sentence) and the loop starts from it.
    pub async fn run_conversation(
        &mut self,
        messages: Vec<Message>,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        let started = self.begin_scoped_run(None);
        let outcome = self.run_conversation_turns(messages, llm).await;
        self.finish_scoped_run(None, started, outcome).await
    }

    async fn run_conversation_turns(
        &mut self,
        messages: Vec<Message>,
        llm: &dyn cog_core::LlmClient,
    ) -> SFResult<serde_json::Value> {
        self.reply_is_the_result = true;
        self.begin_run(None).await?;
        // The caller's history is this turn's whole context. Anything left from
        // an earlier turn would be a conversation the model was never given.
        self.context.clear();
        for message in messages {
            self.context.add_message(message);
        }
        self.absorb_base_instruction();
        self.run_loop(llm, None).await
    }

    /// 每个运行开头那段同一的话，来自部署的声明而不是代码里的常量。
    ///
    /// 两条入口都要它：一次任务尝试与一轮对话在内核里是同一条循环，只有输入
    /// 从哪儿来不同；一段话只给其中一条，就等于平台上又有了两类智能体。
    fn absorb_base_instruction(&mut self) {
        if let Some(text) = crate::agent_kernel::base_instruction::text() {
            self.context.merge_head_system(&text);
        }
    }

    /// The prologue every run shares, whichever way its input arrives.
    ///
    /// The census is a property of this run, so it starts here and is reported
    /// from exactly one place — the run's only exit. Doing it at the
    /// answer-recording site instead would leave every run that ended any other
    /// way (budget spent, upstream error, stall) unmeasured, and those are the
    /// runs a token investigation is about.
    fn begin_scoped_run(&mut self, run_task_id: Option<&str>) -> std::time::Instant {
        self.run_usage = RunUsage::default();
        self.run_iterations = 0;
        // 换了一次运行就是一个新的额度：上一轮报过的那次超大 prompt，不能替这一轮
        // 把嘴堵上，否则第二轮同样的问题会在任何地方都读不到。
        self.context_overrun_reported = false;
        self.run_outcome = None;
        // Set before the first call and cleared at the only exit, so a run that
        // was started without an identity cannot inherit the previous one.
        self.run_task_id = run_task_id.map(str::to_string);
        std::time::Instant::now()
    }

    /// ... and its epilogue, for the same reason.
    async fn finish_scoped_run(
        &mut self,
        run_task_id: Option<&str>,
        started: std::time::Instant,
        outcome: SFResult<serde_json::Value>,
    ) -> SFResult<serde_json::Value> {
        self.report_run_census(run_task_id, started.elapsed()).await;
        self.report_skill_outcome(started.elapsed()).await;
        // 一跑以错误收场，就在这一处报出去：`run_loop` 的三个出口都汇到这里，
        // 逐出口去报就得在每一处重写一遍「报过没有」，而漏掉的那一处正好是
        // 读者最想知道的那一种结局。
        if let Err(ref e) = outcome {
            self.note_run_failure(e).await;
        }
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
        // 与清点行同一身份、同一时刻：`/api/v1/tasks/{id}/logs` 上读到的这一行
        // 说的就是这次运行。没有它，那条路由对每个任务都答 `[]`，而那读起来
        // 和「这个任务什么都没记」一模一样——读者分不出「跑了但没记」与「记录
        // 从来没接上」。写失败不改变运行的结局，只让这一格空着，所以它也只告警。
        let ending = self.run_outcome;
        let (message, ending_label) = match ending {
            Some(e) => (
                format!(
                    "run finished as {e:?} after {} iterations and {} tool calls",
                    metrics.iterations, metrics.tool_calls
                ),
                Some(format!("{e:?}")),
            ),
            None => (
                format!(
                    "run finished after {} iterations and {} tool calls, with no ending recorded",
                    metrics.iterations, metrics.tool_calls
                ),
                None,
            ),
        };
        let entry = cog_core::LogEntry {
            timestamp: metrics.timestamp,
            level: "info".to_string(),
            source: "agent_kernel".to_string(),
            message,
            metadata: serde_json::json!({
                "agent_id": self.config.agent_id,
                "ending": ending_label,
                "total_tokens": metrics.total_tokens,
                "prompt_tokens": metrics.prompt_tokens,
                "completion_tokens": metrics.completion_tokens,
                "tool_calls": metrics.tool_calls,
                "iterations": metrics.iterations,
                "duration_ms": metrics.duration_ms,
            }),
        };
        if let Err(e) = gateway.record_task_log(task_id, entry).await {
            tracing::warn!(
                agent_id = %self.config.agent_id,
                task_id,
                error = %e,
                "run log write failed; this task's log route will not show this run"
            );
        }
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
    ///
    /// `delivered` carries what this run has already handed over; a repeat is
    /// answered with a pointer instead of the payload, so the tool_use /
    /// tool_result pairing the provider requires is kept and the bytes are paid
    /// for once.
    ///
    /// Every argument is an independent coordinate of the one call being
    /// answered (which task, which turn and frame of it, which call id and tool,
    /// what it returned, and what this run has already handed over); bundling
    /// them into a carrier would move the same count behind a name.
    #[allow(clippy::too_many_arguments)]
    async fn tool_result_message(
        &self,
        task_id: Option<&str>,
        turn: u32,
        frame: u32,
        tool_call_id: &str,
        tool_name: &str,
        text: &str,
        delivered: &mut DeliveredResults,
    ) -> Message {
        if let Some(stub) = delivered.repeat_note(tool_name, text, turn) {
            return Message::tool_result_text(tool_call_id, tool_name, &stub);
        }
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
                        .archive(tool_name, &id, cog_core::CONTENT_TYPE_JSON, text)
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

    /// 一次运行的开头：清空这次运行的状态，并在总线上宣布开跑。
    ///
    /// 任务的一次尝试（[`Self::run_turns`]）与调用方给好历史的一段对话
    /// （[`Self::run_conversation`]）都要这一下。放在一处，两条入口就不会
    /// 用两种方式声明同一次开始；而循环本身同在一处，两条入口也就不会跑出
    /// 两种工具行为。
    async fn begin_run(&mut self, run_task_id: Option<&str>) -> SFResult<()> {
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
        .await
    }

    /// The run itself, from the first turn to its answer.
    async fn run_turns(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
        run_task_id: Option<&str>,
    ) -> SFResult<serde_json::Value> {
        self.begin_run(run_task_id).await?;

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
            self.context.merge_head_system(&contract);
        }
        // 平台对每个智能体说的同一段话排在具体指示前面：先是谁都不变的那些
        // （这是什么地方、事实从哪来），再是这件活自己的要求。
        self.absorb_base_instruction();
        // The varying half is rendered request-first, so the attempt counter
        // that used to open it does not sit in front of the one block every
        // attempt of a task shares. See `render_varying_half`.
        //
        // 同一次任务的第二个 attempt 起，请求那一块与对话里已有的逐字节相同。
        // 再发一遍不加任何模型能读到的新东西，却要按整块重新付一次钱——测过
        // 的那段流量里，每轮新增字节有五分之一是这些重复的请求块。所以只在
        // 投影里找不到它时整块发；找得到就只发这一次尝试新增的那一半。
        // 看的是 `messages()` 而不是 `history`：被裁掉的请求模型这一轮确实读
        // 不到，那时必须整块重发。
        let repeated_request = request_members(&payload).is_some_and(|sent| {
            self.context
                .messages()
                .iter()
                .any(|m| m.content().contains(&sent))
        });
        // 附件先取出来，渲染的文档里就不能再有它们：留在文档里，同一段字节会
        // 作为 base64 文本再进一次提示词，模型读不懂它，钱还是照付。
        let (media, payload) = take_media(payload);
        let rendered = repeated_request
            .then(|| render_varying_half_after_request(&payload))
            .flatten()
            .unwrap_or_else(|| cog_core::contract::prompt::render_varying_half(&payload));
        if media.is_empty() {
            self.context.add_message(Message::user(rendered));
        } else {
            // 截图与录屏要作为媒体块交给模型，它才看得见——这是通用循环的
            // 能力，不该只有某一条专用判定路径有。
            let mut blocks = Vec::with_capacity(media.len() + 1);
            blocks.push(cog_core::ContentBlock::text(rendered));
            blocks.extend(media);
            self.context.add_message(Message::user_blocks(blocks));
        }

        self.run_loop(llm, run_task_id).await
    }

    /// 一段运行的主体：从第一条回合到三个出口之一（交付、预算用尽、被规则拦下）。
    ///
    /// 与任务输入那一段分开，不是给对话开一条新路：输入怎么来是两条入口唯一的
    /// 差别，进来之后怎样调工具、怎样过守卫、怎样记预算与结局，只有这一份。
    async fn run_loop(
        &mut self,
        llm: &dyn cog_core::LlmClient,
        run_task_id: Option<&str>,
    ) -> SFResult<serde_json::Value> {
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
        // How long it has been since the checkout changed. Tracked for the whole
        // run, like the refusals above: a run that reads for ten calls, writes,
        // reads for ten more, and writes again is working, and the note is for
        // the run that has stopped writing altogether.
        let mut progress = CheckoutProgress::default();
        // What this run has already handed the model. Kept here rather than read
        // out of the context window, because the window drops its oldest turns
        // when it fills and that is exactly when a repeat starts looking like a
        // good idea. See `DeliveredResults`.
        let mut delivered = DeliveredResults::default();

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

                progress.note_call(&tc.name, &tc.arguments);
                observations.push(observation.clone());
                tool_result_messages.push(
                    self.tool_result_message(
                        run_task_id,
                        iteration,
                        frame as u32,
                        &tc.id,
                        &tc.name,
                        &observation.to_string(),
                        &mut delivered,
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
            // After the results, so the note is the last thing read before the
            // next turn: a stretch of reads with nothing written is exactly the
            // transcript the model would otherwise take as evidence that it is
            // getting somewhere.
            if let Some(note) = progress.take_note() {
                self.context.add_message(Message::user(note));
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
        let result = if self.reply_is_the_result {
            // The reply is the answer. It was already through the output gate
            // above; nothing downstream reads a field out of it, and the reader
            // is the person in front of the screen. The shell some models wrap
            // it in is not part of the sentence they said, so it comes off
            // here, once, rather than in every reader.
            serde_json::json!({
                "status": CONVERSATION_REPLY_STATUS,
                "reply": plain_answer(&draft),
            })
        } else {
            match tokio::time::timeout(RESULT_BUILD_TIMEOUT, async { self.build_result(&draft) })
                .await
            {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    let err = SFError::Agent(format!(
                        "AgentRuntime::build_result timed out after {}s for agent {}",
                        RESULT_BUILD_TIMEOUT.as_secs(),
                        self.config.agent_id
                    ));
                    tracing::warn!(agent_id = %self.config.agent_id, error = %err, "AgentRuntime build_result timeout");
                    return Err(err);
                }
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

        // 这一路每一轮都重发整段历史：工具定义、system、以及上一次的每一步。前面
        // 那一大截逐字节没变，不向上游要缓存就等于按原价把它重买一遍——2026-10-26
        // 实测同一批提示词在不开缓存断点的协议面上只命中 3.6%，在同一天的兼容面上
        // 是 50%～67%。短窗就够：一轮之内连续重发，间隔是秒级，不是小时级。
        let options = cog_core::ChatOptions {
            tools: tool_defs,
            cache_retention: cog_core::CacheRetention::Short,
            ..Default::default()
        }
        .with_actor(&self.actor_label());
        let options = self.stamp_task(options);

        // 同一份请求在上游自报的等待之内只发一次。上面重放它的那些层（pipeline
        // 的 attempt、ralph 的下一轮）拿不到这一层发过的字节，所以它们以为自己在
        // 重试，实际上是在同一个窗口里把同一个问题再买一遍，而答案只可能是同一句
        // 拒绝。判据是整份请求的摘要：换了一份请求就是换了一个问题，那是该发出去的。
        let request = request_digest(&messages, &options);
        if let Some((cause, refusal, remaining)) =
            crate::agent_kernel::replay_guard::suppressed(&request, chrono::Utc::now())
        {
            tracing::warn!(
                agent_id = %self.config.agent_id,
                wait_secs = remaining,
                "the upstream refused this exact request and named when to ask again; not sending it again before then"
            );
            return Err(SFError::upstream_refused_after(
                cause,
                refusal,
                Some(remaining as u64),
            ));
        }

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
            Ok(Err(e)) => {
                self.remember_refusal(&request, &e);
                return Err(e);
            }
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
            let refusal = match response.upstream_failure {
                Some(cause) => SFError::upstream_refused_after(
                    cause,
                    format!("LLM stream error: {error}"),
                    response.retry_after_secs,
                ),
                None => SFError::Agent(format!("LLM stream error: {error}")),
            };
            self.remember_refusal(&request, &refusal);
            return Err(refusal);
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

        // An answer retires the guard: whatever was refused before, this request
        // is now known to be answerable.
        crate::agent_kernel::replay_guard::forget(&request);

        Ok(assistant_msg)
    }

    /// Hold a refusal long enough that the same bytes are not re-sent inside the
    /// wait the upstream named; see [`crate::agent_kernel::replay_guard`].
    ///
    /// Only a refusal that states a wait is remembered. A timeout, a stall or an
    /// untyped error says nothing about when the same request would work, and
    /// remembering those would turn a transient blip into a silence — the guard
    /// would hold a request the upstream is perfectly willing to answer.
    fn remember_refusal(&mut self, request: &str, error: &SFError) {
        let (Some(cause), Some(secs)) = (error.upstream_failure(), error.retry_after_secs()) else {
            return;
        };
        crate::agent_kernel::replay_guard::remember(
            request,
            cause,
            secs,
            error.to_string(),
            chrono::Utc::now(),
        );
    }

    /// 这一趟最后交付什么。
    ///
    /// 只有两种出口：想里带着 JSON 交付物，就用它；没有，那句想本身就是交付物。
    /// 这里的形状曾经还有一个出口——再问一次模型把想改写成 JSON——那条路买不到
    /// 结构，只买到一个回合的等待和一句话的转述，已经去掉。
    fn build_result(&mut self, thought: &str) -> SFResult<serde_json::Value> {
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

        // 想里没有可交付的 JSON：那句想就是答案，不再为它买第二次模型调用。
        //
        // 这里原先会再问一次模型「把上面这段想法改写成 JSON」。那一次调用买不到
        // 任何结构——它没有 schema，返回的键由模型自己起，线上实测回来的是
        // `{"role":"employee","thought":"…"}`，也就是同一句话换了个壳——代价却是
        // 又等一个回合、又记一次账。与人对一句话，读者看见的还因此不是它说的
        // 那句，而是模型对自己那句话的转述。
        //
        // 没有结构的答案下游本来就会读：`{ "result": thought }` 正是那次调用失败
        // 或超时时一直在用的形状（`try_extract_change_artifact` 就按它取产出），
        // 所以去掉这一次调用不新增一种结局，只是把最常见的那条路走成它一直在用的
        // 兜底。
        tracing::info!(
            agent_id = %self.config.agent_id,
            thought_len = %thought.len(),
            "AgentRuntime build_result has no JSON deliverable; the thought is the result"
        );
        Ok(serde_json::json!({ "result": thought }))
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

    /// 报一次「这一跑以错误收场」。
    ///
    /// 执行者面上那一格读的就是这条帧：它把「这个人刚才报错了」写在名单上。
    /// 在那之前，这条帧在整棵树里没有一个生产者——失败只进日志和任务行，名单上
    /// 一格都不变，于是「执行者一个接一个失败」与「平台很安静」在屏幕上同形。
    ///
    /// 分类只用手上已有的判据，不另写一张 21 个变体的对照表：那一张表是 core 的
    /// 知识，抄一份到 agent 里，两边迟早各说各的。四个类各对应一种处置，
    /// 这正是读者要分的：环境（等一会儿）、规则拒绝（换个做法）、输入本身不合法
    /// （改输入）、其余（我们的缺陷）。
    ///
    /// 只报一次：一次运行只有一个结局，而这条帧是从那个唯一的出口发出去的，
    /// 没有第二个地方会替同一跑再发一条。
    async fn note_run_failure(&self, error: &SFError) {
        let (error_code, severity) = failure_class(error);
        let mut details = error.to_string();
        // 这一帧没有任务字段，而「谁失败了」在名单之外还要落到哪条活上。任务
        // 身份挂在正文里，读的人看得见，也没有谁去解析它。
        if let Some(task_id) = self.run_task_id.as_ref() {
            details.push_str(&format!(" (task {task_id})"));
        }
        let event = AgentEvent::AgentError {
            agent_id: self.config.agent_id.clone(),
            error_code: error_code.to_string(),
            severity,
            details,
            crew_id: self.config.crew_id.clone(),
            squad_id: self.config.squad_id.clone(),
            timestamp: chrono::Utc::now(),
        };
        if let Err(e) = self.emit_event(event).await {
            tracing::warn!(
                agent_id = %self.config.agent_id,
                error = %e,
                "run failure could not be announced on the bus"
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

/// 一次失败的结局该怎么写进执行者面：一个机器读的类名，一个给人看的严重度。
///
/// 四个类用的是 core 已经定好的类型化判据（环境 / 规则拒绝 / 输入不合法 / 其余），
/// 不按错误文本猜：文本随上游改写，而这里的分类要决定读者先看哪一行。规则拒绝
/// 是**判决**不是故障——同一条调用再试一次还是同一个答复，把它写成 error 会让
/// 「守住了」在名单上与「坏了」同形。
fn failure_class(error: &SFError) -> (&'static str, cog_core::ErrorSeverity) {
    use cog_core::ErrorSeverity;
    match error {
        SFError::Blocked { .. } => ("guardrail_blocked", ErrorSeverity::Warning),
        other if other.is_permanent_rejection() => ("input_rejected", ErrorSeverity::Warning),
        other if other.is_environment_failure() => ("environment", ErrorSeverity::Critical),
        _ => ("internal", ErrorSeverity::Critical),
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
            // 这一种到今天仍然按自定义名字写盘：一次资源改动没有 agent、没有 task，
            // `WalEventType` 里也没有对应变体。名字不在这张表上，写进去的每一行
            // 都会在重放时被当成坏记录跳过——巡检看不见「这个前缀什么时候被改过」。
            "resource_changed" => "resource_changed",
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

    /// What a checkpoint writes, a restore reads back: the state, the role and
    /// the agent id.
    ///
    /// The writer builds a JSON object and the reader asks for each key with
    /// `.get(..)`; a key renamed on one side alone leaves `.get` finding nothing,
    /// and `restore` treats every one of these as optional -- so the agent comes
    /// back with the values it was configured with, which is exactly what a
    /// snapshot that never carried them would look like.
    #[test]
    fn a_checkpoint_restores_what_it_recorded() {
        let config = RuntimeConfig {
            agent_id: "before-the-snapshot".into(),
            role: "reviewer".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let mut agent_loop = AgentRuntime::new(config, tx);
        agent_loop.state = RuntimeState::Observing;
        let snapshot = agent_loop.checkpoint("trip").expect("checkpoint");

        let other = RuntimeConfig {
            agent_id: "after-the-snapshot".into(),
            role: "planner".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let mut restored = AgentRuntime::new(other, tx);
        restored.restore(&snapshot).expect("restore");

        assert_eq!(restored.state, RuntimeState::Observing);
        assert_eq!(restored.role(), "reviewer");
        assert_eq!(restored.config.agent_id, "before-the-snapshot");
    }

    /// A state word is spelled once: what `Display` prints is what `as_str`
    /// returns, and `parse` reads it back to the same state.
    ///
    /// These were two hand-written `match` blocks -- one in `Display`, one inline
    /// in `restore` -- and the compiler compares neither. Renaming a word on one
    /// side alone compiled, and `restore` then matched nothing and quietly came
    /// back idle, which is indistinguishable from an agent that was idle.
    #[test]
    fn every_state_word_survives_the_trip_through_a_checkpoint() {
        for state in RuntimeState::ALL {
            assert_eq!(state.to_string(), state.as_str());
            assert_eq!(RuntimeState::parse(&state.to_string()), Some(state));
        }
        assert_eq!(RuntimeState::parse("no-such-state"), None);
    }

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
            max_iterations: Some(42),
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
            max_iterations: Some(3),
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
            max_iterations: Some(5),
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
        // The replay guard is process-wide and keyed by the bytes of the request:
        // two tests sending the same empty conversation are one request, and
        // whichever of them was refused first would answer the other. A prompt of
        // its own makes each test a different request.
        runtime.context.add_message(Message::user("refused-typed"));
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
        // Same reason as the typed case above: without a prompt of its own this
        // test would be the same request as any other empty conversation, and a
        // refusal another test remembered first would be read as this call's
        // type -- the exact defect this test exists to rule out.
        runtime
            .context
            .add_message(Message::user("refused-untyped"));
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
        logs: std::sync::Mutex<Vec<(String, cog_core::LogEntry)>>,
    }

    impl CensusSpy {
        fn rows(&self) -> Vec<cog_core::TaskMetrics> {
            self.rows.lock().unwrap().clone()
        }

        fn logs(&self) -> Vec<(String, cog_core::LogEntry)> {
            self.logs.lock().unwrap().clone()
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

        async fn record_task_log(&self, task_id: &str, entry: cog_core::LogEntry) -> SFResult<()> {
            self.logs.lock().unwrap().push((task_id.to_string(), entry));
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
            text: "the answer, in prose",
            // No total reported, as some OpenAI-compatible upstreams do: the
            // halves still have to make it into the row.
            stream_usage: usage(300, 200, 0),
            chat_usage: usage(0, 0, 0),
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
        assert_eq!(row.prompt_tokens, 300);
        assert_eq!(row.completion_tokens, 200);
        assert_eq!(
            row.total_tokens, 500,
            "a call that reports only its halves still contributes its spend"
        );
        assert_eq!(row.iterations, 1);
        assert_eq!(
            runtime.run_usage.llm_calls, 1,
            "一句没有结构的回答只值一次调用：再问一次只为把它包进花括号的那次已经没有了"
        );
    }

    /// 上游说了「多久之后再来」的拒绝，在那一档之内不许把逐字节相同的请求
    /// 再发一遍。
    ///
    /// 重跑往往换一个新的 agent 实例（名字按 task+role 推，ralph 每轮铸一个新
    /// 任务 id），所以这里用两个独立实例发同一份请求：闸若只挂在实例上，第二个
    /// 实例照样会再买一次同一个拒绝；挂在进程上才对，因为说这句话的是上游。
    #[tokio::test]
    async fn an_identical_request_is_not_re_sent_inside_the_wait_the_upstream_named() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// 第一次流事件就是拒绝，并在终态响应里带上等待时长——这条正是
        /// 熔断拒绝的形状（503 + Retry-After）。
        struct RefusingLlm {
            calls: Arc<AtomicUsize>,
        }

        #[async_trait::async_trait]
        impl cog_core::LlmClient for RefusingLlm {
            async fn chat_stream(
                &self,
                _messages: &[Message],
                _options: &cog_core::ChatOptions,
            ) -> SFResult<cog_core::AssistantMessageEventStream> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                let (stream, producer) = cog_core::EventStream::with_capacity(4);
                tokio::spawn(async move {
                    let mut producer = producer;
                    let _ = producer
                        .push(AssistantMessageEvent::Error {
                            reason: cog_core::StopReason::Error,
                            error: Message::assistant_text("the pool has no member for this face"),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                    let response = cog_core::ChatResponse {
                        upstream_failure: Some(
                            cog_core::contract::llm::UpstreamFailure::RateLimited,
                        ),
                        retry_after_secs: Some(600),
                        ..Default::default()
                    };
                    producer.end(response);
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

        let calls = Arc::new(AtomicUsize::new(0));
        let llm = RefusingLlm {
            calls: calls.clone(),
        };
        // 内容对每个用例唯一：这道闸是进程级的，两条用例用同一份请求会互相看见。
        let input =
            serde_json::json!({"task": "replay-guard: an identical request is not re-sent"});

        let mut first = census_runtime("replay-guard-first");
        first
            .run_scoped(input.clone(), &llm, Some("task-replay"))
            .await
            .expect_err("the refusal propagates");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut second = census_runtime("replay-guard-second");
        let err = second
            .run_scoped(input, &llm, Some("task-replay"))
            .await
            .expect_err("a replay is refused with the same failure");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the second send is a replay, not a retry: the upstream named when to ask again"
        );
        assert!(
            err.retry_after_secs().is_some(),
            "the wait has to survive the suppression, or the layer above re-derives one: {err}"
        );
    }

    /// A run that ends on an error says so once on the bus, and says which task
    /// it was working on.
    ///
    /// Without this the roster is a table of agents that never look wrong: a
    /// failure reached the log and the task row, never the screen, so a fleet
    /// failing one agent after another and a quiet platform drew the same
    /// picture. The frame is the only place that cell can come from.
    #[tokio::test]
    async fn a_failed_run_is_announced_on_the_bus_exactly_once() {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        let config = RuntimeConfig {
            agent_id: "failing".into(),
            max_iterations: 1,
            think_stall_timeout_secs: 5,
            final_draft_timeout_secs: 300,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);
        let llm = CensusLlm {
            text: "",
            stream_usage: cog_core::Usage::default(),
            chat_usage: cog_core::Usage::default(),
            error: true,
        };

        runtime
            .run_scoped(serde_json::json!({"task": "x"}), &llm, Some("task-9"))
            .await
            .expect_err("the stream error propagates");

        let mut failures = Vec::new();
        while let Ok(event) = rx.try_recv() {
            if matches!(event, AgentEvent::AgentError { .. }) {
                failures.push(event);
            }
        }
        assert_eq!(failures.len(), 1, "one run has one ending: one report");
        match &failures[0] {
            AgentEvent::AgentError {
                agent_id,
                error_code,
                severity,
                details,
                ..
            } => {
                assert_eq!(agent_id, "failing");
                // 上游没给类型化原因时，这一跑归「我们的缺陷」那一类，不是环境类：
                // 把说不清原因的失败记成外部故障，等于替上游免责。
                assert_eq!(*error_code, "internal");
                assert_eq!(*severity, cog_core::ErrorSeverity::Critical);
                assert!(
                    details.contains("task task-9"),
                    "这条帧要说明是哪条活失败了，实际是：{details}"
                );
            }
            other => panic!("the failure was announced as something else: {other:?}"),
        }
    }

    /// A guardrail refusal is a verdict, not a fault: the same call would be
    /// refused the same way, so it must not be announced as an error.
    #[test]
    fn a_guardrail_refusal_is_announced_as_a_warning() {
        let (code, severity) = failure_class(&SFError::Blocked {
            tool: "write_file".into(),
            reason: "path outside the worktree".into(),
            rule: "write-scope".into(),
        });
        assert_eq!(code, "guardrail_blocked");
        assert_eq!(severity, cog_core::ErrorSeverity::Warning);

        // 环境类与内容类分开：前者等一会儿再试，后者改输入。
        let (code, severity) = failure_class(&SFError::ResourceExhausted("no build slot".into()));
        assert_eq!(code, "environment");
        assert_eq!(severity, cog_core::ErrorSeverity::Critical);
        let (code, _) = failure_class(&SFError::PayloadTooLarge {
            size: 9,
            limit: Some(8),
        });
        assert_eq!(code, "input_rejected");
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

    /// A finished run leaves one ending line where `/api/v1/tasks/{id}/logs`
    /// reads it, keyed the same way its census row is.
    ///
    /// Before this the write side of that route did not exist in the contract at
    /// all: both backends carried an inherent `record_log` and nothing in the
    /// tree called it, so the route answered `[]` for every task ever run. An
    /// empty list from a route with a reader reads as "this task logged
    /// nothing", which is a claim about the task; the truth was a claim about
    /// the wiring.
    #[tokio::test]
    async fn a_finished_run_leaves_one_log_line_under_its_task() {
        let spy = Arc::new(CensusSpy::default());
        let llm = CensusLlm {
            text: r#"{"result":"ok"}"#,
            stream_usage: usage(10, 10, 20),
            chat_usage: cog_core::Usage::default(),
            error: false,
        };
        let mut runtime = census_runtime("census-log").with_observability(spy.clone());

        runtime
            .run_scoped(
                serde_json::json!({"task": "census"}),
                &llm,
                Some("task-log"),
            )
            .await
            .expect("the run delivers");

        let logs = spy.logs();
        assert_eq!(logs.len(), 1, "one run leaves one ending line");
        let (task_id, entry) = &logs[0];
        assert_eq!(
            task_id, "task-log",
            "the line is keyed the way the census row is, or a per-task reader finds neither"
        );
        assert_eq!(entry.source, "agent_kernel");
        assert!(
            entry.message.contains("Delivered"),
            "the line has to name how the run ended, actual: {}",
            entry.message
        );
        assert_eq!(entry.metadata["total_tokens"], 20);
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

    /// 材料里带的截图要作为媒体块递到模型面前，而不是当作 base64 文本。
    ///
    /// 判据看的是模型这一侧真正收到了什么：一个 `Media` 块带着原样的字节与
    /// MIME，而用户消息的文字里既没有那段 base64、也没有 `media` 这个键。
    /// 只断言「跑通了」的话，把附件塞回文档里照样绿——而那正是被删掉的那种做法。
    #[tokio::test]
    async fn an_attachment_reaches_the_model_as_a_media_block() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("media-blocks");
        runtime
            .run(
                serde_json::json!({
                    "task": {"id": "t1"},
                    "media": [
                        {"data_base64": "iVBORw0KGgo=", "mime_type": "image/png"},
                        {"data_base64": "", "mime_type": "image/png"},
                    ],
                }),
                &llm,
            )
            .await
            .expect("the run delivers");

        let calls = llm.calls();
        let user = calls[0]
            .iter()
            .rev()
            .find(|m| matches!(m, Message::User { .. }))
            .expect("a user message");
        let Message::User {
            content: blocks, ..
        } = user
        else {
            unreachable!("the message was matched as a user message")
        };
        let media: Vec<_> = blocks.iter().filter(|b| b.is_media()).collect();
        assert_eq!(
            media.len(),
            1,
            "one attachment, and the empty one is not a block"
        );
        let cog_core::ContentBlock::Media { data, mime_type } = media[0] else {
            unreachable!()
        };
        assert_eq!(data, "iVBORw0KGgo=");
        assert_eq!(mime_type, "image/png");
        assert!(
            !user.content().contains("iVBORw0KGgo="),
            "字节只能以媒体块的形式出现一次，不能同时作为文本再付一次钱"
        );
    }

    /// 没有附件时文档原样渲染：一条纯文本用户消息，不多一个块。
    #[test]
    fn a_document_without_attachments_renders_as_text_alone() {
        let (media, payload) = take_media(serde_json::json!({"task": {"id": "t1"}}));
        assert!(media.is_empty());
        assert_eq!(payload, serde_json::json!({"task": {"id": "t1"}}));

        // 空数组也算没有附件，但键本身仍然要被取走。
        let (media, payload) = take_media(serde_json::json!({"task": 1, "media": []}));
        assert!(media.is_empty());
        assert!(payload.get("media").is_none());
    }

    /// 请求块的两种渲染必须与契约自己那一份逐字节对得上。
    ///
    /// 这里拿 `request_members` 的串去比对对话里已经有的内容，再决定还发不发
    /// 这一块：这个串与契约真正渲染出去的那一段差一个字节，判断就会反过来——
    /// 要么每次都重发（等于没改），要么该发的时候不发（这一轮没有任务可做）。
    /// 所以成员由 `REQUEST_KEYS` 定、字节由契约的渲染函数校验，两边不许各写一份。
    #[test]
    fn the_request_members_are_the_bytes_the_contract_renders() {
        let payload = serde_json::json!({"task": {"id": "t1"}, "context": {"attempt": 2}});
        let members = request_members(&payload).expect("the request block");
        assert!(
            cog_core::contract::prompt::render_varying_half(&payload)
                .starts_with(&format!("{{{members},")),
            "请求块必须原样出现在契约渲染结果的开头"
        );
        assert_eq!(
            render_varying_half_after_request(&payload).as_deref(),
            Some("{\"context\":{\"attempt\":2}}")
        );
    }

    /// 只有请求、没有别的：不剩任何东西可发。
    #[test]
    fn a_payload_with_only_the_request_has_nothing_left_to_send() {
        assert!(
            render_varying_half_after_request(&serde_json::json!({"task": {"id": "t1"}})).is_none(),
            "空消息就是一次什么都没说的回合"
        );
    }

    /// 同一次任务的第二个 attempt 不再把请求整块重发：它已经在对话里。
    ///
    /// 重复的那一块对模型没有新东西，却按整块的字节数再付一次钱；测过的
    /// 那段流量里，每轮新增的字节有五分之一就是这些重复的请求块。第一个
    /// attempt 仍以请求开头（那时对话里还没有它）。
    #[tokio::test]
    async fn a_second_attempt_does_not_resend_the_request_block() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("contract-repeat");
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
        assert!(
            first.starts_with("{\"task\":"),
            "第一个 attempt 的请求必须开头就是它：{first}"
        );
        assert!(
            !second.contains("\"task\":"),
            "请求已经在对话里，再发一遍就是为同一段字节付第二次钱：{second}"
        );
        assert!(
            second.contains("\"attempt\":2"),
            "这一次尝试新增的那一半必须还在：{second}"
        );
    }

    /// 换了任务的 attempt 必须整块发：那不是重复，是模型还没读过的请求。
    #[tokio::test]
    async fn a_different_task_is_sent_in_full() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("contract-new-task");
        runtime
            .run(contract_input(1, "Emit JSON only"), &llm)
            .await
            .expect("the run delivers");
        runtime
            .run(
                serde_json::json!({
                    cog_core::contract::prompt::PROMPT_CONTRACT_KEY: {"instructions": "Emit JSON only"},
                    "task": {"id": "t2"},
                    "context": {"attempt": 2},
                }),
                &llm,
            )
            .await
            .expect("the run delivers");

        let calls = llm.calls();
        let second = calls[1]
            .iter()
            .rev()
            .find(|m| matches!(m, Message::User { .. }))
            .expect("a user message")
            .content();
        assert!(
            second.contains("\"id\":\"t2\""),
            "换了的请求必须整块出现，否则这一轮就没有任务可做：{second}"
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

    /// 想里没有 JSON 的时候，那句想就是答案：不再为它买第二次模型调用。
    ///
    /// 这里曾经会再问一次模型「把上面这段想法改写成 JSON」。那一次调用没有
    /// schema，回来的键由模型自己起——线上看到的就是同一句话换了个壳
    /// （`{"role":"employee","thought":"…"}`）——代价却是一整个回合的等待和一次
    /// 计费，而读者看见的还不是它说的那句。
    ///
    /// 判的是两次读数：模型只被调用一次；交付物就是它说的那句。
    #[tokio::test]
    async fn a_thought_that_is_not_json_is_delivered_without_a_second_call() {
        let said = "变更账上没有未了结的那一条，我读过三本账。";
        let llm = ScriptedReplyLlm::new(&[said]);
        let (tx, _rx) = mpsc::channel(8);
        let mut runtime = AgentRuntime::new(
            RuntimeConfig {
                agent_id: "plain-answer".into(),
                role: "employee".into(),
                ..Default::default()
            },
            tx,
        );
        let result = runtime
            .run(serde_json::json!({ "task": "answer" }), &llm)
            .await
            .expect("一句没有结构的回答也是一句回答");
        assert_eq!(
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "没有结构的回答不该再买一次改写：第二次调用换不到任何字段，只换到一回合的等待"
        );
        assert_eq!(
            result,
            serde_json::json!({ "result": said }),
            "交付物就是它说的那句，不是模型对自己那句话的转述"
        );
    }

    /// 界面上那一轮交付的是说给人听的那一句，不是包着它的壳。
    ///
    /// 实测 2026-11-06：一位员工这样回答 `{"answer":"…"}`，屏幕上读到的是花括号，
    /// 从回信地址收到它的同事读到的是第二层转义（`reply` 里又是一段 JSON）。
    /// 壳只在交付那一刻拆一次，所有读者读到的就是同一句话。
    #[tokio::test]
    async fn a_conversation_delivers_the_sentence_out_of_its_json_shell() {
        let said = "告警亮着一条，是 orion 的心跳。";
        let shelled = format!(r#"{{"answer":"{said}"}}"#);
        let llm = ScriptedReplyLlm::new(&[shelled.as_str()]);
        let (tx, _rx) = mpsc::channel(8);
        let mut runtime = AgentRuntime::new(
            RuntimeConfig {
                agent_id: "shelled-answer".into(),
                role: "employee".into(),
                ..Default::default()
            },
            tx,
        );
        let result = runtime
            .run_conversation(vec![Message::user("现在有告警吗")], &llm)
            .await
            .expect("一轮对话");
        assert_eq!(
            result["reply"],
            serde_json::json!(said),
            "交付出去的还是那个壳：屏幕上读到花括号，同事收到的是转义的 JSON"
        );
    }

    /// 一段真的以 JSON 为内容的回答不会被当成壳拆掉。
    #[test]
    fn a_deliverable_is_not_mistaken_for_a_shell() {
        let deliverable = r#"{"version":2,"enabled":true}"#;
        assert_eq!(plain_answer(deliverable), deliverable, "两个键的对象不是壳");
        let two_fields = r#"{"role":"employee","thought":"一句话"}"#;
        assert_eq!(
            plain_answer(two_fields),
            two_fields,
            "多键的对象不拆：拆了只剩其中一个键"
        );
        assert_eq!(plain_answer(r#"{"reply":"一句话"}"#), "一句话");
        assert_eq!(plain_answer("就是一句话"), "就是一句话");
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
            max_iterations: Some(1),
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

    /// 同一份工具输出在一趟运行里只付一次钱。
    ///
    /// 模型会重读它已经读过的那个文件，而同一次读的答案一直留在对话里：
    /// 实测 2026-10-31 至 2026-11-02 的 1,018 次成功调用里，请求内重复的工具
    /// 输出占全部工具输出的 3%，其中一件 generator 请求被三次相同的 read_file
    /// 撑到 64% 重复。重复的那一份换成指针——配对保住，字节不再付第二次。
    /// 一条声明了这个工具调用的 assistant 消息：没有它，工具结果在进上下文
    /// 时就是孤儿，会被剥掉，测的就不再是重复这件事。
    fn asks_for(tool_call_id: &str) -> Message {
        Message::Assistant {
            content: vec![ContentBlock::tool_call(
                tool_call_id,
                "read_file",
                json!({ "path": "crates/a.rs" }),
            )],
            tool_calls: None,
            usage: None,
            timestamp: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_tool_result_the_run_already_carried_is_not_paid_for_twice() {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let mut runtime = AgentRuntime::new(RuntimeConfig::default(), tx);

        let big = format!(
            "{{\"content\":\"{}\"}}",
            "x".repeat(REPEATED_TOOL_RESULT_MIN_CHARS)
        );
        let mut delivered = DeliveredResults::default();
        let first = runtime
            .tool_result_message(None, 0, 0, "call-1", "read_file", &big, &mut delivered)
            .await;
        assert_eq!(first.content(), big, "第一次读到的东西要原样交给模型");
        runtime.context.add_message(asks_for("call-1"));
        runtime.context.add_message(first);

        let second = runtime
            .tool_result_message(None, 0, 1, "call-2", "read_file", &big, &mut delivered)
            .await;
        assert_ne!(
            second.content(),
            big,
            "同一份输出第二次出现时不该再发一遍：那 {} 个字节已经在上一轮里了",
            big.len()
        );
        let stub: serde_json::Value =
            serde_json::from_str(&second.content()).expect("指针仍然是一份工具结果");
        assert_eq!(stub["deduplicated"], serde_json::json!(true));
        assert!(
            second.content().len() * 2 < big.len(),
            "指针要是指得比它替代的那份的一半还长，就等于没省"
        );

        // 反向对照：换一份内容，它照原样发出去。
        let other = format!(
            "{{\"content\":\"{}\"}}",
            "y".repeat(REPEATED_TOOL_RESULT_MIN_CHARS)
        );
        let third = runtime
            .tool_result_message(None, 0, 2, "call-3", "read_file", &other, &mut delivered)
            .await;
        assert_eq!(third.content(), other, "没重复的输出不许被当成重复");
    }

    /// 记录挂在运行上，不挂在窗口上：窗口把它裁掉之后，重问一次仍然是重复。
    ///
    /// 这是那条门真正会失效的地方。裁剪按轮次组从最前面丢，丢掉的正是模型
    /// 刚读回来的东西；窗口越满、越需要记得「这份已经给过」，而把记录从窗口
    /// 里读，恰好在那时什么都读不到。实测一轮生成（2026-11-06，
    /// `change-ledger-0ea2e24e8f45341e`）：360 次工具调用里 119 次是逐字重复，
    /// 286 次模型调用平均产出 108 个 token。
    #[tokio::test]
    async fn a_repeat_is_still_a_repeat_after_the_window_has_dropped_the_first_copy() {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        // 小窗口：几轮之后第一份结果必然被裁出投影。
        let config = RuntimeConfig {
            context_window_size: 600,
            ..RuntimeConfig::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);
        let mut delivered = DeliveredResults::default();

        let read = format!(
            "{{\"content\":\"{}\"}}",
            "x".repeat(REPEATED_TOOL_RESULT_MIN_CHARS)
        );
        let first = runtime
            .tool_result_message(None, 0, 0, "call-1", "read_file", &read, &mut delivered)
            .await;
        runtime.context.add_message(asks_for("call-1"));
        runtime.context.add_message(first);

        // 把它挤出投影：再灌几轮别的大结果，直到原文已经不在窗口里。
        for round in 1..40u32 {
            let filler = format!("{{\"content\":\"{}-{round}\"}}", "z".repeat(3_000));
            let msg = runtime
                .tool_result_message(
                    None,
                    round,
                    0,
                    &format!("filler-{round}"),
                    "run_command",
                    &filler,
                    &mut delivered,
                )
                .await;
            runtime
                .context
                .add_message(asks_for(&format!("filler-{round}")));
            runtime.context.add_message(msg);
            let still_there = runtime.context.messages().iter().any(|m| {
                matches!(m, Message::ToolResult { .. })
                    && m.content().starts_with("{\"content\":\"xxx")
            });
            if !still_there {
                break;
            }
        }
        assert!(
            !runtime
                .context
                .messages()
                .iter()
                .any(|m| matches!(m, Message::ToolResult { .. })
                    && m.content().starts_with("{\"content\":\"xxx")),
            "这一条要测的是「原文已经不在窗口里」之后的重复，否则它和上一条测的是同一件事"
        );

        let again = runtime
            .tool_result_message(None, 40, 0, "call-2", "read_file", &read, &mut delivered)
            .await;
        let stub: serde_json::Value =
            serde_json::from_str(&again.content()).expect("指针仍然是一份工具结果");
        assert_eq!(
            stub["deduplicated"],
            serde_json::json!(true),
            "原文被裁掉之后重问同一份输出，仍然是重复"
        );
        assert_eq!(stub["first_returned_at_turn"], serde_json::json!(0));
        assert_eq!(stub["times_asked"], serde_json::json!(2));
    }

    /// 超预算的结果也一样认得出来。
    ///
    /// 进入窗口的那一份会被裁，裁出来的标记里带着轮次号，所以同一份原始输出
    /// 每次被裁成的样子都不同。按窗口里那份比对，最大的结果永远比不上——
    /// 而它们正是每轮都要重发、最该被挡住的那一类。
    #[tokio::test]
    async fn an_oversized_result_is_still_recognised_when_it_comes_back() {
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            context_window_size: 600,
            ..RuntimeConfig::default()
        };
        let mut runtime = AgentRuntime::new(config, tx);
        let mut delivered = DeliveredResults::default();

        let huge = format!(
            "{{\"content\":\"{}\"}}",
            "q".repeat(REPEATED_TOOL_RESULT_MIN_CHARS * 4)
        );
        let first = runtime
            .tool_result_message(None, 0, 0, "call-1", "run_command", &huge, &mut delivered)
            .await;
        assert!(
            first.content().len() < huge.len(),
            "这一条要的是「原文被裁过」这个前提，没裁就测不到它"
        );
        runtime.context.add_message(asks_for("call-1"));
        runtime.context.add_message(first);

        let second = runtime
            .tool_result_message(None, 1, 0, "call-2", "run_command", &huge, &mut delivered)
            .await;
        let stub: serde_json::Value =
            serde_json::from_str(&second.content()).expect("指针仍然是一份工具结果");
        assert_eq!(
            stub["deduplicated"],
            serde_json::json!(true),
            "被裁过的结果换了轮次号，也仍然是同一份输出"
        );
    }

    fn read_call() -> serde_json::Value {
        serde_json::json!({ "command": "grep -rn foo crates/" })
    }

    fn write_call() -> serde_json::Value {
        serde_json::json!({ "command": "sed -i 's/a/b/' crates/x.rs" })
    }

    /// The reading this exists for: a run that searches and never writes is
    /// told so, twice at most, and a run that writes in between never hears it.
    #[test]
    fn a_stretch_of_searches_is_told_and_a_write_ends_it() {
        let mut progress = CheckoutProgress::default();
        for _ in 0..CALLS_WITHOUT_CHANGE - 1 {
            progress.note_call("run_command", &read_call());
            assert!(progress.take_note().is_none(), "一处还没到的一声都不该响");
        }
        progress.note_call("run_command", &read_call());
        assert!(progress.take_note().is_some(), "四十次只读之后必须说出来");
        assert!(progress.take_note().is_none(), "同一个数不该每轮都说一遍");

        // A write resets the stretch: the next forty reads earn their own note...
        progress.note_call("run_command", &write_call());
        for _ in 0..CALLS_WITHOUT_CHANGE - 1 {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_none());
        progress.note_call("run_command", &read_call());
        assert!(progress.take_note().is_some());

        // ...and one run hears it twice at most.
        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_none(), "一条运行里最多说两次");
    }

    /// A run that writes every few reads is working, however long it runs: the
    /// count is of calls since the checkout last changed, not of calls.
    #[test]
    fn a_run_that_writes_between_its_searches_is_never_told_it_is_not_moving() {
        let mut progress = CheckoutProgress::default();
        for _ in 0..20 {
            for _ in 0..CALLS_WITHOUT_CHANGE - 1 {
                progress.note_call("run_command", &read_call());
            }
            progress.note_call("run_command", &write_call());
            assert!(progress.take_note().is_none());
        }
    }

    /// Which calls count as changing the checkout. The write tool by its name;
    /// the shell by the form of the command, with the forms that throw bytes
    /// away, report instead of formatting, or only read, left out.
    #[test]
    fn the_calls_that_change_the_checkout_are_read_off_the_call() {
        let cases: &[(&str, serde_json::Value, bool)] = &[
            ("write_file", serde_json::json!({ "path": "a.rs" }), true),
            ("read_file", serde_json::json!({ "path": "a.rs" }), false),
            (
                "run_command",
                serde_json::json!({ "command": "grep -rn foo ." }),
                false,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "sed -n '1,40p' a.rs" }),
                false,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "cargo fmt --check" }),
                false,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "cargo check 2>&1 > /dev/null" }),
                false,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "cat > a.rs <<'EOF'\nx\nEOF" }),
                true,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "sed -i 's/a/b/' a.rs" }),
                true,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "cargo fmt --edition 2021 a.rs" }),
                true,
            ),
            (
                "run_command",
                serde_json::json!({ "command": "git add -N a.rs" }),
                true,
            ),
        ];
        for (tool, arguments, expected) in cases {
            assert_eq!(
                changes_the_checkout(tool, arguments),
                *expected,
                "{tool} {arguments} 该判成 {}",
                if *expected { "写" } else { "只读" }
            );
        }
    }
}
