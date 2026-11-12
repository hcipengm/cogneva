use cog_core::attempt_journal::{self, EffectClass, JournalEntry, JournalKind};
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

/// 一条还在跑的工具调用，多久把攒下的输出往外发一次。
///
/// 执行器交上来的切片大小不受我们控制：一条 `cargo build` 的进度可能几十毫秒
/// 一行，整份日志在十分钟里能到几万行。每一片发一帧的话，这场构建会在 WAL、
/// WebSocket 和界面上各变成几万条记录，而读的人要看的只是「它还在动、动到哪
/// 了」。攒到一个节拍再发，滚动的观感不变，帧数按节拍被削掉。这个数是给人眼
/// 看的：比它慢，界面看上去像卡住了；比它快，一屏之内叠着好几帧还没读完。
// cap: bound — 界面那一格的刷新节奏是人的阅读节奏：比这快，几帧挤在一起还没读完就滚过去了
const TOOL_UPDATE_FLUSH: Duration = Duration::from_millis(200);

/// 一帧进度最多带多少字符。
///
/// 界面上一条工具调用留的是一格，不是一份日志：那一格只留最后 8000 字符，超过
/// 这个量的帧到了那里也只剩下尾巴。在这里就裁，省下的是总线和 WAL 上的字节，
/// 读的人看到的东西一点不少。
// cap: bound — 与界面那一格的窗口同源：更长的帧到了读者那里一样只剩尾巴
const TOOL_UPDATE_CHARS: usize = 8_000;

/// 把一段新输出接在进度的尾巴上，超出这一帧的容量时丢掉最旧的那一段。
///
/// 撞到界时丢开头而不是丢结尾：读的人要的是「它刚说了什么」。开头早在上一个
/// 节拍就发出去过，把它留在这里只会让这一帧更重，而不会让谁多看到一个字。
fn note_tool_update(partial: &mut String, chunk: &str) {
    partial.push_str(chunk);
    if partial.len() > TOOL_UPDATE_CHARS {
        let mut cut = partial.len() - TOOL_UPDATE_CHARS;
        // 字符边界：一段 UTF-8 从中间切开，那条帧在界面上就是一个替换符
        while !partial.is_char_boundary(cut) {
            cut += 1;
        }
        partial.drain(..cut);
    }
}

/// 这条运行该不该被「一直在读、没动过树」收住。
///
/// 先得这件活自己声称要动这棵树：没有这份声明时，「工作树没动」不构成任何证据
/// ——评审、规划、一段对话本来就不该动它，拿这一条去收它们，等于把「它做了本职
/// 工作」读成「它没有进展」。声明随任务来（`Task::deliverable`），不随接手
/// 的人的角色来：同一个工程师换了件活，答案跟着那件活变。
///
/// 再加上已经提醒过两次、之后又走满一整段调用没写过一次，才算绕圈。判据里没有
/// 钟：一次跑得很久的调用只算一次，在干活的长活不会被这一条收走。
fn circling_unproductively(config: &cog_core::RuntimeConfig, progress: &CheckoutProgress) -> bool {
    config.declares_change() && progress.circling()
}

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
    /// 工具执行之前那一问由谁回答，以及这一轮处在哪种模式下。
    ///
    /// 没有配置回答者时不是「不问」，而是「要问的那一步拒绝」：见
    /// [`crate::permission::decide`]。默认模式是 `bypass`，也就是今天的行为——
    /// 不问、全放行——但这个选择有名字，因此会话上可读、可改。
    permission: Option<Arc<dyn crate::permission::PermissionDecider>>,
    permission_mode: crate::permission::PermissionMode,
    /// What the run in flight has been billed for so far. Reset by every
    /// [`Self::run_scoped`]; read when the run ends.
    run_usage: RunUsage,
    /// Turns the run in flight has actually started.
    run_iterations: u32,
    /// 这次尝试是从日志里接着跑的第几轮之后开始的。迭代预算从它之后算：
    /// 被杀掉之前花掉的轮次已经花过了，不该从这一次的额度里再扣一遍。
    resumed_turns: u32,
    /// How the run in flight ended. Reset by every [`Self::run_scoped`] and set
    /// by [`Self::note_run_ending`], so the run's single exit can report the
    /// skill the role was bound to with the same verdict the observable counted
    /// — the ending is decided at three different places, and only the exit is
    /// common to all of them.
    run_outcome: Option<crate::observable::RunOutcome>,
    /// 这一轮跑的轮次上限是谁给的（见 [`crate::observable::CeilingSource`]）。
    ///
    /// 在构造时定下，与 `config.max_iterations` 是同一判定的两个出口：一个数，
    /// 一个来源。运行结束时两样一起报给 observable，读者才能把「这一轮的上限
    /// 是量出来的还是操作者钉的」指出来，而不是只看到一个数。
    ceiling_source: crate::observable::CeilingSource,
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
    /// 上一次运行替哪件活干的（`begin_scoped_run` 收到的那一个）。
    ///
    /// 与上面那个不同：那个是「这一次运行」，运行结束就清；这个跨运行留着，
    /// 只回答一个问题——这一次的活还是不是上一次那一件。换了活的那一刻，上一件
    /// 活留在进程里的对话不再属于这一次：员工无状态，下一次从账与收件箱重建
    /// （见 `begin_scoped_run`）。
    last_run_task: Option<String>,
    /// 这个进程里已经跑过至少一次（`begin_scoped_run` 置位，永不回零）。
    ///
    /// 用来分开「换了另一件活」与「这是本进程的第一次」：两者在 `last_run_task`
    /// 上都是 `None -> Some`，但第一次要接着读上一次进程留下的尝试日志，换了活
    /// 的那一次不能读。
    run_seen: bool,
    /// 这一次运行不接日志续跑（由 `begin_scoped_run` 置位，`run_turns` 读掉即清）。
    resume_is_from_another_task: bool,
    /// 名册。有它才读得到自己那一行的工号与名字（见 `absorb_base_instruction`）。
    roster: Option<Arc<dyn cog_core::AgentRegistry>>,
    /// 这一位在人前的名字，`E0412 林清和`。每轮开头读一次名册，读到了就留着。
    display_name: Option<String>,
    /// 这一轮是否已经问过名册。每轮开头重置：上一轮没登记上的人，这一轮登记
    /// 之后应当读到自己的名字，而不是永远停在第一次的那个答案上。
    identity_looked_up: bool,
    /// 这一次运行写进系统前缀的那一行（id、名字、车道）。
    ///
    /// 留着它是因为重放闸要把它从请求摘要里去掉：上游拒的是这份请求，不是这
    /// 个人；换一个名字重问同一个问题，仍然是在同一个窗口里买同一句拒绝。
    identity_note: Option<String>,
    /// 上一轮并进系统前缀的那一段规则。账上规则集变了之后，靠它把旧的那段
    /// 拿掉（见 `absorb_principles`）。
    principles_note: Option<String>,
    /// 这一条循环自己的规则账。空着时用进程的那一本（见 `absorb_principles`）。
    principle_source: Option<Arc<dyn crate::principles::PrincipleSource>>,
}

/// The token census of one run: every billed LLM call the run made, folded into
/// running totals.
///
/// The totals live here rather than in the events stream because the response
/// carrying them is consumed and dropped at the end of the streaming call — the
/// assistant message handed back up has no token fields, so a run's spend was
/// visible only to the process-wide counters, never to the task that paid it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RunUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    /// Billed calls, including the deliverable-reformat fallback.
    pub llm_calls: u32,
    /// 最近一次调用带进模型的那一截提示词长度。它就是这一跑结束时上下文占了
    /// 多少：累加的总量回答「花了多少」，这一个回答「现在背着多重的历史」。
    pub last_prompt_tokens: u32,
    /// 各次调用按声明价目算出来的钱（美元）。一次都没有报出价目时是 `None`。
    ///
    /// 不能拿 `Some(0.0)` 代答：上游没声明价目时算出来的一律是 0.0，画在界面上
    /// 就是白纸黑字地说这一轮不要钱——而它只是没算。「没算」与「正好是零」在
    /// 读数上要分得开。
    pub cost_usd: Option<f64>,
    /// 最后一次答话的是哪一路模型。上游池里的成员可以互相顶替，所以它是每次
    /// 调用自己报回来的那个名字，不是配置里排第一的那个。
    pub model: Option<String>,
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
        self.last_prompt_tokens = usage.input;
        // 价目没声明时 `calculate_cost` 算出来的四个分量全是 0.0；任意一个非零
        // 才说明这一台真的报了价。加起来而不是只读 `total`：有一条上游把
        // `total` 留成 0 而分量有数。
        let billed = if usage.cost.total > 0.0 {
            usage.cost.total
        } else {
            usage.cost.input + usage.cost.output + usage.cost.cache_read + usage.cost.cache_write
        };
        if billed > 0.0 {
            *self.cost_usd.get_or_insert(0.0) += billed;
        }
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
fn request_digest(
    messages: &[Message],
    options: &cog_core::ChatOptions,
    own_line: Option<&str>,
) -> String {
    let mut hasher = Sha256::new();
    let mut put = |part: &[u8]| {
        hasher.update(part);
        hasher.update([0u8]);
    };
    for message in messages {
        match message {
            Message::System { content, .. } => {
                put(b"system");
                // 平台自己写进去的那一行（这位是谁、在哪条车道）不算「问的问题」。
                // 重跑经常换一个新名字——ralph 每轮铸一个新任务 id，名字跟着变
                // ——把名字算进摘要，同一份被拒的请求就再也不会撞上这道闸，而
                // 闸正是为这种重跑建的：上游拒的是这份请求，不是这个人。
                match own_line.filter(|line| content.contains(*line)) {
                    Some(line) => put(content.replace(line, "").as_bytes()),
                    None => put(content.as_bytes()),
                }
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

/// 流停顿判据收掉一条命时，错误文本以它开头。
///
/// 这条前缀有两个读者：读日志的人，和「最后追问」那条路径上的哨兵。哨兵要分
/// 「到点没给完」（上游卡住）与「问过了没答」（模型确实没东西写）——两者对读
/// 这条变更的人意思相反，处置也相反。让判词与判据共用这一份拼写：各写一遍时，
/// 措辞一动，分类就静默退化成「都算没答」，而那与「从来没判过」同形。
const STREAM_STALL_PREFIX: &str = "AgentRuntime::think_stream timed out: no LLM stream progress";

/// 这个错误是不是流停顿判据给出的。
///
/// 按类型分不了：判据交出去的是 [`SFError::Agent`]，与「模型说了句读不懂的
/// 话」同一个变体。所以分的依据是那条判词自己的前缀——它与写判词的地方共用
/// 一份常量，改写时两处一起改。
fn is_stream_stall(error: &SFError) -> bool {
    matches!(error, SFError::Agent(msg) if msg.starts_with(STREAM_STALL_PREFIX))
}

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
    /// 距上一次真的改动工作树过了多少次调用。
    ///
    /// 与 [`Self::calls_without_change`] 分开：那一个每说一次提醒就清零（它量的是
    /// 「这一声该不该响」），这一个只在真的改动时才清零。收住的时候要报的是后者
    /// ——一条半小时只打了三个慢调用的运行，「三次」是对的，「零次」是刚清过零。
    calls_since_change: u32,
    notes_given: u32,
}

impl CheckoutProgress {
    fn note_call(&mut self, tool: &str, arguments: &serde_json::Value) {
        if changes_the_checkout(tool, arguments) {
            self.calls_without_change = 0;
            self.calls_since_change = 0;
        } else {
            self.calls_without_change = self.calls_without_change.saturating_add(1);
            self.calls_since_change = self.calls_since_change.saturating_add(1);
        }
    }

    /// 这条运行是不是已经绕圈绕到该收住了。
    ///
    /// 与 [`Self::take_note`] 分工：提醒是给模型的，收住是给运行的。被提醒过两次
    /// 之后它仍然能自己走出来，所以收住多要一整段**调用次数**：两次提醒之后再走满
    /// [`CALLS_WITHOUT_CHANGE`] 次调用，一次都没改过工作树，才算。
    ///
    /// 判据是调用次数，不是钟。按钟判会误杀：一次长编译可以在不改工作树的情况下
    /// 跑满半小时，而它显然是在干活——按次判不会，那半小时只贡献一次调用。反过来
    /// 只凭最先那段次数就收也不够：四十次只读在一分钟内就能凑齐（模型一次能并发
    /// 发好几条），那样的运行不是迷路，是刚开始读，所以先说话，再等一段。
    fn circling(&self) -> bool {
        self.notes_given >= NOTHING_CHANGED_NOTES
            && self.calls_without_change >= CALLS_WITHOUT_CHANGE
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
    // A background command runs the same shell; it only answers sooner. Its
    // command is read the same way, because what it does to the checkout is
    // that command's business and not the handle's.
    if tool != "run_command" && tool != "run_command_background" {
        // 打补丁与提交都改树：前者的名字不像写文件，后者的名字里有 "git"，
        // 漏掉它们会让「正在原地打转」这句提示在最该出现的时候不出现。
        return matches!(
            tool,
            "write_file" | "edit_file" | "apply_patch" | "git_commit"
        );
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

    pub fn new(config: RuntimeConfig, event_tx: mpsc::Sender<AgentEvent>) -> Self {
        Self::with_observable(config, event_tx, &crate::observable::global_observable())
    }

    fn with_observable(
        mut config: RuntimeConfig,
        event_tx: mpsc::Sender<AgentEvent>,
        observable: &crate::observable::AgentObservable,
    ) -> Self {
        // 轮次上限只有两个来源：操作者在配置面上钉的数（覆盖），和实测出来的
        // 读数（接手这个人自己的交卷运行，还没有时才取这个进程的）。技能行不是
        // 第三个——那一行按角色名分发，同一件活换个标签接手就换一个天花板，而那
        // 正是「标签决定行为」。写成「没写就取 10」同样不行：2026-11-05 实测
        // generator 在第十轮带着一个没跑完的工具调用被砍，下一轮从头再来，同一条
        // CI 红修了六版。
        let (ceiling, ceiling_source) =
            observable.iteration_budget_decision_for(config.max_iterations, &config.agent_id);
        config.max_iterations = ceiling;

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
            permission: None,
            permission_mode: crate::permission::PermissionMode::Bypass,
            run_usage: RunUsage::default(),
            run_task_id: None,
            last_run_task: None,
            run_seen: false,
            resume_is_from_another_task: false,
            run_iterations: 0,
            resumed_turns: 0,
            run_outcome: None,
            ceiling_source,
            context_overrun_reported: false,
            reply_is_the_result: false,
            actor_label: None,
            call_owner: None,
            call_origin: None,
            roster: None,
            display_name: None,
            identity_looked_up: false,
            identity_note: None,
            principles_note: None,
            principle_source: None,
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

    /// Hand this loop its own rule account.
    ///
    /// The account is normally the process's (installed by the assembly root,
    /// see `cog_agent::install_principle_source`); a loop built with its own
    /// answers from that one instead. A test is the first caller that needs
    /// this: a process-wide account is shared by every test in the binary, and
    /// a rule installed to prove one run reads it would also change what the
    /// other runs read.
    pub fn with_principle_source(
        mut self,
        source: Arc<dyn crate::principles::PrincipleSource>,
    ) -> Self {
        self.principle_source = Some(source);
        self
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

    /// 把名册交给这一轮：它自己的工号与名字从那一行读，不从调用方抄。
    ///
    /// 名字只有一处派生（`EmployeeRecord::label`）。每个构造点各自去名册上
    /// 捞一遍、再各自传进来，漏掉一个构造点，那一种执行体就不知道自己的名字，
    /// 而漏掉是不会报错的——它只是回答「我叫 unknown」。交给运行时之后，凡是
    /// 登记过的执行体都在同一个地方读到同一个名字。
    pub fn with_roster(mut self, roster: Arc<dyn cog_core::AgentRegistry>) -> Self {
        self.roster = Some(roster);
        self
    }

    /// 这一位在人前的名字。名册不可达时的显式来源，也是测试的入口。
    pub fn with_display_name(mut self, name: impl Into<String>) -> Self {
        self.display_name = Some(name.into());
        self
    }

    /// 把这一轮能用的工具交给它：整张注册表，不按角色收窄。
    ///
    /// 手上有什么工具由**这个进程装了什么**决定——有没有沙盒、有没有消息队列、
    /// 有没有出门的客户端——不由「它是哪种手」决定。曾经这里按技能行上声明的
    /// `tools` 裁一刀：同一个进程里的两位对同一张注册表看到两套工具，而裁成哪一套
    /// 取决于注册表上一个自由标签。换个标签，同一件活就做不成了；而裁掉的那一项
    /// 不会报错，它只是「不存在」，模型于是学会了不去用它。
    pub fn with_tools(mut self, tools: ToolRegistry) -> Self {
        self.tools = tools;
        self
    }

    pub fn with_sandbox_backend(mut self, backend: Arc<dyn cog_core::SandboxBackend>) -> Self {
        self.sandbox_backend = Some(backend);
        self
    }

    /// 让这一轮在动手之前先问一句。
    ///
    /// 两样一起给，因为它们回答的是同一个问题：「谁说了算」。只给模式不给回答者，
    /// 要问的那一步会拒绝（[`crate::permission::decide`]），所以调用方必须清楚自己
    /// 选的是哪一种。
    pub fn with_permissions(
        mut self,
        decider: Option<Arc<dyn crate::permission::PermissionDecider>>,
        mode: crate::permission::PermissionMode,
    ) -> Self {
        self.permission = decider;
        self.permission_mode = mode;
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
        // 这一次运行在飞期间，停机的那一侧会等它走到回合边界（见
        // `crate::drain`）。守卫活着就算在飞，落盘在 `Drop` 里，panic 与
        // 提前返回都算结束——否则一次失败的运行会让关停永远等不到零。
        let _in_flight = crate::drain::enter_run();
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
        // 这一轮花了多少、谁答的、答了几回：循环知道这三件事，而读它的只有
        // 网关。趁 `run_usage` 还属于这一跑（下一次 `begin_run` 会清零）抄一份，
        // 挂在这一轮的交付物上带走。它是加法——`reply` 仍然在原来那个键上。
        let usage = self.run_usage.clone();
        let rounds = self.run_iterations;
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let mut outcome = self.finish_scoped_run(None, started, outcome).await;
        if let Some(object) = outcome
            .as_mut()
            .ok()
            .and_then(|value| value.as_object_mut())
        {
            object.insert(
                "usage".to_string(),
                serde_json::json!({
                    "input_tokens": usage.prompt_tokens,
                    "output_tokens": usage.completion_tokens,
                    "total_tokens": usage.total_tokens,
                    "rounds": rounds,
                    "context_tokens": usage.last_prompt_tokens,
                    "duration_ms": elapsed_ms,
                    "model": usage.model,
                    "cost_usd": usage.cost_usd,
                }),
            );
        }
        outcome
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
        self.absorb_base_instruction().await;
        self.run_loop(llm, None).await
    }

    /// 每个运行开头那段同一的话，来自部署的声明而不是代码里的常量。
    ///
    /// 两条入口都要它：一次任务尝试与一轮对话在内核里是同一条循环，只有输入
    /// 从哪儿来不同；一段话只给其中一条，就等于平台上又有了两类智能体。
    ///
    /// 后面再跟一行这一位自己的：叫什么、在哪个工作树里。平台那段话对所有人
    /// 都说同一句，这一行是唯一让模型知道「这些话是**我**在说」的地方——
    /// 没有它，被问「你是谁」只能去工具结果里猜，或者回一句「我是一个 AI 助手」。
    ///
    /// 两段之间还有账上此刻生效的规则（见 `crate::principles`）。它们不是角色
    /// 设定：同一次改写对所有人同时生效，不按这个人是谁挑。上一轮并进去的那
    /// 一段在规则集变了之后要拿掉——留着的话，这一轮读到的是一条已经撤回的
    /// 规则，和它现在的替代者并排放在同一段前缀里。
    async fn absorb_base_instruction(&mut self) {
        if let Some(text) = crate::agent_kernel::base_instruction::text() {
            self.context.merge_head_system(&text);
        }
        self.absorb_principles().await;
        self.look_up_own_name().await;
        let workspace = self.workspace_lane();
        if let Some(note) = crate::agent_kernel::base_instruction::identity_note(
            &self.config.agent_id,
            self.display_name.as_deref(),
            Some(workspace.as_str()),
        ) {
            self.context.merge_head_system(&note);
            self.identity_note = Some(note);
        }
    }

    /// 这一轮账上生效的规则进系统前缀。
    ///
    /// 拿上一轮那一段与这一轮比：一样就什么都不做（它已经在里面了），不一样
    /// 就先把旧的拿掉再放新的。不这么做的话，同一段系统前缀会随着规则改写
    /// 一轮一轮叠上去，而模型每轮付的是全部叠加的字节。
    async fn absorb_principles(&mut self) {
        let section = match self.principle_source.clone() {
            Some(source) => crate::principles::section_from(&*source).await,
            None => crate::principles::for_this_run().await,
        };
        self.apply_principles_section(section);
    }

    /// 把这一轮的规则段落放进系统前缀，换掉上一轮那一段。
    ///
    /// 分成两步是为了让「换掉」本身可测：不必先装一本真账，也能证明同一段
    /// 前缀里不会同时留着撤销前的规则与撤销后的规则。
    fn apply_principles_section(&mut self, section: Option<String>) {
        if self.principles_note.as_deref() == section.as_deref() {
            return;
        }
        if let Some(old) = self.principles_note.take() {
            self.context.drop_from_head_system(&old);
        }
        if let Some(section) = section {
            self.context.merge_head_system(&section);
            self.principles_note = Some(section);
        }
    }

    /// 自己那一行的名字。每一轮开头问一次名册，问到了就留着。
    ///
    /// 只在读不到时每轮重问：工号是登记时发的，第一轮跑在登记之前就不该把
    /// 「待编」记成自己的名字——下一轮登记好了，读到的才是真的那个。
    async fn look_up_own_name(&mut self) {
        if self.identity_looked_up || self.display_name.is_some() {
            return;
        }
        self.identity_looked_up = true;
        let Some(roster) = self.roster.as_ref() else {
            return;
        };
        match roster.employee(&self.config.agent_id).await {
            Ok(Some(row)) => self.display_name = Some(row.label()),
            Ok(None) => {
                // 还没登记上：这一轮不编名字。这一轮剩下的回合也不再问——它
                // 不会在跑到一半时登记上；下一轮开头（`begin_scoped_run` 重置
                // 这个开关）再问。
            }
            Err(e) => tracing::warn!(
                agent_id = %self.config.agent_id,
                error = %e,
                "名册读不到自己那一行：这一轮它不知道自己的名字"
            ),
        }
    }

    /// 这一轮的命令与文件落在哪条工作树车道上。
    ///
    /// 车道由执行者自己决定，不由这一轮接的是哪件活决定：树是员工的，同一件活
    /// 换了个人接就换一棵树，而一件活做完之后那棵树还在。与执行器那边算的是
    /// 同一个名字（`cog_core::worktree_lane`），两边各拼一遍的话，模型会在前缀
    /// 里读到一棵树、在文件里改到另一棵——系统前缀里说错一棵树，比不说更坏。
    fn workspace_lane(&self) -> String {
        cog_core::worktree_lane(&self.config.agent_id)
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
        self.resumed_turns = 0;
        // 换了一次运行就是一个新的额度：上一轮报过的那次超大 prompt，不能替这一轮
        // 把嘴堵上，否则第二轮同样的问题会在任何地方都读不到。
        self.context_overrun_reported = false;
        self.run_outcome = None;
        // 新的一轮可以重新问一次名册：上一轮跑在登记之前、读不到自己那一行，
        // 这一轮登记好了就该读到。
        self.identity_looked_up = false;
        // Set before the first call and cleared at the only exit, so a run that
        // was started without an identity cannot inherit the previous one.
        self.run_task_id = run_task_id.map(str::to_string);
        // 一换活，上一件活留在进程里的对话就不属于这一次了。
        //
        // 员工无状态：一轮结束进程里不留跨轮的上下文，下一次从账（尝试日志）、
        // 收件箱与任务读回来。同一个对象先后被两件活用过时，第二件活如果接着
        // 上一件的第一条消息往下说，模型读到的是别人的请求与别人工具的结果，
        // 并把这些当成自己的前提——它不会报错，它只是照着别人的活说。
        //
        // 同一件活的重跑不清：那是同一段对话的继续，日志续跑也从这个窗口重建
        // （见 `resume_from_journal`）；没有日志可续时窗口里留着的正是上一次
        // 尝试读到的那一份，清掉等于把一次真实的续跑变成一次重来。没有任务
        // 身份的运行（`continue_`、界面里的一段对话）也不清：那是调用方明说
        // 要接着说下去的，对话那一侧自己每轮铺一遍历史。
        //
        // 这个进程里的第一次运行不算「换了活」（`run_seen`）：那一次要接着读
        // 上一次进程留下的尝试日志，正是续跑存在的理由。
        let other_task =
            self.run_seen && run_task_id.is_some() && self.last_run_task.as_deref() != run_task_id;
        if other_task {
            self.context.clear();
            // 系统前缀里那两段随上下文一起没了，「上一轮并进去的是哪一段」的
            // 记录也要跟着清：不清的话规则那一段会被认为还在里面，这一次运行
            // 读到的规则一条都没有。
            self.principles_note = None;
            self.identity_note = None;
            // 下面这一位还会让这一次运行不接日志续跑：日志的键是这一位自己的
            // 名字，里面是另一件活走到一半的对话——续上就等于把它从账上搬了
            // 回来，上面清掉的窗口又从账上长回来（见 `run_turns`）。
        }
        // 每一次运行都重新置这一位：上一次置过而这一次没走到读它的那一步
        // （比如 `begin_run` 就拒绝了），不能替下一次运行决定读不读日志。
        self.resume_is_from_another_task = other_task;
        self.run_seen = true;
        self.last_run_task = run_task_id.map(str::to_string);
        // 新的一次尝试从「刚刚动过」开始，而不是从上一轮留下的时刻开始。上一轮
        // 的读数如果留着，重试的第一拍就会被判成停住——判据本身没错，它读到的
        // 是另一个尝试的时刻。第一次心跳不产生间隔样本（没有前驱），所以这里
        // 不会污染窗口。
        let owner = self.config.owner_task_id.as_deref();
        for task_id in [owner, run_task_id].into_iter().flatten() {
            cog_core::progress::beat(task_id);
        }
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
        // 排空让路不算失败：它是一次计划内的停手，错的是时刻，不是这次运行。
        // 报成 `AgentError` 会让每次滚动部署都在告警面上留一串「失败」，而
        // 真正该被看见的失败会被它冲淡。重投由调用方按错误做，那是另一条路。
        if let Err(ref e) = outcome {
            if self.run_outcome != Some(crate::observable::RunOutcome::Preempted) {
                self.note_run_failure(e).await;
            }
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
            &self.config.agent_id,
            &self.config.role,
            outcome,
            crate::observable::RunCost {
                iterations,
                steps,
                tool_calls,
            },
            crate::observable::RunDecision {
                declaration: self.config.deliverable_state,
                ceiling: self.ceiling_source,
            },
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
        // 一次被叫停的运行也不是关于这个技能的证据：它什么都没做成，不是做得
        // 不对。记进去等于把「别人的部署」记到这个技能头上。
        if matches!(
            ending,
            crate::observable::RunOutcome::Refused | crate::observable::RunOutcome::Preempted
        ) {
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
        // 没有名字就没有这一次运行。缺省不是一个人：一个空 id 会一路走到
        // 账上、事件里、名册外，成为谁都对不上的「某位员工」，而这比拒绝
        // 一次运行难查得多——它看起来像系统在工作。这里拒绝，调用方就会
        // 在造出这个运行的地方把名字补上。
        let agent_id = self.config.agent_id.trim();
        if agent_id.is_empty() {
            return Err(SFError::Config(
                "this run has no agent_id: an employee cannot run without an identity".into(),
            ));
        }
        let agent_id = agent_id.to_string();
        self.config.agent_id = agent_id.clone();
        tracing::info!(agent_id = %agent_id, task_id = run_task_id.unwrap_or(""), "AgentRuntime::run started");
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

    /// 这件任务把对话记在哪一行日志下。
    ///
    /// 判据是「有没有主的任务」：没有任务的身份（内嵌运行、界面里的一段对话、
    /// 常驻角色）不记——它们的第二次运行不是同一次尝试的重跑，记下来只会在
    /// 不相干的两次运行之间搬一段历史。
    ///
    /// 名字取 `agent_id` 而不是任务 id：同一件任务上几个角色各干各的
    /// （`agent_id_for(task, role)`），按任务 id 记会让两个人的对话挤进同一份
    /// 日志。这个名字由任务与角色**算出来**，进程换了重算还是同一个。
    fn journal_key(&self) -> Option<&str> {
        self.config
            .owner_task_id
            .as_deref()
            .map(|_| self.config.agent_id.as_str())
    }

    /// 记一条。没有装日志、或者这次运行没有主的任务，就什么都不做。
    ///
    /// 写不进去不打断运行：续跑是省钱的辅助，把「记不下来」变成「跑不了」，
    /// 等于拿一条辅助设施换掉主线。失败留一条 warn 并计数，读的人分得清
    /// 「日志一直在正常工作」与「日志从来没写进去过」。
    async fn journal(&self, entry: JournalEntry) {
        let Some(key) = self.journal_key() else {
            return;
        };
        let Some(journal) = attempt_journal::current() else {
            return;
        };
        if let Err(e) = journal.append(key, &entry).await {
            attempt_journal::record_write_failure();
            tracing::warn!(agent_id = %self.config.agent_id, error = %e, "attempt journal write failed; this run continues without a resume point");
        }
    }

    /// 记下一条进入过上下文的消息。
    ///
    /// 取的是**上下文里那一条**（`history` 的最后一条），不是交进去的原文：
    /// 工具结果进上下文时会按窗口预算收缩，日志要记的正是模型读到的那一份。
    async fn journal_last_context_message(
        &self,
        kind: JournalKind,
        tool_call_id: Option<String>,
        tool: Option<String>,
    ) {
        let Some(message) = self.context.messages().last().cloned() else {
            return;
        };
        self.journal(JournalEntry::message(kind, message, tool_call_id, tool))
            .await;
    }

    /// 一次尝试跑完了：这次对话不再重放。
    async fn journal_close(&self) {
        self.journal(JournalEntry::closed()).await;
    }

    /// 该不该在回合边界让路。
    ///
    /// 两个条件缺一不可：进程被要求停下，并且这次运行有日志可续。没有任务的
    /// 运行没有可续的地方，提前退出只是把已经做出来的东西扔掉。
    fn should_preempt(&self) -> bool {
        self.journal_key().is_some() && crate::drain::requested()
    }

    /// 在回合边界让路。记下这个结局，把尝试留在开着的状态。
    ///
    /// 返回错误而不是一个「成功的空结果」：这次运行没有交付任何东西，把它读成
    /// 成功会让上层把一件没做完的活记成做完。
    fn preempt(&mut self, iterations: u32) -> SFError {
        tracing::info!(
            agent_id = %self.config.agent_id,
            task_id = self.config.owner_task_id.as_deref().unwrap_or(""),
            iterations,
            in_flight = crate::drain::in_flight(),
            "the process is shutting down; stopping at the turn boundary, the attempt stays open"
        );
        self.note_run_ending(crate::observable::RunOutcome::Preempted, iterations);
        // 事实写在类型上，前缀由它渲染出来：派发层会把错误拍平成文本再判要不要
        // 重试，所以两种通道同时存在——类型给判决读，前缀给只拿到文本的那一层读。
        // 带上它，环境付账，任务的重试预算一格不动；不带它，每滚一次就扣一次，
        // 滚几次之后一件没做错的活被判死。
        SFError::environment_denied(
            cog_core::contract::outcome::FailureCause::ProcessStopping,
            "the process is shutting down; the attempt is left open and the next dispatch \
             resumes from its journal",
        )
    }

    /// 接着上一次跑。
    ///
    /// 日志里有一条没交付的对话，就把它的消息原样交还给上下文，再把**没有
    /// 结果的工具调用**按各自的副作用等级处置掉：读的重跑，写的重跑（工具
    /// 本身拒绝锚点已不在的改写，所以重跑不会落两次），有外部效果的一律不重跑
    /// ——把「不确定」连同它的事实交给模型，让它先去看一眼世界。
    async fn resume_from_journal(&mut self) {
        let Some(key) = self.journal_key().map(str::to_string) else {
            return;
        };
        let Some(journal) = attempt_journal::current() else {
            return;
        };
        let entries = match journal.load(&key).await {
            Ok(entries) => entries,
            Err(e) => {
                attempt_journal::record_write_failure();
                tracing::warn!(agent_id = %self.config.agent_id, error = %e, "attempt journal could not be read; this run starts from the beginning");
                return;
            }
        };
        let Some(plan) = crate::agent_kernel::resume::plan(&entries) else {
            return;
        };
        // 上下文里原本可能还留着上一轮的投影（同一个进程里同一件任务被再派
        // 一次）。日志是耐久的那一份，重建成它，两条路径就只有一份历史。
        self.context.clear();
        let messages = plan.messages.len();
        for message in plan.messages {
            self.context.add_message(message);
        }
        self.resumed_turns = plan.assistant_turns;
        tracing::info!(
            agent_id = %self.config.agent_id,
            task_id = self.config.owner_task_id.as_deref().unwrap_or(""),
            messages,
            resumed_turns = plan.assistant_turns,
            doubts = plan.doubts.len(),
            "resuming an interrupted attempt from its journal"
        );
        self.resolve_doubts(&plan.doubts).await;
    }

    /// 处置没有结果的工具调用。每个都一定会产生一条结果：一个带调用却缺结果的
    /// 助手回合，任何上游都不会接受，续跑发出的第一个请求就会失败。
    async fn resolve_doubts(&mut self, doubts: &[crate::agent_kernel::resume::Doubt]) {
        if doubts.is_empty() {
            return;
        }
        let scope = self
            .run_task_id
            .as_deref()
            .map(|tid| crate::tools::ToolScope {
                task_id: tid.to_string(),
                agent_id: self.config.agent_id.clone(),
            });
        for doubt in doubts {
            let text = match attempt_journal::effect_of(&doubt.tool) {
                // 只读：重跑一次，与它本来会得到的结果没有区别。
                EffectClass::Read => self
                    .tools
                    .execute_scoped(&doubt.tool, doubt.arguments.clone(), scope.as_ref())
                    .await
                    .map(|v| v.to_string())
                    .unwrap_or_else(|e| serde_json::json!({ "error": e.to_string() }).to_string()),
                // 写文件：重跑是安全的，因为工具自己拒绝「锚点已经不在」的改写。
                // 拦在这里替它猜，反而会让一次真的没落上的写被当成落上了。
                EffectClass::Write => self
                    .tools
                    .execute_scoped(&doubt.tool, doubt.arguments.clone(), scope.as_ref())
                    .await
                    .map(|v| v.to_string())
                    .unwrap_or_else(|e| {
                        serde_json::json!({
                            "error": e.to_string(),
                            "note": "this call was in flight when the process stopped and its result was lost; \
                                     this reply is from a re-run. If the anchor is gone, the write most likely \
                                     already landed -- read the file before deciding."
                        })
                        .to_string()
                    }),
                // 有外部效果：**绝不重跑**。提交、推送、起任务、发消息，重跑一次
                // 就在世界上多留一次效果，而这里没有任何东西能事后把它合并掉。
                // 交给模型：它能读，它知道自己在干什么，它比这条路径更清楚
                // 该不该再来一次。
                EffectClass::Effect => serde_json::json!({
                    "interrupted": true,
                    "tool": doubt.tool,
                    "note": "this call was dispatched but the process stopped before its result was \
                             recorded, so whether it took effect is unknown. It was NOT repeated. \
                             Look at the state of the world (the files, the ledger, the command's \
                             visible output) before deciding whether to run it again."
                })
                .to_string(),
            };
            let message = Message::tool_result_text(&doubt.call_id, &doubt.tool, text);
            self.journal(JournalEntry::message(
                JournalKind::ToolResult,
                message.clone(),
                Some(doubt.call_id.clone()),
                Some(doubt.tool.clone()),
            ))
            .await;
            self.context.add_message(message);
        }
    }

    /// The run itself, from the first turn to its answer.
    async fn run_turns(
        &mut self,
        input: serde_json::Value,
        llm: &dyn cog_core::LlmClient,
        run_task_id: Option<&str>,
    ) -> SFResult<serde_json::Value> {
        self.begin_run(run_task_id).await?;

        // 这件任务此前跑到哪儿了：先读日志，读得到就从那里接着跑。
        //
        // 放在契约与输入之前：那两段是**这一次尝试**才有的东西，历史消息排在
        // 它们后面。先续、再铺这次的请求，模型的请求就是「同一段说话的继续」，
        // 上游的前缀缓存也从第一个字节起就命中。
        //
        // 换了一件活的那一次不读：日志的键是这一位自己的名字，里面是另一件活
        // 走到一半的对话。读回来，上面清掉的窗口又从账上长回来。
        if self.resume_is_from_another_task {
            self.resume_is_from_another_task = false;
            tracing::info!(
                agent_id = %self.config.agent_id,
                task_id = run_task_id.unwrap_or(""),
                "this run is for a different task than the attempt in the journal; starting from \
                 the request instead of resuming another task's conversation"
            );
        } else {
            self.resume_from_journal().await;
        }

        // 刚被要求停下：连这一次尝试的请求都不再铺开。日志的最后一行还是
        // 上一次落下的那条有结果的调用，下一次派发从它后面接上——不多记一条
        // 重复的请求，也不多问一次模型。
        if self.should_preempt() {
            return Err(self.preempt(0));
        }

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
        self.absorb_base_instruction().await;
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
            self.journal_last_context_message(JournalKind::User, None, None)
                .await;
        } else {
            // 截图与录屏要作为媒体块交给模型，它才看得见——这是通用循环的
            // 能力，不该只有某一条专用判定路径有。
            let mut blocks = Vec::with_capacity(media.len() + 1);
            blocks.push(cog_core::ContentBlock::text(rendered));
            blocks.extend(media);
            self.context.add_message(Message::user_blocks(blocks));
            self.journal_last_context_message(JournalKind::User, None, None)
                .await;
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

        // 这次尝试的轮次从续跑点之后开始，预算按「这一次还能花几轮」算。
        // 被杀掉之前花掉的那些已经花过了——把它们再扣一遍，一件被中断过两次的
        // 任务会因为「重跑了几遍」而永远跑不完它本来一轮就能做完的活。
        let first_turn = self.resumed_turns;
        let budget_end = first_turn.saturating_add(self.config.max_iterations);
        for iteration in first_turn..budget_end {
            // 排空闸。进程被要求停下时，这里不再向模型发起新一轮：上一轮的工具
            // 批次已经跑完、结果全部落盘，对话正好停在「最后一条有结果的调用
            // 之后」，下一次派发原样接上，一步不丢。只看**回合之间**：在批次
            // 中间退出会留下带调用却没有结果的助手回合，那正是续跑要处置的
            // 不确定——排空就是为了不制造它。
            if self.should_preempt() {
                return Err(self.preempt(iteration.saturating_sub(first_turn)));
            }
            tracing::info!(agent_id = %self.config.agent_id, iteration, "AgentRuntime::run iteration start");
            // 这一步做完了就是「这一轮还在动」的证据。编排侧的回收与执行路由的
            // 守护都读它，而且都只读它：没有任何一条判据看这次运行已经花了多久，
            // 所以一个慢而一直在产出的轮次不会被砍，一个停住的不用等到某个声明的
            // 时限过期。
            //
            // 两个 id 都打：回收查的是 DAG 行上那个 id，而账本按 `owner_task_id`
            // 归属、运行自己按 `run_task_id` 归档，两者在替父任务干活的 agent 上
            // 不是同一个值。只打一个时，另一个 id 下的活永远查不到心跳——判据不会
            // 报错，只会永远判不出停住，而那与「它一直很健康」是同一个读数。
            let owner = self.config.owner_task_id.as_deref();
            let run = self.run_task_id.as_deref();
            for task_id in [owner, run].into_iter().flatten() {
                cog_core::progress::beat(task_id);
            }
            self.run_iterations = iteration + 1 - first_turn;
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

            // 模型说过的话**先于它要调用的工具落盘**。这条顺序是续跑唯一的
            // 判据：日志里有这一条、没有某个调用的结果，当且仅当那次调用执行到
            // 一半进程没了。记在工具之后，这段窗口里丢掉的是「它想干什么」，
            // 工具跑没跑也一并丢了。
            self.journal(JournalEntry::message(
                JournalKind::Assistant,
                assistant_msg.clone(),
                None,
                None,
            ))
            .await;

            // If no tool calls, complete normally
            if tool_calls.is_empty() {
                return self
                    .deliver(
                        llm,
                        thought_text,
                        assistant_msg,
                        iteration + 1 - first_turn,
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
            if iteration + 1 == budget_end {
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
                    // 「没跑」也是一条结果，而且要落盘：不落，下一次续跑读到的
                    // 就是「这几个调用没有结果」，把它们当成执行到一半、再跑一遍
                    // ——预算结束前明明没跑的东西，续跑时被重放了出去。
                    self.journal_last_context_message(
                        JournalKind::ToolResult,
                        Some(tc.id.clone()),
                        Some(tc.name.clone()),
                    )
                    .await;
                }
                self.context
                    .add_message(Message::user(FINAL_DRAFT_INSTRUCTION));
                self.journal_last_context_message(JournalKind::User, None, None)
                    .await;
                // 这一问不再有自己的钟：它有没有卡住由流自己的停顿判据回答
                // （`think_stall_timeout_secs`，每收到一个流事件就重置）。在它之上
                // 再压一个总时长，挡住的不是「卡住的上游」，而是「一直在吐字的
                // 长回答」——那恰好是这一问存在的理由。
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
                    // 失败更接近事实。但两种失败要分开写进哨兵：这一问自己不设钟，
                    // 收场的是流停顿判据（每收到一个事件就重置），所以「到点没给完」
                    // 与「问过了没答」由同一个判据写下的判词分开。
                    Err(e) if is_stream_stall(&e) => {
                        tracing::warn!(
                            agent_id = %self.config.agent_id,
                            error = %e,
                            "the final-draft call stopped moving; the run still ends on its spent budget"
                        );
                        (None, "timeout")
                    }
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
                        .deliver(
                            llm,
                            draft,
                            msg,
                            iteration + 2 - first_turn,
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
                    iteration + 1 - first_turn,
                );

                return Ok(result);
            }

            // Emit ReAct step start before executing tool calls
            self.emit_event(AgentEvent::ReActStepStart {
                agent_id: self.config.agent_id.clone(),
                iteration: iteration - first_turn,
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

                // 动手之前先问。模式能自己定的就不出门；要问的，答案决定这一格是
                // 执行还是把理由交回模型。理由不是工具失败：同一个调用再发一遍会
                // 得到同一个答案，所以它带着 `permission` 标记，让循环看得出这一
                // 条换不掉重试。
                let ask = crate::permission::PermissionAsk {
                    call_id: tc.id.clone(),
                    agent_id: self.config.agent_id.clone(),
                    tool: tc.name.clone(),
                    arguments: tc.arguments.clone(),
                    mode: self.permission_mode,
                };
                if let crate::permission::PermissionVerdict::Deny { reason } =
                    crate::permission::decide(self.permission_mode, self.permission.as_ref(), &ask)
                        .await
                {
                    let denied = serde_json::json!({
                        "error": reason.clone(),
                        "permission": "denied",
                        "mode": self.permission_mode.as_str(),
                    });
                    observations.push(denied.clone());
                    tool_result_messages.push(Message::tool_result_text(
                        &tc.id,
                        &tc.name,
                        denied.to_string(),
                    ));
                    self.emit_event(AgentEvent::ToolExecutionEnd {
                        agent_id: self.config.agent_id.clone(),
                        tool_call_id: tc.id.clone(),
                        result: denied,
                        is_error: true,
                        timestamp: chrono::Utc::now(),
                    })
                    .await?;
                    if let Some(ref after) = self.hooks.after_tool_call {
                        after(&tc.name, &tc.arguments, &Err(SFError::Agent(reason)));
                    }
                    continue;
                }

                self.state = RuntimeState::Acting;
                let scope = run_task_id.map(|tid| crate::tools::ToolScope {
                    task_id: tid.to_string(),
                    agent_id: self.config.agent_id.clone(),
                });
                // What the tool says while it runs. The executor hands its output
                // over in whatever slices it read, and one frame per slice would
                // turn a long build into tens of thousands of records on the
                // bus, in the WAL and in the panel. They are gathered here and
                // sent on a beat instead: the same stream to the reader, fewer
                // frames, each carrying everything since the last one.
                let (update_tx, mut update_rx) = mpsc::unbounded_channel::<String>();
                let sink: crate::tools::ToolUpdateSink = {
                    let update_tx = update_tx.clone();
                    Arc::new(move |chunk: &str| {
                        // The receiver is the select loop below, which lives
                        // through every chunk this call will produce. A send
                        // that fails means the call is already over and there
                        // is nobody left to show it to.
                        let _ = update_tx.send(chunk.to_string());
                    })
                };
                let mut partial = String::new();
                let mut last_flush = tokio::time::Instant::now();
                // The tool is shared by every agent in this process, so the
                // handler cannot tell whose call this is unless the dispatcher
                // says so. It is named around the await rather than passed in:
                // the handler signature is the same for every native tool, and
                // only the addressing tools need a caller.
                let mut call = Box::pin(crate::tools::CALLER_AGENT_ID.scope(
                    self.config.agent_id.clone(),
                    self.tools.execute_scoped_streaming(
                        &tc.name,
                        tc.arguments.clone(),
                        scope.as_ref(),
                        Some(&sink),
                    ),
                ));
                let checked = loop {
                    tokio::select! {
                        res = &mut call => break res,
                        Some(chunk) = update_rx.recv() => {
                            note_tool_update(&mut partial, &chunk);
                            if last_flush.elapsed() >= TOOL_UPDATE_FLUSH {
                                self.flush_tool_update(&tc.id, &mut partial).await?;
                                last_flush = tokio::time::Instant::now();
                            }
                        }
                        // Armed only while something is waiting to be shown: a
                        // call that has said nothing needs no beat, and an idle
                        // timer on every native call would be work for nobody.
                        _ = tokio::time::sleep_until(last_flush + TOOL_UPDATE_FLUSH),
                            if !partial.is_empty() => {
                            self.flush_tool_update(&tc.id, &mut partial).await?;
                            last_flush = tokio::time::Instant::now();
                        }
                    }
                };
                // The call is over; whatever it said last still has to reach the
                // reader, and it has to arrive before the end frame that replaces
                // it. Dropping the tool future first closes the only sender left,
                // so the drain below sees every chunk the call ever sent.
                drop(call);
                drop(sink);
                drop(update_tx);
                while let Some(chunk) = update_rx.recv().await {
                    note_tool_update(&mut partial, &chunk);
                }
                self.flush_tool_update(&tc.id, &mut partial).await?;

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
                iteration: iteration - first_turn,
                thought: thought_text.clone(),
                tool_calls: tool_calls.clone(),
                observations: observations.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // Step 5: Update context
            self.state = RuntimeState::Updating;
            self.context.add_message(assistant_msg);
            for (tr, tc) in tool_result_messages.iter().zip(tool_calls.iter()) {
                self.context.add_message(tr.clone());
                // 记的是上下文收下的那一条（`messages` 的最后一条，可能被窗口
                // 预算收缩过），不是交进去的原文：续跑把另一份交还给模型，
                // 模型读到的历史就与它当时读到的不是同一个东西。
                self.journal_last_context_message(
                    JournalKind::ToolResult,
                    Some(tc.id.clone()),
                    Some(tc.name.clone()),
                )
                .await;
            }
            // After the results, so the note is the last thing read before the
            // next turn: a stretch of reads with nothing written is exactly the
            // transcript the model would otherwise take as evidence that it is
            // getting somewhere.
            if let Some(note) = progress.take_note() {
                self.context.add_message(Message::user(note));
                // 这段提醒也是对话的一部分：续跑把它补回来，模型读到的历史就
                // 与当时逐字节一致，上游的前缀缓存也命中同一段。
                self.journal_last_context_message(JournalKind::User, None, None)
                    .await;
            }

            self.emit_event(AgentEvent::TurnEnd {
                agent_id: self.config.agent_id.clone(),
                message: Message::assistant_text(thought_text.clone()),
                tool_results: tool_result_messages.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await?;

            // 窗口连地板都装不下时，这一轮已经没有记忆可用了：刚跑完的那几条
            // 结果在同一拍就被裁掉，下一轮模型收到的请求与这一轮逐字节相同，
            // 它会照着同一份输入再做一次同样的调用。这不是「模型还需要多几轮」
            // ——多给多少轮都是同一道题、同一个答案。所以在这里收住，用一个
            // 有名有姓的结局报出去，而不是让它把预算花在一份不会变的请求上。
            // 2026-11-06 实测：一件自进化任务这样发了 1,563 次同样的请求体、
            // 跑满一小时、零产出，而它每一次的结局都被读成「预算用完了」。
            if self.context.floor_exceeds_budget() {
                self.state = RuntimeState::Complete;
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    budget = self.config.context_window_size,
                    estimated = self.context.token_count(),
                    "the context window cannot hold the task input and one result at once; \
                     every result is evicted the moment it arrives, so the run ends here \
                     instead of re-sending an identical request until the budget is spent"
                );
                self.note_run_ending(
                    crate::observable::RunOutcome::Unreadable,
                    iteration + 1 - first_turn,
                );
                self.emit_event(AgentEvent::AgentEnd {
                    agent_id: self.config.agent_id.clone(),
                    messages: self.published_messages(),
                    crew_id: None,
                    squad_id: None,
                    timestamp: chrono::Utc::now(),
                })
                .await?;
                return Ok(serde_json::json!({
                    "status": cog_core::contract::outcome::CONTEXT_FLOOR_EXCEEDS_WINDOW_STATUS,
                    "budget": self.config.context_window_size,
                    "estimated": self.context.token_count(),
                    "iterations": iteration + 1 - first_turn,
                }));
            }

            // 一直在读、一直在搜、被提醒过两次之后又走满一整段仍然不写：这条运行
            // 不是在探索，是在绕圈。收住它的判据是调用次数，不是跑了多久——一次长
            // 编译按次只算一次，不会被这一条收走。名字说的是事实：这段调用里一次
            // 都没动过工作树。
            if circling_unproductively(&self.config, &progress) {
                self.state = RuntimeState::Complete;
                tracing::warn!(
                    agent_id = %self.config.agent_id,
                    calls_since_change = progress.calls_since_change,
                    calls_without_change = progress.calls_without_change,
                    notes_given = progress.notes_given,
                    "the checkout has not moved across another full stretch of calls after \
                     the run had been told twice; ending it here instead of spending more \
                     turns on reads"
                );
                self.note_run_ending(
                    crate::observable::RunOutcome::NoProgress,
                    iteration + 1 - first_turn,
                );
                self.emit_event(AgentEvent::AgentEnd {
                    agent_id: self.config.agent_id.clone(),
                    messages: self.published_messages(),
                    crew_id: None,
                    squad_id: None,
                    timestamp: chrono::Utc::now(),
                })
                .await?;
                return Ok(serde_json::json!({
                    "status": cog_core::contract::outcome::NO_CHECKOUT_PROGRESS_STATUS,
                    "calls_since_change": progress.calls_since_change,
                    "iterations": iteration + 1 - first_turn,
                }));
            }
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
                // 同 `final draft` 那一处：这一问有没有卡住由流自己的停顿判据回答，
                // 不在它之上再压一个总时长。一直在吐字的改写不是卡住。
                let (text, message) = match self.think_stream_final(llm).await {
                    Ok(message) => (assistant_text(&message), message),
                    Err(e) => {
                        return Err(self.reply_refusal(
                            rule,
                            format!("{reason} (the rewrite could not be obtained: {e})"),
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
            // 这里曾包着一层 `tokio::time::timeout(RESULT_BUILD_TIMEOUT, …)`。它从来
            // 不可能燃着：`build_result` 是同步函数，里面没有一个 await，被判的那段
            // 代码根本不会把控制权还给运行时，计时器永远等不到自己的那一拍。拆掉它
            // 不改变任何行为——它只是让读的人以为这里有一个界。
            self.build_result(&draft)?
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

        // 交付了，这段对话到此为止：同一件任务的下一次运行从零开始。关闭只写在
        // 交付这一条出口上——预算用尽、被规则拦下、上游报错都留着那条路，它们的
        // 对话是还没交出去的东西，下一次派发正是要从那里接着跑。
        self.journal_close().await;

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
        let request = request_digest(&messages, &options, self.identity_note.as_deref());
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
                "{STREAM_STALL_PREFIX} for {}s while {} for agent {}",
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
        // 谁答的也记在这里，理由与上面那行相同：这个名字只活在这份返回里，
        // 助手消息不带它。空名字不是名字——上游没报就不记，别拿它盖掉上一次
        // 真的报了的那一路。
        if !response.model.trim().is_empty() {
            self.run_usage.model = Some(response.model.clone());
        }
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

    /// 把攒下的输出作为一帧进度发上总线，然后清空缓冲。
    ///
    /// 空缓冲不发：界面上一次空的进度，读起来像「这次调用刚开了个头」，而它
    /// 其实只是这个节拍里没有新的话。
    async fn flush_tool_update(&self, tool_call_id: &str, partial: &mut String) -> SFResult<()> {
        if partial.is_empty() {
            return Ok(());
        }
        let text = std::mem::take(partial);
        self.emit_event(AgentEvent::ToolExecutionUpdate {
            agent_id: self.config.agent_id.clone(),
            tool_call_id: tool_call_id.to_string(),
            partial_result: serde_json::Value::String(text),
            timestamp: chrono::Utc::now(),
        })
        .await
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

/// The test-only seam, kept after every production item on purpose.
///
/// `#[cfg(test)]` is not just an attribute here: one reader of this tree takes
/// "the file's first `#[cfg(test)]` to the end" as the test region, and counts
/// `AgentEvent` constructions above that line as the ones a live face can rely
/// on. A test-only constructor declared next to `new` would move that line to
/// the top of the file and make every event this runtime emits look unbuilt.
#[cfg(test)]
impl AgentRuntime {
    /// Build a runtime whose ceiling is derived from `observable` instead of the
    /// process-wide one.
    ///
    /// The production path reads the global observable on purpose: the sample is
    /// a fact about the work this process does, not about one test's fixture.
    /// Tests that assert what a particular sample produces need their own bucket
    /// — every other test that runs a loop writes into the global one, and a
    /// shared bucket would make the assertion depend on test order.
    pub(crate) fn new_with_observable(
        config: RuntimeConfig,
        event_tx: mpsc::Sender<AgentEvent>,
        observable: &crate::observable::AgentObservable,
    ) -> Self {
        Self::with_observable(config, event_tx, observable)
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

    /// 名册上只有这一位，工号由测试指定。
    struct OneNamed(u32);

    #[async_trait::async_trait]
    impl cog_core::AgentRegistry for OneNamed {
        async fn register(&self, _: &cog_core::AgentRegistration) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn heartbeat(&self, _: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn deregister(&self, _: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn get(&self, _: &str) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
            Ok(None)
        }
        async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
        async fn list_by_role(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
        async fn list_by_capability(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
            Ok(Vec::new())
        }
        async fn employee(
            &self,
            agent_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::EmployeeRecord>> {
            let now = chrono::Utc::now();
            Ok(Some(cog_core::EmployeeRecord {
                agent_id: agent_id.to_string(),
                number: Some(self.0),
                role: "chat".into(),
                first_seen: now,
                last_seen: now,
                retired_at: None,
            }))
        }
    }

    /// 这一位开跑时，系统前缀里有它自己的工号与名字，还有它这一轮落在哪条
    /// 工作树车道上。少了这一段，模型被问「你是谁」只能从工具结果里猜。
    #[tokio::test]
    async fn the_system_prefix_names_this_employee_and_its_workspace() {
        let config = RuntimeConfig {
            agent_id: "kai-a9ab11fc".into(),
            role: "kai-a9ab11fc".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let mut agent_loop = AgentRuntime::new(config, tx).with_roster(Arc::new(OneNamed(412)));

        agent_loop.absorb_base_instruction().await;

        let head = agent_loop
            .context
            .messages()
            .first()
            .expect("the run's system prefix is in the context")
            .content()
            .to_string();
        assert!(
            head.contains(&cog_core::employee_label(412)),
            "the employee's own label is missing: {head}"
        );
        // 同事按这个 id 找得到它：不知道自己的 id，就没法让同事回信。
        assert!(
            head.contains("kai-a9ab11fc"),
            "the agent id peers address is missing: {head}"
        );
        // 与工具侧同一条车道名：命令落在哪棵树，前缀里说的就是哪棵树。
        assert!(
            head.contains(&cog_core::worktree_lane("kai-a9ab11fc")),
            "the workspace lane the calls land in is missing: {head}"
        );
    }

    /// 名册上还没有这个人：前缀只说道路，不编名字。编一个名字会让人以为
    /// 名册上有这一位。
    #[tokio::test]
    async fn an_employee_without_a_roster_row_is_given_no_name() {
        let config = RuntimeConfig {
            agent_id: "no-one-yet".into(),
            role: "no-one-yet".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        struct Empty;
        #[async_trait::async_trait]
        impl cog_core::AgentRegistry for Empty {
            async fn register(&self, _: &cog_core::AgentRegistration) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn heartbeat(&self, _: &str) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn deregister(&self, _: &str) -> cog_core::SFResult<()> {
                Ok(())
            }
            async fn get(
                &self,
                _: &str,
            ) -> cog_core::SFResult<Option<cog_core::AgentRegistration>> {
                Ok(None)
            }
            async fn list(&self) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
            async fn list_by_role(
                &self,
                _: &str,
            ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
            async fn list_by_capability(
                &self,
                _: &str,
            ) -> cog_core::SFResult<Vec<cog_core::AgentRegistration>> {
                Ok(Vec::new())
            }
        }
        let mut agent_loop = AgentRuntime::new(config, tx).with_roster(Arc::new(Empty));

        agent_loop.absorb_base_instruction().await;

        let head = agent_loop
            .context
            .messages()
            .first()
            .expect("the workspace note alone is worth a prefix")
            .content()
            .to_string();
        assert!(!head.contains("名字"), "no name was claimed: {head}");
        assert!(head.contains(&cog_core::worktree_lane("no-one-yet")));
    }

    /// 账上生效的规则在每次运行开头进系统前缀：这是「随基础指令一起交给员工」
    /// 唯一能被模型读到的地方。
    ///
    /// 这一段不按角色分：同一个内核、同一本账，谁跑都拿到同一批规则，所以这里
    /// 断言的是「这个进程装的账上那几条到了前缀里」，而不是「这件事的人拿到了
    /// 这件事的规则」。
    #[tokio::test]
    async fn a_run_starts_with_the_rules_the_account_has_in_force() {
        struct OneRule;

        #[async_trait::async_trait]
        impl crate::principles::PrincipleSource for OneRule {
            async fn in_force(&self) -> cog_core::SFResult<Vec<crate::principles::InForce>> {
                Ok(vec![crate::principles::InForce::new(
                    "build-before-merge",
                    "在合并之前",
                    "构建是不是绿的",
                    "红了就停下来修",
                )
                .expect("a full triple is a rule")])
            }

            async fn note_used(&self, _: &[String]) -> cog_core::SFResult<()> {
                Ok(())
            }
        }

        let config = RuntimeConfig {
            agent_id: "kai-a9ab11fc".into(),
            role: "kai-a9ab11fc".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        // 这一条循环带着自己的账跑，不往进程里装：装进进程的账会被同一个
        // 测试二进制里每一个运行读到，而其中一个运行只有 400 token 的窗口，
        // 多出来的这一段会把它的工具结果挤出上下文。
        let mut agent_loop = AgentRuntime::new(config, tx).with_principle_source(Arc::new(OneRule));

        agent_loop.absorb_base_instruction().await;

        let head = agent_loop
            .context
            .messages()
            .first()
            .expect("the run's system prefix is in the context")
            .content()
            .to_string();
        for needle in [
            "build-before-merge",
            "在合并之前",
            "构建是不是绿的",
            "红了就停下来修",
        ] {
            assert!(
                head.contains(needle),
                "rule part {needle:?} is missing: {head}"
            );
        }
    }

    /// 两轮之间规则集变了：撤销前的那一段必须离开前缀。
    ///
    /// 并进去是按「这一段在不在」判断的，不拿掉旧的就再并一段新的，模型这一轮
    /// 会并排读到已经撤回的规则和它的替代者，而它分不出哪条还算数。
    #[tokio::test]
    async fn a_rule_withdrawn_between_turns_stops_being_read() {
        let config = RuntimeConfig {
            agent_id: "kai-a9ab11fc".into(),
            role: "kai-a9ab11fc".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let mut agent_loop = AgentRuntime::new(config, tx);

        let before = crate::principles::section(&[crate::principles::InForce::new(
            "gone-rule",
            "第一轮的情况",
            "第一轮的判据",
            "第一轮的动作",
        )
        .expect("a full triple is a rule")])
        .expect("one rule is a section");
        let after = crate::principles::section(&[crate::principles::InForce::new(
            "replacement-rule",
            "第二轮的情况",
            "第二轮的判据",
            "第二轮的动作",
        )
        .expect("a full triple is a rule")])
        .expect("one rule is a section");

        agent_loop.apply_principles_section(Some(before));
        agent_loop.apply_principles_section(Some(after.clone()));

        let head = agent_loop
            .context
            .messages()
            .first()
            .expect("a section alone is worth a prefix")
            .content()
            .to_string();
        assert!(
            !head.contains("gone-rule") && !head.contains("第一轮的判据"),
            "the withdrawn rule is still being read: {head}"
        );
        assert!(
            head.contains(&after),
            "the rule in force is missing: {head}"
        );
        assert_eq!(
            head.matches("平台此刻生效的规则").count(),
            1,
            "the section was appended instead of replaced: {head}"
        );
    }

    /// 规则没变时同一段不再并一次：同一段话按轮数计费，而且重复的行会让人
    /// 以为规则增加了一条。
    #[tokio::test]
    async fn the_same_rules_are_not_prepended_again_on_the_next_turn() {
        let config = RuntimeConfig {
            agent_id: "kai-a9ab11fc".into(),
            role: "kai-a9ab11fc".into(),
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(4);
        let mut agent_loop = AgentRuntime::new(config, tx);

        let section = crate::principles::section(&[crate::principles::InForce::new(
            "steady-rule",
            "任何时候",
            "这件事有没有做完",
            "做完了就回报",
        )
        .expect("a full triple is a rule")])
        .expect("one rule is a section");

        agent_loop.apply_principles_section(Some(section.clone()));
        agent_loop.apply_principles_section(Some(section));

        let head = agent_loop
            .context
            .messages()
            .first()
            .expect("a section alone is worth a prefix")
            .content()
            .to_string();
        assert_eq!(
            head.matches("steady-rule").count(),
            1,
            "the same section was prepended twice: {head}"
        );
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
            role_type: "planner".into(),
        };

        let config = RuntimeConfig {
            role: "skill-prompt-test".into(),
            skill_config: Some(skill.clone()),
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new_with_observable(
            config,
            tx,
            &crate::observable::AgentObservable::new(),
        );
        // No system message should be injected when using structured JSON input
        let messages = agent_loop.get_context().messages();
        assert!(
            messages
                .iter()
                .all(|m| !matches!(m, cog_core::Message::System { .. })),
            "Context window should not contain any system messages"
        );
    }

    /// 技能行不能移动轮次上限。
    ///
    /// 这条以前量的是相反的断言：行上的数字覆盖配置。那一跳就是「按标签分发
    /// 天花板」的入口——行按角色名取，同一件活换个标签接手就换一个上限。现在
    /// 行只剩身份与种类，上限只来自配置面（覆盖）与实测（样本）。
    #[test]
    fn a_skill_row_cannot_move_the_ceiling() {
        let skill = SkillConfig {
            skill_id: "fast-evaluator".into(),
            name: "Fast Evaluator".into(),
            role_type: "evaluator".into(),
        };

        let config = RuntimeConfig {
            role: "skill-budget-test".into(),
            max_iterations: 100,
            skill_config: Some(skill),
            ..Default::default()
        };

        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new_with_observable(
            config,
            tx,
            &crate::observable::AgentObservable::new(),
        );
        assert_eq!(agent_loop.config.max_iterations, 100);
    }

    /// 这个进程自己跑出来的读数决定上限：种子（配置面）只在还没有观测时生效，
    /// 有了观测就不再是那个数。写死一个常量的坏处正在这里——它永远不知道这一
    /// 轮的工作量，而它要裁的恰恰是"还要不要接着干"。
    ///
    /// 样本按进程收，不按角色：同一件活换个标签接手，读到的必须是同一个数。
    /// 这个员工自己还没有交卷样本，所以它读到的是进程样本。
    #[test]
    fn the_iteration_budget_follows_the_runs_this_process_delivered() {
        let observable = crate::observable::AgentObservable::new();
        for iterations in [6u32, 8] {
            observable.record_run(
                "someone-else",
                "budget-calibrated-test",
                crate::observable::RunOutcome::Delivered,
                crate::observable::RunCost {
                    iterations,
                    steps: 20,
                    tool_calls: 5,
                },
                crate::observable::RunDecision {
                    declaration: cog_core::DeclarationState::Unstated,
                    ceiling: crate::observable::CeilingSource::Unmeasured,
                },
            );
        }

        let config = RuntimeConfig {
            agent_id: "a-new-employee".into(),
            role: "budget-calibrated-test".into(),
            // 没人写过上限：只有这时标定才回答。
            max_iterations: u32::MAX,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new_with_observable(config, tx, &observable);

        assert!(
            agent_loop.config.max_iterations > 8,
            "the ceiling must admit the longest run this process has delivered: {}",
            agent_loop.config.max_iterations
        );
        assert!(
            agent_loop.config.max_iterations < u32::MAX,
            "with measurements in hand the answer is a number, not 'unbounded'"
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

    fn skill_naming(role: &str) -> SkillConfig {
        SkillConfig {
            skill_id: format!("{role}-boundary-test"),
            name: role.into(),
            role_type: role.into(),
        }
    }

    /// 工具只有一套：同一张注册表交给两件不同活的运行，两边手上是同一份。
    ///
    /// 这里曾经按技能行上的白名单收窄：同一个进程里的两位对同一张注册表看到两套
    /// 工具，而裁成哪一套取决于注册表上一个自由标签。换个标签，同一件活就做不成了；
    /// 裁掉的那一项不会报错，它只是「不存在」，模型于是学会了不去用它。
    #[test]
    fn every_run_holds_the_whole_registry_its_process_installed() {
        let whole = registry_of(&["read_file", "write_file", "run_command"]);
        let mut expected = whole.names();
        expected.sort();

        for role in ["planner", "generator", "evaluator"] {
            let runtime = runtime_with(Some(skill_naming(role)), role).with_tools(whole.clone());
            let mut held = runtime.tools.names();
            held.sort();
            assert_eq!(
                held, expected,
                "{role} 手上少了几件工具：工具由进程装了什么决定，不由这一行的种类决定"
            );
        }

        // 没有技能行的那一位也一样：没有行不等于没有工具。
        let bare = runtime_with(None, "no-skill-row").with_tools(whole);
        let mut held = bare.tools.names();
        held.sort();
        assert_eq!(held, expected);
    }

    /// 没有观测就原样用种子：凭一个没看见过的事实改数字，只是把猜数换了个地方。
    #[test]
    fn a_process_without_observations_keeps_its_seed_budget() {
        let config = RuntimeConfig {
            role: "budget-unobserved-test".into(),
            max_iterations: 7,
            ..Default::default()
        };
        let (tx, _rx) = mpsc::channel(1);
        let agent_loop = AgentRuntime::new_with_observable(
            config,
            tx,
            &crate::observable::AgentObservable::new(),
        );
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

    /// 同一件活的第二次尝试接着上一次说；换了一件活的那一次不带上一件活的消息。
    ///
    /// 员工无状态：一轮结束，进程里不留跨轮的上下文，下一次从账（尝试日志）、
    /// 收件箱与任务读回来。同一个对象先后替两件活干过时，第二条活的请求里若还
    /// 带着第一条活的消息，模型会拿别人的请求与工具结果当前提——它不会报错，
    /// 它只是照着别人的活往下说。反过来，同一件活的重跑必须还看得见自己上一次
    /// 走到了哪——那是同一段对话的继续，日志续跑也从这里重建。
    #[tokio::test]
    async fn a_run_for_a_new_task_starts_without_the_previous_tasks_messages() {
        let llm = RecordingLlm::default();
        let mut runtime = census_runtime("task-identity-resets-context");
        let first = "第一件活的请求 identity-reset-first";
        let second = "第一件活的第二次尝试 identity-reset-second";
        let other = "另一件活的请求 identity-reset-other";
        for (task, marker, attempt) in [
            ("identity-reset-task-a", first, 1),
            ("identity-reset-task-a", second, 2),
            ("identity-reset-task-b", other, 1),
        ] {
            runtime
                .run_scoped(
                    serde_json::json!({"task": marker, "context": {"attempt": attempt}}),
                    &llm,
                    Some(task),
                )
                .await
                .expect("the run delivers");
        }

        let calls = llm.calls();
        assert_eq!(calls.len(), 3, "每一次运行都花了一次调用");
        let retry = &calls[1];
        assert!(
            retry.iter().any(|m| m.content().contains(first)),
            "同一件活的重跑读不到上一次自己说过什么：这会把一次续跑变成一次重来"
        );
        let other_task = &calls[2];
        assert!(
            !other_task
                .iter()
                .any(|m| m.content().contains("identity-reset-first")
                    || m.content().contains("identity-reset-second")),
            "换了一件活的那次请求里还带着上一件活的消息：员工把上一次运行留在了进程里"
        );
        assert!(
            other_task.iter().any(|m| m.content().contains(other)),
            "新一件活的请求没有进来：{other_task:?}"
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
        // 另一半（它停下的位置不能反过来抬上限）量在 observable 自己的用例里：
        // 样本现在是进程的，这里的每一次运行与同一进程里别的用例共用一份，断言
        // 绝对值就成了对执行顺序的断言。可判的那一半是上面这条：它记进的是
        // 「撞墙」的读数，不是「交卷」的样本。
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
            // 追问挂住时收场的是**这一跑自己的停顿窗口**：它随每个流事件重置，所以
            // 慢而有动静的生成不会被判死，只有一整个窗口一点动静都没有的连接才会。
            // 单独那道「追问墙」已经拆掉（2026-11-10：按钟判死一律换成按动静判停）。
            think_stall_timeout_secs: 1,
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

    /// 窗口连地板都装不下时，这一轮必须在第一条结果之后收住。
    ///
    /// 这个状态下每条消息进来都在同一拍被裁掉：第二轮发出去的请求与第一轮
    /// 逐字节相同，模型只会照着同一份输入再做一次同样的调用，而它每一轮都被
    /// 读成「预算用完了」。实测（2026-11-06，`redrive-change-ledger-f98ddb93…`）
    /// 同一个 179,595 字节的请求体被原样发了 1,563 次、跑满一小时、零产出。
    #[tokio::test]
    async fn a_run_whose_window_cannot_show_it_its_own_results_ends_after_one_turn() {
        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(
            &registry,
            cog_core::Tool {
                name: "noop".into(),
                description: "returns a line of output".into(),
                parameters: serde_json::json!({"type": "object"}),
                implementation: cog_core::ToolImplementation::Native(Arc::new(|_args| {
                    Box::pin(async move { Ok(serde_json::json!({ "stdout": "ok" })) })
                })),
            },
        );
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "floor".into(),
            role: "floor-exceeds-window-test".into(),
            max_iterations: 10,
            // 窗口比任务输入还小：地板自己就放不下。
            context_window_size: 40,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx).with_tools(registry);
        let llm = RepeatingCallLlm {
            calls: std::sync::atomic::AtomicUsize::new(0),
        };

        let result = runtime
            .run(serde_json::json!({ "task": "x".repeat(400) }), &llm)
            .await
            .expect("窗口装不下是这一轮自己的结局，不是一次工具失败");

        assert_eq!(
            result["status"],
            cog_core::contract::outcome::CONTEXT_FLOOR_EXCEEDS_WINDOW_STATUS,
            "结局必须自报是窗口装不下，而不是笼统的「预算用完了」: {result}"
        );
        assert_eq!(
            llm.calls.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "第二轮请求与第一轮逐字节相同，再发一次只会买回同一个答案"
        );
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

    /// 一轮对话把它花掉的量一起带走：界面上的那一行（token、轮次、时长、谁答的、
    /// 算没算钱）全部读这一格。
    ///
    /// 三条一起判：读到的 token 是上游报的那个数；名字是上游报的那一路；费用没
    /// 声明时是 `null` 而不是 0——假模型的 usage 全是零、价目也没声明，这时算出来
    /// 的 0.0 是「没算」，画成 $0.0000 就是白纸黑字地说这一轮不要钱。
    #[tokio::test]
    async fn a_conversation_carries_what_it_spent() {
        let llm = ScriptedReplyLlm::new(&["查过了，没有未了结的变更。"]);
        let (tx, _rx) = mpsc::channel(8);
        let mut runtime = AgentRuntime::new(
            RuntimeConfig {
                agent_id: "spend-reader".into(),
                role: "employee".into(),
                ..Default::default()
            },
            tx,
        );
        let result = runtime
            .run_conversation(vec![Message::user("查一下")], &llm)
            .await
            .expect("一轮对话");

        let usage = result
            .get("usage")
            .expect("这一轮的用量要跟着交付物一起走：界面那一行读的就是它");
        assert!(usage["rounds"].as_u64().unwrap_or(0) >= 1, "{usage}");
        assert_eq!(
            usage["total_tokens"].as_u64(),
            Some(0),
            "假模型报的是 0 个 token"
        );
        assert_eq!(
            usage["model"].as_str(),
            Some("mock"),
            "答话的是哪一路模型，取的是上游自己报回来的那一个"
        );
        assert!(
            usage["cost_usd"].is_null(),
            "没有声明价目时费用是 null：没算不是免费（{usage}）"
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

    /// 一条命令还在跑的时候，它已经说出口的话经总线往外走了。
    ///
    /// 判据要落在「命令结束之前」这一条上，不是「事件里有一帧进度」：等到结束
    /// 才把攒下的全文当一帧发出去，界面上同样是两帧——开始、结束，中间那段空白
    /// 与「它还活着但什么都没说」完全一样，而那正是这个功能要分开的两种状况。
    /// 所以这里让执行器在第一段输出之后停四拍再给第二段，并要求第一帧进度里
    /// 只有第一段。
    #[tokio::test]
    async fn a_running_command_reports_what_it_has_already_said() {
        /// 第一轮要一条命令，第二轮收工。
        struct RunThenAnswer;

        #[async_trait::async_trait]
        impl cog_core::LlmClient for RunThenAnswer {
            async fn chat_stream(
                &self,
                messages: &[Message],
                _options: &cog_core::ChatOptions,
            ) -> SFResult<cog_core::AssistantMessageEventStream> {
                // 这里没有可变状态：调用次数记在流的生产者里没有意义，所以判据
                // 用「消息里已经有工具结果」来决定这一轮是不是最后一轮。
                let answered = messages
                    .iter()
                    .any(|m| matches!(m, Message::ToolResult { .. }));
                let (stream, producer) = cog_core::EventStream::with_capacity(4);
                tokio::spawn(async move {
                    let mut producer = producer;
                    let content = if answered {
                        vec![ContentBlock::Text {
                            text: r#"{"done":true}"#.into(),
                            text_signature: None,
                        }]
                    } else {
                        vec![ContentBlock::tool_call(
                            "call-chatty",
                            "run_command",
                            serde_json::json!({ "command": "echo first; sleep 0.1; echo second" }),
                        )]
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

        struct Chatty;

        #[async_trait::async_trait]
        impl cog_core::SandboxBackend for Chatty {
            async fn execute(
                &self,
                _req: &cog_core::SandboxRequest,
            ) -> SFResult<cog_core::SandboxResult> {
                Ok(cog_core::SandboxResult {
                    stdout: "first\nsecond\n".into(),
                    exit_code: 0,
                    ..Default::default()
                })
            }

            async fn precompile(&self, _bytes: &[u8]) -> SFResult<String> {
                Err(SFError::Agent("no wasm in this test".into()))
            }

            async fn execute_stream(
                &self,
                _req: &cog_core::SandboxRequest,
            ) -> SFResult<cog_core::CommandEventStream> {
                let steps = vec![
                    (
                        Duration::ZERO,
                        cog_core::CommandEvent::Stdout {
                            data: "first\n".into(),
                        },
                    ),
                    // 比节拍长得多的一段沉默：一整条命令的中间，而不是两行之间
                    (
                        TOOL_UPDATE_FLUSH * 4,
                        cog_core::CommandEvent::Stdout {
                            data: "second\n".into(),
                        },
                    ),
                    (Duration::ZERO, cog_core::CommandEvent::Exit { code: 0 }),
                ];
                Ok(Box::pin(futures::stream::unfold(
                    (steps, 0usize),
                    |(steps, index)| async move {
                        let (pause, event) = steps.get(index)?.clone();
                        if !pause.is_zero() {
                            tokio::time::sleep(pause).await;
                        }
                        Some((event, (steps, index + 1)))
                    },
                )))
            }
        }

        let registry = ToolRegistry::new();
        cog_core::ToolRegistry::register(
            &registry,
            cog_core::Tool {
                name: "run_command".into(),
                description: String::new(),
                parameters: serde_json::json!({ "type": "object" }),
                implementation: cog_core::ToolImplementation::Shell(cog_core::ShellOp::Command),
            },
        );
        let registry = registry.with_sandbox_backend(Arc::new(Chatty));

        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        let config = RuntimeConfig {
            agent_id: "chatty-0001".into(),
            max_iterations: 4,
            ..Default::default()
        };
        let mut runtime = AgentRuntime::new(config, tx).with_tools(registry);
        let llm = RunThenAnswer;
        runtime
            .run_scoped(serde_json::json!({ "task": "talk" }), &llm, Some("chatty"))
            .await
            .expect("the run reaches its answer");
        // 运行自己握着总线的写端：不先放掉它，下面那次 recv 永远等不到 None。
        drop(runtime);

        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }

        let start = events
            .iter()
            .position(|e| matches!(e, AgentEvent::ToolExecutionStart { .. }))
            .expect("the call started");
        let end = events
            .iter()
            .position(|e| matches!(e, AgentEvent::ToolExecutionEnd { .. }))
            .expect("the call ended");
        let updates: Vec<(usize, &str)> = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| match event {
                AgentEvent::ToolExecutionUpdate {
                    tool_call_id,
                    partial_result,
                    ..
                } => {
                    assert_eq!(
                        tool_call_id, "call-chatty",
                        "进度的 id 是界面拿来对上那一格的键，它必须是这次调用的"
                    );
                    partial_result.as_str().map(|text| (index, text))
                }
                _ => None,
            })
            .collect();

        assert!(
            !updates.is_empty(),
            "命令跑了几百毫秒，总线上却一帧进度都没有：开始与结束之间是空的，\
             读的人分不出它在干活还是卡住了"
        );
        assert!(
            updates.first().unwrap().0 > start && updates.last().unwrap().0 < end,
            "进度必须落在这次调用的开始与结束之间，否则界面收到的是一个没有归处的格子"
        );
        assert_eq!(
            updates[0].1, "first\n",
            "第一帧进度只该带第一段：带着全文说明它是命令结束后才发出的那一帧"
        );
        let said: String = updates.iter().map(|(_, text)| *text).collect();
        assert_eq!(
            said, "first\nsecond\n",
            "几帧进度按顺序拼起来，就是这条命令说过的全部"
        );
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
            // 跑这一趟的是一个人，有名有姓：没有 id 的运行在入口就拒绝。
            agent_id: "archivist-0001".into(),
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
            role_type: "skill-outcome-role".into(),
        };
        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "outcome-recorder-0001".into(),
            role: "skill-outcome-role".into(),
            skill_config: Some(skill),
            max_iterations: 1,
            think_stall_timeout_secs: 5,
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

    /// 「工作树没动」只对**声称要动它**的那件活是证据，与接手的人的角色无关。
    ///
    /// 判据换输入就换答案：同一份「提醒过两次、之后又走满一整段只读」的证据，
    /// 加上声明就该收，撤回声明就该放。角色名在这里不起作用——一位 generator 跑
    /// 一段读取结论的活，与一位工程师跑一件要交改动的活，各自按任务判。
    #[test]
    fn only_a_task_that_declares_a_change_can_be_stopped_for_not_making_one() {
        let declaring = cog_core::RuntimeConfig {
            agent_id: "kai-a9ab11fc".into(),
            role: cog_core::roles::PLANNER.into(),
            deliverable: cog_core::Deliverable::Change,
            ..Default::default()
        };
        let quiet = cog_core::RuntimeConfig {
            deliverable: cog_core::Deliverable::Unstated,
            ..declaring.clone()
        };

        let mut progress = CheckoutProgress::default();
        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_some());
        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_some(), "两次提醒都要响");
        // 第二次提醒把这一段清零了：此刻新的一段才刚开始，不是绕圈。
        assert!(!circling_unproductively(&declaring, &progress));
        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }

        assert!(
            circling_unproductively(&declaring, &progress),
            "声称要动这棵树、提醒过两次、之后又走满一整段只读：这就是绕圈"
        );
        assert!(
            !circling_unproductively(&quiet, &progress),
            "没有声明要动树时，「树没动」不构成证据：读结论的活本就不该动它"
        );
    }

    /// 收住要多要一整段调用：只提醒过一次——不收；提醒过两次但新的一段还没走满
    /// ——不收；走满了——收。写一次就重新算。
    ///
    /// 这里没有任何时刻参与：判据是调用次数，所以一次跑很久的调用只算一次，一条
    /// 在干活的长活不会被这一条收走。
    #[test]
    fn circling_needs_the_two_notes_and_another_full_stretch() {
        let mut progress = CheckoutProgress::default();

        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_some(), "第一次提醒必须响");
        assert_eq!(progress.notes_given, 1);
        assert!(
            !progress.circling(),
            "只提醒过一次，不收：收住得先把话说两遍"
        );

        for _ in 0..CALLS_WITHOUT_CHANGE {
            progress.note_call("run_command", &read_call());
        }
        assert!(progress.take_note().is_some(), "第二次提醒必须响");
        assert!(
            !progress.circling(),
            "刚说完第二遍就收，这一遍等于白说：提醒之后要给它一整段"
        );

        for _ in 0..CALLS_WITHOUT_CHANGE - 1 {
            progress.note_call("run_command", &read_call());
        }
        assert!(!progress.circling(), "差一次还不算走满一段");
        progress.note_call("run_command", &read_call());
        assert!(
            progress.circling(),
            "两次提醒之后又走满一整段，一次没写：必须收住"
        );

        // 写过一次就重新算：绕圈的那一段从那一次改动算起。
        progress.note_call("run_command", &write_call());
        assert!(!progress.circling(), "一次真的改动之后，那一段要重新算");
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
            (
                "edit_file",
                serde_json::json!({ "path": "a.rs", "old_string": "a", "new_string": "b" }),
                true,
            ),
            // 背景命令跑的是同一套 shell，改不改工作树取决于它跑的是什么——
            // 与 `run_command` 同一把尺，而不是按工具名一律算写。
            (
                "run_command_background",
                serde_json::json!({ "command": "grep -rn foo ." }),
                false,
            ),
            (
                "run_command_background",
                serde_json::json!({ "command": "cargo fmt --edition 2021 a.rs" }),
                true,
            ),
            ("read_file", serde_json::json!({ "path": "a.rs" }), false),
            // 树上还有两种写：落一份补丁，记一笔提交。名字都不像写文件，漏掉
            // 它们会让「这一轮在原地打转」这句提示在最该出现时不出现。
            (
                "apply_patch",
                serde_json::json!({ "patch": "--- a\n+++ b\n" }),
                true,
            ),
            (
                "git_commit",
                serde_json::json!({ "message": "fix the thing" }),
                true,
            ),
            ("git_diff", serde_json::json!({}), false),
            ("git_status", serde_json::json!({}), false),
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

    /// 需要人点头的工具，在人说「不」的时候不执行；理由回到模型手里。
    ///
    /// 这条测的是三件事一起成立：工具体一次都没跑；这一轮没有因此报错退出（理由
    /// 是给模型看的，不是给运行时的）；模型第二次被问到时，读到的是那句理由，
    /// 而不是一条空结果。少任何一条，界面上都会出现「它说它要动手，然后什么也
    /// 没发生」。
    #[tokio::test]
    async fn a_tool_that_needs_a_yes_does_not_run_when_the_answer_is_no() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Denying;
        #[async_trait::async_trait]
        impl crate::permission::PermissionDecider for Denying {
            async fn decide(
                &self,
                _ask: &crate::permission::PermissionAsk,
            ) -> crate::permission::PermissionVerdict {
                crate::permission::PermissionVerdict::Deny {
                    reason: "这一次不同意".into(),
                }
            }
        }

        /// 第一次要动 `write_file`，之后每次只交一句话；第二次被问到时把读到的
        /// 消息抄一份下来，供断言理由到没到。
        struct AskOnceThenAnswer {
            calls: AtomicUsize,
            seen: std::sync::Mutex<Vec<String>>,
        }

        #[async_trait::async_trait]
        impl cog_core::LlmClient for AskOnceThenAnswer {
            async fn chat_stream(
                &self,
                messages: &[Message],
                _options: &cog_core::ChatOptions,
            ) -> SFResult<cog_core::AssistantMessageEventStream> {
                let turn = self.calls.fetch_add(1, Ordering::SeqCst);
                if turn > 0 {
                    let mut seen = self.seen.lock().unwrap();
                    seen.extend(messages.iter().map(|m| format!("{m:?}")));
                }
                let (stream, producer) = cog_core::EventStream::with_capacity(4);
                tokio::spawn(async move {
                    let mut producer = producer;
                    let text = if turn == 0 {
                        "先动手"
                    } else {
                        "好吧，不动了"
                    };
                    let _ = producer
                        .push(AssistantMessageEvent::TextDelta {
                            content_index: 0,
                            delta: text.to_string(),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                    let mut content = vec![ContentBlock::Text {
                        text: text.to_string(),
                        text_signature: None,
                    }];
                    if turn == 0 {
                        content.push(ContentBlock::tool_call(
                            "call-write",
                            "write_file",
                            serde_json::json!({"path": "a.txt", "content": "x"}),
                        ));
                    }
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

        let ran = Arc::new(AtomicUsize::new(0));
        let registry = ToolRegistry::new();
        {
            let ran = Arc::clone(&ran);
            cog_core::ToolRegistry::register(
                &registry,
                cog_core::Tool {
                    name: "write_file".into(),
                    description: "writes a file".into(),
                    parameters: serde_json::json!({"type": "object"}),
                    implementation: cog_core::ToolImplementation::Native(Arc::new(move |_args| {
                        let ran = Arc::clone(&ran);
                        Box::pin(async move {
                            ran.fetch_add(1, Ordering::SeqCst);
                            Ok(serde_json::json!({ "written": true }))
                        })
                    })),
                },
            );
        }

        let (tx, mut rx) = mpsc::channel::<AgentEvent>(100);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let config = RuntimeConfig {
            agent_id: "needs-a-yes".into(),
            role: "needs-a-yes-test".into(),
            max_iterations: 4,
            ..Default::default()
        };
        let decider: Arc<dyn crate::permission::PermissionDecider> = Arc::new(Denying);
        let mut runtime = AgentRuntime::new(config, tx)
            .with_tools(registry)
            .with_permissions(Some(decider), crate::permission::PermissionMode::Default);
        let llm = AskOnceThenAnswer {
            calls: AtomicUsize::new(0),
            seen: std::sync::Mutex::new(Vec::new()),
        };

        runtime
            .run(serde_json::json!({"task": "写一个文件"}), &llm)
            .await
            .expect("人拒绝是一次结论，不是运行时的一次失败");

        assert_eq!(ran.load(Ordering::SeqCst), 0, "没点头的工具一次都不许跑");
        let seen = llm.seen.lock().unwrap().join("\n");
        assert!(
            seen.contains("这一次不同意"),
            "拒绝的理由要回到模型手里，模型才可能换一条路: {seen}"
        );
        assert!(
            seen.contains("permission"),
            "这一条结果要自报是「没被允许」，与一次执行失败分得开: {seen}"
        );
    }
}
