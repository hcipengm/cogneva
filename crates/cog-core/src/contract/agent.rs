//!Pure-trait control-plane contracts for Agent <-> Supervisor communication.
//!Zero `tonic`/`prost` references — concrete gRPC implementations live in
//!`cog-protocol` so that `cog-supervisor` and `cog-agent` depend only on
//!`cog-core`.

use crate::types::AgentEvent;
use crate::{InboxMessage, SFResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

/// Trait for an agent runtime that can execute a single run.
/// This abstraction lives in `cog-core` so that `cog-eval` and other
/// downstream crates can depend on the interface rather than the concrete
/// `cog-agent` crate.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn crate::llm::LlmClient,
    ) -> crate::SFResult<serde_json::Value>;

    /// Return the agent identifier assigned to this runtime.
    fn agent_id(&self) -> &str;

    /// Return the role assigned to this runtime.
    fn role(&self) -> &str;
}

/// Agent loop configuration.
#[derive(Clone)]
pub struct RuntimeConfig {
    /// 谁在跑这一轮。没有缺省值：空着就是「还没说自己是谁」，构造没问题，
    /// 开跑时会被拒绝（见运行时入口的检查）。这里曾经默认成 `"agent"`——
    /// 一个没人雇过、名册上不存在的员工，却会在账上、日志里、名册外一路留下
    /// 痕迹，读到的人都以为平台上真有这么一个人。
    pub agent_id: String,
    /// Agent role as an open string (e.g. "planner", "generator").
    /// Constants are defined in `cog-agent::AgentRole` so new roles can be
    /// added without recompiling `cog-core`.
    pub role: String,
    /// The turn ceiling for this run. `u32::MAX` is "no ceiling stated here".
    ///
    /// A run ends when the model stops asking for tools. What stops a run that
    /// never stops asking is the stall timeout, the context window and the cost
    /// ledger — not a turn count. A turn count is only ever the operator's own
    /// decision for this deployment, and it reaches here through the config
    /// surface or the role's skill, never as a default nobody measured.
    pub max_iterations: u32,
    pub context_window_size: usize,
    /// TTL for the available_skills cache in seconds.
    pub skill_cache_ttl_secs: u64,
    /// Stall timeout for a single streaming think turn, in seconds.
    ///
    /// This is NOT a whole-turn wall-clock cap: a stream that keeps producing
    /// events may run arbitrarily long (slow models legitimately spend many
    /// minutes emitting large generations). The turn is only aborted when no
    /// stream event arrives for this many consecutive seconds, which indicates
    /// a hung connection rather than a slow one.
    pub think_stall_timeout_secs: u64,
    /// Wall-clock cap for the forced final-draft ask, in seconds.
    ///
    /// The stall timeout above bounds silence, not the call: a stream that
    /// keeps trickling events stays inside it indefinitely. This bounds the
    /// call itself, and it applies to the one ask that runs *after* the run's
    /// budget is already spent — the one that must not be open-ended. It is
    /// larger than the stall timeout on purpose: recoveries that produced a
    /// deliverable have run into the hundreds of seconds, and the cap exists
    /// only so a wedged upstream cannot hold a spent run for ten minutes.
    pub final_draft_timeout_secs: u64,
    /// Optional dynamic skill configuration that overrides role defaults.
    pub skill_config: Option<crate::SkillConfig>,
    /// Optional Crew identifier — attached to lifecycle events so the hook
    /// engine can route them to Crew scope.
    pub crew_id: Option<String>,
    /// Optional Squad identifier — attached to lifecycle events so the hook
    /// engine can route them to Squad scope.
    pub squad_id: Option<String>,
    /// 这个 agent 是在替哪一件 DAG 任务干活。
    ///
    /// 与运行身份是两回事：一次运行有自己的 id（PGE 每轮一个 `ralph-pipeline-*`，
    /// 工具产物归档、沙箱工作目录都按它命名），而成本与清点要记在**那件任务**上
    /// ——否则账本与清点表里全是运行 id，按任务 id 去查成本一条都查不到，挣值的
    /// 实际成本恒为 0、预算闸永远读不到。所以运行 id 照旧，归属性另记。
    ///
    /// `None` 表示这个 agent 不属于任何任务（常驻角色、临时评测），它的调用
    /// 就按运行 id 记——不编一个任务名，编出来的会读成一件真花过钱的任务。
    pub owner_task_id: Option<String>,
    /// 这份活交什么（见 [`crate::Task::deliverable`]）。
    ///
    /// 运行时手里只有这份 config，没有任务对象，而「这次运行该不该动手、该按什么
    /// 判」是任务说的。所以拿到任务的那一层（认领、派单、小队建角色）把它抄进来；
    /// 抄的人只有一处入口，判的人只有这一位——按角色名判时，同一个人换个任务就被
    /// 判错。
    ///
    /// 缺省是 `Unstated`：没有声明就是「没说」，那就不拿「树没动」当它没进展的
    /// 证据。这不等于它不会写——它照样能写，只是不会因为没写被收住。用具名状态而
    /// 不是默认的 `WorkProduct`，是为了让「没人说」与「有人说是工作产物」在读数里
    /// 分得开。
    pub deliverable: crate::Deliverable,
    /// 上面那一格的来源属于哪一类（见 [`crate::DeclarationState`]）。
    ///
    /// 与 `deliverable` 是同一份声明的两半：这一格说「这个结论从哪儿来」，那一格
    /// 说「结论是什么」。两格一起写，写的人只有拿到任务的那一层；分开放是因为
    /// 运行期的读数要按来源计数，而结论本身回答不了「是没人说、还是说了读不出来」。
    ///
    /// 缺省 `Unstated`：没人说。它不等于「说了是工作产物」——那会把一个拼错的词
    /// 算进「生产者说过了」里。
    pub deliverable_state: crate::DeclarationState,
    /// 有人就这件活说过的方向，原文照录（见 [`crate::Task::stated_directions`]）。
    ///
    /// 与 `deliverable` 同一个理由、同一条路：运行时没有任务对象，「这一轮什么算好」
    /// 里那些来自人的话只有拿到任务的那一层抄得进来。判别的时候贴在判据后面，判据
    /// 本身不动——方向改的是「往哪儿算好」，不是「什么算完整」。
    ///
    /// 空表示没人就这件活说过方向。空不是「没有偏好」：它只说明这一条通道上没人
    /// 开口，而不是说人没有意见。
    pub stated_directions: Vec<String>,
}

impl RuntimeConfig {
    /// 这一轮是否被预期会改工作树。读声明，不另存一份布尔。
    pub fn declares_change(&self) -> bool {
        self.deliverable.declares_change()
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            agent_id: String::new(),
            role: crate::roles::PLANNER.into(),
            // No ceiling by default. Ten turns was the old default and it is
            // shorter than one round of read-change-run-test: every repair run
            // was cut off with a tool call still pending, and the round after
            // it started the same work over from the top.
            max_iterations: u32::MAX,
            // 32k fallback: 4k windows amputate tool-call history so badly that
            // CJK task prompts alone trigger trimming, leaving the model with
            // only the task text each iteration (amnesia → repeated work).
            context_window_size: 32000,
            skill_cache_ttl_secs: 30,
            think_stall_timeout_secs: 240,
            // Measured against the recoveries that kept a run alive: the
            // longest one that produced a deliverable spent 326s, and the one
            // that hung outright ran 600s. 420s clears every recovery that has
            // ever worked here with margin for the tail, and still cuts the
            // hang well below the ten minutes it used to hold a spent run.
            final_draft_timeout_secs: 420,
            skill_config: None,
            crew_id: None,
            squad_id: None,
            owner_task_id: None,
            deliverable: crate::Deliverable::Unstated,
            deliverable_state: crate::DeclarationState::Unstated,
            stated_directions: Vec::new(),
        }
    }
}

/// High-level agent trait — abstracts the concrete `cog_agent::Agent` so
/// that downstream crates (e.g. `cog-collaboration`) can create and drive
/// agents without depending on `cog-agent`.
#[async_trait]
pub trait Agent: Send + Sync {
    async fn prompt(&self, input: serde_json::Value) -> crate::SFResult<serde_json::Value>;

    /// Prompt on behalf of a concrete task. Implementations that execute
    /// tools must stamp `task_id` onto the run scope so sandbox requests carry
    /// the real DAG task identity instead of a per-call synthetic id.
    /// Default delegates to [`Agent::prompt`] (no scoping), so existing
    /// implementations compile unchanged.
    async fn prompt_for_task(
        &self,
        task_id: &str,
        input: serde_json::Value,
    ) -> crate::SFResult<serde_json::Value> {
        let _ = task_id;
        self.prompt(input).await
    }

    /// The change a run left in its checkout, read from the tree rather than
    /// from the text the model wrote out.
    ///
    /// A diff the model types by hand is prose about a file: its path headers,
    /// context lines and hunk counts are whatever the model remembered, and any
    /// of them can disagree with the file the apply gate reads. A run that was
    /// given a checkout and edited it leaves the same change as a diff git
    /// produced from the file's real bytes, which cannot disagree with the tree
    /// it is applied to. Implementations that can run a command in the run's
    /// checkout return `Some(diff)` when that tree holds one.
    ///
    /// `None` means no tree could be read — no checkout, no backend, a run that
    /// left its tree untouched, or a name in `exclude` that cannot be named to a
    /// shell. That is not "no change": a caller keeps whatever it already had,
    /// so this can only replace a diff with one the tree itself produced.
    ///
    /// `exclude` names files the caller does not want in the diff. A caller's
    /// own artifact is written by the run and read back by the caller, so it is
    /// a report of the change rather than part of it; a diff that carried the
    /// report would be applied as a change that creates it. A name is passed to
    /// the command, so an entry that is not a plain file name makes the read
    /// fail rather than reach the shell. Default: `None`.
    async fn workspace_change(
        &self,
        _task_id: &str,
        _exclude: &[String],
    ) -> crate::SFResult<Option<String>> {
        Ok(None)
    }

    async fn start(&self);
    async fn snapshot(&self, task_id: String) -> crate::SFResult<crate::snapshot::AgentCheckpoint>;
    async fn restore(&self, snapshot: &crate::snapshot::AgentCheckpoint) -> crate::SFResult<()>;

    /// Continue the conversation with additional input. Context is preserved
    /// across calls.
    async fn continue_(&self, input: serde_json::Value) -> crate::SFResult<serde_json::Value>;

    /// Send a steering instruction (injected as a system message).
    async fn steer(&self, instruction: String) -> crate::SFResult<()>;

    /// Abort the current run and reset to idle.
    async fn abort(&self) -> crate::SFResult<()>;

    /// Reset the agent, clearing all context and state.
    async fn reset(&self) -> crate::SFResult<()>;

    /// Get the current agent state.
    async fn state(&self) -> crate::SFResult<crate::AgentState>;

    /// Wait until the agent is no longer active or completing.
    async fn wait_for_idle(&self) -> crate::SFResult<()>;

    /// Restore agent state from a persisted checkpoint by id.
    async fn restore_from_id(&self, checkpoint_id: &str) -> crate::SFResult<()>;

    /// Subscribe to agent lifecycle events.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::AgentEvent>;

    /// Direct streaming access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    async fn chat_stream(
        &self,
        messages: &[crate::Message],
        options: &crate::ChatOptions,
    ) -> crate::SFResult<crate::AssistantMessageEventStream>;

    /// Direct streaming completion access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    async fn complete_stream(
        &self,
        prompt: &str,
        options: &crate::CompleteOptions,
    ) -> crate::SFResult<crate::AssistantMessageEventStream>;

    /// Read a field from the shared ContextBoard for the given task.
    async fn read_board(&self, task_id: &str, field: &str) -> crate::SFResult<Option<String>>;

    /// Write a field to the shared ContextBoard for the given task.
    async fn write_board(&self, task_id: &str, field: &str, value: &str) -> crate::SFResult<()>;

    /// Receive an external inbox message directed at this agent.
    /// The agent delivers the message to its configured message backend so that
    /// the inbox consumer can pick it up.
    async fn receive_message(&self, msg: InboxMessage) -> crate::SFResult<()>;

    /// Review an output string via the agent's self-review capability.
    /// Returns a [`crate::types::self_review::SelfReviewResult`] indicating pass or need-revision.
    /// Default: always pass.  Concrete implementations in `cog-agent` override
    /// this with the full SelfReviewLoop logic.
    async fn review_output(
        &self,
        _output: &str,
        _config: &crate::SelfReviewConfig,
    ) -> crate::SFResult<crate::SelfReviewResult> {
        Ok(crate::SelfReviewResult::Pass {
            score: 1.0,
            summary: "default pass".into(),
        })
    }

    /// Review an output and return the (possibly revised) text together with
    /// the review result. Default: delegates to [`Self::review_output`] and
    /// returns the original output unchanged. Concrete implementations in
    /// `cog-agent` override this to surface the SelfReviewLoop revision.
    async fn review_and_revise(
        &self,
        output: &str,
        config: &crate::SelfReviewConfig,
    ) -> crate::SFResult<(String, crate::SelfReviewResult)> {
        let result = self.review_output(output, config).await?;
        Ok((output.to_string(), result))
    }

    /// Review an output on behalf of the DAG task `task_id`.
    ///
    /// 与 [`Self::review_and_revise`] 分开，是因为「审的是谁的东西」与「这份东西
    /// 是哪件活」是两个身份，而账本分两格记：只留角色，按任务读成本的那一面就
    /// 读不到自检这一族的花销。没有任务的调用走默认实现，与不带任务时逐字相同。
    async fn review_and_revise_for_task(
        &self,
        output: &str,
        config: &crate::SelfReviewConfig,
        _task_id: &str,
    ) -> crate::SFResult<(String, crate::SelfReviewResult)> {
        self.review_and_revise(output, config).await
    }
}

/// Commands that the Supervisor may send to an Agent.
#[derive(Debug, Clone)]
pub enum AgentCommand {
    Kill { reason: String },
    Restart { preserve_context: bool },
    Checkpoint { task_id: String },
    ConfigUpdate { config_json: Vec<u8> },
}

/// Client-side trait — implemented by `cog-protocol` and consumed by `cog-agent`.
#[async_trait]
pub trait AgentLifecycleClient: Send + Sync {
    /// Send a unary heartbeat to the Supervisor.
    async fn heartbeat(&self, agent_id: &str, state: &str) -> SFResult<()>;

    /// Open a Server-Streaming RPC that yields commands from the Supervisor.
    async fn subscribe_commands(
        &self,
        agent_id: &str,
    ) -> SFResult<BoxStream<'static, AgentCommand>>;

    /// Report a single low-frequency critical event (AgentError, ResourceAlert,
    /// TaskStatusChange, StateChange, etc.) via Unary RPC.
    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()>;

    /// Upload a batch of high-frequency events (MessageUpdate, ReAct steps,
    /// tool execution progress, etc.) via Client Streaming RPC.
    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32>;
}

/// Server-side trait — implemented by `cog-protocol` and consumed by `cog-supervisor`.
#[async_trait]
pub trait AgentLifecycleServer: Send + Sync {
    /// Push a command to the specified agent.
    async fn push_command(&self, agent_id: &str, command: AgentCommand) -> SFResult<()>;

    /// Convenience: ask an agent to kill itself.
    async fn kill(&self, agent_id: &str, reason: &str) -> SFResult<bool> {
        self.push_command(
            agent_id,
            AgentCommand::Kill {
                reason: reason.into(),
            },
        )
        .await?;
        Ok(true)
    }

    /// Convenience: ask an agent to restart.
    async fn restart(&self, agent_id: &str, preserve_context: bool) -> SFResult<bool> {
        self.push_command(agent_id, AgentCommand::Restart { preserve_context })
            .await?;
        Ok(true)
    }

    /// Convenience: ask an agent to checkpoint.
    async fn checkpoint(&self, agent_id: &str, task_id: &str) -> SFResult<String> {
        self.push_command(
            agent_id,
            AgentCommand::Checkpoint {
                task_id: task_id.into(),
            },
        )
        .await?;
        Ok(task_id.into())
    }

    /// List currently connected agent IDs.
    async fn connected_agents(&self) -> SFResult<Vec<String>>;

    /// Receive a single critical event reported by an agent via Unary RPC.
    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()>;

    /// Receive a batch of high-frequency events from an agent via Client Streaming RPC.
    /// Returns the number of events accepted.
    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32>;
}

// Agent registration & heartbeat protocol.
// deterministic agent_id (blake3 of hostname + pod_ip + role + uuid_v7),
// Redis TTL registration with heartbeat renewal, and a stable
// [`AgentRegistry`] trait that can be backed by Redis or memory.

/// Resource hints attached to a registration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ResourceInfo {
    pub cpu_cores: u32,
    pub memory_gb: u32,
}

/// Registration payload submitted by an Agent on startup and persisted in Redis.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRegistration {
    pub agent_id: String,
    pub role: String,
    pub workspace_id: String,
    pub capabilities: Vec<String>,
    pub resources: ResourceInfo,
    pub hostname: String,
    pub pod_ip: String,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

impl AgentRegistration {
    /// Build a registration with an externally-supplied `agent_id`.
    /// Callers (e.g. `cog-storage` or the binary crate) are responsible for
    /// generating the id so that `cog-core` stays free of hashing logic.
    pub fn new(
        agent_id: impl Into<String>,
        hostname: impl Into<String>,
        pod_ip: impl Into<String>,
        role: impl Into<String>,
        workspace_id: impl Into<String>,
        capabilities: Vec<String>,
        resources: ResourceInfo,
    ) -> Self {
        let now = Utc::now();
        Self {
            agent_id: agent_id.into(),
            role: role.into(),
            workspace_id: workspace_id.into(),
            capabilities,
            resources,
            hostname: hostname.into(),
            pod_ip: pod_ip.into(),
            registered_at: now,
            last_heartbeat: now,
        }
    }
}

/// 一个员工在名册上待了多久、现在还是不是在职。
///
/// 与 [`AgentRegistration`] 不是同一件事：注册记录只说明「此刻有进程在替这个
/// 名字心跳」，键一过期就什么都没有了；名册行说的是「这个人存在过」，退役之后
/// 仍然留一行。两者分开，离职与「从来没有过这个人」才在读数上分得开。
///
/// 名册行由注册表的写点顺带写出（`register` 建行、`deregister` 盖墓碑），所以
/// 任何会调模型的智能体——常驻员工、按任务现起的成员、对话员工、外部工作单元
/// ——只要走同一条注册路径，就不可能漏在名册外面。名册漏人是这个字段存在之前
/// 的常态：探针与按任务起的成员从来不进任何名单。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmployeeRecord {
    pub agent_id: String,
    /// 工号。建行时从全局计数器上取一个，此后不改、不回收——退役的人留着它，
    /// 正如离职员工保留工号。缺省是 `None`：这一格是后加的，早于它的名册行
    /// 里没有工号，读到 `None` 的人要说「还没编号」，不能拿 0 冒充。
    #[serde(default)]
    pub number: Option<u32>,
    /// 建行时注册表上写的用途标签。它随注册更新，因为它描述的是「此刻这个人
    /// 在做什么」，不是一段写死的性格。
    pub role: String,
    /// 第一次出现的时间。只在建行时写一次，退役后重建这个人也保留原来那一格
    /// ——否则「在职多久」会跟着每一次重启重新计时。
    pub first_seen: DateTime<Utc>,
    /// 最后一次被看到的时间。
    pub last_seen: DateTime<Utc>,
    /// 退役的时间。`None` 是在职。缺省即为 `None`：名册行是另一套写入方
    /// （Redis 里的那段脚本擦掉这一格而不是写 null）写出来的，两侧都得读得回
    /// 同一个形状。
    #[serde(default)]
    pub retired_at: Option<DateTime<Utc>>,
}

impl EmployeeRecord {
    /// 这个人现在还算不算在职。
    pub fn is_active(&self) -> bool {
        self.retired_at.is_none()
    }

    /// 名册、对话、日志里显示的那一行：`E0412 林清和`。
    ///
    /// 展示名只有这一个来源。调用方不要再各自拼一遍「人名#短码」——那样拼
    /// 出来的名字与名册上那一行对不上，读的人会以为是两个人。
    pub fn label(&self) -> String {
        match self.number {
            Some(number) => crate::employee_label(number),
            None => crate::unregistered_label(&self.agent_id),
        }
    }
}

/// Registry abstraction so callers can plug Redis (production) or in-memory (tests).
#[async_trait]
pub trait AgentRegistry: Send + Sync {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()>;
    async fn heartbeat(&self, agent_id: &str) -> SFResult<()>;
    async fn deregister(&self, agent_id: &str) -> SFResult<()>;
    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>>;
    async fn list(&self) -> SFResult<Vec<AgentRegistration>>;
    /// List agents filtered by role.
    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>>;
    /// List agents that have all the given capabilities.
    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>>;
    /// 名册上的全部行，在岗的与退役的都在里面。
    ///
    /// 默认实现返回空：没有名册的实现（测试替身、嵌入式装配）不该被迫编一份
    /// 出来，而「读不到名册」与「名册上没有人」在调用方那一侧是不同的两句话，
    /// 这个默认值把前者留给它自己去说。
    async fn employees(&self) -> SFResult<Vec<EmployeeRecord>> {
        Ok(Vec::new())
    }

    /// 名册上一个人的那一行；不在册返回 `None`。
    ///
    /// 与 [`AgentRegistry::employees`] 分开，是因为读一个人的工号不该把整张
    /// 名册搬过来：入职接口、对话页的称呼都在调用最频繁的那条路上。
    async fn employee(&self, _agent_id: &str) -> SFResult<Option<EmployeeRecord>> {
        Ok(None)
    }
}

// ─── Agent Manager ─────────────────────────────────────────────────────────

/// Lightweight snapshot of a worker agent managed by the agent manager.
/// Does not contain the concrete [`Agent`] handle so that `cog-core`
/// remains decoupled from `cog-agent` internals.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
    /// The task this worker was created for, when it has one.
    ///
    /// `None` is the worker that lives as long as the process — an employee of
    /// the platform rather than a hand hired for one task. A caller that wants
    /// to hand out work and get it back needs this distinction: a worker owned
    /// by another task is already in the middle of that task's conversation,
    /// and handing it a second piece of work either queues behind it or leaves
    /// the two reading each other's steps.
    pub owner_task: Option<String>,
}

/// Agent manager for creating, discovering, and communicating with agent instances.
/// Serves as the service-level gateway to object-level [`Agent`] capabilities.
#[async_trait]
pub trait AgentManager: Send + Sync {
    /// Create and start a new agent worker with the given role and LLM provider.
    /// Returns a shareable handle to the object-level [`Agent`] trait.
    ///
    /// `task_id` is the task this agent works for, or `None` for an agent that
    /// lives as long as the process. It is the only thing that ties an object
    /// to the task whose terminal state should let it go: names differ per role
    /// and per branch, so nothing can recover the owner from the name, and a
    /// name-shaped guess would retire an agent on another task's completion.
    async fn create_agent(
        &self,
        agent_id: &str,
        role: &str,
        llm: std::sync::Arc<dyn crate::llm::LlmClient>,
        task_id: Option<&str>,
    ) -> crate::SFResult<std::sync::Arc<dyn crate::Agent>>;

    /// [`Self::create_agent`], plus what the task says about the run's shape.
    ///
    /// 「这件活交什么」只有拿着任务的那一层答得出来（[`crate::Task::deliverable`]），
    /// 而判它的是运行时。缺省实现忽略这一位、退化成 [`Self::create_agent`]：一个
    /// 还不知道怎么用它的管理器不会因此编造答案，只是那位 agent 拿着 `Unstated`
    /// 跑——而这正是没有声明时的行为。真正把它传进运行时的管理器要覆盖它。
    ///
    /// `stated_directions` 是同一份声明的另一半：有人就这件活说过的方向（见
    /// [`crate::Task::stated_directions`]）。它和 `deliverable` 一起传，是因为两者
    /// 回答的是同一次运行的两个问题——「交什么」与「往哪儿算好」——只传前一个
    /// 的话，判活的标准仍是没人说话的那一份。
    ///
    /// `deliverable` 传的是整份声明而不是结论：[`crate::DeliverableDeclaration`]
    /// 带着来源，运行时靠它把「生产者说的」「平台推的」「没人说」「说了读不出来」
    /// 分成四个可计数的来源。只传结论的话，这四个到了运行时就只剩一个。
    async fn create_agent_declaring(
        &self,
        agent_id: &str,
        role: &str,
        llm: std::sync::Arc<dyn crate::llm::LlmClient>,
        task_id: Option<&str>,
        deliverable: crate::DeliverableDeclaration,
        stated_directions: Vec<String>,
    ) -> crate::SFResult<std::sync::Arc<dyn crate::Agent>> {
        let _ = (deliverable, stated_directions);
        self.create_agent(agent_id, role, llm, task_id).await
    }

    /// Retire every agent this manager created for `task_id`, returning how many
    /// it let go.
    ///
    /// Called once a task has reached a terminal state, which is the only point
    /// where this is safe: a task that goes back on the queue is executed again
    /// and must keep the object it already has, while a terminal task is never
    /// requeued, so nothing can create a same-named successor underneath the
    /// retirement.
    ///
    /// An agent whose retirement could not stop its consumer inside the budget
    /// is still retired: the count reports what left the roster, and the
    /// timeout is reported as its own series rather than folding the two into
    /// one number.
    async fn retire_task_agents(&self, task_id: &str) -> crate::SFResult<usize>;

    /// Dispatch a message using round-robin selection among managed agents.
    async fn dispatch(&self, msg: InboxMessage) -> crate::SFResult<()>;

    /// Return a snapshot of currently live managed agents.
    async fn list_workers(&self) -> crate::SFResult<Vec<WorkerInfo>>;

    /// Gracefully shutdown all managed agents: abort and deregister.
    async fn shutdown(&self) -> crate::SFResult<()>;

    /// Get a managed agent instance by its `agent_id`.
    /// Returns `Arc<dyn Agent>` so downstream can invoke the full object-level
    /// [`Agent`] trait (prompt, steer, snapshot, etc.).
    async fn get_agent(
        &self,
        agent_id: &str,
    ) -> crate::SFResult<Option<std::sync::Arc<dyn crate::Agent>>>;

    /// Let go of the worker named `agent_id`, returning whether one left.
    ///
    /// The rostered employees are not created by a task, so no task completing
    /// ever retires them: a process that stops being the one that serves a name
    /// has no other way to give it up. Stopping its inbox matters as much as
    /// leaving the roster -- a retired consumer keeps reading the group and
    /// answering with a loop nobody asked to run.
    ///
    /// A manager that cannot retire by name answers `false` rather than
    /// guessing; `true` is the claim that the name is now free here.
    async fn retire_agent(&self, agent_id: &str) -> crate::SFResult<bool> {
        let _ = agent_id;
        Ok(false)
    }
}

/// Generate a deterministic agent_id from the four input dimensions.
pub fn generate_agent_id(hostname: &str, pod_ip: &str, role: &str, uuid: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(hostname.as_bytes());
    hasher.update(b"|");
    hasher.update(pod_ip.as_bytes());
    hasher.update(b"|");
    hasher.update(role.as_bytes());
    hasher.update(b"|");
    hasher.update(uuid.as_bytes());
    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal Agent that records the input seen by `prompt` so the default
    /// `prompt_for_task` delegation can be verified without cog-agent.
    struct PromptOnlyAgent;

    #[async_trait::async_trait]
    impl Agent for PromptOnlyAgent {
        async fn prompt(&self, input: serde_json::Value) -> crate::SFResult<serde_json::Value> {
            Ok(serde_json::json!({ "saw": input }))
        }
        async fn start(&self) {}
        async fn snapshot(
            &self,
            _task_id: String,
        ) -> crate::SFResult<crate::snapshot::AgentCheckpoint> {
            unimplemented!()
        }
        async fn restore(
            &self,
            _snapshot: &crate::snapshot::AgentCheckpoint,
        ) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn continue_(&self, _input: serde_json::Value) -> crate::SFResult<serde_json::Value> {
            unimplemented!()
        }
        async fn steer(&self, _instruction: String) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn abort(&self) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn reset(&self) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn state(&self) -> crate::SFResult<crate::AgentState> {
            unimplemented!()
        }
        async fn wait_for_idle(&self) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn restore_from_id(&self, _checkpoint_id: &str) -> crate::SFResult<()> {
            unimplemented!()
        }
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::AgentEvent> {
            unimplemented!()
        }
        async fn chat_stream(
            &self,
            _messages: &[crate::Message],
            _options: &crate::ChatOptions,
        ) -> crate::SFResult<crate::AssistantMessageEventStream> {
            unimplemented!()
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &crate::CompleteOptions,
        ) -> crate::SFResult<crate::AssistantMessageEventStream> {
            unimplemented!()
        }
        async fn read_board(
            &self,
            _task_id: &str,
            _field: &str,
        ) -> crate::SFResult<Option<String>> {
            unimplemented!()
        }
        async fn write_board(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> crate::SFResult<()> {
            unimplemented!()
        }
        async fn receive_message(&self, _msg: InboxMessage) -> crate::SFResult<()> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn prompt_for_task_default_delegates_to_prompt() {
        let agent = PromptOnlyAgent;
        let out = agent
            .prompt_for_task("dag-task-1", serde_json::json!({"k": "v"}))
            .await
            .unwrap();
        // The blanket default carries no scoping machinery; it must behave
        // exactly like prompt for implementors that do not override it.
        assert_eq!(out["saw"]["k"], "v");
    }

    #[test]
    fn agent_registration_construction() {
        let reg = AgentRegistration::new(
            "id-1",
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec!["llm".into()],
            ResourceInfo::default(),
        );
        assert_eq!(reg.agent_id, "id-1");
        assert_eq!(reg.hostname, "host-1");
        assert_eq!(reg.role, "planner");
    }
}
