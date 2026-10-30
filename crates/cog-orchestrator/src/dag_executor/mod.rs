use std::collections::HashMap;
use std::sync::Arc;

use cog_core::{DagMessage, MessageBackend, SFError, SFResult, ShutdownSignal, Task};
use futures::StreamExt;

/// Loop name reported through the background-loop liveness family.
pub const RESULT_RECLAIM_LOOP: &str = "orchestrator_result_reclaim";
/// Loop name reported through the background-loop liveness family.
pub const TASK_CONSUMER_LOOP: &str = "orchestrator_task_consumer";
/// Loop name reported through the background-loop liveness family.
pub const GOAL_CONSUMER_LOOP: &str = "orchestrator_goal_consumer";
/// Loop name reported through the background-loop liveness family.
pub const ORPHAN_RECONCILER_LOOP: &str = "orchestrator_orphan_reconciler";

/// 按历史给新任务排期，至少要这么多条跑完的样本。
///
/// 低于这个数，「中位数」只是最近几次的噪声，而被它排出来的期会被甘特与挣值
/// 当成计划：一条三小时的任务按两个十分钟的样本排成二十分钟，会让它在计划上
/// 「早就该完了」——一个凭空造出来的落后。样本不够就不排，读数上读作「还没
/// 样本」，那是真话。
// cap: bound — 样本不够就不排程；两三个样本排出来的计划会让一条任务在计划上「早就该完了」，那是凭空造出来的落后
pub const MIN_ESTIMATE_SAMPLES: u64 = 5;

/// 估算基线版本串的开头，两种写法共有的那一段：「这一行是估算器按历史写的」。
///
/// 分解算出来的、人排的基线写的是别的（有的直接是时刻），它们不是估算，
/// 估算器不重算也不覆盖。
const ESTIMATE_REV_PREFIX_ANY: &str = "estimate/";

/// 估算基线版本串的开头，连同量出这条工期的尺子：`estimate/work-time-v1/…`。
///
/// 版本串要回答「这一行是谁、按什么算出来的」，因为这个字段也是重算唯一的
/// 依据：尺子换过以后，按旧尺子写的行止期不再可信，得按新的重写一遍，而
/// 「哪把尺子」只能从行上读，日志不会留在库里。旧写法
/// `estimate/<kind>/n<samples>/<rfc3339>` 的第三段就是时刻，没有位置记尺子，
/// 所以改成把尺子放在种类前面：种类里即便带斜杠，也挤不到它前面去。
const ESTIMATE_REV_PREFIX: &str = "estimate/work-time-v1/";

/// 从估算器自己写的版本串里取回种类。
///
/// 尾两段是样本条数与时刻，中间剩下的全部是种类——种类可能带斜杠，所以两头
/// 各剥一段，不按位置取。
fn estimate_rev_kind(rev: &str) -> Option<String> {
    let rest = rev.strip_prefix(ESTIMATE_REV_PREFIX_ANY)?;
    let (rest, _written_at) = rest.rsplit_once('/')?;
    let (kind, _samples) = rest.rsplit_once('/')?;
    if kind.is_empty() {
        None
    } else {
        Some(kind.to_string())
    }
}

pub mod circuit_registry;
pub mod orchestrator;
pub mod retry_matrix;
pub mod task_checkpoint;
pub mod task_phase;

pub use circuit_registry::CircuitBreakerRegistry;
pub use orchestrator::DagExecutor;
pub use retry_matrix::{BackoffStrategy, CircuitBreakerConfig, RetryConfig, RetryMatrix};
pub use task_checkpoint::{
    checkpoint_task, checkpoint_task_with_timeout, CheckpointAgents, CheckpointRound,
    LiveCheckpointAgents, CHECKPOINT_STEP_TIMEOUT,
};
pub use task_phase::{ExitCriteria, PhaseTransitionRules, PhasedTask, TaskPhase};

/// DagExecutor 配置。
#[derive(Debug, Clone)]
pub struct DagExecutorConfig {
    pub redis_url: String,
    pub workspace_id: String,
    pub consumer_group: String,
    pub max_retries: u32,
    /// 结果流上一条消息允许保持未 ack 的时长，超过即被清扫器认领并重走处理。
    /// 必须大于最长处理时长，否则正在处理的消息会被判死而并发重投。
    pub result_claim_idle_secs: u64,
    /// 结果流 pending 清扫的节拍。
    pub result_claim_interval_secs: u64,
    /// 单轮最多认领多少条。
    pub result_claim_batch: usize,
    /// 就绪发布循环的节拍，秒；`0` 表示调用方没有配置，也就没有「下一次
    /// 什么时候看」可说。只用于把准入拒绝的等待时刻写进日志——真正驱动循环
    /// 的节拍由发布方自己的 ticker 决定。
    pub ready_task_poll_interval_secs: u64,
}

impl Default for DagExecutorConfig {
    fn default() -> Self {
        Self {
            redis_url: String::new(),
            workspace_id: String::new(),
            consumer_group: String::new(),
            max_retries: 3,
            result_claim_idle_secs: 600,
            result_claim_interval_secs: 60,
            result_claim_batch: 16,
            ready_task_poll_interval_secs: 0,
        }
    }
}

/// The most one pass puts on a queue when nothing measured the pool.
///
/// Every other loop in this crate hands over a slice per pass rather than
/// draining its backlog, and the ready publisher was the one that did not: with
/// no window bound its batch was the whole ready set. That is not a theoretical
/// bound -- a pod that has just started has read no pool status yet, which is
/// exactly a rollout, and a two-thousand-row ready set went out in one tick
/// against a transport still loading its dataset; the writes were refused one
/// message at a time and the rows sat in `Scheduled` until the stall sweep
/// re-armed them.
///
/// The number is **chosen, not derived**, and it says so: nothing in this tree
/// knows what one hand-over costs, so it cannot be solved from "one pass must
/// be paid for inside one pass". It is the same borrow the change ledger's own
/// hand-over ceiling makes, at the same size, and the reading that says whether
/// it is too tight or too loose is `cogneva_ready_admission_held_total` with
/// `reason="pass_ceiling"`: a healthy publisher holds nothing under that label,
/// and one that holds a large share of every pass is a ceiling that should be
/// re-tuned or a pool reading that never arrives.
// cap: bound — 落进 pass_ceiling 标签的计数是它的读数：健康的发布者在那里占用为零，占了大头的说明这个数该重调
const READY_TASKS_PER_PASS: u64 = 200;

/// What one pass may admit, together with the reading it was derived from.
///
/// The pace rides along so a refusal can be logged with the numbers that
/// produced it: "held three back" alone cannot distinguish a window that is
/// nearly spent from one that pays for nothing, and those are different
/// outages.
#[derive(Debug, Clone, Copy)]
struct AdmissionBound {
    /// The bound, or `None` when a status was read but its measurement cannot
    /// bound the whole pool. That is not the same as "no status at all", which
    /// is `None` at the call site: the coverage cells are what tell a reader
    /// which of the two it is looking at.
    window_jobs: Option<u64>,
    pace: cog_core::LlmWindowPace,
    pool_size: usize,
    /// The pool's own verdict, carried beside the arithmetic because it
    /// overrides it: `jobs_the_window_can_pay_for_status` answers zero for an
    /// unavailable pool whatever the allowance says, so a hold decided this way
    /// has a zero that came from availability and not from the window. Without
    /// this field the refusal prints the window's numbers as its reason, and
    /// the reader is told to wait out a window that already pays for a job.
    unavailable: bool,
    /// The earliest instant the gateway will probe the pool again, unix
    /// seconds; `0` when nothing is suspect.
    ///
    /// This is what makes the unavailable period a **degradation with an end**
    /// rather than a silent freeze: while the pool is unavailable every job is
    /// held whatever the allowance pays for, the hold is not a drop (the task
    /// stays ready and re-asks on the next pass), and this instant is the
    /// earliest one at which the reason can change. It is our retry cadence,
    /// not evidence about the upstreams -- `evidenced_recovery_unix` is that --
    /// so a reader must not present it as when the window reopens.
    retry_at_unix: i64,
}

/// The board's per-column WIP limits for this process, read once.
///
/// Entries that cannot be used are reported here, once, where an operator can
/// see them: a misspelt column that was quietly dropped is a configuration that
/// reads like a limit and blocks nothing, which is the failure the limit exists
/// to make impossible.
fn load_board_wip_limits() -> cog_core::BoardWipLimits {
    let (limits, problems) = cog_core::BoardWipLimits::from_env();
    for problem in problems {
        tracing::warn!(
            variable = cog_core::config::BOARD_WIP_LIMITS_ENV,
            problem = %problem,
            "board WIP limit entry ignored"
        );
    }
    limits
}

/// DagExecutor 运行时。
/// 负责 message-backend 连接、DAG 编排、Agent 消费。
#[derive(Clone)]
pub struct DagExecutorRuntime {
    config: DagExecutorConfig,
    backend: Arc<dyn MessageBackend>,
    orchestrator: Arc<DagExecutor>,
    action_planner: Option<Arc<dyn cog_core::ActionPlanner>>,
    skill_registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    /// Where the pool's window allowance is read from, attached at start.
    ///
    /// A cell rather than a plain field because the publisher is built during
    /// `init`, when the service table does not yet hold the supervisor's
    /// source -- plugins publish in a dependency order this crate does not
    /// control. `start` runs after every `init` has finished, so the attachment
    /// happens there and the cell is shared by every clone of the runtime.
    /// Empty means no source was published (a single-process deployment),
    /// which admits everything: an unread pool is not an empty one.
    pool_source: Arc<tokio::sync::OnceCell<Arc<dyn cog_core::LlmPoolStatusSource>>>,
    /// 每条任务自己那一轮跑下来花掉的 token。
    ///
    /// 与 `pool_source` 同一个形状、同一个理由：发布它的 crate 与本 crate 的
    /// `init` 谁先谁后不由这里决定，而 `start` 是所有 `init` 都跑完之后才开始的，
    /// 所以挂在这一格上、由 `start` 填。空表示没有这个读面（进程内后端），
    /// 那时计划预算不设界——读不到一个花销不等于花销是零。
    observability: Arc<tokio::sync::OnceCell<Arc<dyn cog_core::ObservabilityGateway>>>,
    /// 看板每一列的在制品上限，构造时从环境读一次。
    ///
    /// 读一次而不是每轮读：限制是配置，不是读数；每轮读环境会让同一份配置在
    /// 两个进程里给出两个答案，而派发与看板读面必须按同一个数拦。
    board_wip_limits: cog_core::BoardWipLimits,
}

impl DagExecutorRuntime {
    pub fn new_with_backend(
        config: DagExecutorConfig,
        backend: impl MessageBackend + 'static,
    ) -> Self {
        let orchestrator = DagExecutor::new(config.workspace_id.clone());
        Self {
            config,
            backend: Arc::new(backend),
            orchestrator: Arc::new(orchestrator),
            action_planner: None,
            skill_registry: None,
            pool_source: Arc::new(tokio::sync::OnceCell::new()),
            observability: Arc::new(tokio::sync::OnceCell::new()),
            board_wip_limits: load_board_wip_limits(),
        }
    }

    pub fn new_with_dyn_backend(
        config: DagExecutorConfig,
        backend: Arc<dyn MessageBackend>,
    ) -> Self {
        let orchestrator = DagExecutor::new(config.workspace_id.clone());
        Self {
            config,
            backend,
            orchestrator: Arc::new(orchestrator),
            action_planner: None,
            skill_registry: None,
            pool_source: Arc::new(tokio::sync::OnceCell::new()),
            observability: Arc::new(tokio::sync::OnceCell::new()),
            board_wip_limits: load_board_wip_limits(),
        }
    }

    /// 换一套列上限。生产路径只从环境读（[`load_board_wip_limits`]），这个入口
    /// 给测试用：一个只能在改环境变量下才验证得了的准入，会和同进程里别的用例
    /// 抢同一份环境。
    pub fn with_board_wip_limits(mut self, limits: cog_core::BoardWipLimits) -> Self {
        // 同一份上限也推进执行器：发布侧拦「进 scheduled」，执行器拦「进
        // running」。只喂一边的那天，看板上的上限与真正拦住的那个数就分家了。
        self.orchestrator.set_board_wip_limits(limits.clone());
        self.board_wip_limits = limits;
        self
    }

    /// Point the ready-queue publisher at the pool's own reading.
    ///
    /// Called from `start`; setting it a second time is a no-op and says so,
    /// because two sources would mean two rulers for one budget.
    pub fn attach_pool_status_source(&self, source: Arc<dyn cog_core::LlmPoolStatusSource>) {
        if self.pool_source.set(source).is_err() {
            tracing::warn!("pool status source already attached; keeping the first one");
        }
    }

    /// Point the publisher at the per-task token census.
    ///
    /// Called from `start`, for the same reason the pool source is: the crate
    /// that publishes this reading and this one both publish during `init`, in
    /// an order neither controls. Setting it twice is refused and said out
    /// loud -- two censuses would be two readings of one number, and a budget
    /// judged against either would be a budget whose verdict depends on which
    /// one won.
    pub fn attach_observability(&self, gateway: Arc<dyn cog_core::ObservabilityGateway>) {
        if self.observability.set(gateway).is_err() {
            tracing::warn!("token census already attached; keeping the first one");
        }
    }

    pub fn with_orchestrator(mut self, orchestrator: Arc<DagExecutor>) -> Self {
        self.orchestrator = orchestrator;
        self
    }

    pub fn with_action_planner(mut self, planner: Arc<dyn cog_core::ActionPlanner>) -> Self {
        self.action_planner = Some(planner);
        self
    }

    pub fn with_skill_registry(
        mut self,
        registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
    ) -> Self {
        self.skill_registry = registry;
        self
    }

    /// Return a reference to the internal orchestrator.
    pub fn orchestrator(&self) -> &Arc<DagExecutor> {
        &self.orchestrator
    }

    pub async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()> {
        self.orchestrator.submit_goal(goal, tasks).await
    }

    /// Scan the DAG for ready tasks and publish them to the message backend.
    /// Each ready task is transitioned to `Scheduled` and then the tasks are
    /// published in one batch per queue via [`MessageBackend::publish_batch`]
    /// for lower latency and higher throughput.
    ///
    /// The size check runs **before** the transition, not after it: scheduling
    /// is a one-shot, and a task marked scheduled whose payload the transport
    /// then refuses stays scheduled forever, because the ready scan only ever
    /// looks at pending tasks. So each payload is measured against the bound its
    /// own transport declares, and measured ahead of *its own* transition.
    ///
    /// Measured per payload, not per call: a payload over the bound is held back
    /// at its pending state and offered again on the next scan, while the rest
    /// of its queue and every other queue still go out. Refusing the whole call
    /// would let one task that can never fit stop every queue from dispatching —
    /// the same "a permanent failure blocks everything behind it" shape this
    /// check exists to remove. It would also make every caller that reads an
    /// error here as "do not ack the result" hold its message for redelivery
    /// forever, so the stalls would spread to the result stream too.
    ///
    /// A task that cannot fit is logged with its id; what should eventually
    /// happen to it — no bound it can satisfy means no publish, ever — is a
    /// separate decision about a terminal state, not something to invent here.
    /// The publish path keeps its own check as the last line of defence.
    ///
    /// Admission is the second bound, and it is the pool's rather than the
    /// transport's: a task whose request the gateway is going to refuse is not
    /// dispatched, however well it fits. It is measured here, before the
    /// transition, for the same reason the payload check is: a task held back
    /// stays ready for the next pass, while a task marked scheduled and then
    /// dropped is one the ready scan never returns to. What the window cannot
    /// pay for stays `Pending`, which is the same state a task waiting on a
    /// dependency sits in — no new state, and no reading that claims a task is
    /// running when nothing picked it up.
    ///
    /// A task whose artifact is only readable by its producer does not go on
    /// the queue every worker competes for; see [`crate::ready_queue`].
    pub async fn publish_ready_tasks(&self) -> SFResult<()> {
        let work = self.orchestrator.ready_work().await;
        let columns = work.columns;
        let (ready_tasks, in_flight) = (work.ready, work.in_flight);
        // What the pool's window can still pay for, together with how much of
        // the pool the measurement covered. No bound admits everything: a
        // missing reading is not a closed window, and a publisher that stopped
        // on a missing reading would be driving the platform by the presence
        // of a metric rather than by the quota.
        let pays_for = self.admission_bound().await;
        let window_jobs = pays_for.and_then(|bound| bound.window_jobs);

        // Serialise and group first, changing no state.
        let mut by_stream: HashMap<String, Vec<(Task, Vec<u8>)>> = HashMap::new();
        let mut held_for_owner = 0usize;
        // 计划预算这一道准入，与列上限、池窗口并列：它判的是「这条任务自己
        // 还能不能再花」。拦在这里而不是派发之后，是因为拦住的那条要留在
        // `Pending`——已经进了 `Scheduled` 的行，就绪扫描不会再回来看它。
        // 还没有基线的活先排期，再判预算：预算闸读的就是这张表，排在这一步之后
        // 它才看得到刚写下的那一行。
        self.plan_new_work(&ready_tasks).await;
        let budgets = self.planned_budgets().await;
        let mut budget_holds: HashMap<&'static str, u64> = HashMap::new();
        for task in ready_tasks {
            // A task that names the unit responsible for it waits for that unit
            // to ask for it; see [`crate::ready_queue::carries_an_owner`]. It
            // stays Pending, so it is offered again on the next pass.
            if crate::ready_queue::carries_an_owner(&task) {
                held_for_owner += 1;
                continue;
            }
            if self.spent_its_budget(&task, budgets.as_ref()).await {
                let stream = crate::ready_queue::ready_stream_for(&task, &self.config.workspace_id);
                let kind = crate::ready_queue::stream_kind(&stream, &self.config.workspace_id);
                *budget_holds.entry(kind).or_insert(0) += 1;
                continue;
            }
            let payload = serde_json::to_vec(&task).map_err(SFError::Serialization)?;
            by_stream
                .entry(crate::ready_queue::ready_stream_for(
                    &task,
                    &self.config.workspace_id,
                ))
                .or_default()
                .push((task, payload));
        }
        if held_for_owner > 0 {
            // Named per pass rather than counted into a metric: a task nobody
            // ever asks for stops here, and the pass that stopped offering it
            // is the only evidence of where it went.
            tracing::debug!(
                held = held_for_owner,
                "ready tasks naming an owner are waiting for that owner to claim them"
            );
        }

        // Admission is decided for both queues and written down before any
        // state changes, and on every pass -- including the passes with nothing
        // ready. A reading that only appeared when something was held would
        // freeze its last value on the idle passes and serve a stale refusal as
        // a live one.
        //
        // It is measured against what the queue already owes rather than
        // against zero. A bound compared with zero every pass admits the same
        // allowance again while the previous batch is still queued, which is a
        // burst shaped exactly like no bound at all. `Scheduled` counts as owed
        // even if nothing has picked the task up: it has been handed a slot, and
        // the window has not been charged for it yet.
        //
        // Each queue is judged by its own owed count against the pool's whole
        // allowance. The two queues can therefore each admit up to one window's
        // worth in a pass; that is one batch of overshoot, bounded, and the
        // alternative -- splitting one allowance between queues -- lets a
        // backlog on one queue decide how much the other may dispatch, which is
        // how a stalled queue starves a healthy one.
        // The admission itself, not just its job count: the line below that
        // reports a hold names the gate that held it, and that gate is this
        // value's `bound`. Carrying only the count makes the reporter
        // reconstruct a gate it was already told, and a reconstruction of a
        // closed set defaults to whichever member is written first.
        let mut admitted_by_stream: HashMap<String, crate::ready_queue::PassAdmission> =
            HashMap::new();
        for stream in [
            crate::ready_queue::ready_stream(&self.config.workspace_id),
            crate::ready_queue::process_local_ready_stream(&self.config.workspace_id),
        ] {
            let ready = by_stream.get(&stream).map(|e| e.len()).unwrap_or(0) as u64;
            let owed = in_flight.get(&stream).copied().unwrap_or(0) as u64;
            let admitted = crate::ready_queue::admission_for_pass(
                ready,
                owed,
                window_jobs,
                READY_TASKS_PER_PASS,
            );
            admitted_by_stream.insert(stream.clone(), admitted);
            self.record_admission(
                &stream,
                pays_for,
                owed,
                admitted.jobs,
                ready - admitted.jobs,
                admitted.bound,
            )
            .await;
        }

        let mut first_error: Option<SFError> = None;
        let mut column_holds: HashMap<(String, &'static str), u64> = HashMap::new();
        for (stream, mut entries) in by_stream {
            if let Some(admitted) = admitted_by_stream.get(&stream).copied() {
                if (entries.len() as u64) > admitted.jobs {
                    // The reading the refusal was decided on travels with it:
                    // a hold that names only how many were held leaves the
                    // reader unable to tell a window that is nearly spent from
                    // one that pays for nothing, and those are different
                    // outages. The wake instant is this loop's own next look,
                    // not a promise about the upstream -- that promise lives in
                    // the gateway's snapshot and is not ours to restate.
                    if let Some(bound) = pays_for {
                        tracing::warn!(
                            stream = %stream,
                            ready = entries.len(),
                            admitted = admitted.jobs,
                            owed = in_flight.get(&stream).copied().unwrap_or(0),
                            window_jobs = ?bound.window_jobs,
                            allowance_tokens = bound.pace.allowance_tokens,
                            spent_tokens = bound.pace.spent_tokens,
                            reserve_tokens = bound.pace.reserve_tokens,
                            measured_upstreams = bound.pace.measured_upstreams,
                            pool_size = bound.pool_size,
                            // Zero is "no single window the measured members
                            // share", and printing it as an instant would read
                            // as 1970 — the same trap as `next_look_unix`
                            // below.
                            window_start_unix = ?(bound.pace.window_start_unix > 0)
                                .then_some(bound.pace.window_start_unix),
                            // Absolute, not "in N seconds": a relative figure
                            // is wrong the moment anyone reads the line. `None`
                            // when no cadence was configured -- a zero instant
                            // would read as 1970 rather than as "unknown".
                            next_look_unix = ?(self.config.ready_task_poll_interval_secs > 0).then(
                                || chrono::Utc::now().timestamp().saturating_add(
                                    self.config.ready_task_poll_interval_secs as i64
                                )
                            ),
                            // Which gate said zero, read off the admission this
                            // pass actually made -- the same value the held
                            // counter is labelled with, so the line and the
                            // series cannot disagree. The window arithmetic
                            // below is printed either way, and for an
                            // unavailable pool it contradicts the hold -- the
                            // allowance already covers a job while nothing is
                            // admitted. A reader given only the numbers would
                            // tune the pace for a pool that is simply down,
                            // and a line that named the window while the pass
                            // was sliced by its own ceiling would send that
                            // reader to tune an allowance nothing was reading.
                            refusal = crate::ready_queue::hold_reason(
                                bound.unavailable,
                                admitted.bound,
                            ),
                            "holding ready tasks back: nothing may be admitted this pass, so they stay pending until the pool can serve them"
                        );
                    } else {
                        // This branch is reached on exactly one condition: no
                        // status was read at all. A status that was read but
                        // whose measurement cannot bound the pool arrives as
                        // `Some(bound)` with `window_jobs: None` and takes the
                        // branch above, which is why nothing here tries to
                        // print the pool's numbers -- reading `pays_for` again
                        // in this arm can only yield `None`, and three fields
                        // that always print `None` read like a measurement of
                        // something rather than the absence of one.
                        tracing::info!(
                            stream = %stream,
                            ready = entries.len(),
                            admitted = admitted.jobs,
                            held = entries.len() as u64 - admitted.jobs,
                            ceiling = READY_TASKS_PER_PASS,
                            refusal = crate::ready_queue::hold_reason(
                                false,
                                admitted.bound,
                            ),
                            "no pool status was read this pass, so the batch is this pass's slice; the rest stays pending for the next one"
                        );
                    }
                    entries.truncate(usize::try_from(admitted.jobs).unwrap_or(usize::MAX));
                }
            }
            let limit = self.backend.payload_limit(&stream).await?;
            let mut payloads = Vec::with_capacity(entries.len());
            // 每一列的在制品上限，按项目判。放进逐条循环而不是在批头上截断：
            // 同一批里可能混着多个项目，一个项目满了不该拦住另一个项目。
            // 这一轮已经放进 `scheduled` 的那几条要算进占用，否则一次 pass 就能
            // 把一列灌到上限之外。
            let mut taken: HashMap<String, u64> = HashMap::new();
            for (task, payload) in entries {
                if !self.board_wip_limits.is_empty() {
                    let project = task.project_key().to_string();
                    let here = taken.get(&project).copied().unwrap_or(0);
                    let occupancy = columns.get(&project);
                    let at_limit =
                        crate::ready_queue::column_at_limit(&self.board_wip_limits, |column| {
                            let standing =
                                occupancy.and_then(|c| c.get(column)).copied().unwrap_or(0);
                            // 这一轮已经放行的活落在 scheduled 这一列上。
                            let admitted_here = if column == "scheduled" { here } else { 0 };
                            standing + admitted_here
                        });
                    if let Some(column) = at_limit {
                        *column_holds.entry((project.clone(), column)).or_insert(0) += 1;
                        tracing::debug!(
                            task_id = %task.id,
                            project = %project,
                            column,
                            limit = ?self.board_wip_limits.limit_for(column),
                            "holding a ready task back: its project's board column is at its limit"
                        );
                        continue;
                    }
                    *taken.entry(project).or_insert(0) += 1;
                }
                if let Some(limit) = limit {
                    if payload.len() > limit {
                        tracing::error!(
                            task_id = %task.id,
                            size = payload.len(),
                            limit,
                            "task payload is larger than the transport accepts: holding it back rather than spending its one dispatch on a publish that cannot happen"
                        );
                        continue;
                    }
                }
                if let Err(e) = self.orchestrator.schedule_task(&task.id).await {
                    tracing::warn!(task_id = %task.id, "schedule_task failed during publish: {e}");
                    continue;
                }
                payloads.push(payload);
            }
            if payloads.is_empty() {
                continue;
            }
            // The tasks in `payloads` are already `Scheduled`, and the batch is
            // written one message at a time, so a failure here leaves a prefix
            // of them on the wire and the rest without a message. Nothing is
            // rolled back: a task that was published and then put back in line
            // would be run twice, and running a task twice costs more than
            // waiting for the stall sweep to re-arm the ones that never went
            // out. What the failure does owe its reader is the count -- the
            // sweep's own total cannot tell a transport that refused the write
            // from a consumer that took the message and never started the task,
            // and those send their reader to different components.
            //
            // One stream's refusal is not the other's: the two queues are
            // separate subjects, so the pass still offers the sibling stream
            // rather than letting one dead subject hold every queue back. The
            // first error is kept and returned after both have had their turn.
            if let Err(e) = self.backend.publish_batch(&stream, &payloads).await {
                tracing::warn!(
                    stream = %stream,
                    scheduled = payloads.len(),
                    error = %e,
                    "publish failed after these tasks were moved to Scheduled: the messages that never went out will stay there until the stall sweep re-arms them, and some of this batch may already be on the wire"
                );
                if first_error.is_none() {
                    first_error = Some(e);
                }
                continue;
            }
        }
        // 这一轮被列上限拦下的活，按项目与列记账。计数器而不是 gauge：列满了
        // 这件事会随活干完自己消失，gauge 在填满前后都读 0。没配上限时一条都
        // 不写——一排零会让「没人配」看起来像「没有一次被拦」。
        if !column_holds.is_empty() {
            if let Some(metrics) = self.orchestrator.metrics_backend() {
                for ((project, column), held) in column_holds {
                    let mut labels = HashMap::new();
                    labels.insert(crate::LABEL_PROJECT.to_string(), project);
                    labels.insert(crate::LABEL_COLUMN.to_string(), column.to_string());
                    drop(
                        metrics
                            .record_counter(
                                cog_core::metric_names::BOARD_COLUMN_HELD_TOTAL,
                                held as f64,
                                labels,
                            )
                            .await,
                    );
                }
            }
        }
        // 这一拍被计划预算拦下的活，按队列记账。与列上限同一个理由用计数器：
        // 「花完了」会随一次重规划重新排预算消失，gauge 在前后都读 0。一条都
        // 没拦时不写——一排零会让「没有哪条任务有预算」看起来像「没有一次被拦」。
        if !budget_holds.is_empty() {
            if let Some(metrics) = self.orchestrator.metrics_backend() {
                for (kind, held) in budget_holds {
                    let mut labels = HashMap::new();
                    labels.insert(crate::LABEL_STREAM.to_string(), kind.to_string());
                    labels.insert(
                        crate::LABEL_REASON.to_string(),
                        crate::ready_queue::BUDGET_SPENT_REASON.to_string(),
                    );
                    drop(
                        metrics
                            .record_counter(
                                cog_core::metric_names::READY_ADMISSION_HELD_TOTAL,
                                held as f64,
                                labels,
                            )
                            .await,
                    );
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// 这一 workspace 里每条任务计划花掉的 token，按任务 id 索引；零预算的
    /// 行不留——它在判据里就是「没有计划」（见
    /// [`crate::ready_queue::past_its_budget`]），留着只会让每个读者自己记得跳过。
    ///
    /// 一次读整张表而不是逐条 `task_plan`：这一拍要判的是全部就绪任务的预算，
    /// 逐条问是每条一次往返，而基线的行数比任务少一个量级。读不到（进程内后端
    /// 没有这个面、或者库这一下不可用）返回 `None`：不设界，但也不假装设了界。
    /// 给还没有计划基线的就绪任务排一次期，并把按旧尺子写过期的基线重算一遍。
    ///
    /// 排期用的数是**量出来的**：同一种类已经跑完的任务里，token 清点的中位数
    /// 当预算、开工到最近一次更新的中位数当时长。样本不够（少于
    /// [`MIN_ESTIMATE_SAMPLES`] 条，或者那几条一条花销都没记下来）就不排——
    /// 编一个工期会让甘特图把没排期的活画成已排期，而「没排期」正是读者唯一
    /// 能据以去补估算的信号。
    ///
    /// 只给**就绪**的任务排：它们的依赖都已经满足，起点就是此刻；给一条还被
    /// 上游挡着的活排「现在开始」会画出一条本来不存在的并行。
    ///
    /// 重算只碰估算器自己写的行（版本串以 [`ESTIMATE_REV_PREFIX`] 那一路开头），
    /// 而且只碰尺子已经换过的那一路。为什么必须重算：换尺子之前，按排队时间
    /// 量出来的中位数写进了每一行，而那些行一旦存在就不再进估算——`planned`
    /// 里有了它的 id，这条任务在估算器眼里就「已经排过期」了。尺子换了，它们
    /// 既不会自己变，也不该被删掉：删掉就把起期和预算的来历一起丢了，而这两样
    /// 当时都是量出来的。原地改写保住起期（那是当时的现场），只按现在的尺子
    /// 重写止期。分解算出来的、人排的行（版本串不以 `estimate/` 开头）一律不碰：
    /// 那些是意图，不是估算。
    ///
    /// 写失败不挡派发：任务照派，这一件在甘特上继续读作未排期，日志里留一行
    /// 具名原因。反过来（为了一行基线把活压住）是拿成本换读数。
    async fn plan_new_work(&self, ready: &[Task]) {
        let Some(backend) = self.orchestrator.state_backend() else {
            return;
        };
        let planned: HashMap<String, cog_core::TaskPlan> =
            match backend.task_plans(&self.config.workspace_id).await {
                Ok(rows) => rows
                    .into_iter()
                    .map(|plan| (plan.task_id.clone(), plan))
                    .collect(),
                Err(e) => {
                    // 读不到已有的基线就别写：不知道谁已经有基线时补写，等于把
                    // 别人排好的期按估算覆盖掉。
                    tracing::debug!(
                        workspace_id = %self.config.workspace_id,
                        error = %e,
                        "no plan baselines read this pass; nothing is scheduled from history"
                    );
                    return;
                }
            };
        let now = chrono::Utc::now();
        // 这一拍要写的行，两条来源合成一张单子：起期先定下来，止期与预算随种类
        // 的读数。第三项是「这条是不是重算」——重算的行即使量出零工期也要写回
        // 去（止期清空），因为留着的那条止期正是尺子量错的那一条。
        let mut pending: Vec<(String, String, chrono::DateTime<chrono::Utc>, bool)> = Vec::new();
        for task in ready {
            if !planned.contains_key(&task.id) {
                pending.push((task.id.clone(), task.task_type.kind_key(), now, false));
            }
        }
        // 旧尺子写的行：起期留着当时的现场，止期按现在的尺子重算。它们不必
        // 就绪——排在后面的活一样有工期要读，而「它什么时候轮得到」不改变
        // 「它要干多久」这个问题该用哪把尺子答。
        let mut unreadable_kinds: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        let mut stale_rows = 0usize;
        for plan in planned
            .values()
            .filter(|plan| plan.baseline_rev.starts_with(ESTIMATE_REV_PREFIX_ANY))
        {
            if plan.baseline_rev.starts_with(ESTIMATE_REV_PREFIX) {
                continue;
            }
            let Some(kind) = estimate_rev_kind(&plan.baseline_rev) else {
                continue;
            };
            stale_rows += 1;
            pending.push((
                plan.task_id.clone(),
                kind,
                plan.planned_start.unwrap_or(now),
                true,
            ));
        }
        if pending.is_empty() {
            return;
        }
        let records = match backend
            .task_kind_records(&self.config.workspace_id, MIN_ESTIMATE_SAMPLES)
            .await
        {
            Ok(records) => records,
            Err(e) => {
                tracing::debug!(
                    workspace_id = %self.config.workspace_id,
                    error = %e,
                    "no measured history for any task kind; these tasks stay unscheduled"
                );
                return;
            }
        };
        let by_kind: HashMap<String, cog_core::TaskKindRecord> = records
            .into_iter()
            .map(|record| (record.kind.clone(), record))
            .collect();
        let mut rewritten = 0usize;
        for (task_id, kind, start, stale) in pending {
            let Some(record) = by_kind.get(&kind) else {
                if stale {
                    unreadable_kinds.insert(kind);
                }
                continue;
            };
            // 两条轴都量不出来才不排：一个中位数是缺的那一半，另一条轴还是
            // 量过的。工时能估、花销估不出时，甘特那一条是真有条子的，而
            // `budget_tokens = 0` 在读者那里读作「没有成本计划」（见
            // [`crate::ready_queue::past_its_budget`]），不是「计划花零个 token」。
            if record.median_tokens == 0 && record.median_duration_secs == 0 && !stale {
                continue;
            }
            // 时长中位数为 0 时止期留空：一排零会让甘特把它画成「计划已完成」。
            let planned_end = if record.median_duration_secs == 0 {
                None
            } else {
                Some(start + chrono::Duration::seconds(record.median_duration_secs as i64))
            };
            let plan = cog_core::TaskPlan {
                workspace_id: self.config.workspace_id.clone(),
                task_id: task_id.clone(),
                planned_start: Some(start),
                planned_end,
                budget_tokens: record.median_tokens,
                // 基线版本里同时写着这一版是哪来的：按历史估算，几条样本，
                // 以及量出这条工期的是哪把尺子。读者要问「这个预算谁给的」，
                // 答案得在行上而不是在日志里；尺子换了以后，这个字段也是
                // 下一次重算唯一认得出的旧行标记。
                baseline_rev: format!(
                    "{ESTIMATE_REV_PREFIX}{}/n{}/{}",
                    record.kind,
                    record.samples,
                    now.to_rfc3339()
                ),
                updated_at: now,
            };
            if let Err(e) = backend.save_task_plan(&plan).await {
                tracing::warn!(
                    task_id = %task_id,
                    kind = %record.kind,
                    error = %e,
                    "task goes out unscheduled: its estimated baseline was not stored"
                );
                continue;
            }
            if stale {
                rewritten += 1;
            }
        }
        if stale_rows > 0 {
            // 一次换尺子的迁移，不是每拍都有的读数：名出来是为了让人能对上
            // 「甘特上那批一周长的条什么时候换成十几分钟的」。
            tracing::debug!(
                workspace_id = %self.config.workspace_id,
                stale_rows,
                rewritten,
                unreadable_kinds = %unreadable_kinds.iter().cloned().collect::<Vec<_>>().join(","),
                "re-measured baselines written by an older ruler"
            );
        }
    }

    async fn planned_budgets(&self) -> Option<HashMap<String, u64>> {
        let backend = self.orchestrator.state_backend()?;
        match backend.task_plans(&self.config.workspace_id).await {
            Ok(rows) => Some(
                rows.into_iter()
                    .filter(|plan| plan.budget_tokens > 0)
                    .map(|plan| (plan.task_id, plan.budget_tokens))
                    .collect(),
            ),
            Err(e) => {
                tracing::debug!(
                    workspace_id = %self.config.workspace_id,
                    error = %e,
                    "no plan baselines read this pass; the plan budget does not hold anything"
                );
                None
            }
        }
    }

    /// 这条任务是否已经把它计划的那点 token 花完了。
    ///
    /// 三道都得过才算：有预算（`budgets` 里没有它的行就是没计划）、跑过一遍
    /// （没跑过的任务不可能有花销，问一次就是白问一趟库）、以及花销真的到了线。
    /// 「跑过一遍」读 `started_at` 与 `retry_count`：两条都在任务行上，换进程也
    /// 还在——重排回 `Pending` 只改状态，不清开始时刻。
    ///
    /// 花销读的是这条任务自己的 token 清点（每次运行结束时按任务 id 累加），
    /// 回答的是「这条活自己花了多少」；项目账上那个按调用归因的和数回答的是
    /// 另一个问题（「这个项目花到哪去了」），两者不在此处互换。
    ///
    /// 读不到就放行：没有清点行的任务（从没跑完过一轮、或者清点写失败）与一个
    /// 花光了的任务在读数上都是「读不到」，而把读不到当成花光了，会让一条写失败
    /// 的日志永久停掉整条任务线。
    async fn spent_its_budget(&self, task: &Task, budgets: Option<&HashMap<String, u64>>) -> bool {
        let Some(&budget) = budgets.and_then(|budgets| budgets.get(&task.id)) else {
            return false;
        };
        if task.started_at.is_none() && task.retry_count == 0 {
            return false;
        }
        let Some(gateway) = self.observability.get() else {
            return false;
        };
        match gateway.get_task_metrics(&task.id).await {
            Ok(metrics) => {
                let spent = metrics.total_tokens;
                if !crate::ready_queue::past_its_budget(spent, budget) {
                    return false;
                }
                // 拦下这件事在这一拍只留一条具名日志：计数器给的是这一拍被这道
                // 闸门拦住几条，而把一条任务停在这里的原因（花了多少、预算多少）
                // 只有这一行会说。它不会自己好起来——预算得由一次重规划重新排，
                // 所以这行是「谁、差多少」，不是「等下一拍」。
                tracing::debug!(
                    task_id = %task.id,
                    spent_tokens = spent,
                    budget_tokens = budget,
                    "holding a ready task back: it has spent the tokens its plan gave it"
                );
                true
            }
            Err(e) => {
                tracing::debug!(
                    task_id = %task.id,
                    error = %e,
                    "no token census for this task; its plan budget does not hold it"
                );
                false
            }
        }
    }

    /// What the pool's window can still pay for, with the reading it was
    /// derived from, or `None` when no status was read at all.
    ///
    /// A source that answers nothing this pass is not a healthy pool: both
    /// halves of the signal roll with the deployment, so an unanswered read is
    /// an ordinary sight during a rollout and it admits everything rather than
    /// freezing dispatch on a restart. The reading the publisher is asking for
    /// is a quota; only the quota's own report may close a queue.
    ///
    /// A status that was read is returned even when no bound follows from it,
    /// because "the measurement cannot bound this pool" has three causes --
    /// nothing measured, a reserve of zero, and a ruler that covers only part
    /// of the pool -- and those are different incidents. Carrying the status
    /// out is what lets the reading face say which one is in front of it
    /// instead of printing the same unbounded zero for all three.
    /// Whether the pool's own reading currently says nothing can be served.
    ///
    /// `false` when no source is attached or the read came back empty, and that
    /// is the whole point of putting it here rather than in the sweep: an
    /// unread pool is not a closed one, so silence keeps the sweep's ordinary
    /// behaviour. Only a pool that declares itself off changes what a backlog
    /// means, and only for as long as the declaration stands.
    pub async fn pool_declares_unavailable(&self) -> bool {
        let Some(source) = self.pool_source.get() else {
            return false;
        };
        source
            .status()
            .await
            .map(|status| status.unavailable)
            .unwrap_or(false)
    }

    async fn admission_bound(&self) -> Option<AdmissionBound> {
        let source = self.pool_source.get()?;
        let status = source.status().await?;
        let window_jobs = crate::ready_queue::jobs_the_window_can_pay_for_status(&status);
        Some(AdmissionBound {
            window_jobs,
            pace: status.pace,
            pool_size: status.pool_size,
            unavailable: status.unavailable,
            retry_at_unix: status.next_attempt_unix,
        })
    }

    /// Write one pass's admission arithmetic for one queue.
    ///
    /// A pass with no metrics backend attached says nothing here, which is the
    /// same shape as a pass that was never run -- the loop that attaches the
    /// backend reports that separately rather than letting a missing surface
    /// read as a quiet one.
    async fn record_admission(
        &self,
        stream: &str,
        bound: Option<AdmissionBound>,
        owed: u64,
        admitted: u64,
        held: u64,
        pass_bound: crate::ready_queue::PassBound,
    ) {
        let Some(metrics) = self.orchestrator.metrics_backend() else {
            return;
        };
        let stream_kind = crate::ready_queue::stream_kind(stream, &self.config.workspace_id);
        let cell = |kind: &str| {
            let mut labels = HashMap::new();
            labels.insert(crate::LABEL_STREAM.to_string(), stream_kind.to_string());
            labels.insert(crate::LABEL_KIND.to_string(), kind.to_string());
            labels
        };
        // What the measurement covered, printed even when it bounded nothing:
        // `bounded=0` has four causes -- no status read, nothing measured, a
        // reserve of zero, and a ruler covering part of the pool -- and they
        // call for completely different repairs. Zeros here mean "not known",
        // not "covered nothing measured over a pool of no members".
        let (covered, pool, reserve) = match bound {
            Some(bound) => (
                bound.pace.measured_upstreams as f64,
                bound.pool_size as f64,
                bound.pace.reserve_tokens as f64,
            ),
            None => (0.0, 0.0, 0.0),
        };
        // `bounded` first: it is what tells a reader whether `window_jobs` is a
        // measurement from this pass or a value left over from the last pass
        // that had one.
        for (kind, value) in [
            (
                "bounded",
                f64::from(u8::from(
                    bound.and_then(|bound| bound.window_jobs).is_some(),
                )),
            ),
            ("covered", covered),
            ("pool", pool),
            ("reserve", reserve),
            // Which of the causes above is in front of the reader: `bounded=1`
            // with `window_jobs=0` is a spent window or a pool the gateway has
            // declared unavailable, and only this cell separates them.
            (
                "unavailable",
                f64::from(u8::from(bound.is_some_and(|bound| bound.unavailable))),
            ),
            // The degraded period's own clock, written beside the verdict that
            // opened it: `unavailable=1` with this cell set says the hold has a
            // time it will be re-decided, and `0` says the pool is not why the
            // pass held -- either it is available, or no status was read at
            // all. It is the gateway's probe cadence, not the upstream's
            // promise, so it is not when the window reopens.
            (
                "retry_at_unix",
                bound.map_or(0.0, |bound| bound.retry_at_unix as f64),
            ),
            // The traffic face: whether this pass had anything to decide. An
            // output of zero with an input of zero says nothing about how the
            // gate behaves under pressure, and `owed` is a different quantity
            // -- work already in flight, not work asking to go out.
            ("ready", (admitted + held) as f64),
            ("owed", owed as f64),
            ("admitted", admitted as f64),
        ] {
            drop(
                metrics
                    .record_gauge(
                        cog_core::metric_names::READY_ADMISSION_READINGS,
                        value,
                        cell(kind),
                    )
                    .await,
            );
        }
        if let Some(window_jobs) = bound.and_then(|bound| bound.window_jobs) {
            drop(
                metrics
                    .record_gauge(
                        cog_core::metric_names::READY_ADMISSION_READINGS,
                        window_jobs as f64,
                        cell("window_jobs"),
                    )
                    .await,
            );
        }
        if held > 0 {
            let mut labels = HashMap::new();
            labels.insert(crate::LABEL_STREAM.to_string(), stream_kind.to_string());
            // The reason is the gate that said zero, not the gate that is
            // always named: an unavailable pool refuses work while its window
            // still pays for a job, and calling that `window_exhausted` sends
            // the reader to tune a budget that is not the constraint.
            labels.insert(
                crate::LABEL_REASON.to_string(),
                crate::ready_queue::hold_reason(
                    bound.is_some_and(|bound| bound.unavailable),
                    pass_bound,
                )
                .to_string(),
            );
            drop(
                metrics
                    .record_counter(
                        cog_core::metric_names::READY_ADMISSION_HELD_TOTAL,
                        held as f64,
                        labels,
                    )
                    .await,
            );
        }
    }

    /// Consume task-result messages from the backend and drive DAG state transitions.
    /// Listens on `orchestrator:results:{workspace_id}` for [`DagMessage::TaskComplete`]
    /// and [`DagMessage::TaskFailed`] events, calling `complete_task` / `fail_task`
    /// on the embedded [`DagExecutor`].  After each state change, newly-ready
    /// dependents are automatically published via [`Self::publish_ready_tasks`].
    /// # Graceful shutdown
    /// Pass a [`ShutdownSignal`] to cleanly exit on the next iteration boundary.
    pub async fn run_consumer(&self, shutdown: ShutdownSignal) -> SFResult<()> {
        let result_stream = cog_core::task_result_stream(&self.config.workspace_id);
        let group_name = self.config.consumer_group.clone();

        if let Err(e) = self
            .backend
            .create_consumer_group(&result_stream, &group_name)
            .await
        {
            if !e.to_string().contains("BUSYGROUP") {
                return Err(e);
            }
        }

        // 死信恢复：处理途中死掉的 pod 会把消息留在组内 pending，
        // `subscribe`（XREADGROUP ">"）只读新消息，永远不会重投——而本循环的
        // 失败分支正是靠重投才成立（"Keep the message pending: redelivery …"）。
        // 没有这一步，一次瞬时 complete_task/publish_ready_tasks 失败或一次崩溃
        // 就让那条结果永久丢失：任务在该进程的内存 DAG 里停在 running，依赖它的
        // 下游再也不就绪，且没有任何面能看出丢了什么。
        // 阈值必须大于最长处理时长，否则正在处理的消息会被并发重投；重投是
        // at-least-once，已终态任务会被上面的拒绝分支 ack 丢弃，不会重复迁移。
        let claim_idle_ms = self.config.result_claim_idle_secs.saturating_mul(1000);
        let interval_secs = self.config.result_claim_interval_secs.max(1);
        // 观测面单独成任务，不搭清扫器的节拍：这个循环会在 tick 里就地 await
        // 一条重投消息的完整处理（可能几十分钟），测量挂在同一根节拍上就等于
        // "处理得越久，指标越静止"，而抓取面上一条静止的序列和一条干净流量的
        // 序列是同一个样子——恰好把最该被看见的停滞藏了起来。
        crate::observable::spawn_pending_observer(
            crate::observable::RESULT_STREAM_PENDING_LOOP,
            self.backend.clone(),
            result_stream.clone(),
            group_name.clone(),
            claim_idle_ms,
            std::time::Duration::from_secs(interval_secs),
            shutdown.clone(),
        );

        {
            let sweeper = self.clone();
            let stream = result_stream.clone();
            let group = group_name.clone();
            let sweep_shutdown = shutdown.clone();
            let batch = self.config.result_claim_batch;
            // Detached: this sweeper's fate is not the caller's. The consumer
            // loop below is awaited in place, and the two end together on the
            // same shutdown signal.
            drop(cog_core::loop_health::spawn(
                RESULT_RECLAIM_LOOP,
                cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(
                    interval_secs,
                )),
                sweep_shutdown.clone(),
                // Rebuilt per attempt, so everything the body consumes is cloned here.
                move |beat| {
                    let sweeper = sweeper.clone();
                    let stream = stream.clone();
                    let group = group.clone();
                    let sweep_shutdown = sweep_shutdown.clone();
                    async move {
                        let mut ticker =
                            tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                        loop {
                            beat.beat();
                            tokio::select! {
                                biased;
                                _ = sweep_shutdown.wait() => break,
                                _ = ticker.tick() => {
                                    match sweeper
                                        .backend
                                        .claim_pending(&stream, &group, claim_idle_ms, batch)
                                        .await
                                    {
                                        // No reclaim path on this backend: no
                                        // outcome to record, and nothing to
                                        // sweep. See the contract's note on
                                        // `claim_pending`'s `None`.
                                        Ok(None) => {}
                                        Ok(Some(claimed)) => {
                                            for (msg_id, bytes) in claimed {
                                                tracing::warn!(
                                                    stream = %stream, msg_id = %msg_id,
                                                    "reclaimed a result message left pending by a consumer that never acked it"
                                                );
                                                sweeper
                                                    .handle_result_message(&stream, &group, &msg_id, &bytes)
                                                    .await;
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(
                                                stream = %stream,
                                                "result pending claim sweep failed: {e}"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
            ));
        }

        // Resubscribe on stream failure/end: exiting the task would freeze the
        // consumer group until the next pod restart (a single transient error
        // historically stalled groups for days). The resubscription lives
        // inside the body, so a restart resumes reading the same group.
        //
        // Awaited in place rather than detached: the caller of this function
        // awaits until the loop stops, and its return is what says the stream
        // is no longer served.
        let this = self.clone();
        let _ = cog_core::loop_health::spawn(
            TASK_CONSUMER_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = this.clone();
                let result_stream = result_stream.clone();
                let group_name = group_name.clone();
                let shutdown = shutdown.clone();
                async move {
                    'subscribe: loop {
                        beat.beat();
                        let mut stream =
                            match this.backend.subscribe(&result_stream, &group_name).await {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::warn!("result stream subscribe failed, retrying: {e}");
                                    tokio::select! {
                                        _ = shutdown.wait() => break 'subscribe,
                                        _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => continue 'subscribe,
                                    }
                                }
                            };

                        loop {
                            let next = tokio::select! {
                                biased;
                                _ = shutdown.wait() => break 'subscribe,
                                msg = stream.next() => msg,
                            };

                            let (msg_id, bytes) = match next {
                                Some(Ok(v)) => v,
                                Some(Err(e)) => {
                                    tracing::warn!("result stream error, resubscribing: {e}");
                                    break;
                                }
                                None => {
                                    tracing::warn!("result stream ended, resubscribing");
                                    break;
                                }
                            };

                            this.handle_result_message(&result_stream, &group_name, &msg_id, &bytes)
                                .await;
                        }

                        tokio::select! {
                            _ = shutdown.wait() => break 'subscribe,
                            _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => {}
                        }
                    }
                }
            },
        )
        .await;

        Ok(())
    }

    /// Apply one result message to the DAG and ack it.
    ///
    /// Shared by the live subscription and the pending sweeper so a reclaimed
    /// message takes exactly the path a freshly delivered one takes. A second
    /// implementation for the reclaimed case would drift, and that is the path
    /// that is by definition the least exercised.
    ///
    /// A message is only acked once its state transition has been applied.
    /// Every early return below leaves it pending on purpose, so the sweeper
    /// retries it — a transient failure must not silently strand a result.
    /// Whether a result must be refused because the task it names belongs to
    /// another unit.
    ///
    /// The shared ready queue no longer carries an owned task, and the
    /// consumer refuses one that arrives anyway, so the normal executor never
    /// produces a result for an owned task. An out-of-band worker reports
    /// through this same stream, naming itself in `sender`; that name is
    /// asserted rather than proved, and the check is here to stop the ordinary
    /// mistake -- a look-alike id, a copied command, a unit that picked up the
    /// wrong row -- from finishing someone else's work. Refusing rather than
    /// applying keeps the row where its owner left it.
    async fn result_names_another_owner(&self, task_id: &str, sender: &str) -> bool {
        match self.orchestrator.get_task(task_id).await {
            Some(task) => match task.owner_unit.as_deref() {
                Some(owner) => owner != sender,
                None => false,
            },
            // An unknown task is not this check's business: the transition
            // below rejects it on its own terms, with its own log line.
            None => false,
        }
    }

    async fn handle_result_message(
        &self,
        result_stream: &str,
        group_name: &str,
        msg_id: &str,
        bytes: &[u8],
    ) {
        let msg: DagMessage = match serde_json::from_slice(bytes) {
            Ok(m) => m,
            Err(e) => {
                // Poison message: no future bytes can parse better, so ack it
                // out instead of letting the sweeper redeliver it forever. The
                // bytes go to the dead-letter queue first, because the ack is
                // the step that makes them unrecoverable and a log line is not
                // a place anybody reads a message back from. A failed DLQ write
                // does not hold the message pending: the parse can never
                // succeed, so redelivery buys nothing, and the loss is reported
                // by name instead.
                tracing::warn!(msg_id = %msg_id, "Failed to deserialize DagMessage: {e}");
                if let Err(dlq_err) = self
                    .backend
                    .dlq(
                        result_stream,
                        msg_id,
                        bytes,
                        &format!("unparsable DagMessage: {e}"),
                    )
                    .await
                {
                    tracing::error!(
                        msg_id = %msg_id,
                        "dead-letter write failed; this message's bytes are now lost: {dlq_err}"
                    );
                }
                self.ack_result(result_stream, group_name, msg_id).await;
                return;
            }
        };

        match msg {
            DagMessage::TaskComplete {
                task_id,
                result,
                sender,
                ..
            } => {
                if self.result_names_another_owner(&task_id, &sender).await {
                    tracing::warn!(
                        task_id = %task_id, sender = %sender, msg_id = %msg_id,
                        "result message names a task owned by another unit; dropping it"
                    );
                    self.ack_result(result_stream, group_name, msg_id).await;
                    return;
                }
                match self
                    .orchestrator
                    .complete_task(&task_id, result.clone())
                    .await
                {
                    Ok(scheduled) => {
                        tracing::info!(
                            task_id = %task_id,
                            scheduled = scheduled.len(),
                            "Task completed via message queue"
                        );
                    }
                    Err(e @ cog_core::SFError::TaskFailed { .. }) => {
                        // Duplicate/stale result for an already-terminal or
                        // unknown task: reprocessing can never succeed, so
                        // ack and drop instead of spinning on redelivery.
                        tracing::warn!(
                            task_id = %task_id, msg_id = %msg_id,
                            "result message rejected by DAG ({e}); dropping"
                        );
                        self.ack_result(result_stream, group_name, msg_id).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(task_id = %task_id, "complete_task failed: {e}");
                        return;
                    }
                }
                if let Err(e) = self.publish_ready_tasks().await {
                    tracing::warn!("publish_ready_tasks after complete failed: {e}");
                    return;
                }
                self.ack_result(result_stream, group_name, msg_id).await;
            }
            DagMessage::TaskFailed {
                task_id,
                error,
                error_cause,
                retry_after_secs,
                sender,
                ..
            } => {
                if self.result_names_another_owner(&task_id, &sender).await {
                    tracing::warn!(
                        task_id = %task_id, sender = %sender, msg_id = %msg_id,
                        "failure message names a task owned by another unit; dropping it"
                    );
                    self.ack_result(result_stream, group_name, msg_id).await;
                    return;
                }
                match self
                    .orchestrator
                    .fail_task_after(&task_id, error.clone(), error_cause, retry_after_secs)
                    .await
                {
                    Ok((retried, cancelled, _dlq_pushed)) => {
                        tracing::warn!(
                            task_id = %task_id,
                            retried,
                            cancelled = cancelled.len(),
                            "Task failed via message queue"
                        );
                    }
                    Err(e @ cog_core::SFError::TaskFailed { .. }) => {
                        tracing::warn!(
                            task_id = %task_id, msg_id = %msg_id,
                            "failure result rejected by DAG ({e}); dropping"
                        );
                        self.ack_result(result_stream, group_name, msg_id).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(task_id = %task_id, "fail_task failed: {e}");
                        return;
                    }
                }
                if let Err(e) = self.publish_ready_tasks().await {
                    tracing::warn!("publish_ready_tasks after fail failed: {e}");
                    return;
                }
                self.ack_result(result_stream, group_name, msg_id).await;
            }
            _ => {
                tracing::debug!(msg_id = %msg_id, "Ignoring non-result DagMessage variant");
                self.ack_result(result_stream, group_name, msg_id).await;
            }
        }
    }

    async fn ack_result(&self, result_stream: &str, group_name: &str, msg_id: &str) {
        let ids = [msg_id.to_string()];
        if let Err(e) = self.backend.ack(result_stream, group_name, &ids).await {
            tracing::warn!(msg_id = %msg_id, "Failed to ack result message: {e}");
        }
    }

    /// Consume goal messages from the `goals:{workspace_id}` stream and inject
    /// them into the DAG.
    /// Each [`cog_core::GoalMessage`] is deserialized and submitted via
    /// [`Self::submit_goal`]. After submission, newly-ready tasks are
    /// automatically published to the ready stream.
    ///
    /// Replay safety here comes from task identity, not from the message:
    /// every injection on this path is a duplicate-tolerant batch add, so a
    /// redelivered message carrying task ids already in the DAG adds nothing.
    /// There is no record of processed `message_id`s, and none is needed
    /// while the injections stay idempotent.
    ///
    /// A validation refusal is the exception, and it is why the failure arm
    /// below acknowledges the message: the DAG refusing a plan is a property
    /// of the plan's bytes, not of this attempt, so leaving the message
    /// pending would redeliver it forever and never say why. The refusal is
    /// logged once, with the ids it names, and the message is retired the way
    /// one that cannot be deserialized is.
    fn permanent_refusal(e: &cog_core::SFError) -> bool {
        matches!(e, &cog_core::SFError::Validation(_))
    }

    pub async fn run_goal_consumer(&self, shutdown: ShutdownSignal) -> SFResult<()> {
        let goal_stream = format!("goals:{}", self.config.workspace_id);
        let group = format!("dag-executor-{}", self.config.workspace_id);

        if let Err(e) = self
            .backend
            .create_consumer_group(&goal_stream, &group)
            .await
        {
            if !e.to_string().contains("BUSYGROUP") {
                return Err(e);
            }
        }
        // Resubscription lives inside the body, so a restart resumes reading
        // the same group. Awaited in place rather than detached: the caller of
        // this function awaits until the loop stops.
        let this = self.clone();
        let _ = cog_core::loop_health::spawn(
            GOAL_CONSUMER_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = this.clone();
                let goal_stream = goal_stream.clone();
                let group = group.clone();
                let shutdown = shutdown.clone();
                async move {
        'subscribe: loop {
            beat.beat();
            let mut stream = match this.backend.subscribe(&goal_stream, &group).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("goal stream subscribe failed, retrying: {e}");
                    tokio::select! {
                        _ = shutdown.wait() => break 'subscribe,
                        _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => continue 'subscribe,
                    }
                }
            };

            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown.wait() => break 'subscribe,
                    msg = stream.next() => msg,
                };

                let (msg_id, bytes) = match next {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => {
                        tracing::warn!("goal stream error, resubscribing: {e}");
                        break;
                    }
                    None => {
                        tracing::warn!("goal stream ended, resubscribing");
                        break;
                    }
                };

                let goal: cog_core::GoalMessage = match serde_json::from_slice(&bytes) {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(msg_id = %msg_id, "Failed to deserialize GoalMessage: {e}");
                        if let Err(e) = this
                            .backend
                            .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(msg_id = %msg_id, "Failed to ack poison goal message: {e}");
                        }
                        continue;
                    }
                };

                tracing::info!(
                    goal_id = %goal.message_id,
                    workspace = %goal.workspace_id,
                    task_count = goal.tasks.len(),
                    "Goal received from message queue"
                );

                // Unified path: always route through ActionPlanner.
                // ActionPlanner checks markers and decides:
                //   - verified tasks  → inject directly into DagExecutor
                //   - empty / unverified → decompose via collaboration
                if let (Some(ref planner), Some(ref skill_registry)) =
                    (&this.action_planner, &this.skill_registry)
                {
                    let registry = skill_registry.read().await;
                    let tasks = goal.tasks;
                    match planner.process_goal(&goal.goal, tasks, &registry).await {
                        Ok(ids) => {
                            tracing::info!(
                                goal_id = %goal.message_id,
                                task_count = %ids.len(),
                                "Goal processed via ActionPlanner"
                            );
                        }
                        // Planner failure can be transient (LLM/collaboration),
                        // so the message is left pending rather than acked. A
                        // retry replays it through the same idempotent injection
                        // path; it does not re-submit anything already added.
                        Err(e) => {
                            tracing::warn!(
                                goal_id = %goal.message_id,
                                permanent = Self::permanent_refusal(&e),
                                "ActionPlanner processing failed: {e}"
                            );
                            if Self::permanent_refusal(&e) {
                                if let Err(e) = this
                                    .backend
                                    .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                                    .await
                                {
                                    tracing::warn!(goal_id = %goal.message_id, msg_id = %msg_id, "Failed to ack refused goal message: {e}");
                                }
                            }
                            continue;
                        }
                    }
                } else {
                    // Fallback: direct DagExecutor submission when ActionPlanner unavailable.
                    if let Err(e) = this.submit_goal(&goal.goal, goal.tasks).await {
                        tracing::warn!(
                            goal_id = %goal.message_id,
                            permanent = Self::permanent_refusal(&e),
                            "submit_goal failed: {e}"
                        );
                        if Self::permanent_refusal(&e) {
                            if let Err(e) = this
                                .backend
                                .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                                .await
                            {
                                tracing::warn!(goal_id = %goal.message_id, msg_id = %msg_id, "Failed to ack refused goal message: {e}");
                            }
                        }
                        continue;
                    }
                }

                if let Err(e) = this.publish_ready_tasks().await {
                    tracing::warn!(goal_id = %goal.message_id, "publish_ready_tasks after goal failed: {e}");
                    continue;
                }
                if let Err(e) = this
                    .backend
                    .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                    .await
                {
                    tracing::warn!(goal_id = %goal.message_id, msg_id = %msg_id, "Failed to ack goal message: {e}");
                }
            }

            tokio::select! {
                _ = shutdown.wait() => break 'subscribe,
                _ = tokio::time::sleep(crate::subscribe_backoff::SUBSCRIBE_RETRY_BACKOFF) => {}
            }
        }
                }
            },
        )
        .await;

        Ok(())
    }

    /// Background reconciliation for decomposition orphans: non-executable
    /// parent placeholders with zero children stuck Pending (empty
    /// decomposition, partial write, crash mid-injection). Each stalled
    /// orphan is terminated (Pending → Failed) and raises a persistent
    /// alert so signal watcher re-drives the work as a fresh intent; alerts
    /// resolve once the parent task reaches a terminal state. Rows raised
    /// before a restart are adopted from the alert store on the first tick.
    pub async fn run_orphan_reconciler(
        &self,
        enabled: bool,
        poll_interval_secs: u64,
        stall_after_secs: u64,
        alert_dwell_secs: u64,
        sink: Option<Arc<dyn cog_core::PersistentAlertSink>>,
        shutdown: ShutdownSignal,
    ) {
        if !enabled {
            tracing::info!("decomposition orphan reconciler disabled by config");
            return;
        }
        let interval_secs = poll_interval_secs.max(30);
        let stall_secs = stall_after_secs.max(60);
        // The alert consumer polls independently; a dwell shorter than one scan
        // interval could elude every poll. Default dwell is two intervals.
        let dwell_secs = alert_dwell_secs.max(interval_secs);
        tracing::info!(
            interval_secs,
            stall_after_secs = stall_secs,
            alert_dwell_secs = dwell_secs,
            alert_sink = sink.is_some(),
            "decomposition orphan reconciler started"
        );

        let dwell = chrono::Duration::seconds(dwell_secs as i64);
        let this = self.clone();
        // Awaited in place rather than detached: the caller of this function
        // awaits until the loop stops.
        //
        // The fired-key bookkeeping is built per attempt. Losing it to a panic
        // costs nothing that is not recovered: the first tick of the next
        // attempt re-adopts every active alert from the persistent store.
        let _ = cog_core::loop_health::spawn(
            ORPHAN_RECONCILER_LOOP,
            cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(interval_secs)),
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = this.clone();
                let sink = sink.clone();
                let shutdown = shutdown.clone();
                async move {
                    // Keys this watcher owns, with the earliest time the alert may resolve.
                    let mut firing: std::collections::HashMap<String, OrphanWatch> =
                        std::collections::HashMap::new();
                    let mut adopted = false;
                    let mut ticker =
                        tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                    loop {
                        beat.beat();
                        tokio::select! {
                            biased;
                            _ = shutdown.wait() => break,
                            _ = ticker.tick() => {
                                if !adopted {
                                    adopted = true;
                                    if let Some(sink) = &sink {
                                        for alert in sink
                                            .list_active_persistent_alerts("decomposition_", 1000)
                                            .await
                                        {
                                            if let Some(parent) = alert
                                                .labels
                                                .get("parent_task_id")
                                                .and_then(|v| v.as_str())
                                                .filter(|s| !s.is_empty() && *s != "-")
                                            {
                                                firing.insert(
                                                    alert.dedup_key.clone(),
                                                    OrphanWatch {
                                                        parent_id: parent.to_string(),
                                                        resolve_after: alert.fired_at + dwell,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }
                                let stall = chrono::Duration::seconds(stall_secs as i64);
                                orphan_reconcile_tick(
                                    &this.orchestrator,
                                    sink.as_ref(),
                                    &stall,
                                    &dwell,
                                    &mut firing,
                                )
                                .await;
                            }
                        }
                    }
                }
            },
        )
        .await;
    }
}

/// Firing-key bookkeeping: the parent task the alert is about and the
/// earliest time at which it is allowed to resolve.
struct OrphanWatch {
    parent_id: String,
    resolve_after: chrono::DateTime<chrono::Utc>,
}

/// One reconciler pass. Split out as a free function so the adoption/firing
/// bookkeeping stays testable without a running timer.
async fn orphan_reconcile_tick(
    orchestrator: &Arc<DagExecutor>,
    sink: Option<&Arc<dyn cog_core::PersistentAlertSink>>,
    stall: &chrono::Duration,
    dwell: &chrono::Duration,
    firing: &mut std::collections::HashMap<String, OrphanWatch>,
) {
    let stall_before = chrono::Utc::now() - *stall;
    let orphans = orchestrator.find_decomposition_orphans(stall_before).await;
    let mut fired_this_tick = Vec::new();
    for orphan in orphans {
        let goal_id = orphan.goal_id.as_deref().unwrap_or("-");
        let task_id = orphan.id.clone();
        let dedup_key = format!(
            "{}:{goal_id}:{task_id}",
            cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED
        );
        let reason = "decomposition orphan: non-executable parent task has no children after the stall threshold";
        let message = format!(
            "goal {goal_id} task {task_id}: {reason}; the placeholder is terminated and the work is re-driven via a new intent"
        );
        let labels = serde_json::json!({
            "goal_id": goal_id,
            "parent_task_id": task_id,
            "source": "orphan_reconciler",
            "task_goal": cog_core::bounded_task_goal(
                Some(&orphan.input),
                cog_core::ALERT_LABEL_VALUE_MAX_CHARS,
            ),
        });
        if let Some(sink) = sink {
            let draft = cog_core::PersistentAlertDraft {
                rule: cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED.to_string(),
                dedup_key: dedup_key.clone(),
                severity: cog_core::AlertSeverity::Warning.as_str().to_string(),
                message: message.clone(),
                labels,
            };
            if let Err(e) = sink.set_persistent_alert(true, &draft).await {
                tracing::warn!(task_id = %task_id, error = %e, "failed to fire decomposition orphan alert");
            }
        } else {
            tracing::error!(task_id = %task_id, "{message} (no persistent alert sink attached)");
        }
        firing.insert(
            dedup_key.clone(),
            OrphanWatch {
                parent_id: task_id.clone(),
                resolve_after: chrono::Utc::now() + *dwell,
            },
        );
        fired_this_tick.push(dedup_key);
        if let Err(e) = orchestrator
            .terminate_decomposition_orphan(&task_id, reason.to_string())
            .await
        {
            tracing::warn!(task_id = %task_id, error = %e, "failed to terminate decomposition orphan");
        } else {
            tracing::error!(task_id = %task_id, goal_id = %goal_id, "terminated stalled decomposition orphan");
        }
    }

    // Resolve owned alerts whose parent task is now terminal or gone, but never
    // before the observation dwell elapsed since the alert first fired.
    let now = chrono::Utc::now();
    let mut resolved = Vec::new();
    for (key, watch) in firing.iter() {
        // An alert fired on this pass must survive at least until the next pass:
        // the independent alert consumer may only poll between passes.
        if fired_this_tick.contains(key) {
            continue;
        }
        if now < watch.resolve_after {
            continue;
        }
        let task = orchestrator.get_task(&watch.parent_id).await;
        let done = match task {
            Some(t) => matches!(
                t.status,
                cog_core::TaskStatus::Completed
                    | cog_core::TaskStatus::Failed
                    | cog_core::TaskStatus::Cancelled
            ),
            None => true,
        };
        if done {
            resolved.push(key.clone());
        }
    }
    for key in resolved {
        let rule = key.split(':').next().unwrap_or(&key).to_string();
        if let Some(sink) = sink {
            let draft = cog_core::PersistentAlertDraft {
                rule,
                dedup_key: key.clone(),
                severity: cog_core::AlertSeverity::Info.as_str().to_string(),
                message: String::new(),
                labels: serde_json::json!({}),
            };
            if let Err(e) = sink.set_persistent_alert(false, &draft).await {
                tracing::warn!(key = %key, error = %e, "failed to resolve decomposition alert");
                continue;
            }
        }
        firing.remove(&key);
        tracing::info!(key = %key, "decomposition alert resolved");
    }
}

#[cfg(test)]
mod consumer_ack_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{MessageStream, Observable};
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Queue = HashMap<String, Vec<(String, Vec<u8>)>>;
    type AckLog = Vec<(String, String, Vec<String>)>;
    type PendingLog = Vec<(String, Vec<u8>)>;

    /// 一笔死信写：它回答「哪条消息、带着什么字节、为什么、写回哪条流」。
    #[derive(Clone, Debug)]
    struct DlqWrite {
        stream: String,
        msg_id: String,
        payload: Vec<u8>,
        reason: String,
    }

    /// Scripted backend: subscribe replays a fixed queue per subject then
    /// blocks forever; ack calls are recorded for assertions.
    #[derive(Clone, Default)]
    struct ScriptedBackend {
        queues: Arc<Mutex<Queue>>,
        acks: Arc<Mutex<AckLog>>,
        /// Entries already delivered to some consumer but never acked — what
        /// `claim_pending` hands back. Draining on read mirrors XAUTOCLAIM
        /// taking ownership of the PEL entry.
        pending: Arc<Mutex<PendingLog>>,
        /// How long the pending entries have been outstanding. A real queue
        /// ages them itself; the script has to be told, because the age is what
        /// separates "just handed over" from "abandoned".
        pending_idle_ms: Arc<Mutex<u64>>,
        /// Refuse to hand entries back, the way a reclaim pass that is broken
        /// or unreachable would, so a test can look at what is left behind.
        hold_claims: Arc<std::sync::atomic::AtomicBool>,
        /// A reclaim round that never comes back — the shape a slow or wedged
        /// round has from the loop's point of view.
        stall_claims: Arc<std::sync::atomic::AtomicBool>,
        /// How many times the pending state was asked for, so a test can tell
        /// "the measurement kept running" apart from "the last reading is still
        /// on screen".
        pending_stats_calls: Arc<std::sync::atomic::AtomicUsize>,
        /// The single-message bound this transport declares, or `None` for a
        /// transport that imposes none. A backend that declares one also
        /// refuses a payload over it, the way a real server does, so a test
        /// cannot pass by declaring a bound and then accepting everything.
        payload_limit: Arc<Mutex<Option<usize>>>,
        /// 死信写的记录。
        ///
        /// 记的是**参数**而不是发出去的那条 JSON——测试要钉的是「哪条消息、带着什么字节、
        /// 为什么」被交给了死信队列，记录本身长什么样由契约自己的往返测试钉。四个字段
        /// 各自有名字而不是攒成一个元组：元组里第 3 位和第 4 位都是「一串东西」，
        /// 断言写反了编译器也放行，而这条测试的全部意义就在于那串字节。
        dlq_writes: Arc<Mutex<Vec<DlqWrite>>>,
    }

    impl ScriptedBackend {
        fn enqueue(&self, subject: &str, msg_id: &str, bytes: Vec<u8>) {
            self.queues
                .lock()
                .unwrap()
                .entry(subject.to_string())
                .or_default()
                .push((msg_id.to_string(), bytes));
        }

        fn abandon(&self, msg_id: &str, bytes: Vec<u8>) {
            self.pending
                .lock()
                .unwrap()
                .push((msg_id.to_string(), bytes));
        }

        fn set_pending_idle_ms(&self, ms: u64) {
            *self.pending_idle_ms.lock().unwrap() = ms;
        }

        fn hold_claims(&self) {
            self.hold_claims
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn stall_claims(&self) {
            self.stall_claims
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }

        fn pending_stats_calls(&self) -> usize {
            self.pending_stats_calls
                .load(std::sync::atomic::Ordering::Relaxed)
        }

        fn set_payload_limit(&self, limit: Option<usize>) {
            *self.payload_limit.lock().unwrap() = limit;
        }

        fn acked_ids(&self) -> Vec<String> {
            self.acks
                .lock()
                .unwrap()
                .iter()
                .flat_map(|(_, _, ids)| ids.clone())
                .collect()
        }

        fn dlq_writes(&self) -> Vec<DlqWrite> {
            self.dlq_writes.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MessageBackend for ScriptedBackend {
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(*self.payload_limit.lock().unwrap())
        }

        async fn publish(&self, _subject: &str, payload: &[u8]) -> SFResult<()> {
            if let Some(limit) = *self.payload_limit.lock().unwrap() {
                if payload.len() > limit {
                    return Err(SFError::PayloadTooLarge {
                        size: payload.len(),
                        limit: Some(limit),
                    });
                }
            }
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
        }

        async fn subscribe(&self, subject: &str, _group: &str) -> SFResult<MessageStream> {
            let queue = self
                .queues
                .lock()
                .unwrap()
                .remove(subject)
                .unwrap_or_default();
            let stream =
                futures::stream::iter(queue.into_iter().map(Ok)).chain(futures::stream::pending());
            Ok(Box::pin(stream))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
            self.acks
                .lock()
                .unwrap()
                .push((stream.to_string(), group.to_string(), ids.to_vec()));
            Ok(())
        }
        /// 记下参数而不是走默认实现：这一层测的是**接线**（谁在什么时候把哪条消息的字节
        /// 交出去），记录本身的形状由契约的往返测试钉。写死不覆盖就会走默认实现去
        /// `publish`，而这个 double 的 `publish` 什么都不记，接线错了也看不出来。
        async fn dlq(
            &self,
            stream: &str,
            msg_id: &str,
            payload: &[u8],
            reason: &str,
        ) -> SFResult<()> {
            self.dlq_writes.lock().unwrap().push(DlqWrite {
                stream: stream.to_string(),
                msg_id: msg_id.to_string(),
                payload: payload.to_vec(),
                reason: reason.to_string(),
            });
            Ok(())
        }
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            if self.stall_claims.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
            if self.hold_claims.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(Some(Vec::new()));
            }
            Ok(Some(std::mem::take(&mut *self.pending.lock().unwrap())))
        }
        /// The same split a real queue makes: an entry counts as unreclaimed
        /// once its outstanding time passes the caller's threshold.
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            self.pending_stats_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let count = self.pending.lock().unwrap().len() as u64;
            let idle = *self.pending_idle_ms.lock().unwrap();
            let unreclaimed = if count > 0 && idle > idle_threshold_ms {
                cog_core::PendingStats {
                    count,
                    unreclaimed_count: count,
                    unreclaimed_oldest_idle_ms: idle,
                }
            } else {
                cog_core::PendingStats {
                    count,
                    ..Default::default()
                }
            };
            Ok(Some(unreclaimed))
        }

        /// This double models what a consumer has taken and not acked, which
        /// is the other question; it keeps no unread position a group could be
        /// behind, so it answers "cannot say" rather than a made-up zero.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
    }

    fn test_runtime(backend: ScriptedBackend) -> DagExecutorRuntime {
        test_runtime_with(
            backend,
            DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-ack-test".into(),
                consumer_group: "grp-ack-test".into(),
                max_retries: 1,
                ..DagExecutorConfig::default()
            },
        )
    }

    fn test_runtime_with(
        backend: ScriptedBackend,
        config: DagExecutorConfig,
    ) -> DagExecutorRuntime {
        DagExecutorRuntime::new_with_backend(config, backend)
    }

    async fn wait_for_acks(backend: &ScriptedBackend, n: usize) -> Vec<String> {
        for _ in 0..50 {
            if backend.acks.lock().unwrap().len() >= n {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        backend.acked_ids()
    }

    fn ready_task(id: &str, input: serde_json::Value) -> cog_core::Task {
        let now = chrono::Utc::now();
        cog_core::Task {
            id: id.into(),
            task_type: cog_core::TaskType::DagNode,
            status: cog_core::TaskStatus::Pending,
            input,
            result: None,
            error: None,
            error_cause: None,
            blocked_by: vec![],
            blocks: vec![],
            priority: 1,
            created_at: now,
            updated_at: now,
            agent_id: None,
            workspace_id: Some("ws-limit-test".into()),
            retry_count: 0,
            max_retries: 1,
            retry_not_before: None,
            started_at: None,
            timeout_seconds: 30,
            lease_owner: None,
            lease_expires_at: None,
            action_planner_meta: None,
            goal_id: Some("goal-limit".into()),
            parent_task_id: None,
            sink_task_id: None,
            is_executable: true,
            face_id: None,
            owner_unit: None,
            bound_by_handover_id: None,
            // Every row this platform files says what would close it, and the
            // dispatcher holds one that does not: the fixture is ordinary work,
            // so it carries the criterion ordinary work carries.
            acceptance: vec!["这张票的判据".into()],
        }
    }

    /// An over-limit payload is held back at its own pending state; it must not
    /// stop anything else. Two things have to survive it: the other task in the
    /// same queue, and the task on the other queue. A task marked Scheduled is
    /// never offered again by the ready scan (it reads only Pending), so the one
    /// that cannot be published is the only thing that may stay Pending.
    ///
    /// Discriminating: refusing the whole call — the first cut — leaves the
    /// other two Pending and, worse, makes every caller that reads an error here
    /// as "do not ack the result" redeliver its message forever.
    #[tokio::test]
    async fn an_oversized_payload_holds_back_only_its_own_task() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-limit-test".into(),
            consumer_group: "grp-limit-test".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let runtime = test_runtime_with(backend.clone(), config);

        let small = ready_task("task-small", serde_json::json!({}));
        let big = ready_task("task-big", serde_json::json!({ "blob": "x".repeat(4096) }));
        // Self-evolution routes to the process-local queue, so this one is on a
        // different stream from the other two.
        let mut evolution = ready_task("task-evolution", serde_json::json!({}));
        evolution.task_type = cog_core::TaskType::Custom("self_evolution".into());
        runtime
            .orchestrator()
            .submit_goal(
                "goal-limit",
                vec![small.clone(), big.clone(), evolution.clone()],
            )
            .await
            .unwrap();

        // The bound sits between the payloads: small and evolution fit, big does
        // not. (The scripted backend applies one bound to every subject.)
        let small_len = serde_json::to_vec(&small).unwrap().len();
        let evolution_len = serde_json::to_vec(&evolution).unwrap().len();
        backend.set_payload_limit(Some(small_len.max(evolution_len) + 1));

        runtime
            .publish_ready_tasks()
            .await
            .expect("one over-limit payload must not fail the call");

        for id in ["task-small", "task-evolution"] {
            let task = runtime
                .orchestrator()
                .get_task(id)
                .await
                .expect("task present");
            assert_eq!(
                task.status,
                cog_core::TaskStatus::Scheduled,
                "{id} is publishable and must not be held back by a task that is not"
            );
        }
        let held = runtime
            .orchestrator()
            .get_task("task-big")
            .await
            .expect("task present");
        assert_eq!(
            held.status,
            cog_core::TaskStatus::Pending,
            "the over-limit task stays pending so the next scan offers it again"
        );
    }

    /// 只留计数器的 metrics 后端：这里要看的读数是「谁被上限拦了」，gauge 与
    /// 查询都不参与。
    /// 一次计数写入：指标名、值、标签集。
    type Write = (String, f64, HashMap<String, String>);

    #[derive(Default)]
    struct Counters {
        writes: std::sync::Mutex<Vec<Write>>,
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for Counters {
        async fn record_gauge(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.writes
                .lock()
                .unwrap()
                .push((name.as_str().to_string(), value, labels));
            Ok(())
        }
        async fn record_histogram(
            &self,
            _: cog_core::MetricName,
            _: f64,
            _: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _: &str,
            _: chrono::DateTime<chrono::Utc>,
            _: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 上限拦下活这件事要在读数里看得见，而且要点名是哪个项目、哪一列：一个
    /// 只有条数的读数说不出的，正是「该去松哪一列」。
    #[tokio::test]
    async fn the_column_that_refused_is_named_in_the_reading() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-wip-reading".into(),
            consumer_group: "grp-wip-reading".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let (limits, _) = cog_core::BoardWipLimits::from_spec("scheduled=1");
        let runtime = test_runtime_with(backend, config).with_board_wip_limits(limits);
        let counters = Arc::new(Counters::default());
        runtime.orchestrator().attach_metrics(counters.clone());

        let under = |id: &str| {
            let mut t = ready_task(id, serde_json::json!({}));
            t.goal_id = Some("goal-wip".into());
            t
        };
        runtime
            .orchestrator()
            .submit_goal("goal-wip", vec![under("task-1"), under("task-2")])
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let writes = counters.writes.lock().unwrap().clone();
        let held: Vec<&(String, f64, HashMap<String, String>)> = writes
            .iter()
            .filter(|(name, _, _)| name == "cogneva_board_column_held_total")
            .collect();
        assert_eq!(
            held.len(),
            1,
            "一条判词，不是每条被拦的任务一条：{writes:?}"
        );
        assert_eq!(held[0].1, 1.0, "被拦下的是一条");
        assert_eq!(
            held[0].2.get(crate::LABEL_COLUMN).map(String::as_str),
            Some("scheduled")
        );
        assert_eq!(
            held[0].2.get(crate::LABEL_PROJECT).map(String::as_str),
            Some("goal-wip")
        );
    }

    /// 列上限是准入控制，不是画在列头上的红字：这一列满了，新活留在
    /// `Pending`，下一轮再看——而不是照发出去、然后在界面上标红。
    ///
    /// 区分在这一列上：若上限只是「读面标红」，三条任务会全部变成
    /// `Scheduled`，这条断言就会红。
    #[tokio::test]
    async fn a_full_board_column_holds_ready_tasks_back_in_pending() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-wip-test".into(),
            consumer_group: "grp-wip-test".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let (limits, problems) = cog_core::BoardWipLimits::from_spec("scheduled=1");
        assert!(problems.is_empty(), "{problems:?}");
        let runtime = test_runtime_with(backend, config).with_board_wip_limits(limits);

        let urgent = {
            let mut t = ready_task("task-urgent", serde_json::json!({}));
            t.priority = 9;
            t
        };
        runtime
            .orchestrator()
            .submit_goal(
                "goal-wip",
                vec![
                    urgent,
                    ready_task("task-2", serde_json::json!({})),
                    ready_task("task-3", serde_json::json!({})),
                ],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let status = |id: &'static str| {
            let orchestrator = runtime.orchestrator().clone();
            async move {
                orchestrator
                    .get_task(id)
                    .await
                    .unwrap_or_else(|| panic!("{id} present"))
                    .status
            }
        };
        assert_eq!(
            status("task-urgent").await,
            cog_core::TaskStatus::Scheduled,
            "上限只放一条时，放的是优先级最高的那条"
        );
        for held in ["task-2", "task-3"] {
            assert_eq!(
                status(held).await,
                cog_core::TaskStatus::Pending,
                "列满了就留在 Pending，等下一轮——放出去才是真的超了"
            );
        }
    }

    /// `running` 的上限不是发布那一刻的快照：发布侧只判了「发布时这一列空着」，
    /// 之后 `scheduled` 里攒着的几条一起启动，就能把它顶过上限。
    ///
    /// 区分在这一列上：若 `start_task` 不判上限，第二条也会变成 `Running`，
    /// 而看板那一格仍然印着「1/1 未超」——上限看着在、实际拦不住。
    #[tokio::test]
    async fn starting_a_task_respects_the_running_column_limit() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-wip-start".into(),
            consumer_group: "grp-wip-start".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let (limits, problems) = cog_core::BoardWipLimits::from_spec("running=1");
        assert!(problems.is_empty(), "{problems:?}");
        let runtime = test_runtime_with(backend, config).with_board_wip_limits(limits);
        runtime
            .orchestrator()
            .submit_goal(
                "goal-start",
                vec![
                    ready_task("task-a", serde_json::json!({})),
                    ready_task("task-b", serde_json::json!({})),
                ],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        runtime.orchestrator().start_task("task-a").await.unwrap();
        let refused = runtime.orchestrator().start_task("task-b").await;
        assert!(
            refused.is_err(),
            "running 列上限是 1、已经起了一条，第二条不许再进"
        );
        assert_eq!(
            runtime
                .orchestrator()
                .get_task("task-b")
                .await
                .expect("task-b present")
                .status,
            cog_core::TaskStatus::Scheduled,
            "被拒的那条要留在 Scheduled：拒了还把它移过去就只是没报错"
        );
    }

    /// A task that names its executor must not be offered to the queue every
    /// worker competes for: the first reader of an entry runs it, so an
    /// assigned task published there goes to whoever happens to be idle.
    ///
    /// Discriminating on the column: the designation has to land on the one
    /// the ready scan reads. Written to the delivery column instead, the row
    /// reads back as assigned and still goes out — which is what made an
    /// assignment change nothing about who ran the task.
    #[tokio::test]
    async fn a_task_named_to_an_executor_is_not_offered_to_the_shared_queue() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-limit-test".into(),
            consumer_group: "grp-limit-test".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let runtime = test_runtime_with(backend.clone(), config);
        runtime
            .orchestrator()
            .submit_goal(
                "goal-limit",
                vec![
                    ready_task("task-owned", serde_json::json!({})),
                    ready_task("task-free", serde_json::json!({})),
                ],
            )
            .await
            .unwrap();

        runtime
            .orchestrator()
            .assign_task("task-owned", "unit-6e")
            .await
            .expect("a pending task takes a binding");
        runtime.publish_ready_tasks().await.unwrap();

        let owned = runtime
            .orchestrator()
            .get_task("task-owned")
            .await
            .expect("task present");
        assert_eq!(
            owned.owner_unit.as_deref(),
            Some("unit-6e"),
            "the designation lands on the column the ready scan reads"
        );
        assert_eq!(
            owned.agent_id, None,
            "nobody has delivered this result yet, so the delivery column stays empty"
        );
        assert_eq!(
            owned.status,
            cog_core::TaskStatus::Pending,
            "an owned task waits for its owner instead of being published"
        );

        let free = runtime
            .orchestrator()
            .get_task("task-free")
            .await
            .expect("task present");
        assert_eq!(
            free.status,
            cog_core::TaskStatus::Scheduled,
            "the unowned task from the same submission is still published"
        );

        // Once the row has been offered, a binding can no longer be honoured:
        // the run that happens may already be somebody else's, and completion
        // refuses a result that does not come from the owner. Refusing here
        // keeps the caller from believing it bound work it did not.
        let err = runtime
            .orchestrator()
            .assign_task("task-free", "unit-6e")
            .await
            .expect_err("an offered task cannot be re-bound");
        assert!(
            err.to_string().contains("already been offered"),
            "the refusal has to say why: {err}"
        );
    }

    /// Where a task came from is bookkeeping, not a routing fact: two tasks
    /// that differ only in `input.source` have to be allocated the same way.
    /// A pair that splits — one queued, one held — would mean the allocator is
    /// reading the origin of the work, and no caller above it could undo that.
    ///
    /// Both directions are checked because they are the two allocations there
    /// are: offered to the shared queue, and held for a named owner.
    #[tokio::test]
    async fn allocation_is_source_independent() {
        async fn statuses_after_publish(
            owned: bool,
        ) -> (cog_core::TaskStatus, cog_core::TaskStatus) {
            let backend = ScriptedBackend::default();
            let config = DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-limit-test".into(),
                consumer_group: "grp-limit-test".into(),
                max_retries: 1,
                ..DagExecutorConfig::default()
            };
            let runtime = test_runtime_with(backend.clone(), config);
            runtime
                .orchestrator()
                .submit_goal(
                    "goal-limit",
                    vec![
                        ready_task("task-github", serde_json::json!({ "source": "github" })),
                        ready_task("task-manual", serde_json::json!({ "source": "manual" })),
                    ],
                )
                .await
                .unwrap();
            if owned {
                for id in ["task-github", "task-manual"] {
                    runtime
                        .orchestrator()
                        .assign_task(id, "unit-6e")
                        .await
                        .expect("a pending task takes a binding");
                }
            }
            runtime.publish_ready_tasks().await.unwrap();
            let mut got = Vec::new();
            for id in ["task-github", "task-manual"] {
                got.push(
                    runtime
                        .orchestrator()
                        .get_task(id)
                        .await
                        .expect("task present")
                        .status,
                );
            }
            (got[0], got[1])
        }

        let free = statuses_after_publish(false).await;
        assert_eq!(
            free.0, free.1,
            "both unowned tasks go to the shared queue whatever their source: {free:?}"
        );
        assert_eq!(free.0, cog_core::TaskStatus::Scheduled);

        let owned = statuses_after_publish(true).await;
        assert_eq!(
            owned.0, owned.1,
            "both tasks bound to one unit wait for it, whatever their source: {owned:?}"
        );
        assert_eq!(owned.0, cog_core::TaskStatus::Pending);
    }

    #[tokio::test]
    async fn stale_result_for_unknown_task_is_acked() {
        // Regression: run_consumer discarded msg_id and never acked, so stale
        // results sat in the PEL and were redelivered on every restart/sweep.
        let backend = ScriptedBackend::default();
        let msg = DagMessage::TaskComplete {
            message_id: "m1".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-does-not-exist".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        let bytes = serde_json::to_vec(&msg).unwrap();
        backend.enqueue("orchestrator:results:ws-ack-test", "rid-1", bytes);

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-1".to_string()]);
    }

    #[tokio::test]
    async fn poison_result_message_is_acked() {
        let backend = ScriptedBackend::default();
        backend.enqueue(
            "orchestrator:results:ws-ack-test",
            "rid-poison",
            b"not-json".to_vec(),
        );

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-poison".to_string()]);
    }

    /// 毒消息在 ack 之前先落到死信队列，带着它自己的字节。
    ///
    /// ack 是不可逆的那一步：它一落地，这份字节在这个进程里就没有第二处了。修之前这条
    /// 路径只留一行日志，而日志不是能读回消息的地方——「有个消息死了」和「死的那条里
    /// 写了什么」是两个问题，只答得出前一个的死信队列不值得存在。
    #[tokio::test]
    async fn a_poison_result_reaches_the_dead_letter_queue_carrying_its_bytes() {
        let backend = ScriptedBackend::default();
        let poison = b"not-json \x00\xff".to_vec();
        backend.enqueue(
            "orchestrator:results:ws-ack-test",
            "rid-poison",
            poison.clone(),
        );

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        let writes = backend.dlq_writes();
        assert_eq!(writes.len(), 1, "一条毒消息对应一笔死信写");
        let write = &writes[0];
        assert_eq!(
            write.stream, "orchestrator:results:ws-ack-test",
            "写回它来的那条流"
        );
        assert_eq!(write.msg_id, "rid-poison");
        assert_eq!(
            write.payload, poison,
            "原样带过去——这一格丢了，死信记录就只剩「有个消息死了」"
        );
        assert!(
            write.reason.contains("unparsable"),
            "理由要说明它为什么会在这儿，实际是 {:?}",
            write.reason
        );
    }

    #[tokio::test]
    async fn abandoned_result_is_reclaimed_and_applied() {
        // Regression: `subscribe` reads with XREADGROUP ">" (new messages only),
        // so a result delivered to a consumer that died before acking was never
        // seen again — the task sat Running in the DAG forever and its
        // dependents never became ready. The live loop must not be the only way
        // a result reaches the DAG.
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-ack-test".into(),
            consumer_group: "grp-ack-test".into(),
            max_retries: 1,
            result_claim_idle_secs: 0,
            result_claim_interval_secs: 1,
            result_claim_batch: 16,
            ready_task_poll_interval_secs: 0,
        };
        let runtime = test_runtime_with(backend.clone(), config);

        let task = cog_core::Task {
            id: "task-reclaimed".into(),
            task_type: cog_core::TaskType::DagNode,
            status: cog_core::TaskStatus::Pending,
            input: serde_json::json!({}),
            result: None,
            error: None,
            error_cause: None,
            blocked_by: vec![],
            blocks: vec![],
            priority: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            agent_id: None,
            workspace_id: Some("ws-ack-test".into()),
            retry_count: 0,
            max_retries: 1,
            retry_not_before: None,
            started_at: None,
            timeout_seconds: 30,
            lease_owner: None,
            lease_expires_at: None,
            action_planner_meta: None,
            goal_id: Some("goal-reclaimed".into()),
            parent_task_id: None,
            sink_task_id: None,
            is_executable: true,
            face_id: None,
            owner_unit: None,
            bound_by_handover_id: None,
            acceptance: vec![],
        };
        runtime
            .orchestrator()
            .submit_goal("goal-reclaimed", vec![task])
            .await
            .unwrap();
        // complete_task only accepts a Running task.
        runtime
            .orchestrator()
            .schedule_task("task-reclaimed")
            .await
            .unwrap();
        runtime
            .orchestrator()
            .start_task("task-reclaimed")
            .await
            .unwrap();

        // Nothing on the live subscribe path: the result exists only as a
        // pending, unacked entry.
        let msg = DagMessage::TaskComplete {
            message_id: "m-pending".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-reclaimed".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        backend.abandon("rid-pending", serde_json::to_vec(&msg).unwrap());

        let dag = runtime.orchestrator().clone();
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-pending".to_string()]);
        // The point is the state transition, not merely draining the entry.
        let task = dag.get_task("task-reclaimed").await.expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Completed);
    }

    /// Poll the process-wide pending observable until the stream shows up, so
    /// the assertion is about a measurement the sweep actually made rather than
    /// about a sleep having been long enough.
    async fn wait_for_stream_metrics(stream: &str) -> Vec<cog_core::observability::RawMetric> {
        let observer = crate::observable::stream_pending_observable();
        for _ in 0..100 {
            let metrics = observer.collect_metrics("D8").await.unwrap();
            if metrics
                .iter()
                .any(|m| m.labels.get(crate::LABEL_STREAM).map(String::as_str) == Some(stream))
            {
                return metrics;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("no pending metrics published for {stream} within 5s");
    }

    /// The reclaim pass has to report what it is holding. A message stranded by
    /// a dead consumer stayed invisible for two and a half days precisely
    /// because the loop that could see it published nothing: the fix that takes
    /// the message back is only half the repair, the other half is a number
    /// something can alert on.
    #[tokio::test]
    async fn the_sweep_publishes_what_it_could_not_reclaim() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-observe-test".into(),
            consumer_group: "grp-observe-test".into(),
            max_retries: 1,
            result_claim_idle_secs: 600,
            result_claim_interval_secs: 1,
            result_claim_batch: 16,
            ready_task_poll_interval_secs: 0,
        };
        let runtime = test_runtime_with(backend.clone(), config);
        let stream = "orchestrator:results:ws-observe-test".to_string();

        // A result delivered to a consumer that died, 601s stale, on a reclaim
        // pass that cannot take it back.
        let msg = DagMessage::TaskComplete {
            message_id: "m-observed".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-observed".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        backend.abandon("rid-observed", serde_json::to_vec(&msg).unwrap());
        backend.set_pending_idle_ms(601_000);
        backend.hold_claims();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let metrics = wait_for_stream_metrics(&stream).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        let of = |name: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name
                        && m.labels.get(crate::LABEL_STREAM).map(String::as_str)
                            == Some(stream.as_str())
                })
                .unwrap_or_else(|| panic!("{name} not published for {stream}"))
                .value
        };
        assert_eq!(
            of(crate::observable::STREAM_PENDING_COUNT_METRIC),
            1.0,
            "the pending entry itself must be reported"
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_UNRECLAIMED_METRIC),
            1.0,
            "an entry the reclaim pass could not take back is the finding"
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_UNRECLAIMED_AGE_METRIC),
            601.0
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_CLAIM_IDLE_METRIC),
            600.0,
            "the rule compares the age against the threshold the pass actually uses"
        );
        assert!(
            of(crate::observable::STREAM_PENDING_MEASURE_LAST_METRIC) > 0.0,
            "a measured stream must carry the timestamp its staleness is judged against"
        );
    }

    /// The measurement keeps its own cadence. It used to run inside the reclaim
    /// pass's tick, so a round that took a long time — or never came back —
    /// froze the series along with it, and a frozen series is the same picture
    /// at the scrape as a clean stream. Here the reclaim round never returns,
    /// and the measurement still has to keep running behind it.
    #[tokio::test]
    async fn a_reclaim_round_that_never_returns_does_not_freeze_the_measurement() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-measure-cadence".into(),
            consumer_group: "grp-measure-cadence".into(),
            max_retries: 1,
            result_claim_idle_secs: 600,
            result_claim_interval_secs: 1,
            result_claim_batch: 16,
            ready_task_poll_interval_secs: 0,
        };
        let runtime = test_runtime_with(backend.clone(), config);
        let msg = DagMessage::TaskComplete {
            message_id: "m-stalled".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-stalled".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        backend.abandon("rid-stalled", serde_json::to_vec(&msg).unwrap());
        backend.set_pending_idle_ms(601_000);
        backend.stall_claims();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });

        // A wedged tick could have produced exactly the one reading taken before
        // the loop started; several readings at a 1s cadence can only come from a
        // measurement that is not waiting on the reclaim round.
        for _ in 0..200 {
            if backend.pending_stats_calls() >= 3 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let calls = backend.pending_stats_calls();
        shutdown.trigger();
        let _ = handle.await;

        // The round really is stuck, so those readings cannot have come from it:
        // a round that had returned would have taken the message back and acked
        // it. Without this, a `stall_claims` that silently did nothing would let
        // the measurement look independent while it was still on the tick.
        assert!(
            backend.acked_ids().is_empty(),
            "the reclaim round completed, so this test proves nothing about a stalled one"
        );
        assert!(
            calls >= 3,
            "the measurement stopped with the reclaim round: {calls} reading(s) in ~10s at a 1s cadence"
        );
    }

    #[tokio::test]
    async fn processed_goal_is_acked() {
        let backend = ScriptedBackend::default();
        let goal = cog_core::GoalMessage {
            message_id: "g1".into(),
            timestamp: chrono::Utc::now(),
            workspace_id: "ws-ack-test".into(),
            goal_id: "goal-1".into(),
            goal: "noop goal".into(),
            tasks: vec![],
            priority: 0,
            source: cog_core::GoalSource::Internal,
        };
        let bytes = serde_json::to_vec(&goal).unwrap();
        backend.enqueue("goals:ws-ack-test", "gid-1", bytes);

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_goal_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["gid-1".to_string()]);
    }

    /// 一份被 DAG 判退的计划是**确定性**的：同样的字节下一次重投还是同样的裁决。
    /// 留在 pending 里就是无限重投、且永远不落一句结论，所以它按毒消息处理——ack
    /// 掉并把裁决连同那些 id 写进日志。这里的计划里 `t5` 依赖 `t1`，而 `t1` 已被
    /// 另一份工作占着，正是生产上 t5..t9 被挂到别人行上的那个形状。
    #[tokio::test]
    async fn a_refused_plan_is_acked_instead_of_redelivered_forever() {
        let backend = ScriptedBackend::default();
        let runtime = test_runtime(backend.clone());
        runtime
            .orchestrator()
            .add_tasks_batch(vec![cog_core::Task::new(
                "t1",
                cog_core::TaskType::Generator,
                serde_json::json!({"goal": "another plan"}),
            )])
            .await
            .unwrap();

        let mut dependent = cog_core::Task::new(
            "t5",
            cog_core::TaskType::Generator,
            serde_json::json!({"goal": "this plan"}),
        );
        dependent.blocked_by = vec!["t1".into()];
        let goal = cog_core::GoalMessage {
            message_id: "g-refused".into(),
            timestamp: chrono::Utc::now(),
            workspace_id: "ws-ack-test".into(),
            goal_id: "goal-refused".into(),
            goal: "plan with reused ids".into(),
            tasks: vec![
                cog_core::Task::new(
                    "t1",
                    cog_core::TaskType::Generator,
                    serde_json::json!({"goal": "this plan"}),
                ),
                dependent,
            ],
            priority: 0,
            source: cog_core::GoalSource::Internal,
        };
        backend.enqueue(
            "goals:ws-ack-test",
            "gid-refused",
            serde_json::to_vec(&goal).unwrap(),
        );

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_goal_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["gid-refused".to_string()]);
    }

    // ─── 计划预算：第四道准入 ───

    /// 只答计划基线的 StateBackend。其余方法是契约的其余部分，这条路径不读，
    /// 一律返回空——写成 `unimplemented!()` 会让「没读」从事实退化成假设。
    struct PlanBook {
        /// 表里现有的基线。写进去的会落回这里，跟真后端一样是 upsert——读回
        /// 一份没变的表，会让「写一次就够」这类判据永远看不到第二拍。
        plans: std::sync::Mutex<Vec<cog_core::TaskPlan>>,
        /// 按种类量过的历史。空表示「这一类还没有样本」。
        records: Vec<cog_core::TaskKindRecord>,
        /// 这一趟写下去的基线。判据看写下去的字节，不看写没写。
        saved: std::sync::Mutex<Vec<cog_core::TaskPlan>>,
    }

    impl PlanBook {
        fn with_plans(plans: Vec<cog_core::TaskPlan>) -> Self {
            Self {
                plans: std::sync::Mutex::new(plans),
                records: Vec::new(),
                saved: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn saved(&self) -> Vec<cog_core::TaskPlan> {
            self.saved.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl cog_core::StateBackend for PlanBook {
        async fn get_agent_state(
            &self,
            _agent_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::AgentState>> {
            Ok(None)
        }
        async fn set_agent_state(
            &self,
            _agent_id: &str,
            _state: &cog_core::AgentState,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn cas_agent_state(
            &self,
            _agent_id: &str,
            _expected: &cog_core::AgentState,
            _new: &cog_core::AgentState,
        ) -> cog_core::SFResult<bool> {
            Ok(true)
        }
        async fn get_events(
            &self,
            _task_id: &str,
            _offset: u64,
            _limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::Event>> {
            Ok(Vec::new())
        }
        async fn get_checkpoint(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::TaskCheckpoint>> {
            Ok(None)
        }
        async fn save_checkpoint(
            &self,
            _checkpoint: &cog_core::TaskCheckpoint,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn append_event(
            &self,
            _task_id: &str,
            _event: &cog_core::Event,
        ) -> cog_core::SFResult<u64> {
            Ok(0)
        }
        async fn get_board(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::ContextBoard>> {
            Ok(None)
        }
        async fn set_board_field(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn delete_checkpoint(&self, _task_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn delete_board(&self, _task_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn remove_board_field(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn task_plans(
            &self,
            _workspace_id: &str,
        ) -> cog_core::SFResult<Vec<cog_core::TaskPlan>> {
            Ok(self.plans.lock().unwrap().clone())
        }
        async fn save_task_plan(&self, plan: &cog_core::TaskPlan) -> cog_core::SFResult<()> {
            self.saved.lock().unwrap().push(plan.clone());
            let mut plans = self.plans.lock().unwrap();
            match plans.iter_mut().find(|row| row.task_id == plan.task_id) {
                Some(row) => *row = plan.clone(),
                None => plans.push(plan.clone()),
            }
            Ok(())
        }
        /// 与真正的后端同一套语义：样本不够 `min_samples` 的种类不出现。这样
        /// 一条样本不足的用例验的才是调用方传下去的那个阈值，不是夹具自己
        /// 记得去过滤。
        async fn task_kind_records(
            &self,
            _workspace_id: &str,
            min_samples: u64,
        ) -> cog_core::SFResult<Vec<cog_core::TaskKindRecord>> {
            Ok(self
                .records
                .iter()
                .filter(|r| r.samples >= min_samples)
                .cloned()
                .collect())
        }
    }

    /// 只答每条任务 token 清点的观测面。没有行的任务照契约报错——真实实现就是
    /// 这么答的，把「没有行」也编成零会让这条路径永远看不到「读不到」。
    #[derive(Default)]
    struct Census {
        spent: Mutex<HashMap<String, u64>>,
    }

    impl Census {
        fn with(rows: &[(&str, u64)]) -> Self {
            let mut spent = HashMap::new();
            for (task_id, tokens) in rows {
                spent.insert((*task_id).to_string(), *tokens);
            }
            Self {
                spent: Mutex::new(spent),
            }
        }
    }

    #[async_trait]
    impl cog_core::ObservabilityGateway for Census {
        async fn subscribe_events(
            &self,
            _filter: cog_core::EventFilter,
        ) -> cog_core::SFResult<cog_core::observability::AgentEventStream> {
            unimplemented!("the budget path never subscribes")
        }
        async fn get_agent_state(
            &self,
            _agent_id: &str,
        ) -> cog_core::SFResult<cog_core::AgentState> {
            unimplemented!("the budget path never reads agent state")
        }
        async fn get_task_checkpoint(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::TaskCheckpoint>> {
            unimplemented!("the budget path never reads checkpoints")
        }
        async fn get_task_metrics(
            &self,
            task_id: &str,
        ) -> cog_core::SFResult<cog_core::TaskMetrics> {
            match self.spent.lock().unwrap().get(task_id).copied() {
                Some(total_tokens) => Ok(cog_core::TaskMetrics {
                    task_id: task_id.into(),
                    total_tokens,
                    prompt_tokens: total_tokens,
                    completion_tokens: 0,
                    tool_calls: 0,
                    iterations: 1,
                    duration_ms: 1,
                    timestamp: chrono::Utc::now(),
                }),
                None => Err(cog_core::SFError::Agent(format!(
                    "metrics not found for task {task_id}"
                ))),
            }
        }
        async fn record_task_metrics(
            &self,
            _metrics: cog_core::TaskMetrics,
        ) -> cog_core::SFResult<()> {
            unimplemented!("the budget path never writes the census")
        }
        async fn get_task_logs(
            &self,
            _task_id: &str,
            _limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::LogEntry>> {
            unimplemented!("the budget path never reads logs")
        }
        async fn get_snapshot_url(&self, _snapshot_id: &str) -> cog_core::SFResult<String> {
            unimplemented!("the budget path never reads snapshots")
        }
        async fn get_raw_log_index(
            &self,
            _stream: &str,
            _date: chrono::NaiveDate,
        ) -> cog_core::SFResult<Vec<cog_core::RawLogIndex>> {
            unimplemented!("the budget path never reads raw logs")
        }
        async fn get_cluster_overview(&self) -> cog_core::SFResult<cog_core::ClusterOverview> {
            unimplemented!("the budget path never reads the cluster")
        }
        async fn get_squad_state(
            &self,
            _squad_id: &str,
        ) -> cog_core::SFResult<cog_core::SquadState> {
            unimplemented!("the budget path never reads squads")
        }
        fn publish_event(&self, _event: cog_core::AgentEvent) {}
    }

    fn plan(task_id: &str, budget_tokens: u64) -> cog_core::TaskPlan {
        cog_core::TaskPlan {
            workspace_id: "ws-budget-test".into(),
            task_id: task_id.into(),
            planned_start: None,
            planned_end: None,
            budget_tokens,
            baseline_rev: "rev-1".into(),
            updated_at: chrono::Utc::now(),
        }
    }

    /// 一条已经跑过一轮、又回到就绪队列的任务：`started_at` 是「它跑过」的
    /// 落盘证据，重排只改状态不清它。
    fn ran_before(mut task: cog_core::Task) -> cog_core::Task {
        task.started_at = Some(chrono::Utc::now() - chrono::Duration::minutes(5));
        task
    }

    fn budget_runtime(census: Census, plans: Vec<cog_core::TaskPlan>) -> DagExecutorRuntime {
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-budget-test".into(),
            consumer_group: "grp-budget-test".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let orchestrator = DagExecutor::new("ws-budget-test".into())
            .with_state_backend(Arc::new(PlanBook::with_plans(plans)));
        let runtime = DagExecutorRuntime::new_with_backend(config, ScriptedBackend::default())
            .with_orchestrator(Arc::new(orchestrator));
        runtime.attach_observability(Arc::new(census));
        runtime
    }

    /// 计划预算是一道准入，不是面板上的一个进度条：花完了这条活在下一轮不派，
    /// 留在 `Pending`，等一次重规划重新排预算——而不是照发出去然后再记一笔超支。
    ///
    /// 区分在这条上：若预算只是读数，两条任务都会是 `Scheduled`，这条断言就红。
    #[tokio::test]
    async fn a_ready_task_that_spent_its_plan_budget_is_held_back() {
        let runtime = budget_runtime(
            Census::with(&[("task-spent", 150), ("task-room", 50)]),
            vec![plan("task-spent", 100), plan("task-room", 100)],
        );
        let counters = Arc::new(Counters::default());
        runtime.orchestrator().attach_metrics(counters.clone());

        runtime
            .orchestrator()
            .submit_goal(
                "goal-budget",
                vec![
                    ran_before(ready_task("task-spent", serde_json::json!({}))),
                    ran_before(ready_task("task-room", serde_json::json!({}))),
                ],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let orchestrator = runtime.orchestrator().clone();
        let status = |id: &str| {
            let orchestrator = orchestrator.clone();
            let id = id.to_string();
            async move {
                orchestrator
                    .get_task(&id)
                    .await
                    .unwrap_or_else(|| panic!("{id} present"))
                    .status
            }
        };
        assert_eq!(
            status("task-spent").await,
            cog_core::TaskStatus::Pending,
            "花完预算的一条留在 Pending：派出去就是花一份没人批过的钱"
        );
        assert_eq!(
            status("task-room").await,
            cog_core::TaskStatus::Scheduled,
            "还没花完的一条照派——预算是逐条的，不是把一个项目的账摊到每条上"
        );

        let writes = counters.writes.lock().unwrap().clone();
        let held: Vec<&(String, f64, HashMap<String, String>)> = writes
            .iter()
            .filter(|(name, _, _)| name == "cogneva_ready_admission_held_total")
            .collect();
        assert_eq!(held.len(), 1, "这一拍只拦了一条：{writes:?}");
        assert_eq!(held[0].1, 1.0);
        assert_eq!(
            held[0].2.get(crate::LABEL_REASON).map(String::as_str),
            Some("budget_spent"),
            "原因指到预算这道闸门，不是池窗口——修法完全不同"
        );
        assert_eq!(
            held[0].2.get(crate::LABEL_STREAM).map(String::as_str),
            Some("shared")
        );
    }

    /// 没有清点行不是「花完了」：清点写是尽力而为的，把它读成零会让一条写失败的
    /// 日志永久停掉这条任务线。读不到就放行，与池读数缺失时的处置同一条规矩。
    #[tokio::test]
    async fn a_task_whose_spending_was_never_read_is_not_held() {
        let runtime = budget_runtime(Census::default(), vec![plan("task-unread", 100)]);
        runtime
            .orchestrator()
            .submit_goal(
                "goal-budget",
                vec![ran_before(ready_task("task-unread", serde_json::json!({})))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let task = runtime
            .orchestrator()
            .get_task("task-unread")
            .await
            .expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Scheduled);
    }

    /// 零预算是「这条基线没给 token 计划」，不是「计划花零个 token」。若有清点行
    /// 而预算是零，每一次派发都会被拦下，整条任务线停在原地等一个永不会来的预算。
    #[tokio::test]
    async fn a_plan_with_no_budget_does_not_hold_anything() {
        let runtime = budget_runtime(
            Census::with(&[("task-nobudget", 999_999)]),
            vec![plan("task-nobudget", 0)],
        );
        runtime
            .orchestrator()
            .submit_goal(
                "goal-budget",
                vec![ran_before(ready_task(
                    "task-nobudget",
                    serde_json::json!({}),
                ))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let task = runtime
            .orchestrator()
            .get_task("task-nobudget")
            .await
            .expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Scheduled);
    }

    /// 从没跑过的一条不去问清点：问一次就是一趟白跑的库往返，而就绪队列每一拍
    /// 都可能很大。这里让清点里放一个巨大的数——若过滤没在问之前生效，它会被拦下。
    #[tokio::test]
    async fn a_task_that_never_ran_does_not_ask_the_census() {
        let runtime = budget_runtime(
            Census::with(&[("task-first-run", u64::MAX)]),
            vec![plan("task-first-run", 100)],
        );
        runtime
            .orchestrator()
            .submit_goal(
                "goal-budget",
                vec![ready_task("task-first-run", serde_json::json!({}))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let task = runtime
            .orchestrator()
            .get_task("task-first-run")
            .await
            .expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Scheduled);
    }

    // ─── 按历史排期：甘特条与挣值的那一头 ───

    /// 只有排期这一条腿的运行时。
    fn estimate_runtime(book: Arc<PlanBook>) -> DagExecutorRuntime {
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-budget-test".into(),
            consumer_group: "grp-budget-test".into(),
            max_retries: 1,
            ..DagExecutorConfig::default()
        };
        let orchestrator = DagExecutor::new("ws-budget-test".into()).with_state_backend(book);
        DagExecutorRuntime::new_with_backend(config, ScriptedBackend::default())
            .with_orchestrator(Arc::new(orchestrator))
    }

    fn record(kind: &str, samples: u64, tokens: u64, secs: u64) -> cog_core::TaskKindRecord {
        cog_core::TaskKindRecord {
            kind: kind.into(),
            samples,
            median_tokens: tokens,
            median_duration_secs: secs,
        }
    }

    /// 一件新活按同类历史的中位数排期：预算取 token 中位数，止期取时长中位数。
    ///
    /// 这是甘特条与挣值计划价值的唯一来源——没有这一步，任务图里 0 条基线，
    /// 甘特把每一行都标成未排期，挣值的三个量一个都算不出来。
    #[tokio::test]
    async fn a_new_task_is_scheduled_from_what_its_kind_has_measured() {
        let book = Arc::new(PlanBook {
            records: vec![record("dag_node", 7, 1_000, 600)],
            ..PlanBook::with_plans(Vec::new())
        });
        let runtime = estimate_runtime(book.clone());
        runtime
            .orchestrator()
            .submit_goal(
                "goal-estimate",
                vec![ready_task("task-new", serde_json::json!({}))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let saved = book.saved();
        assert_eq!(saved.len(), 1, "一件没有基线的新活应当被排一次期");
        let plan = &saved[0];
        assert_eq!(plan.task_id, "task-new");
        assert_eq!(plan.budget_tokens, 1_000, "预算取同类 token 中位数");
        let start = plan.planned_start.expect("排了期就有起期");
        let end = plan.planned_end.expect("时长有样本就有止期");
        assert_eq!(
            (end - start).num_seconds(),
            600,
            "止期 = 起期 + 同类时长中位数"
        );
        assert!(
            plan.baseline_rev
                .starts_with("estimate/work-time-v1/dag_node/n7/"),
            "基线版本要写明它是按几条样本、哪把尺子估出来的，得到 {}",
            plan.baseline_rev
        );
    }

    /// 按旧尺子写过期的估算行会在原地重算：起期留在当时，止期按现在的读数重写。
    ///
    /// 不重算的代价是现网实测的：旧尺子把排队时间当工时，五个样本量出 7.66 天，
    /// 按它排的 1804 行在甘特上各画一周。而这些行一旦被写过就不再进估算——
    /// `planned` 里有了这条任务的 id，它「已经排过期」了——所以尺子换了它们
    /// 也不会自己变；删掉又连起期和预算的来历一起没了。
    ///
    /// 这一条也同样钉住「不必就绪」：这条任务不在就绪队列里，仍然要重算。
    #[tokio::test]
    async fn an_estimate_written_by_an_older_ruler_is_remeasured_in_place() {
        let start = chrono::Utc::now() - chrono::Duration::days(3);
        let mut old = plan("task-stale", 0);
        old.planned_start = Some(start);
        old.planned_end = Some(start + chrono::Duration::days(7));
        old.baseline_rev = "estimate/dag_node/n5/2026-10-25T06:00:00+00:00".into();
        let book = Arc::new(PlanBook {
            records: vec![record("dag_node", 6, 1_000, 406)],
            ..PlanBook::with_plans(vec![old])
        });
        let runtime = estimate_runtime(book.clone());
        runtime.publish_ready_tasks().await.unwrap();

        let saved = book.saved();
        assert_eq!(saved.len(), 1, "旧尺子写的那一行要重算一次：{saved:#?}");
        let plan = &saved[0];
        assert_eq!(plan.task_id, "task-stale");
        assert_eq!(
            plan.planned_start,
            Some(start),
            "起期是当时的现场，重算只换止期"
        );
        assert_eq!(
            plan.planned_end.expect("时长有样本就有止期"),
            start + chrono::Duration::seconds(406),
            "止期按现在的时长中位数重写，不再是一周"
        );
        assert_eq!(plan.budget_tokens, 1_000, "预算一样按现在的读数重写");
        assert!(
            plan.baseline_rev
                .starts_with("estimate/work-time-v1/dag_node/n6/"),
            "重算后的版本串要换成新尺子的名字，否则下一拍还会被当成旧行，得到 {}",
            plan.baseline_rev
        );

        // 重算过就不再是旧行：同一批行、同一份读数，第二拍什么都不写。没有这
        // 一条，上面那笔好读数是靠每拍重写 1804 行换来的。
        runtime.publish_ready_tasks().await.unwrap();
        assert_eq!(
            book.saved().len(),
            1,
            "换过尺子的行第二拍不该再写一次：{:#?}",
            book.saved()
        );
    }

    /// 样本不足的种类不排期——但任务照派。排期是读数的一半，不是派发的前置条件：
    /// 为了补一行基线把活压住，是拿交付换仪表盘。
    #[tokio::test]
    async fn a_kind_without_enough_samples_is_left_unscheduled_and_still_dispatched() {
        let book = Arc::new(PlanBook {
            records: vec![record("dag_node", 2, 1_000, 600)],
            ..PlanBook::with_plans(Vec::new())
        });
        let runtime = estimate_runtime(book.clone());
        runtime
            .orchestrator()
            .submit_goal(
                "goal-estimate",
                vec![ready_task("task-thin", serde_json::json!({}))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        assert!(
            book.saved().is_empty(),
            "两条样本的中位数是噪声，不许写成计划"
        );
        let task = runtime
            .orchestrator()
            .get_task("task-thin")
            .await
            .expect("task present");
        assert_eq!(
            task.status,
            cog_core::TaskStatus::Scheduled,
            "没排上期不妨碍它被派出去"
        );
    }

    /// 已经有基线的任务不重排。那份期可能是分解算出来的、也可能是人排的，
    /// 拿同类统计量覆盖它，等于用中位数改掉一份意图。
    #[tokio::test]
    async fn an_already_planned_task_is_not_rescheduled() {
        let book = Arc::new(PlanBook {
            records: vec![record("dag_node", 7, 1_000, 600)],
            ..PlanBook::with_plans(vec![plan("task-planned", 250)])
        });
        let runtime = estimate_runtime(book.clone());
        runtime
            .orchestrator()
            .submit_goal(
                "goal-estimate",
                vec![ready_task("task-planned", serde_json::json!({}))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        assert!(
            book.saved().is_empty(),
            "已排期的任务被估算覆盖了：{:#?}",
            book.saved()
        );
    }

    /// 两条轴各自缺各自的：花销量不出来不等于工时也量不出来。
    ///
    /// 花销中位数为 0 时写 `budget_tokens = 0`，读者那边读作「没有成本计划」——
    /// 预算闸只认大于零的行，所以这行不拦任何东西；但止期是真量过的，甘特有
    /// 那一条。反过来工时中位数为 0 时止期留空、预算照写。两条都量不出来才
    /// 什么都不写：那才是「这一类还没有样本」。
    #[tokio::test]
    async fn the_two_axes_of_a_baseline_are_measured_and_missing_independently() {
        let book = Arc::new(PlanBook {
            records: vec![
                record("dag_node", 7, 0, 600),
                record("reviewer", 7, 1_000, 0),
                record("neither", 7, 0, 0),
            ],
            ..PlanBook::with_plans(Vec::new())
        });
        let runtime = estimate_runtime(book.clone());
        let mut nocost = ready_task("task-nocost", serde_json::json!({}));
        nocost.task_type = cog_core::TaskType::DagNode;
        let mut nodur = ready_task("task-nodur", serde_json::json!({}));
        nodur.task_type = cog_core::TaskType::Reviewer;
        let mut neither = ready_task("task-neither", serde_json::json!({}));
        neither.task_type = cog_core::TaskType::Custom("neither".into());
        runtime
            .orchestrator()
            .submit_goal("goal-estimate", vec![nocost, nodur, neither])
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let saved = book.saved();
        let by_id = |id: &str| saved.iter().find(|plan| plan.task_id == id).cloned();
        let nocost = by_id("task-nocost").expect("工时量得出来就该排期");
        assert_eq!(nocost.budget_tokens, 0, "花销没量出来就写 0，不编一个数");
        assert!(nocost.planned_end.is_some(), "工时量过，止期要写");
        let nodur = by_id("task-nodur").expect("花销量得出来就该排期");
        assert_eq!(nodur.budget_tokens, 1_000);
        assert!(
            nodur.planned_end.is_none(),
            "工时没量出来就止期留空，一排零会读成「计划已完成」"
        );
        assert!(
            by_id("task-neither").is_none(),
            "两条轴都没有样本，写出来只是一行什么都量不出来的基线"
        );
    }

    #[tokio::test]
    async fn a_task_that_never_ran_is_not_judged_by_a_census_it_cannot_have() {
        let runtime = budget_runtime(
            Census::with(&[("task-first", 999_999)]),
            vec![plan("task-first", 100)],
        );
        runtime
            .orchestrator()
            .submit_goal(
                "goal-budget",
                vec![ready_task("task-first", serde_json::json!({}))],
            )
            .await
            .unwrap();
        runtime.publish_ready_tasks().await.unwrap();

        let task = runtime
            .orchestrator()
            .get_task("task-first")
            .await
            .expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Scheduled);
    }
}

#[cfg(test)]
mod orphan_reconciler_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{MessageStream, TaskStatus, TaskType};
    use std::sync::Mutex;

    type Calls = Vec<(bool, String, String)>;

    #[derive(Default)]
    struct RecordingAlertSink {
        calls: Mutex<Calls>,
        drafts: Mutex<Vec<cog_core::PersistentAlertDraft>>,
        active: Mutex<Vec<cog_core::PersistedAlert>>,
    }

    #[async_trait]
    impl cog_core::PersistentAlertSink for RecordingAlertSink {
        async fn set_persistent_alert(
            &self,
            condition: bool,
            draft: &cog_core::PersistentAlertDraft,
        ) -> Result<cog_core::AlertTransition, String> {
            self.calls.lock().unwrap().push((
                condition,
                draft.rule.clone(),
                draft.dedup_key.clone(),
            ));
            self.drafts.lock().unwrap().push(draft.clone());
            Ok(if condition {
                cog_core::AlertTransition::Fired
            } else {
                cog_core::AlertTransition::Resolved
            })
        }

        async fn firing_key_for_rule(&self, _rule: &str) -> Result<Option<String>, String> {
            Ok(None)
        }

        async fn list_active_persistent_alerts(
            &self,
            rule_prefix: &str,
            _limit: i64,
        ) -> Vec<cog_core::PersistedAlert> {
            self.active
                .lock()
                .unwrap()
                .iter()
                .filter(|a| a.rule.starts_with(rule_prefix))
                .cloned()
                .collect()
        }
    }

    /// MessageBackend never touched by the reconciler; satisfies the runtime
    /// constructor only.
    struct NullBackend;

    #[async_trait]
    impl MessageBackend for NullBackend {
        async fn payload_limit(&self, _subject: &str) -> SFResult<Option<usize>> {
            Ok(None)
        }

        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn stop_stream(&self, _subject: &str, _group: &str) {
            // 这个夹具没有长轮询：它的流要么立刻结束，要么永久 pending，
            // 没有一条已发出的读需要等它返回，所以没有可停的东西。
        }

        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
            Ok(())
        }
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Option<Vec<(String, Vec<u8>)>>> {
            Ok(None)
        }
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
        }

        /// A backend that carries nothing carries no backlog either; saying so
        /// is the whole answer.
        async fn stream_lag(&self, _stream: &str, _group: &str) -> SFResult<Option<u64>> {
            Ok(None)
        }
    }

    fn placeholder(id: &str, age_minutes: i64) -> Task {
        let mut t = Task::new(id, TaskType::Generator, serde_json::json!({}));
        t.is_executable = false;
        t.updated_at = chrono::Utc::now() - chrono::Duration::minutes(age_minutes);
        t
    }

    fn test_runtime(dag: Arc<DagExecutor>) -> DagExecutorRuntime {
        DagExecutorRuntime::new_with_backend(
            DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-orphan".into(),
                consumer_group: "grp-orphan".into(),
                max_retries: 1,
                ..DagExecutorConfig::default()
            },
            NullBackend,
        )
        .with_orchestrator(dag)
    }

    #[tokio::test]
    async fn tick_fires_terminates_then_resolves_orphan() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-tick".into()));
        dag.add_task(placeholder("orphan-1", 60)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let mut firing = std::collections::HashMap::new();

        // Zero dwell in this unit test: fire and terminate together on the
        // first pass; resolution eligibility is tested separately.
        let dwell = chrono::Duration::zero();

        // First pass: fire the alert and terminate the placeholder together.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;

        let view = dag.get_task("orphan-1").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        let key = "decomposition_orphaned:-:orphan-1";
        assert_eq!(firing.len(), 1);
        assert!(firing.contains_key(key));
        assert_eq!(
            *recording.calls.lock().unwrap(),
            vec![(true, "decomposition_orphaned".into(), key.into())]
        );

        // Second pass: parent is terminal and dwell elapsed → resolve.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert_eq!(
            recording.calls.lock().unwrap()[1],
            (false, "decomposition_orphaned".into(), key.into())
        );
    }

    #[tokio::test]
    async fn an_orphan_alert_carries_a_bounded_excerpt_not_the_whole_input() {
        // Self-discovery turns a firing alert's labels into a new goal, which
        // becomes the next task's input, which the next alert embeds again.
        // A label that carries the whole input therefore grows one generation
        // deeper for the same non-progress.
        let dag = Arc::new(DagExecutor::new("ws-orphan-labels".into()));
        let mut orphan = Task::new(
            "orphan-big",
            TaskType::Generator,
            serde_json::json!({ "goal": "刷新接口未区分访问令牌".repeat(5000) }),
        );
        orphan.is_executable = false;
        orphan.updated_at = chrono::Utc::now() - chrono::Duration::minutes(60);
        dag.add_task(orphan).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let mut firing = std::collections::HashMap::new();
        orphan_reconcile_tick(
            &dag,
            Some(&sink),
            &chrono::Duration::minutes(30),
            &chrono::Duration::zero(),
            &mut firing,
        )
        .await;

        let drafts = recording.drafts.lock().unwrap();
        let draft = drafts
            .first()
            .expect("the orphan must have raised an alert");
        let carried = draft.labels["task_goal"]
            .as_str()
            .expect("the excerpt is a string");
        assert!(
            carried.chars().count() <= cog_core::ALERT_LABEL_VALUE_MAX_CHARS + 1,
            "the alert label is what the next task input embeds: {} chars",
            carried.chars().count()
        );
        assert!(carried.ends_with('…'));
        assert!(
            draft.labels.get(crate::LABEL_ORIGINAL_INPUT).is_none(),
            "the whole input must not survive as a label"
        );
    }

    #[tokio::test]
    async fn tick_keeps_alert_firing_throughout_dwell_window() {
        // Regression for the multi-pod race: an adopted/fired alert whose
        // parent is already terminal must stay firing until resolve_after so the
        // independent alert consumer gets at least one poll on a firing row.
        let dag = Arc::new(DagExecutor::new("ws-orphan-dwell".into()));
        dag.add_task(placeholder("orphan-2", 60)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let dwell = chrono::Duration::minutes(10);
        let mut firing = std::collections::HashMap::new();

        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert_eq!(
            dag.get_task("orphan-2").await.unwrap().status,
            TaskStatus::Failed
        );

        // Second pass inside the dwell: still firing, no resolve call.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        let key = "decomposition_orphaned:-:orphan-2";
        assert_eq!(firing.len(), 1);
        assert!(firing.contains_key(key));
        assert_eq!(recording.calls.lock().unwrap().len(), 1);

        // Simulate the dwell elapsing: watch becomes eligible and resolves.
        firing.get_mut(key).unwrap().resolve_after =
            chrono::Utc::now() - chrono::Duration::seconds(1);
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert_eq!(recording.calls.lock().unwrap().len(), 2);
        assert!(!recording.calls.lock().unwrap()[1].0);
    }

    #[tokio::test]
    async fn tick_leaves_fresh_placeholder_alone() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-fresh".into()));
        dag.add_task(placeholder("fresh-1", 1)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let mut firing = std::collections::HashMap::new();

        let dwell = chrono::Duration::zero();
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("fresh-1").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn adopted_alert_resolves_when_parent_terminal_and_dwell_elapsed() {
        // Restart adoption: the alert row predates this process by more than
        // one dwell window and the parent has since Failed; the first tick
        // adopts and resolves it.
        let dag = Arc::new(DagExecutor::new("ws-orphan-adopt".into()));
        let mut adopted_parent = placeholder("orphan-old", 120);
        adopted_parent.status = TaskStatus::Failed;
        dag.add_task(adopted_parent).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let key = "decomposition_orphaned:goal-x:orphan-old".to_string();
        recording
            .active
            .lock()
            .unwrap()
            .push(cog_core::PersistedAlert {
                rule: cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED.into(),
                dedup_key: key.clone(),
                severity: "warning".into(),
                state: "firing".into(),
                message: String::new(),
                labels: serde_json::json!({"parent_task_id": "orphan-old"}),
                fired_at: chrono::Utc::now() - chrono::Duration::minutes(10),
                last_seen_at: Some(chrono::Utc::now()),
                acknowledged_at: None,
                acknowledged_by: None,
            });
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let runtime = test_runtime(dag);
        let handle = tokio::spawn(async move {
            runtime
                .run_orphan_reconciler(true, 30, 60, 30, Some(sink), shutdown_clone)
                .await;
        });

        for _ in 0..50 {
            if recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        shutdown.trigger();
        let _ = handle.await;

        assert!(
            recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key),
            "adopted alert not resolved: {:?}",
            recording.calls.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn recently_fired_adopted_alert_keeps_firing_for_one_dwell() {
        // Multi-pod race guard: an alert adopted only seconds after it fired
        // (e.g. a second replica starting) must not resolve on the adopting
        // tick even though the parent is already terminal.
        let dag = Arc::new(DagExecutor::new("ws-orphan-adopt-fresh".into()));
        let mut adopted_parent = placeholder("orphan-new", 120);
        adopted_parent.status = TaskStatus::Failed;
        dag.add_task(adopted_parent).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let key = "decomposition_orphaned:goal-y:orphan-new".to_string();
        recording
            .active
            .lock()
            .unwrap()
            .push(cog_core::PersistedAlert {
                rule: cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED.into(),
                dedup_key: key.clone(),
                severity: "warning".into(),
                state: "firing".into(),
                message: String::new(),
                labels: serde_json::json!({"parent_task_id": "orphan-new"}),
                fired_at: chrono::Utc::now(),
                last_seen_at: Some(chrono::Utc::now()),
                acknowledged_at: None,
                acknowledged_by: None,
            });
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        // Interval 60s: the second tick lands at 60s, beyond this test's wait.
        let runtime = test_runtime(dag);
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            runtime
                .run_orphan_reconciler(true, 60, 60, 60, Some(sink), shutdown_clone)
                .await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        shutdown.trigger();
        let _ = handle.await;

        assert!(
            !recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key),
            "freshly fired adopted alert resolved too early: {:?}",
            recording.calls.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn disabled_reconciler_returns_immediately() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-off".into()));
        dag.add_task(placeholder("orphan-x", 60)).await.unwrap();
        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        let runtime = test_runtime(dag.clone());
        runtime
            .run_orphan_reconciler(false, 30, 60, 60, Some(sink), ShutdownSignal::new())
            .await;

        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("orphan-x").await.unwrap().status,
            TaskStatus::Pending
        );
    }
}
