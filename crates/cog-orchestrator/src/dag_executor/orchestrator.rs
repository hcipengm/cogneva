//! DAG 执行器编排核心：任务状态、租约、续跑与自修复都在这里汇合。
use cog_core::{SFError, SFResult, Task, TaskStatus, TaskType, UpstreamFailure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::circuit_registry::CircuitBreakerRegistry;
use super::retry_matrix::RetryMatrix;
use super::task_checkpoint::{
    self, checkpoint_task_with_timeout, CheckpointAgents, CheckpointRound, CHECKPOINT_STEP_TIMEOUT,
};
use super::task_phase::PhasedTask;
use cog_core::contract::outcome::GuardrailRefusal;
use cog_core::{DeadLetterEntry, DeadLetterQueue, RetryAttempt, SuggestedAction};

/// Loop name reported through the background-loop liveness family.
pub const ARCHIVE_LOOP: &str = "orchestrator_task_archive";

/// Serializable snapshot of the DAG executor state for persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DagStateSnapshot {
    tasks: HashMap<String, Task>,
    dependencies: HashMap<String, HashSet<String>>,
    dependents: HashMap<String, HashSet<String>>,
    retry_history: HashMap<String, Vec<RetryAttempt>>,
    phased_tasks: HashMap<String, PhasedTask>,
}

/// Mutable state protected by a RwLock so that [`DagExecutor`] methods
/// can all take `&self` and be called concurrently.
struct Inner {
    tasks: HashMap<String, Task>,
    dependencies: HashMap<String, HashSet<String>>,
    dependents: HashMap<String, HashSet<String>>,
    retry_history: HashMap<String, Vec<RetryAttempt>>,
    phased_tasks: HashMap<String, PhasedTask>,
    pending_changes: u32,
    last_persist: Option<std::time::Instant>,
}

/// One snapshot of the graph, answering every question admission asks.
///
/// `columns` is per project and counts only tasks that have not finished: a
/// column's occupancy is the work standing in it now, and a completed row is
/// history, not a resident. `tasks` that belong to no goal land under
/// [`cog_core::UNGROUPED_PROJECT`], the same bucket the board reading names.
#[derive(Debug, Clone, Default)]
pub struct ReadyWork {
    pub ready: Vec<Task>,
    /// How many tasks each ready queue already owes, by ready stream.
    pub in_flight: HashMap<String, usize>,
    /// Per project, how many unfinished tasks stand in each board column.
    pub columns: HashMap<String, std::collections::BTreeMap<&'static str, u64>>,
}

pub struct DagExecutor {
    workspace_id: String,
    /// 本进程的运行身份，任务租约的持有者。每次构造新产生一个值，所以它随进程
    /// 消亡而失效——这正是「上一任持有者已经不在了」可以被读出来的依据。
    run_id: String,
    /// 任务租约时长；持有者按它的三分之一续期。
    task_lease: chrono::Duration,
    inner: RwLock<Inner>,
    retry_matrix: RetryMatrix,
    dlq: Option<Arc<dyn DeadLetterQueue>>,
    circuit_registry: Option<Arc<CircuitBreakerRegistry>>,
    event_tx: Option<broadcast::Sender<cog_core::TaskEvent>>,
    raw_logger: Option<Arc<dyn cog_core::RawLogger>>,
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
    batch_persistence_enabled: bool,
    batch_persistence_max_changes: u32,
    batch_persistence_interval_secs: u64,
    archive_enabled: bool,
    archive_after_secs: u64,
    archive_poll_interval_secs: u64,
    /// Where the DAG's own repairs report themselves. Attached after
    /// construction because the storage layer that carries it is initialised
    /// after this crate; absent, a repair is only a log line, which is the one
    /// place a recurring fault can go on happening unread.
    metrics: std::sync::RwLock<Option<Arc<dyn cog_core::MetricsBackend>>>,
    /// Where the agents a task created are let go once it reaches a terminal
    /// state.
    ///
    /// Attached after construction, like the metrics backend: the pool is built
    /// after this executor, so a constructor argument would mean building them
    /// in the other order for the sake of one field. Absent, a terminal task
    /// retires nothing and the roster keeps growing — which is the state this
    /// field exists to end, so it is wired in the same place the pool is.
    agent_manager: std::sync::RwLock<Option<Arc<dyn cog_core::AgentManager>>>,
    /// 看板每一列的在制品上限。
    ///
    /// 发布侧（`DagExecutorRuntime::publish_ready_tasks`）拿它拦「进 `scheduled`」，
    /// 这里拿它拦「进 `running`」。两处判的是同一列上的同一件事，所以读的是同一个
    /// 环境变量、同一个解析器（[`cog_core::BoardWipLimits::from_env`]）——两份解析
    /// 就是两把尺，看板上印的上限迟早不是拦住派发的那个数。
    ///
    /// 构造时就从环境读，而不是等谁记得来注入：一个要靠调用方接上才生效的上限，
    /// 接漏的那天与「这台不限流」在读数上同形，而那正是这个字段要结束的状态。
    ///
    /// 一格而不是一个值，是为了让运行时能把它自己那一份推进来：发布侧拦「进
    /// `scheduled`」，这里拦「进 `running`」，两处必须是**同一个对象**，否则看板
    /// 上印的上限与真正拦住的那个数会在有人只改了一边的那天分家。
    board_wip_limits: std::sync::RwLock<cog_core::BoardWipLimits>,
}

/// 为什么一个 Running 任务需要被回收。
///
/// 两个原因对下游是相反的判定：一个说「换个进程重来」，另一个说「重来也白来」。
/// 它们所以是两种类型而不是同一句话里的两种措辞，是因为读到它们的是重试判定，
/// 而那个判定只认类型与带内标记，不认散文。
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReclaimCause {
    /// 持有者的心跳停了：原进程被换掉了，这一轮没有产出。接手它是对的——环境
    /// 确实换了人，同一份输入在下一个进程里未必重蹈覆辙。所以这个原因**不**
    /// 声明自己是终止性的。
    LeaseExpired {
        owner: Option<String>,
        expired_at: chrono::DateTime<chrono::Utc>,
    },
    /// 持有者还在，但这一轮把预算跑满了还没产出：花费涨了、进展为零。同一份
    /// 输入重跑买不到不同的结果，所以这个原因声明自己是终止性的，省下的是
    /// 下一整轮预算。
    OverBudget { timeout_seconds: u64 },
}

impl ReclaimCause {
    /// 失败原因文本。带内标记由 `cog_core::contract::outcome` 定义，判定读的是
    /// 那个常量而不是这里的字面量——两边各写一份，改一处就会静默失配。
    fn error(&self) -> String {
        match self {
            Self::LeaseExpired { owner, .. } => format!(
                "run lease expired: the process that held this task ({}) stopped renewing it, so the run never produced a result",
                owner.as_deref().unwrap_or("unknown")
            ),
            Self::OverBudget { timeout_seconds } => format!(
                "{}: task ran its whole {}s budget without producing a result",
                cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX,
                timeout_seconds
            ),
        }
    }

    /// 这次回收要不要记在任务自己的重试账上。
    ///
    /// 判据是「这笔账该不该由任务来付」，不是「这次回收严重不严重」。租约过期读
    /// 到的事实是**持有者不在了**——进程被换掉、被杀掉，都不是任务做的，B11 那
    /// 条链的设计里就写死了「转移不计重试」。而这条路上原本唯一的判决是拿回收
    /// 原因去走通用的失败处理，那个处理只认预算：`retry_count < max_retries`，
    /// 于是每滚一次版就替任务扣一格，三次部署就能把一条刚跑起来、没出过任何错的
    /// 回炉判死。类型本来就分好了，这里只是把它读到判定里，不新加读数。
    ///
    /// 预算跑满那档相反：它读到的是任务自己烧掉了一整轮预算、进展为零，那笔账
    /// 由任务买单，仍然按终止处理。
    ///
    /// 不记账的回投不是无界的：每回收一次都要有一个真实的持有者先消失，而它要
    /// 再被回收，得先有新持有者接手、再消失——这一圈里没有任务自己的动作在驱动，
    /// 也没有时钟在驱动，是环境的节拍在驱动。
    fn charges_retry_budget(&self) -> bool {
        match self {
            Self::LeaseExpired { .. } => false,
            Self::OverBudget { .. } => true,
        }
    }
}

/// 回收判据。只有 Running 的任务需要被回收，且必须有一条证据：持有者的租约过期
/// （原进程不在了），或这一轮用光了预算。两条证据都没有就不动它——「没看到续期」
/// 与「看到过期」是两回事，前者在租约还新的时候就是常态。
fn reclaim_cause(
    task: &Task,
    lease: chrono::Duration,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<ReclaimCause> {
    if task.status != TaskStatus::Running {
        return None;
    }
    if let Some(expired_at) = task.lease_expiry(lease) {
        if expired_at < now {
            return Some(ReclaimCause::LeaseExpired {
                owner: task.lease_owner.clone(),
                expired_at,
            });
        }
    }
    let over_budget = task
        .started_at
        .map(|s| (now - s).num_seconds() > task.timeout_seconds as i64)
        .unwrap_or(false);
    if over_budget {
        return Some(ReclaimCause::OverBudget {
            timeout_seconds: task.timeout_seconds,
        });
    }
    None
}

impl DagExecutor {
    pub fn new(workspace_id: String) -> Self {
        Self {
            workspace_id,
            run_id: Uuid::new_v4().to_string(),
            task_lease: chrono::Duration::seconds(cog_core::config::DEFAULT_TASK_LEASE_SECS as i64),
            inner: RwLock::new(Inner {
                tasks: HashMap::new(),
                dependencies: HashMap::new(),
                dependents: HashMap::new(),
                retry_history: HashMap::new(),
                phased_tasks: HashMap::new(),
                pending_changes: 0,
                last_persist: None,
            }),
            retry_matrix: RetryMatrix::defaults(),
            dlq: None,
            circuit_registry: None,
            event_tx: None,
            raw_logger: None,
            state_backend: None,
            batch_persistence_enabled: false,
            batch_persistence_max_changes: 10,
            batch_persistence_interval_secs: 5,
            archive_enabled: false,
            archive_after_secs: 3600,
            archive_poll_interval_secs: 300,
            metrics: std::sync::RwLock::new(None),
            agent_manager: std::sync::RwLock::new(None),
            board_wip_limits: std::sync::RwLock::new(cog_core::BoardWipLimits::from_env().0),
        }
    }

    /// 换一套列上限。生产路径只从环境读（[`Self::new`]），这个入口是运行时把
    /// 自己那一份推进来的地方——它同时喂着发布侧的拦截，两边必须是同一个数。
    pub fn set_board_wip_limits(&self, limits: cog_core::BoardWipLimits) {
        *self.board_wip_limits.write().unwrap() = limits;
    }

    /// Attach the pool whose agents this executor retires at a task's terminal
    /// state. Called once at start-up; a second call replaces the first.
    pub fn attach_agent_manager(&self, manager: Arc<dyn cog_core::AgentManager>) {
        *self
            .agent_manager
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(manager);
    }

    /// Attach the metrics backend the DAG's own repairs report to. Called once
    /// at start-up; a second call replaces the first, so a plugin that
    /// re-runs start-up does not leave two surfaces behind.
    pub fn attach_metrics(&self, metrics: Arc<dyn cog_core::MetricsBackend>) {
        *self.metrics.write().unwrap_or_else(|e| e.into_inner()) = Some(metrics);
    }

    /// The metrics backend this executor reports through, if one was attached.
    ///
    /// One way to reach the handle: a reader that takes the lock itself gets a
    /// snapshot of a different moment than its neighbours, and the three
    /// readings below all have to be answers about the same attachment.
    pub(crate) fn metrics_backend(&self) -> Option<Arc<dyn cog_core::MetricsBackend>> {
        self.metrics
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Record that `count` tasks stalled in `Scheduled` were re-armed.
    ///
    /// This is the repair's own reading, and it has to be a counter rather than
    /// a live gauge: the repair empties the state it repairs, so a gauge would
    /// read zero both when nothing was ever stuck and when everything was just
    /// unstuck. What survives in the total is how often tasks reach the end of
    /// the stall window without ever starting — the symptom, not the cause,
    /// because being put back in line is what the repair does whether the
    /// message was queued or lost.
    ///
    /// So this total cannot say whether the repair worked: being re-armed is
    /// what the repair does either way. The reading that does say so is the
    /// charge's absence — a re-armed task keeps the `retry_count` it had.
    async fn record_stalled_scheduled_reclaimed(
        &self,
        reclaimed: usize,
        reclaimed_with_backlog: usize,
        deferred: usize,
    ) {
        if reclaimed == 0 && reclaimed_with_backlog == 0 && deferred == 0 {
            return;
        }
        let Some(backend) = self.metrics_backend() else {
            tracing::debug!(
                reclaimed,
                reclaimed_with_backlog,
                deferred,
                "reclaimed stalled scheduled tasks; no metrics backend attached"
            );
            return;
        };
        // Three cells, recorded together: `reclaimed` counts the repair's
        // ordinary work, `with_backlog` the rows it took back although their
        // queue still held undelivered entries (the wait ran out of credit),
        // and `deferred` the rows it left alone because that queue still held
        // them. Separate totals, because one total cannot tell a drain from a
        // loss -- and the loss is the case this sweep exists for.
        for (name, count) in [
            (
                cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED,
                reclaimed,
            ),
            (
                cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG,
                reclaimed_with_backlog,
            ),
            (
                cog_core::metric_names::DAG_STALLED_SCHEDULED_DEFERRED_FOR_BACKLOG,
                deferred,
            ),
        ] {
            if count == 0 {
                continue;
            }
            if let Err(e) = backend
                .record_counter(name, count as f64, HashMap::new())
                .await
            {
                tracing::warn!(
                    error = %e,
                    series = name.as_str(),
                    "cannot record the stalled-scheduled reclaim count"
                );
            }
        }
    }

    /// Put every task stalled in `Scheduled` back in line, charging nothing.
    ///
    /// A task found here has never been attempted. `Scheduled` says its ready
    /// message was published, and the two ways it can stay there — the message
    /// is queued behind a full claim pool, or the message died before anyone
    /// ran it — are indistinguishable from a task row. Age is the only reading
    /// this sweep has, and it is the same for a task that was never given a
    /// turn and for one whose message was dropped. Charging on that age bills
    /// the task for a shortage of capacity.
    ///
    /// So it re-arms: back to `Pending`, budget untouched, reason recorded, no
    /// verdict. Undecidable means uncharged, and the task goes back through the
    /// ordinary publisher — one fresh message per stall window at most, and an
    /// earlier message still in flight arrives to find the task already moved
    /// on, which is the stale-message path the consumer already owns.
    ///
    /// Who may charge: whoever sees the acknowledgement, and only for a message
    /// it acked without ever starting the task. That reader lives in
    /// [`crate::task_executor_router`], where the acks happen.
    ///
    /// `served_streams` is what this deployment consumes
    /// ([`crate::ready_queue::consumed_ready_streams`]). The repair is only
    /// meaningful on a stream with a reader: on a queue nobody reads, every row
    /// sits in `Scheduled` by construction — that is the intended state while an
    /// executor is away, and it is the queue that holds the work, not a lost
    /// message. Sweeping it would re-arm and republish the whole backlog every
    /// stall window and grow the queue instead of draining it (measured
    /// 2026-10-18: 1,499 tasks re-armed every 600 s against a process-local
    /// stream whose consumer was switched off, 8,815 messages queued behind
    /// it). Those rows are left to the reader that will arrive.
    ///
    /// `undelivered` is, per stream, how many entries the transport says the
    /// consumer group has not been handed yet ([`cog_core::MessageBackend::stream_lag`]).
    /// A transport that cannot answer leaves its stream out of the map, and a
    /// stream left out is swept exactly as before — an unreadable queue is not
    /// a reading of an empty one.
    ///
    /// A non-zero backlog changes what the age means. The row is old, but its
    /// message may be sitting in a queue that is simply longer than the claim
    /// pool, and re-arming then adds a second copy to the very queue that is
    /// behind. So the sweep waits: while the backlog is non-zero the row is
    /// left alone, but only up to `max_backlog_windows` stall windows. Past
    /// that the backlog is no longer an explanation — a queue that has not
    /// moved in that many windows is not "about to get to it", and the row was
    /// re-armed by a sweep that would otherwise wait forever for a queue that
    /// never drains. Those rows are counted apart
    /// ([`cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG`])
    /// so a backlog that never clears is visible instead of being either
    /// silently waited on or silently folded into the ordinary total.
    ///
    /// The cap is read off the row's own age rather than stored per task: the
    /// row already carries `updated_at`, and a second field recording "how many
    /// times this was deferred" would be state the sweep would have to keep
    /// consistent against the very sweeps it is racing.
    ///
    /// Returns how many tasks it re-armed.
    /// `pool_declares_unavailable` is the pool's own verdict, read in the same
    /// pass by the caller that already reads it for admission. While it holds,
    /// a non-zero backlog is no longer something the cap can outlast: the queue
    /// is not slow, the thing that would drain it has declared itself off. A
    /// row re-armed then is a second copy of a message that is still in line
    /// and that will be delivered the moment the pool returns — the one outcome
    /// the whole backlog branch exists to avoid. Admission already publishes
    /// zero for that state, so the re-armed row would not run either; the
    /// reclaim buys nothing and costs a duplicate.
    pub async fn reclaim_stalled_scheduled(
        &self,
        stall_after_secs: u64,
        served_streams: &[String],
        undelivered: &HashMap<String, Option<u64>>,
        max_backlog_windows: u64,
        pool_declares_unavailable: bool,
    ) -> usize {
        // Floored at the window the transport itself uses before it re-delivers
        // an unacknowledged message. Below it, a message that is merely slow is
        // still claimable by the transport, so acting here would run work that
        // is about to arrive on its own: a caller asking for a shorter window is
        // asking for the duplicate, and does not get one.
        let stall_after_secs =
            stall_after_secs.max(cog_core::config::DEFAULT_READY_CLAIM_IDLE_SECS);
        let stall_before = chrono::Utc::now() - chrono::Duration::seconds(stall_after_secs as i64);
        let stalled = self.find_stalled_scheduled_tasks(stall_before).await;
        let mut reclaimed = 0usize;
        let mut reclaimed_with_backlog = 0usize;
        let mut unserved = 0usize;
        let mut deferred = 0usize;
        let now = chrono::Utc::now();
        for task in stalled {
            let stream = crate::ready_queue::ready_stream_for(&task, &self.workspace_id);
            if !served_streams.iter().any(|served| served == &stream) {
                unserved += 1;
                continue;
            }
            // `None` (no reading, or a transport that cannot say) keeps the
            // ordinary path: silence is not evidence of a backlog.
            let lag = undelivered.get(&stream).copied().flatten();
            let has_backlog = lag.map(|lag| lag > 0).unwrap_or(false);
            if has_backlog {
                let age_secs = (now - task.updated_at).num_seconds().max(0);
                let cap_secs = (stall_after_secs as i64).saturating_mul(max_backlog_windows as i64);
                // A declared pool outage is the third explanation, and it is
                // the one the cap cannot outlast: a queue that has not moved
                // because nothing could run it is not a queue nobody reads.
                if pool_declares_unavailable || age_secs < cap_secs {
                    deferred += 1;
                    continue;
                }
            }
            let error = format!(
                "never started: the task sat in Scheduled for more than {stall_after_secs}s without ever being started, so it was put back in line without charging an attempt"
            );
            match self.reclaim_one_stalled(&task.id, error).await {
                Ok(true) => {
                    reclaimed += 1;
                    if has_backlog {
                        reclaimed_with_backlog += 1;
                    }
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    task_id = %task.id,
                    "cannot reclaim a task stalled in Scheduled: {e}"
                ),
            }
        }
        if unserved > 0 {
            // 静默跳过就是另一处「看起来像空闲」：这些行会一直停在 Scheduled，
            // 直到读这队列的部署回来。说一句谁在等，别让这段空白被读成清扫器坏了。
            tracing::info!(
                unserved,
                "tasks in Scheduled belong to a ready stream this deployment does not read; \
                 leaving them queued for the executor rather than republishing them"
            );
        }
        if deferred > 0 {
            // 推迟也要说一句：不计数的等待读起来和清扫器没在跑是同一件事。
            tracing::info!(
                deferred,
                "tasks stayed in Scheduled because their ready stream still reports undelivered entries"
            );
        }
        self.record_stalled_scheduled_reclaimed(reclaimed, reclaimed_with_backlog, deferred)
            .await;
        reclaimed
    }

    /// Re-arm one stalled task; `false` if it was no longer stalled.
    ///
    /// The sweep runs in every deployment that publishes (the ready queue is
    /// shared, so any of them may publish), which makes the read-then-write a
    /// race the scan itself cannot close: two processes can both find the same
    /// task stalled and both act on it. So the action rides on the state
    /// transition — the write carries `Scheduled` as its precondition and only
    /// the process that still finds it there acts. A task that started, or that
    /// another sweep already picked up, fails the precondition and is left
    /// alone.
    async fn reclaim_one_stalled(&self, task_id: &str, error: String) -> SFResult<bool> {
        let Some(task) = self.get_task(task_id).await else {
            return Ok(false);
        };
        if task.status != TaskStatus::Scheduled {
            return Ok(false);
        }
        let updated = Self::stalled_requeued(&task, &error);
        match self.fg() {
            Some(be) => {
                if let Err(e) = be
                    .dag_transition_task(
                        &self.workspace_id,
                        task_id,
                        &[TaskStatus::Scheduled],
                        &updated,
                    )
                    .await
                {
                    // Losing the conditional write is the ordinary outcome of
                    // two sweeps reaching one task, and it is not a failure:
                    // the task moved on. Only a write that did not land while
                    // the row still reads `Scheduled` means the store refused
                    // work it had just accepted.
                    if matches!(
                        self.get_task(task_id).await,
                        Some(cur) if cur.status == TaskStatus::Scheduled
                    ) {
                        return Err(e);
                    }
                    return Ok(false);
                }
            }
            None => {
                let mut inner = self.inner.write().await;
                let Some(current) = inner.tasks.get_mut(task_id) else {
                    return Ok(false);
                };
                if current.status != TaskStatus::Scheduled {
                    return Ok(false);
                }
                *current = updated.clone();
                drop(inner);
                self.persist_task_fine_grained(&updated).await;
            }
        }
        // `TaskRetried`, not `TaskFailed`: the task failed at nothing. The
        // `retry_count` it carries is deliberately the unchanged one, so a
        // reader watching these events sees the same number repeat — that is
        // the reading that no budget was spent, and it is the only place the
        // distinction is visible after the fact.
        self.emit_event(cog_core::TaskEvent::TaskRetried {
            task_id: task_id.into(),
            retry_count: updated.retry_count,
            timestamp: chrono::Utc::now(),
        })
        .await;
        Ok(true)
    }

    /// The row a stalled task becomes: back in `Pending`, budget untouched,
    /// free to be published again on the next pass.
    ///
    /// No `retry_not_before`: a wait here would be the retry matrix's wait for
    /// an attempt this task never made, and the re-arm is the repair, not the
    /// retry. What bounds the republishing is the stall window itself — the
    /// task leaves `Scheduled`, so the next sweep can only see it after another
    /// full window has gone by.
    fn stalled_requeued(task: &Task, error: &str) -> Task {
        let mut next = task.clone();
        next.status = TaskStatus::Pending;
        next.error = Some(error.to_string());
        next.error_cause = None;
        next.retry_not_before = None;
        next.updated_at = chrono::Utc::now();
        next
    }

    pub fn workspace_id(&self) -> &str {
        &self.workspace_id
    }

    /// 本进程的运行身份。它是租约的持有者标记，也是心跳续期的作用域：续期只
    /// 碰自己起的任务，所以一个不执行任何任务的进程跑这条循环是无操作。
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// 任务租约时长（秒）。
    pub fn task_lease_secs(&self) -> u64 {
        self.task_lease.num_seconds().max(1) as u64
    }

    /// 租约时长取配置面；0 视为未配置，退回默认值，免得一个笔误让所有任务
    /// 一到手就被判为过期。
    pub fn with_task_lease_secs(mut self, secs: u64) -> Self {
        if secs > 0 {
            self.task_lease = chrono::Duration::seconds(secs as i64);
        }
        self
    }

    pub fn with_state_backend(mut self, backend: Arc<dyn cog_core::StateBackend>) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// 这个执行器据以持久化的后端，没有配置时为 `None`。
    ///
    /// 给需要跟着任务图一起写同一后端的调用点用（计划基线写在任务行旁边，
    /// 两者必须落在同一个部署上，否则甘特面会把一个库的任务配上另一个库的
    /// 基线）。返回克隆的 `Arc` 而不是引用，调用方要在 `&self` 的读方法里跨
    /// `await` 持着它。
    pub fn state_backend(&self) -> Option<Arc<dyn cog_core::StateBackend>> {
        self.state_backend.clone()
    }

    pub fn with_batch_persistence(
        mut self,
        enabled: bool,
        max_changes: u32,
        interval_secs: u64,
    ) -> Self {
        self.batch_persistence_enabled = enabled;
        self.batch_persistence_max_changes = max_changes;
        self.batch_persistence_interval_secs = interval_secs;
        self
    }

    pub fn with_archive_config(
        mut self,
        enabled: bool,
        after_secs: u64,
        poll_interval_secs: u64,
    ) -> Self {
        self.archive_enabled = enabled;
        self.archive_after_secs = after_secs;
        self.archive_poll_interval_secs = poll_interval_secs;
        self
    }

    /// Best-effort fine-grained persistence of a single task.
    /// Errors are logged but never block the hot path.
    async fn persist_task_fine_grained(&self, task: &Task) {
        // 存储权威模式下所有写入已直接落库，跳过快照/细粒度双写
        if self.fg().is_some() {
            return;
        }
        if let Some(ref backend) = self.state_backend {
            let workspace_id = self.workspace_id.clone();
            let task_id = task.id.clone();
            let backend = backend.clone();
            let task = task.clone();
            tokio::spawn(async move {
                if let Err(e) = backend.dag_set_task(&workspace_id, &task_id, &task).await {
                    tracing::warn!("dag_set_task failed for {}: {}", task_id, e);
                }
            });
        }
    }

    async fn do_persist(&self) {
        if self.fg().is_some() {
            return;
        }
        if let Some(ref backend) = self.state_backend {
            let inner = self.inner.read().await;
            let snapshot = DagStateSnapshot {
                tasks: inner.tasks.clone(),
                dependencies: inner.dependencies.clone(),
                dependents: inner.dependents.clone(),
                retry_history: inner.retry_history.clone(),
                phased_tasks: inner.phased_tasks.clone(),
            };
            drop(inner);
            let workspace_id = self.workspace_id.clone();
            let backend = backend.clone();
            tokio::spawn(async move {
                match serde_json::to_value(&snapshot) {
                    Ok(value) => {
                        if let Err(e) = backend.save_dag_state(&workspace_id, &value).await {
                            tracing::warn!("DagExecutor persist_state failed: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("DagExecutor state serialization failed: {}", e);
                    }
                }
            });
        }
    }

    async fn persist_state(&self) {
        if !self.batch_persistence_enabled {
            self.do_persist().await;
            return;
        }
        let mut inner = self.inner.write().await;
        inner.pending_changes += 1;
        let now = std::time::Instant::now();
        let should_persist = inner.pending_changes >= self.batch_persistence_max_changes
            || inner
                .last_persist
                .map(|t| {
                    now.duration_since(t)
                        >= std::time::Duration::from_secs(self.batch_persistence_interval_secs)
                })
                .unwrap_or(true);
        if should_persist {
            drop(inner);
            self.do_persist().await;
            let mut inner = self.inner.write().await;
            inner.pending_changes = 0;
            inner.last_persist = Some(now);
        }
    }

    /// Force an immediate full-state checkpoint (same as persist_state but public).
    pub async fn force_checkpoint(&self) {
        self.do_persist().await;
        let mut inner = self.inner.write().await;
        inner.pending_changes = 0;
        inner.last_persist = Some(std::time::Instant::now());
    }

    /// 多副本读穿修复：任务不在本 pod 内存时，从共享存储整体恢复快照后重查。
    /// 修复多副本下结果消息被没有该任务内存态的 pod 消费时报
    /// "Task not found"（2026-08-06 遗留断点）。
    /// 注意此处故意不先 do_persist：本 pod 内存可能比共享快照旧，先刷会
    /// 用过期快照覆盖其他副本写入的新任务。残余限制：批持久化窗口内
    /// （默认 5s/10 变更）对端刚提交的任务仍可能查不到；彻底解法是让共享
    /// 存储成为权威状态（trait 细粒度 dag_* 面已具备，postgres 尚未实现）。
    async fn ensure_task_present(&self, task_id: &str) -> SFResult<()> {
        if self.inner.read().await.tasks.contains_key(task_id) {
            return Ok(());
        }
        if self.state_backend.is_none() {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            });
        }
        if let Err(e) = self.load_from_backend().await {
            tracing::warn!("ensure_task_present reload failed for {}: {}", task_id, e);
        }
        if self.inner.read().await.tasks.contains_key(task_id) {
            Ok(())
        } else {
            Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })
        }
    }

    /// Load state from the configured backend, if any.
    pub async fn load_from_backend(&self) -> SFResult<bool> {
        // 存储权威模式下内存无状态，快照加载无意义
        if self.fg().is_some() {
            return Ok(false);
        }
        if let Some(ref backend) = self.state_backend {
            match backend.load_dag_state(&self.workspace_id).await {
                Ok(Some(value)) => {
                    let snapshot: DagStateSnapshot =
                        serde_json::from_value(value).map_err(SFError::Serialization)?;
                    let mut inner = self.inner.write().await;
                    inner.tasks = snapshot.tasks;
                    inner.dependencies = snapshot.dependencies;
                    inner.dependents = snapshot.dependents;
                    inner.retry_history = snapshot.retry_history;
                    inner.phased_tasks = snapshot.phased_tasks;
                    tracing::info!(
                        "DagExecutor state restored for workspace {}",
                        self.workspace_id
                    );
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(e) => Err(e),
            }
        } else {
            Ok(false)
        }
    }

    /// Archive terminal-state tasks that have been inactive longer than
    /// `archive_after_secs`.  Tasks are persisted via the fine-grained
    /// backend before removal from memory.
    pub async fn archive_terminated_tasks(&self) {
        if !self.archive_enabled || self.state_backend.is_none() {
            return;
        }
        if self.fg().is_some() {
            return self.archive_terminated_tasks_store().await;
        }
        let threshold =
            chrono::Utc::now() - chrono::Duration::seconds(self.archive_after_secs as i64);
        let to_archive: Vec<String> = {
            let inner = self.inner.read().await;
            inner
                .tasks
                .values()
                .filter(|t| {
                    matches!(
                        t.status,
                        TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
                    ) && t.updated_at < threshold
                })
                .map(|t| t.id.clone())
                .collect()
        };
        if to_archive.is_empty() {
            return;
        }
        let mut archived = 0;
        for task_id in &to_archive {
            let task = {
                let inner = self.inner.read().await;
                inner.tasks.get(task_id).cloned()
            };
            if let Some(task) = task {
                if let Some(ref backend) = self.state_backend {
                    if let Err(e) = backend
                        .dag_set_task(&self.workspace_id, task_id, &task)
                        .await
                    {
                        tracing::warn!("archive: dag_set_task failed for {}: {}. Skipping removal from memory.", task_id, e);
                        continue;
                    }
                }
            }
            // Remove from memory structures.
            let mut inner = self.inner.write().await;
            if let Some(deps) = inner.dependencies.get(task_id) {
                for dep_id in deps.clone() {
                    if let Some(dependents) = inner.dependents.get_mut(&dep_id) {
                        dependents.remove(task_id);
                    }
                }
            }
            if let Some(dependents) = inner.dependents.get(task_id) {
                for dep_id in dependents.clone() {
                    if let Some(deps) = inner.dependencies.get_mut(&dep_id) {
                        deps.remove(task_id);
                    }
                }
            }
            inner.tasks.remove(task_id);
            inner.dependencies.remove(task_id);
            inner.dependents.remove(task_id);
            inner.retry_history.remove(task_id);
            inner.phased_tasks.remove(task_id);
            archived += 1;
        }
        if archived > 0 {
            tracing::info!(
                "Archived {} terminated task(s) from memory (workspace {})",
                archived,
                self.workspace_id
            );
        }
    }

    /// Spawn a background task that periodically archives old terminal tasks.
    pub fn start_archive_loop(self: &Arc<Self>) {
        let this = self.clone();
        let interval_secs = self.archive_poll_interval_secs;
        // Nothing hands this loop a stop signal: it is started for the life of
        // the process, so every way it can end — including a clean return —
        // leaves the archiving undone.
        drop(cog_core::loop_health::spawn_unstoppable(
            ARCHIVE_LOOP,
            cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(interval_secs)),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let this = this.clone();
                async move {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        beat.beat();
                        interval.tick().await;
                        this.archive_terminated_tasks().await;
                    }
                }
            },
        ));
        tracing::info!(
            "DagExecutor archive loop started (interval={}s)",
            interval_secs
        );
    }

    pub fn with_raw_logger(mut self, logger: Arc<dyn cog_core::RawLogger>) -> Self {
        self.raw_logger = Some(logger);
        self
    }

    pub fn with_event_tx(mut self, tx: broadcast::Sender<cog_core::TaskEvent>) -> Self {
        self.event_tx = Some(tx);
        self
    }

    pub fn subscribe_events(&self) -> Option<broadcast::Receiver<cog_core::TaskEvent>> {
        self.event_tx.as_ref().map(|tx| tx.subscribe())
    }

    /// The `kind` label value one event is counted under.
    ///
    /// One arm per variant, spelled out rather than caught by a wildcard arm, so
    /// that adding a variant to `TaskEvent` stops this from compiling instead of
    /// landing the new one under whatever the catch-all happened to say. The
    /// string is the `event` tag the same type serializes with, so a reader
    /// holding a raw log line finds the same word on the counter.
    /// Every label [`Self::event_kind`] can produce, in one place so the seed
    /// below and the match beside it cannot drift apart unnoticed.
    ///
    /// The store keys one series per kind, and the pairing rule reads this
    /// counter through `increase()`, which returns nothing over a window
    /// holding no samples — so a kind that has never been emitted is
    /// indistinguishable from one whose series was never wired.
    pub const TASK_EVENT_KINDS: [&str; 9] = [
        "task_created",
        "task_scheduled",
        "task_started",
        "task_completed",
        "task_failed",
        "task_cancelled",
        "task_retried",
        "task_timeout",
        "task_lease_expired",
    ];

    fn event_kind(event: &cog_core::TaskEvent) -> &'static str {
        use cog_core::TaskEvent as E;
        match event {
            E::TaskCreated { .. } => "task_created",
            E::TaskScheduled { .. } => "task_scheduled",
            E::TaskStarted { .. } => "task_started",
            E::TaskCompleted { .. } => "task_completed",
            E::TaskFailed { .. } => "task_failed",
            E::TaskCancelled { .. } => "task_cancelled",
            E::TaskRetried { .. } => "task_retried",
            E::TaskTimeout { .. } => "task_timeout",
            E::TaskLeaseExpired { .. } => "task_lease_expired",
        }
    }

    /// Record a zero for every kind this executor can offer, once at startup.
    ///
    /// The rule that pairs this counter with the consumer's reads both sides
    /// through `increase()`, and that returns nothing over a window holding no
    /// samples. A kind that has never been offered therefore leaves no series,
    /// which reads the same as a series that was never wired — the question a
    /// person asks when a reading looks flat, and one the traffic cannot
    /// answer. Seeded once, the series is present at 0 and that question is
    /// settled without waiting for a task event to arrive.
    ///
    /// The seed adds zero rather than assigning, so a process that restarts
    /// into a store that already holds counts does not erase them.
    pub async fn seed_event_bus_kinds(&self) {
        let Some(backend) = self.metrics_backend() else {
            return;
        };
        for kind in Self::TASK_EVENT_KINDS {
            let labels = HashMap::from([(crate::LABEL_KIND.to_string(), kind.to_string())]);
            if let Err(e) = backend
                .record_counter(cog_core::metric_names::EVENT_BUS_EMITTED_TOTAL, 0.0, labels)
                .await
            {
                tracing::warn!(error = %e, kind, "cannot seed the task event counter");
            }
        }
    }

    /// Publish a task event and count the offer.
    ///
    /// The count is the producing half of a pair the consumer-side counters
    /// cannot complete alone. `..._consumed_total` going flat means one of two
    /// things -- nothing was put on the channel, or the reader stopped taking --
    /// and the loop's own age gauge cannot separate them for an event-driven
    /// loop, whose age is the process's age. This is the series that says the
    /// channel was handed something, and the rule that fires on the pair is what
    /// makes a stalled consumer a reading instead of a quiet hour.
    ///
    /// Counted before the send, so the total is what the executor offered rather
    /// than what the channel delivered. `send` reports failure only when no
    /// receiver is subscribed, and an offer nobody is subscribed to is the case
    /// the consumer side can never show: counting only the delivered ones would
    /// hide exactly it.
    async fn emit_event(&self, event: cog_core::TaskEvent) {
        if let Some(ref tx) = self.event_tx {
            if let Some(backend) = self.metrics_backend() {
                let labels = HashMap::from([(
                    crate::LABEL_KIND.to_string(),
                    Self::event_kind(&event).to_string(),
                )]);
                if let Err(e) = backend
                    .record_counter(cog_core::metric_names::EVENT_BUS_EMITTED_TOTAL, 1.0, labels)
                    .await
                {
                    tracing::warn!(
                        error = %e,
                        kind = Self::event_kind(&event),
                        "cannot record the task events this executor offered"
                    );
                }
            }
            let _ = tx.send(event.clone());
        }
        if let Some(ref logger) = self.raw_logger {
            let record = cog_core::RawRecord {
                meta: cog_core::RawMeta {
                    version: "1.0".into(),
                    stream: "task_raw".into(),
                    recorded_at: chrono::Utc::now(),
                    recorded_by: "cog-orchestrator".into(),
                    sequence: 0,
                    trace_id: Uuid::new_v4().to_string(),
                    span_id: None,
                },
                context: cog_core::RawContext::default(),
                payload: cog_core::RawPayload {
                    direction: "internal".into(),
                    transport: "orchestrator".into(),
                    format: Some("json".into()),
                    raw: match serde_json::to_value(&event) {
                        Ok(v) => v,
                        Err(_) => serde_json::json!({"event": format!("{:?}", event)}),
                    },
                },
            };
            let logger = logger.clone();
            tokio::spawn(async move {
                if let Err(e) = logger.write(record).await {
                    tracing::warn!("RawLogger write failed (task_raw): {}", e);
                }
            });
        }
    }

    pub fn with_retry_matrix(mut self, matrix: RetryMatrix) -> Self {
        self.retry_matrix = matrix;
        self
    }

    pub fn with_dlq(mut self, dlq: Arc<dyn DeadLetterQueue>) -> Self {
        self.dlq = Some(dlq);
        self
    }

    pub fn with_circuit_registry(mut self, registry: Arc<CircuitBreakerRegistry>) -> Self {
        self.circuit_registry = Some(registry);
        self
    }

    pub fn retry_matrix(&self) -> &RetryMatrix {
        &self.retry_matrix
    }

    pub fn set_retry_matrix(&mut self, matrix: RetryMatrix) {
        self.retry_matrix = matrix;
    }

    pub fn dlq(&self) -> Option<&dyn DeadLetterQueue> {
        self.dlq.as_deref()
    }

    /// Submit a goal by dynamically injecting tasks into the existing DAG.
    /// Does **not** clear existing state — tasks are added incrementally.
    /// Returns `Ok(())` for backward compatibility; callers that need the
    /// list of added task IDs should use [`Self::add_tasks_batch`] directly.
    pub async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()> {
        let added = self.add_tasks_batch(tasks).await?;
        tracing::info!(%goal, added_tasks = %added.len(), "DagExecutor dynamically extended");
        Ok(())
    }

    /// Batch add multiple tasks to the existing DAG.
    /// Two-phase commit: validate all first, then commit, to avoid partial
    /// state on cycle detection.
    ///
    /// Inside a plan — a batch where something declares a predecessor — a
    /// held id is skipped only when the row underneath it is the same piece of
    /// work being redelivered; an id reused for different work is refused.
    /// Skipping it instead would queue the rest of the plan while its
    /// dependencies still name the held row, so `blocked_by: [t3]` would wait
    /// on another plan's `t3` — a predecessor that settles for reasons that
    /// have nothing to do with this plan, or never settles at all.
    ///
    /// Batches with no dependency edges keep the old skip: a face's task or a
    /// single intent resubmission reuses its stable id and carries no edges to
    /// attach to the held row, so there is nothing to corrupt, and refusing
    /// them would stop work that is meant to be skipped.
    pub async fn add_tasks_batch(&self, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        if self.fg().is_some() {
            return self.add_tasks_batch_store(tasks).await;
        }
        let mut inner = self.inner.write().await;
        // Phase 1: collect new tasks and their dependencies, skipping duplicates
        let mut validated: Vec<(String, HashSet<String>, Task)> = Vec::new();
        let mut reused: Vec<String> = Vec::new();
        let mut promotions: Vec<Task> = Vec::new();
        let plan_shaped = tasks.iter().any(|t| !t.blocked_by.is_empty());
        for task in tasks {
            let task_id = task.id.clone();
            if let Some(held) = inner.tasks.get(&task_id) {
                if plan_shaped && !may_be_skipped(held, &task) {
                    reused.push(reused_id_note(held, &task));
                    continue;
                }
                if !missing_enrichment(held, &task).is_empty() {
                    promotions.push(task);
                }
                continue; // Idempotent skip of a redelivery
            }
            let deps: HashSet<String> = task.blocked_by.iter().cloned().collect();
            validated.push((task_id, deps, task));
        }
        if !reused.is_empty() {
            return Err(reused_ids_refusal(&reused));
        }
        for incoming in promotions {
            if let Some(held) = inner.tasks.get_mut(&incoming.id) {
                carry_enrichment(held, &incoming);
            }
        }

        // Insert dependency edges into the combined graph (existing + new)
        for (task_id, deps, _) in &validated {
            inner.dependencies.insert(task_id.clone(), deps.clone());
            inner.dependents.insert(task_id.clone(), HashSet::new());
        }

        // Phase 2: validate no circular dependencies in the combined graph
        if let Some(cycle) = Self::detect_cycle(&inner) {
            // Rollback: remove only the newly added dependency entries
            for (task_id, _, _) in &validated {
                inner.dependencies.remove(task_id);
                inner.dependents.remove(task_id);
            }
            return Err(cog_core::SFError::Validation(format!(
                "Circular dependency detected: {}",
                cycle.join(" -> ")
            )));
        }

        // Phase 3: commit — insert tasks, build reverse links, emit events
        let mut added = Vec::new();
        for (task_id, _, task) in validated {
            inner.tasks.insert(task_id.clone(), task);
            let deps: Vec<String> = inner
                .dependencies
                .get(&task_id)
                .unwrap()
                .iter()
                .cloned()
                .collect();
            for dep_id in deps {
                if let Some(dependents) = inner.dependents.get_mut(&dep_id) {
                    dependents.insert(task_id.clone());
                }
            }
            inner.phased_tasks.insert(
                task_id.clone(),
                PhasedTask::new(super::task_phase::TaskPhase::Diagnose, 2),
            );
            drop(inner);
            self.emit_event(cog_core::TaskEvent::TaskCreated {
                task_id: task_id.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;
            // Fine-grained persist for each newly added task
            let task_snapshot = {
                let inner = self.inner.read().await;
                inner.tasks.get(&task_id).cloned()
            };
            if let Some(ref t) = task_snapshot {
                self.persist_task_fine_grained(t).await;
            }
            inner = self.inner.write().await;
            added.push(task_id);
        }

        drop(inner);
        self.persist_state().await;
        Ok(added)
    }

    /// Add a single task to the existing DAG without clearing existing tasks.
    /// Validates that the new task does not introduce a circular dependency.
    /// If a cycle is detected, the insertion is rolled back.
    /// If the task already exists, returns a Validation error (use
    /// [`Self::add_tasks_batch`] for idempotent batch insertion).
    /// 用改过的同一行替换图里已有的那一条，并把改动落到持久层。
    ///
    /// 只换行本身，不动依赖边：调用方改的是父节点、验收标准、归属这类字段，
    /// 而边由 `blocked_by` 单独维护——重挂父节点不该悄悄改掉「等谁跑完」。
    /// 行不存在时拒绝，不顺手插入一条新的：改一条查不到的行的请求，多半是
    /// id 写错了，把它变成新建会把一个笔误写成一次真实的写入。
    pub async fn replace_task(&self, task: Task) -> SFResult<()> {
        if self.fg().is_some() {
            let be = self.fg().expect("store mode");
            if be
                .dag_get_task(&self.workspace_id, &task.id)
                .await?
                .is_none()
            {
                return Err(cog_core::SFError::TaskFailed {
                    task_id: task.id.clone(),
                    reason: "Task not found".into(),
                });
            }
            be.dag_set_task(&self.workspace_id, &task.id, &task).await?;
            return Ok(());
        }
        self.ensure_task_present(&task.id).await?;
        {
            let mut inner = self.inner.write().await;
            let slot =
                inner
                    .tasks
                    .get_mut(&task.id)
                    .ok_or_else(|| cog_core::SFError::TaskFailed {
                        task_id: task.id.clone(),
                        reason: "Task not found".into(),
                    })?;
            *slot = task.clone();
        }
        self.persist_task_fine_grained(&task).await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn add_task(&self, task: Task) -> SFResult<()> {
        if self.fg().is_some() {
            return self.add_task_store(task).await;
        }
        let mut inner = self.inner.write().await;
        let task_id = task.id.clone();
        if inner.tasks.contains_key(&task_id) {
            return Err(cog_core::SFError::Validation(format!(
                "Task {} already exists",
                task_id
            )));
        }
        let deps: HashSet<String> = task.blocked_by.iter().cloned().collect();
        inner.dependencies.insert(task_id.clone(), deps);
        inner.dependents.insert(task_id.clone(), HashSet::new());
        inner.tasks.insert(task_id.clone(), task);

        // Build reverse dependency links
        let deps: Vec<String> = inner
            .dependencies
            .get(&task_id)
            .unwrap()
            .iter()
            .cloned()
            .collect();
        for dep_id in deps {
            if let Some(dependents) = inner.dependents.get_mut(&dep_id) {
                dependents.insert(task_id.clone());
            }
        }

        // Validate no circular dependencies
        if let Some(cycle) = Self::detect_cycle(&inner) {
            // Rollback on cycle
            inner.tasks.remove(&task_id);
            inner.dependencies.remove(&task_id);
            inner.dependents.remove(&task_id);
            for deps in inner.dependents.values_mut() {
                deps.remove(&task_id);
            }
            return Err(cog_core::SFError::Validation(format!(
                "Circular dependency detected: {}",
                cycle.join(" -> ")
            )));
        }

        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskCreated {
            task_id: task_id.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        let task_snapshot = {
            let inner = self.inner.read().await;
            inner.tasks.get(&task_id).cloned()
        };
        if let Some(ref t) = task_snapshot {
            self.persist_task_fine_grained(t).await;
        }
        self.persist_state().await;
        Ok(())
    }

    fn detect_cycle(inner: &Inner) -> Option<Vec<String>> {
        detect_cycle_graph(&inner.dependencies)
    }

    // ─── 存储权威模式（终态）─────────────────────────────────────
    // 后端 dag_supports_fine_grained()=true 时，任务/依赖/重试历史的权威
    // 副本在共享存储（postgres 事务保证跨 pod 原子），本结构只做编排
    // （事件、熔断、DLQ、observable），Inner 的 tasks/dependencies/
    // dependents 不再使用。无后端或后端只支持快照时走原有内存路径。

    /// 返回支持细粒度 DAG 的后端（存储权威模式开关）。
    fn fg(&self) -> Option<&Arc<dyn cog_core::StateBackend>> {
        self.state_backend
            .as_ref()
            .filter(|b| b.dag_supports_fine_grained())
    }

    /// 两种模式统一的任务集读取。
    async fn all_tasks_unified(&self) -> Vec<Task> {
        self.all_tasks_checked().await.unwrap_or_default()
    }

    /// 同上，但保留读失败。
    ///
    /// 内部的调用方把失败当成空图就够了——它们各自还有一层判据。读数不行：一次
    /// 读不出来的抓取若报成零行，读起来正好是「图里没有任务」，也就是最干净的那
    /// 个形状。调用方必须能分清这两种。
    pub async fn all_tasks_checked(&self) -> SFResult<Vec<Task>> {
        if let Some(be) = self.fg() {
            be.dag_get_all_tasks(&self.workspace_id).await
        } else {
            Ok(self.inner.read().await.tasks.values().cloned().collect())
        }
    }

    /// 存储模式单任务读。
    async fn store_task(&self, task_id: &str) -> SFResult<Task> {
        let be = self.fg().expect("store mode");
        be.dag_get_task(&self.workspace_id, task_id)
            .await?
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })
    }

    /// 存储模式单任务状态迁移：读出 → 校验/改写 → CAS 写回 → 发事件。
    async fn store_transition<F>(
        &self,
        task_id: &str,
        expected: &[TaskStatus],
        reject_reason: &str,
        mutate: F,
        event: Option<cog_core::TaskEvent>,
    ) -> SFResult<()>
    where
        F: FnOnce(&mut Task),
    {
        let be = self.fg().expect("store mode");
        let mut task = self.store_task(task_id).await?;
        if !expected.contains(&task.status) {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: reject_reason.replace("{}", &format!("{:?}", task.status)),
            });
        }
        mutate(&mut task);
        task.updated_at = chrono::Utc::now();
        be.dag_transition_task(&self.workspace_id, task_id, expected, &task)
            .await?;
        if let Some(ev) = event {
            self.emit_event(ev).await;
        }
        Ok(())
    }

    async fn add_tasks_batch_store(&self, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        let be = self.fg().expect("store mode");
        let existing = be.dag_get_all_tasks(&self.workspace_id).await?;
        let mut graph: HashMap<String, HashSet<String>> = existing
            .iter()
            .map(|t| (t.id.clone(), t.blocked_by.iter().cloned().collect()))
            .collect();
        let held_by_id: HashMap<&str, &Task> =
            existing.iter().map(|t| (t.id.as_str(), t)).collect();

        let mut validated = Vec::new();
        let mut reused: Vec<String> = Vec::new();
        let mut promotions: Vec<Task> = Vec::new();
        let plan_shaped = tasks.iter().any(|t| !t.blocked_by.is_empty());
        for task in tasks {
            if let Some(held) = held_by_id.get(task.id.as_str()) {
                if plan_shaped && !may_be_skipped(held, &task) {
                    reused.push(reused_id_note(held, &task));
                    continue;
                }
                if !missing_enrichment(held, &task).is_empty() {
                    promotions.push(task);
                }
                continue; // 幂等跳过：同一份工作的重投
            }
            graph.insert(task.id.clone(), task.blocked_by.iter().cloned().collect());
            validated.push(task);
        }
        if !reused.is_empty() {
            return Err(reused_ids_refusal(&reused));
        }
        for incoming in promotions {
            if let Some(mut held) = existing.iter().find(|t| t.id == incoming.id).cloned() {
                carry_enrichment(&mut held, &incoming);
                be.dag_set_task(&self.workspace_id, &held.id, &held).await?;
            }
        }
        if let Some(cycle) = detect_cycle_graph(&graph) {
            return Err(cog_core::SFError::Validation(format!(
                "Circular dependency detected: {}",
                cycle.join(" -> ")
            )));
        }
        // 注意：跨 pod 并发 add 各自基于稍早的图做环检测，极端情况下可能
        // 漏检环——任务提交通常来自单一 planner，接受该窗口（注释存档）。
        let mut added = Vec::new();
        for task in validated {
            let task_id = task.id.clone();
            be.dag_set_task(&self.workspace_id, &task_id, &task).await?;
            self.emit_event(cog_core::TaskEvent::TaskCreated {
                task_id: task_id.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;
            added.push(task_id);
        }
        Ok(added)
    }

    async fn add_task_store(&self, task: Task) -> SFResult<()> {
        let be = self.fg().expect("store mode");
        if be
            .dag_get_task(&self.workspace_id, &task.id)
            .await?
            .is_some()
        {
            return Err(cog_core::SFError::Validation(format!(
                "Task {} already exists",
                task.id
            )));
        }
        let added = self.add_tasks_batch_store(vec![task]).await?;
        debug_assert_eq!(added.len(), 1);
        Ok(())
    }

    async fn complete_task_store(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        let be = self.fg().expect("store mode");
        let task = self.store_task(task_id).await?;
        if task.status != TaskStatus::Running {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot complete task in {:?} state — it may have been handled by timeout or retry",
                    task.status
                ),
            });
        }
        if let Some(ref reg) = self.circuit_registry {
            let _ = reg.record_success(&task.task_type);
        }
        let scheduled = be
            .dag_complete_task(&self.workspace_id, task_id, result.clone())
            .await?;
        // 就绪下游保持 Pending，由 publish_ready_tasks → schedule_task 翻转
        // 并发 TaskScheduled 事件；此处只发根任务完成事件
        self.emit_event(cog_core::TaskEvent::TaskCompleted {
            task_id: task_id.into(),
            result: Some(result),
            scheduled_dependents: scheduled.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        crate::observable::global_observable().record_task(true);
        Ok(scheduled)
    }

    async fn fail_task_store(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        let be = self.fg().expect("store mode");
        let task = self.store_task(task_id).await?;
        if let Some(ref reg) = self.circuit_registry {
            reg.record_failure(&task.task_type)?;
        }
        be.dag_append_retry(
            &self.workspace_id,
            task_id,
            &RetryAttempt {
                attempt: task.retry_count + 1,
                error: error.clone(),
                timestamp: chrono::Utc::now(),
            },
        )
        .await?;
        // A refusal names one call and one rule, not a verdict on the task: the
        // work may still be done by another route. Hand it back to the next
        // attempt through `input`, which is the part of a task a role reads,
        // and hand it back once — a second refusal means the rule did not move,
        // and the ordinary failure path below then gives it no retries.
        if let Some(refusal) = cog_core::contract::outcome::guardrail_refusal_in(&error) {
            if !Self::carries_a_refusal(&task.input) {
                let requeued = self
                    .requeue_without_charge_store(
                        task_id,
                        &error,
                        None,
                        None,
                        &[
                            TaskStatus::Pending,
                            TaskStatus::Scheduled,
                            TaskStatus::Running,
                        ],
                        Some(&refusal),
                    )
                    .await?;
                if requeued {
                    return Ok((true, Vec::new(), false));
                }
            }
        }
        // 环境付账的失败不进任务的预算账：回队列等上游回来，`retry_count`
        // 原样不动。与内存路径同一判据、同一个回投动作（界的说明见
        // [`Self::the_environment_pays`]）。
        if Self::the_environment_pays(&error, cause, retry_after_secs) {
            let requeued = self
                .requeue_without_charge_store(
                    task_id,
                    &error,
                    cause,
                    retry_after_secs,
                    // 能落上的是"还欠一轮"的那几种状态：报告通常在 Running
                    // 到达，也可能晚于一次回收（那时行已经是 Pending），或者
                    // 晚于重新发布（Scheduled）。终态不在集合里——判死的和跑完
                    // 的不接受一次回投，那等于把它们复活。
                    &[
                        TaskStatus::Pending,
                        TaskStatus::Scheduled,
                        TaskStatus::Running,
                    ],
                    None,
                )
                .await?;
            if !requeued {
                tracing::warn!(
                    task_id = %task_id,
                    "an upstream refusal arrived for a task that had already moved on; \
                     leaving it to whoever moved it"
                );
            }
            return Ok((requeued, Vec::new(), false));
        }

        // 与内存路径同一个判据：不能靠重跑清除的失败不给重试预算。两处调的是
        // 同一个函数，所以「同一个判据」是结构上成立的，不是靠两边都写对。
        let max_retries = self.effective_max_retries(&error, cause, &task.task_type);
        // 退避随任务一起落库，不留在调度循环的内存里：判定重试的进程与之后
        // 重新投递它的进程可能不是同一个。
        let retry_delay =
            self.retry_matrix
                .delay_with_hint(&task.task_type, task.retry_count, retry_after_secs);
        let (retried, cancelled) = be
            .dag_fail_task(
                &self.workspace_id,
                task_id,
                error.clone(),
                cause,
                max_retries,
                retry_delay,
            )
            .await?;
        for dep_id in &cancelled {
            self.emit_event(cog_core::TaskEvent::TaskCancelled {
                task_id: dep_id.clone(),
                reason: format!(
                    "Cascade cancelled: upstream task '{}' permanently failed",
                    task_id
                ),
                timestamp: chrono::Utc::now(),
            })
            .await;
        }
        self.emit_event(cog_core::TaskEvent::TaskFailed {
            task_id: task_id.into(),
            error,
            retried,
            cancelled: cancelled.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        crate::observable::global_observable().record_task(false);
        let dlq_pushed = !retried && self.dlq.is_some();
        Ok((retried, cancelled, dlq_pushed))
    }

    /// The store-mode half of [`Self::cancel_task_with_reason`]: one conditional
    /// write on the backend, then the events for the task and everything it
    /// cascaded to.
    async fn cancel_task_store_with(&self, task_id: &str, reason: String) -> SFResult<Vec<String>> {
        let be = self.fg().expect("store mode");
        let cancelled = be
            .dag_cancel_task(&self.workspace_id, task_id, reason.clone())
            .await?;
        self.emit_event(cog_core::TaskEvent::TaskCancelled {
            task_id: task_id.into(),
            reason: reason.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        for dep_id in &cancelled {
            self.emit_event(cog_core::TaskEvent::TaskCancelled {
                task_id: dep_id.clone(),
                reason: format!(
                    "Cascade cancelled: upstream task '{}' was cancelled",
                    task_id
                ),
                timestamp: chrono::Utc::now(),
            })
            .await;
        }
        Ok(cancelled)
    }

    async fn check_timeouts_store(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        let be = self.fg().expect("store mode");
        let now = chrono::Utc::now();
        let all = be
            .dag_get_all_tasks(&self.workspace_id)
            .await
            .unwrap_or_default();
        let reclaimable: Vec<(Task, ReclaimCause)> = all
            .into_iter()
            .filter_map(|t| reclaim_cause(&t, self.task_lease, now).map(|c| (t, c)))
            .collect();

        let mut results = Vec::new();
        for (t, cause) in reclaimable {
            // 再确认仍 Running：扫描到此间可能已被执行器完成
            match be.dag_get_task(&self.workspace_id, &t.id).await {
                Ok(Some(cur)) if cur.status == TaskStatus::Running => {}
                _ => continue,
            }
            self.emit_reclaim(&t, &cause).await;
            // 该不该记账由类型说了算（见 `ReclaimCause::charges_retry_budget`）：
            // 不记账的回收不走通用的失败处理，那条路只剩预算一个判据。
            if !cause.charges_retry_budget() {
                match self
                    .requeue_without_charge_store(
                        &t.id,
                        &cause.error(),
                        None,
                        None,
                        &[TaskStatus::Running],
                        None,
                    )
                    .await
                {
                    Ok(true) => results.push((t.id.clone(), true, Vec::new(), false)),
                    Ok(false) => {}
                    Err(e) => tracing::warn!(
                        task_id = %t.id,
                        "cannot re-arm a task whose owner is gone: {e}"
                    ),
                }
                continue;
            }
            // 回收是编排层观测到的事实，不是传输层给的信号：没有状态码可依，
            // 类型留空，让下游知道这次失败只有文本。
            if let Ok((retried, cancelled, dlq_pushed)) =
                self.fail_task(&t.id, cause.error(), None).await
            {
                if retried {
                    let _ = self
                        .store_transition(
                            &t.id,
                            &[TaskStatus::Pending],
                            "Cannot transition task in {} state",
                            |task| task.clear_run(),
                            None,
                        )
                        .await;
                }
                results.push((t.id.clone(), retried, cancelled, dlq_pushed));
            }
        }
        results
    }

    /// The row a task becomes when the cause of the failure is the
    /// environment's and not the task's: back in `Pending`, any departed
    /// owner's claim cleared, and `retry_count` carried over untouched.
    ///
    /// Deliberately the same shape as [`Self::stalled_requeued`], because it
    /// rests on the same rule: a charge has to be justified by what the task
    /// did, and neither "the process holding it was replaced" nor "the upstream
    /// said come back in N" is the task's doing.
    ///
    /// It differs from the stall re-arm in one respect — it keeps a
    /// `retry_not_before`. A task requeued this way had a turn that was cut
    /// short (mid-run, or on a refusal the upstream itself timed), so pacing
    /// the next dispatch is pacing the work itself; a stalled one never
    /// started, so any wait would be the matrix's wait for an attempt it never
    /// made.
    fn requeued_without_charge(
        task: &Task,
        error: &str,
        cause: Option<UpstreamFailure>,
        delay: std::time::Duration,
    ) -> Task {
        let now = chrono::Utc::now();
        let mut next = task.clone();
        next.status = TaskStatus::Pending;
        next.error = Some(error.to_string());
        next.error_cause = cause;
        next.retry_not_before = Some(
            now + chrono::Duration::from_std(delay).unwrap_or_else(|_| chrono::Duration::zero()),
        );
        next.clear_run();
        next.updated_at = now;
        next
    }

    /// Whether this task's input already carries a refusal — i.e. it has
    /// already been re-dispatched on one. One refusal is the allowance: the
    /// second one is the rule answering the same question again.
    fn carries_a_refusal(input: &serde_json::Value) -> bool {
        input.get("guardrail_refusal").is_some()
    }

    /// Plant the refusal in the part of the task the next attempt actually
    /// reads, and clear any backoff: the next attempt is a *different*
    /// request, so whatever the previous one was waiting for does not apply.
    ///
    /// `None` when the input is not an object — there is nowhere to plant it.
    /// The caller then falls through to the ordinary failure path, which gives
    /// a refusal no retries at all. A guessed tool or rule would be worse than
    /// that dead end: the next attempt would be told to avoid the wrong call.
    fn with_the_refusal_planted(mut next: Task, refusal: &GuardrailRefusal) -> Option<Task> {
        let map = next.input.as_object_mut()?;
        map.insert("guardrail_refusal".to_string(), refusal.payload());
        next.retry_not_before = None;
        Some(next)
    }

    /// 把一条任务放回队列，**不记重试**。
    ///
    /// 写的条件是 `expected`：这条路上有第二个写者（被执行器完成、被别的副本的
    /// 回收循环接手），条件不成立就是任务已经走了，不是失败——与清扫器同一条
    /// 理由，所以也只是放弃这一轮，不报错。
    ///
    /// 等待取上游明说的时长与策略退避的较大者：`retry_after_secs` 为 `None`
    /// 时就是策略退避（回收路径），为 `Some` 时是上游自己的测量。
    ///
    /// 返回 `false` 表示条件没落上；`true` 表示任务已经回到 `Pending`。
    async fn requeue_without_charge_store(
        &self,
        task_id: &str,
        error: &str,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
        expected: &[TaskStatus],
        plant: Option<&GuardrailRefusal>,
    ) -> SFResult<bool> {
        let be = self.fg().expect("store mode");
        let Some(cur) = be.dag_get_task(&self.workspace_id, task_id).await? else {
            return Ok(false);
        };
        if !expected.contains(&cur.status) {
            return Ok(false);
        }
        let delay =
            self.retry_matrix
                .delay_with_hint(&cur.task_type, cur.retry_count, retry_after_secs);
        let next = Self::requeued_without_charge(&cur, error, cause, delay);
        let next = match plant {
            Some(refusal) => match Self::with_the_refusal_planted(next, refusal) {
                Some(planted) => planted,
                // No object to plant into: leave the row where it is and let
                // the ordinary failure path judge it.
                None => return Ok(false),
            },
            None => next,
        };
        if let Err(e) = be
            .dag_transition_task(&self.workspace_id, task_id, expected, &next)
            .await
        {
            // 只有「写没落上、行还停在原状态」才是存储拒绝了刚接受的工作；
            // 否则就是任务已经走到别处去了。
            if matches!(
                self.get_task(task_id).await,
                Some(c) if expected.contains(&c.status)
            ) {
                return Err(e);
            }
            return Ok(false);
        }
        // 与清扫器同一处读数：`TaskRetried` 带的是**没变过**的 `retry_count`，
        // 重复出现的同一个数就是「这一轮没动它的预算」。
        self.emit_event(cog_core::TaskEvent::TaskRetried {
            task_id: task_id.into(),
            retry_count: next.retry_count,
            timestamp: chrono::Utc::now(),
        })
        .await;
        Ok(true)
    }

    async fn archive_terminated_tasks_store(&self) {
        // 存储权威模式下终态任务本身就是持久记录，不做"内存→细粒度存储"
        // 的搬迁（那是内存模式的归档语义）。保留行即保留审计轨迹。
        let be = self.fg().expect("store mode");
        let threshold =
            chrono::Utc::now() - chrono::Duration::seconds(self.archive_after_secs as i64);
        let all = be
            .dag_get_all_tasks(&self.workspace_id)
            .await
            .unwrap_or_default();
        let to_archive: Vec<String> = all
            .iter()
            .filter(|t| {
                matches!(
                    t.status,
                    TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
                ) && t.updated_at < threshold
            })
            .map(|t| t.id.clone())
            .collect();
        for task_id in &to_archive {
            if let Err(e) = be.dag_remove_task(&self.workspace_id, task_id).await {
                tracing::warn!("archive: dag_remove_task failed for {}: {}", task_id, e);
            }
        }
        if !to_archive.is_empty() {
            tracing::info!(
                "Archived {} terminated task(s) from store (workspace {})",
                to_archive.len(),
                self.workspace_id
            );
        }
    }

    pub async fn find_ready_tasks(&self) -> Vec<Task> {
        self.ready_tasks_and_in_flight().await.0
    }

    /// The ready tasks, plus how many tasks are already owed on each ready
    /// queue, from one read of the graph.
    ///
    /// The second half exists because admission asks "how many may run" and
    /// that question is only answerable against what is already owed: a bound
    /// compared with zero every pass admits the whole allowance again while the
    /// previous batch is still queued. `Running` counts as owed -- someone is
    /// spending the window on it right now. So does `Scheduled` while its owner
    /// may still be there: the publisher marks a task scheduled before it
    /// writes it to the transport, so the row has been handed a slot even if
    /// nothing has picked it up yet.
    ///
    /// A `Scheduled` row whose lease has lapsed does not count. Whoever held it
    /// is gone, and the message that put it there is either acked without the
    /// task having started or lost with the queue it was written to. Re-arming
    /// that row is the stall sweep's job, and until the sweep reaches it the
    /// row is waiting, not spending: nothing about it will consume an
    /// allowance. Counting it turns a leak into a ceiling -- two thousand rows
    /// that reached this state over three days and never left are then
    /// subtracted from every window the pool ever opens, and the reading is a
    /// queue that is full when the queue is in fact empty.
    ///
    /// `lease_expiry` is read here rather than `lease_expires_at` for the reason
    /// it exists: a row that carries a start time and no explicit lease is
    /// still judgeable, and one carrying neither is not evidence of expiry at
    /// all.
    ///
    /// Per queue, not per process: the process-local queue can hold a backlog
    /// of its own for weeks while the shared queue is empty, and one combined
    /// count would let that backlog stop work that is not behind it.
    pub async fn ready_tasks_and_in_flight(
        &self,
    ) -> (Vec<Task>, std::collections::HashMap<String, usize>) {
        let work = self.ready_work().await;
        (work.ready, work.in_flight)
    }

    /// One read of the graph, answering every question admission asks: what is
    /// ready, what each queue already owes, and how each project's board
    /// columns are occupied.
    ///
    /// The column counts are per project because a WIP limit is a board's
    /// limit, and a board is one project: two projects filling the same column
    /// are not each other's backpressure. They are counted here rather than in
    /// a second scan so the occupancy a decision is made on and the ready set
    /// it is made about come from the same snapshot -- a second read could see
    /// work finish in between and refuse against a column that has already
    /// drained.
    pub async fn ready_work(&self) -> ReadyWork {
        let tasks = self.all_tasks_unified().await;
        let mut columns: std::collections::HashMap<
            String,
            std::collections::BTreeMap<&'static str, u64>,
        > = std::collections::HashMap::new();
        for t in &tasks {
            if t.status.is_terminal() {
                continue;
            }
            *columns
                .entry(t.project_key().to_string())
                .or_default()
                .entry(t.status.as_str())
                .or_insert(0) += 1;
        }
        let now = chrono::Utc::now();
        let mut in_flight: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for t in tasks.iter().filter(|t| match t.status {
            TaskStatus::Running => true,
            TaskStatus::Scheduled => t
                .lease_expiry(self.task_lease)
                .is_none_or(|expires| expires > now),
            _ => false,
        }) {
            *in_flight
                .entry(crate::ready_queue::ready_stream_for(t, &self.workspace_id))
                .or_default() += 1;
        }
        let by_id: std::collections::HashMap<&str, &Task> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        // Work the dispatcher would have started had it said what done means.
        // Counted by kind and named once per process: holding the row is the
        // safe half of this rule, and a kind that files one has to be findable
        // rather than merely stuck.
        let mut unclosable: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for t in tasks.iter().filter(|t| t.status == TaskStatus::Pending) {
            if t.is_executable
                && !crate::ready_queue::is_face_duty(t)
                && !crate::ready_queue::states_what_done_means(t)
            {
                *unclosable.entry(t.task_type.kind_key()).or_default() += 1;
            }
        }
        name_unclosable_kinds(&unclosable);
        let mut ready: Vec<Task> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .filter(|t| crate::ready_queue::is_dispatchable_work(t))
            // 退避未到期的重试不是"就绪"，重投它等于取消退避。周期性发布者每次
            // 都会扫到这里，所以这个过滤同时也是退避到点后的重新投递者。
            .filter(|t| t.retry_not_before.is_none_or(|due| due <= now))
            .filter(|t| {
                t.blocked_by.iter().all(|dep_id| {
                    by_id
                        .get(dep_id.as_str())
                        .map(|dep| dep.status == TaskStatus::Completed)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .collect();
        ready.sort_by_key(|t| -t.priority);
        ReadyWork {
            ready,
            in_flight,
            columns,
        }
    }

    /// Return all tasks that are ready for execution (Pending or Scheduled).
    /// Unlike [`Self::find_ready_tasks`], this includes tasks that have already
    /// been transitioned to `Scheduled` — useful for query endpoints.
    pub async fn get_ready_tasks(&self) -> Vec<Task> {
        let tasks = self.all_tasks_unified().await;
        let by_id: std::collections::HashMap<&str, &Task> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        let mut ready: Vec<Task> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending || t.status == TaskStatus::Scheduled)
            .filter(|t| {
                t.blocked_by.iter().all(|dep_id| {
                    by_id
                        .get(dep_id.as_str())
                        .map(|dep| dep.status == TaskStatus::Completed)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .collect();
        ready.sort_by_key(|t| -t.priority);
        ready
    }

    /// Pure classifier for decomposition orphans: Pending, non-executable
    /// parent placeholders that no task claims as its parent and that have
    /// not been touched since `stall_before`. Such tasks can never be
    /// scheduled and never fail on their own — they only exist when
    /// decomposition persisted the placeholder without any children
    /// (empty LLM result, partial write, crash mid-injection).
    pub fn decomposition_orphans<'a>(
        tasks: impl IntoIterator<Item = &'a Task>,
        stall_before: chrono::DateTime<chrono::Utc>,
    ) -> Vec<String> {
        let tasks: Vec<&Task> = tasks.into_iter().collect();
        let has_child: std::collections::HashSet<&str> = tasks
            .iter()
            .filter_map(|t| t.parent_task_id.as_deref())
            .collect();
        tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .filter(|t| !t.is_executable)
            .filter(|t| !has_child.contains(t.id.as_str()))
            .filter(|t| t.updated_at < stall_before)
            .map(|t| t.id.clone())
            .collect()
    }

    /// Scan the DAG for childless non-executable placeholders stalled in
    /// Pending since before `stall_before`.
    pub async fn find_decomposition_orphans(
        &self,
        stall_before: chrono::DateTime<chrono::Utc>,
    ) -> Vec<Task> {
        let tasks = self.all_tasks_unified().await;
        let ids: std::collections::HashSet<String> =
            Self::decomposition_orphans(&tasks, stall_before)
                .into_iter()
                .collect();
        tasks.into_iter().filter(|t| ids.contains(&t.id)).collect()
    }

    /// Pure classifier for tasks whose ready message was consumed without the
    /// task ever reaching `Running`.
    ///
    /// `Scheduled` records one thing: a ready message carrying this task was
    /// handed to the transport. The state machine and the transport are then
    /// two different objects holding the same fact, and only one of them is
    /// durable. When the message is acked and the task is not started — the
    /// consumer died in between, or the start came back with an infrastructure
    /// error and the message was dropped — the row keeps a state that nothing
    /// revisits: the publisher scans `Pending`, the timeout checker reclaims
    /// `Running`, and the alert rules read no task state at all. The task is
    /// held forever, and the silence reads exactly like an idle DAG.
    ///
    /// A row older than `stall_before` is that case. Anything younger is
    /// simply in flight — the transport needs a moment, and the sweeper that
    /// re-delivers an unacknowledged message has not had its turn yet.
    pub fn stalled_scheduled<'a>(
        tasks: impl IntoIterator<Item = &'a Task>,
        stall_before: chrono::DateTime<chrono::Utc>,
    ) -> Vec<String> {
        tasks
            .into_iter()
            .filter(|t| t.status == TaskStatus::Scheduled)
            .filter(|t| t.updated_at < stall_before)
            .map(|t| t.id.clone())
            .collect()
    }

    /// Scan the DAG for tasks stalled in `Scheduled` since before
    /// `stall_before`.
    pub async fn find_stalled_scheduled_tasks(
        &self,
        stall_before: chrono::DateTime<chrono::Utc>,
    ) -> Vec<Task> {
        let tasks = self.all_tasks_unified().await;
        let ids: std::collections::HashSet<String> = Self::stalled_scheduled(&tasks, stall_before)
            .into_iter()
            .collect();
        tasks.into_iter().filter(|t| ids.contains(&t.id)).collect()
    }

    /// Pure classifier for Pending tasks that nothing can ever unblock.
    ///
    /// Readiness asks every predecessor to be `Completed`. A predecessor that
    /// ended `Failed` or `Cancelled` will never become that, and one that is not
    /// in the graph at all never will either — so the task behind it is Pending
    /// for the rest of the deployment's life. Nothing is wrong with the
    /// executor in that state: the publisher scans the row on every tick, the
    /// readiness filter rejects it on every tick, and the row's age keeps
    /// growing in `cogneva_task_oldest_untouched_seconds` while it reads like a
    /// stalled queue.
    ///
    /// The failure and cancel paths already cancel the dependents that existed
    /// when they ran. This covers the ones that were added afterwards — a plan
    /// whose ids collide with rows of an earlier goal, or a dependency written
    /// against a task that was never persisted. Returns each such task with the
    /// predecessor that settled it and what that predecessor's state is.
    pub fn unblockable_dependents<'a>(
        tasks: impl IntoIterator<Item = &'a Task>,
    ) -> Vec<(String, String, String)> {
        let tasks: Vec<&Task> = tasks.into_iter().collect();
        let by_id: HashMap<&str, &Task> = tasks.iter().map(|t| (t.id.as_str(), *t)).collect();
        tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .filter_map(|t| {
                let blocker =
                    t.blocked_by
                        .iter()
                        .find(|dep_id| match by_id.get(dep_id.as_str()) {
                            Some(dep) => {
                                matches!(dep.status, TaskStatus::Failed | TaskStatus::Cancelled)
                            }
                            None => true,
                        })?;
                let state = by_id
                    .get(blocker.as_str())
                    .map(|dep| dep.status.as_str())
                    .unwrap_or("missing");
                Some((t.id.clone(), blocker.clone(), state.to_string()))
            })
            .collect()
    }

    /// Cancel every Pending task no earlier task can ever release.
    ///
    /// Cancelling is what the failure path does to the dependents that exist at
    /// the moment it fails; this is the same outcome for the ones that appear
    /// after it. The task never ran, so nothing is charged against a retry
    /// budget, and the cancel cascades to its own dependents through the
    /// ordinary cancel path, which is also what derives the goal row's terminal
    /// state from its declared sink.
    ///
    /// A predecessor that is absent is not the same evidence as one that has
    /// settled. A batch is written one row at a time, so a row whose predecessor
    /// is later in the same batch is briefly referencing an id that is not in
    /// the graph yet, and this sweep runs between ticks. Only a row older than
    /// `missing_grace` may be read as "that id never existed"; a settled
    /// predecessor needs no grace, because it cannot come back.
    pub async fn reclaim_unblockable_dependents(
        &self,
        missing_grace: chrono::Duration,
    ) -> Vec<String> {
        let tasks = self.all_tasks_unified().await;
        let missing_before = chrono::Utc::now() - missing_grace;
        let by_id: HashMap<&str, &Task> = tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        let mut cancelled = Vec::new();
        for (task_id, blocker, blocker_state) in Self::unblockable_dependents(tasks.iter()) {
            if blocker_state == "missing" {
                let old_enough = by_id
                    .get(task_id.as_str())
                    .is_some_and(|t| t.updated_at < missing_before);
                if !old_enough {
                    continue;
                }
            }
            let reason = format!(
                "Cancelled without being run: predecessor '{blocker}' is {blocker_state}, \
                 so this task can never become ready"
            );
            match self.cancel_task_with_reason(&task_id, reason).await {
                Ok(downstream) => {
                    tracing::warn!(
                        task_id = %task_id,
                        blocker = %blocker,
                        blocker_state = %blocker_state,
                        downstream = downstream.len(),
                        "cancelled a task whose predecessor had already settled: it could never become ready"
                    );
                    cancelled.push(task_id);
                }
                Err(e) => tracing::warn!(
                    task_id = %task_id,
                    blocker = %blocker,
                    "cannot cancel a task that can never become ready: {e}"
                ),
            }
        }
        cancelled
    }

    /// Validate that a task is a terminable orphan: Pending non-executable
    /// placeholder with no child task pointing at it.
    fn ensure_terminable_orphan<'a>(
        task_id: &str,
        tasks: impl Iterator<Item = &'a Task>,
    ) -> SFResult<()> {
        let tasks: Vec<&Task> = tasks.collect();
        let task = tasks.iter().find(|t| t.id == task_id).ok_or_else(|| {
            cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            }
        })?;
        if task.status != TaskStatus::Pending || task.is_executable {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Task {} is not a pending non-executable placeholder (status={:?})",
                    task_id, task.status
                ),
            });
        }
        if tasks
            .iter()
            .any(|t| t.parent_task_id.as_deref() == Some(task_id))
        {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Task {} still has child tasks; only a childless placeholder can be terminated as a decomposition orphan",
                    task_id
                ),
            });
        }
        Ok(())
    }

    /// Move a stalled decomposition orphan from Pending straight to Failed
    /// without retry/cascade semantics: the task was never runnable and has
    /// no edges, so the regular fail_task path (retry matrix, dependents
    /// cancellation) does not apply.
    pub async fn terminate_decomposition_orphan(
        &self,
        task_id: &str,
        reason: String,
    ) -> SFResult<()> {
        if let Some(be) = self.fg().cloned() {
            let all = be.dag_get_all_tasks(&self.workspace_id).await?;
            Self::ensure_terminable_orphan(task_id, all.iter())?;
            let task_error = reason.clone();
            self.store_transition(
                task_id,
                &[TaskStatus::Pending],
                "Cannot terminate decomposition orphan in {} state",
                move |t| {
                    t.status = TaskStatus::Failed;
                    t.error = Some(task_error.clone());
                },
                Some(cog_core::TaskEvent::TaskFailed {
                    task_id: task_id.into(),
                    error: reason,
                    retried: false,
                    cancelled: Vec::new(),
                    timestamp: chrono::Utc::now(),
                }),
            )
            .await?;
            return Ok(());
        }
        let mut inner = self.inner.write().await;
        Self::ensure_terminable_orphan(task_id, inner.tasks.values())?;
        let task = inner.tasks.get_mut(task_id).expect("validated above");
        task.status = TaskStatus::Failed;
        task.error = Some(reason.clone());
        task.updated_at = chrono::Utc::now();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskFailed {
            task_id: task_id.into(),
            error: reason,
            retried: false,
            cancelled: Vec::new(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn schedule_task(&self, task_id: &str) -> SFResult<()> {
        if self.fg().is_some() {
            return self
                .store_transition(
                    task_id,
                    &[TaskStatus::Pending],
                    "Cannot schedule task in {} state",
                    |t| t.status = TaskStatus::Scheduled,
                    Some(cog_core::TaskEvent::TaskScheduled {
                        task_id: task_id.into(),
                        timestamp: chrono::Utc::now(),
                    }),
                )
                .await;
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        if task.status != TaskStatus::Pending {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot schedule task in {:?} state", task.status),
            });
        }

        task.status = TaskStatus::Scheduled;
        let task_snapshot = task.clone();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskScheduled {
            task_id: task_id.into(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    /// Bind a task to the unit that has to run it.
    ///
    /// The name goes on `owner_unit`, not `agent_id`. The two columns answer
    /// different questions -- `agent_id` is who delivered the result, a
    /// designation is who was told to -- and only the owner column takes a task
    /// off the shared ready queue. Writing the designation into `agent_id`
    /// reads back as assigned while the task still sits in the queue every
    /// worker competes for, which is the one thing a designation has to change.
    ///
    /// Only a task that has not been offered yet can be bound. `Scheduled`
    /// means the row has been handed to the ready queue, so the run that
    /// happens may already be somebody else's; binding it now would make that
    /// result unacceptable, because completion refuses a sender that is not the
    /// owner, and the row would stay unfinished with nothing to show why.
    ///
    /// No event is emitted. The status does not move: the task waits for its
    /// owner in `Pending`, so announcing `task_scheduled` would tell every
    /// reader it had been handed to a worker when nobody had picked it up.
    pub async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()> {
        if self.fg().is_some() {
            return self
                .store_transition(
                    task_id,
                    &[TaskStatus::Pending],
                    "Cannot assign task in {} state: it has already been offered to the \
                     shared ready queue, and a binding made now could not be honoured",
                    |t| t.owner_unit = Some(agent_id.into()),
                    None,
                )
                .await;
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        if task.status != TaskStatus::Pending {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot assign task in {:?} state: it has already been offered to the \
                     shared ready queue, and a binding made now could not be honoured",
                    task.status
                ),
            });
        }

        task.owner_unit = Some(agent_id.into());
        let task_snapshot = task.clone();
        drop(inner);
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    /// `running` 那一列满了就不再放一条进去。
    ///
    /// 发布侧已经按项目判过一次（`ready_queue::column_at_limit`），但那一次判的是
    /// **发布**那一刻的在制品数：一条活要经过「进 scheduled」与「进 running」两步，
    /// 两步之间别的活可以继续把 `running` 填满。只在发布侧判，上限就只在那一刻成立
    /// ——`scheduled` 里攒着的几条一起启动，就能把它顶过上限，而看板那一格照样印着
    /// 「未超」。上限是准入控制，不是某一刻的快照，所以判在真正跨进去的那一步上。
    ///
    /// 没配 `running` 上限的进程在这里不读图：这条判据不给所有部署加一次全图读。
    async fn refuse_when_the_running_column_is_full(&self, task_id: &str) -> SFResult<()> {
        // 读一格只为拿一个数：守卫在这里就放掉，别让它跨过下面的 await。
        let limit = self
            .board_wip_limits
            .read()
            .unwrap()
            .limit_for(TaskStatus::Running.as_str());
        let Some(limit) = limit else {
            return Ok(());
        };
        // 任务不存在时不在这里抢先判：接下来的转换会给出它自己那条更具体的理由，
        // 而被这里说成「列满了」只会把人引到错误的方向。
        let Some(task) = self.get_task(task_id).await else {
            return Ok(());
        };
        let project = task.project_key();
        let running = self
            .all_tasks_unified()
            .await
            .iter()
            .filter(|t| t.status == TaskStatus::Running && t.project_key() == project)
            .count();
        if running < limit {
            return Ok(());
        }
        Err(cog_core::SFError::Conflict(format!(
            "任务 {task_id} 进不了 running：项目 {project} 的 running 列已满（{running}/{limit}），\
             上限是准入控制，不放行第 {} 条",
            limit + 1
        )))
    }

    pub async fn start_task(&self, task_id: &str) -> SFResult<()> {
        self.refuse_when_the_running_column_is_full(task_id).await?;
        if self.fg().is_some() {
            let (owner, lease) = (self.run_id.clone(), self.task_lease);
            return self
                .store_transition(
                    task_id,
                    &[TaskStatus::Scheduled],
                    "Cannot start task in {} state",
                    |t| t.begin_run(&owner, lease, chrono::Utc::now()),
                    Some(cog_core::TaskEvent::TaskStarted {
                        task_id: task_id.into(),
                        timestamp: chrono::Utc::now(),
                    }),
                )
                .await;
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        if task.status != TaskStatus::Scheduled {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot start task in {:?} state", task.status),
            });
        }

        task.begin_run(&self.run_id, self.task_lease, chrono::Utc::now());
        let task_snapshot = task.clone();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskStarted {
            task_id: task_id.into(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    /// 任务走到终态时要做的事：先把它名下的 agent 放掉，再把它的终态与答案
    /// 抄到声明了它的那一行上。
    ///
    /// 两件事共用这一个判据，因为**终态只有一处定义**：多叫一次只是多读一行、
    /// 多回收一次（回收是幂等的），叫漏一次不会写错——漏掉的那条路只会让目标行
    /// 停在 Pending，由孤儿对账器兜住；而漏掉的回收会让名册一直涨。
    ///
    /// 目标行（不可执行的父占位）自己不执行，谁也不会按它跑出一个结果来——它的终态只能是
    /// 抄来的。抄谁由**声明**说了算：`parent_task_id` 指向的那一行上写着 `sink_task_id`，
    /// 只有它等于本任务的 id 时才抄。读的是声明，不是「谁没有下游」——按后者全局重算，会把
    /// 别的目标的行、单独提交的行一起算成汇点，那正是声明要替掉的那种算法。
    async fn on_task_terminal(&self, task_id: &str) {
        let Some(mut sink) = self.get_task(task_id).await else {
            return;
        };
        if !matches!(
            sink.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            return;
        }
        // 回收排在抄写之前，且对每个终态任务都做——汇点只是终态任务里的一小类。
        //
        // 只按终态回收：回队列的重试（`retried == true`）本来就不调这里，它们下一次
        // 执行接的应该是同一个对象；按失败回收会让每次记账重试都换一个新对象、丢一次
        // 上下文、重开一条连接。
        self.retire_task_agents(task_id).await;
        let Some(parent_id) = sink.parent_task_id.clone() else {
            return;
        };
        let Some(mut row) = self.get_task(&parent_id).await else {
            return;
        };
        if row.sink_task_id.as_deref() != Some(task_id) {
            return;
        }
        let completed = sink.status == TaskStatus::Completed;
        row.status = sink.status;
        // 答案只长在跑成的那条路上；判死与取消的行不留半个结果。
        row.result = if completed { sink.result.take() } else { None };
        row.error = sink.error.take();
        row.error_cause = sink.error_cause.take();
        row.updated_at = chrono::Utc::now();
        if let Some(be) = self.fg() {
            if let Err(e) = be.dag_set_task(&self.workspace_id, &parent_id, &row).await {
                tracing::warn!(
                    goal_row = %parent_id,
                    sink = %task_id,
                    "cannot record the goal row's terminal state: {e}"
                );
                return;
            }
        } else {
            let mut inner = self.inner.write().await;
            match inner.tasks.get_mut(&parent_id) {
                Some(existing) => *existing = row,
                None => {
                    tracing::warn!(
                        goal_row = %parent_id,
                        sink = %task_id,
                        "the goal row this sink is declared on is not in memory; the derived \
                         terminal state was not recorded"
                    );
                    return;
                }
            }
        }
        tracing::info!(
            goal_row = %parent_id,
            sink = %task_id,
            status = ?sink.status,
            "goal row terminal derived from its sink"
        );
    }

    /// 放掉一个终态任务名下的 agent。
    ///
    /// 拿不到池子就什么都不做、也不报错：`attach_agent_manager` 与其它启动期装配
    /// 走同一条路，缺了它这一格，回收就不存在——但这与「任务没有 agent」在行为上
    /// 是同一件事，只有名册会一直涨。这条缺口由名册的年龄读数现形，不由这里报错。
    async fn retire_task_agents(&self, task_id: &str) {
        let manager = self
            .agent_manager
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let Some(manager) = manager else {
            return;
        };
        match manager.retire_task_agents(task_id).await {
            Ok(retired) if retired > 0 => tracing::info!(
                task_id,
                retired,
                "retired the agents of a task that reached its terminal state"
            ),
            Ok(_) => {}
            Err(e) => tracing::warn!(
                task_id,
                "could not retire the agents of a terminal task: {e}"
            ),
        }
    }

    pub async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        if self.fg().is_some() {
            let scheduled = self.complete_task_store(task_id, result).await?;
            self.on_task_terminal(task_id).await;
            return Ok(scheduled);
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        // Guard against race with timeout detector: only complete if still Running.
        if task.status != TaskStatus::Running {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot complete task in {:?} state — it may have been handled by timeout or retry",
                    task.status
                ),
            });
        }

        task.status = TaskStatus::Completed;
        task.result = Some(result.clone());

        // Record success on circuit breaker if configured
        if let Some(ref reg) = self.circuit_registry {
            let _ = reg.record_success(&task.task_type);
        }

        // Auto-schedule dependents whose dependencies are now all completed.
        // 只判定并返回就绪 id，不翻转状态：Scheduled 的单一含义是"已发布到
        // ready 流"，翻转由 publish_ready_tasks → schedule_task 完成。
        let dependents_to_schedule: Vec<String> = inner
            .dependents
            .get(task_id)
            .map(|deps| deps.iter().cloned().collect())
            .unwrap_or_default();

        let mut scheduled = Vec::new();
        for dep_id in dependents_to_schedule {
            if let Some(dep_task) = inner.tasks.get(&dep_id) {
                let all_deps_completed = dep_task.blocked_by.iter().all(|bid| {
                    inner
                        .tasks
                        .get(bid)
                        .map(|t| t.status == TaskStatus::Completed)
                        .unwrap_or(false)
                });
                if all_deps_completed && dep_task.status == TaskStatus::Pending {
                    scheduled.push(dep_id.clone());
                }
            }
        }

        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskCompleted {
            task_id: task_id.into(),
            result: Some(result.clone()),
            scheduled_dependents: scheduled.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;

        // Fine-grained persist for completed task and newly scheduled dependents
        let completed_task = {
            let inner = self.inner.read().await;
            inner.tasks.get(task_id).cloned()
        };
        if let Some(ref t) = completed_task {
            self.persist_task_fine_grained(t).await;
        }
        for dep_id in &scheduled {
            let dep_task = {
                let inner = self.inner.read().await;
                inner.tasks.get(dep_id).cloned()
            };
            if let Some(ref t) = dep_task {
                self.persist_task_fine_grained(t).await;
            }
        }

        crate::observable::global_observable().record_task(true);

        self.force_checkpoint().await;
        self.on_task_terminal(task_id).await;
        Ok(scheduled)
    }

    fn collect_downstream(inner: &Inner, task_id: &str) -> Vec<String> {
        let mut result = Vec::new();
        let mut stack = vec![task_id.to_string()];
        let mut visited = HashSet::new();

        while let Some(current) = stack.pop() {
            if let Some(deps) = inner.dependents.get(&current) {
                for dep_id in deps {
                    if visited.insert(dep_id.clone()) {
                        result.push(dep_id.clone());
                        stack.push(dep_id.clone());
                    }
                }
            }
        }

        result
    }

    /// Whether re-running this task could succeed.
    ///
    /// Two independent channels can say a failure is deterministic, and either
    /// one is enough. [`UpstreamFailure::is_terminal`] is the typed channel,
    /// filled when the transport saw an HTTP status. The reason prefix is the
    /// in-band channel, and it is what carries the verdict when the failure is
    /// discovered one or more crates deeper than the transport — a generator
    /// that produced nothing because its prompt never reached the upstream
    /// reports it as text, and no status code survives that far.
    ///
    /// Both are consulted because they answer the same question from different
    /// distances, and a retry that only reads the near one pays again for every
    /// failure whose cause was found far away.
    fn is_retryable_failure(error: &str, cause: Option<UpstreamFailure>) -> bool {
        !cause.is_some_and(UpstreamFailure::is_terminal)
            && !cog_core::contract::outcome::is_deterministic_failure(error)
    }

    /// The retry budget a failure actually gets.
    ///
    /// Not the task type's declared budget, which is only what a failure a
    /// re-run could clear receives: one that nothing about a re-run can change
    /// is given none, so the number its verdict compares against is zero. A row
    /// therefore carries one number and is judged by another, and which is
    /// which depends on the class of the failure -- never on the field.
    ///
    /// Both backends ask here rather than each branching on their own: a stored
    /// verdict and an in-memory one that disagreed about the same failure would
    /// be two retry semantics wearing one name, and both would look right on
    /// whichever backend the reader happened to be looking at.
    fn effective_max_retries(
        &self,
        error: &str,
        cause: Option<UpstreamFailure>,
        task_type: &TaskType,
    ) -> u32 {
        if Self::is_retryable_failure(error, cause) {
            self.retry_matrix.max_retries(task_type)
        } else {
            0
        }
    }

    /// 这笔账谁付：任务，还是它撞上的环境。
    ///
    /// [`Self::is_retryable_failure`] 答的是"预算适不适用"，这条答的是同族的
    /// 另一半——"这笔账记在谁头上"。两条读同一组输入（类型通道 + 带内标记），
    /// 所以这不是第二条判定链，是同一个判决点上的第二个问题。
    ///
    /// 判据是**上游给出了一次测量**：它说了什么时候值得再来。"282 秒后重试"
    /// 是上游对自己复位时刻的测量，任务只是恰好在那一刻问了，判它一份预算等于
    /// 让问的人替答的人付账。没有这句话的失败（一次 5xx、一次传输抖动）不是
    /// 窗口，是一次抖动，仍然记在任务的预算上；`is_terminal` 的环境因（配额
    /// 耗尽、凭证被拒）同样判死——环境不是一律不记账。
    ///
    /// 界就长在这个判据里，不在别处另定：回投的等待取"上游明说的时长"与策略
    /// 退避的较大者（[`RetryMatrix::delay_with_hint`]），所以每一次回投之前都
    /// 必须先有上游的一次新测量。没有测量就不回投，也没有哪个数字是这里发明的。
    fn the_environment_pays(
        error: &str,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> bool {
        let stated_a_wait = retry_after_secs
            .or_else(|| cog_core::contract::llm::retry_after_hint_in(error))
            .is_some();
        stated_a_wait && Self::is_retryable_failure(error, cause)
    }

    /// Fail a task with the given error.
    /// Returns `(retried, cancelled, dlq_pushed)` where:
    /// - `retried` = `true` if the task was sent back to Pending for retry
    /// - `cancelled` = list of downstream tasks cascade-cancelled
    /// - `dlq_pushed` = `true` if the task was moved to DLQ on final failure
    pub async fn fail_task(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        self.fail_task_after(task_id, error, cause, None).await
    }

    /// [`Self::fail_task`] for a failure whose upstream named the wait it wants
    /// before the next attempt. See [`RetryMatrix::delay_with_hint`] for how the
    /// stated wait and the policy delay are combined.
    pub async fn fail_task_after(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        if self.fg().is_some() {
            let (retried, cancelled, dlq_pushed) = self
                .fail_task_store(task_id, error, cause, retry_after_secs)
                .await?;
            // 判死才算终态：回队列的两种（记账重试、环境付账回投）都还没走到头。
            if !retried {
                self.on_task_terminal(task_id).await;
            }
            // 级联取消也是一条终态——被判死的上游拖下水的那种。
            for dep_id in &cancelled {
                self.on_task_terminal(dep_id).await;
            }
            return Ok((retried, cancelled, dlq_pushed));
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let (task_type, retry_count) = {
            let task =
                inner
                    .tasks
                    .get_mut(task_id)
                    .ok_or_else(|| cog_core::SFError::TaskFailed {
                        task_id: task_id.into(),
                        reason: "Task not found".into(),
                    })?;
            (task.task_type.clone(), task.retry_count)
        };

        // Record failure on circuit breaker
        if let Some(ref reg) = self.circuit_registry {
            reg.record_failure(&task_type)?;
        }

        // Track retry history
        inner
            .retry_history
            .entry(task_id.to_string())
            .or_default()
            .push(RetryAttempt {
                attempt: retry_count + 1,
                error: error.clone(),
                timestamp: chrono::Utc::now(),
            });

        // A refusal names one call and one rule, not a verdict on the task: the
        // work may still be done by another route. Hand it back to the next
        // attempt through `input`, which is the part of a task a role reads,
        // and hand it back once — a second refusal means the rule did not move,
        // and the ordinary failure path below then gives it no retries.
        if let Some(refusal) = cog_core::contract::outcome::guardrail_refusal_in(&error) {
            let already = inner
                .tasks
                .get(task_id)
                .is_some_and(|t| Self::carries_a_refusal(&t.input));
            if !already {
                let now = chrono::Utc::now();
                let task = inner.tasks.get_mut(task_id).expect("task exists");
                let mut next = task.clone();
                next.status = TaskStatus::Pending;
                next.error = Some(error.clone());
                next.error_cause = None;
                next.clear_run();
                next.updated_at = now;
                if let Some(next) = Self::with_the_refusal_planted(next, &refusal) {
                    *task = next.clone();
                    drop(inner);
                    self.emit_event(cog_core::TaskEvent::TaskRetried {
                        task_id: task_id.into(),
                        retry_count: next.retry_count,
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                    self.persist_task_fine_grained(&next).await;
                    self.force_checkpoint().await;
                    return Ok((true, Vec::new(), false));
                }
            }
        }

        // 环境付账的失败不进任务的预算账：回队列等上游回来，`retry_count`
        // 原样不动。等待就是上游自己报的那个时长（界的说明见
        // [`Self::the_environment_pays`]）——没有那条测量就走不到这里。
        if Self::the_environment_pays(&error, cause, retry_after_secs) {
            let delay =
                self.retry_matrix
                    .delay_with_hint(&task_type, retry_count, retry_after_secs);
            let now = chrono::Utc::now();
            let task = inner.tasks.get_mut(task_id).expect("task exists");
            task.status = TaskStatus::Pending;
            task.error = Some(error.clone());
            task.error_cause = cause;
            task.retry_not_before = Some(
                now + chrono::Duration::from_std(delay)
                    .unwrap_or_else(|_| chrono::Duration::zero()),
            );
            task.clear_run();
            task.updated_at = now;

            drop(inner);
            // 与清扫器同一处读数：`TaskRetried` 带的是**没变过**的 `retry_count`，
            // 重复出现的同一个数就是"这一轮没动它的预算"。
            self.emit_event(cog_core::TaskEvent::TaskRetried {
                task_id: task_id.into(),
                retry_count,
                timestamp: chrono::Utc::now(),
            })
            .await;
            let task_snapshot = {
                let inner = self.inner.read().await;
                inner.tasks.get(task_id).cloned()
            };
            if let Some(ref t) = task_snapshot {
                self.persist_task_fine_grained(t).await;
            }
            self.force_checkpoint().await;
            return Ok((true, Vec::new(), false));
        }

        // 确定性失败不受重试预算约束：同样的输入在同一环境里再来一次必然同样
        // 失败，多付的只是那一轮烧掉的 token 和上游调用。预算清零比"重试三次
        // 都白跑"更省，也让失败原因在第一轮就浮出来而不是被三次重试盖住。
        let max_retries = self.effective_max_retries(&error, cause, &task_type);

        let task = inner.tasks.get_mut(task_id).expect("task exists");
        if retry_count < max_retries {
            task.retry_count = retry_count + 1;
            task.status = TaskStatus::Pending;
            task.error = Some(error.clone());
            task.error_cause = cause;
            task.updated_at = chrono::Utc::now();
            task.retry_not_before = Some(
                chrono::Utc::now()
                    + self
                        .retry_matrix
                        .delay_with_hint(&task_type, retry_count, retry_after_secs),
            );

            drop(inner);
            self.emit_event(cog_core::TaskEvent::TaskFailed {
                task_id: task_id.into(),
                error: error.clone(),
                retried: true,
                cancelled: Vec::new(),
                timestamp: chrono::Utc::now(),
            })
            .await;
            let task_snapshot = {
                let inner = self.inner.read().await;
                inner.tasks.get(task_id).cloned()
            };
            if let Some(ref t) = task_snapshot {
                self.persist_task_fine_grained(t).await;
            }

            crate::observable::global_observable().record_task(false);
            self.force_checkpoint().await;
            Ok((true, Vec::new(), false)) // retried
        } else {
            task.status = TaskStatus::Failed;
            task.error = Some(error.clone());
            task.error_cause = cause;
            task.updated_at = chrono::Utc::now();
            // 终态任务若留下一个未来的时间戳，人工重投它时会先被退避挡掉——
            // 那个时间戳只在"回到 Pending 等下一次"这条路上有意义。
            task.retry_not_before = None;

            // Mark that DLQ push is needed; callers should call `push_to_dlq` async.
            let dlq_pushed = self.dlq.is_some();

            // Cascade cancel all downstream dependents since they can never be unblocked.
            let mut cancelled = Vec::new();
            let downstream = Self::collect_downstream(&inner, task_id);
            for dep_id in downstream {
                if let Some(t) = inner.tasks.get_mut(&dep_id) {
                    if t.status != TaskStatus::Cancelled
                        && t.status != TaskStatus::Failed
                        && t.status != TaskStatus::Completed
                    {
                        t.status = TaskStatus::Cancelled;
                        t.error = Some(format!(
                            "Cascade cancelled: upstream task '{}' permanently failed with error: {}",
                            task_id, error
                        ));
                        // 级联取消的原因是"上游永久失败"，不是上游拒绝我们：
                        // 类型不跟着传递。
                        t.error_cause = None;
                        t.updated_at = chrono::Utc::now();
                        cancelled.push(dep_id.clone());
                        drop(inner);
                        self.emit_event(cog_core::TaskEvent::TaskCancelled {
                            task_id: dep_id,
                            reason: format!(
                                "Cascade cancelled: upstream task '{}' permanently failed",
                                task_id
                            ),
                            timestamp: chrono::Utc::now(),
                        })
                        .await;
                        inner = self.inner.write().await;
                    }
                }
            }

            drop(inner);
            self.emit_event(cog_core::TaskEvent::TaskFailed {
                task_id: task_id.into(),
                error: error.clone(),
                retried: false,
                cancelled: cancelled.clone(),
                timestamp: chrono::Utc::now(),
            })
            .await;

            // Fine-grained persist for failed task and all cascade-cancelled tasks
            let failed_task = {
                let inner = self.inner.read().await;
                inner.tasks.get(task_id).cloned()
            };
            if let Some(ref t) = failed_task {
                self.persist_task_fine_grained(t).await;
            }
            for dep_id in &cancelled {
                let dep_task = {
                    let inner = self.inner.read().await;
                    inner.tasks.get(dep_id).cloned()
                };
                if let Some(ref t) = dep_task {
                    self.persist_task_fine_grained(t).await;
                }
            }

            crate::observable::global_observable().record_task(false);
            self.force_checkpoint().await;
            self.on_task_terminal(task_id).await;
            for dep_id in &cancelled {
                self.on_task_terminal(dep_id).await;
            }
            Ok((false, cancelled, dlq_pushed)) // permanently failed
        }
    }

    /// Asynchronously push a failed task to the DLQ.
    /// Callers should use this after `fail_task` returns `(false, _, _)`
    /// to ensure the DLQ entry is persisted.
    pub async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool> {
        if let Some(ref dlq) = self.dlq {
            let (task, history) = if let Some(be) = self.fg() {
                let task = self.store_task(task_id).await?;
                let history = be
                    .dag_get_retry_history(&self.workspace_id, task_id)
                    .await
                    .unwrap_or_default();
                (task, history)
            } else {
                self.ensure_task_present(task_id).await?;
                let inner = self.inner.read().await;
                let task = inner
                    .tasks
                    .get(task_id)
                    .ok_or_else(|| SFError::TaskFailed {
                        task_id: task_id.into(),
                        reason: "Task not found".into(),
                    })?
                    .clone();
                let history = inner
                    .retry_history
                    .get(task_id)
                    .cloned()
                    .unwrap_or_default();
                (task, history)
            };

            let entry = DeadLetterEntry {
                original_task_id: task_id.into(),
                task,
                final_error: error,
                retry_history: history,
                enqueued_at: chrono::Utc::now(),
                suggested_action: SuggestedAction::ManualRetry,
            };

            dlq.enqueue(entry).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>> {
        self.cancel_task_with_reason(task_id, "Direct cancellation".into())
            .await
    }

    /// [`Self::cancel_task`] with a reason that says why, not just that.
    ///
    /// The caller that knows more than "someone cancelled it" — the sweep that
    /// finds a task no predecessor can ever release — has to be able to leave
    /// that in the row: the reason is the only trace the task keeps.
    pub async fn cancel_task_with_reason(
        &self,
        task_id: &str,
        reason: String,
    ) -> SFResult<Vec<String>> {
        if self.fg().is_some() {
            let cancelled = self.cancel_task_store_with(task_id, reason).await?;
            self.on_task_terminal(task_id).await;
            for dep_id in &cancelled {
                self.on_task_terminal(dep_id).await;
            }
            return Ok(cancelled);
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        task.status = TaskStatus::Cancelled;
        task.error = Some(reason.clone());

        let task_snapshot = task.clone();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskCancelled {
            task_id: task_id.into(),
            reason: reason.clone(),
            timestamp: chrono::Utc::now(),
        })
        .await;
        self.persist_task_fine_grained(&task_snapshot).await;

        let mut inner = self.inner.write().await;
        let mut cancelled = Vec::new();
        let downstream = Self::collect_downstream(&inner, task_id);
        for dep_id in downstream {
            if let Some(t) = inner.tasks.get_mut(&dep_id) {
                if t.status != TaskStatus::Cancelled
                    && t.status != TaskStatus::Failed
                    && t.status != TaskStatus::Completed
                {
                    t.status = TaskStatus::Cancelled;
                    t.error = Some(format!(
                        "Cascade cancelled: upstream task '{}' was cancelled",
                        task_id
                    ));
                    t.error_cause = None;
                    t.updated_at = chrono::Utc::now();
                    cancelled.push(dep_id.clone());
                    drop(inner);
                    self.emit_event(cog_core::TaskEvent::TaskCancelled {
                        task_id: dep_id.clone(),
                        reason: format!(
                            "Cascade cancelled: upstream task '{}' was cancelled",
                            task_id
                        ),
                        timestamp: chrono::Utc::now(),
                    })
                    .await;
                    let dep_snapshot = {
                        let inner = self.inner.read().await;
                        inner.tasks.get(&dep_id).cloned()
                    };
                    if let Some(ref t) = dep_snapshot {
                        self.persist_task_fine_grained(t).await;
                    }
                    inner = self.inner.write().await;
                }
            }
        }

        drop(inner);
        self.persist_state().await;
        self.on_task_terminal(task_id).await;
        for dep_id in &cancelled {
            self.on_task_terminal(dep_id).await;
        }
        Ok(cancelled)
    }

    pub async fn retry_task(&self, task_id: &str) -> SFResult<()> {
        if self.fg().is_some() {
            return self
                .store_transition(
                    task_id,
                    &[TaskStatus::Failed],
                    "Cannot retry task in {} state; only Failed tasks can be retried",
                    |t| {
                        t.status = TaskStatus::Pending;
                        t.retry_count = 0;
                        t.error = None;
                        t.error_cause = None;
                        t.clear_run();
                    },
                    Some(cog_core::TaskEvent::TaskRetried {
                        task_id: task_id.into(),
                        retry_count: 0,
                        timestamp: chrono::Utc::now(),
                    }),
                )
                .await;
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;

        // Clear retry history on manual retry first (avoids double mutable borrow)
        inner.retry_history.remove(task_id);

        let task = inner
            .tasks
            .get_mut(task_id)
            .ok_or_else(|| cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            })?;

        if task.status != TaskStatus::Failed {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!(
                    "Cannot retry task in {:?} state; only Failed tasks can be retried",
                    task.status
                ),
            });
        }

        task.status = TaskStatus::Pending;
        task.retry_count = 0;
        task.error = None;
        task.error_cause = None;
        task.clear_run();
        task.updated_at = chrono::Utc::now();
        let task_snapshot = task.clone();

        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskRetried {
            task_id: task_id.into(),
            retry_count: 0,
            timestamp: chrono::Utc::now(),
        })
        .await;
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn all_completed(&self) -> bool {
        let tasks = self.all_tasks_unified().await;
        !tasks.is_empty() && tasks.iter().all(|t| t.status == TaskStatus::Completed)
    }

    pub async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        if self.fg().is_some() {
            return self.check_timeouts_store().await;
        }
        let now = chrono::Utc::now();
        let reclaimable: Vec<(Task, ReclaimCause)> = {
            let inner = self.inner.read().await;
            inner
                .tasks
                .values()
                .filter_map(|t| reclaim_cause(t, self.task_lease, now).map(|c| (t.clone(), c)))
                .collect()
        };

        let mut results = Vec::new();
        for (task, cause) in reclaimable {
            let task_id = task.id.clone();

            // Re-verify under read lock before failing: the task may have been
            // completed by the executor between the scan above and now.
            let still_running = {
                let inner = self.inner.read().await;
                inner
                    .tasks
                    .get(&task_id)
                    .map(|t| t.status == TaskStatus::Running)
                    .unwrap_or(false)
            };
            if !still_running {
                tracing::info!(task_id = %task_id, "Task no longer Running, skipping reclaim");
                continue;
            }

            self.emit_reclaim(&task, &cause).await;

            // 该不该记账由类型说了算（见 `ReclaimCause::charges_retry_budget`）：
            // 不记账的回收不走通用的失败处理，那条路只剩预算一个判据。
            if !cause.charges_retry_budget() {
                let mut inner = self.inner.write().await;
                let Some(current) = inner.tasks.get_mut(&task_id) else {
                    continue;
                };
                if current.status != TaskStatus::Running {
                    continue;
                }
                let delay = self
                    .retry_matrix
                    .delay(&current.task_type, current.retry_count);
                *current = Self::requeued_without_charge(current, &cause.error(), None, delay);
                let snapshot = current.clone();
                drop(inner);
                self.persist_task_fine_grained(&snapshot).await;
                self.emit_event(cog_core::TaskEvent::TaskRetried {
                    task_id: task_id.clone(),
                    retry_count: snapshot.retry_count,
                    timestamp: chrono::Utc::now(),
                })
                .await;
                results.push((task_id, true, Vec::new(), false));
                continue;
            }

            if let Ok((retried, cancelled, dlq_pushed)) =
                self.fail_task(&task_id, cause.error(), None).await
            {
                if retried {
                    let mut inner = self.inner.write().await;
                    if let Some(t) = inner.tasks.get_mut(&task_id) {
                        t.clear_run();
                    }
                }
                results.push((task_id, retried, cancelled, dlq_pushed));
            }
        }

        results
    }

    /// 心跳：把本进程持有的活跃任务租约整体推后一次。只碰自己起的任务，所以
    /// 一个不执行任何任务的进程跑它等于空转查询。
    pub async fn renew_leases(&self) -> SFResult<usize> {
        let now = chrono::Utc::now();
        let lease = self.task_lease;
        let owner = self.run_id.clone();

        if let Some(be) = self.fg() {
            let all = be.dag_get_all_tasks(&self.workspace_id).await?;
            let mut renewed = 0usize;
            for mut task in all {
                if !task.renew_lease(&owner, lease, now) {
                    continue;
                }
                task.updated_at = now;
                // CAS 在 Running 上：本进程读与写之间任务可能已经结束，那时这次
                // 续期就该落空而不是把它写回 Running。
                if be
                    .dag_transition_task(
                        &self.workspace_id,
                        &task.id,
                        &[TaskStatus::Running],
                        &task,
                    )
                    .await
                    .is_ok()
                {
                    renewed += 1;
                }
            }
            return Ok(renewed);
        }

        let mut inner = self.inner.write().await;
        let mut renewed = 0usize;
        for task in inner.tasks.values_mut() {
            if task.renew_lease(&owner, lease, now) {
                task.updated_at = now;
                renewed += 1;
            }
        }
        Ok(renewed)
    }

    /// 为本进程持有的在跑任务落检查点，并把续跑指针挂到各自的板上。
    ///
    /// 枚举面与续期同源（租约在本进程名下的 `Running` 任务）：续期证明「这些任务
    /// 是我的」，而检查点写的是同一批任务——两处对「我的」的定义必须是同一个，
    /// 否则会给别人的任务写续跑点。
    ///
    /// 返回本轮真正写下的续跑点数。
    pub async fn checkpoint_owned_tasks(&self, agents: &dyn CheckpointAgents) -> usize {
        let Some(state) = self.state_backend.clone() else {
            tracing::debug!(
                "no state backend attached: a checkpoint would have nowhere to hang its resume pointer"
            );
            return 0;
        };
        let mut saved = 0usize;
        for task_id in self.owned_running_tasks().await {
            // 每个动作都带上单步等待上限：一个卡死的 agent 快照或存储调用只
            // 计一次 `failed`，其余 agent/任务照常落检查点，毒不倒整轮。
            // spawn 处另有随节拍推导、并向存活体系申报的整轮预算兜底。
            let round =
                checkpoint_task_with_timeout(&task_id, agents, &state, CHECKPOINT_STEP_TIMEOUT)
                    .await;
            saved += round.saved;
            self.record_task_checkpoint(&round).await;
        }
        saved
    }

    /// 本进程持有的在跑任务 id。
    ///
    /// 持有者就是本进程的运行身份（`run_id`），没有第二份注册表：进程消亡时它
    /// 随之失效，这正是「这一份进度已经没人管了」可以被读出来的方式。
    async fn owned_running_tasks(&self) -> Vec<String> {
        let owner = self.run_id.as_str();
        let ours = |task: &Task| {
            task.status == TaskStatus::Running && task.lease_owner.as_deref() == Some(owner)
        };
        if let Some(be) = self.fg() {
            return be
                .dag_get_all_tasks(&self.workspace_id)
                .await
                .map(|all| {
                    all.iter()
                        .filter(|t| ours(t))
                        .map(|t| t.id.clone())
                        .collect()
                })
                .unwrap_or_default();
        }
        let inner = self.inner.read().await;
        inner
            .tasks
            .values()
            .filter(|t| ours(t))
            .map(|t| t.id.clone())
            .collect()
    }

    /// 产出侧自己的读数。四个量各记各的（标签 `outcome`），失败不与成功共用
    /// 同一个计数器——那正是「修好了」与「没修好」分不开的方式。
    async fn record_task_checkpoint(&self, round: &CheckpointRound) {
        // 静默轮也记：「这一轮没有要写的」与「这个任务没有产出方」是两件事，
        // 只在有 agent 时计数的话，第二种情况在读数上不存在。
        let backend = self.metrics_backend();
        let Some(backend) = backend else {
            tracing::debug!(
                saved = round.saved,
                unpersisted = round.unpersisted,
                failed = round.failed,
                "task checkpoints written; no metrics backend attached"
            );
            return;
        };
        for (outcome, value) in task_checkpoint::outcome_counts(round) {
            if value == 0.0 {
                continue;
            }
            self.write_checkpoint_outcome(&backend, outcome, value)
                .await;
        }
    }

    /// 把产出侧的结局词表按零发布一遍。
    ///
    /// 产出侧的格子只在真有任务在跑时才会有值：一个手上没有在跑任务的进程，每一轮
    /// 什么都不写，于是这个计数器**整条**都查不到——「产出侧接上了、此刻没有工作」
    /// 与「产出侧从来没接上」在读数上同形，而这条链失败的样子正是这样（任务从头
    /// 跑，没有错误、没有日志、没有规则）。恢复端对同一件事就是这么办的：两个结局
    /// 都发布、零也发布（见 `cog-collaboration` 的 `RESUME_OUTCOMES`）。这里照同一
    /// 套，且只在循环起手时做一次：词表落地，值由各轮去加。
    pub async fn publish_checkpoint_outcomes(&self) {
        let Some(backend) = self.metrics_backend() else {
            tracing::debug!("task checkpoint outcomes have no metrics backend to land on");
            return;
        };
        for outcome in task_checkpoint::CHECKPOINT_OUTCOMES {
            self.write_checkpoint_outcome(&backend, outcome, 0.0).await;
        }
    }

    /// 产出侧读数的一格：一个计数器，一格一个 `outcome`。
    async fn write_checkpoint_outcome(
        &self,
        backend: &Arc<dyn cog_core::MetricsBackend>,
        outcome: &str,
        value: f64,
    ) {
        let labels = HashMap::from([(crate::LABEL_OUTCOME.to_string(), outcome.to_string())]);
        if let Err(e) = backend
            .record_counter(cog_core::metric_names::TASK_CHECKPOINT, value, labels)
            .await
        {
            tracing::warn!(error = %e, outcome, "cannot record the task checkpoint count");
        }
    }

    async fn emit_reclaim(&self, task: &Task, cause: &ReclaimCause) {
        let event = match cause {
            ReclaimCause::LeaseExpired { owner, expired_at } => {
                cog_core::TaskEvent::TaskLeaseExpired {
                    task_id: task.id.clone(),
                    owner: owner.clone(),
                    expired_at: *expired_at,
                    timestamp: chrono::Utc::now(),
                }
            }
            ReclaimCause::OverBudget { timeout_seconds } => cog_core::TaskEvent::TaskTimeout {
                task_id: task.id.clone(),
                timeout_seconds: *timeout_seconds,
                timestamp: chrono::Utc::now(),
            },
        };
        self.emit_event(event).await;
    }

    pub async fn get_task(&self, task_id: &str) -> Option<Task> {
        if let Some(be) = self.fg() {
            return be
                .dag_get_task(&self.workspace_id, task_id)
                .await
                .ok()
                .flatten();
        }
        self.ensure_task_present(task_id).await.ok()?;
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).cloned()
    }

    pub async fn get_all_tasks(&self) -> Vec<Task> {
        self.all_tasks_unified().await
    }

    pub async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>> {
        if let Some(be) = self.fg() {
            let ids = be
                .dag_get_dependents(&self.workspace_id, task_id)
                .await
                .ok()?;
            let mut out = Vec::with_capacity(ids.len());
            for id in ids {
                if let Ok(Some(t)) = be.dag_get_task(&self.workspace_id, &id).await {
                    out.push(t);
                }
            }
            return Some(out);
        }
        let inner = self.inner.read().await;
        inner.dependents.get(task_id).map(|ids| {
            ids.iter()
                .filter_map(|id| inner.tasks.get(id).cloned())
                .collect()
        })
    }

    pub async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>> {
        if self.fg().is_some() {
            let t = self.store_task(task_id).await.ok()?;
            let tasks = self.all_tasks_unified().await;
            let by_id: std::collections::HashMap<&str, &Task> =
                tasks.iter().map(|t| (t.id.as_str(), t)).collect();
            return Some(
                t.blocked_by
                    .iter()
                    .filter_map(|id| by_id.get(id.as_str()).map(|t| (*t).clone()))
                    .collect(),
            );
        }
        let inner = self.inner.read().await;
        inner.dependencies.get(task_id).map(|ids| {
            ids.iter()
                .filter_map(|id| inner.tasks.get(id).cloned())
                .collect()
        })
    }

    pub async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        let tasks = self.all_tasks_unified().await;
        let mut edges = Vec::new();
        for t in &tasks {
            for dep_id in &t.blocked_by {
                edges.push((dep_id.clone(), t.id.clone()));
            }
        }
        (tasks, edges)
    }

    pub async fn delete_task(&self, task_id: &str) -> SFResult<()> {
        if let Some(be) = self.fg() {
            self.store_task(task_id).await?;
            return be.dag_remove_task(&self.workspace_id, task_id).await;
        }
        self.ensure_task_present(task_id).await?;
        let mut inner = self.inner.write().await;
        if !inner.tasks.contains_key(task_id) {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: "Task not found".into(),
            });
        }

        // Remove this task from dependents of its dependencies
        if let Some(deps) = inner.dependencies.get(task_id) {
            for dep_id in deps.clone() {
                if let Some(dependents) = inner.dependents.get_mut(&dep_id) {
                    dependents.remove(task_id);
                }
            }
        }

        // Remove this task from dependencies of its dependents
        if let Some(dependents) = inner.dependents.get(task_id) {
            for dependent_id in dependents.clone() {
                if let Some(deps) = inner.dependencies.get_mut(&dependent_id) {
                    deps.remove(task_id);
                }
            }
        }

        inner.tasks.remove(task_id);
        inner.dependencies.remove(task_id);
        inner.dependents.remove(task_id);
        inner.retry_history.remove(task_id);

        if let Some(ref backend) = self.state_backend {
            let workspace_id = self.workspace_id.clone();
            let task_id = task_id.to_string();
            let backend = backend.clone();
            tokio::spawn(async move {
                if let Err(e) = backend.dag_remove_task(&workspace_id, &task_id).await {
                    tracing::warn!("dag_remove_task failed for {}: {}", task_id, e);
                }
            });
        }

        Ok(())
    }

    /// Get retry history for a task.
    pub async fn get_retry_history(&self, task_id: &str) -> Option<Vec<RetryAttempt>> {
        if let Some(be) = self.fg() {
            return be
                .dag_get_retry_history(&self.workspace_id, task_id)
                .await
                .ok();
        }
        let inner = self.inner.read().await;
        inner.retry_history.get(task_id).cloned()
    }

    /// Crew-level AND semantics hook.
    /// When one task in a crew (squad) enters the DLQ, the crew may
    /// trigger a retry rather than immediately failing.  This method
    /// returns `true` if *any* task in the given set is still
    /// retryable (has not exhausted its retries).
    pub async fn crew_can_retry(&self, task_ids: &[String]) -> bool {
        let tasks = self.all_tasks_unified().await;
        let by_id: std::collections::HashMap<&str, &Task> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        task_ids.iter().any(|id| {
            by_id.get(id.as_str()).is_some_and(|t| {
                let max = self.retry_matrix.max_retries(&t.task_type);
                t.retry_count < max && t.status != TaskStatus::Failed
            })
        })
    }

    /// Retry all failed tasks in a crew.  Returns the number of tasks
    /// that were retried.
    pub async fn crew_retry_all(&self, task_ids: &[String]) -> usize {
        let mut retried = 0;
        for id in task_ids {
            if let Ok(()) = self.retry_task(id).await {
                retried += 1;
            }
        }
        retried
    }

    pub async fn dlq_len(&self) -> SFResult<usize> {
        match self.dlq {
            Some(ref dlq) => dlq.len().await,
            None => Ok(0),
        }
    }

    pub async fn replay_dlq(&self, task_id: &str) -> SFResult<bool> {
        match self.dlq {
            Some(ref dlq) => match dlq.replay(task_id).await {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(e) => Err(e),
            },
            None => Ok(false),
        }
    }
}

use async_trait::async_trait;

/// Whether `held` may stand for `incoming` when the id is already in the graph:
/// a redelivery of the same piece of work, or a verdict row written over an id
/// the caller already knows.
///
/// Only the submission is compared. Status, attempts, lease and timestamps all
/// move on their own after the row lands, so a replay days later still reads as
/// the same task; what a redelivery must reproduce is what was asked for.
///
/// A Failed or Cancelled row is a verdict, not work: the failure path
/// re-injects the plan's own rows with that status to record why they will not
/// run. It starts nothing and releases nothing, so a held id is the expected
/// outcome there even though its payload is not the payload of the held row.
fn may_be_skipped(held: &Task, incoming: &Task) -> bool {
    if matches!(incoming.status, TaskStatus::Failed | TaskStatus::Cancelled) {
        return true;
    }
    held.task_type == incoming.task_type && held.input == incoming.input
}

/// What the held row is missing that this submission carries.
///
/// A batch is idempotent by id: an id the graph already holds is dropped rather
/// than written. That is right for a redelivery of the same work and wrong for a
/// row that was persisted first and promoted second -- a caller that wanted a
/// row to read back before submitting it, and then submits a row carrying the
/// goal, the parent, the answer's carrier and the planner's verdict. The row's
/// existence is exactly the reason the promotion does not run, so the second
/// phase silently does nothing and the row stays half-written: it answers
/// `tasks/get`, it just never says which goal it serves or where its answer
/// lands. Nothing downstream can tell that apart from a submission whose
/// promotion is still in flight.
///
/// Only the fields a caller can fill *after* the row exists are listed, and only
/// to fill an absence: a held row that already carries a value keeps it, so a
/// re-drive of a decomposition cannot overwrite where the last one put the
/// answer. Writing over an existing value is a different decision -- who owns
/// this row -- and it is not made here.
fn missing_enrichment(held: &Task, incoming: &Task) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if held.goal_id.is_none() && incoming.goal_id.is_some() {
        missing.push("goal_id");
    }
    if held.parent_task_id.is_none() && incoming.parent_task_id.is_some() {
        missing.push("parent_task_id");
    }
    if held.sink_task_id.is_none() && incoming.sink_task_id.is_some() {
        missing.push("sink_task_id");
    }
    if held.action_planner_meta.is_none() && incoming.action_planner_meta.is_some() {
        missing.push("action_planner_meta");
    }
    if held.face_id.is_none() && incoming.face_id.is_some() {
        missing.push("face_id");
    }
    if held.owner_unit.is_none() && incoming.owner_unit.is_some() {
        missing.push("owner_unit");
    }
    if held.bound_by_handover_id.is_none() && incoming.bound_by_handover_id.is_some() {
        missing.push("bound_by_handover_id");
    }
    if held.acceptance.is_empty() && !incoming.acceptance.is_empty() {
        missing.push("acceptance");
    }
    // One direction only. A promotion may turn a row into a placeholder -- it
    // is the row the goal hangs under, and the row `sink_task_id` names answers
    // for it -- but never the other way round: making a placeholder executable
    // would put a row whose `input` is the goal text, not an instruction, in
    // front of an executor.
    if held.is_executable && !incoming.is_executable {
        missing.push("is_executable");
    }
    missing
}

/// Fill what the held row lacks from the submission that carried it. Absences
/// only -- see [`missing_enrichment`] for why this never overwrites.
fn carry_enrichment(held: &mut Task, incoming: &Task) {
    if held.goal_id.is_none() {
        held.goal_id = incoming.goal_id.clone();
    }
    if held.parent_task_id.is_none() {
        held.parent_task_id = incoming.parent_task_id.clone();
    }
    if held.sink_task_id.is_none() {
        held.sink_task_id = incoming.sink_task_id.clone();
    }
    if held.action_planner_meta.is_none() {
        held.action_planner_meta = incoming.action_planner_meta.clone();
    }
    if held.face_id.is_none() {
        held.face_id = incoming.face_id.clone();
    }
    if held.owner_unit.is_none() {
        held.owner_unit = incoming.owner_unit.clone();
    }
    if held.bound_by_handover_id.is_none() {
        held.bound_by_handover_id = incoming.bound_by_handover_id.clone();
    }
    if held.acceptance.is_empty() {
        held.acceptance = incoming.acceptance.clone();
    }
    if !incoming.is_executable {
        held.is_executable = false;
    }
    // The row did change, so its timestamp has to move with it: a reader that
    // sorts by `updated_at` would otherwise show a promoted row as untouched
    // since the day it was persisted.
    held.updated_at = chrono::Utc::now();
}

/// One line per reused id: whose row holds it and what state that row is in.
/// The first question a reader has is which plan this submission would have
/// started depending on.
fn reused_id_note(held: &Task, incoming: &Task) -> String {
    format!(
        "'{}' is held by goal {} ({}) while this submission belongs to goal {}",
        incoming.id,
        held.goal_id.as_deref().unwrap_or("unknown"),
        held.status.as_str(),
        incoming.goal_id.as_deref().unwrap_or("unknown"),
    )
}

/// The refusal a batch gets when any id in it was reused for different work.
/// The whole batch is refused rather than the offending rows skipped: the rest
/// of the plan names those ids as predecessors, so queueing it would attach it
/// to rows that settle for unrelated reasons — or never settle, which is how
/// seven rows sat Pending for fifteen hours.
fn reused_ids_refusal(reused: &[String]) -> cog_core::SFError {
    cog_core::SFError::Validation(format!(
        "refusing the submission: task ids reused for different work — {}. \
         Skipping them would leave the rest of the plan waiting on the held rows",
        reused.join("; ")
    ))
}

/// 在依赖图（task → blocked_by 集合）上 DFS 检测环，两种模式共用。
fn detect_cycle_graph(deps: &HashMap<String, HashSet<String>>) -> Option<Vec<String>> {
    fn dfs(
        node: &str,
        deps: &HashMap<String, HashSet<String>>,
        visited: &mut HashSet<String>,
        stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(node.to_string());
        stack.insert(node.to_string());
        path.push(node.to_string());

        if let Some(children) = deps.get(node) {
            for dep in children {
                if !visited.contains(dep) {
                    if let Some(cycle) = dfs(dep, deps, visited, stack, path) {
                        return Some(cycle);
                    }
                } else if stack.contains(dep) {
                    let idx = path.iter().position(|p| p == dep).unwrap_or(0);
                    return Some(path[idx..].to_vec());
                }
            }
        }

        path.pop();
        stack.remove(node);
        None
    }

    let mut visited = HashSet::new();
    let mut stack = HashSet::new();
    let mut path = Vec::new();
    for task_id in deps.keys() {
        if !visited.contains(task_id) {
            if let Some(cycle) = dfs(task_id, deps, &mut visited, &mut stack, &mut path) {
                return Some(cycle);
            }
        }
    }
    None
}

#[async_trait]
impl cog_core::DagExecutor for DagExecutor {
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()> {
        self.submit_goal(goal, tasks).await
    }

    async fn add_tasks_batch(&self, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        self.add_tasks_batch(tasks).await
    }

    async fn add_task(&self, task: Task) -> SFResult<()> {
        self.add_task(task).await
    }

    async fn replace_task(&self, task: Task) -> SFResult<()> {
        self.replace_task(task).await
    }

    async fn schedule_task(&self, task_id: &str) -> SFResult<()> {
        self.schedule_task(task_id).await
    }

    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()> {
        self.assign_task(task_id, agent_id).await
    }

    async fn start_task(&self, task_id: &str) -> SFResult<()> {
        self.start_task(task_id).await
    }

    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        self.complete_task(task_id, result).await
    }

    async fn fail_task(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        self.fail_task(task_id, error, cause).await
    }

    async fn fail_task_after(
        &self,
        task_id: &str,
        error: String,
        cause: Option<UpstreamFailure>,
        retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        // 写全类型名指向固有方法：同名 trait 方法就在这里，靠方法解析的优先级
        // 去区分两者，读的人看不出走的是哪一条。
        DagExecutor::fail_task_after(self, task_id, error, cause, retry_after_secs).await
    }

    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>> {
        self.cancel_task(task_id).await
    }

    async fn retry_task(&self, task_id: &str) -> SFResult<()> {
        self.retry_task(task_id).await
    }

    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool> {
        self.push_to_dlq(task_id, error).await
    }

    async fn dlq_len(&self) -> SFResult<usize> {
        self.dlq_len().await
    }

    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool> {
        self.replay_dlq(task_id).await
    }

    async fn find_ready_tasks(&self) -> Vec<Task> {
        self.find_ready_tasks().await
    }

    async fn get_ready_tasks(&self) -> Vec<Task> {
        self.get_ready_tasks().await
    }

    async fn get_all_tasks(&self) -> Vec<Task> {
        self.get_all_tasks().await
    }

    async fn get_task(&self, task_id: &str) -> Option<Task> {
        self.get_task(task_id).await
    }

    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>> {
        self.get_dependents(task_id).await
    }

    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>> {
        self.get_dependencies(task_id).await
    }

    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        self.get_graph().await
    }

    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        self.check_timeouts().await
    }

    async fn delete_task(&self, task_id: &str) -> SFResult<()> {
        self.delete_task(task_id).await
    }

    async fn all_completed(&self) -> bool {
        self.all_completed().await
    }

    async fn crew_can_retry(&self, task_ids: &[String]) -> bool {
        self.crew_can_retry(task_ids).await
    }

    async fn crew_retry_all(&self, task_ids: &[String]) -> usize {
        self.crew_retry_all(task_ids).await
    }
}

/// Say which kinds have pending work the dispatcher is holding for want of a
/// criterion, once per process per kind.
///
/// The scan runs on every poll, and a held row stays held, so the log line has
/// to be an event rather than a level: repeating it every poll trains the
/// reader to skip it, which is the same as not logging at all. Once per kind is
/// what makes it actionable -- the reader learns the kind, fixes its producer or
/// adds it to a settle pass, and the next deploy says whether that worked.
fn name_unclosable_kinds(unclosable: &std::collections::BTreeMap<String, usize>) {
    static NAMED: std::sync::OnceLock<std::sync::Mutex<HashSet<String>>> =
        std::sync::OnceLock::new();
    let named = NAMED.get_or_init(|| std::sync::Mutex::new(HashSet::new()));
    let Ok(mut named) = named.lock() else {
        return;
    };
    for (kind, rows) in unclosable {
        if named.insert(kind.clone()) {
            tracing::warn!(
                kind,
                rows,
                "pending work is not dispatched because it states no acceptance criterion; the \
                 rows stay on the board until their producer or a settle pass gives them one"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{
        AgentState, ContextBoard, Event, MetricsBackend, StateBackend, TaskCheckpoint, TaskType,
    };

    /// 本部署读哪些就绪队列——清扫器的范围判据，与运行期同一个函数。
    fn served(workspace: &str, executor_role: bool) -> Vec<String> {
        crate::ready_queue::consumed_ready_streams(workspace, executor_role)
    }

    /// 一次清扫，带着各队列的未投递读数——即运行期问传输层拿到的那张表。
    async fn sweep_with(
        dag: &DagExecutor,
        stall_after_secs: u64,
        streams: &[String],
        undelivered: &HashMap<String, Option<u64>>,
    ) -> usize {
        dag.reclaim_stalled_scheduled(stall_after_secs, streams, undelivered, 3, false)
            .await
    }

    /// 传输层答不出积压时的清扫：大多数测试只关心「超过窗就收回」这一条。
    async fn sweep(dag: &DagExecutor, stall_after_secs: u64, streams: &[String]) -> usize {
        sweep_with(dag, stall_after_secs, streams, &HashMap::new()).await
    }

    /// 一条自我进化任务，落进过程内队列——与现网那批 `change-ledger-*` 同形。
    fn evolution_task(id: &str) -> Task {
        Task::new(
            id,
            TaskType::Generator,
            serde_json::json!({ "evolution_mode": "generate_change" }),
        )
    }

    /// 欠账计数只算「有人可能正在花这个窗口」的行。
    ///
    /// 三个 Scheduled 形状放进同一张图里，`owed` 必须只把后两个算进去：
    /// 租约过期的那条，持有者已经没了、消息不是被吞了就是被 ack 掉了，它正等着
    /// 清扫器把它放回队列，等着的行不花任何窗口的额度。
    #[tokio::test]
    async fn a_scheduled_row_nobody_holds_is_not_owed() {
        let pod = DagExecutor::new("ws-owed".into());
        let local = crate::ready_queue::process_local_ready_stream("ws-owed");

        let mut abandoned = evolution_task("abandoned");
        abandoned.status = TaskStatus::Scheduled;
        abandoned.lease_owner = Some("gone".into());
        abandoned.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::hours(3));
        pod.add_task(abandoned).await.unwrap();

        let mut held = evolution_task("held");
        held.status = TaskStatus::Scheduled;
        held.lease_owner = Some("here".into());
        held.lease_expires_at = Some(chrono::Utc::now() + chrono::Duration::minutes(5));
        pod.add_task(held).await.unwrap();

        let mut unclaimed = evolution_task("unclaimed");
        unclaimed.status = TaskStatus::Scheduled;
        pod.add_task(unclaimed).await.unwrap();

        let (_, owed) = pod.ready_tasks_and_in_flight().await;
        assert_eq!(
            owed.get(&local).copied().unwrap_or(0),
            2,
            "a lease that lapsed three hours ago must not be subtracted from the \
             next window the pool opens: {owed:?}"
        );
    }

    /// 记下每一次计数的序列名：断言的是「这条曲线被记过没有」，不是内部原子。
    #[derive(Default)]
    struct RecordingMetrics(std::sync::Mutex<Vec<&'static str>>);

    impl RecordingMetrics {
        fn recorded(&self, name: cog_core::MetricName) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|recorded| **recorded == name.as_str())
                .count()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for RecordingMetrics {
        async fn record_gauge(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            self.0.lock().unwrap().push(name.as_str());
            Ok(())
        }
        async fn record_histogram(
            &self,
            _name: cog_core::MetricName,
            _value: f64,
            _labels: HashMap<String, String>,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(&self, _name: &str) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> SFResult<()> {
            Ok(())
        }
    }

    /// 只实现 DAG 快照存取的最小 StateBackend，模拟两个 pod 共享的存储。
    #[derive(Default)]
    struct StubStateBackend {
        snapshots: tokio::sync::Mutex<HashMap<String, serde_json::Value>>,
    }

    #[async_trait::async_trait]
    impl StateBackend for StubStateBackend {
        async fn get_agent_state(&self, _agent_id: &str) -> SFResult<Option<AgentState>> {
            Ok(None)
        }
        async fn set_agent_state(&self, _agent_id: &str, _state: &AgentState) -> SFResult<()> {
            Ok(())
        }
        async fn cas_agent_state(
            &self,
            _agent_id: &str,
            _expected: &AgentState,
            _new: &AgentState,
        ) -> SFResult<bool> {
            Ok(true)
        }
        async fn get_checkpoint(&self, _task_id: &str) -> SFResult<Option<TaskCheckpoint>> {
            Ok(None)
        }
        async fn save_checkpoint(&self, _checkpoint: &TaskCheckpoint) -> SFResult<()> {
            Ok(())
        }
        async fn append_event(&self, _task_id: &str, _event: &Event) -> SFResult<u64> {
            Ok(0)
        }
        async fn get_events(
            &self,
            _task_id: &str,
            _offset: u64,
            _limit: usize,
        ) -> SFResult<Vec<Event>> {
            Ok(Vec::new())
        }
        async fn get_board(&self, _task_id: &str) -> SFResult<Option<ContextBoard>> {
            Ok(None)
        }
        async fn set_board_field(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> SFResult<()> {
            Ok(())
        }
        async fn delete_checkpoint(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn delete_board(&self, _task_id: &str) -> SFResult<()> {
            Ok(())
        }
        async fn remove_board_field(&self, _task_id: &str, _field: &str) -> SFResult<()> {
            Ok(())
        }
        async fn save_dag_state(
            &self,
            workspace_id: &str,
            state: &serde_json::Value,
        ) -> SFResult<()> {
            self.snapshots
                .lock()
                .await
                .insert(workspace_id.to_string(), state.clone());
            Ok(())
        }
        async fn load_dag_state(&self, workspace_id: &str) -> SFResult<Option<serde_json::Value>> {
            Ok(self.snapshots.lock().await.get(workspace_id).cloned())
        }
    }

    /// 多副本读穿：pod B 内存没有该任务（没走 load_from_backend 启动恢复），
    /// get_task/状态迁移必须从共享快照回填而不是报 "Task not found"。
    #[tokio::test]
    async fn test_read_through_recovers_task_from_shared_snapshot() {
        let backend = Arc::new(StubStateBackend::default());

        let pod_a = DagExecutor::new("ws-rt".into()).with_state_backend(backend.clone());
        let task = Task::new("t-rt-1", TaskType::Generator, serde_json::json!({}));
        let task_id = task.id.clone();
        pod_a.add_task(task).await.unwrap();
        // do_persist 是 tokio::spawn 的异步落盘，等它写完
        for _ in 0..100 {
            if backend.snapshots.lock().await.contains_key("ws-rt") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(backend.snapshots.lock().await.contains_key("ws-rt"));

        // pod B：同一共享存储、全新内存，故意不调 load_from_backend
        let pod_b = DagExecutor::new("ws-rt".into()).with_state_backend(backend);
        assert!(pod_b.inner.read().await.tasks.is_empty());

        let found = pod_b.get_task(&task_id).await;
        assert!(found.is_some(), "read-through should hydrate from snapshot");
        assert!(pod_b.inner.read().await.tasks.contains_key(&task_id));

        // 状态迁移同样读穿
        pod_b.schedule_task(&task_id).await.unwrap();
        assert_eq!(
            pod_b.get_task(&task_id).await.unwrap().status,
            TaskStatus::Scheduled
        );
    }

    /// 无共享存储时行为不变：仍然是 "Task not found"。
    #[tokio::test]
    async fn test_missing_task_without_backend_still_errors() {
        let pod = DagExecutor::new("ws-solo".into());
        let err = pod.schedule_task("nope").await.unwrap_err();
        assert!(err.to_string().contains("Task not found"));
    }

    /// 终态双副本：两个 DagExecutor 共享一个支持细粒度 dag_* 的后端，
    /// 模拟双 pod —— 内存不再权威，所有读写直落共享存储。
    fn fg_pods() -> (DagExecutor, DagExecutor) {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        (
            DagExecutor::new("ws-fg".into()).with_state_backend(backend.clone()),
            DagExecutor::new("ws-fg".into()).with_state_backend(backend),
        )
    }

    fn chain(ids: &[&str]) -> Vec<Task> {
        ids.iter()
            .enumerate()
            .map(|(i, id)| {
                let mut t = Task::new(*id, TaskType::Generator, serde_json::json!({}));
                if i > 0 {
                    t.blocked_by = vec![ids[i - 1].to_string()];
                }
                t
            })
            .collect()
    }

    /// 幂等的边界是「同一份工作」，不是「同一个 id」：同一任务重投（类型与输入
    /// 逐字节相同）必须被吞掉，而拿旧 id 装新活要把整份提交判退——否则其余各行的
    /// `blocked_by` 会挂到别人的行上。
    #[tokio::test]
    async fn a_redelivery_of_the_same_task_is_still_idempotent() {
        let dag = DagExecutor::new("ws-replay".into());
        let task = Task::new("t1", TaskType::Generator, serde_json::json!({"goal": "a"}));
        assert_eq!(
            dag.add_tasks_batch(vec![task.clone()]).await.unwrap(),
            vec!["t1".to_string()]
        );
        let queued = dag.add_tasks_batch(vec![task]).await.unwrap();
        assert!(queued.is_empty(), "a replay queues nothing: {queued:?}");
        assert_eq!(dag.get_all_tasks().await.len(), 1);
    }

    #[tokio::test]
    async fn an_id_reused_for_different_work_refuses_the_whole_batch() {
        let dag = DagExecutor::new("ws-reuse".into());
        dag.add_tasks_batch(vec![Task::new(
            "t1",
            TaskType::Generator,
            serde_json::json!({"goal": "old"}),
        )])
        .await
        .unwrap();

        let mut dependent = Task::new(
            "t5",
            TaskType::Generator,
            serde_json::json!({"goal": "new"}),
        );
        dependent.blocked_by = vec!["t1".into()];
        let err = dag
            .add_tasks_batch(vec![
                Task::new(
                    "t1",
                    TaskType::Generator,
                    serde_json::json!({"goal": "new"}),
                ),
                dependent,
            ])
            .await
            .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("reused for different work"), "{text}");
        assert!(text.contains("'t1'"), "{text}");
        // 整份判退：t5 也没有进图，否则它会挂到一条与它无关的前驱上。
        assert!(dag.get_task("t5").await.is_none());
        let held = dag.get_task("t1").await.unwrap();
        assert_eq!(
            held.input,
            serde_json::json!({"goal": "old"}),
            "the held row is untouched"
        );
    }

    #[tokio::test]
    async fn the_store_path_refuses_an_id_reused_for_different_work() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-reuse-fg".into()).with_state_backend(backend);
        dag.add_tasks_batch(vec![Task::new(
            "t1",
            TaskType::Generator,
            serde_json::json!({"goal": "old"}),
        )])
        .await
        .unwrap();
        let mut dependent = Task::new(
            "t5",
            TaskType::Generator,
            serde_json::json!({"goal": "new"}),
        );
        dependent.blocked_by = vec!["t1".into()];
        let err = dag
            .add_tasks_batch(vec![
                Task::new(
                    "t1",
                    TaskType::Generator,
                    serde_json::json!({"goal": "new"}),
                ),
                dependent,
            ])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("reused for different work"),
            "{err}"
        );
        assert_eq!(dag.get_all_tasks().await.len(), 1);
        assert!(dag.get_task("t5").await.is_none());
    }

    /// 没有依赖边的批次照旧幂等跳过：一个面自己的任务、一条意图的单条重投都用稳定
    /// id，但没有边会挂到被占的那一行上，判退会把本来要跳过的工作挡死。
    #[tokio::test]
    async fn an_independent_id_reuse_is_still_skipped() {
        let dag = DagExecutor::new("ws-reuse-independent".into());
        dag.add_tasks_batch(vec![Task::new(
            "face:release",
            TaskType::Generator,
            serde_json::json!({"goal": "old text"}),
        )])
        .await
        .unwrap();
        let queued = dag
            .add_tasks_batch(vec![Task::new(
                "face:release",
                TaskType::Generator,
                serde_json::json!({"goal": "edited text"}),
            )])
            .await
            .unwrap();
        assert!(queued.is_empty(), "the held id is skipped: {queued:?}");
    }

    /// 判退本身也要落得下去：失败路径把自己的行以 Failed 重投，id 必然被占，
    /// 而那一行不是新工作——它不启动任何东西，也不挡任何东西。
    #[tokio::test]
    async fn a_failed_verdict_row_is_held_rather_than_refused() {
        let dag = DagExecutor::new("ws-verdict".into());
        dag.add_tasks_batch(vec![Task::new(
            "t1",
            TaskType::Generator,
            serde_json::json!({"goal": "old"}),
        )])
        .await
        .unwrap();
        let mut verdict = Task::new(
            "t1",
            TaskType::Generator,
            serde_json::json!({"goal": "new"}),
        );
        verdict.status = TaskStatus::Failed;
        let queued = dag.add_tasks_batch(vec![verdict]).await.unwrap();
        assert!(queued.is_empty(), "the verdict is held: {queued:?}");
    }

    /// A row persisted before it is promoted: the second submission carries the
    /// goal, the parent and the row that answers for it, and the id is already
    /// in the graph. Skipping it because "the row is already there" leaves the
    /// row exactly as empty as the pre-placed one — it answers `tasks/get` and
    /// never says which goal it serves or where its answer lands, which reads
    /// downstream as work still in flight.
    ///
    /// principle: feedback_an_idempotent_batch_skips_the_row_that_was_supposed_to_carry_the_new_fields
    #[tokio::test]
    async fn a_row_persisted_before_its_promotion_is_promoted_rather_than_skipped() {
        let dag = DagExecutor::new("ws-promote".into());
        let pre_placed = Task::new(
            "goal-1",
            TaskType::Generator,
            serde_json::json!({"goal": "pay the invoices"}),
        );
        dag.add_tasks_batch(vec![pre_placed]).await.unwrap();

        let mut promoted = Task::new(
            "goal-1",
            TaskType::Generator,
            serde_json::json!({"goal": "pay the invoices"}),
        );
        promoted.goal_id = Some("g-1".into());
        promoted.sink_task_id = Some("sink-1".into());
        promoted.is_executable = false;
        let queued = dag.add_tasks_batch(vec![promoted]).await.unwrap();
        assert!(queued.is_empty(), "nothing new is queued: {queued:?}");

        let held = dag
            .get_task("goal-1")
            .await
            .expect("the row is still there");
        assert_eq!(
            held.goal_id.as_deref(),
            Some("g-1"),
            "the promotion's goal has to reach the row it was written for"
        );
        assert_eq!(
            held.sink_task_id.as_deref(),
            Some("sink-1"),
            "without the carrier declaration the row never publishes an answer"
        );
        assert!(
            !held.is_executable,
            "the promotion also says this row is a placeholder, not work"
        );
        assert_eq!(
            dag.get_all_tasks().await.len(),
            1,
            "the promotion writes the held row instead of adding a second one"
        );
    }

    /// The same on the store path, where "the row is already there" is a read of
    /// the backend rather than of memory — the path every pod actually takes.
    #[tokio::test]
    async fn the_store_path_promotes_a_pre_placed_row_too() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-promote-fg".into()).with_state_backend(backend);
        dag.add_tasks_batch(vec![Task::new(
            "goal-2",
            TaskType::Generator,
            serde_json::json!({"goal": "close the books"}),
        )])
        .await
        .unwrap();

        let mut promoted = Task::new(
            "goal-2",
            TaskType::Generator,
            serde_json::json!({"goal": "close the books"}),
        );
        promoted.goal_id = Some("g-2".into());
        promoted.sink_task_id = Some("sink-2".into());
        dag.add_tasks_batch(vec![promoted]).await.unwrap();

        let held = dag
            .get_task("goal-2")
            .await
            .expect("the row is still there");
        assert_eq!(held.goal_id.as_deref(), Some("g-2"));
        assert_eq!(held.sink_task_id.as_deref(), Some("sink-2"));
        assert_eq!(dag.get_all_tasks().await.len(), 1, "no second row appears");
    }

    /// The other half, so the fix cannot be "always overwrite": a re-drive that
    /// names a different carrier must not move where the first answer went.
    #[tokio::test]
    async fn a_promotion_never_moves_a_value_the_row_already_carries() {
        let dag = DagExecutor::new("ws-promote-keep".into());
        let mut first = Task::new(
            "goal-3",
            TaskType::Generator,
            serde_json::json!({"goal": "ship it"}),
        );
        first.goal_id = Some("g-3".into());
        first.sink_task_id = Some("sink-first".into());
        dag.add_tasks_batch(vec![first]).await.unwrap();

        let mut again = Task::new(
            "goal-3",
            TaskType::Generator,
            serde_json::json!({"goal": "ship it"}),
        );
        again.goal_id = Some("g-3".into());
        again.sink_task_id = Some("sink-second".into());
        dag.add_tasks_batch(vec![again]).await.unwrap();

        let held = dag.get_task("goal-3").await.unwrap();
        assert_eq!(
            held.sink_task_id.as_deref(),
            Some("sink-first"),
            "which row carries the answer is decided once; a re-drive that \
             quietly repointed it would leave the first answer nowhere"
        );
    }

    #[tokio::test]
    async fn test_fg_add_visible_across_pods_without_load() {
        let (pod_a, pod_b) = fg_pods();
        pod_a.add_tasks_batch(chain(&["t1", "t2"])).await.unwrap();

        // pod B 内存为空、不调 load_from_backend，依然全量可见
        assert!(pod_b.inner.read().await.tasks.is_empty());
        assert!(pod_b.get_task("t1").await.is_some());
        assert_eq!(pod_b.get_all_tasks().await.len(), 2);
        let (nodes, edges) = pod_b.get_graph().await;
        assert_eq!(nodes.len(), 2);
        assert_eq!(edges, vec![("t1".to_string(), "t2".to_string())]);
        let deps = pod_b.get_dependencies("t2").await.unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].id, "t1");
        let dependents = pod_b.get_dependents("t1").await.unwrap();
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].id, "t2");
    }

    #[tokio::test]
    async fn test_fg_complete_cascades_ready_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        pod_a.add_tasks_batch(chain(&["t1", "t2"])).await.unwrap();

        // 跨 pod 接力：A 调度，B 启动并完成
        pod_a.schedule_task("t1").await.unwrap();
        pod_b.start_task("t1").await.unwrap();
        let unlocked = pod_b
            .complete_task("t1", serde_json::json!({"ok": true}))
            .await
            .unwrap();
        assert_eq!(unlocked, vec!["t2".to_string()]);

        // complete 同事务判定 t2 就绪并返回其 id，但保持 Pending——
        // Scheduled 只表示"已发布到 ready 流"，由发布方 schedule_task 翻转
        let t2_view = pod_a.get_task("t2").await.unwrap();
        assert_eq!(t2_view.status, TaskStatus::Pending);
        assert!(pod_a.get_ready_tasks().await.iter().any(|t| t.id == "t2"));
        assert!(!pod_a.all_completed().await);

        // 模拟发布方：翻 Scheduled（跨 pod CAS），随后可启动并完成
        pod_b.schedule_task("t2").await.unwrap();
        pod_a.start_task("t2").await.unwrap();
        pod_b
            .complete_task("t2", serde_json::json!({"ok": true}))
            .await
            .unwrap();
        assert!(pod_a.all_completed().await);
    }

    /// 目标行（不可执行、声明了汇点）+ 汇点一条，两条一起进图。
    async fn add_goal_and_sink(
        dag: &DagExecutor,
        goal_id: &str,
        declared_sink: &str,
        sink_id: &str,
    ) {
        let mut goal_row = Task::goal_row(goal_id, "a question");
        goal_row.sink_task_id = Some(declared_sink.to_string());
        let mut sink = Task::new(sink_id, TaskType::Generator, serde_json::json!({}));
        sink.parent_task_id = Some(goal_id.to_string());
        dag.add_tasks_batch(vec![goal_row, sink]).await.unwrap();
    }

    /// 汇点走到终态时，目标行跟着它走：目标行自己不执行，它的终态只能是抄来的。
    #[tokio::test]
    async fn a_completed_sink_hands_its_terminal_state_to_the_goal_row() {
        let dag = DagExecutor::new("ws-sink-complete".into());
        add_goal_and_sink(&dag, "goal-1", "child-1", "child-1").await;

        dag.schedule_task("child-1").await.unwrap();
        dag.start_task("child-1").await.unwrap();
        dag.complete_task("child-1", serde_json::json!({"answer": 42}))
            .await
            .unwrap();

        let row = dag.get_task("goal-1").await.unwrap();
        assert_eq!(row.status, TaskStatus::Completed);
        assert_eq!(row.result, Some(serde_json::json!({"answer": 42})));
        assert!(!row.is_executable, "抄来的终态不改「它自己不执行」");
    }

    /// 抄的是**被声明的那一条**：没被声明的任务走完终态不动目标行，被声明的那条判死
    /// 就让目标行跟着判死，且不留半个结果。
    #[tokio::test]
    async fn a_goal_row_derives_only_from_the_sink_it_declares() {
        let dag = DagExecutor::new("ws-sink-declared".into());
        add_goal_and_sink(&dag, "goal-1", "child-2", "child-1").await;
        let mut declared = Task::new("child-2", TaskType::Generator, serde_json::json!({}));
        declared.parent_task_id = Some("goal-1".into());
        declared.retry_count = u32::MAX - 1; // 超过任何 max_retries，fail 必终败
        dag.add_task(declared).await.unwrap();

        dag.schedule_task("child-1").await.unwrap();
        dag.start_task("child-1").await.unwrap();
        dag.complete_task("child-1", serde_json::json!({"answer": "not the one"}))
            .await
            .unwrap();
        let row = dag.get_task("goal-1").await.unwrap();
        assert_eq!(row.status, TaskStatus::Pending);
        assert_eq!(row.result, None);

        dag.schedule_task("child-2").await.unwrap();
        dag.start_task("child-2").await.unwrap();
        let (retried, _, _) = dag.fail_task("child-2", "boom".into(), None).await.unwrap();
        assert!(!retried);
        let row = dag.get_task("goal-1").await.unwrap();
        assert_eq!(row.status, TaskStatus::Failed);
        assert_eq!(row.error.as_deref(), Some("boom"));
        assert_eq!(row.result, None, "判死的行不留半个结果");
    }

    /// 存储权威模式同一条规则：终态落在共享存储里的那一行上，两个 pod 读到的是同一份。
    #[tokio::test]
    async fn a_sink_terminal_derives_the_goal_row_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        add_goal_and_sink(&pod_a, "goal-1", "child-1", "child-1").await;

        pod_a.schedule_task("child-1").await.unwrap();
        pod_b.start_task("child-1").await.unwrap();
        pod_b
            .complete_task("child-1", serde_json::json!({"answer": "shared"}))
            .await
            .unwrap();

        let row = pod_a.get_task("goal-1").await.unwrap();
        assert_eq!(row.status, TaskStatus::Completed);
        assert_eq!(row.result, Some(serde_json::json!({"answer": "shared"})));
    }

    #[tokio::test]
    async fn test_fg_fail_terminal_cascades_cancel_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        let mut tasks = chain(&["root", "mid", "leaf"]);
        tasks[0].retry_count = u32::MAX - 1; // 超过任何 max_retries，fail 必终败
        pod_a.add_tasks_batch(tasks).await.unwrap();

        pod_a.schedule_task("root").await.unwrap();
        pod_b.start_task("root").await.unwrap();
        let (retried, cancelled, _dlq) =
            pod_b.fail_task("root", "boom".into(), None).await.unwrap();
        assert!(!retried);
        assert!(cancelled.contains(&"mid".to_string()));
        assert!(cancelled.contains(&"leaf".to_string()));

        // 级联取消跨 pod 可见
        assert_eq!(
            pod_a.get_task("mid").await.unwrap().status,
            TaskStatus::Cancelled
        );
        assert_eq!(
            pod_a.get_task("leaf").await.unwrap().status,
            TaskStatus::Cancelled
        );
    }

    /// 失败的类型随任务记录落库并跨 pod 可读：读记录的人（例如按类型决定
    /// 退避的消费方）拿到的是类型，不是那句文本。
    #[tokio::test]
    async fn test_fg_failure_cause_is_stored_and_readable_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        pod_a
            .add_task(Task::new("t1", TaskType::Generator, serde_json::json!({})))
            .await
            .unwrap();
        pod_a.schedule_task("t1").await.unwrap();
        pod_b.start_task("t1").await.unwrap();

        pod_b
            .fail_task(
                "t1",
                "LLM upstream refused (quota_exhausted): spent".into(),
                Some(UpstreamFailure::QuotaExhausted),
            )
            .await
            .unwrap();

        let view = pod_a.get_task("t1").await.unwrap();
        assert_eq!(view.error_cause, Some(UpstreamFailure::QuotaExhausted));
        // 文本照旧保存：类型是给判定的，人读的还是那一句。
        assert!(view.error.unwrap().contains("quota_exhausted"));
    }

    #[tokio::test]
    async fn test_fg_cas_conflict_rejects_stale_transition() {
        let (pod_a, pod_b) = fg_pods();
        pod_a
            .add_task(Task::new("t1", TaskType::Generator, serde_json::json!({})))
            .await
            .unwrap();
        pod_a.schedule_task("t1").await.unwrap();
        pod_b.start_task("t1").await.unwrap(); // Running

        // pod A 持过期视图再 start：状态已不在 [Scheduled]，CAS 拒绝
        let err = pod_a.start_task("t1").await.unwrap_err();
        assert!(err.to_string().contains("Cannot start task"));
    }

    #[tokio::test]
    async fn test_fg_retry_history_shared_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        pod_a
            .add_task(Task::new("t1", TaskType::Generator, serde_json::json!({})))
            .await
            .unwrap();
        pod_a.schedule_task("t1").await.unwrap();
        pod_b.start_task("t1").await.unwrap();
        let (retried, _, _) = pod_b.fail_task("t1", "flaky".into(), None).await.unwrap();
        assert!(retried);

        // 重试历史写共享存储，pod A 直接可读
        let history = pod_a.get_retry_history("t1").await.unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].error, "flaky");
        // retry 后回 Pending（非 Scheduled），可重新走调度流程
        assert_eq!(
            pod_a.get_task("t1").await.unwrap().status,
            TaskStatus::Pending
        );
        pod_a.schedule_task("t1").await.unwrap();
    }

    #[tokio::test]
    async fn test_fg_delete_and_cancel_across_pods() {
        let (pod_a, pod_b) = fg_pods();
        pod_a
            .add_tasks_batch(chain(&["root", "child"]))
            .await
            .unwrap();

        let cancelled = pod_b.cancel_task("root").await.unwrap();
        assert_eq!(cancelled, vec!["child".to_string()]);
        assert_eq!(
            pod_a.get_task("root").await.unwrap().status,
            TaskStatus::Cancelled
        );

        pod_a.delete_task("child").await.unwrap();
        assert!(pod_b.get_task("child").await.is_none());
        assert_eq!(pod_b.get_all_tasks().await.len(), 1);
    }

    fn placeholder(id: &str, updated_at: chrono::DateTime<chrono::Utc>) -> Task {
        let mut t = Task::new(id, TaskType::Generator, serde_json::json!({}));
        t.is_executable = false;
        t.updated_at = updated_at;
        t
    }

    #[test]
    fn test_decomposition_orphans_classifier() {
        let now = chrono::Utc::now();
        let stale = now - chrono::Duration::minutes(60);
        let fresh = now - chrono::Duration::minutes(1);
        let stall_before = now - chrono::Duration::minutes(30);

        // Stale childless non-executable placeholder is the only orphan.
        let orphan = placeholder("orphan", stale);

        // Same shape but a child task points at it: not an orphan.
        let parent_with_child = placeholder("parent", stale);
        let mut child = Task::new("child", TaskType::Generator, serde_json::json!({}));
        child.parent_task_id = Some("parent".into());

        // Executable pending task, even stale and childless: runnable, not orphan.
        let mut runnable = Task::new("runnable", TaskType::Generator, serde_json::json!({}));
        runnable.updated_at = stale;

        // Fresh childless placeholder: within the stall window, not orphan yet.
        let fresh_placeholder = placeholder("fresh", fresh);

        // Already-terminated placeholder is no longer stuck Pending.
        let mut terminated = placeholder("terminated", stale);
        terminated.status = TaskStatus::Failed;

        let tasks = [
            orphan,
            parent_with_child,
            child,
            runnable,
            fresh_placeholder,
            terminated,
        ];
        let orphans = DagExecutor::decomposition_orphans(tasks.iter(), stall_before);
        assert_eq!(orphans, vec!["orphan".to_string()]);

        // Boundary: updated_at exactly at stall_before is not older → not orphan.
        let boundary = placeholder("boundary", stall_before);
        assert!(DagExecutor::decomposition_orphans([&boundary], stall_before).is_empty());
    }

    #[tokio::test]
    async fn test_find_and_terminate_stale_orphan_in_memory() {
        let dag = DagExecutor::new("ws-orphan".into());
        let stale = chrono::Utc::now() - chrono::Duration::minutes(60);

        let parent = placeholder("sig-alert-stale", stale);
        let mut child = Task::new("child-1", TaskType::Generator, serde_json::json!({}));
        child.parent_task_id = Some("parent-live".into());
        let live_parent = placeholder("parent-live", stale);

        dag.add_task(parent).await.unwrap();
        dag.add_task(live_parent).await.unwrap();
        dag.add_task(child).await.unwrap();

        let stall_before = chrono::Utc::now() - chrono::Duration::minutes(30);
        let orphans = dag.find_decomposition_orphans(stall_before).await;
        assert_eq!(orphans.len(), 1);
        assert_eq!(orphans[0].id, "sig-alert-stale");

        dag.terminate_decomposition_orphan(
            "sig-alert-stale",
            "decomposition produced no executable tasks".into(),
        )
        .await
        .unwrap();

        let view = dag.get_task("sig-alert-stale").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert!(!view.is_executable);
        assert!(view
            .error
            .as_deref()
            .unwrap()
            .contains("no executable tasks"));

        // Once terminated it drops out of future scans.
        assert!(dag
            .find_decomposition_orphans(stall_before)
            .await
            .is_empty());

        // Terminating a placeholder that still has children is rejected.
        let err = dag
            .terminate_decomposition_orphan("parent-live", "must fail".into())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("still has child tasks"));
    }

    #[tokio::test]
    async fn test_terminate_stale_orphan_in_store_mode() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-orphan-fg".into()).with_state_backend(backend);
        let stale = chrono::Utc::now() - chrono::Duration::minutes(60);
        dag.add_task(placeholder("sig-alert-store", stale))
            .await
            .unwrap();

        let stall_before = chrono::Utc::now() - chrono::Duration::minutes(30);
        assert_eq!(
            dag.find_decomposition_orphans(stall_before)
                .await
                .into_iter()
                .map(|t| t.id)
                .collect::<Vec<_>>(),
            vec!["sig-alert-store".to_string()]
        );

        dag.terminate_decomposition_orphan("sig-alert-store", "empty plan".into())
            .await
            .unwrap();
        let view = dag.get_task("sig-alert-store").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert!(view.error.as_deref().unwrap().contains("empty plan"));
    }

    /// 判定只看「前驱会不会变成 Completed」这一件事：Completed 与仍在路上的
    /// Pending 都不算，Failed、Cancelled、图里没有这个 id 都算。漏掉最后一种，
    /// 一个指向没落盘的 id 的依赖就又变回永久 Pending。
    #[tokio::test]
    async fn an_unblockable_pending_task_names_the_predecessor_that_settled_it() {
        let mut done = Task::new("done", TaskType::DagNode, serde_json::json!({}));
        done.status = TaskStatus::Completed;
        let mut failed = Task::new("failed", TaskType::DagNode, serde_json::json!({}));
        failed.status = TaskStatus::Failed;
        let mut cancelled = Task::new("cancelled", TaskType::DagNode, serde_json::json!({}));
        cancelled.status = TaskStatus::Cancelled;
        let pending = Task::new("pending", TaskType::DagNode, serde_json::json!({}));

        let mut after_done = Task::new("after-done", TaskType::DagNode, serde_json::json!({}));
        after_done.blocked_by = vec!["done".into()];
        let mut after_failed = Task::new("after-failed", TaskType::DagNode, serde_json::json!({}));
        after_failed.blocked_by = vec!["failed".into()];
        let mut after_cancelled =
            Task::new("after-cancelled", TaskType::DagNode, serde_json::json!({}));
        after_cancelled.blocked_by = vec!["cancelled".into()];
        let mut after_ghost = Task::new("after-ghost", TaskType::DagNode, serde_json::json!({}));
        after_ghost.blocked_by = vec!["ghost".into()];
        let mut after_pending =
            Task::new("after-pending", TaskType::DagNode, serde_json::json!({}));
        after_pending.blocked_by = vec!["pending".into()];

        let tasks = vec![
            done,
            failed,
            cancelled,
            pending,
            after_done,
            after_failed,
            after_cancelled,
            after_ghost,
            after_pending,
        ];
        let got = DagExecutor::unblockable_dependents(tasks.iter());
        assert_eq!(
            got,
            vec![
                (
                    "after-failed".to_string(),
                    "failed".to_string(),
                    "failed".to_string()
                ),
                (
                    "after-cancelled".to_string(),
                    "cancelled".to_string(),
                    "cancelled".to_string()
                ),
                (
                    "after-ghost".to_string(),
                    "ghost".to_string(),
                    "missing".to_string()
                ),
            ],
            "only settled predecessors — and absent ones — make a task unblockable"
        );
    }

    /// 生产标本（2026-10-18 12:19Z 的 `DSC-20261018-CB9687C9`）：一条依赖在被
    /// 写进来之前就已经 Cancelled 的任务，清扫器若不当场收掉，它会永远停在
    /// Pending，`oldest_untouched` 一路涨而执行器没有任何毛病。收掉时理由要点名
    /// 前驱，并顺着级联把它的下游一起收掉。
    #[tokio::test]
    async fn a_task_added_after_its_predecessor_settled_is_cancelled_with_the_reason() {
        let dag = DagExecutor::new("ws-unblockable".into());
        dag.add_task(Task::new("pre", TaskType::DagNode, serde_json::json!({})))
            .await
            .unwrap();
        dag.cancel_task("pre").await.unwrap();

        let mut dep = Task::new("dep", TaskType::DagNode, serde_json::json!({}));
        dep.blocked_by = vec!["pre".into()];
        dag.add_task(dep).await.unwrap();
        let mut grand = Task::new("grand", TaskType::DagNode, serde_json::json!({}));
        grand.blocked_by = vec!["dep".into()];
        dag.add_task(grand).await.unwrap();

        assert_eq!(
            dag.reclaim_unblockable_dependents(chrono::Duration::zero())
                .await,
            vec!["dep".to_string()]
        );

        let view = dag.get_task("dep").await.unwrap();
        assert_eq!(view.status, TaskStatus::Cancelled);
        let reason = view.error.expect("the cancellation has to say why");
        assert!(reason.contains("'pre' is cancelled"), "{reason}");
        assert_eq!(
            dag.get_task("grand").await.unwrap().status,
            TaskStatus::Cancelled,
            "the cascade has to take the downstream rows too"
        );
        assert!(
            dag.reclaim_unblockable_dependents(chrono::Duration::zero())
                .await
                .is_empty(),
            "a settled row must not be reported twice"
        );
    }

    /// 存储模式走的是 `dag_cancel_task` 那一笔条件写，reason 也必须落进去。
    #[tokio::test]
    async fn the_store_path_cancels_an_unblockable_dependent_with_the_reason() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-unblockable-fg".into()).with_state_backend(backend);
        dag.add_task(Task::new("pre", TaskType::DagNode, serde_json::json!({})))
            .await
            .unwrap();
        dag.cancel_task("pre").await.unwrap();

        let mut dep = Task::new("dep", TaskType::DagNode, serde_json::json!({}));
        dep.blocked_by = vec!["pre".into()];
        dag.add_task(dep).await.unwrap();

        assert_eq!(
            dag.reclaim_unblockable_dependents(chrono::Duration::zero())
                .await,
            vec!["dep".to_string()]
        );
        let view = dag.get_task("dep").await.unwrap();
        assert_eq!(view.status, TaskStatus::Cancelled);
        assert!(
            view.error
                .as_deref()
                .unwrap_or_default()
                .contains("can never become ready"),
            "{:?}",
            view.error
        );
    }

    /// 缺前驱需要宽限：批量是一行一行写的，同批里排在后面的前驱在被读到的那一刻
    /// 就是「图里没有这个 id」。宽限内不动它，宽限后才按「这个 id 从未落过盘」收掉。
    #[tokio::test]
    async fn a_missing_predecessor_is_only_acted_on_after_the_grace() {
        let dag = DagExecutor::new("ws-unblockable-missing".into());
        let mut dep = Task::new("dep", TaskType::DagNode, serde_json::json!({}));
        dep.blocked_by = vec!["later-in-the-same-batch".into()];
        dag.add_task(dep).await.unwrap();

        assert!(
            dag.reclaim_unblockable_dependents(chrono::Duration::minutes(10))
                .await
                .is_empty(),
            "a row younger than the grace may be waiting for a predecessor in its own batch"
        );
        assert_eq!(
            dag.get_task("dep").await.unwrap().status,
            TaskStatus::Pending
        );

        assert_eq!(
            dag.reclaim_unblockable_dependents(chrono::Duration::zero())
                .await,
            vec!["dep".to_string()]
        );
        assert_eq!(
            dag.get_task("dep").await.unwrap().status,
            TaskStatus::Cancelled
        );
    }

    /// 重试预算按失败原因发放，不按任务类型固定发。生成链在配额耗尽时报
    /// `terminal_env_failure`：同一环境里再来一次必然是同一个结果，重试只是
    /// 把整条 Squad→PGE 流水线连同它的 LLM 调用再买一遍。这条测试钉住的是
    /// "预算没被发出去"——只钉 `retried=false` 的话，一个把状态留在 Pending 的
    /// 实现也能过。
    #[tokio::test]
    async fn a_deterministic_failure_is_not_retried() {
        let dag = DagExecutor::new("ws-terminal".into());
        let task = Task::new("t-terminal", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, cancelled, _) = dag
            .fail_task(
                &task_id,
                "terminal_env_failure: generator produced no artifacts (environment/protocol failure)"
                    .into(),
                None,
            )
            .await
            .unwrap();

        assert!(!retried, "a deterministic failure must not be retried");
        assert!(cancelled.is_empty());
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert_eq!(view.retry_count, 0, "the budget was never spent");
        assert!(view.retry_not_before.is_none());
    }

    /// A refusal names one call and one rule, not a verdict on the work: the
    /// task is handed back once with the rule planted where the next role
    /// reads its request, and its budget is untouched — the task did not fail,
    /// the call did. The second refusal is the rule answering again, and the
    /// ordinary deterministic path ends it with no retries.
    #[tokio::test]
    async fn a_refusal_is_handed_back_once_with_its_rule() {
        let dag = DagExecutor::new("ws-refusal".into());
        let task = Task::new(
            "t-refusal",
            TaskType::DagNode,
            serde_json::json!({ "goal": "ship the change" }),
        );
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let reason = cog_core::SFError::Blocked {
            tool: "git_commit".into(),
            reason: "this scope may not write to the shared tree".into(),
            rule: "no-shared-tree-writes".into(),
        }
        .to_string();

        let (retried, cancelled, _) = dag.fail_task(&task_id, reason.clone(), None).await.unwrap();
        assert!(retried, "a refused call must not be read as a failed task");
        assert!(cancelled.is_empty());
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        assert_eq!(
            view.retry_count, 0,
            "handing the refusal back spends no budget"
        );
        assert!(
            view.retry_not_before.is_none(),
            "the next attempt is a different request; the old wait does not apply"
        );
        assert_eq!(
            view.input["goal"], "ship the change",
            "the request itself must not be rewritten"
        );
        let planted = &view.input["guardrail_refusal"];
        assert_eq!(planted["tool"], "git_commit");
        assert_eq!(planted["rule"], "no-shared-tree-writes");
        assert!(
            !planted["instruction"].as_str().unwrap_or("").is_empty(),
            "facts without an instruction tell the model what happened, not what to do"
        );

        dag.schedule_task(&task_id).await.unwrap();
        let (retried, _, _) = dag.fail_task(&task_id, reason, None).await.unwrap();
        assert!(!retried, "only one re-dispatch is bought");
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert_eq!(view.retry_count, 0, "the refusal never spends retry budget");
    }

    /// A marker that does not carry the rendered fields plants nothing and
    /// buys no re-dispatch: the safe reading of "refused, details lost" is the
    /// ordinary deterministic path, never a guessed tool name in the input.
    #[tokio::test]
    async fn a_refusal_without_its_fields_is_not_handed_back() {
        let dag = DagExecutor::new("ws-refusal-opaque".into());
        let task = Task::new("t-opaque", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = dag
            .fail_task(
                &task_id,
                "guardrail_blocked: something refused somewhere".into(),
                None,
            )
            .await
            .unwrap();

        assert!(!retried);
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert!(view.input.get("guardrail_refusal").is_none());
    }

    /// 判定要认的是声明本身，不是它在字符串里的字节位置：reason 被上层错误
    /// 类型包装后才到达这里是常态，那时声明前缀前面多出一段上下文。只认首字节
    /// 会把这种失败读成"没有声明原因"，确定性失败照样花掉整份重试预算——每一轮
    /// 都要重跑一遍完整流水线，而条件一次都没变过。
    #[tokio::test]
    async fn a_wrapped_deterministic_failure_is_not_retried() {
        let dag = DagExecutor::new("ws-wrapped".into());
        let task = Task::new("t-wrapped", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, cancelled, _) = dag
            .fail_task(
                &task_id,
                "Agent execution error: terminal_env_failure: generator produced no artifacts (environment/protocol failure)"
                    .into(),
                None,
            )
            .await
            .unwrap();

        assert!(
            !retried,
            "a wrapper must not hide the declared cause from the retry decision"
        );
        assert!(cancelled.is_empty());
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert_eq!(view.retry_count, 0, "the budget was never spent");
    }

    /// 类型通道与文本通道各自独立成立。传输层看到的配额耗尽（402/401）在
    /// 文本里不出现任何前缀，只凭文本判断就会把它当可重试；这条钉住类型
    /// 通道没有被文本判据吞掉。
    #[tokio::test]
    async fn a_terminal_upstream_cause_is_not_retried() {
        let dag = DagExecutor::new("ws-cause".into());
        let task = Task::new("t-cause", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = dag
            .fail_task(
                &task_id,
                "upstream refused".into(),
                Some(UpstreamFailure::QuotaExhausted),
            )
            .await
            .unwrap();

        assert!(!retried);
        assert_eq!(
            dag.get_task(&task_id).await.unwrap().status,
            TaskStatus::Failed
        );
    }

    /// 面行的常驻职责不被派发器接手，同一份图里的普通工作照常就绪。
    ///
    /// 这一行是可执行的（漂移读数只把可执行行算作「owner 有开口的工作」），
    /// 所以只按 `is_executable` 过滤会让派发器把它当活接走：跑满整轮预算、
    /// 一事无成、按退化循环判死，面的唯一在场证据随之消失，而没有任何路径
    /// 重建这一行。
    #[tokio::test]
    async fn a_faces_standing_duty_is_never_dispatched() {
        let dag = DagExecutor::new("ws-face-duty".into());
        let duty = Task::new(
            "face-work-x",
            TaskType::Custom(crate::governance_acts::FACE_DUTY_TASK_TYPE.into()),
            serde_json::json!({}),
        );
        let duty_id = duty.id.clone();
        dag.add_task(duty).await.unwrap();
        assert!(
            duty_id.starts_with("face-work-"),
            "the fixture is shaped like the row the duty constructor writes"
        );

        let mut work = Task::new("work-x", TaskType::DagNode, serde_json::json!({}));
        work.acceptance.push("门测试跑绿".into());
        let work_id = work.id.clone();
        dag.add_task(work).await.unwrap();

        let ready: Vec<String> = dag
            .find_ready_tasks()
            .await
            .into_iter()
            .map(|t| t.id)
            .collect();
        assert!(
            !ready.contains(&duty_id),
            "the dispatcher took a standing duty as work: {ready:?}"
        );
        assert!(
            ready.contains(&work_id),
            "ordinary work must stay ready or the exclusion is too wide: {ready:?}"
        );
    }

    /// 可重试的失败回到 Pending，但要等够该任务类型的退避才重新就绪。
    /// 没有这一步，重试就是零延迟重投——`RetryMatrix::delay` 配了也等于没有。
    #[tokio::test]
    async fn a_transient_failure_waits_out_its_backoff() {
        let dag = DagExecutor::new("ws-backoff".into());
        let task = Task::new("t-backoff", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = dag
            .fail_task(&task_id, "upstream is unreachable".into(), None)
            .await
            .unwrap();

        assert!(retried, "a transient failure keeps its retry");
        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        assert_eq!(view.retry_count, 1);
        // DagNode 的第一次退避是 5s；判据取"明显晚于现在"而不是具体秒数，
        // 免得把策略里的数值复制进测试后两边各自漂移。
        let due = view.retry_not_before.expect("a backoff deadline");
        assert!(
            due > chrono::Utc::now() + chrono::Duration::seconds(3),
            "retry deadline {due} is not held off"
        );

        // 未到期的重试不是就绪任务：发布者扫到它也不能重投。
        assert!(
            !dag.find_ready_tasks().await.iter().any(|t| t.id == task_id),
            "a task inside its backoff window was offered as ready"
        );
    }

    /// 存续模式走的是另一条路径（判定在 `dag_fail_task` 里，退避要随 JSONB
    /// 一起落库）。双 pod 下判定与重新投递不是同一个进程，只测内存路径会漏掉
    /// 退避在过界时丢掉的那种实现。
    #[tokio::test]
    async fn store_mode_holds_off_a_transient_retry_too() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-backoff-fg".into()).with_state_backend(backend);
        let task = Task::new("t-backoff-fg", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = dag
            .fail_task(&task_id, "upstream is unreachable".into(), None)
            .await
            .unwrap();
        assert!(retried);

        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.retry_count, 1);
        let due = view
            .retry_not_before
            .expect("a backoff deadline survived the store");
        assert!(due > chrono::Utc::now() + chrono::Duration::seconds(3));
        assert!(!dag.find_ready_tasks().await.iter().any(|t| t.id == task_id));

        // 存续模式下终止性失败同样不重试。
        let other = Task::new("t-terminal-fg", TaskType::DagNode, serde_json::json!({}));
        let other_id = other.id.clone();
        dag.add_task(other).await.unwrap();
        dag.schedule_task(&other_id).await.unwrap();
        let (retried, _, _) = dag
            .fail_task(&other_id, "terminal_env_failure: no artifacts".into(), None)
            .await
            .unwrap();
        assert!(!retried);
        assert_eq!(
            dag.get_task(&other_id).await.unwrap().status,
            TaskStatus::Failed
        );
    }

    /// 上游说了要等多久时，重试时刻由它说了算——只要它说的比策略更久。
    /// 两条路径都要看：内存路径与存续路径各算各的退避，只测一条会漏掉另一条
    /// 把明说的时长丢掉。
    #[tokio::test]
    async fn a_stated_wait_outlasts_the_policy_delay_on_both_paths() {
        const STATED: u64 = 300;

        let dag = DagExecutor::new("ws-stated".into());
        let task = Task::new("t-stated", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = dag
            .fail_task_after(
                &task_id,
                "upstream refused".into(),
                Some(UpstreamFailure::RateLimited),
                Some(STATED),
            )
            .await
            .unwrap();
        assert!(retried, "一次限流仍可重试，只是要等上游说的时长");
        let due = dag
            .get_task(&task_id)
            .await
            .unwrap()
            .retry_not_before
            .expect("a backoff deadline");
        // DagNode 的第一次退避是 5s。取比它高一个数量级的判据，免得把策略里
        // 的数值抄进测试，两边各自漂移。
        assert!(
            due > chrono::Utc::now() + chrono::Duration::seconds(240),
            "上游说的 300s 没有盖过 5s 的策略退避: {due}"
        );

        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let stored = DagExecutor::new("ws-stated-fg".into()).with_state_backend(backend);
        let task = Task::new("t-stated-fg", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        stored.add_task(task).await.unwrap();
        stored.schedule_task(&task_id).await.unwrap();

        let (retried, _, _) = stored
            .fail_task_after(
                &task_id,
                "upstream refused".into(),
                Some(UpstreamFailure::RateLimited),
                Some(STATED),
            )
            .await
            .unwrap();
        assert!(retried);
        let due = stored
            .get_task(&task_id)
            .await
            .unwrap()
            .retry_not_before
            .expect("a backoff deadline survived the store");
        assert!(
            due > chrono::Utc::now() + chrono::Duration::seconds(240),
            "落库路径把明说的时长丢了: {due}"
        );
    }

    /// 上游停供是环境，不是任务做的：网关 503 每次自报"282 秒后重试"，被拒多少次
    /// 都不该花掉任务自己的预算。跨小时的窗口里，DagNode 的 3 格预算在第 15 分钟
    /// 就烧完了，而任务从头到尾没做错任何事。
    ///
    /// 钉的是"被拒 N 次之后任务仍然可跑"，不是"重试次数变多了"：只钉次数的话，
    /// 一个把 retry_count 一路加到 max、再回 Pending 的实现照样能过——那正是要修
    /// 的那一个。
    ///
    /// 同一条测试里钉住反面：终态的环境因（这里是没有复位窗口的鉴权拒绝，它甚至
    /// 也带了一个等待时长）照旧判死。判据一旦放宽成"环境一律不记账"，这条就翻车。
    #[tokio::test]
    async fn an_upstream_outage_never_spends_the_tasks_budget() {
        let dag = DagExecutor::new("ws-outage".into());
        let task = Task::new("t-outage", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();
        dag.start_task(&task_id).await.unwrap();

        // 与线上那一笔同形：网关 503 把自己的原始 body 原样带出去，等待时长在
        // 角色把错误写进自己 content 那一跳就只剩文本（类型当场丢了）。
        let refusal = format!(
            "terminal_env_failure: environment_error: LLM upstream refused (server_error): \
             LLM stream error: API error (HTTP 503): \
             {{\"error\":\"所有 LLM 上游当前不可用\",\"quota_window_secs\":18000,\
             \"retry_after_seconds\":282}}{}",
            cog_core::contract::llm::render_retry_after(282)
        );

        // 停供横跨数小时，每次被拒都自报 282 秒；次数取得远多于预算的 3 格。
        const REFUSALS: u32 = 40;
        for i in 0..REFUSALS {
            let (retried, cancelled, _) = dag
                .fail_task_after(&task_id, refusal.clone(), None, Some(282))
                .await
                .unwrap();
            assert!(retried, "第 {i} 次被拒后任务必须还在队列里");
            assert!(cancelled.is_empty(), "环境停供不该级联取消下游");
            let view = dag.get_task(&task_id).await.unwrap();
            assert_eq!(view.status, TaskStatus::Pending, "第 {i} 次被拒后");
            assert_eq!(
                view.retry_count, 0,
                "被拒 {i} 次，一格预算都不该记在任务头上"
            );
            let due = view.retry_not_before.expect("回投要等上游说的那个时长");
            assert!(
                due > chrono::Utc::now() + chrono::Duration::seconds(240),
                "第 {i} 次回投等的是上游报的 282 秒，不是策略的 5 秒"
            );
        }

        // 上游回来，任务照常落地。等待是上游定的，测试里直接按"窗口走完"派发。
        dag.schedule_task(&task_id).await.unwrap();
        dag.start_task(&task_id).await.unwrap();
        dag.complete_task(&task_id, serde_json::json!({"ok": true}))
            .await
            .unwrap();
        assert_eq!(
            dag.get_task(&task_id).await.unwrap().status,
            TaskStatus::Completed
        );

        // 反面：终态的环境因照旧判死，哪怕它也说了一个等待时长。
        let dead = Task::new("t-auth", TaskType::DagNode, serde_json::json!({}));
        let dead_id = dead.id.clone();
        dag.add_task(dead).await.unwrap();
        dag.schedule_task(&dead_id).await.unwrap();
        dag.start_task(&dead_id).await.unwrap();
        let (retried, _, _) = dag
            .fail_task_after(
                &dead_id,
                "LLM upstream refused (auth_rejected): invalid api key".into(),
                Some(UpstreamFailure::Auth),
                Some(282),
            )
            .await
            .unwrap();
        assert!(!retried, "鉴权被拒没有复位窗口，一个等待时长救不了它");
        let view = dag.get_task(&dead_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert_eq!(view.retry_count, 0, "判死不是花预算花的");
    }

    /// 存续模式的同一判据：判定在存储里做，回投要随 JSONB 一起落库。双 pod 下
    /// 判定与重新投递不是同一个进程，只测内存路径会漏掉另一条。
    #[tokio::test]
    async fn store_mode_also_lets_the_environment_pay() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-outage-fg".into()).with_state_backend(backend);
        let task = Task::new("t-outage-fg", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();
        dag.start_task(&task_id).await.unwrap();

        let refusal = format!(
            "terminal_env_failure: LLM upstream refused (server_error): HTTP 503{}",
            cog_core::contract::llm::render_retry_after(282)
        );

        const REFUSALS: u32 = 12;
        for i in 0..REFUSALS {
            let (retried, _, _) = dag
                .fail_task_after(&task_id, refusal.clone(), None, Some(282))
                .await
                .unwrap();
            assert!(retried, "第 {i} 次被拒后任务必须还在队列里（存续路径）");
            let view = dag.get_task(&task_id).await.unwrap();
            assert_eq!(view.status, TaskStatus::Pending, "第 {i} 次被拒后");
            assert_eq!(view.retry_count, 0, "存续路径也不该记这笔账");
            let due = view.retry_not_before.expect("回投要随行落库");
            assert!(due > chrono::Utc::now() + chrono::Duration::seconds(240));
        }

        dag.schedule_task(&task_id).await.unwrap();
        dag.start_task(&task_id).await.unwrap();
        dag.complete_task(&task_id, serde_json::json!({"ok": true}))
            .await
            .unwrap();
        assert_eq!(
            dag.get_task(&task_id).await.unwrap().status,
            TaskStatus::Completed
        );
    }

    /// 走唯一入口拿到执行权的任务必然带租约。没有租约的 Running 行没有"到期
    /// 时刻"这条证据，也就没有任何东西会去回收它——它到不了任何终态。
    #[tokio::test]
    async fn starting_a_task_grants_it_a_lease() {
        let dag = DagExecutor::new("ws-lease-start".into()).with_task_lease_secs(60);
        let task = Task::new("t-start", TaskType::DagNode, serde_json::json!({}));
        let task_id = task.id.clone();
        dag.add_task(task).await.unwrap();
        dag.schedule_task(&task_id).await.unwrap();
        dag.start_task(&task_id).await.unwrap();

        let view = dag.get_task(&task_id).await.unwrap();
        assert_eq!(view.status, TaskStatus::Running);
        assert_eq!(view.lease_owner.as_deref(), Some(dag.run_id()));
        let expires = view.lease_expires_at.expect("a lease expiry");
        assert!(
            expires > chrono::Utc::now(),
            "到期时刻必须在将来: {expires}"
        );
        assert!(
            expires <= chrono::Utc::now() + chrono::Duration::seconds(61),
            "到期时刻不能超过租约时长: {expires}"
        );
    }

    /// 心跳只续自己持有的、还在跑的任务。续别人的会让一个旁观者把另一个进程的
    /// 死线不断推后，那个进程死了也永远判不到过期，回收就再也不会发生。
    #[tokio::test]
    async fn renewal_touches_only_own_live_claims() {
        let dag = DagExecutor::new("ws-renew".into()).with_task_lease_secs(60);

        let mine = Task::new("t-mine", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(mine).await.unwrap();
        dag.schedule_task("t-mine").await.unwrap();
        dag.start_task("t-mine").await.unwrap();

        let mut theirs = Task::new("t-theirs", TaskType::DagNode, serde_json::json!({}));
        theirs.begin_run(
            "some-other-process",
            chrono::Duration::seconds(60),
            chrono::Utc::now(),
        );
        dag.add_task(theirs).await.unwrap();

        let pending = Task::new("t-pending", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(pending).await.unwrap();

        let mine_before = dag
            .get_task("t-mine")
            .await
            .unwrap()
            .lease_expires_at
            .unwrap();
        let theirs_before = dag
            .get_task("t-theirs")
            .await
            .unwrap()
            .lease_expires_at
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert_eq!(
            dag.renew_leases().await.unwrap(),
            1,
            "只有自己持有的 running 任务该被续期"
        );
        assert!(
            dag.get_task("t-mine")
                .await
                .unwrap()
                .lease_expires_at
                .unwrap()
                > mine_before,
            "自己的租约要被推后"
        );
        assert_eq!(
            dag.get_task("t-theirs")
                .await
                .unwrap()
                .lease_expires_at
                .unwrap(),
            theirs_before,
            "别人持有的租约一个字节都不能碰"
        );
        assert!(dag
            .get_task("t-pending")
            .await
            .unwrap()
            .lease_expires_at
            .is_none());
    }

    /// 换版把原进程连根拔掉时，它手里的任务停在 Running，而 Running 到不了任何
    /// 终态——没有回收就永久卡住。租约到期是"原进程不在了"的证据，接手它是对的，
    /// 所以这个原因不能被读成终止性失败，更不能记在任务自己的重试账上：一次部署
    /// 就扣一格的话，三次部署就能把一条没出过错的回炉判死。
    #[tokio::test]
    async fn a_lease_expired_task_is_reclaimed_without_spending_its_budget() {
        let dag = DagExecutor::new("ws-expired".into()).with_task_lease_secs(60);
        let task = Task::new("t-expired", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(task).await.unwrap();
        dag.schedule_task("t-expired").await.unwrap();
        dag.start_task("t-expired").await.unwrap();

        // 心跳停了：租约到期时刻被推回过去，而这一轮离预算耗尽还远。
        {
            let mut inner = dag.inner.write().await;
            let t = inner.tasks.get_mut("t-expired").unwrap();
            t.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
            t.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
            t.timeout_seconds = 3600;
        }

        let results = dag.check_timeouts().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1, "租约过期必须回到队列里，不是被判死");

        let view = dag.get_task("t-expired").await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        let reason = view.error.expect("a failure reason");
        assert!(reason.contains("run lease expired"), "{reason}");
        assert!(
            !cog_core::contract::outcome::is_deterministic_failure(&reason),
            "部署换人不是确定性失败；标成确定性会让 discovery 把这条意图永久 Blocked 掉"
        );
        assert!(view.lease_owner.is_none(), "回收后租约要清干净");
        assert_eq!(
            view.retry_count, 0,
            "换了持有者不是任务做的：这次回收一格预算都不该扣"
        );
        assert!(
            view.retry_not_before
                .is_some_and(|due| due > chrono::Utc::now()),
            "回 Pending 的任务带着退避死线"
        );
    }

    /// 回归：一次版本滚动就能把一条**预算已经用光**的回炉判死，因为回收这条路
    /// 唯一的判决是 `retry_count < max_retries`，而它读的是任务自己烧了多少格，
    /// 不是这次回收该不该由任务付账。这里把预算先摆到用完，回收后任务仍要活着，
    /// 且账面上那格数**不能**再涨——重复出现的同一个数就是「这一轮没动预算」。
    #[tokio::test]
    async fn a_budget_exhausted_task_whose_owner_is_replaced_is_not_judged() {
        let dag = DagExecutor::new("ws-expired-spent".into()).with_task_lease_secs(60);
        let task = Task::new("t-spent", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(task).await.unwrap();
        dag.schedule_task("t-spent").await.unwrap();
        dag.start_task("t-spent").await.unwrap();
        let spent = dag.retry_matrix.max_retries(&TaskType::DagNode).max(1);

        {
            let mut inner = dag.inner.write().await;
            let t = inner.tasks.get_mut("t-spent").unwrap();
            t.retry_count = spent;
            t.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
            t.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
            t.timeout_seconds = 3600;
        }

        let results = dag.check_timeouts().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].1, "预算用完也不许把这次回收判成终态");

        let view = dag.get_task("t-spent").await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        assert_eq!(view.retry_count, spent, "回收不动任务的重试账");
    }

    /// 存储模式走的是另一条实现，判据必须一样：换了持有者不记账，任务回到队列。
    #[tokio::test]
    async fn a_store_claim_whose_owner_is_replaced_keeps_its_budget() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-lease-nc-fg".into())
            .with_state_backend(backend.clone())
            .with_task_lease_secs(60);
        dag.add_task(Task::new(
            "t-nc-fg",
            TaskType::DagNode,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        dag.schedule_task("t-nc-fg").await.unwrap();
        dag.start_task("t-nc-fg").await.unwrap();

        let mut row = backend
            .dag_get_task("ws-lease-nc-fg", "t-nc-fg")
            .await
            .unwrap()
            .expect("the row was persisted");
        let spent = dag.retry_matrix.max_retries(&TaskType::DagNode).max(1);
        row.retry_count = spent;
        row.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        row.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        row.timeout_seconds = 3600;
        backend
            .dag_set_task("ws-lease-nc-fg", "t-nc-fg", &row)
            .await
            .unwrap();

        assert_eq!(dag.check_timeouts().await.len(), 1);
        let after = backend
            .dag_get_task("ws-lease-nc-fg", "t-nc-fg")
            .await
            .unwrap()
            .expect("the row is still there");
        assert_eq!(
            after.status,
            TaskStatus::Pending,
            "换了持有者不是任务做的，预算用完也不能判死"
        );
        assert_eq!(after.retry_count, spent, "回收不动任务的重试账");
    }

    /// 持有者还活着却把整段预算跑满，是"花了钱没产出"。同一份输入重跑买不到不同
    /// 的结果，所以这个原因声明自己是终止性的——否则每一轮都要重新买一遍完整预算。
    #[tokio::test]
    async fn an_over_budget_task_is_reclaimed_as_a_deterministic_failure() {
        let dag = DagExecutor::new("ws-budget".into()).with_task_lease_secs(60);
        let task = Task::new("t-budget", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(task).await.unwrap();
        dag.schedule_task("t-budget").await.unwrap();
        dag.start_task("t-budget").await.unwrap();

        {
            let mut inner = dag.inner.write().await;
            let t = inner.tasks.get_mut("t-budget").unwrap();
            // 租约刚续过（持有者还活着），但 started_at 已经超出预算。
            t.lease_expires_at = Some(chrono::Utc::now() + chrono::Duration::seconds(60));
            t.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(3601));
            t.timeout_seconds = 3600;
        }

        let results = dag.check_timeouts().await;
        assert_eq!(results.len(), 1);
        assert!(!results[0].1, "预算耗尽不能重投，那等于重买一轮");

        let view = dag.get_task("t-budget").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        assert_eq!(view.retry_count, 0, "确定性失败不花预算");
        let reason = view.error.expect("a failure reason");
        assert!(
            cog_core::contract::outcome::declares(
                &reason,
                cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX
            ),
            "{reason}"
        );
        assert!(cog_core::contract::outcome::is_deterministic_failure(
            &reason
        ));
    }

    /// 部署前起的任务行里没有租约字段。没有租约就没有"到期时刻"，但 started_at
    /// 加上租约仍然是它该被回收的时刻——否则这些存量行要么永远 Running，要么
    /// 得白等满整个 timeout。
    #[tokio::test]
    async fn a_legacy_claim_without_a_lease_expires_from_its_start_time() {
        let dag = DagExecutor::new("ws-legacy".into()).with_task_lease_secs(60);
        let mut task = Task::new("t-legacy", TaskType::DagNode, serde_json::json!({}));
        task.status = TaskStatus::Running;
        task.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(120));
        task.timeout_seconds = 3600;
        dag.add_task(task).await.unwrap();

        assert_eq!(
            dag.check_timeouts().await.len(),
            1,
            "存量 Running 行必须按 started_at 判过期"
        );
        assert!(dag
            .get_task("t-legacy")
            .await
            .unwrap()
            .error
            .unwrap()
            .contains("run lease expired"));
    }

    /// 没在跑的任务不是回收对象：等退避或被依赖挡住是 Pending 的常态，把它们
    /// 当僵尸清扫会直接吃掉整条 DAG。
    #[tokio::test]
    async fn a_pending_task_is_never_reclaimed() {
        let dag = DagExecutor::new("ws-pending".into()).with_task_lease_secs(60);
        let mut task = Task::new("t-pending", TaskType::DagNode, serde_json::json!({}));
        task.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(3600));
        task.timeout_seconds = 1;
        dag.add_task(task).await.unwrap();

        assert!(dag.check_timeouts().await.is_empty());
        assert_eq!(
            dag.get_task("t-pending").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    /// 落库模式下回收同样成立：这是两个部署实际走的路径，租约的权威在存储行上，
    /// 不在任何一个进程的内存里。
    #[tokio::test]
    async fn a_lease_expired_store_claim_is_reclaimed() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let dag = DagExecutor::new("ws-lease-fg".into())
            .with_state_backend(backend.clone())
            .with_task_lease_secs(60);
        let task = Task::new("t-fg", TaskType::DagNode, serde_json::json!({}));
        dag.add_task(task).await.unwrap();
        dag.schedule_task("t-fg").await.unwrap();
        dag.start_task("t-fg").await.unwrap();

        // 直接改存储行：持有者心跳停了，而这一轮离预算耗尽还远。
        let mut row = backend
            .dag_get_task("ws-lease-fg", "t-fg")
            .await
            .unwrap()
            .expect("the row was persisted");
        row.lease_expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        row.started_at = Some(chrono::Utc::now() - chrono::Duration::seconds(5));
        row.timeout_seconds = 3600;
        backend
            .dag_set_task("ws-lease-fg", "t-fg", &row)
            .await
            .unwrap();

        assert_eq!(dag.check_timeouts().await.len(), 1);
        let view = dag.get_task("t-fg").await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        assert!(!cog_core::contract::outcome::is_deterministic_failure(
            view.error.as_deref().unwrap()
        ));
    }

    /// 心跳续期在存储行上做 CAS 校验身份：另一个进程即使扫到我的任务也不能替它
    /// 续期，否则我的进程死了，它手里的租约被旁观者一直续着，永远不会被回收。
    #[tokio::test]
    async fn a_peer_process_cannot_renew_my_store_claim() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let pod_a = DagExecutor::new("ws-lease-renew-fg".into())
            .with_state_backend(backend.clone())
            .with_task_lease_secs(60);
        let pod_b = DagExecutor::new("ws-lease-renew-fg".into()).with_state_backend(backend);

        let task = Task::new("t-fg-renew", TaskType::DagNode, serde_json::json!({}));
        pod_a.add_task(task).await.unwrap();
        pod_a.schedule_task("t-fg-renew").await.unwrap();
        pod_a.start_task("t-fg-renew").await.unwrap();

        let before = pod_a
            .get_task("t-fg-renew")
            .await
            .unwrap()
            .lease_expires_at
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;

        assert_eq!(
            pod_b.renew_leases().await.unwrap(),
            0,
            "旁观者续不了别人的租约"
        );
        assert_eq!(
            pod_b
                .get_task("t-fg-renew")
                .await
                .unwrap()
                .lease_expires_at
                .unwrap(),
            before
        );

        assert_eq!(pod_a.renew_leases().await.unwrap(), 1);
        assert!(
            pod_a
                .get_task("t-fg-renew")
                .await
                .unwrap()
                .lease_expires_at
                .unwrap()
                > before
        );
    }

    /// 发布器只扫 `Pending`，超时检查只回收 `Running`：一个离开 `Pending` 又没
    /// 走到 `Running` 的任务不在任何一条重扫的判据面上，它会一直停在
    /// `Scheduled` 而没有任何东西会再来看它。这条读数是它重新可见的唯一入口，
    /// 所以它必须只挑出那一个状态、那一段时间——刚进 `Scheduled` 的任务是一条
    /// 还在路上的消息，回收它就是把同样的活干两遍。
    #[tokio::test]
    async fn a_stalled_scheduled_task_is_one_past_the_window_and_nothing_else() {
        let dag = DagExecutor::new("ws-stalled-select".into());
        for id in ["stalled", "fresh", "running", "pending", "done"] {
            dag.add_task(Task::new(id, TaskType::LlmCall, serde_json::json!({})))
                .await
                .unwrap();
        }
        let old = chrono::Utc::now() - chrono::Duration::minutes(30);
        {
            let mut inner = dag.inner.write().await;
            for (id, status) in [
                ("stalled", TaskStatus::Scheduled),
                ("fresh", TaskStatus::Scheduled),
                ("running", TaskStatus::Running),
                ("pending", TaskStatus::Pending),
                ("done", TaskStatus::Completed),
            ] {
                let task = inner.tasks.get_mut(id).unwrap();
                task.status = status;
                task.updated_at = old;
            }
            // Same state as the stalled one; only its age separates the two.
            inner.tasks.get_mut("fresh").unwrap().updated_at = chrono::Utc::now();
        }
        let tasks = dag.get_all_tasks().await;
        assert_eq!(
            DagExecutor::stalled_scheduled(
                tasks.iter(),
                chrono::Utc::now() - chrono::Duration::minutes(10)
            ),
            vec!["stalled".to_string()],
            "only the old `Scheduled` row is a lost message: the fresh one is in flight, \
             and the others are states that already have a reclaimer"
        );
    }

    /// 调用方给的窗口比传输层自己重投未确认消息的窗口更短时，不能因此把还在
    /// 传输窗口内的消息当成丢掉的活再干一遍：请求的窗口按
    /// `DEFAULT_READY_CLAIM_IDLE_SECS` 兜底，更短的调用拿不到重复执行。
    #[tokio::test]
    async fn a_window_shorter_than_the_transport_floor_is_not_honored() {
        let dag = DagExecutor::new("ws-stalled-floor".into());
        dag.add_task(Task::new("young", TaskType::LlmCall, serde_json::json!({})))
            .await
            .unwrap();
        dag.schedule_task("young").await.unwrap();
        // 比调用方请求的 60s 老，但远在兜底后的 600s 窗口之内。
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("young").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(5);
        }

        assert_eq!(
            sweep(&dag, 60, &served("ws-stalled-floor", true)).await,
            0,
            "a caller asking for a window below the transport's own re-delivery window is \
             asking for the duplicate, and does not get one"
        );
        let young = dag.get_task("young").await.unwrap();
        assert_eq!(young.status, TaskStatus::Scheduled);
        assert_eq!(young.retry_count, 0);
    }

    #[tokio::test]
    async fn reclaiming_a_stalled_task_puts_it_back_in_line_without_charging() {
        let dag = DagExecutor::new("ws-stalled-reclaim".into());
        for id in ["stalled", "fresh"] {
            dag.add_task(Task::new(id, TaskType::LlmCall, serde_json::json!({})))
                .await
                .unwrap();
            dag.schedule_task(id).await.unwrap();
        }
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("stalled").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(30);
        }

        assert_eq!(
            sweep(&dag, 600, &served("ws-stalled-reclaim", true)).await,
            1
        );

        let stalled = dag.get_task("stalled").await.unwrap();
        assert_eq!(stalled.status, TaskStatus::Pending);
        assert_eq!(
            stalled.retry_count, 0,
            "the task never ran, so no attempt was spent on it"
        );
        assert!(
            stalled.retry_not_before.is_none(),
            "the re-arm is the repair, not a retry: it goes back out on the next publish pass"
        );
        assert!(stalled
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("never started"));

        // One re-arm per stall, not one per sweep: the task is no longer
        // `Scheduled`, so the next sweep has nothing to pick up.
        assert_eq!(
            sweep(&dag, 600, &served("ws-stalled-reclaim", true)).await,
            0
        );
        assert_eq!(dag.get_task("stalled").await.unwrap().retry_count, 0);

        let fresh = dag.get_task("fresh").await.unwrap();
        assert_eq!(fresh.status, TaskStatus::Scheduled);
        assert_eq!(fresh.retry_count, 0);
    }

    /// 本部署不读的那条队列上，停在 `Scheduled` 不是坏消息，是任务在等执行器。
    ///
    /// 2026-10-18 现场：执行器角色被关掉后，进程本地队列只剩发布器，没有消费者；
    /// 清扫器每 600 s 把 1,499 笔任务放回队列并重发，消息总数从几千涨到 8,815，
    /// 队列越扫越长。判据：同一个任务，同一个年龄，读这条队列的部署收回它，
    /// 不读的部署不碰它。
    #[tokio::test]
    async fn a_queue_this_deployment_does_not_read_waits_for_its_executor() {
        let generate = || {
            Task::new(
                "generate",
                TaskType::Custom("platform_issue_fix".into()),
                serde_json::json!({"evolution_mode": "generate_change"}),
            )
        };
        let dag = DagExecutor::new("ws-stalled-unserved".into());
        dag.add_task(generate()).await.unwrap();
        dag.schedule_task("generate").await.unwrap();
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("generate").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(30);
        }
        assert!(dag.get_task("generate").await.unwrap().is_self_evolution());

        assert_eq!(
            sweep(&dag, 600, &served("ws-stalled-unserved", false)).await,
            0,
            "no reader means the queue is the holder, not a lost message: re-arming here \
             republished the whole backlog every stall window"
        );
        let waiting = dag.get_task("generate").await.unwrap();
        assert_eq!(waiting.status, TaskStatus::Scheduled);
        assert_eq!(waiting.retry_count, 0);

        assert_eq!(
            sweep(&dag, 600, &served("ws-stalled-unserved", true)).await,
            1,
            "the deployment that reads the queue is the one that can tell a lost message \
             from a queued one, so it is the one that repairs it"
        );
        assert_eq!(
            dag.get_task("generate").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    /// 队列还压着没投出去的消息时，停在 `Scheduled` 不是「消息丢了」。
    ///
    /// 年龄对「消息排在队伍里」与「消息丢了」同值，能分开这两件事的只有队列
    /// 自己的读数。判据：同一笔任务、同一个年龄，传输层报积压 ⇒ 不动，报
    /// 清零 ⇒ 收回；推迟这件事本身也要留一条曲线，否则「在等」读起来和
    /// 「清扫器没跑」是同一个样子。
    #[tokio::test]
    async fn a_backlog_of_undelivered_entries_defers_the_sweep() {
        let dag = DagExecutor::new("ws-stalled-backlog".into());
        dag.add_task(Task::new(
            "queued",
            TaskType::LlmCall,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        dag.schedule_task("queued").await.unwrap();
        {
            let mut inner = dag.inner.write().await;
            // 一个多窗、但还没到「等太久」上限的年龄：推迟要看的是积压，
            // 上限要的是三个窗，两者分开才测得出来是哪一个在起作用。
            inner.tasks.get_mut("queued").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(15);
        }
        let metrics = Arc::new(RecordingMetrics::default());
        dag.attach_metrics(metrics.clone());
        let streams = served("ws-stalled-backlog", true);
        let stream = crate::ready_queue::ready_stream("ws-stalled-backlog");

        let mut undelivered = HashMap::new();
        undelivered.insert(stream.clone(), Some(8_815u64));
        assert_eq!(
            sweep_with(&dag, 600, &streams, &undelivered).await,
            0,
            "the message may still be in line: re-arming here adds a second copy to the \
             queue that is already behind"
        );
        let still = dag.get_task("queued").await.unwrap();
        assert_eq!(still.status, TaskStatus::Scheduled);
        assert_eq!(still.retry_count, 0);
        assert_eq!(
            metrics.recorded(cog_core::metric_names::DAG_STALLED_SCHEDULED_DEFERRED_FOR_BACKLOG),
            1,
            "a wait nobody counts is indistinguishable from a sweep that never ran"
        );

        // 积压排空的那一刻，同一个年龄就该被收回。
        undelivered.insert(stream, Some(0));
        assert_eq!(sweep_with(&dag, 600, &streams, &undelivered).await, 1);
        assert_eq!(
            dag.get_task("queued").await.unwrap().status,
            TaskStatus::Pending
        );
        assert_eq!(
            metrics.recorded(cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG),
            0,
            "no backlog at the moment of the reclaim, so this one is the ordinary repair"
        );
    }

    /// 积压只是「消息还可能在队伍里」的解释，不是无限期的豁免。
    ///
    /// 计数说的是有多少条在等，不是有没有人在读。等满 N 个窗仍不动，这条队列
    /// 已经不是「慢」；照常收回，但记到另一条曲线上——否则一条永远排不空的
    /// 队列要么被无限期地等下去，要么被静默折进普通总数里，两种都读不出来。
    #[tokio::test]
    async fn a_backlog_that_never_drains_stops_being_an_explanation() {
        let dag = DagExecutor::new("ws-stalled-backlog-cap".into());
        dag.add_task(Task::new(
            "stranded",
            TaskType::LlmCall,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        dag.schedule_task("stranded").await.unwrap();
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("stranded").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(60);
        }
        let metrics = Arc::new(RecordingMetrics::default());
        dag.attach_metrics(metrics.clone());
        let streams = served("ws-stalled-backlog-cap", true);
        let stream = crate::ready_queue::ready_stream("ws-stalled-backlog-cap");

        let mut undelivered = HashMap::new();
        undelivered.insert(stream, Some(8_815u64));
        assert_eq!(
            sweep_with(&dag, 600, &streams, &undelivered).await,
            1,
            "60 minutes is three stall windows: a queue that has not moved in that long \
             is not about to get to this row"
        );
        assert_eq!(
            dag.get_task("stranded").await.unwrap().status,
            TaskStatus::Pending
        );
        assert_eq!(
            metrics.recorded(cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG),
            1,
            "this reclaim is the one that has to be visible apart from the ordinary total"
        );
    }

    /// 池子自己宣布停摆时，积压上限不再是解释的终点。
    ///
    /// 与上一条同一个年龄、同一份积压，只多一格「池子不可用」：这一趟必须一条
    /// 都不收回。上限的判据是「队列这么久没动，就不是慢」，而池子停摆时队列不动
    /// 有第三种解释——能跑它的东西关了。这时收回换不到任何进展：准入对同一份
    /// 读数已经给出零，收回的那条既不会跑，消息也还在队伍里，等于给一条排空的
    /// 队列再造一份副本，等池子回来时可能跑两遍。
    #[tokio::test]
    async fn a_declared_pool_outage_outlasts_the_backlog_cap() {
        let dag = DagExecutor::new("ws-stalled-pool-down".into());
        dag.add_task(Task::new(
            "waiting_on_pool",
            TaskType::LlmCall,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        dag.schedule_task("waiting_on_pool").await.unwrap();
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("waiting_on_pool").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(60);
        }
        let metrics = Arc::new(RecordingMetrics::default());
        dag.attach_metrics(metrics.clone());
        let streams = served("ws-stalled-pool-down", true);
        let stream = crate::ready_queue::ready_stream("ws-stalled-pool-down");

        let mut undelivered = HashMap::new();
        undelivered.insert(stream, Some(8_815u64));
        assert_eq!(
            dag.reclaim_stalled_scheduled(600, &streams, &undelivered, 3, true)
                .await,
            0,
            "an hour past the cap is still the pool's fault while the pool says it is down: \
             re-arming republishes a message that is already in line"
        );
        let still = dag.get_task("waiting_on_pool").await.unwrap();
        assert_eq!(still.status, TaskStatus::Scheduled);
        assert_eq!(still.retry_count, 0);
        assert_eq!(
            metrics.recorded(cog_core::metric_names::DAG_STALLED_SCHEDULED_DEFERRED_FOR_BACKLOG),
            1,
            "the wait still has to be counted, or it reads as a sweep that never ran"
        );
        assert_eq!(
            metrics.recorded(cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED_WITH_BACKLOG),
            0,
            "nothing was taken back, so the reclaim-with-backlog curve must not move"
        );

        // 池子回来的那一拍，同一个年龄重新按上限判：队列还是那条队列，
        // 解释没了，收回照旧。
        assert_eq!(
            dag.reclaim_stalled_scheduled(600, &streams, &undelivered, 3, false)
                .await,
            1,
            "once the pool is back the cap applies again"
        );
        assert_eq!(
            dag.get_task("waiting_on_pool").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    /// 传输层答不出（`None`）不等于队列是空的：照旧走普通路径。
    #[tokio::test]
    async fn an_unreadable_queue_is_not_an_empty_one() {
        let dag = DagExecutor::new("ws-stalled-unknown".into());
        dag.add_task(Task::new(
            "stalled",
            TaskType::LlmCall,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        dag.schedule_task("stalled").await.unwrap();
        {
            let mut inner = dag.inner.write().await;
            inner.tasks.get_mut("stalled").unwrap().updated_at =
                chrono::Utc::now() - chrono::Duration::minutes(30);
        }
        let streams = served("ws-stalled-unknown", true);
        let stream = crate::ready_queue::ready_stream("ws-stalled-unknown");

        let mut undelivered = HashMap::new();
        undelivered.insert(stream, None);
        assert_eq!(
            sweep_with(&dag, 600, &streams, &undelivered).await,
            1,
            "a backend that cannot answer must not inherit a made-up zero, nor a made-up \
             backlog: silence keeps the ordinary behaviour"
        );
    }

    /// 一个在 `Scheduled` 上停多久的任务都不会被这条扫描判终态，哪怕它的预算
    /// 早就用满：停在那个状态说明它**没被跑过**，清扫器读到的只有年纪，而年纪
    /// 对「消息排在队伍里」与「消息丢了」同值。判不出的代价不能记在任务头上，
    /// 所以它只被放回队列，下游也不会被级联取消。
    #[tokio::test]
    async fn a_stalled_task_with_a_spent_budget_is_still_not_judged() {
        let dag = DagExecutor::new("ws-stalled-budget".into());
        dag.add_task(Task::new(
            "stalled",
            TaskType::LlmCall,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
        // 指向已经在图里的那一笔：反向边只为已存在的依赖记。
        let mut downstream = Task::new("downstream", TaskType::LlmCall, serde_json::json!({}));
        downstream.blocked_by = vec!["stalled".into()];
        dag.add_task(downstream).await.unwrap();
        dag.schedule_task("stalled").await.unwrap();
        let budget = dag.retry_matrix().max_retries(&TaskType::LlmCall);
        {
            let mut inner = dag.inner.write().await;
            let task = inner.tasks.get_mut("stalled").unwrap();
            task.updated_at = chrono::Utc::now() - chrono::Duration::minutes(30);
            task.retry_count = budget;
        }

        assert_eq!(
            sweep(&dag, 600, &served("ws-stalled-budget", true)).await,
            1
        );

        let stalled = dag.get_task("stalled").await.unwrap();
        assert_eq!(stalled.status, TaskStatus::Pending);
        assert_eq!(stalled.retry_count, budget, "still not one attempt charged");
        assert_eq!(
            dag.get_task("downstream").await.unwrap().status,
            TaskStatus::Pending,
            "a task that has not failed cannot cancel what waits on it"
        );
    }

    /// 扫描在每一个会发布的部署里都跑（就绪队列是共享的，谁都能发），所以
    /// "读到还在 Scheduled 就动手"这中间有一段扫描自己关不上的缝：两个进程可以
    /// 同时认定同一个任务卡住。这里让两个进程先后扫同一份存储，证明动作挂在状态
    /// 迁移上，只有一个能落，另一个不会把已经在路上的那一笔第二次挪走。
    #[tokio::test]
    async fn a_shared_store_re_arms_a_stalled_task_once_for_the_whole_cluster() {
        let backend: Arc<dyn StateBackend> = Arc::new(cog_test_support::MemoryStateBackend::new());
        let pod_a = DagExecutor::new("ws-stalled-fg".into()).with_state_backend(backend.clone());
        let pod_b = DagExecutor::new("ws-stalled-fg".into()).with_state_backend(backend);

        pod_a
            .add_task(Task::new("t-fg", TaskType::LlmCall, serde_json::json!({})))
            .await
            .unwrap();
        pod_a.schedule_task("t-fg").await.unwrap();
        let mut aged = pod_a.get_task("t-fg").await.unwrap();
        aged.updated_at = chrono::Utc::now() - chrono::Duration::minutes(30);
        pod_a
            .state_backend
            .as_ref()
            .unwrap()
            .dag_transition_task("ws-stalled-fg", "t-fg", &[TaskStatus::Scheduled], &aged)
            .await
            .unwrap();

        assert_eq!(sweep(&pod_a, 600, &served("ws-stalled-fg", true)).await, 1);
        assert_eq!(
            sweep(&pod_b, 600, &served("ws-stalled-fg", true)).await,
            0,
            "the second sweep finds the task already requeued and leaves it alone"
        );
        let after = pod_b.get_task("t-fg").await.unwrap();
        assert_eq!(after.status, TaskStatus::Pending);
        assert_eq!(after.retry_count, 0);
    }

    /// 产出侧的词表必须在没有任何任务时也落到指标面上。
    ///
    /// 这是「链接上了、只是没活干」唯一能被读到的形状：产出侧的格子只在真有任务
    /// 在跑时才加值，没有任务的部署里整条计数器查不到——而那与「产出侧从没接上」
    /// 同形，正是这条链失败时的样子。
    #[tokio::test]
    async fn the_producer_publishes_its_outcome_vocabulary_before_any_work_arrives() {
        use std::collections::BTreeSet;

        let metrics = Arc::new(cog_test_support::MemoryMetricsBackend::new());
        let executor = DagExecutor::new("ws-vocabulary".to_string());
        executor.attach_metrics(metrics.clone());

        executor.publish_checkpoint_outcomes().await;

        let totals = metrics
            .query_counter_totals(cog_core::metric_names::TASK_CHECKPOINT.as_str())
            .await
            .expect("the published counter totals");
        let published: BTreeSet<&str> = totals
            .iter()
            .map(|sample| {
                sample
                    .labels
                    .get(crate::LABEL_OUTCOME)
                    .map(String::as_str)
                    .unwrap_or("")
            })
            .collect();
        let expected: BTreeSet<&str> = task_checkpoint::CHECKPOINT_OUTCOMES
            .iter()
            .copied()
            .collect();
        assert_eq!(published, expected, "each outcome is a series of its own");
        assert!(
            totals.iter().all(|sample| sample.value == 0.0),
            "the vocabulary lands at zero; the rounds add the values"
        );
    }

    /// 行里带的界与判决用的界是两个数，差在失败类上：靠重跑清除不了的失败拿
    /// 不到预算，因为重跑改变不了它。判据钉在这里——哪天有人让判决改读行里那
    /// 个字段，这条会红。
    #[test]
    fn a_failure_a_rerun_cannot_clear_is_judged_by_no_budget() {
        let orchestrator = DagExecutor::new("ws".to_string());
        let task_type = TaskType::Custom("change_ledger".to_string());
        let declared = orchestrator.retry_matrix().max_retries(&task_type);
        assert!(
            declared > 0,
            "the case only means anything while the declared budget is a real one"
        );

        // 暂态失败：判决用的就是声明值。
        assert_eq!(
            orchestrator.effective_max_retries(
                "upstream_unavailable",
                Some(UpstreamFailure::ServerError),
                &task_type,
            ),
            declared
        );

        // 判死的失败：声明值一格不动，判决用的是 0。
        assert_eq!(
            orchestrator.effective_max_retries(
                "terminal_env_failure: environment_error",
                Some(UpstreamFailure::Auth),
                &task_type,
            ),
            0
        );
    }

    /// 播种用的那一串 kind 与 `event_kind` 的 match 是同一份规格的两处写法：
    /// 这里逐个造一个变体走一遍 match，把结果与那串比。任一边加了变体而另一边
    /// 没跟上，`increase()` 都会给它留下一条永远缺席的序列——而未写的格与
    /// 值为 0 的格在读数面上同形。
    #[test]
    fn the_seeded_kind_set_is_what_event_kind_produces() {
        let now = chrono::Utc::now();
        let events = vec![
            cog_core::TaskEvent::TaskCreated {
                task_id: "t".into(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskScheduled {
                task_id: "t".into(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskStarted {
                task_id: "t".into(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskCompleted {
                task_id: "t".into(),
                result: None,
                scheduled_dependents: Vec::new(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskFailed {
                task_id: "t".into(),
                error: "e".into(),
                retried: false,
                cancelled: Vec::new(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskCancelled {
                task_id: "t".into(),
                reason: "r".into(),
                timestamp: now,
            },
            cog_core::TaskEvent::TaskRetried {
                task_id: "t".into(),
                retry_count: 1,
                timestamp: now,
            },
            cog_core::TaskEvent::TaskTimeout {
                task_id: "t".into(),
                timeout_seconds: 1,
                timestamp: now,
            },
            cog_core::TaskEvent::TaskLeaseExpired {
                task_id: "t".into(),
                owner: None,
                expired_at: now,
                timestamp: now,
            },
        ];

        let produced: HashSet<&str> = events.iter().map(DagExecutor::event_kind).collect();
        let seeded: HashSet<&str> = DagExecutor::TASK_EVENT_KINDS.into_iter().collect();

        assert_eq!(
            produced.len(),
            events.len(),
            "两个变体映射到同一个 kind：播种会把两条序列当成一条"
        );
        assert_eq!(
            produced, seeded,
            "播种的 kind 串与 event_kind 的 match 漂开了：差额的那一个会永远缺席"
        );
    }
}
