//! DAG 执行器编排核心：任务状态、租约、续跑与自修复都在这里汇合。
use cog_core::{SFError, SFResult, Task, TaskStatus, UpstreamFailure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::circuit_registry::CircuitBreakerRegistry;
use super::retry_matrix::RetryMatrix;
use super::task_checkpoint::{self, checkpoint_task, CheckpointAgents, CheckpointRound};
use super::task_phase::PhasedTask;
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

pub struct DagExecutor {
    workspace_id: String,
    /// 本进程的运行身份，任务租约的持有者。每次构造新产生一个值，所以它随进程
    /// 消亡而失效——这正是「上一任持有者已经不在了」可以被读出来的依据。
    run_id: String,
    /// 任务租约时长；持有者按它的三分之一续期。
    task_lease: chrono::Duration,
    inner: RwLock<Inner>,
    retry_matrix: RetryMatrix,
    dlq: Option<Box<dyn DeadLetterQueue>>,
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
        }
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
    fn metrics_backend(&self) -> Option<Arc<dyn cog_core::MetricsBackend>> {
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
    async fn record_stalled_scheduled_reclaimed(&self, count: usize) {
        if count == 0 {
            return;
        }
        let Some(backend) = self.metrics_backend() else {
            tracing::debug!(
                count,
                "reclaimed stalled scheduled tasks; no metrics backend attached"
            );
            return;
        };
        if let Err(e) = backend
            .record_counter(
                cog_core::metric_names::DAG_STALLED_SCHEDULED_RECLAIMED,
                count as f64,
                HashMap::new(),
            )
            .await
        {
            tracing::warn!(error = %e, "cannot record the stalled-scheduled reclaim count");
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
    /// Returns how many tasks it re-armed.
    pub async fn reclaim_stalled_scheduled(&self, stall_after_secs: u64) -> usize {
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
        for task in stalled {
            let error = format!(
                "never started: the task sat in Scheduled for more than {stall_after_secs}s without ever being started, so it was put back in line without charging an attempt"
            );
            match self.reclaim_one_stalled(&task.id, error).await {
                Ok(true) => reclaimed += 1,
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    task_id = %task.id,
                    "cannot reclaim a task stalled in Scheduled: {e}"
                ),
            }
        }
        self.record_stalled_scheduled_reclaimed(reclaimed).await;
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
        });
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

    fn emit_event(&self, event: cog_core::TaskEvent) {
        if let Some(ref tx) = self.event_tx {
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

    pub fn with_dlq(mut self, dlq: Box<dyn DeadLetterQueue>) -> Self {
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
        self.dlq.as_ref().map(|b| b.as_ref())
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
    /// state on cycle detection. Existing tasks with duplicate IDs are
    /// idempotently skipped.
    pub async fn add_tasks_batch(&self, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        if self.fg().is_some() {
            return self.add_tasks_batch_store(tasks).await;
        }
        let mut inner = self.inner.write().await;
        // Phase 1: collect new tasks and their dependencies, skipping duplicates
        let mut validated: Vec<(String, HashSet<String>, Task)> = Vec::new();
        for task in tasks {
            let task_id = task.id.clone();
            if inner.tasks.contains_key(&task_id) {
                continue; // Idempotent skip
            }
            let deps: HashSet<String> = task.blocked_by.iter().cloned().collect();
            validated.push((task_id, deps, task));
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
            });
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
        });
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
        if let Some(be) = self.fg() {
            be.dag_get_all_tasks(&self.workspace_id)
                .await
                .unwrap_or_default()
        } else {
            self.inner.read().await.tasks.values().cloned().collect()
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
            self.emit_event(ev);
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

        let mut validated = Vec::new();
        for task in tasks {
            if graph.contains_key(&task.id) {
                continue; // 幂等跳过
            }
            graph.insert(task.id.clone(), task.blocked_by.iter().cloned().collect());
            validated.push(task);
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
            });
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
        });
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

        // 与内存路径同一个判据：不能靠重跑清除的失败不给重试预算。
        let max_retries = if Self::is_retryable_failure(&error, cause) {
            self.retry_matrix.max_retries(&task.task_type)
        } else {
            0
        };
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
            });
        }
        self.emit_event(cog_core::TaskEvent::TaskFailed {
            task_id: task_id.into(),
            error,
            retried,
            cancelled: cancelled.clone(),
            timestamp: chrono::Utc::now(),
        });
        crate::observable::global_observable().record_task(false);
        let dlq_pushed = !retried && self.dlq.is_some();
        Ok((retried, cancelled, dlq_pushed))
    }

    async fn cancel_task_store(&self, task_id: &str) -> SFResult<Vec<String>> {
        let be = self.fg().expect("store mode");
        let cancelled = be
            .dag_cancel_task(&self.workspace_id, task_id, "Direct cancellation".into())
            .await?;
        self.emit_event(cog_core::TaskEvent::TaskCancelled {
            task_id: task_id.into(),
            reason: "Direct cancellation".into(),
            timestamp: chrono::Utc::now(),
        });
        for dep_id in &cancelled {
            self.emit_event(cog_core::TaskEvent::TaskCancelled {
                task_id: dep_id.clone(),
                reason: format!(
                    "Cascade cancelled: upstream task '{}' was cancelled",
                    task_id
                ),
                timestamp: chrono::Utc::now(),
            });
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
            self.emit_reclaim(&t, &cause);
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
        });
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
        let tasks = self.all_tasks_unified().await;
        let by_id: std::collections::HashMap<&str, &Task> =
            tasks.iter().map(|t| (t.id.as_str(), t)).collect();
        let now = chrono::Utc::now();
        let mut ready: Vec<Task> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Pending)
            .filter(|t| t.is_executable)
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
        ready
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
        });
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
        });
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()> {
        if self.fg().is_some() {
            return self
                .store_transition(
                    task_id,
                    &[TaskStatus::Pending, TaskStatus::Scheduled],
                    "Cannot assign task in {} state",
                    |t| t.agent_id = Some(agent_id.into()),
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

        if task.status != TaskStatus::Pending && task.status != TaskStatus::Scheduled {
            return Err(cog_core::SFError::TaskFailed {
                task_id: task_id.into(),
                reason: format!("Cannot assign task in {:?} state", task.status),
            });
        }

        task.agent_id = Some(agent_id.into());
        let task_snapshot = task.clone();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskScheduled {
            task_id: task_id.into(),
            timestamp: chrono::Utc::now(),
        });
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn start_task(&self, task_id: &str) -> SFResult<()> {
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
        });
        self.persist_task_fine_grained(&task_snapshot).await;
        self.persist_state().await;
        Ok(())
    }

    pub async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        if self.fg().is_some() {
            return self.complete_task_store(task_id, result).await;
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
        });

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
            return self
                .fail_task_store(task_id, error, cause, retry_after_secs)
                .await;
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
            });
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
        let retryable = Self::is_retryable_failure(&error, cause);
        let max_retries = if retryable {
            self.retry_matrix.max_retries(&task_type)
        } else {
            0
        };

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
            });
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
                        });
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
            });

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
        if self.fg().is_some() {
            return self.cancel_task_store(task_id).await;
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

        let task_snapshot = task.clone();
        drop(inner);
        self.emit_event(cog_core::TaskEvent::TaskCancelled {
            task_id: task_id.into(),
            reason: "Direct cancellation".into(),
            timestamp: chrono::Utc::now(),
        });
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
                    });
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
        });
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

            self.emit_reclaim(&task, &cause);

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
                });
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
            let round = checkpoint_task(&task_id, agents, &state).await;
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
        let labels = HashMap::from([("outcome".to_string(), outcome.to_string())]);
        if let Err(e) = backend
            .record_counter(cog_core::metric_names::TASK_CHECKPOINT, value, labels)
            .await
        {
            tracing::warn!(error = %e, outcome, "cannot record the task checkpoint count");
        }
    }

    fn emit_reclaim(&self, task: &Task, cause: &ReclaimCause) {
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
        self.emit_event(event);
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

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{
        AgentState, ContextBoard, Event, MetricsBackend, StateBackend, TaskCheckpoint, TaskType,
    };

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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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

        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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
            dag.reclaim_stalled_scheduled(60).await,
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

        assert_eq!(dag.reclaim_stalled_scheduled(600).await, 1);

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
        assert_eq!(dag.reclaim_stalled_scheduled(600).await, 0);
        assert_eq!(dag.get_task("stalled").await.unwrap().retry_count, 0);

        let fresh = dag.get_task("fresh").await.unwrap();
        assert_eq!(fresh.status, TaskStatus::Scheduled);
        assert_eq!(fresh.retry_count, 0);
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

        assert_eq!(dag.reclaim_stalled_scheduled(600).await, 1);

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
        let backend: Arc<dyn StateBackend> = Arc::new(cog_storage::MemoryStateBackend::new());
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

        assert_eq!(pod_a.reclaim_stalled_scheduled(600).await, 1);
        assert_eq!(
            pod_b.reclaim_stalled_scheduled(600).await,
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

        let metrics = Arc::new(cog_storage::MemoryMetricsBackend::new());
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
                    .get("outcome")
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
}
