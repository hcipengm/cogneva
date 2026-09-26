use cog_core::{SFError, SFResult, Task, TaskStatus, UpstreamFailure};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::circuit_registry::CircuitBreakerRegistry;
use super::retry_matrix::RetryMatrix;
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
        }
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
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let beat = cog_core::loop_health::register(
                ARCHIVE_LOOP,
                cog_core::loop_health::Cadence::Periodic(interval.period()),
            );
            // Nothing hands this loop a stop signal: it is started for the life of
            // the process, so every way it can end — including a clean return —
            // leaves the archiving undone.
            let _mortality = beat.watch_death_unconditionally();
            loop {
                beat.beat();
                interval.tick().await;
                this.archive_terminated_tasks().await;
            }
        });
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
    use cog_core::{AgentState, ContextBoard, Event, StateBackend, TaskCheckpoint, TaskType};

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
    /// 所以这个原因不能被读成终止性失败，否则一次部署就会把这条链永久封死。
    #[tokio::test]
    async fn a_lease_expired_task_is_reclaimed_and_stays_retryable() {
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
        assert!(results[0].1, "租约过期必须回到重试预算里，不是被判死");

        let view = dag.get_task("t-expired").await.unwrap();
        assert_eq!(view.status, TaskStatus::Pending);
        let reason = view.error.expect("a failure reason");
        assert!(reason.contains("run lease expired"), "{reason}");
        assert!(
            !cog_core::contract::outcome::is_deterministic_failure(&reason),
            "部署换人不是确定性失败；标成确定性会让 discovery 把这条意图永久 Blocked 掉"
        );
        assert!(view.lease_owner.is_none(), "回收后租约要清干净");
        assert!(
            view.retry_not_before
                .is_some_and(|due| due > chrono::Utc::now()),
            "回 Pending 的任务带着退避死线"
        );
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
}
