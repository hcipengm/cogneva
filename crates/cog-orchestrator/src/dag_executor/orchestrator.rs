use cog_core::{SFError, SFResult, Task, TaskStatus};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};
use uuid::Uuid;

use super::circuit_registry::CircuitBreakerRegistry;
use super::retry_matrix::RetryMatrix;
use super::task_phase::PhasedTask;
use cog_core::{DeadLetterEntry, DeadLetterQueue, RetryAttempt, SuggestedAction};

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

impl DagExecutor {
    pub fn new(workspace_id: String) -> Self {
        Self {
            workspace_id,
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

    /// Load state from the configured backend, if any.
    pub async fn load_from_backend(&self) -> SFResult<bool> {
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
            loop {
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
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        let mut path = Vec::new();

        for task_id in inner.dependencies.keys() {
            if !visited.contains(task_id) {
                if let Some(cycle) =
                    Self::dfs_cycle(task_id, inner, &mut visited, &mut stack, &mut path)
                {
                    return Some(cycle);
                }
            }
        }
        None
    }

    fn dfs_cycle(
        node: &str,
        inner: &Inner,
        visited: &mut HashSet<String>,
        stack: &mut HashSet<String>,
        path: &mut Vec<String>,
    ) -> Option<Vec<String>> {
        visited.insert(node.to_string());
        stack.insert(node.to_string());
        path.push(node.to_string());

        if let Some(deps) = inner.dependencies.get(node) {
            for dep in deps {
                if !visited.contains(dep) {
                    if let Some(cycle) = Self::dfs_cycle(dep, inner, visited, stack, path) {
                        return Some(cycle);
                    }
                } else if stack.contains(dep) {
                    let idx = path.iter().position(|p| p == dep).unwrap_or(0);
                    let cycle = path[idx..].to_vec();
                    return Some(cycle);
                }
            }
        }

        path.pop();
        stack.remove(node);
        None
    }

    pub async fn find_ready_tasks(&self) -> Vec<Task> {
        let inner = self.inner.read().await;
        let mut ready: Vec<Task> = inner
            .tasks
            .values()
            .filter(|t| t.status == TaskStatus::Pending)
            .filter(|t| t.is_executable)
            .filter(|t| {
                t.blocked_by.iter().all(|dep_id| {
                    inner
                        .tasks
                        .get(dep_id)
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
        let inner = self.inner.read().await;
        let mut ready: Vec<Task> = inner
            .tasks
            .values()
            .filter(|t| t.status == TaskStatus::Pending || t.status == TaskStatus::Scheduled)
            .filter(|t| {
                t.blocked_by.iter().all(|dep_id| {
                    inner
                        .tasks
                        .get(dep_id)
                        .map(|dep| dep.status == TaskStatus::Completed)
                        .unwrap_or(false)
                })
            })
            .cloned()
            .collect();
        ready.sort_by_key(|t| -t.priority);
        ready
    }

    pub async fn schedule_task(&self, task_id: &str) -> SFResult<()> {
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

        task.status = TaskStatus::Running;
        task.started_at = Some(chrono::Utc::now());
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
                    if let Some(t) = inner.tasks.get_mut(&dep_id) {
                        t.status = TaskStatus::Scheduled;
                        scheduled.push(dep_id.clone());
                        drop(inner);
                        self.emit_event(cog_core::TaskEvent::TaskScheduled {
                            task_id: dep_id,
                            timestamp: chrono::Utc::now(),
                        });
                        inner = self.inner.write().await;
                    }
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

    /// Fail a task with the given error.
    /// Returns `(retried, cancelled, dlq_pushed)` where:
    /// - `retried` = `true` if the task was sent back to Pending for retry
    /// - `cancelled` = list of downstream tasks cascade-cancelled
    /// - `dlq_pushed` = `true` if the task was moved to DLQ on final failure
    pub async fn fail_task(
        &self,
        task_id: &str,
        error: String,
    ) -> SFResult<(bool, Vec<String>, bool)> {
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

        let max_retries = self.retry_matrix.max_retries(&task_type);

        let task = inner.tasks.get_mut(task_id).expect("task exists");
        if retry_count < max_retries {
            task.retry_count = retry_count + 1;
            task.status = TaskStatus::Pending;
            task.error = Some(error.clone());
            task.updated_at = chrono::Utc::now();

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
            task.updated_at = chrono::Utc::now();

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
            let inner = self.inner.read().await;
            let task = inner
                .tasks
                .get(task_id)
                .ok_or_else(|| SFError::TaskFailed {
                    task_id: task_id.into(),
                    reason: "Task not found".into(),
                })?;

            let history = inner
                .retry_history
                .get(task_id)
                .cloned()
                .unwrap_or_default();

            let entry = DeadLetterEntry {
                original_task_id: task_id.into(),
                task: task.clone(),
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
        task.started_at = None;
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
        let inner = self.inner.read().await;
        !inner.tasks.is_empty()
            && inner
                .tasks
                .values()
                .all(|t| t.status == TaskStatus::Completed)
    }

    pub async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        let now = chrono::Utc::now();
        let task_ids: Vec<String> = {
            let inner = self.inner.read().await;
            inner
                .tasks
                .values()
                .filter(|t| t.status == TaskStatus::Running)
                .filter(|t| {
                    t.started_at
                        .map(|s| (now - s).num_seconds() > t.timeout_seconds as i64)
                        .unwrap_or(false)
                })
                .map(|t| t.id.clone())
                .collect()
        };

        let mut results = Vec::new();
        for task_id in task_ids {
            let timeout_seconds = {
                let inner = self.inner.read().await;
                inner
                    .tasks
                    .get(&task_id)
                    .map(|t| t.timeout_seconds)
                    .unwrap_or(0)
            };

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
                tracing::info!(task_id = %task_id, "Task no longer Running, skipping timeout");
                continue;
            }

            self.emit_event(cog_core::TaskEvent::TaskTimeout {
                task_id: task_id.clone(),
                timeout_seconds,
                timestamp: chrono::Utc::now(),
            });

            if let Ok((retried, cancelled, dlq_pushed)) = self
                .fail_task(
                    &task_id,
                    format!("Task timed out after {} seconds", timeout_seconds),
                )
                .await
            {
                if retried {
                    let mut inner = self.inner.write().await;
                    if let Some(t) = inner.tasks.get_mut(&task_id) {
                        t.started_at = None;
                    }
                }
                results.push((task_id, retried, cancelled, dlq_pushed));
            }
        }

        results
    }

    pub async fn get_task(&self, task_id: &str) -> Option<Task> {
        let inner = self.inner.read().await;
        inner.tasks.get(task_id).cloned()
    }

    pub async fn get_all_tasks(&self) -> Vec<Task> {
        let inner = self.inner.read().await;
        inner.tasks.values().cloned().collect()
    }

    pub async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>> {
        let inner = self.inner.read().await;
        inner.dependents.get(task_id).map(|ids| {
            ids.iter()
                .filter_map(|id| inner.tasks.get(id).cloned())
                .collect()
        })
    }

    pub async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>> {
        let inner = self.inner.read().await;
        inner.dependencies.get(task_id).map(|ids| {
            ids.iter()
                .filter_map(|id| inner.tasks.get(id).cloned())
                .collect()
        })
    }

    pub async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        let inner = self.inner.read().await;
        let nodes: Vec<Task> = inner.tasks.values().cloned().collect();
        let mut edges = Vec::new();
        for (task_id, deps) in &inner.dependencies {
            for dep_id in deps {
                edges.push((dep_id.clone(), task_id.clone()));
            }
        }
        (nodes, edges)
    }

    pub async fn delete_task(&self, task_id: &str) -> SFResult<()> {
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
        let inner = self.inner.read().await;
        inner.retry_history.get(task_id).cloned()
    }

    /// Crew-level AND semantics hook.
    /// When one task in a crew (squad) enters the DLQ, the crew may
    /// trigger a retry rather than immediately failing.  This method
    /// returns `true` if *any* task in the given set is still
    /// retryable (has not exhausted its retries).
    pub async fn crew_can_retry(&self, task_ids: &[String]) -> bool {
        let inner = self.inner.read().await;
        task_ids.iter().any(|id| {
            inner.tasks.get(id).is_some_and(|t| {
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

    async fn fail_task(&self, task_id: &str, error: String) -> SFResult<(bool, Vec<String>, bool)> {
        self.fail_task(task_id, error).await
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
