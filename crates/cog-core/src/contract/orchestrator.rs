use crate::{SFResult, Task};
use async_trait::async_trait;

/// Abstract interface for orchestrating tasks without hard-coupling to
/// `sf-orchestrator::DagExecutor`.
/// Intended for use by `cog-supervisor` and any other crate that needs to
/// read or mutate the task graph but must not depend on `sf-orchestrator`.
/// All methods take `&self` so that callers can hold a plain
/// `Arc<dyn OrchestratorControl>`; the implementation is responsible for
/// internal synchronisation.
#[async_trait]
pub trait OrchestratorControl: Send + Sync {
    /// Submit a new goal with explicit pre-defined tasks.
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()>;

    /// Submit a goal with optional tasks. If `tasks` is empty, the orchestrator
    /// will automatically decompose the goal into atomic tasks via ActionPlanner.
    /// Returns the list of task IDs that were added to the graph.
    async fn submit_goal_auto(&self, goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>>;

    /// Assign a pending/scheduled task to a specific agent.
    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()>;

    /// Add a single task to the existing graph.
    async fn add_task(&self, task: Task) -> SFResult<()>;

    /// Returns true if any task in the set is still retryable.
    async fn crew_can_retry(&self, task_ids: &[String]) -> bool;

    /// Retry all failed tasks in the given set. Returns the number retried.
    async fn crew_retry_all(&self, task_ids: &[String]) -> usize;

    /// Return all tasks whose dependencies are satisfied and are ready to run.
    async fn get_ready_tasks(&self) -> Vec<Task>;

    /// Return every task currently in the graph.
    async fn get_all_tasks(&self) -> Vec<Task>;

    /// Push a failed task to the dead-letter queue.
    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool>;

    /// Retry a single failed task.
    async fn retry_task(&self, task_id: &str) -> SFResult<()>;

    /// Return the number of items in the dead-letter queue.
    async fn dlq_len(&self) -> SFResult<usize>;

    /// Mark a scheduled task as running.
    async fn start_task(&self, task_id: &str) -> SFResult<()>;

    /// Mark a running task as completed.
    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>>;

    /// Mark a running task as failed and drive retry / cancellation logic.
    /// Returns `(retried, cancelled_ids, dlq_pushed)`.
    async fn fail_task(&self, task_id: &str, error: String) -> SFResult<(bool, Vec<String>, bool)>;

    /// Cancel a task and return cascaded cancellations.
    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>>;

    /// Look up a single task by id.
    async fn get_task(&self, task_id: &str) -> Option<Task>;

    /// Schedule a pending task (move to scheduled state).
    async fn schedule_task(&self, task_id: &str) -> SFResult<()>;

    /// Check for timed-out tasks and return their ids + side-effects.
    /// Tuple is `(task_id, retried, cancelled_ids, dlq_pushed)`.
    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)>;

    /// Return direct dependents of a task.
    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return direct dependencies of a task.
    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return the full task graph as (nodes, edges).
    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>);

    /// Delete a task from the graph.
    async fn delete_task(&self, task_id: &str) -> SFResult<()>;

    /// Return true when every task in the graph is completed.
    async fn all_completed(&self) -> bool;

    /// Replay a task from the DLQ if present. Returns true when replayed.
    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool>;
}

/// Core DAG execution engine interface.
/// Decouples the orchestrator control layer from concrete DAG implementations
/// so that alternative execution backends (distributed, external workflow
/// engines, etc.) can be plugged in without changing control-plane code.
#[async_trait]
pub trait DagExecutor: Send + Sync {
    /// Submit a goal by injecting tasks into the DAG.
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()>;

    /// Batch add tasks. Returns the IDs of tasks that were actually inserted
    /// (duplicates are idempotently skipped).
    async fn add_tasks_batch(&self, tasks: Vec<Task>) -> SFResult<Vec<String>>;

    /// Add a single task. Fails if the task already exists or introduces a cycle.
    async fn add_task(&self, task: Task) -> SFResult<()>;

    // ─── State transitions ───────────────────────────────────────────────

    /// Schedule a pending task.
    async fn schedule_task(&self, task_id: &str) -> SFResult<()>;

    /// Assign a task to a specific agent.
    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()>;

    /// Mark a scheduled task as running.
    async fn start_task(&self, task_id: &str) -> SFResult<()>;

    /// Mark a running task as completed. Returns IDs of newly-scheduled dependents.
    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>>;

    /// Mark a running task as failed. Returns `(retried, cancelled_ids, dlq_pushed)`.
    async fn fail_task(&self, task_id: &str, error: String) -> SFResult<(bool, Vec<String>, bool)>;

    /// Cancel a task and cascade-cancel all downstream dependents.
    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>>;

    /// Retry a failed task (reset to Pending).
    async fn retry_task(&self, task_id: &str) -> SFResult<()>;

    // ─── DLQ operations ──────────────────────────────────────────────────

    /// Push a failed task to the dead-letter queue.
    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool>;

    /// Return the number of items in the DLQ.
    async fn dlq_len(&self) -> SFResult<usize>;

    /// Replay a task from the DLQ back into the DAG.
    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool>;

    // ─── Queries ─────────────────────────────────────────────────────────

    /// Tasks whose dependencies are satisfied and are Pending.
    async fn find_ready_tasks(&self) -> Vec<Task>;

    /// Tasks that are Pending or Scheduled with all deps completed.
    async fn get_ready_tasks(&self) -> Vec<Task>;

    /// All tasks currently in the graph.
    async fn get_all_tasks(&self) -> Vec<Task>;

    /// Look up a single task by id.
    async fn get_task(&self, task_id: &str) -> Option<Task>;

    /// Direct dependents of a task.
    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Direct dependencies of a task.
    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>>;

    /// Return the full task graph as (nodes, edges).
    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>);

    // ─── Bulk / utility ──────────────────────────────────────────────────

    /// Check for timed-out running tasks and drive retry/cancel logic.
    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)>;

    /// Remove a task from the graph.
    async fn delete_task(&self, task_id: &str) -> SFResult<()>;

    /// True when every task in the graph is completed.
    async fn all_completed(&self) -> bool;

    /// True if any task in the set is still retryable.
    async fn crew_can_retry(&self, task_ids: &[String]) -> bool;

    /// Retry all failed tasks in the set. Returns number retried.
    async fn crew_retry_all(&self, task_ids: &[String]) -> usize;
}
