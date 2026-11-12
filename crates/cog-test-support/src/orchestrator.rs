//! A stand-in for the orchestrator control plane when the subject under test is a
//! reader over it.
//!
//! The graph readers in the gateway ask two questions of the same thing: what the
//! tasks are, and whether that read succeeded. The production implementation that
//! holds its graph in memory cannot fail a read, so it never exercises the branch
//! where the store behind the graph is down; a stub that can answer both ways is
//! the only way to put that branch under a test.
//!
//! [`TaskGraphStub::whose_read_fails`] keeps the two readings distinguishable on
//! purpose: the fold ([`OrchestratorControl::get_all_tasks`]) answers an empty
//! graph exactly as the production orchestrator does, while the reporting read
//! ([`OrchestratorControl::get_all_tasks_checked`]) says the read failed. A test
//! that only looked at the first would see the same bytes for "no work has ever
//! been queued" and "the store cannot be reached".

use std::sync::Mutex;

use cog_core::{OrchestratorControl, SFError, SFResult, Task, UpstreamFailure};

/// An orchestrator whose graph is whatever the test put in it.
pub struct TaskGraphStub {
    tasks: Mutex<Vec<Task>>,
    /// When set, [`OrchestratorControl::get_all_tasks_checked`] answers this error
    /// and the folding read answers an empty graph -- the shape a store-backed
    /// orchestrator has when its store is unreachable.
    read_failure: Option<String>,
}

impl TaskGraphStub {
    /// A graph with nothing in it, and a read that works.
    pub fn empty() -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
            read_failure: None,
        }
    }

    /// A graph holding exactly these tasks.
    pub fn holding(tasks: Vec<Task>) -> Self {
        Self {
            tasks: Mutex::new(tasks),
            read_failure: None,
        }
    }

    /// A graph whose store cannot be reached: the reporting read fails, the
    /// folding read answers empty.
    pub fn whose_read_fails(reason: impl Into<String>) -> Self {
        Self {
            tasks: Mutex::new(Vec::new()),
            read_failure: Some(reason.into()),
        }
    }

    fn tasks(&self) -> Vec<Task> {
        self.tasks.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl OrchestratorControl for TaskGraphStub {
    async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
        Ok(())
    }

    async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        Ok(tasks.into_iter().map(|task| task.id).collect())
    }

    async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn add_task(&self, task: Task) -> SFResult<()> {
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }

    async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
        false
    }

    async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
        0
    }

    async fn get_ready_tasks(&self) -> Vec<Task> {
        Vec::new()
    }

    async fn get_all_tasks(&self) -> Vec<Task> {
        self.tasks()
    }

    async fn get_all_tasks_checked(&self) -> SFResult<Vec<Task>> {
        match &self.read_failure {
            Some(reason) => Err(SFError::IO(reason.clone())),
            None => Ok(self.tasks()),
        }
    }

    async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
        Ok(false)
    }

    async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn dlq_len(&self) -> SFResult<usize> {
        Ok(0)
    }

    async fn start_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn complete_task(
        &self,
        _task_id: &str,
        _result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn fail_task(
        &self,
        _task_id: &str,
        _error: String,
        _cause: Option<UpstreamFailure>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        Ok((false, Vec::new(), false))
    }

    async fn fail_task_after(
        &self,
        _task_id: &str,
        _error: String,
        _cause: Option<UpstreamFailure>,
        _retry_after_secs: Option<u64>,
    ) -> SFResult<(bool, Vec<String>, bool)> {
        Ok((false, Vec::new(), false))
    }

    async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
        Ok(Vec::new())
    }

    async fn get_task(&self, task_id: &str) -> Option<Task> {
        self.tasks().into_iter().find(|task| task.id == task_id)
    }

    async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        Vec::new()
    }

    async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
        None
    }

    async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
        None
    }

    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        (self.tasks(), Vec::new())
    }

    async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
        Ok(())
    }

    async fn all_completed(&self) -> bool {
        self.tasks().iter().all(|task| task.status.is_terminal())
    }

    async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
        Ok(false)
    }
}
