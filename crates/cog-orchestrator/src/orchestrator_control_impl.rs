use async_trait::async_trait;
use cog_core::{OrchestratorControl, SFResult, Task};
use std::sync::Arc;

/// High-level orchestrator control that composes a [`DagExecutor`] and an
/// [`ActionPlanner`] to provide the unified [`OrchestratorControl`] interface.
/// This struct lives in `cog-orchestrator` and delegates DAG state
/// transitions to the internally-mutable [`DagExecutor`] implementation,
/// while `submit_goal_auto` routes through [`ActionPlanner`] for
/// goal decomposition when needed.
pub struct OrchestratorControlImpl {
    dag_executor: Arc<dyn cog_core::DagExecutor>,
    action_planner: Option<Arc<dyn cog_core::ActionPlanner>>,
    skill_registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>>,
}

impl OrchestratorControlImpl {
    pub fn new(dag_executor: Arc<dyn cog_core::DagExecutor>) -> Self {
        Self {
            dag_executor,
            action_planner: None,
            skill_registry: None,
        }
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
}

#[async_trait]
impl OrchestratorControl for OrchestratorControlImpl {
    async fn submit_goal(&self, goal: &str, tasks: Vec<Task>) -> SFResult<()> {
        self.dag_executor.submit_goal(goal, tasks).await
    }

    async fn submit_goal_auto(&self, goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
        if let (Some(ref planner), Some(ref skill_registry)) =
            (&self.action_planner, &self.skill_registry)
        {
            let registry = skill_registry.read().await;
            let ids = planner.process_goal(goal, tasks, &registry).await?;
            return Ok(ids);
        }
        let task_ids: Vec<String> = tasks.iter().map(|t| t.id.clone()).collect();
        self.dag_executor.submit_goal(goal, tasks).await?;
        Ok(task_ids)
    }

    async fn assign_task(&self, task_id: &str, agent_id: &str) -> SFResult<()> {
        self.dag_executor.assign_task(task_id, agent_id).await
    }

    async fn add_task(&self, task: Task) -> SFResult<()> {
        self.dag_executor.add_task(task).await
    }

    async fn crew_can_retry(&self, task_ids: &[String]) -> bool {
        self.dag_executor.crew_can_retry(task_ids).await
    }

    async fn crew_retry_all(&self, task_ids: &[String]) -> usize {
        self.dag_executor.crew_retry_all(task_ids).await
    }

    async fn get_ready_tasks(&self) -> Vec<Task> {
        self.dag_executor.get_ready_tasks().await
    }

    async fn get_all_tasks(&self) -> Vec<Task> {
        self.dag_executor.get_all_tasks().await
    }

    async fn push_to_dlq(&self, task_id: &str, error: String) -> SFResult<bool> {
        self.dag_executor.push_to_dlq(task_id, error).await
    }

    async fn retry_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.retry_task(task_id).await
    }

    async fn dlq_len(&self) -> SFResult<usize> {
        self.dag_executor.dlq_len().await
    }

    async fn start_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.start_task(task_id).await
    }

    async fn complete_task(
        &self,
        task_id: &str,
        result: serde_json::Value,
    ) -> SFResult<Vec<String>> {
        self.dag_executor.complete_task(task_id, result).await
    }

    async fn fail_task(&self, task_id: &str, error: String) -> SFResult<(bool, Vec<String>, bool)> {
        self.dag_executor.fail_task(task_id, error).await
    }

    async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>> {
        self.dag_executor.cancel_task(task_id).await
    }

    async fn get_task(&self, task_id: &str) -> Option<Task> {
        self.dag_executor.get_task(task_id).await
    }

    async fn schedule_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.schedule_task(task_id).await
    }

    async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
        self.dag_executor.check_timeouts().await
    }

    async fn get_dependents(&self, task_id: &str) -> Option<Vec<Task>> {
        self.dag_executor.get_dependents(task_id).await
    }

    async fn get_dependencies(&self, task_id: &str) -> Option<Vec<Task>> {
        self.dag_executor.get_dependencies(task_id).await
    }

    async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
        self.dag_executor.get_graph().await
    }

    async fn delete_task(&self, task_id: &str) -> SFResult<()> {
        self.dag_executor.delete_task(task_id).await
    }

    async fn all_completed(&self) -> bool {
        self.dag_executor.all_completed().await
    }

    async fn replay_dlq(&self, task_id: &str) -> SFResult<bool> {
        self.dag_executor.replay_dlq(task_id).await
    }
}
