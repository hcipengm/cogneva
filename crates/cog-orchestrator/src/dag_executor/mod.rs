use std::sync::Arc;

use cog_core::{DagMessage, MessageBackend, SFError, SFResult, ShutdownSignal, Task};
use futures::StreamExt;

pub mod circuit_registry;
pub mod orchestrator;
pub mod retry_matrix;
pub mod task_phase;
pub mod task_transfer;

pub use circuit_registry::CircuitBreakerRegistry;
pub use orchestrator::DagExecutor;
pub use retry_matrix::{BackoffStrategy, CircuitBreakerConfig, RetryConfig, RetryMatrix};
pub use task_phase::{ExitCriteria, PhaseTransitionRules, PhasedTask, TaskPhase};
pub use task_transfer::{
    RecoveredTask, StaleTaskDetector, TaskTransferCoordinator, TaskTransferEvent, TransferReason,
    TASK_TRANSFER_STREAM,
};

/// DagExecutor 配置。
#[derive(Debug, Clone)]
pub struct DagExecutorConfig {
    pub redis_url: String,
    pub workspace_id: String,
    pub consumer_group: String,
    pub max_retries: u32,
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
    /// Each ready task is transitioned to `Scheduled` and then all tasks are
    /// published in a single batch via [`MessageBackend::publish_batch`] for
    /// lower latency and higher throughput.
    pub async fn publish_ready_tasks(&self) -> SFResult<()> {
        let ready_tasks: Vec<Task> = self.orchestrator.find_ready_tasks().await;
        if ready_tasks.is_empty() {
            return Ok(());
        }

        let ready_stream = format!("orchestrator:ready:{}", self.config.workspace_id);
        let mut payloads = Vec::with_capacity(ready_tasks.len());

        for task in ready_tasks {
            if let Err(e) = self.orchestrator.schedule_task(&task.id).await {
                tracing::warn!(task_id = %task.id, "schedule_task failed during publish: {e}");
                continue;
            }
            let payload = serde_json::to_vec(&task).map_err(SFError::Serialization)?;
            payloads.push(payload);
        }

        if !payloads.is_empty() {
            self.backend.publish_batch(&ready_stream, &payloads).await?;
        }
        Ok(())
    }

    /// Consume task-result messages from the backend and drive DAG state transitions.
    /// Listens on `orchestrator:results:{workspace_id}` for [`DagMessage::TaskComplete`]
    /// and [`DagMessage::TaskFailed`] events, calling `complete_task` / `fail_task`
    /// on the embedded [`DagExecutor`].  After each state change, newly-ready
    /// dependents are automatically published via [`Self::publish_ready_tasks`].
    /// # Graceful shutdown
    /// Pass a [`ShutdownSignal`] to cleanly exit on the next iteration boundary.
    pub async fn run_consumer(&self, shutdown: ShutdownSignal) -> SFResult<()> {
        let result_stream = format!("orchestrator:results:{}", self.config.workspace_id);
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

        let mut stream = self.backend.subscribe(&result_stream, &group_name).await?;

        loop {
            let next = tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                msg = stream.next() => msg,
            };

            let (_msg_id, bytes) = match next {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => break,
            };

            let msg: DagMessage = match serde_json::from_slice(&bytes) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("Failed to deserialize DagMessage: {e}");
                    continue;
                }
            };

            match msg {
                DagMessage::TaskComplete {
                    task_id, result, ..
                } => {
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
                        Err(e) => {
                            tracing::warn!(task_id = %task_id, "complete_task failed: {e}");
                        }
                    }
                    if let Err(e) = self.publish_ready_tasks().await {
                        tracing::warn!("publish_ready_tasks after complete failed: {e}");
                    }
                }
                DagMessage::TaskFailed { task_id, error, .. } => {
                    match self.orchestrator.fail_task(&task_id, error.clone()).await {
                        Ok((retried, cancelled, _dlq_pushed)) => {
                            tracing::warn!(
                                task_id = %task_id,
                                retried,
                                cancelled = cancelled.len(),
                                "Task failed via message queue"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(task_id = %task_id, "fail_task failed: {e}");
                        }
                    }
                    if let Err(e) = self.publish_ready_tasks().await {
                        tracing::warn!("publish_ready_tasks after fail failed: {e}");
                    }
                }
                _ => {
                    tracing::debug!("Ignoring non-result DagMessage variant");
                }
            }
        }

        Ok(())
    }

    /// Consume goal messages from the `goals:{workspace_id}` stream and inject
    /// them into the DAG.
    /// Each [`cog_core::GoalMessage`] is deserialized, deduplicated by
    /// `message_id`, and submitted via [`Self::submit_goal`].  After submission,
    /// newly-ready tasks are automatically published to the ready stream.
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
        let mut stream = self.backend.subscribe(&goal_stream, &group).await?;

        loop {
            let next = tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                msg = stream.next() => msg,
            };

            let (_msg_id, bytes) = match next {
                Some(Ok(v)) => v,
                Some(Err(e)) => return Err(e),
                None => break,
            };

            let goal: cog_core::GoalMessage = match serde_json::from_slice(&bytes) {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!("Failed to deserialize GoalMessage: {e}");
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
                (&self.action_planner, &self.skill_registry)
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
                    Err(e) => {
                        tracing::warn!(
                            goal_id = %goal.message_id,
                            "ActionPlanner processing failed: {e}"
                        );
                        continue;
                    }
                }
            } else {
                // Fallback: direct DagExecutor submission when ActionPlanner unavailable.
                if let Err(e) = self.submit_goal(&goal.goal, goal.tasks).await {
                    tracing::warn!(goal_id = %goal.message_id, "submit_goal failed: {e}");
                    continue;
                }
            }

            if let Err(e) = self.publish_ready_tasks().await {
                tracing::warn!(goal_id = %goal.message_id, "publish_ready_tasks after goal failed: {e}");
            }
        }

        Ok(())
    }
}
