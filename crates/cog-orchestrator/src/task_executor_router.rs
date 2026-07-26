use async_trait::async_trait;
use cog_core::{
    DagMessage, MessageBackend, OrchestratorControl, SFError, SFResult, ShutdownSignal, Task,
    TaskExecutor, TaskResult, TaskType,
};
use futures::StreamExt;
use std::sync::Arc;

/// Dispatcher that routes ready tasks to the first [`TaskExecutor`] whose
/// [`TaskExecutor::supports`] returns `true`.
/// Decouples the orchestrator from concrete execution crates
/// (`cog-collaboration`, `cog-extension`) so new backends can be registered
/// at start-up without modifying the orchestrator.
#[derive(Clone)]
pub struct TaskExecutorRouter {
    executors: Arc<tokio::sync::RwLock<Vec<Arc<dyn TaskExecutor>>>>,
    orchestrator: Option<Arc<dyn OrchestratorControl>>,
}

impl TaskExecutorRouter {
    pub fn new() -> Self {
        Self {
            executors: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            orchestrator: None,
        }
    }

    pub async fn with_executor(self, executor: Arc<dyn TaskExecutor>) -> Self {
        self.executors.write().await.push(executor);
        self
    }

    pub fn with_orchestrator(mut self, orchestrator: Arc<dyn OrchestratorControl>) -> Self {
        self.orchestrator = Some(orchestrator);
        self
    }

    pub async fn register(&self, executor: Arc<dyn TaskExecutor>) {
        self.executors.write().await.push(executor);
    }

    /// Find a matching executor and run the task.
    /// Returns [`SFError::Agent`] when no registered executor claims the task
    /// type.
    pub async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        let executor = self
            .executors
            .read()
            .await
            .iter()
            .find(|e| e.supports(&task.task_type))
            .cloned()
            .ok_or_else(|| {
                SFError::Agent(format!(
                    "No executor supports task type {:?}",
                    task.task_type
                ))
            })?;
        executor.execute(task).await
    }

    /// Consume ready tasks from the message backend, execute them, and publish
    /// results back to the results stream.
    pub async fn run_consumer(
        &self,
        task_backend: Arc<dyn MessageBackend>,
        result_backend: Arc<dyn MessageBackend>,
        workspace_id: &str,
        shutdown: ShutdownSignal,
    ) -> SFResult<()> {
        let ready_stream = format!("orchestrator:ready:{workspace_id}");
        // Use a stable group name per workspace so that pod restacks do not
        // leak an unbounded number of consumer groups and so messages are not
        // lost when a new pod takes over. The group name intentionally omits
        // a random UUID; Redis Streams consumer groups are persistent and a
        // restarted consumer will resume from the last acknowledged ID.
        let group = format!("executor-loop-{workspace_id}");

        task_backend
            .create_consumer_group(&ready_stream, &group)
            .await?;
        let mut stream = task_backend.subscribe(&ready_stream, &group).await?;

        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                msg = stream.next() => match msg {
                    Some(Ok((msg_id, bytes))) => {
                        let task: Task = match serde_json::from_slice(&bytes) {
                            Ok(t) => t,
                            Err(e) => {
                                tracing::warn!("Failed to deserialize task from ready stream: {e}");
                                continue;
                            }
                        };

                        // Notify orchestrator that task is now running.
                        if let Some(ref orch) = self.orchestrator {
                            if let Err(e) = orch.start_task(&task.id).await {
                                tracing::warn!(task_id = %task.id, "Failed to start task via orchestrator: {e}");
                            }
                        }

                        let result = self.execute(&task).await;

                        let (msg, payload) = match result {
                            Ok(r) => {
                                let msg = DagMessage::TaskComplete {
                                    message_id: format!("res-{}", task.id),
                                    timestamp: chrono::Utc::now(),
                                    task_id: task.id.clone(),
                                    result: r.output.clone(),
                                    sender: "executor-loop".into(),
                                    recipient: "dag-executor".into(),
                                };
                                let payload = serde_json::to_vec(&msg).map_err(SFError::Serialization)?;
                                (msg, payload)
                            }
                            Err(e) => {
                                let msg = DagMessage::TaskFailed {
                                    message_id: format!("res-{}", task.id),
                                    timestamp: chrono::Utc::now(),
                                    task_id: task.id.clone(),
                                    error: e.to_string(),
                                    sender: "executor-loop".into(),
                                    recipient: "dag-executor".into(),
                                };
                                let payload = serde_json::to_vec(&msg).map_err(SFError::Serialization)?;
                                (msg, payload)
                            }
                        };

                        let result_stream = format!("orchestrator:results:{workspace_id}");
                        if let Err(e) = result_backend.publish(&result_stream, &payload).await {
                            tracing::warn!(task_id = %task.id, "Failed to publish result: {e}");
                        } else {
                            tracing::info!(task_id = %task.id, "Published task result to {result_stream}");
                        }
                        if let Err(e) = task_backend.ack(&ready_stream, &group, std::slice::from_ref(&msg_id)).await {
                            tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack ready message: {e}");
                        }
                        let _ = msg;
                    }
                    Some(Err(e)) => tracing::warn!("task stream error: {e}"),
                    None => break,
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TaskExecutor for TaskExecutorRouter {
    fn supports(&self, task_type: &TaskType) -> bool {
        // supports is a sync trait method; use try_read to avoid blocking.
        match self.executors.try_read() {
            Ok(guard) => guard.iter().any(|e| e.supports(task_type)),
            Err(_) => false,
        }
    }

    async fn execute(&self, task: &Task) -> SFResult<TaskResult> {
        TaskExecutorRouter::execute(self, task).await
    }
}

impl Default for TaskExecutorRouter {
    fn default() -> Self {
        Self::new()
    }
}
