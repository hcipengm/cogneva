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

        // Pending 恢复：pod 在执行途中死掉时，消息在组内永 pending，
        // `subscribe`（XREADGROUP ">"）只读新消息，永远不会重投——旧消息会
        // 堵住依赖它的下游任务。后台清扫器周期性 XAUTOCLAIM 认领 idle 超
        // 阈值的 pending 消息并重走完整执行管线。
        // 阈值必须大于最长任务执行时长，否则正在执行的长任务会被误判死亡
        // 而并发重投（at-least-once：下游 complete_task 需容忍重复）。
        const PENDING_IDLE_MS: u64 = 10 * 60 * 1000;
        const CLAIM_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);
        const CLAIM_BATCH: usize = 16;
        {
            let sweeper = self.clone();
            let sweep_task_backend = task_backend.clone();
            let sweep_result_backend = result_backend.clone();
            let sweep_stream = ready_stream.clone();
            let sweep_group = group.clone();
            let sweep_shutdown = shutdown.clone();
            let sweep_workspace = workspace_id.to_string();
            tokio::spawn(async move {
                let pipe = ReadyPipeline {
                    task_backend: &sweep_task_backend,
                    result_backend: &sweep_result_backend,
                    ready_stream: &sweep_stream,
                    group: &sweep_group,
                    workspace_id: &sweep_workspace,
                };
                let mut ticker = tokio::time::interval(CLAIM_INTERVAL);
                loop {
                    tokio::select! {
                        biased;
                        _ = sweep_shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match sweep_task_backend
                                .claim_pending(&sweep_stream, &sweep_group, PENDING_IDLE_MS, CLAIM_BATCH)
                                .await
                            {
                                Ok(claimed) if !claimed.is_empty() => {
                                    tracing::warn!(
                                        stream = %sweep_stream,
                                        count = claimed.len(),
                                        "Claimed idle pending ready messages for re-execution"
                                    );
                                    for (msg_id, bytes) in claimed {
                                        sweeper.process_ready_message(&pipe, msg_id, &bytes).await;
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    tracing::warn!(stream = %sweep_stream, "Pending claim sweep failed: {e}")
                                }
                            }
                        }
                    }
                }
            });
        }

        let mut stream = task_backend.subscribe(&ready_stream, &group).await?;
        let pipe = ReadyPipeline {
            task_backend: &task_backend,
            result_backend: &result_backend,
            ready_stream: &ready_stream,
            group: &group,
            workspace_id,
        };

        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                msg = stream.next() => match msg {
                    Some(Ok((msg_id, bytes))) => {
                        self.process_ready_message(&pipe, msg_id, &bytes).await;
                    }
                    Some(Err(e)) => tracing::warn!("task stream error: {e}"),
                    None => break,
                }
            }
        }

        Ok(())
    }

    /// 执行一条 ready 消息的全管线：反序列化 → start_task → execute →
    /// 发布结果 → ack。主订阅循环与 pending 恢复清扫器共用。
    async fn process_ready_message(&self, pipe: &ReadyPipeline<'_>, msg_id: String, bytes: &[u8]) {
        let task: Task = match serde_json::from_slice(bytes) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!("Failed to deserialize task from ready stream: {e}");
                return;
            }
        };

        // Notify orchestrator that task is now running.
        if let Some(ref orch) = self.orchestrator {
            if let Err(e) = orch.start_task(&task.id).await {
                // 任务行已不存在（历史残留消息/已删除）：执行无意义，
                // 结果也无法落库，ack 后直接丢弃，避免白跑 LLM 并被
                // 清扫器反复认领。
                if e.to_string().contains("Task not found") {
                    tracing::warn!(task_id = %task.id, msg_id = %msg_id, "ready message references unknown task; dropping");
                    if let Err(e) = pipe
                        .task_backend
                        .ack(pipe.ready_stream, pipe.group, std::slice::from_ref(&msg_id))
                        .await
                    {
                        tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack stale ready message: {e}");
                    }
                    return;
                }
                tracing::warn!(task_id = %task.id, "Failed to start task via orchestrator: {e}");
            }
        }

        let result = self.execute(&task).await;

        let payload = match result {
            Ok(r) => {
                let msg = DagMessage::TaskComplete {
                    message_id: format!("res-{}", task.id),
                    timestamp: chrono::Utc::now(),
                    task_id: task.id.clone(),
                    result: r.output.clone(),
                    sender: "executor-loop".into(),
                    recipient: "dag-executor".into(),
                };
                match serde_json::to_vec(&msg) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(task_id = %task.id, "Failed to serialize task result: {e}");
                        return;
                    }
                }
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
                match serde_json::to_vec(&msg) {
                    Ok(p) => p,
                    Err(e) => {
                        tracing::warn!(task_id = %task.id, "Failed to serialize task failure: {e}");
                        return;
                    }
                }
            }
        };

        let result_stream = format!("orchestrator:results:{}", pipe.workspace_id);
        if let Err(e) = pipe.result_backend.publish(&result_stream, &payload).await {
            tracing::warn!(task_id = %task.id, "Failed to publish result: {e}");
        } else {
            tracing::info!(task_id = %task.id, "Published task result to {result_stream}");
        }
        if let Err(e) = pipe
            .task_backend
            .ack(pipe.ready_stream, pipe.group, std::slice::from_ref(&msg_id))
            .await
        {
            tracing::warn!(task_id = %task.id, msg_id = %msg_id, "Failed to ack ready message: {e}");
        }
    }
}

/// process_ready_message 的共享上下文，避免参数列表过长。
struct ReadyPipeline<'a> {
    task_backend: &'a Arc<dyn MessageBackend>,
    result_backend: &'a Arc<dyn MessageBackend>,
    ready_stream: &'a str,
    group: &'a str,
    workspace_id: &'a str,
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
