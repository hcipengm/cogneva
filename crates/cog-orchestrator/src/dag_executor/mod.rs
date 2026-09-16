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

        // Resubscribe on stream failure/end: exiting the task would freeze the
        // consumer group until the next pod restart (a single transient error
        // historically stalled groups for days).
        'subscribe: loop {
            let mut stream = match self.backend.subscribe(&result_stream, &group_name).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("result stream subscribe failed, retrying: {e}");
                    tokio::select! {
                        _ = shutdown.wait() => break 'subscribe,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => continue 'subscribe,
                    }
                }
            };

            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown.wait() => break 'subscribe,
                    msg = stream.next() => msg,
                };

                let (msg_id, bytes) = match next {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => {
                        tracing::warn!("result stream error, resubscribing: {e}");
                        break;
                    }
                    None => {
                        tracing::warn!("result stream ended, resubscribing");
                        break;
                    }
                };

                let msg: DagMessage = match serde_json::from_slice(&bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        // Poison message: no future bytes can parse better, so ack
                        // it out instead of letting the sweeper redeliver forever.
                        tracing::warn!(msg_id = %msg_id, "Failed to deserialize DagMessage: {e}");
                        if let Err(e) = self
                            .backend
                            .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(msg_id = %msg_id, "Failed to ack poison result message: {e}");
                        }
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
                            Err(e @ cog_core::SFError::TaskFailed { .. }) => {
                                // Duplicate/stale result for an already-terminal or
                                // unknown task: reprocessing can never succeed, so
                                // ack and drop instead of spinning on redelivery.
                                tracing::warn!(
                                    task_id = %task_id, msg_id = %msg_id,
                                    "result message rejected by DAG ({e}); dropping"
                                );
                                if let Err(e) = self
                                    .backend
                                    .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                                    .await
                                {
                                    tracing::warn!(
                                        task_id = %task_id, msg_id = %msg_id,
                                        "Failed to ack stale result message: {e}"
                                    );
                                }
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(task_id = %task_id, "complete_task failed: {e}");
                                continue;
                            }
                        }
                        if let Err(e) = self.publish_ready_tasks().await {
                            // Keep the message pending: redelivery hits the
                            // terminal-task reject above but retries scheduling.
                            tracing::warn!("publish_ready_tasks after complete failed: {e}");
                            continue;
                        }
                        if let Err(e) = self
                            .backend
                            .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(task_id = %task_id, msg_id = %msg_id, "Failed to ack result message: {e}");
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
                            Err(e @ cog_core::SFError::TaskFailed { .. }) => {
                                tracing::warn!(
                                    task_id = %task_id, msg_id = %msg_id,
                                    "failure result rejected by DAG ({e}); dropping"
                                );
                                if let Err(e) = self
                                    .backend
                                    .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                                    .await
                                {
                                    tracing::warn!(
                                        task_id = %task_id, msg_id = %msg_id,
                                        "Failed to ack stale failure message: {e}"
                                    );
                                }
                                continue;
                            }
                            Err(e) => {
                                tracing::warn!(task_id = %task_id, "fail_task failed: {e}");
                                continue;
                            }
                        }
                        if let Err(e) = self.publish_ready_tasks().await {
                            tracing::warn!("publish_ready_tasks after fail failed: {e}");
                            continue;
                        }
                        if let Err(e) = self
                            .backend
                            .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(task_id = %task_id, msg_id = %msg_id, "Failed to ack failure message: {e}");
                        }
                    }
                    _ => {
                        tracing::debug!(msg_id = %msg_id, "Ignoring non-result DagMessage variant");
                        if let Err(e) = self
                            .backend
                            .ack(&result_stream, &group_name, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(msg_id = %msg_id, "Failed to ack unhandled result message: {e}");
                        }
                    }
                }
            }

            tokio::select! {
                _ = shutdown.wait() => break 'subscribe,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
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
        'subscribe: loop {
            let mut stream = match self.backend.subscribe(&goal_stream, &group).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("goal stream subscribe failed, retrying: {e}");
                    tokio::select! {
                        _ = shutdown.wait() => break 'subscribe,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => continue 'subscribe,
                    }
                }
            };

            loop {
                let next = tokio::select! {
                    biased;
                    _ = shutdown.wait() => break 'subscribe,
                    msg = stream.next() => msg,
                };

                let (msg_id, bytes) = match next {
                    Some(Ok(v)) => v,
                    Some(Err(e)) => {
                        tracing::warn!("goal stream error, resubscribing: {e}");
                        break;
                    }
                    None => {
                        tracing::warn!("goal stream ended, resubscribing");
                        break;
                    }
                };

                let goal: cog_core::GoalMessage = match serde_json::from_slice(&bytes) {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(msg_id = %msg_id, "Failed to deserialize GoalMessage: {e}");
                        if let Err(e) = self
                            .backend
                            .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                            .await
                        {
                            tracing::warn!(msg_id = %msg_id, "Failed to ack poison goal message: {e}");
                        }
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
                        // Planner failure can be transient (LLM/collaboration):
                        // leave pending so redelivery retries; dedup by message_id
                        // prevents double submission.
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
                    continue;
                }
                if let Err(e) = self
                    .backend
                    .ack(&goal_stream, &group, std::slice::from_ref(&msg_id))
                    .await
                {
                    tracing::warn!(goal_id = %goal.message_id, msg_id = %msg_id, "Failed to ack goal message: {e}");
                }
            }

            tokio::select! {
                _ = shutdown.wait() => break 'subscribe,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }

        Ok(())
    }

    /// Background reconciliation for decomposition orphans: non-executable
    /// parent placeholders with zero children stuck Pending (empty
    /// decomposition, partial write, crash mid-injection). Each stalled
    /// orphan is terminated (Pending → Failed) and raises a persistent
    /// alert so signal watcher re-drives the work as a fresh intent; alerts
    /// resolve once the parent task reaches a terminal state. Rows raised
    /// before a restart are adopted from the alert store on the first tick.
    pub async fn run_orphan_reconciler(
        &self,
        enabled: bool,
        poll_interval_secs: u64,
        stall_after_secs: u64,
        sink: Option<Arc<dyn cog_core::PersistentAlertSink>>,
        shutdown: ShutdownSignal,
    ) {
        if !enabled {
            tracing::info!("decomposition orphan reconciler disabled by config");
            return;
        }
        let interval_secs = poll_interval_secs.max(30);
        let stall_secs = stall_after_secs.max(60);
        tracing::info!(
            interval_secs,
            stall_after_secs = stall_secs,
            alert_sink = sink.is_some(),
            "decomposition orphan reconciler started"
        );

        // dedup_key → parent task id, for keys this watcher owns.
        let mut firing: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut adopted = false;
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        loop {
            tokio::select! {
                biased;
                _ = shutdown.wait() => break,
                _ = ticker.tick() => {
                    if !adopted {
                        adopted = true;
                        if let Some(sink) = &sink {
                            for alert in sink
                                .list_active_persistent_alerts("decomposition_", 1000)
                                .await
                            {
                                if let Some(parent) = alert
                                    .labels
                                    .get("parent_task_id")
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty() && *s != "-")
                                {
                                    firing.insert(alert.dedup_key.clone(), parent.to_string());
                                }
                            }
                        }
                    }
                    let stall = chrono::Duration::seconds(stall_secs as i64);
                    orphan_reconcile_tick(&self.orchestrator, sink.as_ref(), &stall, &mut firing)
                        .await;
                }
            }
        }
    }
}

/// One reconciler pass. Split out as a free function so the adoption/firing
/// bookkeeping stays testable without a running timer.
async fn orphan_reconcile_tick(
    orchestrator: &Arc<DagExecutor>,
    sink: Option<&Arc<dyn cog_core::PersistentAlertSink>>,
    stall: &chrono::Duration,
    firing: &mut std::collections::HashMap<String, String>,
) {
    let stall_before = chrono::Utc::now() - *stall;
    let orphans = orchestrator.find_decomposition_orphans(stall_before).await;
    // Keys fired in this pass must survive until the next pass: signal_watcher
    // polls firing rows on its own cadence, so resolving within the same pass
    // would let it miss the alert and defeat the re-drive.
    let mut newly_fired: std::collections::HashSet<String> = std::collections::HashSet::new();
    for orphan in orphans {
        let goal_id = orphan.goal_id.as_deref().unwrap_or("-");
        let task_id = orphan.id.clone();
        let dedup_key = format!(
            "{}:{goal_id}:{task_id}",
            cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED
        );
        let reason = "decomposition orphan: non-executable parent task has no children after the stall threshold";
        let message = format!(
            "goal {goal_id} task {task_id}: {reason}; the placeholder is terminated and the work is re-driven via a new intent"
        );
        let labels = serde_json::json!({
            "goal_id": goal_id,
            "parent_task_id": task_id,
            "source": "orphan_reconciler",
            "original_input": orphan.input,
        });
        if let Some(sink) = sink {
            let draft = cog_core::PersistentAlertDraft {
                rule: cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED.to_string(),
                dedup_key: dedup_key.clone(),
                severity: cog_core::AlertSeverity::Warning.as_str().to_string(),
                message: message.clone(),
                labels,
            };
            if let Err(e) = sink.set_persistent_alert(true, &draft).await {
                tracing::warn!(task_id = %task_id, error = %e, "failed to fire decomposition orphan alert");
            }
        } else {
            tracing::error!(task_id = %task_id, "{message} (no persistent alert sink attached)");
        }
        firing.insert(dedup_key.clone(), task_id.clone());
        newly_fired.insert(dedup_key);
        if let Err(e) = orchestrator
            .terminate_decomposition_orphan(&task_id, reason.to_string())
            .await
        {
            tracing::warn!(task_id = %task_id, error = %e, "failed to terminate decomposition orphan");
        } else {
            tracing::error!(task_id = %task_id, goal_id = %goal_id, "terminated stalled decomposition orphan");
        }
    }

    // Resolve owned alerts whose parent task is now terminal or gone.
    let mut resolved = Vec::new();
    for (key, parent_id) in firing.iter() {
        if newly_fired.contains(key) {
            continue;
        }
        let task = orchestrator.get_task(parent_id).await;
        let done = match task {
            Some(t) => matches!(
                t.status,
                cog_core::TaskStatus::Completed
                    | cog_core::TaskStatus::Failed
                    | cog_core::TaskStatus::Cancelled
            ),
            None => true,
        };
        if done {
            resolved.push(key.clone());
        }
    }
    for key in resolved {
        let rule = key.split(':').next().unwrap_or(&key).to_string();
        if let Some(sink) = sink {
            let draft = cog_core::PersistentAlertDraft {
                rule,
                dedup_key: key.clone(),
                severity: cog_core::AlertSeverity::Info.as_str().to_string(),
                message: String::new(),
                labels: serde_json::json!({}),
            };
            if let Err(e) = sink.set_persistent_alert(false, &draft).await {
                tracing::warn!(key = %key, error = %e, "failed to resolve decomposition alert");
                continue;
            }
        }
        firing.remove(&key);
        tracing::info!(key = %key, "decomposition alert resolved");
    }
}

#[cfg(test)]
mod consumer_ack_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::MessageStream;
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Queue = HashMap<String, Vec<(String, Vec<u8>)>>;
    type AckLog = Vec<(String, String, Vec<String>)>;

    /// Scripted backend: subscribe replays a fixed queue per subject then
    /// blocks forever; ack calls are recorded for assertions.
    #[derive(Clone, Default)]
    struct ScriptedBackend {
        queues: Arc<Mutex<Queue>>,
        acks: Arc<Mutex<AckLog>>,
    }

    impl ScriptedBackend {
        fn enqueue(&self, subject: &str, msg_id: &str, bytes: Vec<u8>) {
            self.queues
                .lock()
                .unwrap()
                .entry(subject.to_string())
                .or_default()
                .push((msg_id.to_string(), bytes));
        }

        fn acked_ids(&self) -> Vec<String> {
            self.acks
                .lock()
                .unwrap()
                .iter()
                .flat_map(|(_, _, ids)| ids.clone())
                .collect()
        }
    }

    #[async_trait]
    impl MessageBackend for ScriptedBackend {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn subscribe(&self, subject: &str, _group: &str) -> SFResult<MessageStream> {
            let queue = self
                .queues
                .lock()
                .unwrap()
                .remove(subject)
                .unwrap_or_default();
            let stream =
                futures::stream::iter(queue.into_iter().map(Ok)).chain(futures::stream::pending());
            Ok(Box::pin(stream))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, stream: &str, group: &str, ids: &[String]) -> SFResult<()> {
            self.acks
                .lock()
                .unwrap()
                .push((stream.to_string(), group.to_string(), ids.to_vec()));
            Ok(())
        }
    }

    fn test_runtime(backend: ScriptedBackend) -> DagExecutorRuntime {
        DagExecutorRuntime::new_with_backend(
            DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-ack-test".into(),
                consumer_group: "grp-ack-test".into(),
                max_retries: 1,
            },
            backend,
        )
    }

    async fn wait_for_acks(backend: &ScriptedBackend, n: usize) -> Vec<String> {
        for _ in 0..50 {
            if backend.acks.lock().unwrap().len() >= n {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        backend.acked_ids()
    }

    #[tokio::test]
    async fn stale_result_for_unknown_task_is_acked() {
        // Regression: run_consumer discarded msg_id and never acked, so stale
        // results sat in the PEL and were redelivered on every restart/sweep.
        let backend = ScriptedBackend::default();
        let msg = DagMessage::TaskComplete {
            message_id: "m1".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-does-not-exist".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        let bytes = serde_json::to_vec(&msg).unwrap();
        backend.enqueue("orchestrator:results:ws-ack-test", "rid-1", bytes);

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-1".to_string()]);
    }

    #[tokio::test]
    async fn poison_result_message_is_acked() {
        let backend = ScriptedBackend::default();
        backend.enqueue(
            "orchestrator:results:ws-ack-test",
            "rid-poison",
            b"not-json".to_vec(),
        );

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-poison".to_string()]);
    }

    #[tokio::test]
    async fn processed_goal_is_acked() {
        let backend = ScriptedBackend::default();
        let goal = cog_core::GoalMessage {
            message_id: "g1".into(),
            timestamp: chrono::Utc::now(),
            workspace_id: "ws-ack-test".into(),
            goal_id: "goal-1".into(),
            goal: "noop goal".into(),
            tasks: vec![],
            priority: 0,
            source: cog_core::GoalSource::Internal,
        };
        let bytes = serde_json::to_vec(&goal).unwrap();
        backend.enqueue("goals:ws-ack-test", "gid-1", bytes);

        let runtime = test_runtime(backend.clone());
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_goal_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["gid-1".to_string()]);
    }
}

#[cfg(test)]
mod orphan_reconciler_tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::{MessageStream, TaskStatus, TaskType};
    use std::sync::Mutex;

    type Calls = Vec<(bool, String, String)>;

    #[derive(Default)]
    struct RecordingAlertSink {
        calls: Mutex<Calls>,
        active: Mutex<Vec<cog_core::PersistedAlert>>,
    }

    #[async_trait]
    impl cog_core::PersistentAlertSink for RecordingAlertSink {
        async fn set_persistent_alert(
            &self,
            condition: bool,
            draft: &cog_core::PersistentAlertDraft,
        ) -> Result<(), String> {
            self.calls.lock().unwrap().push((
                condition,
                draft.rule.clone(),
                draft.dedup_key.clone(),
            ));
            Ok(())
        }

        async fn list_active_persistent_alerts(
            &self,
            rule_prefix: &str,
            _limit: i64,
        ) -> Vec<cog_core::PersistedAlert> {
            self.active
                .lock()
                .unwrap()
                .iter()
                .filter(|a| a.rule.starts_with(rule_prefix))
                .cloned()
                .collect()
        }
    }

    /// MessageBackend never touched by the reconciler; satisfies the runtime
    /// constructor only.
    struct NullBackend;

    #[async_trait]
    impl MessageBackend for NullBackend {
        async fn publish(&self, _subject: &str, _payload: &[u8]) -> SFResult<()> {
            Ok(())
        }
        async fn subscribe(&self, _subject: &str, _group: &str) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn subscribe_from(
            &self,
            _subject: &str,
            _group: &str,
            _start_id: &str,
        ) -> SFResult<MessageStream> {
            Ok(Box::pin(futures::stream::pending()))
        }
        async fn create_consumer_group(&self, _stream: &str, _group: &str) -> SFResult<()> {
            Ok(())
        }
        async fn ack(&self, _stream: &str, _group: &str, _ids: &[String]) -> SFResult<()> {
            Ok(())
        }
    }

    fn placeholder(id: &str, age_minutes: i64) -> Task {
        let mut t = Task::new(id, TaskType::Generator, serde_json::json!({}));
        t.is_executable = false;
        t.updated_at = chrono::Utc::now() - chrono::Duration::minutes(age_minutes);
        t
    }

    fn test_runtime(dag: Arc<DagExecutor>) -> DagExecutorRuntime {
        DagExecutorRuntime::new_with_backend(
            DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-orphan".into(),
                consumer_group: "grp-orphan".into(),
                max_retries: 1,
            },
            NullBackend,
        )
        .with_orchestrator(dag)
    }

    #[tokio::test]
    async fn tick_fires_terminates_then_resolves_orphan() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-tick".into()));
        dag.add_task(placeholder("orphan-1", 60)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let mut firing = std::collections::HashMap::new();

        // First pass: fire the alert and terminate the placeholder together.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &mut firing).await;

        let view = dag.get_task("orphan-1").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        let key = "decomposition_orphaned:-:orphan-1";
        assert_eq!(firing.len(), 1);
        assert!(firing.contains_key(key));
        assert_eq!(
            *recording.calls.lock().unwrap(),
            vec![(true, "decomposition_orphaned".into(), key.into())]
        );

        // Second pass: parent is terminal → resolve, firing map emptied.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &mut firing).await;
        assert!(firing.is_empty());
        assert_eq!(
            recording.calls.lock().unwrap()[1],
            (false, "decomposition_orphaned".into(), key.into())
        );
    }

    #[tokio::test]
    async fn tick_leaves_fresh_placeholder_alone() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-fresh".into()));
        dag.add_task(placeholder("fresh-1", 1)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let mut firing = std::collections::HashMap::new();

        orphan_reconcile_tick(&dag, Some(&sink), &stall, &mut firing).await;
        assert!(firing.is_empty());
        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("fresh-1").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn adopted_alert_resolves_when_parent_already_terminal() {
        // Restart adoption: the alert row predates this process and the parent
        // task has since Failed; the first tick must resolve the adopted key.
        let dag = Arc::new(DagExecutor::new("ws-orphan-adopt".into()));
        let mut adopted_parent = placeholder("orphan-old", 120);
        adopted_parent.status = TaskStatus::Failed;
        dag.add_task(adopted_parent).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let key = "decomposition_orphaned:goal-x:orphan-old".to_string();
        recording
            .active
            .lock()
            .unwrap()
            .push(cog_core::PersistedAlert {
                rule: cog_core::ALERT_RULE_DECOMPOSITION_ORPHANED.into(),
                dedup_key: key.clone(),
                severity: "warning".into(),
                state: "firing".into(),
                message: String::new(),
                labels: serde_json::json!({"parent_task_id": "orphan-old"}),
                fired_at: chrono::Utc::now(),
            });
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let runtime = test_runtime(dag);
        let handle = tokio::spawn(async move {
            runtime
                .run_orphan_reconciler(true, 30, 60, Some(sink), shutdown_clone)
                .await;
        });

        for _ in 0..50 {
            if recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        shutdown.trigger();
        let _ = handle.await;

        assert!(
            recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key),
            "adopted alert not resolved: {:?}",
            recording.calls.lock().unwrap()
        );
    }

    #[tokio::test]
    async fn disabled_reconciler_returns_immediately() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-off".into()));
        dag.add_task(placeholder("orphan-x", 60)).await.unwrap();
        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        let runtime = test_runtime(dag.clone());
        runtime
            .run_orphan_reconciler(false, 30, 60, Some(sink), ShutdownSignal::new())
            .await;

        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("orphan-x").await.unwrap().status,
            TaskStatus::Pending
        );
    }
}
