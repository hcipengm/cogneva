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
    /// 结果流上一条消息允许保持未 ack 的时长，超过即被清扫器认领并重走处理。
    /// 必须大于最长处理时长，否则正在处理的消息会被判死而并发重投。
    pub result_claim_idle_secs: u64,
    /// 结果流 pending 清扫的节拍。
    pub result_claim_interval_secs: u64,
    /// 单轮最多认领多少条。
    pub result_claim_batch: usize,
}

impl Default for DagExecutorConfig {
    fn default() -> Self {
        Self {
            redis_url: String::new(),
            workspace_id: String::new(),
            consumer_group: String::new(),
            max_retries: 3,
            result_claim_idle_secs: 600,
            result_claim_interval_secs: 60,
            result_claim_batch: 16,
        }
    }
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

        // 死信恢复：处理途中死掉的 pod 会把消息留在组内 pending，
        // `subscribe`（XREADGROUP ">"）只读新消息，永远不会重投——而本循环的
        // 失败分支正是靠重投才成立（"Keep the message pending: redelivery …"）。
        // 没有这一步，一次瞬时 complete_task/publish_ready_tasks 失败或一次崩溃
        // 就让那条结果永久丢失：任务在该进程的内存 DAG 里停在 running，依赖它的
        // 下游再也不就绪，且没有任何面能看出丢了什么。
        // 阈值必须大于最长处理时长，否则正在处理的消息会被并发重投；重投是
        // at-least-once，已终态任务会被上面的拒绝分支 ack 丢弃，不会重复迁移。
        let claim_idle_ms = self.config.result_claim_idle_secs.saturating_mul(1000);
        {
            let sweeper = self.clone();
            let stream = result_stream.clone();
            let group = group_name.clone();
            let sweep_shutdown = shutdown.clone();
            let interval_secs = self.config.result_claim_interval_secs.max(1);
            let batch = self.config.result_claim_batch;
            tokio::spawn(async move {
                // 观测面：清扫器是唯一同时知道"扫哪条流"和"按什么阈值扫"的地方，
                // 由它把 pending 状态报给指标面。先量一次再进循环，让系列在首个
                // 节拍前就存在——否则一个从没量过的流和一个量过发现干净的流
                // 在抓取面上都是"没有证据"。
                let observer = crate::observable::stream_pending_observable();
                observer
                    .measure(
                        &*sweeper.backend,
                        &stream,
                        &group,
                        claim_idle_ms,
                        interval_secs,
                    )
                    .await;
                let mut ticker =
                    tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                loop {
                    tokio::select! {
                        biased;
                        _ = sweep_shutdown.wait() => break,
                        _ = ticker.tick() => {
                            match sweeper
                                .backend
                                .claim_pending(&stream, &group, claim_idle_ms, batch)
                                .await
                            {
                                Ok(claimed) => {
                                    for (msg_id, bytes) in claimed {
                                        tracing::warn!(
                                            stream = %stream, msg_id = %msg_id,
                                            "reclaimed a result message left pending by a consumer that never acked it"
                                        );
                                        sweeper
                                            .handle_result_message(&stream, &group, &msg_id, &bytes)
                                            .await;
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        stream = %stream,
                                        "result pending claim sweep failed: {e}"
                                    );
                                }
                            }
                            // 每轮必量，包括认领失败那一轮：报出去的是这一轮扫完仍然
                            // 超龄的那部分，也就是清扫器没能收回的工作。认领失败时这
                            // 个数最该被看见，走到 continue 就会把它漏掉。
                            observer
                                .measure(
                                    &*sweeper.backend,
                                    &stream,
                                    &group,
                                    claim_idle_ms,
                                    interval_secs,
                                )
                                .await;
                        }
                    }
                }
            });
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

                self.handle_result_message(&result_stream, &group_name, &msg_id, &bytes)
                    .await;
            }

            tokio::select! {
                _ = shutdown.wait() => break 'subscribe,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
            }
        }

        Ok(())
    }

    /// Apply one result message to the DAG and ack it.
    ///
    /// Shared by the live subscription and the pending sweeper so a reclaimed
    /// message takes exactly the path a freshly delivered one takes. A second
    /// implementation for the reclaimed case would drift, and that is the path
    /// that is by definition the least exercised.
    ///
    /// A message is only acked once its state transition has been applied.
    /// Every early return below leaves it pending on purpose, so the sweeper
    /// retries it — a transient failure must not silently strand a result.
    async fn handle_result_message(
        &self,
        result_stream: &str,
        group_name: &str,
        msg_id: &str,
        bytes: &[u8],
    ) {
        let msg: DagMessage = match serde_json::from_slice(bytes) {
            Ok(m) => m,
            Err(e) => {
                // Poison message: no future bytes can parse better, so ack it
                // out instead of letting the sweeper redeliver it forever.
                tracing::warn!(msg_id = %msg_id, "Failed to deserialize DagMessage: {e}");
                self.ack_result(result_stream, group_name, msg_id).await;
                return;
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
                        self.ack_result(result_stream, group_name, msg_id).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(task_id = %task_id, "complete_task failed: {e}");
                        return;
                    }
                }
                if let Err(e) = self.publish_ready_tasks().await {
                    tracing::warn!("publish_ready_tasks after complete failed: {e}");
                    return;
                }
                self.ack_result(result_stream, group_name, msg_id).await;
            }
            DagMessage::TaskFailed {
                task_id,
                error,
                error_cause,
                ..
            } => {
                match self
                    .orchestrator
                    .fail_task(&task_id, error.clone(), error_cause)
                    .await
                {
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
                        self.ack_result(result_stream, group_name, msg_id).await;
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(task_id = %task_id, "fail_task failed: {e}");
                        return;
                    }
                }
                if let Err(e) = self.publish_ready_tasks().await {
                    tracing::warn!("publish_ready_tasks after fail failed: {e}");
                    return;
                }
                self.ack_result(result_stream, group_name, msg_id).await;
            }
            _ => {
                tracing::debug!(msg_id = %msg_id, "Ignoring non-result DagMessage variant");
                self.ack_result(result_stream, group_name, msg_id).await;
            }
        }
    }

    async fn ack_result(&self, result_stream: &str, group_name: &str, msg_id: &str) {
        let ids = [msg_id.to_string()];
        if let Err(e) = self.backend.ack(result_stream, group_name, &ids).await {
            tracing::warn!(msg_id = %msg_id, "Failed to ack result message: {e}");
        }
    }

    /// Consume goal messages from the `goals:{workspace_id}` stream and inject
    /// them into the DAG.
    /// Each [`cog_core::GoalMessage`] is deserialized and submitted via
    /// [`Self::submit_goal`]. After submission, newly-ready tasks are
    /// automatically published to the ready stream.
    ///
    /// Replay safety here comes from task identity, not from the message:
    /// every injection on this path is a duplicate-tolerant batch add, so a
    /// redelivered message carrying task ids already in the DAG adds nothing.
    /// There is no record of processed `message_id`s, and none is needed
    /// while the injections stay idempotent.
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
                        // Planner failure can be transient (LLM/collaboration),
                        // so the message is left pending rather than acked. A
                        // retry replays it through the same idempotent injection
                        // path; it does not re-submit anything already added.
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
        alert_dwell_secs: u64,
        sink: Option<Arc<dyn cog_core::PersistentAlertSink>>,
        shutdown: ShutdownSignal,
    ) {
        if !enabled {
            tracing::info!("decomposition orphan reconciler disabled by config");
            return;
        }
        let interval_secs = poll_interval_secs.max(30);
        let stall_secs = stall_after_secs.max(60);
        // The alert consumer polls independently; a dwell shorter than one scan
        // interval could elude every poll. Default dwell is two intervals.
        let dwell_secs = alert_dwell_secs.max(interval_secs);
        tracing::info!(
            interval_secs,
            stall_after_secs = stall_secs,
            alert_dwell_secs = dwell_secs,
            alert_sink = sink.is_some(),
            "decomposition orphan reconciler started"
        );

        // Keys this watcher owns, with the earliest time the alert may resolve.
        let mut firing: std::collections::HashMap<String, OrphanWatch> =
            std::collections::HashMap::new();
        let dwell = chrono::Duration::seconds(dwell_secs as i64);
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
                                    firing.insert(
                                        alert.dedup_key.clone(),
                                        OrphanWatch {
                                            parent_id: parent.to_string(),
                                            resolve_after: alert.fired_at + dwell,
                                        },
                                    );
                                }
                            }
                        }
                    }
                    let stall = chrono::Duration::seconds(stall_secs as i64);
                    orphan_reconcile_tick(
                        &self.orchestrator,
                        sink.as_ref(),
                        &stall,
                        &dwell,
                        &mut firing,
                    )
                    .await;
                }
            }
        }
    }
}

/// Firing-key bookkeeping: the parent task the alert is about and the
/// earliest time at which it is allowed to resolve.
struct OrphanWatch {
    parent_id: String,
    resolve_after: chrono::DateTime<chrono::Utc>,
}

/// One reconciler pass. Split out as a free function so the adoption/firing
/// bookkeeping stays testable without a running timer.
async fn orphan_reconcile_tick(
    orchestrator: &Arc<DagExecutor>,
    sink: Option<&Arc<dyn cog_core::PersistentAlertSink>>,
    stall: &chrono::Duration,
    dwell: &chrono::Duration,
    firing: &mut std::collections::HashMap<String, OrphanWatch>,
) {
    let stall_before = chrono::Utc::now() - *stall;
    let orphans = orchestrator.find_decomposition_orphans(stall_before).await;
    let mut fired_this_tick = Vec::new();
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
        firing.insert(
            dedup_key.clone(),
            OrphanWatch {
                parent_id: task_id.clone(),
                resolve_after: chrono::Utc::now() + *dwell,
            },
        );
        fired_this_tick.push(dedup_key);
        if let Err(e) = orchestrator
            .terminate_decomposition_orphan(&task_id, reason.to_string())
            .await
        {
            tracing::warn!(task_id = %task_id, error = %e, "failed to terminate decomposition orphan");
        } else {
            tracing::error!(task_id = %task_id, goal_id = %goal_id, "terminated stalled decomposition orphan");
        }
    }

    // Resolve owned alerts whose parent task is now terminal or gone, but never
    // before the observation dwell elapsed since the alert first fired.
    let now = chrono::Utc::now();
    let mut resolved = Vec::new();
    for (key, watch) in firing.iter() {
        // An alert fired on this pass must survive at least until the next pass:
        // the independent alert consumer may only poll between passes.
        if fired_this_tick.contains(key) {
            continue;
        }
        if now < watch.resolve_after {
            continue;
        }
        let task = orchestrator.get_task(&watch.parent_id).await;
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
    use cog_core::{MessageStream, Observable};
    use std::collections::HashMap;
    use std::sync::Mutex;

    type Queue = HashMap<String, Vec<(String, Vec<u8>)>>;
    type AckLog = Vec<(String, String, Vec<String>)>;
    type PendingLog = Vec<(String, Vec<u8>)>;

    /// Scripted backend: subscribe replays a fixed queue per subject then
    /// blocks forever; ack calls are recorded for assertions.
    #[derive(Clone, Default)]
    struct ScriptedBackend {
        queues: Arc<Mutex<Queue>>,
        acks: Arc<Mutex<AckLog>>,
        /// Entries already delivered to some consumer but never acked — what
        /// `claim_pending` hands back. Draining on read mirrors XAUTOCLAIM
        /// taking ownership of the PEL entry.
        pending: Arc<Mutex<PendingLog>>,
        /// How long the pending entries have been outstanding. A real queue
        /// ages them itself; the script has to be told, because the age is what
        /// separates "just handed over" from "abandoned".
        pending_idle_ms: Arc<Mutex<u64>>,
        /// Refuse to hand entries back, the way a reclaim pass that is broken
        /// or unreachable would, so a test can look at what is left behind.
        hold_claims: Arc<std::sync::atomic::AtomicBool>,
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

        fn abandon(&self, msg_id: &str, bytes: Vec<u8>) {
            self.pending
                .lock()
                .unwrap()
                .push((msg_id.to_string(), bytes));
        }

        fn set_pending_idle_ms(&self, ms: u64) {
            *self.pending_idle_ms.lock().unwrap() = ms;
        }

        fn hold_claims(&self) {
            self.hold_claims
                .store(true, std::sync::atomic::Ordering::Relaxed);
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
        async fn claim_pending(
            &self,
            _stream: &str,
            _group: &str,
            _min_idle_ms: u64,
            _count: usize,
        ) -> SFResult<Vec<(String, Vec<u8>)>> {
            if self.hold_claims.load(std::sync::atomic::Ordering::Relaxed) {
                return Ok(Vec::new());
            }
            Ok(std::mem::take(&mut *self.pending.lock().unwrap()))
        }
        /// The same split a real queue makes: an entry counts as unreclaimed
        /// once its outstanding time passes the caller's threshold.
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            let count = self.pending.lock().unwrap().len() as u64;
            let idle = *self.pending_idle_ms.lock().unwrap();
            let unreclaimed = if count > 0 && idle > idle_threshold_ms {
                cog_core::PendingStats {
                    count,
                    unreclaimed_count: count,
                    unreclaimed_oldest_idle_ms: idle,
                }
            } else {
                cog_core::PendingStats {
                    count,
                    ..Default::default()
                }
            };
            Ok(Some(unreclaimed))
        }
    }

    fn test_runtime(backend: ScriptedBackend) -> DagExecutorRuntime {
        test_runtime_with(
            backend,
            DagExecutorConfig {
                redis_url: "memory".into(),
                workspace_id: "ws-ack-test".into(),
                consumer_group: "grp-ack-test".into(),
                max_retries: 1,
                ..DagExecutorConfig::default()
            },
        )
    }

    fn test_runtime_with(
        backend: ScriptedBackend,
        config: DagExecutorConfig,
    ) -> DagExecutorRuntime {
        DagExecutorRuntime::new_with_backend(config, backend)
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
    async fn abandoned_result_is_reclaimed_and_applied() {
        // Regression: `subscribe` reads with XREADGROUP ">" (new messages only),
        // so a result delivered to a consumer that died before acking was never
        // seen again — the task sat Running in the DAG forever and its
        // dependents never became ready. The live loop must not be the only way
        // a result reaches the DAG.
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-ack-test".into(),
            consumer_group: "grp-ack-test".into(),
            max_retries: 1,
            result_claim_idle_secs: 0,
            result_claim_interval_secs: 1,
            result_claim_batch: 16,
        };
        let runtime = test_runtime_with(backend.clone(), config);

        let task = cog_core::Task {
            id: "task-reclaimed".into(),
            task_type: cog_core::TaskType::DagNode,
            status: cog_core::TaskStatus::Pending,
            input: serde_json::json!({}),
            result: None,
            error: None,
            error_cause: None,
            blocked_by: vec![],
            blocks: vec![],
            priority: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            agent_id: None,
            workspace_id: Some("ws-ack-test".into()),
            retry_count: 0,
            max_retries: 1,
            retry_not_before: None,
            started_at: None,
            timeout_seconds: 30,
            action_planner_meta: None,
            goal_id: Some("goal-reclaimed".into()),
            parent_task_id: None,
            is_executable: true,
        };
        runtime
            .orchestrator()
            .submit_goal("goal-reclaimed", vec![task])
            .await
            .unwrap();
        // complete_task only accepts a Running task.
        runtime
            .orchestrator()
            .schedule_task("task-reclaimed")
            .await
            .unwrap();
        runtime
            .orchestrator()
            .start_task("task-reclaimed")
            .await
            .unwrap();

        // Nothing on the live subscribe path: the result exists only as a
        // pending, unacked entry.
        let msg = DagMessage::TaskComplete {
            message_id: "m-pending".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-reclaimed".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        backend.abandon("rid-pending", serde_json::to_vec(&msg).unwrap());

        let dag = runtime.orchestrator().clone();
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let acks = wait_for_acks(&backend, 1).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        assert_eq!(acks, vec!["rid-pending".to_string()]);
        // The point is the state transition, not merely draining the entry.
        let task = dag.get_task("task-reclaimed").await.expect("task present");
        assert_eq!(task.status, cog_core::TaskStatus::Completed);
    }

    /// Poll the process-wide pending observable until the stream shows up, so
    /// the assertion is about a measurement the sweep actually made rather than
    /// about a sleep having been long enough.
    async fn wait_for_stream_metrics(stream: &str) -> Vec<cog_core::observability::RawMetric> {
        let observer = crate::observable::stream_pending_observable();
        for _ in 0..100 {
            let metrics = observer.collect_metrics("D8").await.unwrap();
            if metrics
                .iter()
                .any(|m| m.labels.get("stream").map(String::as_str) == Some(stream))
            {
                return metrics;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("no pending metrics published for {stream} within 5s");
    }

    /// The reclaim pass has to report what it is holding. A message stranded by
    /// a dead consumer stayed invisible for two and a half days precisely
    /// because the loop that could see it published nothing: the fix that takes
    /// the message back is only half the repair, the other half is a number
    /// something can alert on.
    #[tokio::test]
    async fn the_sweep_publishes_what_it_could_not_reclaim() {
        let backend = ScriptedBackend::default();
        let config = DagExecutorConfig {
            redis_url: "memory".into(),
            workspace_id: "ws-observe-test".into(),
            consumer_group: "grp-observe-test".into(),
            max_retries: 1,
            result_claim_idle_secs: 600,
            result_claim_interval_secs: 1,
            result_claim_batch: 16,
        };
        let runtime = test_runtime_with(backend.clone(), config);
        let stream = "orchestrator:results:ws-observe-test".to_string();

        // A result delivered to a consumer that died, 601s stale, on a reclaim
        // pass that cannot take it back.
        let msg = DagMessage::TaskComplete {
            message_id: "m-observed".into(),
            timestamp: chrono::Utc::now(),
            task_id: "task-observed".into(),
            result: serde_json::json!({"ok": true}),
            sender: "exec".into(),
            recipient: "dag".into(),
        };
        backend.abandon("rid-observed", serde_json::to_vec(&msg).unwrap());
        backend.set_pending_idle_ms(601_000);
        backend.hold_claims();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move { runtime.run_consumer(shutdown_clone).await });
        let metrics = wait_for_stream_metrics(&stream).await;
        shutdown.trigger();
        let _ = handle.await.unwrap();

        let of = |name: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == name
                        && m.labels.get("stream").map(String::as_str) == Some(stream.as_str())
                })
                .unwrap_or_else(|| panic!("{name} not published for {stream}"))
                .value
        };
        assert_eq!(
            of(crate::observable::STREAM_PENDING_COUNT_METRIC),
            1.0,
            "the pending entry itself must be reported"
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_UNRECLAIMED_METRIC),
            1.0,
            "an entry the reclaim pass could not take back is the finding"
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_UNRECLAIMED_AGE_METRIC),
            601.0
        );
        assert_eq!(
            of(crate::observable::STREAM_PENDING_CLAIM_IDLE_METRIC),
            600.0,
            "the rule compares the age against the threshold the pass actually uses"
        );
        assert!(
            of(crate::observable::STREAM_PENDING_MEASURE_LAST_METRIC) > 0.0,
            "a measured stream must carry the timestamp its staleness is judged against"
        );
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
        async fn pending_stats(
            &self,
            _stream: &str,
            _group: &str,
            _idle_threshold_ms: u64,
        ) -> SFResult<Option<cog_core::PendingStats>> {
            Ok(None)
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
                ..DagExecutorConfig::default()
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

        // Zero dwell in this unit test: fire and terminate together on the
        // first pass; resolution eligibility is tested separately.
        let dwell = chrono::Duration::zero();

        // First pass: fire the alert and terminate the placeholder together.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;

        let view = dag.get_task("orphan-1").await.unwrap();
        assert_eq!(view.status, TaskStatus::Failed);
        let key = "decomposition_orphaned:-:orphan-1";
        assert_eq!(firing.len(), 1);
        assert!(firing.contains_key(key));
        assert_eq!(
            *recording.calls.lock().unwrap(),
            vec![(true, "decomposition_orphaned".into(), key.into())]
        );

        // Second pass: parent is terminal and dwell elapsed → resolve.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert_eq!(
            recording.calls.lock().unwrap()[1],
            (false, "decomposition_orphaned".into(), key.into())
        );
    }

    #[tokio::test]
    async fn tick_keeps_alert_firing_throughout_dwell_window() {
        // Regression for the multi-pod race: an adopted/fired alert whose
        // parent is already terminal must stay firing until resolve_after so the
        // independent alert consumer gets at least one poll on a firing row.
        let dag = Arc::new(DagExecutor::new("ws-orphan-dwell".into()));
        dag.add_task(placeholder("orphan-2", 60)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let dwell = chrono::Duration::minutes(10);
        let mut firing = std::collections::HashMap::new();

        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert_eq!(
            dag.get_task("orphan-2").await.unwrap().status,
            TaskStatus::Failed
        );

        // Second pass inside the dwell: still firing, no resolve call.
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        let key = "decomposition_orphaned:-:orphan-2";
        assert_eq!(firing.len(), 1);
        assert!(firing.contains_key(key));
        assert_eq!(recording.calls.lock().unwrap().len(), 1);

        // Simulate the dwell elapsing: watch becomes eligible and resolves.
        firing.get_mut(key).unwrap().resolve_after =
            chrono::Utc::now() - chrono::Duration::seconds(1);
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert_eq!(recording.calls.lock().unwrap().len(), 2);
        assert!(!recording.calls.lock().unwrap()[1].0);
    }

    #[tokio::test]
    async fn tick_leaves_fresh_placeholder_alone() {
        let dag = Arc::new(DagExecutor::new("ws-orphan-fresh".into()));
        dag.add_task(placeholder("fresh-1", 1)).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();
        let stall = chrono::Duration::minutes(30);
        let mut firing = std::collections::HashMap::new();

        let dwell = chrono::Duration::zero();
        orphan_reconcile_tick(&dag, Some(&sink), &stall, &dwell, &mut firing).await;
        assert!(firing.is_empty());
        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("fresh-1").await.unwrap().status,
            TaskStatus::Pending
        );
    }

    #[tokio::test]
    async fn adopted_alert_resolves_when_parent_terminal_and_dwell_elapsed() {
        // Restart adoption: the alert row predates this process by more than
        // one dwell window and the parent has since Failed; the first tick
        // adopts and resolves it.
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
                fired_at: chrono::Utc::now() - chrono::Duration::minutes(10),
            });
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let runtime = test_runtime(dag);
        let handle = tokio::spawn(async move {
            runtime
                .run_orphan_reconciler(true, 30, 60, 30, Some(sink), shutdown_clone)
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
    async fn recently_fired_adopted_alert_keeps_firing_for_one_dwell() {
        // Multi-pod race guard: an alert adopted only seconds after it fired
        // (e.g. a second replica starting) must not resolve on the adopting
        // tick even though the parent is already terminal.
        let dag = Arc::new(DagExecutor::new("ws-orphan-adopt-fresh".into()));
        let mut adopted_parent = placeholder("orphan-new", 120);
        adopted_parent.status = TaskStatus::Failed;
        dag.add_task(adopted_parent).await.unwrap();

        let recording = Arc::new(RecordingAlertSink::default());
        let key = "decomposition_orphaned:goal-y:orphan-new".to_string();
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
                labels: serde_json::json!({"parent_task_id": "orphan-new"}),
                fired_at: chrono::Utc::now(),
            });
        let sink: Arc<dyn cog_core::PersistentAlertSink> = recording.clone();

        // Interval 60s: the second tick lands at 60s, beyond this test's wait.
        let runtime = test_runtime(dag);
        let shutdown = ShutdownSignal::new();
        let shutdown_clone = shutdown.clone();
        let handle = tokio::spawn(async move {
            runtime
                .run_orphan_reconciler(true, 60, 60, 60, Some(sink), shutdown_clone)
                .await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        shutdown.trigger();
        let _ = handle.await;

        assert!(
            !recording
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(c, _, k)| !*c && k == &key),
            "freshly fired adopted alert resolved too early: {:?}",
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
            .run_orphan_reconciler(false, 30, 60, 60, Some(sink), ShutdownSignal::new())
            .await;

        assert!(recording.calls.lock().unwrap().is_empty());
        assert_eq!(
            dag.get_task("orphan-x").await.unwrap().status,
            TaskStatus::Pending
        );
    }
}
