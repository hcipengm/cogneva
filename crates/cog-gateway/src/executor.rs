//! Autonomous task executor that processes ready tasks via TaskExecutorRouter.
//! This used to live inline in main.rs but was moved here to keep
//! the entry layer clean (pure wiring only).

use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

/// Spawns the task timeout checker background task.
pub fn spawn_timeout_checker(
    state: Arc<crate::GatewayState>,
    mut shutdown: broadcast::Receiver<()>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let timed_out = state.orchestrator.check_timeouts().await;
                    if !timed_out.is_empty() {
                        let ids: Vec<String> = timed_out.iter().map(|(id, _, _, _)| id.clone()).collect();
                        warn!("Task timeout checker detected {} expired task(s): {:?}", timed_out.len(), ids);
                    }
                }
                _ = shutdown.recv() => {
                    info!("Task timeout checker shutting down");
                    break;
                }
            }
        }
    })
}

/// Bridges orchestrator TaskEvents onto the AgentEvent bus as
/// `TaskStatusChange`, so WebSocket clients subscribed to `task:*` see goal
/// progress in real time instead of waiting for the next /processes poll.
pub fn spawn_task_event_bridge(
    state: Arc<crate::GatewayState>,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut task_event_rx = state.subscribe_task_events();
        loop {
            tokio::select! {
                Ok(event) = task_event_rx.recv() => {
                    use cog_core::TaskEvent as TE;
                    let (task_id, status, timestamp) = match event {
                        TE::TaskCreated { task_id, timestamp } => (task_id, "pending", timestamp),
                        TE::TaskScheduled { task_id, timestamp } => (task_id, "scheduled", timestamp),
                        TE::TaskStarted { task_id, timestamp } => (task_id, "running", timestamp),
                        TE::TaskCompleted { task_id, timestamp, .. } => (task_id, "completed", timestamp),
                        TE::TaskFailed { task_id, timestamp, .. } => (task_id, "failed", timestamp),
                        TE::TaskCancelled { task_id, timestamp, .. } => (task_id, "cancelled", timestamp),
                        TE::TaskRetried { task_id, timestamp, .. } => (task_id, "retried", timestamp),
                        TE::TaskTimeout { task_id, timestamp, .. } => (task_id, "timeout", timestamp),
                    };
                    let _ = state.event_tx.send(cog_core::AgentEvent::TaskStatusChange {
                        task_id,
                        status: status.to_string(),
                        agent_id: None,
                        crew_id: None,
                        squad_id: None,
                        timestamp,
                    });
                }
                _ = shutdown.recv() => {
                    break;
                }
            }
        }
    })
}

/// Spawns the collaboration graph listener background task.
pub fn spawn_collaboration_listener(
    state: Arc<crate::GatewayState>,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Some(ref graph) = state.collaboration_graph {
            let mut task_event_rx = state.subscribe_task_events();
            let graph = graph.clone();
            loop {
                tokio::select! {
                    Ok(event) = task_event_rx.recv() => {
                        match event {
                            cog_core::TaskEvent::TaskCompleted { task_id, timestamp, .. } => {
                                let dependents: Vec<String> = {
                                    state.orchestrator.get_dependents(&task_id).await
                                        .map(|deps| deps.into_iter().map(|t| t.id.clone()).collect())
                                        .unwrap_or_default()
                                };
                                for dependent in dependents {
                                    graph.add_link(crate::collaboration::CollaborationLink {
                                        source_task_id: task_id.clone(),
                                        target_task_id: dependent,
                                        link_type: crate::collaboration::CollaborationLinkType::HandOff,
                                        agent_id: None,
                                        timestamp,
                                    }).await;
                                }
                            }
                            cog_core::TaskEvent::TaskFailed { task_id, retried, timestamp, .. } => {
                                let link_type = if retried {
                                    crate::collaboration::CollaborationLinkType::Retry
                                } else {
                                    crate::collaboration::CollaborationLinkType::DeadLetter
                                };
                                graph.add_link(crate::collaboration::CollaborationLink {
                                    source_task_id: task_id.clone(),
                                    target_task_id: task_id.clone(),
                                    link_type,
                                    agent_id: None,
                                    timestamp,
                                }).await;
                            }
                            _ => {}
                        }
                    }
                    _ = shutdown.recv() => {
                        break;
                    }
                }
            }
        }
    })
}

/// Task runner backed by [`GatewayState`] — published as [`dyn TaskExecutionCallback`](cog_core::TaskExecutionCallback)
/// so that `cog-agent` can spawn pool workers without depending on `cog-gateway`.
pub struct GatewayTaskRunner {
    state: Arc<crate::GatewayState>,
}

impl GatewayTaskRunner {
    pub fn new(state: Arc<crate::GatewayState>) -> Self {
        Self { state }
    }
}

#[async_trait::async_trait]
impl cog_core::TaskExecutionCallback for GatewayTaskRunner {
    async fn execute_task(&self, task: cog_core::Task) {
        let task_id = task.id.clone();

        if let Some(ref engine) = self.state.hook_engine {
            engine.emit_detached(
                cog_core::HookEvent::new(cog_core::HookTrigger::OnAgentStart)
                    .with_task_id(&task_id)
                    .with_payload(serde_json::json!({"task_type": task.task_type})),
            );
        }

        // Notify orchestrator that task is now running (mirrors TaskExecutorRouter behaviour).
        if let Err(e) = self.state.orchestrator.start_task(&task_id).await {
            warn!("TaskRunner failed to start task {}: {}", task_id, e);
            return;
        }

        let result = self.state.task_executors.execute(&task).await;

        match result {
            Ok(value) => {
                info!("Task {} completed successfully", task_id);
                if let Some(ref engine) = self.state.hook_engine {
                    engine.emit_detached(
                        cog_core::HookEvent::new(cog_core::HookTrigger::OnTaskComplete)
                            .with_task_id(&task_id)
                            .with_payload(serde_json::json!({"success": true, "result": value.output, "metadata": value.metadata})),
                    );
                }
                if let Err(e) = self
                    .state
                    .orchestrator
                    .complete_task(&task_id, value.output.clone())
                    .await
                {
                    warn!("TaskRunner failed to complete task {}: {}", task_id, e);
                }
            }
            Err(e) => {
                let error = e.to_string();
                warn!("Task {} failed: {}", task_id, error);
                if let Some(ref engine) = self.state.hook_engine {
                    engine.emit_detached(
                        cog_core::HookEvent::new(cog_core::HookTrigger::OnTaskFail)
                            .with_task_id(&task_id)
                            .with_payload(serde_json::json!({"error": &error})),
                    );
                }
                let (_retried, _cancelled, _dlq) =
                    match self.state.orchestrator.fail_task(&task_id, error).await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("TaskRunner failed to fail task {}: {}", task_id, e);
                            (false, Vec::new(), false)
                        }
                    };
            }
        }

        if let Some(ref engine) = self.state.hook_engine {
            engine.emit_detached(
                cog_core::HookEvent::new(cog_core::HookTrigger::OnAgentEnd).with_task_id(&task_id),
            );
        }
    }
}
