//! Autonomous task executor that processes ready tasks via TaskExecutorRouter.
//! This used to live inline in main.rs but was moved here to keep
//! the entry layer clean (pure wiring only).

use std::sync::Arc;
use tracing::{info, warn};

/// 循环名：本文件起的两个后台任务各一个，报在 `cogneva_loop_*` 的 `loop` 标签上。
pub const TIMEOUT_CHECKER_LOOP: &str = "gateway_timeout_checker";
/// 循环名，见 [`TIMEOUT_CHECKER_LOOP`] 的说明。
pub const COLLABORATION_LISTENER_LOOP: &str = "gateway_collaboration_listener";

/// Spawns the task timeout checker background task.
pub fn spawn_timeout_checker(
    state: Arc<crate::GatewayState>,
    shutdown: cog_core::shutdown::ShutdownSignal,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    let interval = std::time::Duration::from_secs(interval_secs);
    let stop = shutdown.clone();
    cog_core::loop_health::spawn(
        TIMEOUT_CHECKER_LOOP,
        cog_core::loop_health::Cadence::Periodic(interval),
        shutdown,
        move |beat| async move {
            let mut interval = tokio::time::interval(interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                // 每轮盖一次，没超时也盖：一轮里没有任何任务到期是常态，不能读成循环停了。
                beat.beat();
                tokio::select! {
                    biased;
                    _ = stop.wait() => {
                        info!("Task timeout checker shutting down");
                        return;
                    }
                    _ = interval.tick() => {}
                }
                let timed_out = state.orchestrator.check_timeouts().await;
                if !timed_out.is_empty() {
                    let ids: Vec<String> =
                        timed_out.iter().map(|(id, _, _, _)| id.clone()).collect();
                    warn!(
                        "Task timeout checker detected {} expired task(s): {:?}",
                        timed_out.len(),
                        ids
                    );
                }
            }
        },
    )
}

/// Spawns the collaboration graph listener background task.
pub fn spawn_collaboration_listener(
    state: Arc<crate::GatewayState>,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(graph) = state.collaboration_graph.clone() else {
            // 没有协作图就没什么可听：这一支不是循环停了，而是这个部署根本没有
            // 这个循环。登记放它后面，「没起过」才不会读成「死了一次」。
            return;
        };
        let beat = cog_core::loop_health::register(
            COLLABORATION_LISTENER_LOOP,
            // 只在任务事件到达时干活，所以没有周期可判：年龄照报，但不作为「卡住」的
            // 依据——队列空着等和在处理里卡住，从这个面看是一样的。
            cog_core::loop_health::Cadence::EventDriven,
        );
        let _mortality = beat.watch_death(shutdown.clone());
        let mut task_event_rx = state.subscribe_task_events();
        loop {
            beat.beat();
            tokio::select! {
                biased;
                _ = shutdown.wait() => return,
                recv = task_event_rx.recv() => match recv {
                    Ok(event) => match event {
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
                    },
                    // 落后说明这个消费者没跟上，不是它坏了：丢掉的几环补不回来，
                    // 继续消费即可。让它静默卡在一条永不就绪的分支上，才是把
                    // 「循环还在但事件丢了」变成看不出来的那种做法。
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!(skipped, "collaboration listener fell behind on task events");
                    }
                    // 生产者没了：再等也等不到事件，这个循环的活干完了。
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        warn!("task event channel closed; collaboration listener exiting");
                        return;
                    }
                },
            }
        }
    })
}

/// Task runner backed by [`crate::GatewayState`] — published as [`dyn TaskExecutionCallback`](cog_core::TaskExecutionCallback)
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
                let cause = e.upstream_failure();
                let retry_after_secs = e.retry_after_secs();
                warn!("Task {} failed: {}", task_id, error);
                if let Some(ref engine) = self.state.hook_engine {
                    engine.emit_detached(
                        cog_core::HookEvent::new(cog_core::HookTrigger::OnTaskFail)
                            .with_task_id(&task_id)
                            .with_payload(serde_json::json!({"error": &error})),
                    );
                }
                let (_retried, _cancelled, _dlq) = match self
                    .state
                    .orchestrator
                    .fail_task_after(&task_id, error, cause, retry_after_secs)
                    .await
                {
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
