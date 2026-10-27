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
        // The body is rebuilt per attempt, so handles it consumes are cloned
        // in the closure; see spawn_polling_loop in cog-github.
        move |beat| {
            let stop = stop.clone();
            let state = Arc::clone(&state);
            async move {
                let mut interval = tokio::time::interval(interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    // Stamped every cycle, timeout or not: a round with nothing
                    // due is the ordinary case and is not the loop having stopped.
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
            }
        },
    )
}

/// 等一条监管者事件；这个部署没有监管者时永远等下去。
///
/// `select!` 的两支必须是同一类 future，而没有监管者的部署不该因此退出循环——
/// 交接与死信仍然要记。用一个永不就绪的分支表达「这条流不存在」，比在循环里
/// 复制一份没有监管者的版本干净。
async fn recv_supervisor(
    rx: &mut Option<tokio::sync::broadcast::Receiver<cog_core::SupervisorEvent>>,
) -> Result<cog_core::SupervisorEvent, tokio::sync::broadcast::error::RecvError> {
    match rx.as_mut() {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// 一次重启打断了哪些执行：这个执行者手上**仍在跑**的任务。
///
/// 只算 `Running`。待执行、已排期的任务没被这次重启打断，给它们记一条「重启」
/// 会把「这个任务被重来过」说成一件没发生的事；也正因为只算 Running，一个空转
/// 的执行者被重启不会产生边——界面上「重启」为 0 说的是「没有在跑的任务被重启
/// 打断」，不是「没有重启过执行者」。
/// 一条边的「谁干的」：结果是谁交的（`agent_id`），没有就用「谁被指定来做」
/// （`owner_unit`）。
///
/// 两列答的不是同一个问题，所以按这个顺序取：交过结果的人一定经手过，被指定的人
/// 可能还没开工。都空就留空——把「没人指定、也没人交」写成某个默认值，会把一条
/// 无主的边说成有主，而那一列存在的全部意义就是回答谁干的。
pub fn owner_of_task(tasks: &[cog_core::Task], task_id: &str) -> Option<String> {
    tasks
        .iter()
        .find(|task| task.id == task_id)
        .and_then(|task| task.agent_id.clone().or_else(|| task.owner_unit.clone()))
}

/// 同一条规则，从编排器现读一遍任务图。任务行是这条信息的正本：加边的时候去读，
/// 而不是在边上留一个空位等人事后猜。
async fn owner_from_graph(state: &crate::GatewayState, task_id: &str) -> Option<String> {
    let (tasks, _edges) = state.orchestrator.get_graph().await;
    owner_of_task(&tasks, task_id)
}

pub fn tasks_interrupted_by_restart(tasks: &[cog_core::Task], agent_id: &str) -> Vec<String> {
    tasks
        .iter()
        .filter(|task| task.status == cog_core::TaskStatus::Running)
        .filter(|task| task.agent_id.as_deref() == Some(agent_id))
        .map(|task| task.id.clone())
        .collect()
}

/// Spawns the collaboration graph listener background task.
pub fn spawn_collaboration_listener(
    state: Arc<crate::GatewayState>,
    shutdown: cog_core::shutdown::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    // The task below is the deployment gate, not the loop: it ends by starting
    // the loop, so a caller must not read its completion as the loop's fate.
    tokio::spawn(async move {
        let Some(graph) = state.collaboration_graph.clone() else {
            // 没有协作图就没什么可听：这一支不是循环停了，而是这个部署根本没有
            // 这个循环。登记放它后面，「没起过」才不会读成「死了一次」。
            return;
        };
        // The receiver is subscribed once and shared across attempts: a restart
        // that re-subscribed would land at the channel's current tail, and every
        // link whose TaskCompleted/TaskFailed arrived while the loop was dead
        // would never be added. This lock has one holder.
        let task_event_rx = Arc::new(tokio::sync::Mutex::new(state.subscribe_task_events()));
        // 第二条事件流：执行者被重启。协作图的第四类边（重启）此前没有任何写入者，
        // 计数恒为 0 而界面上分不清「没发生过」与「没人写」。与任务事件同样只订一次：
        // 重订会落在通道当前尾巴上，重启期间发生的重启就丢了。
        let supervisor_rx = Arc::new(tokio::sync::Mutex::new(
            state
                .supervisor
                .as_ref()
                .map(|supervisor| supervisor.subscribe()),
        ));
        // Seeded after the graph check, so a deployment without this loop seeds
        // nothing, and before the first event, so the series is present at 0
        // from the first scrape: the rule pairing this counter with the
        // executor's reads both sides through `increase()`, and a series absent
        // for want of traffic is indistinguishable from one never wired. The
        // seed adds zero, so a restart into a store that already holds a count
        // does not erase it.
        if let Some(backend) = state.metrics_backend.as_ref() {
            let labels = std::collections::HashMap::from([(
                crate::LABEL_CONSUMER.to_string(),
                crate::LAG_CONSUMER_COLLABORATION_LISTENER.to_string(),
            )]);
            if let Err(e) = backend
                .record_counter(
                    cog_core::metric_names::EVENT_BUS_CONSUMED_TOTAL,
                    0.0,
                    labels,
                )
                .await
            {
                warn!(
                    error = %e,
                    "cannot seed the collaboration listener's taken-events counter"
                );
            }
        }
        drop(cog_core::loop_health::spawn(
            COLLABORATION_LISTENER_LOOP,
            // 只在任务事件到达时干活，所以没有周期可判：年龄照报，但不作为「卡住」的
            // 依据——队列空着等和在处理里卡住，从这个面看是一样的。
            cog_core::loop_health::Cadence::EventDriven,
            shutdown.clone(),
            // Rebuilt per attempt, so everything the body consumes is cloned here.
            move |beat| {
                let graph = graph.clone();
                let state = Arc::clone(&state);
                let task_event_rx = Arc::clone(&task_event_rx);
                let supervisor_rx = Arc::clone(&supervisor_rx);
                let shutdown = shutdown.clone();
                async move {
                    loop {
                        beat.beat();
                        let mut task_event_rx = task_event_rx.lock().await;
                        let mut supervisor_rx = supervisor_rx.lock().await;
                        tokio::select! {
                            biased;
                            _ = shutdown.wait() => return,
                            recv = task_event_rx.recv() => match recv {
                                Ok(event) => {
                                    // Counted for every event taken, before the
                                    // handler: a completion whose task has no
                                    // dependents is work taken that leaves no link
                                    // behind, so the link count alone would read
                                    // this loop as idle while it is working.
                                    //
                                    // 这句在 `task_event_rx` 的锁里——那把锁从
                                    // `recv()` 一直握到 handler 结束，所以这里的
                                    // 一次存储往返是串在消费路径上的：它给这条环
                                    // 加的那点延迟，正好也是 `Lagged` 的成因。任务
                                    // 事件低频，代价可以接受；但读这个 rate 的人要
                                    // 知道它是「通道载了多少」的下界，且写变慢时它
                                    // 与 lagged 会一起动。
                                    crate::record_consumer_consumed(
                                        &state.metrics_backend,
                                        crate::LAG_CONSUMER_COLLABORATION_LISTENER,
                                        1,
                                    )
                                    .await;
                                    match event {
                                    cog_core::TaskEvent::TaskCompleted { task_id, timestamp, .. } => {
                                        let dependents: Vec<String> = {
                                            state.orchestrator.get_dependents(&task_id).await
                                                .map(|deps| deps.into_iter().map(|t| t.id.clone()).collect())
                                                .unwrap_or_default()
                                        };
                                        // 交接边记的是「谁交出来的」：任务结果是谁交的
                                        // 就写在它自己那一行上，加边时去读，而不是让
                                        // 这一列永远是空的。
                                        let agent_id = owner_from_graph(&state, &task_id).await;
                                        for dependent in dependents {
                                            graph.add_link(crate::collaboration::CollaborationLink {
                                                source_task_id: task_id.clone(),
                                                target_task_id: dependent,
                                                link_type: crate::collaboration::CollaborationLinkType::HandOff,
                                                agent_id: agent_id.clone(),
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
                                        // 失败边记的是「谁把它跑挂的」：同一条任务行上
                                        // 交结果的人和被指定的人，取得到哪个算哪个。
                                        let agent_id = owner_from_graph(&state, &task_id).await;
                                        graph.add_link(crate::collaboration::CollaborationLink {
                                            source_task_id: task_id.clone(),
                                            target_task_id: task_id.clone(),
                                            link_type,
                                            agent_id,
                                            timestamp,
                                        }).await;
                                    }
                                    _ => {}
                                    }
                                }
                                // 落后说明这个消费者没跟上，不是它坏了：丢掉的几环补不回来，
                                // 继续消费即可。让它静默卡在一条永不就绪的分支上，才是把
                                // 「循环还在但事件丢了」变成看不出来的那种做法。
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    warn!(skipped, "collaboration listener fell behind on task events");
                                    // The count the warn carries is gone once the
                                    // log line ages out; the counter is what a
                                    // reader can still ask "did this ever happen"
                                    // against, now that the age gauge alone cannot
                                    // tell idle from a loop that woke and lost.
                                    crate::record_consumer_lag(
                                        &state.metrics_backend,
                                        crate::LAG_CONSUMER_COLLABORATION_LISTENER,
                                        skipped,
                                    )
                                    .await;
                                }
                                // 生产者没了：再等也等不到事件，这个循环的活干完了。
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    warn!("task event channel closed; collaboration listener exiting");
                                    return;
                                }
                            },
                            event = recv_supervisor(&mut supervisor_rx) => match event {
                                Ok(cog_core::SupervisorEvent::AgentRestarted { agent_id, timestamp, .. }) => {
                                    // 执行者被重启时手上还在跑的任务，它的执行被打断了，
                                    // 重启后接着做——这正是「重启」这条边要说的事。
                                    let interrupted: Vec<String> = {
                                        let (tasks, _edges) = state.orchestrator.get_graph().await;
                                        tasks_interrupted_by_restart(&tasks, &agent_id)
                                    };
                                    for task_id in interrupted {
                                        graph.add_link(crate::collaboration::CollaborationLink {
                                            source_task_id: task_id.clone(),
                                            target_task_id: task_id,
                                            link_type: crate::collaboration::CollaborationLinkType::Respawn,
                                            // 四类边里唯一带执行者的一类：重启的是谁，
                                            // 加边的那一刻就知道。
                                            agent_id: Some(agent_id.clone()),
                                            timestamp,
                                        }).await;
                                    }
                                }
                                Ok(_) => {}
                                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                    // 不并进任务事件那个 lag 计数：两条通道的落后不是同
                                    // 一件事，合成一个数会让「任务事件被丢」与「重启事件被
                                    // 丢」看起来一样。这些边由「此刻手上有什么」推出来，
                                    // 丢掉的几环不会留下错边。
                                    warn!(skipped, "collaboration listener fell behind on supervisor events");
                                }
                                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                    // 监管者没了，重启边从此不会再有。任务事件那条通道还
                                    // 活着的话，交接与死信照旧要记——所以这里不退出。
                                    warn!("supervisor event channel closed; respawn links stop here");
                                }
                            },
                        }
                    }
                }
            },
        ));
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

#[cfg(test)]
mod restart_interrupts_tests {
    use super::{owner_of_task, tasks_interrupted_by_restart};
    use cog_core::{Task, TaskStatus, TaskType};

    fn task(id: &str, status: TaskStatus, agent: Option<&str>) -> Task {
        let mut task = Task::new(id, TaskType::Custom("probe".into()), serde_json::json!({}));
        task.status = status;
        task.agent_id = agent.map(str::to_string);
        task
    }

    #[test]
    fn a_restart_interrupts_only_the_running_tasks_of_that_agent() {
        let tasks = vec![
            task("running-held", TaskStatus::Running, Some("a1")),
            task("queued-held", TaskStatus::Pending, Some("a1")),
            task("running-other", TaskStatus::Running, Some("a2")),
            task("running-unclaimed", TaskStatus::Running, None),
            task("done-held", TaskStatus::Completed, Some("a1")),
        ];
        assert_eq!(
            tasks_interrupted_by_restart(&tasks, "a1"),
            vec!["running-held".to_string()]
        );
    }

    /// 交过结果的人优先于被指定的人：两列答的不是同一个问题，而边上那一格问的是
    /// 「谁干的」。
    #[test]
    fn an_edge_names_the_deliverer_before_the_designee() {
        let mut delivered = task("done", TaskStatus::Completed, Some("deliverer"));
        delivered.owner_unit = Some("designee".to_string());
        let mut designated = task("queued", TaskStatus::Pending, None);
        designated.owner_unit = Some("designee".to_string());
        let unowned = task("unowned", TaskStatus::Pending, None);
        let tasks = vec![delivered, designated, unowned];

        assert_eq!(owner_of_task(&tasks, "done").as_deref(), Some("deliverer"));
        assert_eq!(owner_of_task(&tasks, "queued").as_deref(), Some("designee"));
        // 既没人指定也没人交：边上是空的，不是某个默认的「某个人」。
        assert_eq!(owner_of_task(&tasks, "unowned"), None);
        assert_eq!(owner_of_task(&tasks, "not-in-the-graph"), None);
    }

    #[test]
    fn an_idle_agent_restart_interrupts_nothing() {
        // 空转的执行者被重启不产生边：它的任务要么没开始，要么已经收口。
        let tasks = vec![
            task("done", TaskStatus::Completed, Some("idle")),
            task("queued", TaskStatus::Scheduled, Some("idle")),
        ];
        assert!(tasks_interrupted_by_restart(&tasks, "idle").is_empty());
    }
}
