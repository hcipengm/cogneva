//! Orchestrator plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Loop name reported through the background-loop liveness family.
pub const DAG_CHECKPOINT_LOOP: &str = "orchestrator_dag_checkpoint";
/// Loop name reported through the background-loop liveness family.
pub const READY_TASK_PUBLISHER_LOOP: &str = "orchestrator_ready_task_publisher";
/// Loop name reported through the background-loop liveness family.
pub const TASK_LEASE_RENEWER_LOOP: &str = "orchestrator_task_lease_renewer";
/// Loop name reported through the background-loop liveness family.
pub const TASK_CHECKPOINT_LOOP: &str = "orchestrator_task_checkpoint";

/// Orchestrator plugin that self-assembles the DAG executor and related services.
pub struct OrchestratorPlugin {
    initialized: bool,
    shared_orchestrator: Option<Arc<crate::DagExecutor>>,
    exec_loop: Option<Arc<crate::TaskExecutorRouter>>,
    action_planner: Option<Arc<crate::ActionPlanOrchestrator>>,
}

impl OrchestratorPlugin {
    /// Create a plugin that will build orchestrator services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            shared_orchestrator: None,
            exec_loop: None,
            action_planner: None,
        }
    }
}

impl Default for OrchestratorPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for OrchestratorPlugin {
    fn name(&self) -> &'static str {
        "orchestrator"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // ── Consume dependencies ──
        let task_event_tx =
            (*ctx.require::<tokio::sync::broadcast::Sender<cog_core::TaskEvent>>()?).clone();

        let state_backend = ctx.require_service::<dyn cog_core::StateBackend>()?;

        let raw_logger = ctx.require_service::<dyn cog_core::RawLogger>()?;

        let message_backend = ctx.consume_service::<dyn cog_core::MessageBackend>();

        // Consume all TaskExecutors from plugins (pin-style)
        let task_executors: Vec<Arc<dyn cog_core::TaskExecutor>> =
            ctx.consume_all_services::<dyn cog_core::TaskExecutor>();
        info!(
            "OrchestratorPlugin found {} TaskExecutor(s)",
            task_executors.len()
        );

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            workspace_id,
            batch_persistence_enabled,
            batch_persistence_max_changes,
            batch_persistence_interval_secs,
            archive_enabled,
            archive_after_secs,
            archive_poll_interval_secs,
            _data_dir,
            pattern_db_max_size,
            pattern_max_age_days,
            redis_url,
            consumer_group,
            max_retries,
            strict_persistence,
            task_lease_secs,
        ) = {
            let config = ctx.config();
            (
                config.dag_executor.workspace_id.clone(),
                config.dag_executor.batch_persistence_enabled,
                config.dag_executor.batch_persistence_max_changes,
                config.dag_executor.batch_persistence_interval_secs,
                config.dag_executor.archive_enabled,
                config.dag_executor.archive_after_secs,
                config.dag_executor.archive_poll_interval_secs,
                config.app.data_dir.clone(),
                config.system.pattern_db_max_size,
                config.system.pattern_max_age_days,
                config.dag_executor.redis_url.clone(),
                config.dag_executor.consumer_group.clone(),
                config.dag_executor.max_retries,
                config.system.strict_persistence,
                config.dag_executor.task_lease_secs,
            )
        };

        // ── Build DagExecutor ──
        let dag_executor = crate::DagExecutor::new(workspace_id.clone())
            .with_event_tx(task_event_tx.clone())
            .with_raw_logger(raw_logger.clone())
            .with_state_backend(state_backend.clone())
            .with_batch_persistence(
                batch_persistence_enabled,
                batch_persistence_max_changes,
                batch_persistence_interval_secs,
            )
            .with_archive_config(
                archive_enabled,
                archive_after_secs,
                archive_poll_interval_secs,
            )
            .with_task_lease_secs(task_lease_secs);
        if let Err(e) = dag_executor.load_from_backend().await {
            warn!("DagExecutor failed to load state from backend: {}", e);
        }
        // Immediately archive old terminal tasks after loading snapshot
        dag_executor.archive_terminated_tasks().await;
        let shared_orchestrator = Arc::new(dag_executor);
        self.shared_orchestrator = Some(shared_orchestrator.clone());
        ctx.publish(shared_orchestrator.clone());
        // Note: control is built after action_planner is ready (see below).
        info!("OrchestratorPlugin DAG executor published");

        // ── Build TaskExecutorRouter (task_executors) ──
        let mut exec_loop = crate::TaskExecutorRouter::new();
        // Add all TaskExecutors collected from pin-style
        for executor in task_executors {
            exec_loop = exec_loop.with_executor(executor).await;
        }
        let exec_loop_arc = Arc::new(exec_loop);
        ctx.publish(exec_loop_arc.clone());
        self.exec_loop = Some(exec_loop_arc.clone());
        info!("OrchestratorPlugin executor loop published");

        // ── Build ActionPlanOrchestrator ──
        let object_backend = match ctx.consume_service::<dyn cog_core::ObjectBackend>() {
            Some(b) => b,
            None => {
                return Err(cog_core::SFError::Config(
                    "No ObjectBackend available for OrchestratorPlugin".into(),
                ));
            }
        };

        let decomposition_max_attempts =
            ctx.config().dag_executor.decomposition_max_attempts.max(1);
        let mut action_plan_orchestrator = crate::ActionPlanOrchestrator::new()
            .with_object_backend(object_backend)
            .with_max_pattern_db_size(pattern_db_max_size)
            .with_max_pattern_age_days(pattern_max_age_days)
            .with_task_executor(exec_loop_arc.clone())
            .with_dag_executor(shared_orchestrator.clone())
            .with_decomposition_max_attempts(decomposition_max_attempts)
            .with_self_evolution_timeout_secs(
                ctx.config().dag_executor.self_evolution_timeout_secs,
            );

        if let Some(vb) = ctx.consume_service::<dyn cog_core::VectorBackend>() {
            info!("VectorBackend connected for pattern-db hybrid retrieval");
            action_plan_orchestrator = action_plan_orchestrator.with_vector_backend(vb);
        } else {
            if strict_persistence {
                return Err(cog_core::SFError::Config(
                    "No VectorBackend published; pattern-db would fall back to in-memory retrieval (strict_persistence=true)".into(),
                ));
            }
            warn!("No VectorBackend published. Pattern-db will use in-memory retrieval.");
        }

        action_plan_orchestrator.load_patterns().await;

        if let Ok(pattern_file) = std::env::var("COGNEVA_PATTERN_DB_FILE") {
            let path = std::path::Path::new(&pattern_file);
            if path.exists() {
                if let Err(e) = action_plan_orchestrator
                    .inject_patterns_from_file(path)
                    .await
                {
                    warn!(
                        "Failed to inject seed patterns from {}: {}",
                        pattern_file, e
                    );
                }
            } else {
                warn!(
                    "COGNEVA_PATTERN_DB_FILE set to {} but file not found",
                    pattern_file
                );
            }
        }

        let planner_concrete = Arc::new(action_plan_orchestrator);
        self.action_planner = Some(planner_concrete.clone());
        let planner: Arc<dyn cog_core::ActionPlanner> = planner_concrete;

        // ── Build DagExecutorRuntime ──
        let runtime_backend = match message_backend.clone() {
            Some(b) => b,
            None => {
                return Err(cog_core::SFError::Config(
                    "No message backend available for DagExecutorRuntime".into(),
                ));
            }
        };
        let runtime_config = crate::DagExecutorConfig {
            redis_url,
            workspace_id,
            consumer_group,
            max_retries,
            result_claim_idle_secs: ctx.config().dag_executor.result_claim_idle_secs,
            result_claim_interval_secs: ctx.config().dag_executor.result_claim_interval_secs,
            result_claim_batch: ctx.config().dag_executor.result_claim_batch,
        };
        let skill_registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>> =
            ctx.consume::<tokio::sync::RwLock<cog_core::SkillRegistry>>();
        let dag_executor_runtime =
            crate::DagExecutorRuntime::new_with_dyn_backend(runtime_config, runtime_backend)
                .with_orchestrator(shared_orchestrator.clone())
                .with_action_planner(planner.clone())
                .with_skill_registry(skill_registry.clone());

        ctx.publish(Arc::new(dag_executor_runtime));
        info!("OrchestratorPlugin DAG executor runtime published");

        let skill_registry: Option<Arc<tokio::sync::RwLock<cog_core::SkillRegistry>>> =
            ctx.consume::<tokio::sync::RwLock<cog_core::SkillRegistry>>();

        // Build OrchestratorControlImpl with action_planner + skill_registry for auto-decomposition.
        let control: Arc<dyn cog_core::OrchestratorControl> = Arc::new(
            crate::OrchestratorControlImpl::new(shared_orchestrator.clone())
                .with_action_planner(planner.clone())
                .with_skill_registry(skill_registry.clone()),
        );
        ctx.publish_service(control);
        info!("OrchestratorPlugin DAG executor control published");

        ctx.publish_service(planner);
        info!("OrchestratorPlugin action plan orchestrator published");

        // Observable publish (pin-style)
        ctx.publish_observable(crate::observable::global_observable());
        // Stream pending state is a live gauge of what the consumer loops are
        // holding unacked, not a per-dimension rollup, so it goes up as its own
        // observable that answers whatever dimension is asked.
        ctx.publish_observable(crate::observable::stream_pending_observable());
        info!("OrchestratorPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        // Persistent alert port, published by the observability plugin in
        // init; attach before any goal message can be processed.
        let alert_sink = ctx.consume_service::<dyn cog_core::PersistentAlertSink>();
        if alert_sink.is_none() {
            warn!("no PersistentAlertSink; decomposition failures degrade to error logging");
        }
        if let Some(planner) = &self.action_planner {
            if let Some(sink) = alert_sink.clone() {
                planner.attach_alert_sink(sink).await;
            }
        }

        if let Some(ref orch) = self.shared_orchestrator {
            // Where the DAG's own repairs report themselves. Taken here rather
            // than at init: the storage plugin's layer sits after this one, so
            // the service table does not hold it yet during init. Absent, a
            // repair is only a log line, which does not change what it repairs.
            if let Some(metrics) = ctx.consume_service::<dyn cog_core::MetricsBackend>() {
                orch.attach_metrics(metrics);
            } else {
                warn!("no MetricsBackend; DAG self-repairs report as log lines only");
            }
            // Start archive background loop
            if ctx.config().dag_executor.archive_enabled {
                orch.start_archive_loop();
            }
            if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                let orch = orch.clone();
                let shutdown_rx = broadcast_tx.0.subscribe();
                let checkpoint_stop = cog_core::ShutdownSignal::new();
                let checkpoint_guard = checkpoint_stop.clone();
                // The receiver is shared across attempts: a restart that
                // re-subscribed could miss a shutdown that fired while the loop
                // was dead, and would then keep checkpointing for a process on
                // its way out. This lock has one holder.
                let shutdown_rx = std::sync::Arc::new(tokio::sync::Mutex::new(shutdown_rx));
                drop(cog_core::loop_health::spawn(
                    DAG_CHECKPOINT_LOOP,
                    cog_core::loop_health::Cadence::Periodic(std::time::Duration::from_secs(30)),
                    checkpoint_guard,
                    // Rebuilt per attempt, so everything the body consumes is cloned here.
                    move |beat| {
                        let orch = orch.clone();
                        let shutdown_rx = std::sync::Arc::clone(&shutdown_rx);
                        let checkpoint_stop = checkpoint_stop.clone();
                        async move {
                            let mut interval =
                                tokio::time::interval(std::time::Duration::from_secs(30));
                            interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {
                                beat.beat();
                                let mut shutdown_rx = shutdown_rx.lock().await;
                                tokio::select! {
                                    _ = interval.tick() => {
                                        orch.force_checkpoint().await;
                                        tracing::debug!("DagExecutor periodic checkpoint saved");
                                    }
                                    _ = shutdown_rx.recv() => {
                                        // The broadcast is this loop's own stop event, so
                                        // it must also mark the exit as intended.
                                        checkpoint_stop.trigger();
                                        tracing::info!("DagExecutor checkpoint task shutting down gracefully");
                                        break;
                                    }
                                }
                            }
                        }
                    },
                ));
            }
        }

        // Re-register TaskExecutor services now that all plugins have finished
        // init. This avoids ordering races when collaboration/reflection publish
        // executors in later layers.
        if let Some(ref exec_loop) = self.exec_loop {
            for executor in ctx.consume_all_services::<dyn cog_core::TaskExecutor>() {
                exec_loop.register(executor).await;
            }
        }

        // ── DagExecutorRuntime consumers (message-queue-driven mode) ──
        if let Some(runtime_holder) = ctx.consume::<crate::DagExecutorRuntime>() {
            if let Some(ref exec_loop) = self.exec_loop {
                if let Some(backend) = ctx.consume_service::<dyn cog_core::MessageBackend>() {
                    if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                        let runtime = (*runtime_holder).clone();
                        let exec_loop = exec_loop.clone();
                        let workspace_id = ctx.config().dag_executor.workspace_id.clone();
                        let ready_task_poll_interval_secs =
                            ctx.config().dag_executor.ready_task_poll_interval_secs;
                        // 变更生成的产物写进本进程的 change_dir，而扫它的循环只在
                        // 执行器部署里跑：谁执行生成，谁就得是扫得动那份产物的进程。
                        // 这个开关和执行器循环、基线移植、自我发现用的是同一个。
                        let owns_executor_role = ctx.config().self_evolution.executor_enabled;

                        let dag_shutdown = cog_core::ShutdownSignal::new();
                        let dag_shutdown_clone = dag_shutdown.clone();
                        let mut shutdown_rx = broadcast_tx.0.subscribe();
                        tokio::spawn(async move {
                            let _ = shutdown_rx.recv().await;
                            dag_shutdown_clone.trigger();
                        });

                        // DagExecutorRuntime goal consumer.
                        let goal_shutdown = dag_shutdown.clone();
                        let runtime_clone = runtime.clone();
                        tokio::spawn(async move {
                            if let Err(e) = runtime_clone.run_goal_consumer(goal_shutdown).await {
                                tracing::warn!("DagExecutorRuntime goal consumer exited: {e}");
                            }
                        });

                        // DagExecutorRuntime result consumer.
                        let runtime_shutdown = dag_shutdown.clone();
                        let runtime_clone = runtime.clone();
                        tokio::spawn(async move {
                            if let Err(e) = runtime_clone.run_consumer(runtime_shutdown).await {
                                tracing::warn!("DagExecutorRuntime result consumer exited: {e}");
                            }
                        });

                        // Periodic ready-task publisher.
                        //
                        // It also reclaims tasks stalled in `Scheduled`, whose
                        // one exit is the ready message the publisher handed to
                        // the transport: when that message is consumed and the
                        // task does not start, the publisher is the only scan
                        // whose subject is exactly that state. A task cannot be
                        // found stalled before it is older than the stall
                        // window, so sweeping faster than that window can see
                        // nothing the previous sweep missed — one sweep per
                        // window is the whole resolution this reading has, and
                        // it keeps the extra whole-table scan off the publish
                        // cadence.
                        let scheduled_task_stall_secs =
                            ctx.config().dag_executor.scheduled_task_stall_secs;
                        let reclaim_every_ticks = (scheduled_task_stall_secs
                            / ready_task_poll_interval_secs.max(1))
                        .max(1);
                        let pub_shutdown = dag_shutdown.clone();
                        let publisher_runtime = runtime.clone();
                        drop(cog_core::loop_health::spawn(
                            READY_TASK_PUBLISHER_LOOP,
                            cog_core::loop_health::Cadence::Periodic(
                                std::time::Duration::from_secs(ready_task_poll_interval_secs),
                            ),
                            pub_shutdown.clone(),
                            // Rebuilt per attempt, so everything the body consumes is cloned here.
                            move |beat| {
                                let publisher_runtime = publisher_runtime.clone();
                                let pub_shutdown = pub_shutdown.clone();
                                async move {
                                    let mut interval =
                                        tokio::time::interval(std::time::Duration::from_secs(
                                            ready_task_poll_interval_secs,
                                        ));
                                    interval.set_missed_tick_behavior(
                                        tokio::time::MissedTickBehavior::Skip,
                                    );
                                    // A lost tick only brings the next sweep
                                    // forward, so nothing here has to survive a
                                    // restart.
                                    let mut ticks_since_reclaim = 0u64;
                                    loop {
                                        beat.beat();
                                        tokio::select! {
                                            _ = interval.tick() => {
                                                ticks_since_reclaim += 1;
                                                if ticks_since_reclaim >= reclaim_every_ticks {
                                                    ticks_since_reclaim = 0;
                                                    let reclaimed = publisher_runtime
                                                        .orchestrator()
                                                        .reclaim_stalled_scheduled(
                                                            scheduled_task_stall_secs,
                                                        )
                                                        .await;
                                                    if reclaimed > 0 {
                                                        tracing::warn!(
                                                            reclaimed,
                                                            "reclaimed tasks stalled in Scheduled: their ready messages were consumed without starting them"
                                                        );
                                                    }
                                                }
                                                if let Err(e) =
                                                    publisher_runtime.publish_ready_tasks().await
                                                {
                                                    tracing::warn!("publish_ready_tasks failed: {e}");
                                                }
                                            }
                                            _ = pub_shutdown.wait() => break,
                                        }
                                    }
                                }
                            },
                        ));

                        // Lease renewer: every worker deployment claims tasks off
                        // the shared ready queue, so every one of them has to keep
                        // the leases of its own claims fresh — a deployment that
                        // claimed a task and stopped renewing it would have that
                        // task taken away by whoever sweeps next. The renewal is
                        // scoped to this process's run id, so a deployment holding
                        // no task renews nothing and the loop is a no-op query.
                        let renew_shutdown = dag_shutdown.clone();
                        let renew_orchestrator = self
                            .shared_orchestrator
                            .clone()
                            .expect("shared orchestrator");
                        let renew_lease_secs = ctx.config().dag_executor.task_lease_secs;
                        let cadence = std::time::Duration::from_secs((renew_lease_secs / 3).max(1));
                        drop(cog_core::loop_health::spawn(
                            TASK_LEASE_RENEWER_LOOP,
                            cog_core::loop_health::Cadence::Periodic(cadence),
                            renew_shutdown.clone(),
                            // Rebuilt per attempt, so everything the body consumes is cloned here.
                            move |beat| {
                                let renew_orchestrator = renew_orchestrator.clone();
                                let renew_shutdown = renew_shutdown.clone();
                                async move {
                                    let mut interval = tokio::time::interval(cadence);
                                    interval.set_missed_tick_behavior(
                                        tokio::time::MissedTickBehavior::Skip,
                                    );
                                    loop {
                                        beat.beat();
                                        tokio::select! {
                                            _ = interval.tick() => {
                                                if let Err(e) =
                                                    renew_orchestrator.renew_leases().await
                                                {
                                                    tracing::warn!("task lease renewal failed: {e}");
                                                }
                                            }
                                            _ = renew_shutdown.wait() => break,
                                        }
                                    }
                                }
                            },
                        ));

                        // Task checkpoint producer: the write side of the resume
                        // chain. A task knows its progress only while it runs, so
                        // the checkpoint has to be written on a cadence — the
                        // moment a task is killed or its version is rolled over
                        // there is no time left to save anything. Every process
                        // writes checkpoints for the tasks it holds; a process
                        // holding none (the idle replica, the control-plane pod)
                        // enumerates nothing and the round is a no-op query.
                        let checkpoint_store =
                            ctx.consume_service::<dyn cog_core::CheckpointStore>();
                        let checkpoint_manager =
                            ctx.consume_service::<dyn cog_core::AgentManager>();
                        let checkpoint_interval_secs =
                            ctx.config().dag_executor.task_checkpoint_interval_secs;
                        match (checkpoint_store, checkpoint_manager) {
                            (Some(store), Some(manager)) if checkpoint_interval_secs > 0 => {
                                let agents =
                                    Arc::new(crate::LiveCheckpointAgents::new(manager, store));
                                let checkpoint_shutdown = dag_shutdown.clone();
                                let checkpoint_orchestrator = self
                                    .shared_orchestrator
                                    .clone()
                                    .expect("shared orchestrator");
                                let cadence =
                                    std::time::Duration::from_secs(checkpoint_interval_secs);
                                drop(cog_core::loop_health::spawn(
                                    TASK_CHECKPOINT_LOOP,
                                    cog_core::loop_health::Cadence::Periodic(cadence),
                                    checkpoint_shutdown.clone(),
                                    // Rebuilt per attempt, so everything the body consumes is cloned here.
                                    move |beat| {
                                        let agents = agents.clone();
                                        let checkpoint_orchestrator =
                                            checkpoint_orchestrator.clone();
                                        let checkpoint_shutdown = checkpoint_shutdown.clone();
                                        async move {
                                            let mut interval = tokio::time::interval(cadence);
                                            interval.set_missed_tick_behavior(
                                                tokio::time::MissedTickBehavior::Skip,
                                            );
                                            // 词表先落地：这一格只在真有任务在跑时才会
                                            // 加值，没有任务的部署里整条计数器都查不到，
                                            // 而那与「产出侧没接上」同形。发布在这里而
                                            // 不是启动处，是因为它就该随循环的启用与否
                                            // 出现——循环关掉时不该留下一条恒为 0 的读数。
                                            checkpoint_orchestrator
                                                .publish_checkpoint_outcomes()
                                                .await;
                                            loop {
                                                beat.beat();
                                                tokio::select! {
                                                    _ = interval.tick() => {
                                                        let saved = checkpoint_orchestrator
                                                            .checkpoint_owned_tasks(agents.as_ref())
                                                            .await;
                                                        if saved > 0 {
                                                            tracing::debug!(
                                                                saved,
                                                                "task checkpoint producer wrote resume points"
                                                            );
                                                        }
                                                    }
                                                    _ = checkpoint_shutdown.wait() => break,
                                                }
                                            }
                                        }
                                    },
                                ));
                            }
                            // 0 关掉它：不登记一个永远不会做事的循环——「空转」与
                            // 「这条链在工作」在读数上同形，那正是本链当初的病。
                            (_, _) if checkpoint_interval_secs == 0 => {
                                info!("task checkpoint producer disabled by config");
                            }
                            _ => {
                                warn!(
                                    "task checkpoint producer not started: the agent manager or the checkpoint store is unavailable, so a checkpoint could not be taken or read back"
                                );
                            }
                        }

                        // Decomposition orphan reconciler.
                        let reconcile_shutdown = dag_shutdown.clone();
                        let reconcile_runtime = runtime.clone();
                        let reconcile_sink = alert_sink.clone();
                        let orphan_cfg = ctx.config().dag_executor.clone();
                        tokio::spawn(async move {
                            reconcile_runtime
                                .run_orphan_reconciler(
                                    orphan_cfg.decomposition_orphan_watch_enabled,
                                    orphan_cfg.decomposition_orphan_poll_interval_secs,
                                    orphan_cfg.decomposition_orphan_stall_after_secs,
                                    orphan_cfg.decomposition_orphan_alert_dwell_secs,
                                    reconcile_sink,
                                    reconcile_shutdown,
                                )
                                .await;
                        });

                        // TaskExecutorRouter task consumer.
                        let exec_shutdown = dag_shutdown.clone();
                        let orchestrator = self
                            .shared_orchestrator
                            .clone()
                            .expect("shared orchestrator");
                        let exec_loop = exec_loop.as_ref().clone().with_orchestrator(Arc::new(
                            crate::OrchestratorControlImpl::new(orchestrator),
                        )
                            as Arc<dyn cog_core::OrchestratorControl>);
                        tokio::spawn(async move {
                            if let Err(e) = exec_loop
                                .run_consumer(
                                    backend.clone(),
                                    backend,
                                    &workspace_id,
                                    owns_executor_role,
                                    exec_shutdown,
                                )
                                .await
                            {
                                tracing::warn!("TaskExecutorRouter consumer exited: {e}");
                            }
                        });
                    }
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        info!("OrchestratorPlugin shutdown");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "orchestrator",
    // `agent` is a strong edge because the checkpoint producer reads the agent
    // manager while this plugin's `init` wires it up: only `requires` orders
    // `init` in this framework, and a soft edge would leave that read racing the
    // pool's publish — losing the race does not fail, it silently leaves the
    // resume chain without its write side, which is the state this edge exists
    // to end.
    requires: &["storage", "agent"],
    optional_requires: &["llm", "stream", "collaboration", "extension"],
    factory: || Box::new(OrchestratorPlugin::new()),
};
