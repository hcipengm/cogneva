//! Orchestrator plugin — implements [`cog_core::SystemPlugin`].
//! 它在插件框架里自组装 DAG 执行器及其后台循环。

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

/// How many stall windows a task may wait behind a queue's undelivered
/// entries before the sweep takes it back anyway.
///
/// A backlog explains a slow message, not an absent one, but only for as long
/// as it could plausibly drain: the count says how many entries are waiting,
/// not whether anything is reading them. Three windows is long enough that a
/// queue merely behind on capacity still gets its turn, and short enough that
/// a queue nobody drains stops being an excuse — past it the row is re-armed
/// and counted apart, so the queue's refusal to move becomes a reading instead
/// of an indefinite silence.
pub const BACKLOG_WINDOWS_BEFORE_RECLAIM: u64 = 3;

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

        // ── Dead-letter queue ──
        //
        // The executor writes a task here once `fail_task` reports it can never
        // succeed. The concrete backend is the storage plugin's to own; this
        // plugin speaks the `cog-core` trait and consumes the service, exactly
        // as it takes the state backend, raw logger and message backend above.
        // Taken with `require_service`, not `consume_service`, so a deployment
        // that publishes no queue fails here by name rather than starting an
        // executor whose permanently-failed tasks silently go nowhere -- which
        // is the state this wiring exists to end.
        let dlq = ctx.require_service::<dyn cog_core::DeadLetterQueue>()?;

        // ── Build DagExecutor ──
        //
        // The circuit-breaker registry is deliberately not wired here. The
        // executor records successes and failures into it, but nothing ever
        // calls the registry's `allow()` gate and no metric reads its state, so
        // a wired registry would accumulate breaker state nobody consumes -- a
        // mechanism with no reader, which is a second source of truth about
        // task health rather than a control on it. Making it live is a separate
        // change that has to add the reader (a dispatch-site `allow()` check);
        // until then the field stays `None` on purpose, and the reason is
        // written down here so "never built" and "built but never read" do not
        // look the same from outside.
        let dag_executor = crate::DagExecutor::new(workspace_id.clone())
            .with_event_tx(task_event_tx.clone())
            .with_raw_logger(raw_logger.clone())
            .with_state_backend(state_backend.clone())
            .with_dlq(dlq)
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
            ready_task_poll_interval_secs: ctx.config().dag_executor.ready_task_poll_interval_secs,
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
        // The current shape of the task graph, read from the executor that owns
        // it. Separate from the D1 counters above: those meter this process's
        // outcomes, this one reports the graph itself, which outlives the
        // process and is what a task ledger's readers ask about.
        ctx.publish_observable(Arc::new(crate::observable::TaskGraphObservable::new(
            shared_orchestrator.clone(),
        )));
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
            // 入队准入读的是池自己的读数。在这里取而不是 init：supervisor 与
            // 本插件都在 init 发布，谁先谁后不由本 crate 决定，而 start 是所有
            // init 都跑完之后才开始的。取不到就是一个进程内的部署——池的判定
            // 本来也没有第二个读者，准入随之不设界（读不到不等于关窗）。
            match ctx.consume_service::<dyn cog_core::LlmPoolStatusSource>() {
                Some(source) => runtime_holder.attach_pool_status_source(source),
                None => info!(
                    "OrchestratorPlugin: no LLM pool status source; ready-queue admission admits everything"
                ),
            }
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
                        // 本部署真正在读的就绪队列。清扫器与发布器都按它划界：
                        // 不读的那条队列上，任务停在 Scheduled 是"等执行器回来"，
                        // 不是"消息丢了"。
                        let served_ready_streams = crate::ready_queue::consumed_ready_streams(
                            &workspace_id,
                            owns_executor_role,
                        );
                        // 与消费者同一个组名：问积压和收消息必须是同一组，否则
                        // 问到的是一条不存在的组，读出来永远是「没有积压」。
                        let ready_group = crate::ready_queue::ready_group(&workspace_id);

                        // Seeded before either consumer starts, so the series is
                        // present from the first scrape rather than from the
                        // first task event: a counter absent for want of
                        // traffic reads the same as one that was never wired.
                        runtime.orchestrator().seed_event_bus_kinds().await;

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
                        // It also re-arms tasks stalled in `Scheduled`, whose
                        // one exit is the ready message the publisher handed to
                        // the transport: when that message is consumed and the
                        // task does not start, the publisher is the only scan
                        // whose subject is exactly that state. A task cannot be
                        // found stalled before it is older than the stall
                        // window, so sweeping faster than that window can see
                        // nothing the previous sweep missed — one sweep per
                        // window is the whole resolution this reading has, and
                        // it keeps the extra whole-table scan off the publish
                        // cadence. It is also what bounds the re-arm: a task put
                        // back in line leaves `Scheduled`, so the next sweep can
                        // only see it again after another full window.
                        let scheduled_task_stall_secs =
                            ctx.config().dag_executor.scheduled_task_stall_secs;
                        let reclaim_every_ticks = (scheduled_task_stall_secs
                            / ready_task_poll_interval_secs.max(1))
                        .max(1);
                        let pub_shutdown = dag_shutdown.clone();
                        let publisher_runtime = runtime.clone();
                        let publisher_served_streams = served_ready_streams.clone();
                        let publisher_backend = backend.clone();
                        let publisher_ready_group = ready_group.clone();
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
                                let served_ready_streams = publisher_served_streams.clone();
                                let publisher_backend = publisher_backend.clone();
                                let ready_group = publisher_ready_group.clone();
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
                                                    // 只扫本部署真的在读的队列：不读的
                                                    // 那条上，堆积就是任务在等执行器回来，
                                                    // 每窗重投一次只会在没人读的队列里
                                                    // 复制出更多消息。
                                                    //
                                                    // 每条被扫的队列再问一句「还有多少条
                                                    // 没投出去」：有积压时消息可能正排在
                                                    // 队伍里，这时重投只是给同一条队伍再
                                                    // 添一份。传达不到就按「没有读数」
                                                    // 扫，与问不出积压的传输层同一条路。
                                                    let mut undelivered =
                                                        std::collections::HashMap::new();
                                                    for stream in &served_ready_streams {
                                                        let lag = match publisher_backend
                                                            .stream_lag(stream, &ready_group)
                                                            .await
                                                        {
                                                            Ok(lag) => lag,
                                                            Err(e) => {
                                                                tracing::warn!(
                                                                    stream = %stream,
                                                                    error = %e,
                                                                    "cannot read how many entries the ready stream still owes this group; sweeping it as if there were no backlog"
                                                                );
                                                                None
                                                            }
                                                        };
                                                        undelivered.insert(stream.clone(), lag);
                                                    }
                                                    let reclaimed = publisher_runtime
                                                        .orchestrator()
                                                        .reclaim_stalled_scheduled(
                                                            scheduled_task_stall_secs,
                                                            &served_ready_streams,
                                                            &undelivered,
                                                            BACKLOG_WINDOWS_BEFORE_RECLAIM,
                                                        )
                                                        .await;
                                                    if reclaimed > 0 {
                                                        tracing::warn!(
                                                            reclaimed,
                                                            "re-armed tasks that reached the end of the stall window without ever starting; no attempt was charged"
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
                                // 单步超时的兜底：整轮（枚举本进程全部在跑任务并
                                // 逐个落点）的总耗时也必须有界。预算随循环自己注册
                                // 的节拍推导（留 5s 余量、至少 10s），不是写死的
                                // 常量；节拍调大，这条判断跟着走。名册再大、下游再
                                // 慢，控制权也会在节拍内回到循环顶部，beat 不会停。
                                let round_budget = std::cmp::max(
                                    cadence.saturating_sub(std::time::Duration::from_secs(5)),
                                    std::time::Duration::from_secs(10),
                                );
                                // 申报 = 推导自施加方：工期界取施加它的对象
                                // （整轮预算），节拍取循环真正睡眠的等待，所以
                                // 告警的墙随二者一起动，而不是一个会过时的整数。
                                let declared = cog_core::loop_health::Cadence::PeriodicWithWork {
                                    period: cadence,
                                    work: round_budget,
                                };
                                drop(cog_core::loop_health::spawn(
                                    TASK_CHECKPOINT_LOOP,
                                    declared,
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
                                                        match tokio::time::timeout(
                                                            round_budget,
                                                            checkpoint_orchestrator
                                                                .checkpoint_owned_tasks(
                                                                    agents.as_ref(),
                                                                ),
                                                        )
                                                        .await
                                                        {
                                                            Ok(saved) => {
                                                                if saved > 0 {
                                                                    tracing::debug!(
                                                                        saved,
                                                                        "task checkpoint producer wrote resume points"
                                                                    );
                                                                }
                                                            }
                                                            Err(_) => {
                                                                tracing::warn!(
                                                                    budget_secs =
                                                                        round_budget
                                                                            .as_secs(),
                                                                    "task checkpoint round exceeded its budget and was cut short; some tasks were not checkpointed this cycle"
                                                                );
                                                            }
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

        // Change ledger loop.
        //
        // The ledger's authoritative copy is the platform's own table, and this
        // is the one loop that maintains it: resident, elected through the owner
        // lease, reporting through the background-loop liveness family. It is
        // started only where a ledger store was published -- a process whose
        // storage holds no shared database cannot keep one account, and a
        // process-local ledger would report its own state as the platform's.
        // Not started is not a fault here: a deployment without a shared
        // database is one that has no ledger to keep.
        match ctx.consume_service::<dyn cog_core::ChangeLedgerStore>() {
            Some(store) => {
                let ledger_shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                crate::change_ledger::spawn_change_ledger_loop(
                    store,
                    ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>(),
                    ctx.consume_service::<dyn cog_core::OrchestratorControl>(),
                    ctx.consume_service::<dyn cog_core::LlmPoolStatusSource>(),
                    ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    ledger_shutdown,
                    crate::change_measure::tree_from_environment(),
                    // The principle ledger is the third intake: the rows it calls
                    // unpaid are what this account aims a change at. Optional,
                    // the same way the arbitrating role is -- a process whose
                    // storage holds no shared database keeps no principle ledger
                    // either, and refusing to keep the change account over that
                    // would trade a missing intake for a missing ledger.
                    ctx.consume_service::<dyn cog_core::PrincipleLedgerStore>(),
                    // The second face of "is it done": whoever can read which
                    // revision the workloads are running publishes it, and this
                    // loop compares its landed rows against that. Optional, like
                    // the other collaborators -- a process with no client or no
                    // grant to the cluster has no such reading to offer, and a
                    // loop that refused to keep the account over that would
                    // trade a missing reading for a missing ledger. Which it is
                    // is said at start-up.
                    ctx.consume_service::<dyn cog_core::LiveRevisionSource>(),
                    // The event store, which is where a handover's product
                    // lands. This loop is the reader that gets back to it from
                    // the account's own rows: the id a change was dispatched
                    // under is a function of its identity, so what it produced
                    // is found again without an edge stored anywhere. Optional
                    // like the loop's other collaborators, and said so at
                    // start-up when it is absent: a reader with no store would
                    // write the same zeros on every pass as a pass that read
                    // nothing.
                    ctx.consume_service::<dyn cog_core::StateBackend>(),
                );
            }
            None => info!("change ledger loop not started: no ChangeLedgerStore published"),
        }

        // Principle ledger loop.
        //
        // Same shape as the change ledger's and for the same reasons: the
        // account's authoritative copy is the platform's own table, this is the
        // one loop that maintains it, and it is elected through the owner lease
        // so the table has one writer. It is started only where a ledger store
        // was published, and not being started is not a fault -- a deployment
        // without a shared database has no account to keep.
        match ctx.consume_service::<dyn cog_core::PrincipleLedgerStore>() {
            Some(store) => {
                let principle_shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                crate::principle_ledger::spawn_principle_ledger_loop(
                    store,
                    ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>(),
                    principle_shutdown,
                    // The same tree the change ledger measures against: what the
                    // platform's own source declares about itself is read out of
                    // the commit this deployment builds from, one repository and
                    // one mount.
                    crate::change_measure::tree_from_environment(),
                );
            }
            None => info!("principle ledger loop not started: no PrincipleLedgerStore published"),
        }

        // Resource ledger loop.
        //
        // The same four parts the accounts above play -- authoritative table,
        // resident reader, consumer, write back -- over the two facts this
        // platform makes about resources that leave nothing behind: a bound the
        // control law moved at runtime, and a piece of work a gate turned away.
        // Started only where an event store was published, and not being
        // started is not a fault: a deployment without a shared database has
        // nowhere to put either, and the process still counts its own refusals
        // on the reading face it already publishes.
        match ctx.consume_service::<dyn cog_core::ResourceEventStore>() {
            Some(store) => {
                let resource_shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                crate::resource_ledger::spawn_resource_ledger_loop(
                    store,
                    ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>(),
                    ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    resource_shutdown,
                );
            }
            None => info!("resource ledger loop not started: no ResourceEventStore published"),
        }

        // Backup account loop.
        //
        // The account's rows are written by the backup runs -- separate
        // processes on a schedule, each with its own connection -- and this is
        // the only reader. It is resident rather than part of the backup program
        // because the case it exists for is a run that was killed: the process
        // that would have published a reading for it is the one that died, so
        // nothing inside a run can report that the run never came back. The same
        // residency is what lets this loop be the one that concludes such a row:
        // a run that was killed cannot come back to close its own row, and the
        // next run is a day away at best -- long after every reader has spent
        // that day counting a run that is not going.
        //
        // Started only where the store was published, and not started is not a
        // fault: a deployment without a shared database holds no account to
        // read, and the backup runner writes its rows there too.
        match ctx.consume_service::<dyn cog_core::BackupRunStore>() {
            Some(store) => {
                let backup_shutdown = ctx
                    .consume::<cog_core::ShutdownSignal>()
                    .map(|s| (*s).clone())
                    .unwrap_or_default();
                crate::backup_runs::spawn_backup_runs_loop(
                    store,
                    ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>(),
                    ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                    backup_shutdown,
                );
            }
            None => info!("backup account loop not started: no BackupRunStore published"),
        }

        // SOP dispatch loop.
        //
        // Which faces to keep planned is configuration, not code: the loop runs
        // only where the environment names them, and a deployment that names
        // none carries the capability dormant and makes no outbound call. Every
        // missing piece is a reason not to start rather than a reason to run
        // half-wired -- a dispatch loop that cannot fetch, cannot inject, or
        // cannot say which hands exist would report a face's plan as kept while
        // nothing about it was.
        match crate::sop::SopDispatchConfig::from_env() {
            None => info!("SOP dispatch not started: no task face sources configured"),
            Some(config) => {
                let http = ctx.consume_service::<dyn cog_core::HttpClient>();
                let skills = ctx.consume::<tokio::sync::RwLock<cog_core::SkillRegistry>>();
                match (self.action_planner.clone(), http, skills) {
                    (Some(planner), Some(http), Some(skills)) => {
                        let shutdown = ctx
                            .consume::<cog_core::ShutdownSignal>()
                            .map(|s| (*s).clone())
                            .unwrap_or_default();
                        crate::sop::spawn_sop_dispatch_loop(
                            planner as Arc<dyn cog_core::ActionPlanner>,
                            skills,
                            http,
                            ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                            ctx.consume_service::<dyn cog_core::OwnerLeaseBroker>(),
                            shutdown,
                            config,
                        );
                    }
                    (planner, http, skills) => warn!(
                        planner = planner.is_some(),
                        http = http.is_some(),
                        skills = skills.is_some(),
                        "SOP dispatch not started: a face list is configured but the \
                         planner, the fetch client, or the skill registry is not published"
                    ),
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
