//! Orchestrator plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tracing::{info, warn};

/// Orchestrator plugin that self-assembles the DAG executor and related services.
pub struct OrchestratorPlugin {
    initialized: bool,
    shared_orchestrator: Option<Arc<crate::DagExecutor>>,
    exec_loop: Option<Arc<crate::TaskExecutorRouter>>,
}

impl OrchestratorPlugin {
    /// Create a plugin that will build orchestrator services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            shared_orchestrator: None,
            exec_loop: None,
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
        let task_event_tx = (*ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::TaskEvent>>()
            .expect("task_event_tx"))
        .clone();

        let state_backend = ctx
            .consume_service::<dyn cog_core::StateBackend>()
            .expect("state_backend");

        let raw_logger = ctx
            .consume_service::<dyn cog_core::RawLogger>()
            .expect("raw logger");

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
            );
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

        let mut action_plan_orchestrator = crate::ActionPlanOrchestrator::new()
            .with_object_backend(object_backend)
            .with_max_pattern_db_size(pattern_db_max_size)
            .with_max_pattern_age_days(pattern_max_age_days)
            .with_task_executor(exec_loop_arc.clone())
            .with_dag_executor(shared_orchestrator.clone());

        if let Some(vb) = ctx.consume_service::<dyn cog_core::VectorBackend>() {
            info!("VectorBackend connected for pattern-db hybrid retrieval");
            action_plan_orchestrator = action_plan_orchestrator.with_vector_backend(vb);
        } else {
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

        let planner: Arc<dyn cog_core::ActionPlanner> = Arc::new(action_plan_orchestrator);

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
        ctx.publish_service(crate::observable::global_observable());
        info!("OrchestratorPlugin observable published");

        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if let Some(ref orch) = self.shared_orchestrator {
            // Start archive background loop
            if ctx.config().dag_executor.archive_enabled {
                orch.start_archive_loop();
            }
            if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
                let orch = orch.clone();
                let mut shutdown_rx = broadcast_tx.0.subscribe();
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            _ = interval.tick() => {
                                orch.force_checkpoint().await;
                                tracing::debug!("DagExecutor periodic checkpoint saved");
                            }
                            _ = shutdown_rx.recv() => {
                                tracing::info!("DagExecutor checkpoint task shutting down gracefully");
                                break;
                            }
                        }
                    }
                });
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
                        let pub_shutdown = dag_shutdown.clone();
                        tokio::spawn(async move {
                            let mut interval = tokio::time::interval(
                                std::time::Duration::from_secs(ready_task_poll_interval_secs),
                            );
                            interval
                                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                            loop {
                                tokio::select! {
                                    _ = interval.tick() => {
                                        if let Err(e) = runtime.publish_ready_tasks().await {
                                            tracing::warn!("publish_ready_tasks failed: {e}");
                                        }
                                    }
                                    _ = pub_shutdown.wait() => break,
                                }
                            }
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

        // ── StaleTaskDetector ──
        if let Some(agent_registry) = ctx.consume_service::<dyn cog_core::AgentRegistry>() {
            let snapshot_store = ctx.consume_service::<dyn cog_core::CheckpointStore>();
            let state_backend = ctx.consume_service::<dyn cog_core::StateBackend>();
            if let (Some(snapshot_store), Some(state_backend)) = (snapshot_store, state_backend) {
                let config = ctx.config();
                let _redis_url = config.dag_executor.redis_url.clone();
                let poll_secs = config.system.stale_task_detector_poll_secs;

                let transfer_backend: Arc<dyn cog_core::MessageBackend> =
                    match ctx.consume_service::<dyn cog_core::MessageBackend>() {
                        Some(b) => b,
                        None => {
                            warn!("No MessageBackend available for StaleTaskDetector. Skipping.");
                            return Ok(());
                        }
                    };
                let transfer_coordinator = Arc::new(crate::TaskTransferCoordinator::new(
                    transfer_backend.clone(),
                    snapshot_store,
                    state_backend,
                ));
                if let Err(e) = transfer_backend
                    .create_consumer_group(transfer_coordinator.stream_name(), "cogneva-transfer")
                    .await
                {
                    if !e.to_string().contains("BUSYGROUP") {
                        warn!(
                            "create_consumer_group({}) failed: {}",
                            transfer_coordinator.stream_name(),
                            e
                        );
                    }
                }
                let stale_detector = Arc::new(
                    crate::StaleTaskDetector::new(agent_registry, transfer_coordinator)
                        .with_poll_interval(std::time::Duration::from_secs(poll_secs)),
                );
                if let Some(shutdown_signal) = ctx.consume::<cog_core::ShutdownSignal>() {
                    let _stale_handle = stale_detector.clone().spawn((*shutdown_signal).clone());
                    info!("StaleTaskDetector started");
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
    requires: &["storage"],
    optional_requires: &["llm", "stream", "collaboration", "extension"],
    provides: &[
        "OrchestratorControl",
        "TaskExecutor",
        "ActionPlanner",
        "DagExecutorRuntime",
        "Observable",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "Sender<TaskEvent>",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "StateBackend",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "RawLogger",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "LlmClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MessageBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ObjectBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "VectorBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "AgentRegistry",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "CheckpointStore",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "TaskExecutor",
            required: false,
        },
    ],
    factory: || Box::new(OrchestratorPlugin::new()),
};
