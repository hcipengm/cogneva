//! Supervisor plugin — implements [`cog_core::SystemPlugin`].

use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

/// Holder so `watch::Sender<SupervisorConfig>` can be stored in [`cog_core::PluginContext`].
pub struct SupervisorConfigTxHolder(pub tokio::sync::watch::Sender<crate::SupervisorConfig>);

/// Supervisor plugin that self-assembles the supervision lifecycle.
pub struct SupervisorPlugin {
    initialized: bool,
    supervisor: Option<Arc<crate::Supervisor>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    task_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SupervisorPlugin {
    /// Create a plugin that will build supervisor services during `init`.
    pub fn new() -> Self {
        Self {
            initialized: false,
            supervisor: None,
            shutdown_tx: Mutex::new(None),
            task_handle: Mutex::new(None),
        }
    }
}

impl Default for SupervisorPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for SupervisorPlugin {
    fn name(&self) -> &'static str {
        "supervisor"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        // Snapshot config values to drop immutable borrow before publishing.
        let (
            supervisor_config,
            _observability_event_channel_capacity,
            alert_history_max,
            dag_executor_workspace_id,
        ) = {
            let config = ctx.config();
            (
                config.supervisor.clone(),
                config.system.observability_event_channel_capacity,
                config.supervisor.alert_history_max,
                config.dag_executor.workspace_id.clone(),
            )
        };

        // ── Consume dependencies ──
        let quota_manager = ctx
            .consume_service::<dyn cog_core::WorkspaceQuotaSource>()
            .expect("quota manager");
        let state_backend = ctx
            .consume_service::<dyn cog_core::StateBackend>()
            .expect("state_backend");
        let supervisor_orchestrator = ctx
            .consume_service::<dyn cog_core::OrchestratorControl>()
            .expect("orchestrator control");
        let event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            .expect("event sender");
        let event_tx = (*event_tx).clone();
        let meta_learning = ctx.consume_service::<dyn cog_core::MetaLearning>();

        // ── Consume observability gateway ──
        let gateway = ctx
            .consume_service::<dyn cog_core::ObservabilityGateway>()
            .expect("observability gateway")
            .clone();

        // ── Build supervisor registry ──
        let supervisor_registry = Arc::new(
            crate::AgentRegistry::new()
                .with_heartbeat_history_max(supervisor_config.heartbeat_history_max),
        );
        ctx.publish(supervisor_registry.clone());
        info!("SupervisorPlugin registry published");

        // ── Build supervisor ──
        let (supervisor_config_tx, supervisor_config_rx) =
            tokio::sync::watch::channel(supervisor_config.clone().into());

        let scheduler_gate = Arc::new(crate::SchedulerGate::new());

        let supervisor = Arc::new(
            crate::Supervisor::new(
                supervisor_config.into(),
                supervisor_registry.clone(),
                state_backend.clone(),
                supervisor_orchestrator,
                quota_manager,
                gateway,
                scheduler_gate.clone(),
                event_tx.subscribe(),
                meta_learning,
            )
            .with_config_watch(supervisor_config_rx),
        );
        info!("Supervisor created");
        supervisor.track_workspace(dag_executor_workspace_id);

        // ── Build alert store ──
        let alert_store = Arc::new(crate::AlertStore::with_max_alerts(alert_history_max));
        let alert_event_rx = supervisor.subscribe();
        let alert_store_for_task = alert_store.clone();
        tokio::spawn(async move { alert_store_for_task.run(alert_event_rx).await });

        // ── Publish everything ──
        ctx.publish_service(supervisor.clone() as Arc<dyn cog_core::Supervisor>);
        let alert_store_trait: Arc<dyn cog_core::AlertStore> = alert_store.clone();
        ctx.publish_service(alert_store_trait);
        ctx.publish_service(scheduler_gate.clone() as Arc<dyn cog_core::SchedulerGate>);
        ctx.publish_service(supervisor_registry.clone() as Arc<dyn cog_core::HeartbeatRegistry>);
        ctx.publish(Arc::new(SupervisorConfigTxHolder(supervisor_config_tx)));
        ctx.publish(Arc::new(supervisor.event_sender()));
        info!("SupervisorPlugin event sender published");

        // ── Publish binary switcher for self-evolution deployments ──
        let self_evolution = ctx.config().self_evolution.clone();
        if self_evolution.enabled {
            let switcher_config = crate::binary_switcher::BinarySwitcherConfig {
                binary_dir: PathBuf::from(&self_evolution.binary_dir),
                binary_name: "cogneva".into(),
                health_url: format!("http://127.0.0.1:{}/health", ctx.config().gateway.http_port),
                health_check_grace_period_secs: self_evolution.health_check_grace_period_secs,
                health_check_interval_secs: self_evolution.health_check_interval_secs,
                health_check_max_retries: self_evolution.health_check_max_retries,
                systemd_service_name: "cogneva".into(),
                sidecar_socket_path: PathBuf::from("/run/cogneva/sidecar.sock"),
            };
            let switcher: Arc<dyn cog_core::BinarySwitcher> =
                crate::binary_switcher::build_switcher(
                    &self_evolution.switch_mode,
                    switcher_config,
                );
            ctx.publish_service(switcher);
            info!(
                mode = %self_evolution.switch_mode,
                "SupervisorPlugin binary switcher published"
            );
        }

        self.supervisor = Some(supervisor);
        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let supervisor = self.supervisor.clone().expect("supervisor not initialized");

        let handle = tokio::spawn(async move {
            supervisor
                .run(async move {
                    shutdown_rx.await.ok();
                })
                .await;
            info!("Supervisor task exited");
        });

        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        *self.task_handle.lock().await = Some(handle);

        // ── Multi-backend consumer ──
        let config = ctx.config();
        if config.multi_backend_consumer.enabled {
            if let Some(backend) = ctx.consume_service::<dyn cog_core::MessageBackend>() {
                if let Some(event_tx) =
                    ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
                {
                    let consumer = crate::MultiBackendEventConsumer::new(
                        backend.clone(),
                        (*event_tx).clone(),
                        &config.multi_backend_consumer.channel,
                    )
                    .with_retry_interval(config.multi_backend_consumer.retry_interval_secs);
                    info!("MultiBackendEventConsumer started (primary: NATS/Redis)");
                    consumer.spawn();
                }
            } else {
                info!("MultiBackendEventConsumer disabled: no message backend available");
            }
        } else {
            info!("MultiBackendEventConsumer disabled");
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.task_handle.lock().await.take() {
            if let Err(e) = handle.await {
                warn!("supervisor task shutdown error: {}", e);
            }
        }
        info!("supervisor plugin shutdown complete");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "supervisor",
    requires: &[
        "quota",
        "storage",
        "orchestrator",
        "stream",
        "observability",
    ],
    optional_requires: &["reflection"],
    provides: &[
        "Supervisor",
        "AlertStore",
        "HeartbeatRegistry",
        "SchedulerGate",
        "SupervisorConfigTx",
        "Sender<SupervisorEvent>",
        "BinarySwitcher",
    ],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "WorkspaceQuotaSource",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "StateBackend",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "OrchestratorControl",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Sender<AgentEvent>",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "ObservabilityGateway",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "MetaLearning",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MessageBackend",
            required: false,
        },
    ],
    factory: || Box::new(SupervisorPlugin::new()),
};
