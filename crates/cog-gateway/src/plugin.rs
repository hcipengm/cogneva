//! Gateway plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

type ServerHandle = tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

/// Gateway plugin that self-assembles [`crate::GatewayState`] and drives the HTTP server lifecycle.
pub struct GatewayPlugin {
    state: Option<Arc<crate::GatewayState>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_handle: Mutex<Option<ServerHandle>>,
    initialized: bool,
}

impl GatewayPlugin {
    /// Create a plugin that will build the gateway state during `init`.
    pub fn new() -> Self {
        Self {
            state: None,
            shutdown_tx: Mutex::new(None),
            server_handle: Mutex::new(None),
            initialized: false,
        }
    }
}

impl Default for GatewayPlugin {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl cog_core::SystemPlugin for GatewayPlugin {
    fn name(&self) -> &'static str {
        "gateway"
    }

    async fn init(&mut self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        if self.initialized {
            return Ok(());
        }

        let config = ctx.config();

        // ── Consume dependencies ──
        let event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            .expect("event sender");
        let event_tx = (*event_tx).clone();
        let task_event_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::TaskEvent>>()
            .expect("task event sender")
            .clone();
        let jwt_manager = ctx
            .consume_service::<dyn cog_core::AuthProvider>()
            .expect("jwt manager");
        let quota_manager = ctx
            .consume_service::<dyn cog_core::QuotaManager>()
            .expect("quota manager")
            .clone();
        let hierarchy_manager: Option<Arc<dyn cog_core::HierarchyManager>> =
            ctx.consume_service::<dyn cog_core::HierarchyManager>();
        let raw_logger = ctx
            .consume_service::<dyn cog_core::RawLogger>()
            .expect("raw logger");
        let memory_backend = ctx.consume_service::<dyn cog_core::MemoryBackend>();
        let memory_ingestor = ctx.consume_service::<dyn cog_core::MemoryIngestor>();
        let metrics_backend = ctx.consume_service::<dyn cog_core::MetricsBackend>();
        let metrics_exporter: Option<Arc<dyn cog_core::MetricsExporter>> =
            ctx.consume_service::<dyn cog_core::MetricsExporter>();
        let search_backend: Option<Arc<dyn cog_core::SearchBackend>> =
            ctx.consume_service::<dyn cog_core::SearchBackend>();
        let raw_log_index_store = ctx.consume_service::<dyn cog_core::RawLogIndexStore>();
        let hook_engine: Option<Arc<dyn cog_core::HookEngine>> =
            ctx.consume_service::<dyn cog_core::HookEngine>();
        let pg_pool_explain = ctx
            .consume::<cog_core::ExplainPool>()
            .and_then(|p| p.0.clone());
        let hook_archive = ctx.consume_service::<dyn cog_core::HookArchive>();
        let media_backend = ctx.consume_service::<dyn cog_core::MediaBackend>();
        let shared_orchestrator: Arc<dyn cog_core::OrchestratorControl> = ctx
            .consume_service::<dyn cog_core::OrchestratorControl>()
            .expect("orchestrator control")
            .clone();
        let task_executors: Arc<dyn cog_core::TaskExecutor> = ctx
            .consume_service::<dyn cog_core::TaskExecutor>()
            .expect("task executor")
            .clone();
        let agent_registry = ctx
            .consume_service::<dyn cog_core::AgentRegistry>()
            .expect("agent registry");
        let observability_gateway = ctx.consume_service::<dyn cog_core::ObservabilityGateway>();
        let wiki_adapter = ctx.consume_service::<dyn cog_core::WikiBackend>();
        let supervisor: Arc<dyn cog_core::Supervisor> = ctx
            .consume_service::<dyn cog_core::Supervisor>()
            .expect("supervisor")
            .clone();
        let alert_store: Arc<dyn cog_core::AlertStore> = ctx
            .consume_service::<dyn cog_core::AlertStore>()
            .expect("alert store")
            .clone();
        let supervisor_registry: Arc<dyn cog_core::HeartbeatRegistry> = ctx
            .consume_service::<dyn cog_core::HeartbeatRegistry>()
            .expect("supervisor registry")
            .clone();
        let snapshot_store = ctx
            .consume_service::<dyn cog_core::CheckpointStore>()
            .expect("snapshot store");
        let trace_store = ctx
            .consume_service::<dyn cog_core::TraceStore>()
            .expect("trace store");
        let replay_engine: Arc<dyn cog_core::ReplayEngine> = ctx
            .consume_service::<dyn cog_core::ReplayEngine>()
            .expect("replay engine");
        let session_manager = ctx
            .consume_service::<dyn cog_core::SessionManager>()
            .expect("session manager");
        let sandbox_backend = ctx
            .consume_service::<dyn cog_core::SandboxBackend>()
            .expect("sandbox backend");
        let plugin_registry = ctx
            .consume_service::<dyn cog_core::PluginRegistry>()
            .expect("plugin registry");
        let guardrail = ctx
            .consume_service::<dyn cog_core::Guardrail>()
            .expect("guardrail");
        let eval_service: Option<Arc<dyn cog_core::EvalService>> =
            ctx.consume_service::<dyn cog_core::EvalService>();
        let observables: Vec<Arc<dyn cog_core::Observable>> =
            ctx.consume_all_services::<dyn cog_core::Observable>();
        let mcp_client: Option<Arc<dyn cog_core::McpClient>> =
            ctx.consume_service::<dyn cog_core::McpClient>();
        let external_skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();
        let event_publisher: Option<Arc<dyn cog_core::EventPublisher>> =
            ctx.consume_service::<dyn cog_core::EventPublisher>();
        let websocket_client: Option<Arc<dyn cog_core::WebSocketClient>> =
            ctx.consume_service::<dyn cog_core::WebSocketClient>();
        let _http_client: Arc<dyn cog_core::HttpClient> = ctx
            .consume_service::<dyn cog_core::HttpClient>()
            .expect("http client");
        let agent_pool = ctx.consume_service::<dyn cog_core::AgentManager>();
        let evolution_admin: Option<Arc<dyn cog_core::EvolutionAdmin>> =
            ctx.consume_service::<dyn cog_core::EvolutionAdmin>();
        let evolution_stream =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::EvolutionPatchInfo>>();
        let audit_stream: Option<Arc<dyn cog_core::AuditStream>> =
            ctx.consume_service::<dyn cog_core::AuditStream>();

        // ── Build auxiliary components ──

        let backend_health_probe = crate::state_builder::init_backend_health_probe(
            config,
            &pg_pool_explain,
            &memory_backend,
        );
        // media_backend is consumed from cog-storage plugin as dyn MediaBackend
        let notification_dispatcher = ctx.consume_service::<dyn cog_core::NotificationDispatcher>();
        let notification_tx = ctx
            .consume::<tokio::sync::broadcast::Sender<cog_core::Notification>>()
            .map(|t| (*t).clone())
            .unwrap_or_else(|| tokio::sync::broadcast::channel(16).0);

        // ── Build GatewayState ──
        let gateway_state = crate::state_builder::build_gateway_state(
            config,
            &event_tx,
            &task_event_tx,
            &jwt_manager,
            &quota_manager,
            &hierarchy_manager,
            &raw_logger,
            &memory_backend,
            &memory_ingestor,
            &metrics_backend,
            &metrics_exporter,
            &search_backend,
            &raw_log_index_store,
            &hook_engine,
            &hook_archive,
            &shared_orchestrator,
            task_executors,
            &agent_registry,
            &observability_gateway,
            &wiki_adapter,
            &supervisor,
            &alert_store,
            &backend_health_probe,
            &supervisor_registry,
            &snapshot_store,
            &trace_store,
            &replay_engine,
            &session_manager,
            &sandbox_backend,
            &plugin_registry,
            &guardrail,
            &eval_service,
            observables,
            &mcp_client,
            &external_skill_registry,
            &agent_pool,
            &event_publisher,
            &media_backend,
            &notification_dispatcher,
            &notification_tx,
            &ctx.consume_service::<dyn cog_core::NotificationStore>(),
            &websocket_client,
            &evolution_admin,
            &evolution_stream,
            &audit_stream,
        )
        .await
        .map_err(|e| cog_core::SFError::Config(format!("gateway state build failed: {}", e)))?;

        ctx.publish(gateway_state.clone());
        info!("GatewayState published");

        let task_runner = Arc::new(crate::executor::GatewayTaskRunner::new(
            gateway_state.clone(),
        ));
        let task_runner_dyn: Arc<dyn cog_core::TaskExecutionCallback> = task_runner;
        ctx.publish_service(task_runner_dyn);
        info!("TaskExecutionCallback published");

        self.state = Some(gateway_state);
        self.initialized = true;
        Ok(())
    }

    async fn start(&self, ctx: &cog_core::PluginContext) -> cog_core::SFResult<()> {
        let state = self.state.clone().expect("gateway state not initialized");
        let app = crate::create_router(state);
        let http_port = ctx.config().gateway.http_port;
        let addr = std::net::SocketAddr::from(([0, 0, 0, 0], http_port));
        info!("HTTP server listening on http://{}", addr);

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| cog_core::SFError::IO(format!("bind failed: {e}")))?;

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                    warn!("Shutdown signal received, stopping server...");
                })
                .await
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        });

        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        *self.server_handle.lock().await = Some(handle);

        // ── Hook -> WebSocket forwarder ──
        if let Some(hook_engine) = ctx.consume_service::<dyn cog_core::HookEngine>() {
            if let Some(event_tx) =
                ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            {
                if let Some(publisher) = ctx.consume_service::<dyn cog_core::EventPublisher>() {
                    crate::hook_forwarder::HookToWsForwarder::spawn(
                        hook_engine.clone(),
                        (*event_tx).clone(),
                        Some(publisher),
                    );
                }
            }
        }

        // ── Collaboration listener + timeout checker ──
        if let Some(broadcast_tx) = ctx.consume::<cog_core::ShutdownBroadcastTx>() {
            let state = self.state.clone().expect("gateway state not initialized");
            let _collab_handle = crate::executor::spawn_collaboration_listener(
                state.clone(),
                broadcast_tx.0.subscribe(),
            );
            let _bridge_handle = crate::executor::spawn_task_event_bridge(
                state.clone(),
                broadcast_tx.0.subscribe(),
            );
            let _timeout_handle = crate::executor::spawn_timeout_checker(
                state.clone(),
                broadcast_tx.0.subscribe(),
                ctx.config().system.timeout_checker_interval_secs,
            );
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.server_handle.lock().await.take() {
            if let Err(e) = handle.await {
                warn!("gateway server shutdown error: {}", e);
            }
        }
        info!("gateway plugin shutdown complete");
        Ok(())
    }
}

/// Static descriptor for auto-discovery.
pub const DESCRIPTOR: cog_core::PluginDescriptor = cog_core::PluginDescriptor {
    name: "gateway",
    requires: &[
        "stream",
        "auth",
        "quota",
        "storage",
        "orchestrator",
        "supervisor",
        "extension",
        "guardrail",
        "observability",
        "net",
    ],
    optional_requires: &[
        "memory",
        "agent",
        "wiki",
        "eval",
        "protocol",
        "notification",
        "reflection",
    ],
    provides: &["GatewayState", "TaskExecutionCallback"],
    consumes: &[
        cog_core::ConsumeSpec {
            type_name: "Sender<AgentEvent>",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Sender<TaskEvent>",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "AuthProvider",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "QuotaManager",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "RawLogger",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "OrchestratorControl",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "TaskExecutor",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "AgentRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Supervisor",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "AlertStore",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "HeartbeatRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "CheckpointStore",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "TraceStore",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "ReplayEngine",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "SessionManager",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "SandboxBackend",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "PluginRegistry",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Guardrail",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "HttpClient",
            required: true,
        },
        cog_core::ConsumeSpec {
            type_name: "Observable",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "HierarchyManager",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MemoryBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MemoryIngestor",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MetricsBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MetricsExporter",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "SearchBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "RawLogIndexStore",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "HookEngine",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ExplainPool",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "HookArchive",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "MediaBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ObservabilityGateway",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "WikiBackend",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "EvalService",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "McpClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "ExternalSkillRegistry",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "EventPublisher",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "WebSocketClient",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "AgentManager",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "NotificationDispatcher",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "Sender<Notification>",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "NotificationStore",
            required: false,
        },
        cog_core::ConsumeSpec {
            type_name: "EvolutionAdmin",
            required: false,
        },
    ],
    factory: || Box::new(GatewayPlugin::new()),
};
