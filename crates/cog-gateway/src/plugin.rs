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
        let event_tx = ctx.require::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()?;
        let event_tx = (*event_tx).clone();
        let task_event_tx = ctx
            .require::<tokio::sync::broadcast::Sender<cog_core::TaskEvent>>()?
            .clone();
        let jwt_manager = ctx.require_service::<dyn cog_core::AuthProvider>()?;
        let quota_manager = ctx.require_service::<dyn cog_core::QuotaManager>()?.clone();
        let hierarchy_manager: Option<Arc<dyn cog_core::HierarchyManager>> =
            ctx.consume_service::<dyn cog_core::HierarchyManager>();
        let raw_logger = ctx.require_service::<dyn cog_core::RawLogger>()?;
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
            .consume::<cog_storage::ExplainPool>()
            .and_then(|p| p.0.clone());
        let hook_archive = ctx.consume_service::<dyn cog_core::HookArchive>();
        let media_backend = ctx.consume_service::<dyn cog_core::MediaBackend>();
        let shared_orchestrator: Arc<dyn cog_core::OrchestratorControl> = ctx
            .require_service::<dyn cog_core::OrchestratorControl>()?
            .clone();
        let task_executors: Arc<dyn cog_core::TaskExecutor> =
            ctx.require_service::<dyn cog_core::TaskExecutor>()?.clone();
        let agent_registry = ctx.require_service::<dyn cog_core::AgentRegistry>()?;
        let observability_gateway = ctx.consume_service::<dyn cog_core::ObservabilityGateway>();
        let wiki_adapter = ctx.consume_service::<dyn cog_core::WikiBackend>();
        let supervisor: Arc<dyn cog_core::Supervisor> =
            ctx.require_service::<dyn cog_core::Supervisor>()?.clone();
        let alert_store: Arc<dyn cog_core::AlertStore> =
            ctx.require_service::<dyn cog_core::AlertStore>()?.clone();
        // Optional: absent when no database is configured, in which case
        // alerts stay notification-only and the durable half is simply empty.
        let active_alert_source: Option<Arc<dyn cog_core::ActiveAlertSource>> =
            ctx.consume_service::<dyn cog_core::ActiveAlertSource>();
        let supervisor_registry: Arc<dyn cog_core::HeartbeatRegistry> = ctx
            .require_service::<dyn cog_core::HeartbeatRegistry>()?
            .clone();
        let snapshot_store = ctx.require_service::<dyn cog_core::CheckpointStore>()?;
        let trace_store = ctx.require_service::<dyn cog_core::TraceStore>()?;
        let replay_engine: Arc<dyn cog_core::ReplayEngine> =
            ctx.require_service::<dyn cog_core::ReplayEngine>()?;
        let session_manager = ctx.require_service::<dyn cog_core::SessionManager>()?;
        // Account system: PG-backed user store + platform identity linkage,
        // published by the storage plugin. Absent → bootstrap login paths
        // (admin password / demo switch) stay in effect.
        let user_store: Option<Arc<dyn cog_core::UserStore>> =
            ctx.consume_service::<dyn cog_core::UserStore>();
        let platform_identities: Option<Arc<dyn cog_core::PlatformIdentityStore>> =
            ctx.consume_service::<dyn cog_core::PlatformIdentityStore>();
        // 贡献通道属主控制（策略门禁 + 暂存补发），由平台集成插件发布；
        // 缺失时策略/暂存路由降级为显式错误。
        let contribution_control: Option<Arc<dyn cog_core::ContributionControl>> =
            ctx.consume_service::<dyn cog_core::ContributionControl>();
        // Login rate limiting shares the Redis the session manager uses;
        // without Redis the limiter stays off (login itself still works).
        let login_rate_limiter: Option<Arc<crate::auth::LoginRateLimiter>> =
            match ctx.consume::<cog_storage::RedisClient>() {
                Some(client) => match cog_redis::connect(&client.0).await {
                    Ok(conn) => Some(Arc::new(crate::auth::LoginRateLimiter::new(
                        conn,
                        crate::auth::LOGIN_MAX_ATTEMPTS,
                        crate::auth::LOGIN_WINDOW_SECONDS,
                    ))),
                    Err(e) => {
                        warn!("login rate limiter disabled — redis connection failed: {e}");
                        None
                    }
                },
                None => {
                    warn!("login rate limiter disabled — redis client not published");
                    None
                }
            };
        let sandbox_backend = ctx.require_service::<dyn cog_core::SandboxBackend>()?;
        let plugin_registry = ctx.require_service::<dyn cog_core::PluginRegistry>()?;
        let guardrail = ctx.require_service::<dyn cog_core::Guardrail>()?;
        let eval_service: Option<Arc<dyn cog_core::EvalService>> =
            ctx.consume_service::<dyn cog_core::EvalService>();
        let observables: Vec<Arc<dyn cog_core::Observable>> =
            ctx.consume_all_services::<dyn cog_core::Observable>();
        info!(
            count = observables.len(),
            "Gateway consumed Observable services"
        );
        let mcp_client: Option<Arc<dyn cog_core::McpClient>> =
            ctx.consume_service::<dyn cog_core::McpClient>();
        let external_skill_registry = ctx.consume_service::<dyn cog_core::ExternalSkillRegistry>();
        let event_publisher: Option<Arc<dyn cog_core::EventPublisher>> =
            ctx.consume_service::<dyn cog_core::EventPublisher>();
        let websocket_client: Option<Arc<dyn cog_core::WebSocketClient>> =
            ctx.consume_service::<dyn cog_core::WebSocketClient>();
        let _http_client: Arc<dyn cog_core::HttpClient> =
            ctx.require_service::<dyn cog_core::HttpClient>()?;
        let agent_pool = ctx.consume_service::<dyn cog_core::AgentManager>();
        let evolution_admin: Option<Arc<dyn cog_core::EvolutionAdmin>> =
            ctx.consume_service::<dyn cog_core::EvolutionAdmin>();
        let evolution_stream =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::EvolutionChangeInfo>>();
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
            &active_alert_source,
            &backend_health_probe,
            &supervisor_registry,
            &snapshot_store,
            &trace_store,
            &replay_engine,
            &session_manager,
            &login_rate_limiter,
            &user_store,
            &platform_identities,
            &contribution_control,
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
        // 进程重启后把 Secret 里持久化的回流策略灌回控制器（best-effort；
        // 网关持有 Secret 访问权，平台插件进程内只留活档）。
        if let Some(control) = state.contribution_control.clone() {
            tokio::spawn(async move {
                // 只有读得到 Secret 的那个进程才是属主。其余部署（进化 / 沙盒 /
                // 网关）同一个二进制同样会走到这里，读不到却照旧宣称「已从集群
                // Secret 恢复」——把一次读不到说成了成功。
                if !crate::contribution_admin::is_contribution_secret_owner().await {
                    return;
                }
                let Ok(kube) = crate::llm_admin::KubeClient::in_cluster() else {
                    warn!("contribution policy not restored: no in-cluster client");
                    return;
                };
                match crate::contribution_admin::read_contrib_config(&kube).await {
                    Some(config) => {
                        let policy = crate::contribution_admin::policy_from_config(&config);
                        control.set_policy(policy);
                        info!(
                            policy = policy.as_str(),
                            "contribution policy restored from cluster secret"
                        );
                    }
                    None => warn!(
                        "contribution policy not restored: the stored config could not be read"
                    ),
                }
            });
        }
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
            let _timeout_handle = crate::executor::spawn_timeout_checker(
                state.clone(),
                broadcast_tx.0.subscribe(),
                ctx.config().system.timeout_checker_interval_secs,
            );
            // Gitee OAuth token refresher: no-ops unless OAuth-mode material
            // is present in the Secret.
            let gateway_cfg = &ctx.config().gateway;
            let _gitee_refresh = crate::contribution_admin::spawn_gitee_token_refresher(
                broadcast_tx.0.subscribe(),
                gateway_cfg.effective_contribution_oauth_refresh_interval_secs(),
                gateway_cfg.effective_contribution_oauth_refresh_threshold_secs(),
            );
            // git 身份自举：网关自己得有上游身份，否则装完机没人给它配密钥。
            // 有 token 全自动登记部署密钥；没有则把公钥挂出来等 WebUI 收 token。
            let _git_identity =
                crate::git_identity::spawn_git_identity_bootstrap(broadcast_tx.0.subscribe());
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
        "github",
    ],
    factory: || Box::new(GatewayPlugin::new()),
};
