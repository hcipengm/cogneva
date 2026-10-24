//! Gateway plugin — implements [`cog_core::SystemPlugin`].

use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};
use tracing::{info, warn};

type ServerHandle = tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

/// Gateway plugin that self-assembles [`crate::GatewayState`] and drives the HTTP server lifecycle.
pub struct GatewayPlugin {
    state: Option<Arc<crate::GatewayState>>,
    /// 一个监听一个 sender：本进程现在有两个面（主 API 与零凭证内部面），各自
    /// 起停。合成一个 sender 的话，后开的那个面一存进来就把先开的那个的 sender
    /// 顶掉——被顶掉的那端立刻收到"发送方没了"，于是主 API 在启动时就自己关了。
    shutdown_tx: Mutex<Vec<oneshot::Sender<()>>>,
    server_handle: Mutex<Vec<ServerHandle>>,
    initialized: bool,
}

impl GatewayPlugin {
    /// Create a plugin that will build the gateway state during `init`.
    pub fn new() -> Self {
        Self {
            state: None,
            shutdown_tx: Mutex::new(Vec::new()),
            server_handle: Mutex::new(Vec::new()),
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

        // 出站客户端：由 net 插件发布，本进程所有出站（含贡献通道调
        // github/gitee 的那些）都用这一份，不再各自从配置造一份。
        let http_client: Arc<dyn cog_core::HttpClient> =
            ctx.require_service::<dyn cog_core::HttpClient>()?;
        crate::contribution_admin::install_http_client(http_client).map_err(|e| {
            cog_core::SFError::Config(format!("outbound client install failed: {e}"))
        })?;

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
        // 聊天记录的落点。没有这个池就是这套部署没有那个库：那一趟照样回答，只是
        // 不写下来——绝不用进程内的一张表顶替它，那正是这次改动拆掉的形状。
        let chat_pool = ctx
            .consume::<cog_storage::MessagesPool>()
            .and_then(|p| p.0.clone());
        let chat_turns: crate::ChatTurnHandle = chat_pool.as_ref().map(|pool| {
            Arc::new(cog_storage::PostgresChatTurnStore::new(pool.clone()))
                as Arc<dyn cog_storage::ChatTurnStore>
        });
        // 对象存储与它的目录。上传面此前拿到的是 None，于是这条路由在现网永远是
        // 503；即便接上后端，桶里也只有字节、没有一行说它们是什么。两半都在才装：
        // 只有目录会让恢复去取不存在的对象，只有桶则留下谁也点不到名的数据。
        let object_backend: Option<Arc<dyn cog_core::ObjectBackend>> =
            ctx.consume_service::<dyn cog_core::ObjectBackend>();
        let datasets: Option<Arc<cog_storage::DatasetStore>> = match (&object_backend, &chat_pool) {
            (Some(objects), Some(pool)) => Some(Arc::new(cog_storage::DatasetStore::new(
                objects.clone(),
                Arc::new(cog_storage::PostgresDatasetCatalog::new(pool.clone())),
            ))),
            _ => None,
        };
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
        // Login rate limiting shares the key-value face the session manager uses;
        // without one the limiter stays off (login itself still works).
        let key_value: Option<Arc<dyn cog_core::KeyValueStore>> =
            ctx.consume_service::<dyn cog_core::KeyValueStore>();
        let login_rate_limiter: Option<Arc<crate::auth::LoginRateLimiter>> =
            match key_value.as_ref() {
                Some(store) => Some(Arc::new(crate::auth::LoginRateLimiter::new(
                    store.clone(),
                    crate::auth::LOGIN_MAX_ATTEMPTS,
                    crate::auth::LOGIN_WINDOW_SECONDS,
                ))),
                None => {
                    warn!("login rate limiter disabled — no key-value store published");
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
            downstream_event_publisher(ctx);
        let websocket_client: Option<Arc<dyn cog_core::WebSocketClient>> =
            ctx.consume_service::<dyn cog_core::WebSocketClient>();
        let _http_client: Arc<dyn cog_core::HttpClient> =
            ctx.require_service::<dyn cog_core::HttpClient>()?;
        let agent_pool = ctx.consume_service::<dyn cog_core::AgentManager>();
        // The inbox queue, for agents this process does not own. Optional for
        // the same reason the pool is: a gateway assembled without the stream
        // plugin serves the same routes and answers a cross-process address
        // with a refusal rather than pretending it was queued.
        let message_backend = ctx.consume_service::<dyn cog_core::MessageBackend>();
        let evolution_admin: Option<Arc<dyn cog_core::EvolutionAdmin>> =
            ctx.consume_service::<dyn cog_core::EvolutionAdmin>();
        let evolution_stream =
            ctx.consume::<tokio::sync::broadcast::Sender<cog_core::EvolutionChangeInfo>>();
        let audit_stream: Option<Arc<dyn cog_core::AuditStream>> =
            ctx.consume_service::<dyn cog_core::AuditStream>();
        // 外部提交的价值判定落到存储面，由持有共用库的插件发布；本进程只拿写面。
        let taste_intent_sink: Option<Arc<dyn cog_core::TasteIntentSink>> =
            ctx.consume_service::<dyn cog_core::TasteIntentSink>();
        // 组装好的知识层（memory + wiki 的合成体，含混合检索与重排），由 wiki 插件发布。
        // 它和上面那两个 backend 不是一回事：那两个是各自的店，这个是检索真正去问的那一层。
        let knowledge_backend: Option<Arc<dyn cog_core::KnowledgeBackend>> =
            ctx.consume_service::<dyn cog_core::KnowledgeBackend>();
        // 令牌花费的读出面（读的是 durable rollup，不是逐笔账本），由
        // observability 插件发布。缺席时那条路由显式报「未配置」，而不是回一笔空账。
        let llm_usage_reader: Option<Arc<dyn cog_core::LlmUsageReader>> =
            ctx.consume_service::<dyn cog_core::LlmUsageReader>();

        // ── Build auxiliary components ──

        let backend_health_probe = crate::state_builder::init_backend_health_probe(
            config,
            &pg_pool_explain,
            &memory_backend,
            &key_value,
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
            &knowledge_backend,
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
            &message_backend,
            &event_publisher,
            &media_backend,
            &notification_dispatcher,
            &notification_tx,
            &ctx.consume_service::<dyn cog_core::NotificationStore>(),
            &websocket_client,
            &evolution_admin,
            &evolution_stream,
            &audit_stream,
            &taste_intent_sink,
            &llm_usage_reader,
            &ctx.consume_service::<dyn cog_core::GovernanceStore>(),
            &chat_turns,
            &object_backend,
            &datasets,
            // 拿不到就是这套部署没有发布停机信号，那它拿到的是一个永不触发的
            // 信号，语义等同于「没有人给过它停的办法」。
            &ctx.consume::<cog_core::ShutdownSignal>()
                .map(|s| (*s).clone())
                .unwrap_or_default(),
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
        let app = crate::create_router(state.clone());
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

        self.shutdown_tx.lock().await.push(shutdown_tx);
        self.server_handle.lock().await.push(handle);

        // 零凭证的集群内面：跑 squad 的 Pod 手里没有凭证，主 API 那几条 memory
        // 路由按角色判，它永远进不来。所以那几条挂到另一个端口上，判据换成网络
        // 事实（见部署面的 ingress 策略）。端口为 0＝这个部署不开这条面。
        let internal_port = ctx.config().gateway.internal_port;
        if internal_port != 0 {
            let internal_app = crate::create_internal_router(state.clone());
            let internal_addr = std::net::SocketAddr::from(([0, 0, 0, 0], internal_port));
            let (internal_shutdown_tx, internal_shutdown_rx) = oneshot::channel::<()>();
            let internal_listener = tokio::net::TcpListener::bind(internal_addr)
                .await
                .map_err(|e| cog_core::SFError::IO(format!("bind {internal_addr} failed: {e}")))?;
            info!("internal API listening on http://{}", internal_addr);
            let internal_handle = tokio::spawn(async move {
                axum::serve(internal_listener, internal_app)
                    .with_graceful_shutdown(async move {
                        let _ = internal_shutdown_rx.await;
                        warn!("Shutdown signal received, stopping internal server...");
                    })
                    .await
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
            });
            self.shutdown_tx.lock().await.push(internal_shutdown_tx);
            self.server_handle.lock().await.push(internal_handle);
        }

        // ── Hook -> WebSocket forwarder ──
        if let Some(hook_engine) = ctx.consume_service::<dyn cog_core::HookEngine>() {
            if let Some(event_tx) =
                ctx.consume::<tokio::sync::broadcast::Sender<cog_core::AgentEvent>>()
            {
                if let Some(publisher) = ctx.consume_service::<dyn cog_core::EventPublisher>() {
                    // 停机信号：拿得到就挂在进程的停机信号上（触发后退出的那一次
                    // 不算「循环死了」）；拿不到就是这套部署没有发布停机信号，
                    // 那它拿到的是一个永不触发的信号，语义等同于「没有人给过它
                    // 停的办法」。两种情况都比「退出不可见」好。
                    // `consume` 交回的是 `Arc<ShutdownSignal>`（发布侧存的就是那个），
                    // 这里取信号本身，所以多一层解引用克隆。
                    let forwarder_stop = ctx
                        .consume::<cog_core::ShutdownSignal>()
                        .map(|s| (*s).clone())
                        .unwrap_or_default();
                    crate::hook_forwarder::HookToWsForwarder::spawn(
                        hook_engine.clone(),
                        (*event_tx).clone(),
                        Some(publisher),
                        ctx.consume_service::<dyn cog_core::MetricsBackend>(),
                        forwarder_stop,
                    );
                }
            }
        }

        // ── Collaboration listener + timeout checker ──
        //
        // 这几个后台循环都挂在进程的停机信号上，与它们在循环表里报的名字同源：
        // 信号触发后它们退出，退出才不会被记成「循环死了」。
        if let Some(shutdown_signal) = ctx.consume::<cog_core::ShutdownSignal>() {
            let state = self.state.clone().expect("gateway state not initialized");
            let _collab_handle = crate::executor::spawn_collaboration_listener(
                state.clone(),
                (*shutdown_signal).clone(),
            );
            let _timeout_handle = crate::executor::spawn_timeout_checker(
                state.clone(),
                (*shutdown_signal).clone(),
                ctx.config().system.timeout_checker_interval_secs,
            );
            // Gitee OAuth token refresher: no-ops unless OAuth-mode material
            // is present in the Secret.
            let gateway_cfg = &ctx.config().gateway;
            let _gitee_refresh = crate::contribution_admin::spawn_gitee_token_refresher(
                (*shutdown_signal).clone(),
                gateway_cfg.effective_contribution_oauth_refresh_interval_secs(),
                gateway_cfg.effective_contribution_oauth_refresh_threshold_secs(),
            );
            // git 身份自举：网关自己得有上游身份，否则装完机没人给它配密钥。
            // 有 token 全自动登记部署密钥；没有则把公钥挂出来等 WebUI 收 token。
            let _git_identity =
                crate::git_identity::spawn_git_identity_bootstrap((*shutdown_signal).clone());
        }

        Ok(())
    }

    async fn shutdown(&self) -> cog_core::SFResult<()> {
        for tx in self.shutdown_tx.lock().await.drain(..) {
            let _ = tx.send(());
        }
        for handle in self.server_handle.lock().await.drain(..) {
            if let Err(e) = handle.await {
                warn!("gateway server shutdown error: {}", e);
            }
        }
        info!("gateway plugin shutdown complete");
        Ok(())
    }
}

/// The publisher the downstream consumers actually read.
///
/// With the event plane switched on, the supervisor re-ingest and the memory
/// ingestor subscribe to the dedicated plane backend. The plain
/// [`cog_core::EventPublisher`] is bound to the task-queue backend, so events
/// published through it land on a stream nobody reads — the hook forwarder
/// would report success and lose every event. Prefer the plane publisher
/// whenever the stream plugin published one and fall back to the global one
/// on deployments that serve both roles from a single backend.
fn downstream_event_publisher(
    ctx: &cog_core::PluginContext,
) -> Option<Arc<dyn cog_core::EventPublisher>> {
    ctx.consume::<cog_core::EventPlanePublisher>()
        .map(|plane| plane.0.clone())
        .or_else(|| ctx.consume_service::<dyn cog_core::EventPublisher>())
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingPublisher(Arc<AtomicUsize>);

    #[async_trait::async_trait]
    impl cog_core::EventPublisher for CountingPublisher {
        async fn publish(&self, _event: &cog_core::AgentEvent) -> cog_core::SFResult<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn an_event() -> cog_core::AgentEvent {
        cog_core::AgentEvent::TaskStatusChange {
            task_id: "t-1".into(),
            status: "hook_event".into(),
            agent_id: Some("a-1".into()),
            crew_id: Some("c-1".into()),
            squad_id: Some("s-1".into()),
            timestamp: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn a_published_event_reaches_the_plane_the_readers_subscribe_to() {
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        let plane = Arc::new(CountingPublisher(Arc::new(AtomicUsize::new(0))));
        let global = Arc::new(CountingPublisher(Arc::new(AtomicUsize::new(0))));
        ctx.publish(Arc::new(cog_core::EventPlanePublisher(plane.clone())));
        let global_dyn: Arc<dyn cog_core::EventPublisher> = global.clone();
        ctx.publish_service(global_dyn);

        let chosen = downstream_event_publisher(&ctx).expect("a publisher must be chosen");
        chosen.publish(&an_event()).await.unwrap();

        assert_eq!(
            plane.0.load(Ordering::SeqCst),
            1,
            "the plane is where the supervisor and the ingestor read"
        );
        assert_eq!(
            global.0.load(Ordering::SeqCst),
            0,
            "the task-queue publisher has no reader once the plane is on"
        );
    }

    #[tokio::test]
    async fn without_a_plane_the_global_publisher_is_used() {
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        let global = Arc::new(CountingPublisher(Arc::new(AtomicUsize::new(0))));
        let global_dyn: Arc<dyn cog_core::EventPublisher> = global.clone();
        ctx.publish_service(global_dyn);

        let chosen = downstream_event_publisher(&ctx).expect("a publisher must be chosen");
        chosen.publish(&an_event()).await.unwrap();

        assert_eq!(global.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_backend_means_no_publisher() {
        let ctx = cog_core::PluginContext::new(cog_core::Config::default());
        assert!(downstream_event_publisher(&ctx).is_none());
    }
}
