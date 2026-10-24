//! GatewayState builder — moved from cogneva assembly layer.

use std::sync::Arc;
use tokio::sync::broadcast;

/// 循环名：把 supervisor 事件排进网关缓存的消费环，报在 `cogneva_loop_*` 的
/// `loop` 标签上。
///
/// 前缀是本仓库已有的约定（[`crate::executor::COLLABORATION_LISTENER_LOOP`] 也叫
/// `gateway_*`）；与它落后时写的 `consumer` 标签值
/// （[`crate::LAG_CONSUMER_SUPERVISOR_EVENT_CACHE`]）只在后半段同字面——两个面说
/// 的是同一个消费者，用后缀对起来。
pub const SUPERVISOR_EVENT_CACHE_LOOP: &str = "gateway_supervisor_event_cache";

/// Backend health probe.
pub fn init_backend_health_probe(
    config: &cog_core::Config,
    pg_pool_explain: &Option<sqlx::PgPool>,
    memory_backend: &Option<Arc<dyn cog_core::MemoryBackend>>,
    key_value: &Option<Arc<dyn cog_core::KeyValueStore>>,
) -> Option<Arc<crate::backend_health::BackendHealthProbe>> {
    Some(Arc::new(crate::backend_health::BackendHealthProbe {
        key_value: key_value.clone(),
        pg_pool: pg_pool_explain.clone(),
        nats_url: config.dag_executor.nats_url.clone(),
        memory_backend: memory_backend.clone(),
    }))
}

/// Assemble the [`crate::GatewayState`] — the single source of truth for all
/// runtime components accessible from HTTP/WebSocket handlers.
#[allow(clippy::too_many_arguments)]
pub async fn build_gateway_state(
    config: &cog_core::Config,
    event_tx: &broadcast::Sender<cog_core::AgentEvent>,
    task_event_tx: &broadcast::Sender<cog_core::TaskEvent>,
    jwt_manager: &Arc<dyn cog_core::AuthProvider>,
    quota_manager: &Arc<dyn cog_core::QuotaManager>,
    hierarchy_manager: &Option<Arc<dyn cog_core::HierarchyManager>>,
    raw_logger: &Arc<dyn cog_core::RawLogger>,
    memory_backend: &Option<Arc<dyn cog_core::MemoryBackend>>,
    memory_ingestor: &Option<Arc<dyn cog_core::MemoryIngestor>>,
    metrics_backend: &Option<Arc<dyn cog_core::MetricsBackend>>,
    metrics_exporter: &Option<Arc<dyn cog_core::MetricsExporter>>,
    search_backend: &Option<Arc<dyn cog_core::SearchBackend>>,
    raw_log_index_store: &Option<Arc<dyn cog_core::RawLogIndexStore>>,
    hook_engine: &Option<Arc<dyn cog_core::HookEngine>>,
    hook_archive: &Option<Arc<dyn cog_core::HookArchive>>,
    shared_orchestrator: &Arc<dyn cog_core::OrchestratorControl>,
    task_executors: Arc<dyn cog_core::TaskExecutor>,
    agent_registry: &Arc<dyn cog_core::AgentRegistry>,
    observability_gateway: &Option<Arc<dyn cog_core::ObservabilityGateway>>,
    wiki_adapter: &Option<Arc<dyn cog_core::WikiBackend>>,
    knowledge_backend: &Option<Arc<dyn cog_core::KnowledgeBackend>>,
    supervisor: &Arc<dyn cog_core::Supervisor>,
    alert_store: &Arc<dyn cog_core::AlertStore>,
    active_alert_source: &Option<Arc<dyn cog_core::ActiveAlertSource>>,
    backend_health_probe: &Option<Arc<crate::backend_health::BackendHealthProbe>>,
    supervisor_registry: &Arc<dyn cog_core::HeartbeatRegistry>,
    snapshot_store: &Arc<dyn cog_core::CheckpointStore>,
    trace_store: &Arc<dyn cog_core::TraceStore>,
    replay_engine: &Arc<dyn cog_core::ReplayEngine>,
    session_manager: &Arc<dyn cog_core::SessionManager>,
    login_rate_limiter: &Option<Arc<crate::auth::LoginRateLimiter>>,
    user_store: &Option<Arc<dyn cog_core::UserStore>>,
    platform_identities: &Option<Arc<dyn cog_core::PlatformIdentityStore>>,
    contribution_control: &Option<Arc<dyn cog_core::ContributionControl>>,
    sandbox_backend: &Arc<dyn cog_core::SandboxBackend>,
    plugin_registry: &Arc<dyn cog_core::PluginRegistry>,
    guardrail: &Arc<dyn cog_core::Guardrail>,
    eval_service: &Option<Arc<dyn cog_core::EvalService>>,
    observables: Vec<Arc<dyn cog_core::Observable>>,
    mcp_client: &Option<Arc<dyn cog_core::McpClient>>,
    external_skill_registry: &Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    agent_pool: &Option<Arc<dyn cog_core::AgentManager>>,
    message_backend: &Option<Arc<dyn cog_core::MessageBackend>>,
    event_publisher: &Option<Arc<dyn cog_core::EventPublisher>>,
    media_backend: &Option<Arc<dyn cog_core::MediaBackend>>,
    notification_dispatcher: &Option<Arc<dyn cog_core::NotificationDispatcher>>,
    notification_tx: &broadcast::Sender<cog_core::Notification>,
    notification_store: &Option<Arc<dyn cog_core::NotificationStore>>,
    websocket_client: &Option<Arc<dyn cog_core::WebSocketClient>>,
    evolution_admin: &Option<Arc<dyn cog_core::EvolutionAdmin>>,
    evolution_stream: &Option<Arc<tokio::sync::broadcast::Sender<cog_core::EvolutionChangeInfo>>>,
    audit_stream: &Option<Arc<dyn cog_core::AuditStream>>,
    taste_intent_sink: &Option<Arc<dyn cog_core::TasteIntentSink>>,
    llm_usage_reader: &Option<Arc<dyn cog_core::LlmUsageReader>>,
    governance_store: &Option<Arc<dyn cog_core::GovernanceStore>>,
    principle_ledger: &Option<Arc<dyn cog_core::PrincipleLedgerStore>>,
    change_ledger: &Option<Arc<dyn cog_core::ChangeLedgerStore>>,
    chat_turns: &crate::ChatTurnHandle,
    agent_messages: &crate::AgentMessageHandle,
    object_backend: &Option<Arc<dyn cog_core::ObjectBackend>>,
    datasets: &Option<Arc<cog_storage::DatasetStore>>,
    shutdown: &cog_core::ShutdownSignal,
) -> Result<Arc<crate::GatewayState>, Box<dyn std::error::Error>> {
    // Cache of the supervisor's event channel, filled by the drain task below.
    // The supervisor's contract exposes neither the last scale-out
    // recommendation nor its cycle, so the broadcast it already produces is
    // the only source for both.
    let supervisor_event_cache =
        Arc::new(crate::supervisor_control::SupervisorEventCache::default());
    let gateway_state = Arc::new(crate::GatewayState {
        config: std::sync::RwLock::new(config.gateway.clone()),
        data_dir: config.app.data_dir.clone(),
        request_timeout_secs: std::sync::atomic::AtomicU64::new(
            config.gateway.request_timeout_secs,
        ),
        sandbox_task_timeout_secs: std::sync::atomic::AtomicU64::new(
            config.gateway.sandbox_task_timeout_secs,
        ),
        event_tx: event_tx.clone(),
        task_event_tx: task_event_tx.clone(),
        jwt_manager: jwt_manager.clone(),
        quota_manager: quota_manager.clone(),
        hierarchy_manager: hierarchy_manager.clone(),
        action_plan_store: Default::default(),
        collaboration_graph: Some(Arc::new(crate::collaboration::CollaborationGraph::new())),
        raw_logger: raw_logger.clone(),
        memory_backend: memory_backend.clone(),
        memory_ingestor: memory_ingestor.clone(),
        metrics_backend: metrics_backend.clone(),
        metrics_exporter: metrics_exporter.clone(),
        search_backend: search_backend.clone(),
        raw_log_index_store: raw_log_index_store.clone(),
        hook_engine: hook_engine.clone(),
        hook_archive: hook_archive.clone(),
        orchestrator: shared_orchestrator.clone(),
        task_executors,
        agent_registry: Some(agent_registry.clone()),
        observability_gateway: observability_gateway.clone(),
        connection_manager: Some(Arc::new(
            crate::websocket_protocol::ConnectionManager::with_event_cache_capacity(
                config.system.websocket_event_cache_capacity,
            ),
        )),
        wiki_adapter: wiki_adapter.clone(),
        knowledge_backend: knowledge_backend.clone(),
        user_store: user_store.clone(),
        platform_identities: platform_identities.clone(),
        contribution_control: contribution_control.clone(),
        login_rate_limiter: login_rate_limiter.clone(),
        supervisor: Some(supervisor.clone()),
        supervisor_event_cache: supervisor_event_cache.clone(),
        alert_store: Some(alert_store.clone()),
        active_alert_source: active_alert_source.clone(),
        backend_health_probe: backend_health_probe.clone(),
        heartbeat_history: Some(supervisor_registry.clone()),
        snapshot_store: Some(snapshot_store.clone()),
        trace_store: Some(trace_store.clone()),
        replay_engine: Some(replay_engine.clone()),
        session_manager: Some(session_manager.clone()),
        sandbox_backend: Some(sandbox_backend.clone()),
        plugin_registry: Some(plugin_registry.clone()),
        guardrail: Some(guardrail.clone()),
        eval_service: eval_service.clone(),
        observables,
        metrics_dimensions: config.metrics.scrape_dimensions.clone(),
        mcp_client: mcp_client.clone(),
        object_backend: object_backend.clone(),
        notification_store: notification_store.clone(),
        notification_dispatcher: notification_dispatcher.clone(),
        notification_tx: notification_tx.clone(),
        workspace_store: None,
        governance_store: governance_store.clone(),
        principle_ledger: principle_ledger.clone(),
        change_ledger: change_ledger.clone(),
        external_skill_registry: external_skill_registry.clone(),
        agent_pool: agent_pool.clone(),
        message_backend: message_backend.clone(),
        event_publisher: event_publisher.clone(),
        media_backend: media_backend.clone(),
        websocket_client: websocket_client.clone(),
        evolution_admin: evolution_admin.clone(),
        evolution_stream: evolution_stream.clone(),
        audit_stream: audit_stream.clone(),
        taste_intent_sink: taste_intent_sink.clone(),
        llm_usage_reader: llm_usage_reader.clone(),
        llm_client: Arc::new(std::sync::RwLock::new(None)),
        chat_turns: chat_turns.clone(),
        agent_messages: agent_messages.clone(),
        datasets: datasets.clone(),
    });

    // Drain the supervisor's event channel into the cache. A lagged reader
    // keeps going (the channel dropped the oldest events under back-pressure —
    // the cache is "the last event seen", not a count, so a gap is a gap, not
    // a lost fact to recover); a closed channel ends the loop so the cache
    // stops being fed stale values.
    //
    // 这条环以前是个裸 `tokio::spawn`：它退出之后读数面上什么都没有——缓存从此
    // 不再更新，而看起来与「supervisor 一直没说话」同形。登记之后它的退出落进
    // `cogneva_loop_deaths_total`，停与不动才分得开。
    {
        let cache = supervisor_event_cache.clone();
        let lag_metrics = metrics_backend.clone();
        let stop = shutdown.clone();
        // 订阅一次、跨重启共用：body 每次尝试都重建，重新订阅会落在通道当前的
        // 尾巴上，环死期间的事件就再也补不回来。
        let rx = Arc::new(tokio::sync::Mutex::new(supervisor.subscribe()));
        drop(cog_core::loop_health::spawn(
            SUPERVISOR_EVENT_CACHE_LOOP,
            cog_core::loop_health::Cadence::EventDriven,
            stop.clone(),
            move |beat| {
                let cache = cache.clone();
                let lag_metrics = lag_metrics.clone();
                let rx = Arc::clone(&rx);
                let stop = stop.clone();
                async move {
                    loop {
                        // 每一轮都盖一次章，无论队列里有没有东西：事件驱动的环大部分
                        // 时间停在空队列上，只在有事件时才盖，报出来的是队列的安静。
                        beat.beat();
                        let received = {
                            let mut rx = rx.lock().await;
                            tokio::select! {
                                biased;
                                _ = stop.wait() => return,
                                received = rx.recv() => received,
                            }
                        };
                        match received {
                            Ok(event) => cache.observe(&event),
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(
                                    skipped,
                                    "supervisor event cache lagged; the last recommendation may be stale"
                                );
                                crate::record_consumer_lag(
                                    &lag_metrics,
                                    crate::LAG_CONSUMER_SUPERVISOR_EVENT_CACHE,
                                    skipped,
                                )
                                .await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                        }
                    }
                }
            },
        ));
    }

    Ok(gateway_state)
}
