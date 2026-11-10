use std::sync::Arc;
use tokio::sync::broadcast;

/// HTTP / 传输层状态
#[derive(Clone)]
pub struct HttpState {
    pub config: cog_core::GatewayConfig,
    pub event_tx: broadcast::Sender<cog_core::AgentEvent>,
    pub task_event_tx: broadcast::Sender<cog_core::TaskEvent>,
}

/// 认证与会话状态
#[derive(Clone)]
pub struct AuthState {
    pub jwt_manager: Arc<dyn cog_core::AuthProvider>,
    pub user_store: Option<Arc<dyn cog_core::UserStore>>,
    pub platform_identities: Option<Arc<dyn cog_core::PlatformIdentityStore>>,
    pub login_rate_limiter: Option<Arc<crate::auth::LoginRateLimiter>>,
    pub session_manager: Option<Arc<dyn cog_core::SessionManager>>,
}

/// 运行时编排状态
#[derive(Clone)]
pub struct RuntimeState {
    pub orchestrator: Arc<dyn cog_core::OrchestratorControl>,
    pub task_executors: Arc<dyn cog_core::TaskExecutor>,
    pub agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    pub hook_engine: Option<Arc<dyn cog_core::HookEngine>>,
    pub action_plan_store: crate::action_plan::ActionPlanStore,
    pub collaboration_graph: Option<Arc<crate::collaboration::CollaborationGraph>>,
    pub wiki_adapter: Option<Arc<dyn cog_core::WikiBackend>>,
    /// 把投进 wiki 的来源读成页面。缺席时文档照存，只是没有页面替它把内容讲清楚。
    pub wiki_distiller: Option<Arc<dyn cog_core::WikiDistiller>>,
    pub connection_manager: Option<Arc<crate::websocket_protocol::ConnectionManager>>,
}

/// 基础设施与可观测性状态
#[derive(Clone)]
pub struct InfraState {
    pub raw_logger: Arc<dyn cog_core::RawLogger>,
    pub memory_backend: Option<Arc<dyn cog_core::MemoryBackend>>,
    pub metrics_backend: Option<Arc<dyn cog_core::MetricsBackend>>,
    pub metrics_exporter: Option<Arc<dyn cog_core::MetricsExporter>>,
    pub search_backend: Option<Arc<dyn cog_core::SearchBackend>>,
    pub raw_log_index_store: Option<Arc<dyn cog_core::RawLogIndexStore>>,
    pub hook_archive: Option<Arc<dyn cog_core::HookArchive>>,
    pub observability_gateway: Option<Arc<dyn cog_core::ObservabilityGateway>>,
}

/// 运维与配额状态
#[derive(Clone)]
pub struct OpsState {
    pub quota_manager: Arc<dyn cog_core::QuotaManager>,
    /// 逐笔 token 账（`gateway_llm_usage`）的读口，给需要按项目判花销的运维读面用。
    ///
    /// 与上面的 `quota_manager` 并排，因为两者答的是同一个问题的两半：额度拦的是
    /// 下一次调用，账判的是已经花掉的。它是 `None` 时「这个项目有没有超预算」判不了，
    /// 那一格报的是「装配没接上」，不是零花销——把缺账读成零，等于让超支的项目与
    /// 在预算内的项目同形。装配处（`GatewayState::ops_state`）照原样把它灌进来，
    /// 只有真的没有这个面时才留空。
    pub llm_usage: Option<Arc<dyn cog_core::LlmUsageLedger>>,
    pub hierarchy_manager: Option<Arc<dyn cog_core::HierarchyManager>>,
    pub supervisor: Option<Arc<dyn cog_core::Supervisor>>,
    pub agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    pub alert_store: Option<Arc<dyn cog_core::AlertStore>>,
    pub backend_health_probe: Option<Arc<crate::backend_health::BackendHealthProbe>>,
    pub heartbeat_history: Option<Arc<dyn cog_core::HeartbeatRegistry>>,
    pub snapshot_store: Option<Arc<dyn cog_core::CheckpointStore>>,
    pub guardrail: Option<Arc<dyn cog_core::Guardrail>>,
    pub eval_service: Option<Arc<dyn cog_core::EvalService>>,
}
