use axum::{
    extract::{Path, Query, Request, State, WebSocketUpgrade},
    http::{header, Method, StatusCode},
    middleware::{from_fn, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use base64::Engine as _;
use cog_core::TraceContext;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub mod action_plan;
pub mod agents;
pub mod alert_store;
pub mod approval_gate;
pub mod audit;
pub mod auth;
pub mod backend_health;
pub mod chat;
pub mod collaboration;
pub mod dashboard;
pub mod error;
pub mod evolution;
pub mod executor;
pub mod files;
pub mod heartbeat_history;
pub mod hook_forwarder;
pub mod hooks;
pub mod llm_admin;
pub mod memory;
pub mod notifications;
pub mod observability;
pub mod plugin;
pub mod prometheus_render;
pub mod quota_middleware;
pub mod raw_logs;
pub mod security_gateway;
pub mod sessions;
pub mod state;
pub mod state_builder;
pub mod supervisor_control;
pub mod supervisor_status;
pub mod takeover;
pub mod tasks;
pub mod websocket;
pub mod websocket_protocol;
pub mod websocket_streams;
pub mod wiki;
pub mod workspaces;

pub use approval_gate::{
    AgentAction, ApprovalRequest, ApprovalRule, ApprovalStatus, HumanApprovalGate,
};
pub use state::{AuthState, HttpState, InfraState, OpsState, RuntimeState};

/// Shared slot for the LLM client used by the WebSocket chat handler.
pub type SharedLlmClient = Arc<std::sync::RwLock<Option<Arc<dyn cog_core::LlmClient>>>>;
/// Per-session chat histories: session_id → (messages oldest-first, last-touched).
pub type ChatSessionStore = Arc<
    tokio::sync::Mutex<
        std::collections::HashMap<String, (Vec<cog_core::Message>, chrono::DateTime<chrono::Utc>)>,
    >,
>;

/// Gateway 应用状态，包含所有服务组件。
pub struct GatewayState {
    pub config: std::sync::RwLock<cog_core::GatewayConfig>,
    /// Base data directory for file-backed operations (datasets, reports, checkpoints).
    pub data_dir: String,
    /// Hot-path timeouts stored as atomics to avoid RwLock on every request.
    pub request_timeout_secs: std::sync::atomic::AtomicU64,
    pub sandbox_task_timeout_secs: std::sync::atomic::AtomicU64,
    pub event_tx: broadcast::Sender<cog_core::AgentEvent>,
    pub task_event_tx: broadcast::Sender<cog_core::TaskEvent>,
    pub jwt_manager: Arc<dyn cog_core::AuthProvider>,
    pub quota_manager: Arc<dyn cog_core::QuotaManager>,
    /// 5-level quota hierarchy (user → workspace → team → org → global). When
    /// `None`, the gateway falls back to the legacy single-level
    /// [`cog_core::QuotaManager`] surface only.
    pub hierarchy_manager: Option<Arc<dyn cog_core::HierarchyManager>>,
    pub raw_logger: Arc<dyn cog_core::RawLogger>,
    pub memory_backend: Option<Arc<dyn cog_core::MemoryBackend>>,
    pub memory_ingestor: Option<Arc<dyn cog_core::MemoryIngestor>>,
    pub metrics_backend: Option<Arc<dyn cog_core::MetricsBackend>>,
    pub metrics_exporter: Option<Arc<dyn cog_core::MetricsExporter>>,
    pub search_backend: Option<Arc<dyn cog_core::SearchBackend>>,
    pub raw_log_index_store: Option<Arc<dyn cog_core::RawLogIndexStore>>,
    pub orchestrator: Arc<dyn cog_core::OrchestratorControl>,
    pub task_executors: Arc<dyn cog_core::TaskExecutor>,
    pub agent_registry: Option<Arc<dyn cog_core::AgentRegistry>>,
    pub observability_gateway: Option<Arc<dyn cog_core::ObservabilityGateway>>,
    pub hook_archive: Option<Arc<dyn cog_core::HookArchive>>,
    pub hook_engine: Option<Arc<dyn cog_core::HookEngine>>,
    pub connection_manager: Option<Arc<websocket_protocol::ConnectionManager>>,
    pub wiki_adapter: Option<Arc<dyn cog_core::WikiBackend>>,
    pub user_store: Option<Arc<dyn cog_core::UserStore>>,
    pub login_rate_limiter: Option<Arc<auth::LoginRateLimiter>>,
    pub session_manager: Option<Arc<dyn cog_core::SessionManager>>,
    /// In-memory store for [`cog_core::ActionPlan`] resources exposed via the
    /// `/api/v1/action-plan` REST endpoints. Per-process; replace with a
    /// Redis-backed implementation when persistence across restarts is needed.
    pub action_plan_store: action_plan::ActionPlanStore,
    pub collaboration_graph: Option<Arc<collaboration::CollaborationGraph>>,
    pub supervisor: Option<Arc<dyn cog_core::Supervisor>>,
    pub alert_store: Option<Arc<dyn cog_core::AlertStore>>,
    pub backend_health_probe: Option<Arc<backend_health::BackendHealthProbe>>,
    pub snapshot_store: Option<Arc<dyn cog_core::CheckpointStore>>,
    pub heartbeat_history: Option<Arc<dyn cog_core::HeartbeatRegistry>>,
    pub trace_store: Option<Arc<dyn cog_core::TraceStore>>,
    pub replay_engine: Option<Arc<dyn cog_core::ReplayEngine>>,
    /// Sandbox backend for WASM execution.
    pub sandbox_backend: Option<Arc<dyn cog_core::SandboxBackend>>,
    /// Plugin registry for third-party plugin discovery / loading.
    pub plugin_registry: Option<Arc<dyn cog_core::PluginRegistry>>,
    /// Guardrail — automated safety layer (content filter + prompt injection + PII + tool guard).
    pub guardrail: Option<Arc<dyn cog_core::Guardrail>>,
    /// Eval service — self-test suite execution for bootstrap validation and evolution verification.
    pub eval_service: Option<Arc<dyn cog_core::EvalService>>,
    /// Evolution admin service — manual control over the self-evolution pipeline.
    pub evolution_admin: Option<Arc<dyn cog_core::EvolutionAdmin>>,
    /// 补丁行变更广播（接管台 SSE `/api/v1/evolution/stream` 订阅源）。
    pub evolution_stream: Option<Arc<tokio::sync::broadcast::Sender<cog_core::EvolutionPatchInfo>>>,
    /// 不可篡改审计流（审计 3.5/3.6）：配额执法等安全事件写入哈希链。
    pub audit_stream: Option<Arc<dyn cog_core::AuditStream>>,
    /// Observables — 各业务 crate 暴露的系统级指标（D5/D8/D9）。
    pub observables: Vec<Arc<dyn cog_core::Observable>>,
    /// MCP client — discover and call external MCP tools.
    pub mcp_client: Option<Arc<dyn cog_core::McpClient>>,
    /// Object storage backend for file uploads/downloads.
    pub object_backend: Option<Arc<dyn cog_core::ObjectBackend>>,
    /// Notification store (per-process in-memory by default; replace with persistent store when needed).
    pub notification_store: Option<Arc<dyn cog_core::NotificationStore>>,
    /// Notification dispatcher — pushes to WebSocket + optional webhook after persistence.
    pub notification_dispatcher: Option<Arc<dyn cog_core::NotificationDispatcher>>,
    /// Broadcast sender cloned into every WebSocket handler so that notifications
    /// can be delivered to connected clients.
    pub notification_tx: broadcast::Sender<cog_core::Notification>,
    /// In-memory workspace store (per-process; replace with persistent store when needed).
    pub workspace_store:
        Option<Arc<std::sync::RwLock<std::collections::HashMap<String, workspaces::Workspace>>>>,
    /// External skill registry (Markdown-based skills).
    pub external_skill_registry: Option<Arc<dyn cog_core::ExternalSkillRegistry>>,
    /// Global agent pool for production multi-agent parallelism.
    pub agent_pool: Option<Arc<dyn cog_core::AgentManager>>,
    /// Semantic event publisher — Gateway consumes this instead of the raw
    /// `MessageBackend` so that it does not depend on transport-level details.
    /// Backed by `cog-stream::MqEventPublisher` when MQ is configured.
    pub event_publisher: Option<Arc<dyn cog_core::EventPublisher>>,
    /// Media backend (LiveKit) for WebRTC rooms and recording.
    pub media_backend: Option<Arc<dyn cog_core::MediaBackend>>,
    /// WebSocket client for outbound connections (A2A, external services).
    pub websocket_client: Option<Arc<dyn cog_core::WebSocketClient>>,
    /// LLM client backing the WebSocket `chat_message` handler. Injected
    /// after all plugins start (the llm plugin is not ordered before
    /// gateway); `None` until then — chat requests get an explicit error
    /// reply instead of being dropped silently.
    pub llm_client: SharedLlmClient,
    /// Per-session chat history for the WebSocket chat handler
    /// (session_id → messages, oldest first, capped).
    pub chat_sessions: ChatSessionStore,
}

impl GatewayState {
    /// Subscribe to task lifecycle events from the orchestrator.
    pub fn subscribe_task_events(&self) -> broadcast::Receiver<cog_core::TaskEvent> {
        self.task_event_tx.subscribe()
    }

    /// 按职责拆分后的 HTTP 状态访问器
    pub fn http_state(&self) -> state::HttpState {
        state::HttpState {
            config: self
                .config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .clone(),
            event_tx: self.event_tx.clone(),
            task_event_tx: self.task_event_tx.clone(),
        }
    }

    /// 按职责拆分后的认证状态访问器
    pub fn auth_state(&self) -> state::AuthState {
        state::AuthState {
            jwt_manager: self.jwt_manager.clone(),
            user_store: self.user_store.clone(),
            login_rate_limiter: self.login_rate_limiter.clone(),
            session_manager: self.session_manager.clone(),
        }
    }

    /// 按职责拆分后的运行时状态访问器
    pub fn runtime_state(&self) -> state::RuntimeState {
        state::RuntimeState {
            orchestrator: self.orchestrator.clone(),
            task_executors: self.task_executors.clone(),
            agent_registry: self.agent_registry.clone(),
            hook_engine: self.hook_engine.clone(),
            action_plan_store: self.action_plan_store.clone(),
            collaboration_graph: self.collaboration_graph.clone(),
            wiki_adapter: self.wiki_adapter.clone(),
            connection_manager: self.connection_manager.clone(),
        }
    }

    /// 按职责拆分后的基础设施状态访问器
    pub fn infra_state(&self) -> state::InfraState {
        state::InfraState {
            raw_logger: self.raw_logger.clone(),
            memory_backend: self.memory_backend.clone(),
            metrics_backend: self.metrics_backend.clone(),
            metrics_exporter: self.metrics_exporter.clone(),
            search_backend: self.search_backend.clone(),
            raw_log_index_store: self.raw_log_index_store.clone(),
            hook_archive: self.hook_archive.clone(),
            observability_gateway: self.observability_gateway.clone(),
        }
    }

    /// 按职责拆分后的运维状态访问器
    pub fn ops_state(&self) -> state::OpsState {
        state::OpsState {
            quota_manager: self.quota_manager.clone(),
            hierarchy_manager: self.hierarchy_manager.clone(),
            supervisor: self.supervisor.clone(),
            agent_registry: self.agent_registry.clone(),
            alert_store: self.alert_store.clone(),
            backend_health_probe: self.backend_health_probe.clone(),
            heartbeat_history: self.heartbeat_history.clone(),
            snapshot_store: self.snapshot_store.clone(),
            guardrail: self.guardrail.clone(),
            eval_service: self.eval_service.clone(),
        }
    }

    /// Persist a notification and then dispatch it to all configured push channels.
    pub async fn create_notification(
        &self,
        notification: cog_core::Notification,
    ) -> cog_core::SFResult<()> {
        if let Some(ref store) = self.notification_store {
            store.create(notification.clone()).await?;
        }
        if let Some(ref dispatcher) = self.notification_dispatcher {
            dispatcher.dispatch(&notification).await?;
        }
        Ok(())
    }
}

/// 创建 HTTP + WebSocket 路由。
pub fn create_router(state: Arc<GatewayState>) -> Router {
    let jwt = state.jwt_manager.clone();

    // 公开路由（无需认证）
    let public = Router::new()
        .route("/health", get(health_check))
        .route("/health/live", get(liveness_check))
        .route("/health/ready", get(readiness_check))
        .route("/api/v1/auth/login", post(auth::login_handler))
        .route("/api/v1/auth/register", post(auth::register_handler))
        .route("/api/v1/auth/refresh", post(auth::refresh_handler))
        .route("/metrics", get(prometheus_metrics_handler))
        .route("/ws", get(ws_handler))
        .route("/api/v1/evolution/stream", get(evolution::stream_handler))
        .route("/dashboard", get(dashboard::dashboard_handler))
        .route("/takeover", get(takeover::takeover_handler))
        // WebUI 单页应用：根路径直接给 index.html，/assets 给构建产物，
        // 浏览器打开即用（一键拉起场景没有独立前端服务）
        .route("/", get(dashboard::dashboard_handler))
        .nest_service(
            "/assets",
            tower_http::services::ServeDir::new(dashboard::web_dir().join("assets")),
        );

    // Helper closures for common middleware stacks
    let auth_layer = {
        let jwt = jwt.clone();
        from_fn(move |req: Request, next: Next| {
            let jwt = jwt.clone();
            async move { auth::middleware::auth_middleware(jwt, req, next).await }
        })
    };

    let session_layer = from_fn({
        let state = state.clone();
        move |req: Request, next: Next| {
            let sm = state.session_manager.clone();
            async move {
                if let Some(ref session_mgr) = sm {
                    if let Some(claims) = req.extensions().get::<cog_core::Claims>().cloned() {
                        if let Ok(user_id) = claims.user_id() {
                            if let Some(token) = req
                                .headers()
                                .get("x-session-token")
                                .and_then(|h| h.to_str().ok())
                            {
                                if let Ok(session_id) = uuid::Uuid::parse_str(token) {
                                    match session_mgr.get(user_id, session_id).await {
                                        Ok(Some(_)) => {
                                            let _ = session_mgr.refresh(user_id, session_id).await;
                                        }
                                        Ok(None) => {
                                            return (
                                                StatusCode::UNAUTHORIZED,
                                                Json(json!({
                                                    "error": "session_not_found",
                                                    "message": "Session expired or invalid"
                                                })),
                                            )
                                                .into_response();
                                        }
                                        Err(e) => {
                                            tracing::warn!("Session validation error: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                next.run(req).await
            }
        }
    });

    let quota_layer = from_fn({
        let state = state.clone();
        move |req: Request, next: Next| {
            let manager = state.quota_manager.clone();
            async move { crate::quota_middleware::quota_middleware(manager, req, next).await }
        }
    });

    // Admin-level protected routes (orchestrator / agents / quota-admin / hooks write)
    let admin_routes = Router::new()
        .route("/api/v1/tasks", post(tasks::create_task_handler))
        .route(
            "/api/v1/tasks/{id}/schedule",
            post(tasks::schedule_task_handler),
        )
        .route("/api/v1/tasks/{id}/start", post(tasks::start_task_handler))
        .route(
            "/api/v1/tasks/{id}/complete",
            post(tasks::complete_task_handler),
        )
        .route("/api/v1/tasks/{id}/fail", post(tasks::fail_task_handler))
        .route(
            "/api/v1/tasks/{id}/cancel",
            post(tasks::cancel_task_handler),
        )
        .route(
            "/api/v1/tasks/{id}",
            axum::routing::delete(tasks::delete_task_handler),
        )
        .route("/api/v1/tasks/{id}/retry", post(tasks::retry_task_handler))
        .route(
            "/api/v1/tasks/check-timeouts",
            post(tasks::check_timeouts_handler),
        )
        .route("/api/v1/agents/register", post(agents::register_handler))
        .route(
            "/api/v1/agents/{id}",
            axum::routing::delete(agents::deregister_handler),
        )
        .route(
            "/api/v1/agents/{id}/heartbeat",
            post(agents::heartbeat_handler),
        )
        .route("/api/v1/agents/{id}/kill", post(agents::kill_handler))
        .route("/api/v1/agents/{id}/restart", post(agents::restart_handler))
        .route(
            "/api/v1/agents/{id}/checkpoint",
            post(agents::checkpoint_handler),
        )
        .route("/api/v1/quota/recharge", post(quota_recharge_handler))
        .route("/api/v1/hooks", post(hooks::create_hook_handler))
        .route(
            "/api/v1/hooks/{id}",
            axum::routing::delete(hooks::delete_hook_handler),
        )
        .route("/api/v1/memory/ingest", post(memory::ingest_handler))
        .route(
            "/api/v1/memory/ingest/batch",
            post(memory::batch_ingest_handler),
        )
        .route(
            "/api/v1/memory/raw/{id}",
            axum::routing::delete(memory::delete_raw_handler),
        )
        .route(
            "/api/v1/memory/schema/{id}",
            axum::routing::delete(memory::delete_schema_handler),
        )
        .route(
            "/api/v1/memory/summary/{id}",
            axum::routing::delete(memory::delete_summary_handler),
        )
        .route(
            "/api/v1/action-plan",
            post(action_plan::create_action_plan_handler),
        )
        .route(
            "/api/v1/action-plan/{id}",
            axum::routing::delete(action_plan::delete_action_plan_handler),
        )
        .route("/api/v1/plugins", get(plugins_list_handler))
        .route("/api/v1/plugins", post(plugin_upload_handler))
        .route(
            "/api/v1/plugins/{id}",
            axum::routing::delete(plugin_unload_handler),
        )
        .route("/api/v1/sandbox/execute", post(sandbox_execute_handler))
        .route("/api/v1/guard/check", post(guard_check_handler))
        .route("/api/v1/eval/run", post(eval_run_handler))
        .route("/api/v1/eval/compare", post(eval_compare_handler))
        .route("/api/v1/eval/datasets", get(eval_list_datasets_handler))
        .route(
            "/api/v1/eval/report/{report_id}",
            get(eval_get_report_handler),
        )
        .route(
            "/api/v1/evolution/patches",
            get(evolution::list_patches_handler),
        )
        .route(
            "/api/v1/evolution/patches/{id}/apply",
            post(evolution::apply_patch_handler),
        )
        .route(
            "/api/v1/evolution/patches/{id}/deploy",
            post(evolution::deploy_patch_handler),
        )
        .route(
            "/api/v1/evolution/patches/{id}/approve",
            post(evolution::approve_patch_handler),
        )
        .route(
            "/api/v1/evolution/policies/evaluate",
            post(evolution::evaluate_policy_handler),
        )
        .route("/api/v1/evolution/metrics", get(evolution::metrics_handler))
        .route(
            "/api/v1/evolution/rollback",
            post(evolution::rollback_handler),
        )
        .route("/api/v1/evolution/events", get(evolution::events_handler))
        .route(
            "/api/v1/admin/llm-status",
            get(llm_admin::llm_status_handler),
        )
        .route(
            "/api/v1/admin/llm-config",
            post(llm_admin::llm_config_handler),
        )
        .layer(quota_layer.clone())
        .layer(auth::middleware::require_role(
            cog_core::RoleRequirement::Admin,
        ))
        .layer(session_layer.clone())
        .layer(auth_layer.clone());

    // Operator-level protected routes (orchestrator read / agents read / hooks read / memory read / raw_logs)
    let operator_routes = Router::new()
        .route("/api/v1/tasks/{id}", get(tasks::get_task_handler))
        .route("/api/v1/tasks/list", get(tasks::list_tasks_handler))
        .route("/api/v1/tasks/ready", get(tasks::get_ready_tasks_handler))
        .route("/api/v1/tasks/status", get(tasks::task_summary_handler))
        .route("/api/v1/tasks/graph", get(tasks::get_task_graph_handler))
        .route(
            "/api/v1/tasks/{id}/dependents",
            get(tasks::get_task_dependents_handler),
        )
        .route(
            "/api/v1/tasks/{id}/blocking",
            get(tasks::get_task_dependencies_handler),
        )
        .route("/api/v1/agents", get(agents::list_handler))
        .route("/api/v1/agents/{id}", get(agents::get_handler))
        .route("/api/v1/hooks", get(hooks::list_hooks_handler))
        .route(
            "/api/v1/memory/search",
            post(memory::unified_search_handler),
        )
        .route("/api/v1/memory/schema", get(memory::schema_search_handler))
        .route(
            "/api/v1/memory/schema/list",
            get(memory::list_schema_handler),
        )
        .route(
            "/api/v1/memory/summary/list",
            get(memory::list_summary_handler),
        )
        .route(
            "/api/v1/memory/summary/search",
            post(memory::summary_search_handler),
        )
        .route("/api/v1/memory/raw", get(memory::list_raw_handler))
        .route("/api/v1/memory/raw/{id}", get(memory::get_raw_handler))
        .route("/api/v1/memory/stats", get(memory::stats_handler))
        .route("/api/v1/memory/metrics", get(memory::metrics_handler))
        .route("/api/v1/raw_logs", get(raw_logs::list_raw_logs_handler))
        .route("/api/v1/quota", get(quota_info_handler))
        .route("/api/v1/quota/remaining", get(quota_remaining_handler))
        .route("/api/v1/quota/usage", get(quota_usage_handler))
        .route("/api/v1/quota/workspace/{id}", get(quota_workspace_handler))
        .route(
            "/api/v1/action-plan",
            get(action_plan::list_action_plans_handler),
        )
        .route(
            "/api/v1/action-plan/{id}",
            get(action_plan::get_action_plan_handler),
        )
        .route("/api/v1/protocol/mcp/tools", get(mcp_tools_list_handler))
        .route(
            "/api/v1/protocol/a2a/agent-card",
            get(a2a_agent_card_handler),
        )
        .route(
            "/api/v1/cluster/overview",
            get(observability::cluster_overview_handler),
        )
        .route(
            "/api/v1/squads/{id}",
            get(observability::squad_state_handler),
        )
        .route(
            "/api/v1/agents/{id}/state",
            get(observability::agent_state_handler),
        )
        .route(
            "/api/v1/tasks/{id}/metrics",
            get(observability::task_metrics_handler),
        )
        .route(
            "/api/v1/tasks/{id}/logs",
            get(observability::task_logs_handler),
        )
        .route("/api/v1/events", get(observability::events_handler))
        .route(
            "/api/v1/events/live",
            get(observability::events_live_handler),
        )
        .route("/api/v1/search", get(observability::search_handler))
        .route(
            "/api/v1/agents/{id}/stream",
            get(websocket_streams::agent_stream_handler),
        )
        .route(
            "/api/v1/tasks/{id}/stream",
            get(websocket_streams::task_stream_handler),
        )
        .route(
            "/api/v1/cluster/stream",
            get(websocket_streams::cluster_stream_handler),
        )
        .route(
            "/api/v1/tasks/{id}/checkpoint",
            get(observability::task_checkpoint_handler),
        )
        .route(
            "/api/v1/snapshots/{id}",
            get(observability::snapshot_url_handler),
        )
        .route("/api/v1/snapshots", get(snapshots_list_handler))
        .route("/api/v1/traces", get(traces_list_handler))
        .route("/api/v1/traces/{id}", get(trace_get_handler))
        .route("/api/v1/traces/{id}/replay", post(trace_replay_handler))
        .route(
            "/api/v1/raw_logs/index",
            get(observability::raw_log_index_handler),
        )
        .route(
            "/api/v1/audit/events",
            get(audit::list_audit_events_handler),
        )
        .route(
            "/api/v1/audit/events/{id}",
            get(audit::replay_audit_event_handler),
        )
        .route(
            "/api/v1/collaboration/graph",
            get(collaboration::collaboration_graph_handler),
        )
        .route(
            "/api/v1/collaboration/links/{id}",
            get(collaboration::task_collaboration_handler),
        )
        .route("/api/v1/supervisor/status", get(supervisor_status_handler))
        .route("/api/v1/hooks/health", get(hooks_health_handler))
        .route(
            "/api/v1/supervisor/queue_stats",
            get(supervisor_queue_stats_handler),
        )
        .route("/api/v1/supervisor/gate", get(supervisor_gate_handler))
        .route(
            "/api/v1/supervisor/gate/pause",
            post(supervisor_gate_pause_handler),
        )
        .route(
            "/api/v1/supervisor/gate/resume",
            post(supervisor_gate_resume_handler),
        )
        .route(
            "/api/v1/supervisor/control_plane",
            get(supervisor_control_plane_handler),
        )
        .route("/api/v1/dlq/retry", post(dlq_retry_handler))
        .route("/api/v1/health/backends", get(backends_health_handler))
        .route("/api/v1/tasks/{id}/timeline", get(task_timeline_handler))
        .route(
            "/api/v1/agents/{id}/heartbeats",
            get(agent_heartbeats_handler),
        )
        .route("/api/v1/alerts/active", get(alerts_active_handler))
        .layer(quota_layer.clone())
        .layer(auth::middleware::require_role(
            cog_core::RoleRequirement::Operator,
        ))
        .layer(session_layer.clone())
        .layer(auth_layer.clone());

    // Viewer-level protected routes (wiki read/write + user-facing core APIs)
    let viewer_routes = Router::new()
        .route("/api/v1/wiki", get(wiki::list_handler))
        .route("/api/v1/wiki", post(wiki::create_handler))
        .route("/api/v1/wiki/info", get(wiki::info_handler))
        .route("/api/v1/wiki/document", get(wiki::get_document_handler))
        .route("/api/v1/wiki/search", post(wiki::search_handler))
        .route("/api/v1/sessions", get(sessions::list_sessions_handler))
        .route("/api/v1/sessions", post(sessions::create_session_handler))
        .route("/api/v1/sessions/{id}", get(sessions::get_session_handler))
        .route(
            "/api/v1/sessions/{id}",
            axum::routing::put(sessions::update_session_handler),
        )
        .route(
            "/api/v1/sessions/{id}",
            axum::routing::delete(sessions::delete_session_handler),
        )
        .route(
            "/api/v1/sessions/{id}/messages",
            get(sessions::list_messages_handler),
        )
        .route(
            "/api/v1/sessions/{id}/messages",
            post(sessions::send_message_handler),
        )
        .route(
            "/api/v1/sessions/{id}/messages/{message_id}/regenerate",
            post(sessions::regenerate_message_handler),
        )
        .route(
            "/api/v1/workspaces",
            get(workspaces::list_workspaces_handler),
        )
        .route(
            "/api/v1/workspaces/{id}",
            get(workspaces::get_workspace_handler),
        )
        .route(
            "/api/v1/workspaces/{id}/members",
            get(workspaces::list_workspace_members_handler),
        )
        .route("/api/v1/files/upload", post(files::upload_file_handler))
        .route("/api/v1/files/{id}", get(files::get_file_handler))
        .route(
            "/api/v1/files/{id}",
            axum::routing::delete(files::delete_file_handler),
        )
        .route(
            "/api/v1/notifications",
            get(notifications::list_notifications_handler),
        )
        .route(
            "/api/v1/notifications/{id}/read",
            post(notifications::mark_read_handler),
        )
        .route(
            "/api/v1/notifications/read-all",
            post(notifications::mark_all_read_handler),
        )
        .layer(quota_layer.clone())
        .layer(auth::middleware::require_role(
            cog_core::RoleRequirement::Viewer,
        ))
        .layer(session_layer.clone())
        .layer(auth_layer.clone());

    // General protected routes (auth self-service)
    let general_protected = Router::new()
        .route("/api/v1/auth/logout", post(auth::logout_handler))
        .route("/api/v1/auth/session", get(auth::validate_session_handler))
        .route("/api/v1/auth/me", get(auth::get_me_handler))
        .route(
            "/api/v1/auth/me",
            axum::routing::put(auth::update_me_handler),
        )
        .route("/api/v1/auth/device", post(auth::device_handler))
        .route("/api/v1/auth/biometric", post(auth::biometric_handler))
        .layer(quota_layer.clone())
        .layer(session_layer.clone())
        .layer(auth_layer.clone());

    let protected = admin_routes
        .merge(operator_routes)
        .merge(viewer_routes)
        .merge(general_protected);

    let cors_layer = build_cors_layer(
        &state
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .cors_origins,
    );

    public
        .merge(protected)
        .layer(from_fn({
            let state = state.clone();
            move |mut req: Request, next: Next| {
                let logger = state.raw_logger.clone();
                let metrics = state.metrics_backend.clone();
                let method = req.method().to_string();
                let uri = req.uri().path().to_string();
                let request_id = uuid::Uuid::new_v4().to_string();

                // Extract or generate distributed tracing context.
                let headers: HashMap<String, String> = req
                    .headers()
                    .iter()
                    .filter_map(|(k, v)| {
                        Some((k.as_str().to_lowercase(), v.to_str().ok()?.to_string()))
                    })
                    .collect();
                let trace_ctx = TraceContext::from_headers(&headers)
                    .unwrap_or_else(|| TraceContext::generate().with_parent(&request_id));
                req.extensions_mut().insert(request_id.clone());
                req.extensions_mut().insert(trace_ctx.clone());

                async move {
                    let start = std::time::Instant::now();
                    let mut response = next.run(req).await;
                    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;
                    let status = response.status().as_u16();

                    if let Ok(hv) = axum::http::HeaderValue::from_str(&request_id) {
                        response.headers_mut().insert(
                            axum::http::header::HeaderName::from_static("x-request-id"),
                            hv,
                        );
                    }
                    if let Ok(hv) = axum::http::HeaderValue::from_str(&trace_ctx.trace_id) {
                        response.headers_mut().insert(
                            axum::http::header::HeaderName::from_static("x-trace-id"),
                            hv,
                        );
                    }
                    if let Ok(hv) = axum::http::HeaderValue::from_str(&trace_ctx.span_id) {
                        response
                            .headers_mut()
                            .insert(axum::http::header::HeaderName::from_static("x-span-id"), hv);
                    }

                    let record = cog_core::RawRecord {
                        meta: cog_core::RawMeta {
                            version: "1.0".into(),
                            stream: "transport_raw".into(),
                            recorded_at: chrono::Utc::now(),
                            recorded_by: "cog-gateway".into(),
                            sequence: 0,
                            trace_id: trace_ctx.trace_id.clone(),
                            span_id: Some(trace_ctx.span_id.clone()),
                        },
                        context: cog_core::RawContext::default(),
                        payload: cog_core::RawPayload {
                            direction: "inbound".into(),
                            transport: "http".into(),
                            format: Some("json".into()),
                            raw: serde_json::json!({
                                "method": method,
                                "uri": uri,
                                "status": status,
                            }),
                        },
                    };

                    if let Err(e) = logger.write(record).await {
                        tracing::warn!("RawLogger write failed (http): {}", e);
                    }

                    if let Some(ref mb) = metrics {
                        let mut labels = HashMap::new();
                        labels.insert("method".into(), method.clone());
                        labels.insert("endpoint".into(), uri.clone());
                        labels.insert("status".into(), status.to_string());
                        if let Err(e) = mb
                            .record_counter("http_requests_total", 1.0, labels.clone())
                            .await
                        {
                            tracing::warn!("Failed to record http request counter: {}", e);
                        }
                        if let Err(e) = mb
                            .record_histogram("http_request_duration_ms", duration_ms, labels)
                            .await
                        {
                            tracing::warn!("Failed to record http request histogram: {}", e);
                        }
                    }

                    response
                }
            }
        }))
        .layer(from_fn(security_headers_middleware))
        .layer(cors_layer)
        .layer(RequestBodyLimitLayer::new(10 * 1024 * 1024))
        .layer(from_fn({
            let state = state.clone();
            move |req: Request, next: Next| {
                let state = state.clone();
                async move {
                    let timeout_secs = state
                        .request_timeout_secs
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let timeout = std::time::Duration::from_secs(timeout_secs);
                    match tokio::time::timeout(timeout, next.run(req)).await {
                        Ok(response) => response,
                        Err(_) => {
                            let mut response = Response::new(axum::body::Body::empty());
                            *response.status_mut() = StatusCode::REQUEST_TIMEOUT;
                            response
                        }
                    }
                }
            }
        }))
        .layer(CompressionLayer::new())
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn build_cors_layer(origins: &[String]) -> CorsLayer {
    if origins.iter().any(|o| o == "*") {
        CorsLayer::new()
            .allow_origin(tower_http::cors::Any)
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::COOKIE])
    } else {
        let allow_origins: Vec<axum::http::HeaderValue> =
            origins.iter().filter_map(|o| o.parse().ok()).collect();
        CorsLayer::new()
            .allow_origin(allow_origins)
            .allow_methods([Method::GET, Method::POST, Method::DELETE, Method::OPTIONS])
            .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE, header::COOKIE])
            .allow_credentials(true)
            .expose_headers([
                header::HeaderName::from_static("x-request-id"),
                header::HeaderName::from_static("x-trace-id"),
                header::HeaderName::from_static("x-span-id"),
                header::HeaderName::from_static("x-parent-span-id"),
            ])
    }
}

/// Prometheus metrics endpoint — serves memory operation counters,
/// latency histograms, and task operation counters from the MetricsBackend
/// in text format, plus prometheus registry metrics from MetricsExporter.
async fn prometheus_metrics_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let mut body = String::new();

    // Serve MetricsBackend time-series metrics if available
    if let Some(mb) = state.metrics_backend.as_ref() {
        let end = chrono::Utc::now();
        let start = end - chrono::Duration::seconds(300); // last 5 minutes

        match mb
            .query_counter_range("memory_operations_total", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_counters(
                    "memory_operations_total",
                    "Total number of memory backend operations",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query counters: {}", e);
            }
        }

        match mb
            .query_histogram_range("memory_operation_latency_ms", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_histograms(
                    "memory_operation_latency_ms",
                    "Memory backend operation latency in milliseconds",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query histograms: {}", e);
            }
        }

        match mb
            .query_counter_range("task_operations_total", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_counters(
                    "task_operations_total",
                    "Total number of task lifecycle operations",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query task counters: {}", e);
            }
        }

        match mb
            .query_counter_range("http_requests_total", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_counters(
                    "http_requests_total",
                    "Total number of HTTP requests",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query http request counters: {}", e);
            }
        }

        match mb
            .query_histogram_range("http_request_duration_ms", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_histograms(
                    "http_request_duration_ms",
                    "HTTP request duration in milliseconds",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query http request histograms: {}", e);
            }
        }

        match mb
            .query_counter_range("tier_migration_total", start, end)
            .await
        {
            Ok(samples) => {
                body.push_str(&prometheus_render::render_counters(
                    "tier_migration_total",
                    "Total number of tier migration operations",
                    &samples,
                ));
            }
            Err(e) => {
                tracing::warn!("Failed to query tier migration counters: {}", e);
            }
        }
    }

    // Append prometheus registry metrics from MetricsExporter if available
    if let Some(exporter) = state.metrics_exporter.as_ref() {
        match exporter.encode() {
            Ok(encoded) => {
                if let Ok(text) = String::from_utf8(encoded) {
                    if !text.trim().is_empty() {
                        if !body.is_empty() {
                            body.push('\n');
                        }
                        body.push_str(&text);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to encode prometheus metrics: {}", e);
            }
        }
    }

    if body.is_empty() {
        return (StatusCode::SERVICE_UNAVAILABLE, "metrics backend disabled").into_response();
    }

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
        .into_response()
}

async fn health_check() -> impl IntoResponse {
    "OK"
}

async fn liveness_check() -> impl IntoResponse {
    "OK"
}

async fn readiness_check(State(state): State<Arc<GatewayState>>) -> Response {
    // If memory backend is configured, verify it is accessible.
    if let Some(ref backend) = state.memory_backend {
        if let Err(e) = backend.health_check().await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status": "not_ready", "component": "memory_backend", "error": e.to_string()})),
            )
                .into_response();
        }
    }

    // If metrics backend is configured, verify it is accessible.
    if let Some(ref backend) = state.metrics_backend {
        if let Err(e) = backend.health_check().await {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"status": "not_ready", "component": "metrics_backend", "error": e.to_string()})),
            )
                .into_response();
        }
    }

    (StatusCode::OK, Json(json!({"status": "ready"}))).into_response()
}

/// Parse query string into a HashMap of key-value pairs.
fn parse_query_params(query: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            params.insert(k.to_string(), v.to_string());
        }
    }
    params
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<GatewayState>>,
    req: Request,
) -> Response {
    // WebSocket auth: prefer query-param token (mobile clients), fall back
    // to Authorization header.
    let query = req.uri().query().unwrap_or("");
    let query_params = parse_query_params(query);

    let token = query_params.get("token").cloned().or_else(|| {
        req.headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .and_then(|auth| {
                let parts: Vec<&str> = auth.splitn(2, ' ').collect();
                if parts.len() == 2 && parts[0].eq_ignore_ascii_case("bearer") {
                    Some(parts[1].to_string())
                } else {
                    None
                }
            })
    });

    let Some(token) = token else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing_token", "message": "JWT token required"})),
        )
            .into_response();
    };

    let claims = match state.jwt_manager.verify_token(&token).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("WebSocket JWT verification failed: {}", e);
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid_token", "message": "JWT token invalid or expired"})),
            )
                .into_response();
        }
    };

    let user_id = claims.sub;

    // Extract device params per spec section 1.3
    let device_id = query_params.get("device_id").cloned();
    let platform = query_params.get("platform").cloned();
    let app_version = query_params.get("app_version").cloned();

    // Extract or propagate distributed tracing context from headers.
    let headers: HashMap<String, String> = req
        .headers()
        .iter()
        .filter_map(|(k, v)| Some((k.as_str().to_lowercase(), v.to_str().ok()?.to_string())))
        .collect();
    let trace_ctx = TraceContext::from_headers(&headers).unwrap_or_else(TraceContext::generate);

    ws.on_upgrade(move |socket| {
        websocket::handle_socket(
            socket,
            state,
            user_id,
            device_id,
            platform,
            app_version,
            trace_ctx,
        )
    })
    .into_response()
}

/// 查询配额信息。
async fn quota_info_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let remaining = state.quota_manager.get_remaining("anonymous").await;
    (StatusCode::OK, Json(json!({ "remaining": remaining }))).into_response()
}

/// 查询个人剩余配额。
async fn quota_remaining_handler(
    State(state): State<Arc<GatewayState>>,
    request: Request,
) -> Response {
    let user_id = request
        .extensions()
        .get::<String>()
        .cloned()
        .unwrap_or_else(|| "anonymous".to_string());

    let remaining = state.quota_manager.get_remaining(&user_id).await;
    (
        StatusCode::OK,
        Json(json!({ "user_id": user_id, "remaining": remaining })),
    )
        .into_response()
}

/// 查询个人配额使用汇总。
/// 返回字段：
/// - `user_id`, `remaining`, `used_today`, `total_quota` — 单用户视角的传统字段。
/// - `hierarchy` — 仅在 `hierarchy_manager` 可用且查询参数提供任意一个父级
///   作用域 (`workspace_id` / `team_id` / `organization_id`) 时返回。包含 5
///   级配额评估结果（`allowed` / `warnings` / `blocked_by` / `scopes`）。
async fn quota_usage_handler(
    State(state): State<Arc<GatewayState>>,
    axum::extract::Query(params): axum::extract::Query<QuotaUsageQuery>,
    request: Request,
) -> Response {
    let user_id = request
        .extensions()
        .get::<String>()
        .cloned()
        .unwrap_or_else(|| "anonymous".to_string());

    let summary = state.quota_manager.get_user_summary(&user_id).await;
    let mut body = serde_json::json!({
        "user_id": user_id,
        "remaining": summary.remaining,
        "used_today": summary.used_today,
        "total_quota": summary.total_quota,
    });

    if let Some(hier) = state.hierarchy_manager.as_ref() {
        let ctx = cog_core::QuotaContext {
            user_id: Some(user_id.clone()),
            workspace_id: params.workspace_id.clone(),
            team_id: params.team_id.clone(),
            organization_id: params.organization_id.clone(),
            global_id: params.global_id.clone(),
        };
        let decision = hier.check(&ctx, 0).await;
        if let Some(obj) = body.as_object_mut() {
            obj.insert(
                "hierarchy".to_string(),
                serde_json::to_value(&decision).unwrap_or(serde_json::json!(null)),
            );
        }
    }

    (StatusCode::OK, Json(body)).into_response()
}

/// Query parameters for [`quota_usage_handler`]. Any subset may be supplied;
/// scopes whose id is `None` are skipped during the cascade walk.
#[derive(Debug, Default, serde::Deserialize)]
struct QuotaUsageQuery {
    #[serde(default)]
    workspace_id: Option<String>,
    #[serde(default)]
    team_id: Option<String>,
    #[serde(default)]
    organization_id: Option<String>,
    #[serde(default)]
    global_id: Option<String>,
}

/// 充值配额（用户或工作空间）。
#[derive(Debug, serde::Deserialize)]
struct RechargePayload {
    target_type: String, // "user" | "workspace"
    target_id: String,
    tokens: u64,
    #[serde(default)]
    valid_until: Option<chrono::DateTime<chrono::Utc>>,
}

async fn quota_recharge_handler(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<RechargePayload>,
) -> Response {
    let result = match payload.target_type.as_str() {
        "user" => {
            state
                .quota_manager
                .recharge(&payload.target_id, payload.tokens, payload.valid_until)
                .await
        }
        "workspace" => {
            state
                .quota_manager
                .recharge_workspace(&payload.target_id, payload.tokens, payload.valid_until)
                .await
        }
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "invalid_target_type",
                    "message": format!("target_type must be 'user' or 'workspace', got: {}", other)
                })),
            )
                .into_response();
        }
    };

    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "target_type": payload.target_type,
                "target_id": payload.target_id,
                "tokens_added": payload.tokens,
                "status": "success"
            })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!("Quota recharge failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "recharge_failed", "message": e.to_string()})),
            )
                .into_response()
        }
    }
}

/// 查询工作空间配额信息。
async fn quota_workspace_handler(
    State(state): State<Arc<GatewayState>>,
    axum::extract::Path(workspace_id): axum::extract::Path<String>,
) -> Response {
    let summary = state
        .quota_manager
        .get_workspace_summary(&workspace_id)
        .await;
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "workspace_id": workspace_id,
            "remaining": summary.remaining,
            "used_today": summary.used_today,
            "total_quota": summary.total_quota,
        })),
    )
        .into_response()
}

/// Add common security headers to all responses.
async fn security_headers_middleware(req: Request, next: Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        header::HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::HeaderName::from_static("x-frame-options"),
        header::HeaderValue::from_static("DENY"),
    );
    headers.insert(
        header::HeaderName::from_static("referrer-policy"),
        header::HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    response
}

// ─── Supervisor Status Handler ───

async fn supervisor_status_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let report = supervisor.run_health_pass().await.unwrap_or_default();
            let snapshot = supervisor_status::SupervisorStatusSnapshot {
                cycle: 0,
                healthy_agents: report.healthy.len(),
                dead_agents: report.dead.len(),
                suspect_agents: report.suspect.len(),
                stuck_agents: report.stuck.len(),
                pending_handoffs: 0,
                scheduler_paused: supervisor.gate().is_paused(),
                last_rebalance: None,
                timestamp: chrono::Utc::now(),
            };
            (StatusCode::OK, Json(snapshot)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

// ─── HookEngine Health Handler ───

async fn hooks_health_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let mut tiers = serde_json::Map::new();

    // Tier 1: Redis (always present if hook_engine is configured)
    tiers.insert(
        "redis".to_string(),
        json!({"status": "unknown", "configured": state.hook_engine.is_some()}),
    );

    // Tier 2 & 3: We can't introspect deeply without adding methods to HookEngine,
    // but we can report whether the hook_engine and hook_archive are present.
    tiers.insert(
        "archive".to_string(),
        json!({"status": if state.hook_archive.is_some() { "up" } else { "disabled" }, "configured": state.hook_archive.is_some()}),
    );

    let overall = if state.hook_engine.is_some() {
        "up"
    } else {
        "down"
    };

    (
        StatusCode::OK,
        Json(json!({
            "overall": overall,
            "tiers": tiers,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        })),
    )
        .into_response()
}

// ─── Supervisor Queue Stats Handler ───

async fn supervisor_queue_stats_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let pending_handoffs = supervisor.autonomous_pending_count().await;
            let retry_count = supervisor.autonomous_retry_count().await;
            let dlq_count = supervisor.orchestrator_dlq_len().await.unwrap_or(0);
            let stats = supervisor_control::QueueStats {
                pending_handoffs,
                retry_count,
                dlq_count,
                cycle: 0,
                timestamp: chrono::Utc::now(),
            };
            (StatusCode::OK, Json(stats)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

// ─── Supervisor Gate Handler ───

async fn supervisor_gate_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let status = supervisor_control::GateStatus {
                paused: supervisor.gate().is_paused(),
                timestamp: chrono::Utc::now(),
            };
            (StatusCode::OK, Json(status)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

async fn supervisor_gate_pause_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let was_paused = supervisor.gate().pause();
            (
                StatusCode::OK,
                Json(json!({"paused": true, "was_already_paused": was_paused})),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

async fn supervisor_gate_resume_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let was_paused = supervisor.gate().resume();
            (
                StatusCode::OK,
                Json(json!({"paused": false, "was_paused": was_paused})),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

// ─── Supervisor Control Plane Handler ───

async fn supervisor_control_plane_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.supervisor {
        Some(ref supervisor) => {
            let url = supervisor.control_plane_url();
            let status = supervisor_control::ControlPlaneStatus {
                enabled: url.is_some(),
                last_report: None,
                last_success: None,
                endpoint: url,
            };
            (StatusCode::OK, Json(status)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "supervisor not configured"})),
        )
            .into_response(),
    }
}

// ─── DLQ Retry Handler ───

#[derive(Debug, serde::Deserialize)]
struct DlqRetryPayload {
    task_id: String,
}

async fn dlq_retry_handler(
    State(state): State<Arc<GatewayState>>,
    Json(payload): Json<DlqRetryPayload>,
) -> Response {
    // First, attempt to replay from DLQ (remove it from DLQ if present).
    let replayed = match state.orchestrator.replay_dlq(&payload.task_id).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "dlq_replay_failed", "message": e.to_string()})),
            )
                .into_response();
        }
    };

    // Then retry the task in the orchestrator.
    match state.orchestrator.retry_task(&payload.task_id).await {
        Ok(()) => (
            StatusCode::OK,
            Json(json!({
                "task_id": payload.task_id,
                "replayed_from_dlq": replayed,
                "status": "retried"
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "retry_failed", "message": e.to_string()})),
        )
            .into_response(),
    }
}

// ─── Backends Health Handler ───

async fn backends_health_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.backend_health_probe {
        Some(ref probe) => {
            let matrix = probe.probe().await;
            (StatusCode::OK, Json(matrix)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "backend health probe not configured"})),
        )
            .into_response(),
    }
}

// ─── Task Timeline Handler ───

async fn task_timeline_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
) -> Response {
    let task = match state.orchestrator.get_task(&task_id).await {
        Some(t) => t,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "task_not_found", "task_id": task_id})),
            )
                .into_response();
        }
    };

    let mut milestones = Vec::new();

    milestones.push(json!({
        "event": "created",
        "timestamp": task.created_at.to_rfc3339(),
        "details": { "task_type": task.task_type }
    }));

    if let Some(started) = task.started_at {
        milestones.push(json!({
            "event": "started",
            "timestamp": started.to_rfc3339(),
            "details": {}
        }));
    }

    milestones.push(json!({
        "event": "status_changed",
        "timestamp": task.updated_at.to_rfc3339(),
        "details": { "status": format!("{:?}", task.status) }
    }));

    if let Some(ref error) = task.error {
        milestones.push(json!({
            "event": "failed",
            "timestamp": task.updated_at.to_rfc3339(),
            "details": { "error": error }
        }));
    }

    (
        StatusCode::OK,
        Json(json!({
            "task_id": task_id,
            "milestones": milestones,
        })),
    )
        .into_response()
}

// ─── Agent Heartbeats Handler ───

async fn agent_heartbeats_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
) -> Response {
    match state.heartbeat_history {
        Some(ref registry) => {
            let history: Vec<heartbeat_history::HeartbeatHistoryEntry> = registry
                .get_heartbeat_history(&agent_id)
                .into_iter()
                .map(|record| heartbeat_history::HeartbeatHistoryEntry {
                    agent_id: record.agent_id,
                    timestamp: record.timestamp.to_rfc3339(),
                    status: format!("{:?}", record.status).to_lowercase(),
                    load_score: record.load_score,
                    task_count: record.task_count,
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "agent_id": agent_id,
                    "count": history.len(),
                    "heartbeats": history,
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "heartbeat_history source not configured"})),
        )
            .into_response(),
    }
}

// ─── Alerts Active Handler ───

async fn alerts_active_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.alert_store {
        Some(ref store) => {
            let alerts: Vec<alert_store::AlertEntry> = store
                .list_active(100)
                .into_iter()
                .map(|alert| alert_store::AlertEntry {
                    id: alert.id,
                    severity: match alert.severity {
                        cog_core::AlertSeverity::Warning => "warning".to_string(),
                        cog_core::AlertSeverity::Critical => "critical".to_string(),
                        cog_core::AlertSeverity::Info => "info".to_string(),
                    },
                    event_type: alert.event_type,
                    message: alert.message,
                    agent_id: alert.agent_id,
                    task_id: alert.task_id,
                    crew_id: alert.crew_id,
                    timestamp: alert.timestamp.to_rfc3339(),
                    resolved: alert.resolved,
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "alerts": alerts,
                    "count": alerts.len(),
                })),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "alert store not configured"})),
        )
            .into_response(),
    }
}

// ─── Snapshots List Handler ───

#[derive(Debug, serde::Deserialize)]
struct SnapshotsListQuery {
    #[serde(default = "default_snapshot_limit")]
    pub limit: usize,
}

fn default_snapshot_limit() -> usize {
    100
}

async fn snapshots_list_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<SnapshotsListQuery>,
) -> Response {
    match state.snapshot_store {
        Some(ref store) => match store.list(query.limit).await {
            Ok(snapshots) => {
                let items: Vec<serde_json::Value> = snapshots
                    .into_iter()
                    .map(|s| {
                        serde_json::json!({
                            "snapshot_id": s.checkpoint_id,
                            "task_id": s.task_id,
                            "event_offset": s.event_offset,
                            "timestamp": s.timestamp.to_rfc3339(),
                        })
                    })
                    .collect();
                (StatusCode::OK, Json(json!({"snapshots": items}))).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "list_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "snapshot_store_not_configured"})),
        )
            .into_response(),
    }
}

// ─── Trace & Replay Handlers ───

#[derive(Debug, Deserialize)]
struct TracesListQuery {
    #[serde(default = "default_trace_limit")]
    limit: usize,
}

fn default_trace_limit() -> usize {
    100
}

async fn traces_list_handler(
    State(state): State<Arc<GatewayState>>,
    Query(query): Query<TracesListQuery>,
) -> Response {
    match state.trace_store {
        Some(ref store) => match store.list_meta(query.limit).await {
            Ok(meta) => {
                let items: Vec<serde_json::Value> = meta
                    .into_iter()
                    .map(|m| {
                        serde_json::json!({
                            "trace_id": m.trace_id,
                            "agent_id": m.agent_id,
                            "task_id": m.task_id,
                            "event_count": m.event_count,
                            "created_at": m.created_at.to_rfc3339(),
                            "tier": format!("{:?}", m.tier).to_lowercase(),
                        })
                    })
                    .collect();
                (StatusCode::OK, Json(json!({"traces": items}))).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "list_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "trace_store_not_configured"})),
        )
            .into_response(),
    }
}

async fn trace_get_handler(
    State(state): State<Arc<GatewayState>>,
    Path(trace_id): Path<String>,
) -> Response {
    match state.trace_store {
        Some(ref store) => match store.load(&trace_id).await {
            Ok(Some(trace)) => (
                StatusCode::OK,
                Json(serde_json::to_value(trace).unwrap_or_default()),
            )
                .into_response(),
            Ok(None) => (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "trace_not_found", "trace_id": trace_id})),
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "load_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "trace_store_not_configured"})),
        )
            .into_response(),
    }
}

async fn trace_replay_handler(
    State(state): State<Arc<GatewayState>>,
    Path(trace_id): Path<String>,
) -> Response {
    match state.replay_engine {
        Some(ref engine) => {
            let events = Arc::new(std::sync::Mutex::new(Vec::new()));
            let events_for_closure = events.clone();
            match engine
                .replay(
                    &trace_id,
                    Box::new(move |event| {
                        events_for_closure
                            .lock()
                            .unwrap()
                            .push(serde_json::to_value(&event).unwrap_or_default());
                        Ok(())
                    }),
                )
                .await
            {
                Ok(replayed) => {
                    let ev = events.lock().unwrap();
                    (
                        StatusCode::OK,
                        Json(json!({
                            "trace_id": trace_id,
                            "replayed": replayed,
                            "events": &*ev,
                        })),
                    )
                        .into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "replay_failed", "message": e.to_string()})),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "replay_engine_not_configured"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Plugin handlers
// ---------------------------------------------------------------------------

async fn plugins_list_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.plugin_registry {
        Some(ref registry) => {
            // MVP: list from local dir only. In future support remote registry query.
            match registry.discover("/opt/cogneva/plugins").await {
                Ok(manifests) => {
                    (StatusCode::OK, Json(json!({ "plugins": manifests }))).into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "discovery_failed", "message": e.to_string()})),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "plugin_registry_not_configured"})),
        )
            .into_response(),
    }
}

async fn plugin_upload_handler(State(state): State<Arc<GatewayState>>, body: String) -> Response {
    match state.plugin_registry {
        Some(ref registry) => match serde_json::from_str::<cog_core::PluginManifest>(&body) {
            Ok(manifest) => match registry.fetch(&manifest).await {
                Ok(bytes) => match registry.load(&bytes, &manifest).await {
                    Ok(handle) => (
                        StatusCode::OK,
                        Json(json!({
                            "plugin_id": handle.plugin_id,
                            "name": handle.manifest.name,
                            "status": "loaded"
                        })),
                    )
                        .into_response(),
                    Err(e) => (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "load_failed", "message": e.to_string()})),
                    )
                        .into_response(),
                },
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "fetch_failed", "message": e.to_string()})),
                )
                    .into_response(),
            },
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid_manifest", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "plugin_registry_not_configured"})),
        )
            .into_response(),
    }
}

async fn plugin_unload_handler(
    State(state): State<Arc<GatewayState>>,
    Path(plugin_id): Path<String>,
) -> Response {
    match state.plugin_registry {
        Some(ref _registry) => {
            // MVP: unload is a no-op until registry maintains loaded handles by id.
            (
                StatusCode::OK,
                Json(json!({ "plugin_id": plugin_id, "status": "unloaded" })),
            )
                .into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "plugin_registry_not_configured"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Sandbox handlers
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct SandboxExecuteRequest {
    payload: serde_json::Value,
    input: serde_json::Value,
    timeout_secs: Option<u64>,
}

async fn sandbox_execute_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<SandboxExecuteRequest>,
) -> Response {
    match state.sandbox_backend {
        Some(ref backend) => {
            let payload = if let Some(bytes_b64) =
                req.payload.get("wasm_bytes_b64").and_then(|v| v.as_str())
            {
                match base64::engine::general_purpose::STANDARD.decode(bytes_b64) {
                    Ok(bytes) => cog_core::SandboxPayload::Wasm {
                        bytes,
                        entry: req
                            .payload
                            .get("entry")
                            .and_then(|v| v.as_str())
                            .unwrap_or("main")
                            .into(),
                    },
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "invalid_base64", "message": e.to_string()})),
                        )
                            .into_response();
                    }
                }
            } else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "unknown_payload_type"})),
                )
                    .into_response();
            };

            let sandbox_req = cog_core::SandboxRequest {
                task_id: uuid::Uuid::new_v4().to_string(),
                agent_id: "gateway".into(),
                payload,
                input: req.input,
                timeout: std::time::Duration::from_secs(
                    req.timeout_secs.unwrap_or(
                        state
                            .sandbox_task_timeout_secs
                            .load(std::sync::atomic::Ordering::Relaxed),
                    ),
                ),
                limits: Default::default(),
            };

            match backend.execute(&sandbox_req).await {
                Ok(result) => (
                    StatusCode::OK,
                    Json(json!({
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                        "exit_code": result.exit_code,
                        "output": result.output,
                        "duration_ms": result.duration_ms,
                    })),
                )
                    .into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "execution_failed", "message": e.to_string()})),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "sandbox_backend_not_configured"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Guardrail handlers
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct GuardCheckRequest {
    messages: Vec<serde_json::Value>,
    tool_call: Option<serde_json::Value>,
    response: Option<String>,
    check_type: String, // "input" | "output" | "tool"
}

async fn guard_check_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<GuardCheckRequest>,
) -> Response {
    match state.guardrail {
        Some(ref guardrail) => {
            let result = match req.check_type.as_str() {
                "input" => {
                    let messages: Vec<cog_core::Message> = req.messages.into_iter()
                        .filter_map(|v| serde_json::from_value(v).ok())
                        .collect();
                    guardrail.check_input(&messages).await
                }
                "output" => {
                    let response = req.response.unwrap_or_default();
                    guardrail.check_output(&response).await
                }
                "tool" => {
                    let tool_call = req.tool_call.and_then(|v| serde_json::from_value::<cog_core::ToolCall>(v).ok())
                        .unwrap_or(cog_core::ToolCall { id: "".into(), name: "".into(), arguments: serde_json::Value::Null });
                    guardrail.check_tool_call(&tool_call).await
                }
                _ => return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "invalid_check_type", "valid": ["input", "output", "tool"]})),
                ).into_response(),
            };
            let (status, body) = match result {
                cog_core::GuardResult::Pass => (StatusCode::OK, json!({"verdict": "pass"})),
                cog_core::GuardResult::Block { reason, rule } => (
                    StatusCode::FORBIDDEN,
                    json!({"verdict": "block", "reason": reason, "rule": rule}),
                ),
                cog_core::GuardResult::Warn { reason, rule } => (
                    StatusCode::OK,
                    json!({"verdict": "warn", "reason": reason, "rule": rule}),
                ),
            };
            (status, Json(body)).into_response()
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "guardrail_not_configured"})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Eval handlers
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct EvalRunRequest {
    dataset_path: String,
    #[allow(dead_code)]
    max_concurrency: Option<usize>,
}

async fn eval_run_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<EvalRunRequest>,
) -> Response {
    match state.eval_service {
        Some(ref service) => match service.run_eval(&req.dataset_path).await {
            Ok(report) => (StatusCode::OK, Json(report)).into_response(),
            Err(e) => (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "eval_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "eval_service_not_configured"})),
        )
            .into_response(),
    }
}

async fn eval_compare_handler(
    State(state): State<Arc<GatewayState>>,
    Json(req): Json<serde_json::Value>,
) -> Response {
    match state.eval_service {
        Some(ref service) => {
            let baseline = req
                .get("baseline")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let challenger = req
                .get("challenger")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let baseline_name = req
                .get("baseline_name")
                .and_then(|v| v.as_str())
                .unwrap_or("baseline");
            let challenger_name = req
                .get("challenger_name")
                .and_then(|v| v.as_str())
                .unwrap_or("challenger");
            match service
                .compare_eval(baseline, challenger, baseline_name, challenger_name)
                .await
            {
                Ok(report) => (StatusCode::OK, Json(report)).into_response(),
                Err(e) => (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "compare_failed", "message": e.to_string()})),
                )
                    .into_response(),
            }
        }
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "eval_service_not_configured"})),
        )
            .into_response(),
    }
}

async fn eval_list_datasets_handler(State(state): State<Arc<GatewayState>>) -> Response {
    let datasets_dir = std::path::PathBuf::from(format!("{}/eval/datasets", state.data_dir));
    let mut datasets = vec![];

    if datasets_dir.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&datasets_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                    if ext == "jsonl" || ext == "yaml" || ext == "yml" {
                        let name = path
                            .file_stem()
                            .unwrap_or_default()
                            .to_string_lossy()
                            .to_string();
                        let case_count = if ext == "jsonl" {
                            std::fs::read_to_string(&path)
                                .map(|c| c.lines().filter(|l| !l.trim().is_empty()).count())
                                .unwrap_or(0)
                        } else {
                            0
                        };
                        datasets.push(cog_core::EvalDatasetInfo {
                            name: name.clone(),
                            path: path.display().to_string(),
                            case_count,
                            tags: vec![ext.into()],
                        });
                    }
                }
            }
        }
    }

    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    let resp = cog_core::EvalListDatasetsResponse { datasets };
    (StatusCode::OK, Json(json!(resp))).into_response()
}

async fn eval_get_report_handler(
    State(state): State<Arc<GatewayState>>,
    Path(report_id): Path<String>,
) -> Response {
    let report_path = std::path::PathBuf::from(format!(
        "{}/eval/reports/{}.json",
        state.data_dir, report_id
    ));
    if !report_path.exists() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "report_not_found", "report_id": report_id})),
        )
            .into_response();
    }

    match std::fs::read_to_string(&report_path) {
        Ok(content) => {
            let dataset_name = serde_json::from_str::<serde_json::Value>(&content)
                .ok()
                .and_then(|v| {
                    v.get("dataset_name")
                        .and_then(|d| d.as_str())
                        .map(String::from)
                })
                .unwrap_or_default();
            let report_markdown = match state.eval_service {
                Some(ref service) => match service.render_report(&content, "markdown").await {
                    Ok(md) => md,
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "render_failed", "message": e.to_string()})),
                        )
                            .into_response();
                    }
                },
                None => String::new(),
            };
            let resp = cog_core::EvalGetReportResponse {
                run_id: report_id,
                dataset_name,
                report_markdown,
                report_json: content,
            };
            (StatusCode::OK, Json(json!(resp))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "read_failed", "message": e.to_string()})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Protocol handlers (MCP + A2A)
// ---------------------------------------------------------------------------

async fn mcp_tools_list_handler(State(state): State<Arc<GatewayState>>) -> Response {
    match state.mcp_client {
        Some(ref client) => match client.list_tools().await {
            Ok(tools) => (StatusCode::OK, Json(json!({"tools": tools}))).into_response(),
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "mcp_list_failed", "message": e.to_string()})),
            )
                .into_response(),
        },
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "mcp_client_not_configured"})),
        )
            .into_response(),
    }
}

async fn a2a_agent_card_handler(State(state): State<Arc<GatewayState>>) -> Response {
    // Expose cogneva's own Agent Card
    let card = cog_core::AgentCard {
        name: "cogneva".into(),
        description: "Cogneva autonomous multi-agent collaboration platform".into(),
        url: format!(
            "http://0.0.0.0:{}",
            state
                .config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .http_port
        ),
        version: env!("CARGO_PKG_VERSION").into(),
        capabilities: cog_core::AgentCapabilities {
            streaming: true,
            push_notifications: false,
            state_transition_history: true,
        },
        skills: vec![
            cog_core::AgentSkill {
                id: "task_orchestration".into(),
                name: "Task Orchestration".into(),
                description: "DAG-based task scheduling and execution".into(),
                tags: vec!["orchestration".into(), "dag".into()],
                examples: vec![],
            },
            cog_core::AgentSkill {
                id: "multi_agent_collaboration".into(),
                name: "Multi-Agent Collaboration".into(),
                description: "Crew-based multi-agent problem solving".into(),
                tags: vec!["collaboration".into(), "crew".into()],
                examples: vec![],
            },
        ],
        authentication: cog_core::AgentAuthentication {
            schemes: vec!["jwt".into()],
            credentials: None,
        },
    };
    (StatusCode::OK, Json(json!(card))).into_response()
}
