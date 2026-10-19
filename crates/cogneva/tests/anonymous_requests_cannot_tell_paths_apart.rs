//! An anonymous request gets the same answer whether or not the path it names
//! exists.
//!
//! The gateway's authentication is a blanket layer, not a per-route one: a
//! request without a token is rejected before routing decides anything, so the
//! answer cannot depend on which path was asked for. That is a property of where
//! the layer is attached, and it is invisible from the route table -- moving it
//! onto the individual protected routers leaves every route working, for callers
//! that have a token, while giving anonymous ones a different answer for a path
//! that exists than for one that never will. That difference is a way to read
//! the route table from outside without a credential.
//!
//! So the check asks the router twice, anonymously, with one path that is
//! served and one that is not, and requires the two answers to be equal. It does
//! not name the status: which code the layer answers with is a separate decision,
//! and pinning it here would turn a change of that decision into a failure of
//! this one.
//!
//! The equality alone would be worth nothing if the two paths were
//! indistinguishable for some other reason -- two absent paths answer alike
//! whatever the layer does. So the ability to tell them apart is itself checked,
//! in the same run, with a token: authenticated, the served path must not come
//! back "no such route" and the never-registered one must. Without that half, a
//! router whose routes all vanished would pass this file.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    GatewayConfig, PreCheckResult, QuotaManager, QuotaSummary, SFResult, User, UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};

/// A served, protected path. Chosen from the general protected routers, which is
/// where a per-route attachment would be added first.
const SERVED: &str = "/api/v1/auth/me";

/// A path this router will never have. It is the pair of `SERVED` in every
/// request below; nothing here claims it should be servable.
const NEVER_REGISTERED: &str = "/api/v1/this-path-was-never-registered";

/// The quota store the state has a slot for, permitting everything it is asked.
///
/// Nothing here spends anything. A refusal would be a statement about this
/// stand-in rather than about the layer being judged.
struct OpenQuota;

#[async_trait::async_trait]
impl QuotaManager for OpenQuota {
    async fn pre_check(
        &self,
        _user_id: &str,
        _workspace_id: Option<&str>,
        _estimated_tokens: u64,
    ) -> PreCheckResult {
        PreCheckResult {
            allowed: true,
            remaining: u64::MAX,
            estimated_cost: 0.0,
        }
    }

    async fn finalize(
        &self,
        _user_id: &str,
        _workspace_id: Option<&str>,
        _estimated_tokens: u64,
        _actual_tokens: u64,
    ) -> SFResult<()> {
        Ok(())
    }

    async fn get_remaining(&self, _user_id: &str) -> u64 {
        u64::MAX
    }

    async fn get_workspace_remaining(&self, _workspace_id: &str) -> u64 {
        u64::MAX
    }

    async fn get_used_today(&self, _user_id: &str) -> u64 {
        0
    }

    async fn get_workspace_used_today(&self, _workspace_id: &str) -> u64 {
        0
    }

    async fn get_request_count_today(&self, _user_id: &str) -> u64 {
        0
    }

    async fn get_user_summary(&self, user_id: &str) -> QuotaSummary {
        QuotaSummary {
            target_id: user_id.to_string(),
            target_type: "user".into(),
            total_quota: u64::MAX,
            remaining: u64::MAX,
            used_today: 0,
        }
    }

    async fn get_workspace_summary(&self, workspace_id: &str) -> QuotaSummary {
        QuotaSummary {
            target_id: workspace_id.to_string(),
            target_type: "workspace".into(),
            total_quota: u64::MAX,
            remaining: u64::MAX,
            used_today: 0,
        }
    }

    async fn recharge(
        &self,
        _user_id: &str,
        _tokens: u64,
        _valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()> {
        Ok(())
    }

    async fn recharge_workspace(
        &self,
        _workspace_id: &str,
        _tokens: u64,
        _valid_until: Option<chrono::DateTime<chrono::Utc>>,
    ) -> SFResult<()> {
        Ok(())
    }
}

/// The gateway state the router is built over: every component absent or a
/// stand-in, and none of them reached by a request that only has to be
/// classified.
fn state() -> Arc<GatewayState> {
    let jwt_manager: Arc<dyn cog_core::AuthProvider> =
        Arc::new(JwtManager::new(JwtConfig::default()));
    let raw_logger: Arc<dyn cog_core::RawLogger> = Arc::new(cog_storage::MemoryRawLogger::new());
    let dag_executor = Arc::new(cog_orchestrator::DagExecutor::new("test-workspace".into()));
    let orchestrator = Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag_executor));

    Arc::new(GatewayState {
        data_dir: "/tmp".into(),
        config: std::sync::RwLock::new(GatewayConfig {
            http_port: 0,
            ws_port: 0,
            metrics_port: 9090,
            cors_origins: vec!["*".into()],
            websocket_timeout_secs: 30,
            websocket_inactivity_timeout_secs: 90,
            websocket_tick_secs: 5,
            notification_limit: 50,
            sandbox_task_timeout_secs: 30,
            request_timeout_secs: 30,
            notification_webhook_url: None,
            ..Default::default()
        }),
        request_timeout_secs: std::sync::atomic::AtomicU64::new(30),
        sandbox_task_timeout_secs: std::sync::atomic::AtomicU64::new(30),
        event_tx: broadcast::channel(16).0,
        evolution_stream: None,
        task_event_tx: broadcast::channel(16).0,
        jwt_manager,
        quota_manager: Arc::new(OpenQuota),
        hierarchy_manager: None,
        action_plan_store: Default::default(),
        collaboration_graph: None,
        raw_logger,
        memory_backend: None,
        memory_ingestor: None,
        metrics_backend: None,
        metrics_exporter: None,
        search_backend: None,
        raw_log_index_store: None,
        hook_archive: None,
        hook_engine: None,
        orchestrator,
        task_executors: Arc::new(cog_orchestrator::TaskExecutorRouter::new()),
        agent_registry: None,
        observability_gateway: None,
        connection_manager: None,
        wiki_adapter: None,
        knowledge_backend: None,
        user_store: None,
        platform_identities: None,
        contribution_control: None,
        login_rate_limiter: None,
        session_manager: None,
        heartbeat_history: None,
        snapshot_store: None,
        object_backend: None,
        notification_store: None,
        supervisor: None,
        supervisor_event_cache: Arc::new(
            cog_gateway::supervisor_control::SupervisorEventCache::default(),
        ),
        alert_store: None,
        active_alert_source: None,
        backend_health_probe: None,
        trace_store: None,
        replay_engine: None,
        sandbox_backend: None,
        plugin_registry: None,
        guardrail: None,
        eval_service: None,
        observables: Vec::new(),
        metrics_dimensions: Vec::new(),
        mcp_client: None,
        workspace_store: None,
        governance_store: None,
        external_skill_registry: None,
        notification_dispatcher: None,
        notification_tx: broadcast::channel(16).0,
        agent_pool: None,
        event_publisher: None,
        media_backend: None,
        websocket_client: None,
        evolution_admin: None,
        audit_stream: None,
        taste_intent_sink: None,
        llm_usage_reader: None,
        llm_client: std::sync::Arc::new(std::sync::RwLock::new(None)),
        chat_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
    })
}

/// A token this router's own signer accepts, so a request reaches routing
/// instead of stopping at the layer that answers every anonymous one.
async fn bearer(state: &Arc<GatewayState>) -> String {
    let user = User {
        id: uuid::Uuid::new_v4(),
        phone: None,
        email: None,
        username: "anonymous-face-probe".into(),
        display_name: None,
        avatar_url: None,
        status: UserStatus::Active,
        user_type: UserType::Admin,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    state
        .jwt_manager
        .generate_token(&user, vec![], vec![])
        .await
        .expect("the state's signer issues a token")
        .access_token
}

/// What the router answers one path, with or without a credential.
async fn status_of(app: &axum::Router, path: &str, token: Option<&str>) -> StatusCode {
    let mut request = Request::builder()
        .method("GET")
        .uri(path)
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let request = request
        .body(Body::from(""))
        .expect("the request is well formed");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    // Read the body rather than drop the response: a handler that starts
    // streaming and is dropped mid-body is a different failure, and this check
    // is not about bodies.
    let _ = to_bytes(response.into_body(), usize::MAX).await;
    status
}

/// "No such route here", or "that route takes another verb". Both are answers
/// about registration; what the path behind them does is not this file's subject.
fn unserved(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED
}

#[tokio::test]
async fn an_anonymous_request_gets_one_answer_whatever_path_it_names() {
    let state = state();
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    // The control on the probe itself. With a credential the two paths are
    // distinguishable -- one is routed, the other is not -- so an anonymous
    // equality below is the layer's doing and not the paths' likeness.
    let served_with_token = status_of(&app, SERVED, Some(&token)).await;
    let absent_with_token = status_of(&app, NEVER_REGISTERED, Some(&token)).await;
    let served_anonymous = status_of(&app, SERVED, None).await;
    let absent_anonymous = status_of(&app, NEVER_REGISTERED, None).await;
    let all = format!(
        "with a token: {SERVED} -> {served_with_token}, {NEVER_REGISTERED} -> \
         {absent_with_token}; anonymously: {SERVED} -> {served_anonymous}, \
         {NEVER_REGISTERED} -> {absent_anonymous}"
    );

    assert!(
        !unserved(served_with_token),
        "{SERVED} is not routed at all, so this probe cannot tell a path that exists \
         from one that does not, and everything below would pass for that reason \
         ({all})"
    );
    assert!(
        unserved(absent_with_token),
        "{NEVER_REGISTERED} is routed, so it is not the control this needs ({all})"
    );

    assert_eq!(
        served_anonymous, absent_anonymous,
        "an anonymous request is answered differently for a path that exists \
         ({SERVED} -> {served_anonymous}) than for one that never will \
         ({NEVER_REGISTERED} -> {absent_anonymous}). The difference is readable \
         without a credential, and it is the route table it reads"
    );
}
