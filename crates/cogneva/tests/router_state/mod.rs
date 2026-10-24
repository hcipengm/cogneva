//! A gateway state with no store behind it, for contract tests about routes.
//!
//! The suites that judge the route table need a [`GatewayState`] and nothing
//! else: they ask whether a path is served and what a handler answers, and both
//! answers come from code that runs before any store is reached. But the state
//! has a slot for every component the gateway can be given, so building one is
//! long, and it has to be rebuilt whenever a slot is added. Three copies of that
//! list already existed, each one breaking the same way when a field appeared.
//! It lives here once instead.
//!
//! Every component is either absent or a stand-in, and the stand-in is never
//! asked anything: a refusal from it would be a statement about this fixture
//! rather than about the route being judged. That is also why the quota
//! stand-in permits everything it is asked -- a test that wants a refusal has to
//! arrange for one, not inherit it from here.
//!
//! A suite that needs a store, an agent pool or a message backend is not this
//! fixture's caller; it builds its own state around the thing it is judging.

use std::sync::Arc;

use tokio::sync::broadcast;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    GatewayConfig, PreCheckResult, QuotaManager, QuotaSummary, SFResult, User, UserStatus, UserType,
};
use cog_gateway::GatewayState;

/// The quota store the state has a slot for, permitting everything it is asked.
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

/// The gateway state the router is built over: every component either absent or
/// a stand-in, and none of them touched by a request that only has to be routed.
pub fn stateless() -> Arc<GatewayState> {
    stateless_with_datasets(None)
}

/// The same state, with a dataset store (and so an object store) behind it.
///
/// Separate from [`stateless`] rather than another argument there: the suites
/// that only route a request should not have to name a store they never touch,
/// and a suite that does touch one should be visible in its first line.
pub fn stateless_with_datasets(
    datasets: Option<Arc<cog_storage::DatasetStore>>,
) -> Arc<GatewayState> {
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
        message_backend: None,
        event_publisher: None,
        media_backend: None,
        websocket_client: None,
        evolution_admin: None,
        audit_stream: None,
        taste_intent_sink: None,
        llm_usage_reader: None,
        llm_client: std::sync::Arc::new(std::sync::RwLock::new(None)),
        chat_turns: None,
        datasets,
    })
}

/// A token this router's own signer accepts, so a request reaches routing
/// instead of stopping at the layer that answers every anonymous one.
///
/// The name is the caller's so a failing test says which suite minted the token
/// it used; nothing reads it.
pub async fn admin_bearer(state: &Arc<GatewayState>, username: &str) -> String {
    let user = User {
        id: uuid::Uuid::new_v4(),
        phone: None,
        email: None,
        username: username.into(),
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
