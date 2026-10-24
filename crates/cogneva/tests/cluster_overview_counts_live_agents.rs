//! The cluster overview counts the agents that are alive, not a table nobody
//! writes.
//!
//! The storage layer answered `total_agents` / `active_agents` from
//! `cog_agent_states`, which has no production writer, so the route printed
//! zero executors while the roster listed live workers and the face readings
//! were owned by them. The route now fills both cells from the registry an
//! agent writes its own presence into — the same one `/api/v1/agents` reads.
//!
//! Two cases, because the fix has two halves: a registry that answers must
//! replace the storage layer's answer, and an absent registry must leave the
//! cells empty rather than letting a zero stand as a reading.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    AgentRegistration, AgentRegistry, GatewayConfig, PreCheckResult, QuotaManager, QuotaSummary,
    ResourceInfo, SFResult, StateBackend, User, UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};
use cog_storage::{MemoryAgentRegistry, MemoryObservabilityGateway, MemoryStateBackend};

/// The quota store the state has a slot for, with no store behind it.
///
/// Nothing here spends anything. The slot exists because the gateway state
/// carries one, and the honest filler is one that permits what it is asked.
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

/// The gateway state the router is built over. Everything is absent or a
/// stand-in except the two surfaces this route reads.
fn state(agent_registry: Option<Arc<MemoryAgentRegistry>>) -> Arc<GatewayState> {
    let jwt_manager: Arc<dyn cog_core::AuthProvider> =
        Arc::new(JwtManager::new(JwtConfig::default()));
    let raw_logger: Arc<dyn cog_core::RawLogger> = Arc::new(cog_storage::MemoryRawLogger::new());
    let dag_executor = Arc::new(cog_orchestrator::DagExecutor::new("test-workspace".into()));
    let orchestrator = Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag_executor));
    let state_backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());

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
        agent_registry: agent_registry.map(|r| r as Arc<dyn cog_core::AgentRegistry>),
        observability_gateway: Some(Arc::new(MemoryObservabilityGateway::new(state_backend))),
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
        principle_ledger: None,
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
        agent_messages: None,
        datasets: None,
    })
}

/// A token this router's own signer accepts, so a request reaches a handler
/// instead of stopping at the layer that answers anonymous ones.
async fn bearer(state: &Arc<GatewayState>) -> String {
    let user = User {
        id: uuid::Uuid::new_v4(),
        phone: None,
        email: None,
        username: "cluster-overview-probe".into(),
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

async fn overview(state: Arc<GatewayState>) -> (StatusCode, serde_json::Value) {
    let token = bearer(&state).await;
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/cluster/overview")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .expect("the request is well formed");
    let response = create_router(state)
        .oneshot(request)
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// Two workers registered and beating is two executors, and the storage
/// layer's zero does not stand.
#[tokio::test]
async fn the_executor_count_is_the_live_registry() {
    let registry = Arc::new(MemoryAgentRegistry::new());
    for id in ["cogneva-worker-0", "cogneva-worker-1"] {
        registry
            .register(&AgentRegistration::new(
                id,
                "worker-pod",
                "10.42.0.10",
                "worker",
                "cogneva",
                Vec::new(),
                ResourceInfo {
                    cpu_cores: 1,
                    memory_gb: 1,
                },
            ))
            .await
            .expect("the registry takes a registration");
    }

    let (status, body) = overview(state(Some(registry))).await;

    assert_eq!(status, StatusCode::OK, "the route answers: {body}");
    assert_eq!(body["total_agents"], 2, "{body}");
    assert_eq!(body["active_agents"], 2, "{body}");
}

/// With no registry there is no presence reading, and the cells stay empty.
/// A zero here would be the same string the broken count printed.
#[tokio::test]
async fn without_a_registry_the_count_is_unknown_not_zero() {
    let (status, body) = overview(state(None)).await;

    assert_eq!(status, StatusCode::OK, "the route answers: {body}");
    assert!(
        body["total_agents"].is_null(),
        "no registry is no reading, not an empty fleet: {body}"
    );
    assert!(body["active_agents"].is_null(), "{body}");
}
