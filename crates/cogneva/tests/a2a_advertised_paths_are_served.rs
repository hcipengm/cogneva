//! Every path the Agent Card advertises answers on the router that is served.
//!
//! The card is what an A2A caller reads before it asks for anything, and each
//! flag on it is a promise about a path. A flag left set after its route was
//! deleted is a card that lies, and nothing that reads the card can show it: the
//! flag is still set, the constant is still there, and the caller that believed
//! them gets a 404. The flag and the route are two statements in two places,
//! which is exactly how this card once advertised streaming while no path in the
//! tree contained `message/stream`.
//!
//! So this asks the router instead of the card. The promises are read from the
//! list the card's own flags are derived from, so a capability added there is
//! checked here without being added anywhere, and each listed path is requested
//! the way a caller would. Both verbs are tried rather than the one the route is
//! registered for: which verb a path answers on is one more hand-held fact, and
//! a probe carrying its own copy of it cannot notice the copy going stale.
//!
//! `404` and `405` are the router's answers for "no such route here" and "that
//! route takes another verb"; anything else means a handler for that path
//! exists. The check is therefore about registration and not about what the
//! handler then does, which is the part other suites cover.
//!
//! Two things this had to grow, both found by asking the router rather than by
//! reasoning about it:
//!
//! - The request is **authenticated**. This router answers `401` to every
//!   request it is given -- including paths it does not have -- before routing
//!   decides anything, so an anonymous probe gets a rejection whatever the path
//!   is and can tell a deleted route from a served one exactly never. A token
//!   from the state's own signer is what gets past that layer.
//! - There is a **control path**, one that is certainly not registered, and it
//!   has to come back `404`. Without it, the probe's question is "did anything
//!   answer", and any future catch-all, fallback or blanket rejection makes that
//!   answer yes for every path in the tree -- a gate that reports success by
//!   having lost the ability to see the difference. With it, the ability to
//!   tell is itself a checked claim, and it is checked in the same run.
//!
//! Its own router rather than the shared end-to-end harness, because what this
//! judges has nothing to do with any store: the harness there builds its state
//! around a quota store and skips the whole file when none is reachable, and a
//! contract about the route table should not go quiet on a machine that has no
//! database on it. The one component this state does need a store for is
//! supplied by a stand-in below, and the stand-in is never asked anything: a
//! request that reaches a handler at all is already the answer.

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

/// The quota store the state has a slot for, with no store behind it.
///
/// Nothing in this file spends anything. The slot exists because the gateway
/// state carries one, and the honest filler is one that permits what it is asked
/// -- a refusal here would be a statement about this stand-in rather than about
/// the route table, which is the one thing being judged.
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
        username: "advertised-paths-probe".into(),
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

/// What the router answers one method on one path.
async fn status_of(app: &axum::Router, method: &str, path: &str, token: &str) -> StatusCode {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from("{}"))
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

/// A path is unserved when the router says it has no such route, or has one
/// under a different verb. Both are answers about registration; the path
/// behind them does not matter, and neither does what a handler would say.
fn unserved(status: StatusCode) -> bool {
    status == StatusCode::NOT_FOUND || status == StatusCode::METHOD_NOT_ALLOWED
}

#[tokio::test]
async fn every_advertised_capability_names_a_path_the_router_serves() {
    let state = state();
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    // The control: a path nobody registered. If this is not answered with "no
    // such route", this probe cannot see the difference it is asking about --
    // and every advertised path below would pass for that reason alone.
    let control = format!("{}-that-is-not-a-route", cog_gateway::a2a::MESSAGE_STREAM);
    let mut control_unserved = false;
    for method in ["POST", "GET"] {
        if unserved(status_of(&app, method, &control, &token).await) {
            control_unserved = true;
        }
    }
    assert!(
        control_unserved,
        "a path that was never registered ({control}) was answered as though it were a \
         route, so this probe cannot tell an advertised path from an absent one"
    );

    let mut probed = 0usize;
    for advertised in cog_gateway::a2a::ADVERTISED {
        probed += 1;
        let mut served = false;
        for method in ["POST", "GET"] {
            if !unserved(status_of(&app, method, advertised.endpoint, &token).await) {
                served = true;
            }
        }
        assert!(
            served,
            "the card advertises `{}`, naming {}, and no verb on the router reaches a \
             handler for it -- the promise has no endpoint behind it",
            advertised.capability, advertised.endpoint
        );
    }
    assert!(
        probed > 0,
        "an empty list of promises is nothing to check: this gate would pass without asking"
    );
}
