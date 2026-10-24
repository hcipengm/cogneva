//! The change account is readable from the product, not only from SQL.
//!
//! The ledger's reads existed and its writer ran every pass, and the only way
//! to see what the corpus owes was to connect to the deployment's database by
//! hand: how many demands sit in each state, how the outstanding work is spread
//! across bands, and whether intake had actually been folded in. The owner's
//! only interface is the WebUI, so the account needs a route that answers those
//! questions — and answers them over the whole set, not over the slice it
//! prints.
//!
//! These cases build the router over a stand-in account and ask what a caller
//! sees. The store's own semantics (ordering, the set-once entry revision,
//! the reason columns) are tested against PostgreSQL where they are decided;
//! what is checked here is the route: which cells it promises, where it takes
//! its totals from, and what it says when there is no account at all.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    ChangeBlock, ChangeIdentity, ChangeLedgerRow, ChangeLedgerStore, ChangePriority, ChangeReading,
    ChangeSource, ChangeState, GatewayConfig, PreCheckResult, QuotaManager, QuotaSummary, SFResult,
    User, UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};

/// An account holding rows the cases below describe, with every write path
/// unimplemented on purpose: a read route that writes is a route this stand-in
/// should catch rather than absorb.
struct FakeLedger {
    rows: Vec<ChangeLedgerRow>,
    pending_declarations: u64,
    pending_retirements: u64,
}

impl FakeLedger {
    fn new(
        rows: Vec<ChangeLedgerRow>,
        pending_declarations: u64,
        pending_retirements: u64,
    ) -> Self {
        Self {
            rows,
            pending_declarations,
            pending_retirements,
        }
    }
}

fn row(state: ChangeState, doc: &str, scope: &str) -> ChangeLedgerRow {
    ChangeLedgerRow {
        identity: ChangeIdentity::new("grep", scope, format!("literal-in-{scope}")),
        state,
        title: Some(format!("a demand from {doc}")),
        assertion: Some(format!("{scope} carries what {doc} asks for")),
        sources: vec![ChangeSource {
            doc: doc.to_string(),
            verdict: Some("adopt".into()),
            priority: ChangePriority::P1,
            block: ChangeBlock::None,
        }],
        first_seen_at: chrono::Utc::now(),
        measured_at: None,
        measured_rev: None,
        delivered_rev: None,
        landed_at: None,
        unsettled_reason: None,
        retire_reason: None,
        retired_at: None,
    }
}

#[async_trait::async_trait]
impl ChangeLedgerStore for FakeLedger {
    async fn record_readings(
        &self,
        _readings: &[ChangeReading],
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        unimplemented!("the read route does not record readings")
    }

    async fn declare(&self, _readings: &[ChangeReading]) -> SFResult<usize> {
        unimplemented!("the read route does not declare readings")
    }

    async fn fold_declarations(
        &self,
        _limit: usize,
        _first_seen_rev: Option<&str>,
    ) -> SFResult<usize> {
        unimplemented!("the read route does not fold declarations")
    }

    async fn fold_retirements(&self, _limit: usize) -> SFResult<usize> {
        unimplemented!("the read route does not fold retirements")
    }

    async fn pending_declarations(&self) -> SFResult<u64> {
        Ok(self.pending_declarations)
    }

    async fn pending_retirements(&self) -> SFResult<u64> {
        Ok(self.pending_retirements)
    }

    async fn apply_measurement(&self, _measurement: &cog_core::ChangeMeasurement) -> SFResult<()> {
        unimplemented!("the read route does not measure")
    }

    async fn outstanding(&self, limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        Ok(self
            .rows
            .iter()
            .filter(|row| row.state.is_outstanding())
            .take(limit)
            .cloned()
            .collect())
    }

    async fn owed_by_band(&self) -> SFResult<Vec<(ChangePriority, usize)>> {
        Ok(ChangePriority::ALL
            .into_iter()
            .map(|band| {
                let rows = self
                    .rows
                    .iter()
                    .filter(|row| row.state.is_outstanding())
                    .filter(|row| row.sources.iter().any(|source| source.priority == band))
                    .count();
                (band, rows)
            })
            .collect())
    }

    async fn landed_due(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the read face does not ask this question yet")
    }

    async fn landed_touching(
        &self,
        _paths: &[String],
        _limit: usize,
    ) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the read face does not ask this question yet")
    }

    async fn landed_awaiting_parent_observation(
        &self,
        _limit: usize,
    ) -> SFResult<Vec<(ChangeIdentity, String)>> {
        unimplemented!("the read face does not ask this question yet")
    }

    async fn record_parent_observation(
        &self,
        _identity: &ChangeIdentity,
        _observation: &str,
    ) -> SFResult<bool> {
        unimplemented!("the read route does not backfill")
    }

    async fn record_reopen(&self, _reopen: &cog_core::ChangeReopen) -> SFResult<()> {
        unimplemented!("the read route does not record reopens")
    }

    async fn retired_surface_moved(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the read face does not ask this question yet")
    }

    async fn retired_answered_elsewhere(&self, _limit: usize) -> SFResult<Vec<ChangeLedgerRow>> {
        unimplemented!("the read face does not ask this question yet")
    }

    async fn record_delivery(&self, _identity: &ChangeIdentity, _rev: &str) -> SFResult<()> {
        unimplemented!("the read route does not record deliveries")
    }

    async fn get(&self, identity: &ChangeIdentity) -> SFResult<Option<ChangeLedgerRow>> {
        Ok(self
            .rows
            .iter()
            .find(|row| &row.identity == identity)
            .cloned())
    }

    async fn census(&self) -> SFResult<Vec<(ChangeState, u64)>> {
        Ok(ChangeState::ALL
            .into_iter()
            .map(|state| {
                let rows = self.rows.iter().filter(|row| row.state == state).count() as u64;
                (state, rows)
            })
            .collect())
    }

    async fn record_dispatch_state(&self, _state: &cog_core::DispatchState) -> SFResult<()> {
        unimplemented!("the read route does not write the dispatch state")
    }
}

/// The quota store the state has a slot for, with no store behind it.
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

/// The gateway state the router is built over. Everything is absent except the
/// account under test.
fn state(change_ledger: Option<Arc<FakeLedger>>) -> Arc<GatewayState> {
    let jwt_manager: Arc<dyn cog_core::AuthProvider> =
        Arc::new(JwtManager::new(JwtConfig::default()));
    let raw_logger: Arc<dyn cog_core::RawLogger> = Arc::new(cog_storage::MemoryRawLogger::new());
    let dag_executor = Arc::new(cog_orchestrator::DagExecutor::new("test-workspace".into()));
    let orchestrator = Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag_executor));
    let state_backend: Arc<dyn cog_core::StateBackend> =
        Arc::new(cog_storage::MemoryStateBackend::new());

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
        observability_gateway: Some(Arc::new(cog_storage::MemoryObservabilityGateway::new(
            state_backend,
        ))),
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
        change_ledger: change_ledger.map(|ledger| ledger as Arc<dyn ChangeLedgerStore>),
        key_value: None,
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

/// A token this router's own signer accepts, so a request reaches the handler
/// instead of stopping at the layer that answers anonymous ones.
async fn bearer(state: &Arc<GatewayState>) -> String {
    let user = User {
        id: uuid::Uuid::new_v4(),
        phone: None,
        email: None,
        username: "change-ledger-probe".into(),
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

async fn get(
    state: Arc<GatewayState>,
    path: &str,
    token: Option<&str>,
) -> (StatusCode, serde_json::Value) {
    let mut request = Request::builder().method("GET").uri(path);
    if let Some(token) = token {
        request = request.header("Authorization", format!("Bearer {token}"));
    }
    let response = create_router(state)
        .oneshot(
            request
                .body(Body::empty())
                .expect("the request is well formed"),
        )
        .await
        .expect("the router answers a request");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("the body is readable");
    let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

/// The route answers the same rows to the deployment's own operator.
#[tokio::test]
async fn the_account_is_readable_with_a_token() {
    let ledger = Arc::new(FakeLedger::new(
        vec![
            row(ChangeState::Pending, "doc-a.md", "crates/cog-gateway/src"),
            row(ChangeState::Landed, "doc-b.md", "crates/cog-storage/src"),
        ],
        0,
        0,
    ));
    let state = state(Some(ledger));
    let token = bearer(&state).await;

    let (status, body) = get(state, "/api/v1/governance/changes", Some(&token)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2, "{body}");
}

/// Every state is a cell even when it holds nothing, and the two totals are
/// taken over the census rather than over the page.
#[tokio::test]
async fn an_empty_state_is_still_a_cell() {
    let ledger = Arc::new(FakeLedger::new(
        vec![row(
            ChangeState::Landed,
            "doc-a.md",
            "crates/cog-gateway/src",
        )],
        3,
        1,
    ));
    let state = state(Some(ledger));
    let token = bearer(&state).await;

    let (status, body) = get(state, "/api/v1/governance/changes", Some(&token)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let census = body["census"].as_array().expect("census is a list");
    assert_eq!(census.len(), 6, "every state is a cell: {body}");
    let pending = census
        .iter()
        .find(|cell| cell["state"] == "pending")
        .expect("pending is a cell even with no rows");
    assert_eq!(pending["rows"], 0, "{body}");
    assert_eq!(pending["outstanding"], true, "{body}");
    let landed = census
        .iter()
        .find(|cell| cell["state"] == "landed")
        .expect("landed is a cell");
    assert_eq!(landed["rows"], 1, "{body}");
    assert_eq!(landed["outstanding"], false, "{body}");
    assert_eq!(body["outstanding_total"], 0, "nothing is owed: {body}");

    let bands = body["owed_by_band"].as_array().expect("bands are a list");
    assert_eq!(bands.len(), 5, "every band is a cell: {body}");
    assert!(
        bands.iter().all(|band| band["rows"] == 0),
        "the only row is landed: {body}"
    );

    assert_eq!(body["pending_declarations"], 3, "{body}");
    assert_eq!(body["pending_retirements"], 1, "{body}");
}

/// The backlog is a count over the whole account, not the length of the list
/// the route prints. A page of 100 out of 150 outstanding rows is a page; the
/// number a reader sizes the work by is the 150.
#[tokio::test]
async fn the_outstanding_total_is_not_the_page_length() {
    let rows: Vec<ChangeLedgerRow> = (0..150)
        .map(|i| {
            row(
                ChangeState::Pending,
                &format!("doc-{i}.md"),
                "crates/cog-gateway/src",
            )
        })
        .collect();
    let ledger = Arc::new(FakeLedger::new(rows, 0, 0));
    let state = state(Some(ledger));
    let token = bearer(&state).await;

    let (status, body) = get(state, "/api/v1/governance/changes", Some(&token)).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    let page = body["outstanding"].as_array().expect("rows are a list");
    assert_eq!(page.len(), 100, "the page is bounded: {body}");
    assert_eq!(
        body["outstanding_total"], 150,
        "the total is the account, not the page: {body}"
    );
    assert_eq!(
        body["owed_by_band"]
            .as_array()
            .expect("bands are a list")
            .iter()
            .map(|band| band["rows"].as_u64().unwrap_or(0))
            .sum::<u64>(),
        150,
        "the band counts are the same population as the total: {body}"
    );
    assert_eq!(body["total"], 150, "{body}");
}

/// An anonymous caller is refused before the account is read.
#[tokio::test]
async fn the_route_is_not_public() {
    let ledger = Arc::new(FakeLedger::new(Vec::new(), 0, 0));
    let state = state(Some(ledger));

    let (status, _body) = get(state, "/api/v1/governance/changes", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// No account is not an empty account. Answering zeros here would print the
/// same thing a deployment with nothing owed prints, and the two want opposite
/// work.
#[tokio::test]
async fn without_a_ledger_the_route_says_so() {
    let state = state(None);
    let token = bearer(&state).await;

    let (status, body) = get(state, "/api/v1/governance/changes", Some(&token)).await;

    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "no ledger is not zero rows: {body}"
    );
}
