//! The governance read face answers from the roster it is given, and a task
//! that names a face is checked when it is submitted.
//!
//! The handlers are thin, and that is the reason this file exists: a thin
//! handler is easy to write and easy to leave unwired. `resolve_bindings` has
//! its own unit tests, but a unit test of a function nothing calls is a test of
//! a function nothing calls -- the router is what decides whether the check
//! ever runs. So each case here goes through the router with a store behind it
//! and asks what a caller would see.
//!
//! The store is a stand-in that holds face rows in memory. That is enough for
//! both questions: what the read face prints is exactly what the store returns,
//! and whether the write face consults the store is visible in the answer it
//! gives to a name the store does not have.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    FaceKind, FaceSpec, GatewayConfig, GovernanceStore, PreCheckResult, QuotaManager, QuotaSummary,
    SFResult, Task, TaskStatus, User, UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};

/// A roster with no table behind it: the face rows it is handed, and nothing else.
///
/// The write methods are no-ops rather than errors, because none of the cases
/// below is about what a write returns; a refusal here would be a statement
/// about this stand-in and would show up as a failure of the read it is not
/// holding.
struct Roster {
    faces: tokio::sync::Mutex<Vec<Task>>,
}

impl Roster {
    fn new(faces: Vec<Task>) -> Self {
        Self {
            faces: tokio::sync::Mutex::new(faces),
        }
    }
}

#[async_trait::async_trait]
impl GovernanceStore for Roster {
    async fn face_nodes(&self) -> SFResult<Vec<Task>> {
        Ok(self.faces.lock().await.clone())
    }

    async fn open_face_bound_tasks(&self) -> SFResult<Vec<Task>> {
        Ok(Vec::new())
    }

    async fn save_face_node(&self, task: &Task) -> SFResult<()> {
        let mut faces = self.faces.lock().await;
        match faces.iter_mut().find(|face| face.id == task.id) {
            Some(existing) => *existing = task.clone(),
            None => faces.push(task.clone()),
        }
        Ok(())
    }

    async fn save_face_node_if_revision(&self, task: &Task, _expected: i64) -> SFResult<bool> {
        self.save_face_node(task).await?;
        Ok(true)
    }

    async fn governance_nodes(&self, _task_type: &str) -> SFResult<Vec<Task>> {
        Ok(Vec::new())
    }

    async fn save_governance_node(&self, _task: &Task) -> SFResult<()> {
        Ok(())
    }

    async fn save_governance_node_if_status(
        &self,
        _task: &Task,
        _expected: &[TaskStatus],
    ) -> SFResult<bool> {
        Ok(true)
    }

    async fn save_governance_node_with_event(
        &self,
        _event: &Task,
        _node: &Task,
        _expected: &[TaskStatus],
    ) -> SFResult<bool> {
        Ok(true)
    }

    async fn apply_handover(
        &self,
        _handover: &Task,
        _face: &Task,
        _expected_revision: i64,
    ) -> SFResult<Option<usize>> {
        Ok(None)
    }
}

fn face(face_id: &str, owner: &str, kind: FaceKind) -> Task {
    cog_core::Face {
        face_id: face_id.into(),
        owner_unit: Some(owner.into()),
        bound_by_handover_id: None,
        spec: FaceSpec::first(face_id, kind),
        updated_at: chrono::Utc::now(),
    }
    .to_task(None)
}

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

/// The gateway state the router is built over. Every component is absent or a
/// stand-in except the one under test.
fn state(governance_store: Option<Arc<dyn GovernanceStore>>) -> Arc<GatewayState> {
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
        governance_store,
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
    })
}

/// A token this router's own signer accepts, so a request reaches a handler
/// instead of stopping at the layer that answers anonymous ones.
async fn bearer(state: &Arc<GatewayState>) -> String {
    let user = User {
        id: uuid::Uuid::new_v4(),
        phone: None,
        email: None,
        username: "governance-route-probe".into(),
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

async fn ask(
    app: &axum::Router,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .method(method)
        .uri(path)
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_owned()))
        .expect("the request is well formed");
    let response = app
        .clone()
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

/// The roster route prints the rows it was given, with the counts that separate
/// faces from the lines that cut across them. Two counts rather than a total:
/// a table that lost a face and gained a line still adds up to the same number.
#[tokio::test]
async fn the_face_roster_is_what_the_store_holds() {
    let store: Arc<dyn GovernanceStore> = Arc::new(Roster::new(vec![
        face("任务管理", "c2", FaceKind::Face),
        face("提交门", "变更", FaceKind::Cross),
    ]));
    let state = state(Some(store));
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    let (status, body) = ask(&app, "GET", "/api/v1/governance/faces", &token, "{}").await;
    assert_eq!(status, StatusCode::OK, "the roster route answers: {body}");
    assert_eq!(body["census"]["faces"], 1);
    assert_eq!(body["census"]["cross"], 1);
    let ids: Vec<&str> = body["faces"]
        .as_array()
        .expect("faces is an array")
        .iter()
        .map(|face| face["face_id"].as_str().expect("a face has an id"))
        .collect();
    assert!(
        ids.contains(&"任务管理") && ids.contains(&"提交门"),
        "{ids:?}"
    );
}

/// The drift route recomputes from the roster rather than serving a stored
/// verdict, so its answer names every face in the store.
#[tokio::test]
async fn the_drift_reading_is_computed_for_every_face() {
    let store: Arc<dyn GovernanceStore> = Arc::new(Roster::new(vec![
        face("任务管理", "c2", FaceKind::Face),
        face("提交门", "变更", FaceKind::Cross),
    ]));
    let state = state(Some(store));
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    let (status, body) = ask(&app, "GET", "/api/v1/governance/drift", &token, "{}").await;
    assert_eq!(status, StatusCode::OK, "the drift route answers: {body}");
    assert_eq!(
        body["readings"].as_array().map(|rows| rows.len()),
        Some(2),
        "one reading per face: {body}"
    );
}

/// With no shared database there is no roster, and the route says so. Answering
/// an empty list instead would read as "no faces", which is the one verdict a
/// missing store must never produce.
#[tokio::test]
async fn a_missing_store_is_unavailable_rather_than_an_empty_roster() {
    let state = state(None);
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    let (status, _) = ask(&app, "GET", "/api/v1/governance/faces", &token, "{}").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

fn submission(face_id: &str, unit_id: Option<&str>) -> String {
    serde_json::json!({
        "goal": "bound row",
        "workspace_id": "test-workspace",
        "intent_kind": "task",
        "tasks": [{
            "id": "t1",
            "task_type": "dag_node",
            "input": {},
            "planner": {"verified": true},
            "face_id": face_id,
            "unit_id": unit_id,
        }],
    })
    .to_string()
}

/// The submission route asks the roster before it queues anything. A name the
/// roster does not have, and a unit that contradicts the face's owner, are both
/// refused at the door -- and the control row, which names a face that does
/// exist with its own owner, is not refused. Without that control the test
/// would pass on a handler that refuses every submission.
#[tokio::test]
async fn a_submission_naming_a_face_is_checked_against_the_roster() {
    let store: Arc<dyn GovernanceStore> =
        Arc::new(Roster::new(vec![face("任务管理", "c2", FaceKind::Face)]));
    let state = state(Some(store));
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    let (unknown, body) = ask(
        &app,
        "POST",
        "/api/v1/tasks",
        &token,
        &submission("不存在的面", None),
    )
    .await;
    assert_eq!(unknown, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "owner_mismatch", "{body}");

    let (mismatch, body) = ask(
        &app,
        "POST",
        "/api/v1/tasks",
        &token,
        &submission("任务管理", Some("not-c2")),
    )
    .await;
    assert_eq!(mismatch, StatusCode::CONFLICT, "{body}");
    assert_eq!(body["error"], "owner_mismatch", "{body}");

    let (accepted, body) = ask(
        &app,
        "POST",
        "/api/v1/tasks",
        &token,
        &submission("任务管理", Some("c2")),
    )
    .await;
    assert_ne!(
        accepted,
        StatusCode::CONFLICT,
        "a row naming the face's own owner is not a mismatch: {body}"
    );
}

/// A goal's rows are replaced by what the planner returns, so a binding written
/// on one never reaches the graph. Accepting it tells the submitter the work was
/// bound to a face that will never see it.
#[tokio::test]
async fn a_binding_on_a_row_the_planner_will_replace_is_refused() {
    let store: Arc<dyn GovernanceStore> =
        Arc::new(Roster::new(vec![face("任务管理", "c2", FaceKind::Face)]));
    let state = state(Some(store));
    let app = create_router(state.clone());
    let token = bearer(&state).await;

    let mut body: serde_json::Value =
        serde_json::from_str(&submission("任务管理", Some("c2"))).expect("valid request");
    body["intent_kind"] = serde_json::json!("goal");

    let (status, answer) = ask(&app, "POST", "/api/v1/tasks", &token, &body.to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{answer}");
}
