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

use crate::common;

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    AgentRegistration, AgentRegistry, GatewayConfig, KeyValueStore, PreCheckResult, QuotaManager,
    QuotaSummary, ResourceInfo, SFResult, StateBackend, User, UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};
use cog_storage::{MemoryAgentRegistry, MemoryObservabilityGateway, MemoryStateBackend};
use common::orchestrator::control;

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
    state_with(agent_registry, None)
}

fn state_with(
    agent_registry: Option<Arc<MemoryAgentRegistry>>,
    key_value: Option<Arc<FakeKeyValue>>,
) -> Arc<GatewayState> {
    let jwt_manager: Arc<dyn cog_core::AuthProvider> =
        Arc::new(JwtManager::new(JwtConfig::default()));
    let raw_logger: Arc<dyn cog_core::RawLogger> = Arc::new(cog_storage::MemoryRawLogger::new());
    let orchestrator = control("test-workspace");
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
        change_ledger: None,
        key_value: key_value.map(|kv| kv as Arc<dyn cog_core::KeyValueStore>),
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
        llm_usage: None,
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

/// A key-value face holding whatever the case put in it.
///
/// Only `get` and `set` are implemented: the reading under test reads one key,
/// and a stand-in that quietly grew a second behaviour would make the route
/// look exercised on a path nothing calls.
#[derive(Default)]
struct FakeKeyValue {
    values: std::sync::Mutex<std::collections::HashMap<String, String>>,
}

#[async_trait::async_trait]
impl cog_core::KeyValueStore for FakeKeyValue {
    async fn get(&self, key: &str) -> SFResult<Option<String>> {
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    async fn set(&self, key: &str, value: &str) -> SFResult<()> {
        self.values
            .lock()
            .unwrap()
            .insert(key.to_string(), value.to_string());
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &str, _ttl_secs: u64) -> SFResult<()> {
        self.set(key, value).await
    }

    async fn del(&self, _key: &str) -> SFResult<()> {
        unimplemented!("the pool reading does not delete")
    }

    async fn expire(&self, _key: &str, _ttl_secs: i64) -> SFResult<()> {
        unimplemented!("the pool reading does not set expiry")
    }

    async fn ttl(&self, _key: &str) -> SFResult<i64> {
        unimplemented!("the pool reading does not ask for a TTL")
    }

    async fn incr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!("the pool reading does not count")
    }

    async fn decr(&self, _key: &str, _by: i64) -> SFResult<i64> {
        unimplemented!("the pool reading does not count")
    }

    async fn incr_ex(&self, _key: &str, _by: i64, _ttl_secs: u64) -> SFResult<i64> {
        unimplemented!("the pool reading does not count")
    }

    async fn sadd(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!("the pool reading does not touch sets")
    }

    async fn srem(&self, _key: &str, _member: &str) -> SFResult<()> {
        unimplemented!("the pool reading does not touch sets")
    }

    async fn smembers(&self, _key: &str) -> SFResult<Vec<String>> {
        unimplemented!("the pool reading does not touch sets")
    }

    async fn hset(&self, _key: &str, _field: &str, _value: &str) -> SFResult<()> {
        unimplemented!("the pool reading does not touch hashes")
    }

    async fn hget(&self, _key: &str, _field: &str) -> SFResult<Option<String>> {
        unimplemented!("the pool reading does not touch hashes")
    }

    async fn hdel(&self, _key: &str, _field: &str) -> SFResult<()> {
        unimplemented!("the pool reading does not touch hashes")
    }

    async fn hvals(&self, _key: &str) -> SFResult<Vec<String>> {
        unimplemented!("the pool reading does not touch hashes")
    }

    async fn hgetall(&self, _key: &str) -> SFResult<Vec<(String, String)>> {
        unimplemented!("the pool reading does not touch hashes")
    }

    async fn hlen(&self, _key: &str) -> SFResult<u64> {
        unimplemented!("the pool reading does not touch hashes")
    }
}

/// The pool's own verdict reaches the route, including when it comes back.
///
/// The two instants are the whole point of publishing this: an outage whose
/// only statement is "not available" leaves the operator with no bound on how
/// long to wait, and one upstream here states no instant at all — only the
/// length of its window.
#[tokio::test]
async fn the_pool_verdict_is_the_snapshot_and_not_a_recomputation() {
    let kv = Arc::new(FakeKeyValue::default());
    let snapshot = serde_json::json!({
        "unavailable": true,
        "unavailable_upstreams": ["https://a.example/v1|model-a", "https://b.example/v1|model-b"],
        "pool_size": 2,
        "declared_min_healthy": 1,
        "evidenced_recovery_unix": 1_793_807_999,
        "next_attempt_unix": 1_792_839_444,
        "quota_window_secs": 2_592_000,
        "last_success_unix": 1_792_800_000,
    });
    kv.set(cog_core::LLM_POOL_STATUS_KEY, &snapshot.to_string())
        .await
        .expect("the stand-in takes the snapshot");

    let (status, body) = overview(state_with(None, Some(kv))).await;

    assert_eq!(status, StatusCode::OK, "the route answers: {body}");
    let pool = &body["llm_pool"];
    assert_eq!(pool["available"], false, "{body}");
    assert_eq!(pool["pool_size"], 2, "{body}");
    assert_eq!(pool["min_healthy"], 1, "{body}");
    assert_eq!(pool["unavailable_count"], 2, "{body}");
    assert_eq!(pool["quota_window_secs"], 2_592_000, "{body}");
    assert!(
        pool["recovery_at"].is_string(),
        "the stated reset instant is the reading that bounds the outage: {body}"
    );
    assert!(pool["next_attempt_at"].is_string(), "{body}");
    assert!(pool["last_served_at"].is_string(), "{body}");
    assert!(
        !pool.to_string().contains("example"),
        "upstream identities stay on the gateway that holds the credentials: {body}"
    );
}

/// A snapshot nobody published is not a healthy pool. The writer holds the
/// key under a TTL, so an absent key means the writer is gone, and a
/// fabricated "available" would send the operator looking elsewhere.
#[tokio::test]
async fn no_snapshot_is_unknown_not_available() {
    let kv = Arc::new(FakeKeyValue::default());

    let (status, body) = overview(state_with(None, Some(kv))).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["llm_pool"].is_null(),
        "nothing to read is not a verdict: {body}"
    );
}

/// The window's allowance account has to reach the route, both ways round.
///
/// The projection has its own test; this one answers the question that test
/// cannot: does the reading a person opens carry the account at all? Between
/// the snapshot and the wire there is a projection and a serialization, and a
/// dropped field fails neither of them. The spend faces answer how much went
/// out; without this half nobody can say how much the window had, so a window
/// nearly gone and one barely touched read the same everywhere else.
///
/// The empty case is judged too, because the wrong zero is worse than a
/// missing number: a pace of zeros says the window allows nothing, while the
/// truth is that no upstream has been measured refusing a spent window yet.
#[tokio::test]
async fn the_allowance_account_reaches_the_route_and_reads_unmeasured_when_it_never_was() {
    let kv = Arc::new(FakeKeyValue::default());
    let snapshot = serde_json::json!({
        "unavailable": false,
        "unavailable_upstreams": [],
        "pool_size": 1,
        "evidenced_recovery_unix": 0,
        "next_attempt_unix": 0,
        "quota_window_secs": 2_592_000,
        "last_success_unix": 1_792_800_000,
        "pace": {
            "measured_upstreams": 1,
            "allowance_tokens": 16_620_054,
            "spent_tokens": 4_000_000,
            "reserve_tokens": 358_943,
            "reserve_population": 533,
            "window_secs": 2_592_000,
            "window_start_unix": 1_793_028_477,
        },
    });
    kv.set(cog_core::LLM_POOL_STATUS_KEY, &snapshot.to_string())
        .await
        .expect("the stand-in takes the snapshot");

    let (status, body) = overview(state_with(None, Some(kv))).await;

    assert_eq!(status, StatusCode::OK, "the route answers: {body}");
    let pace = &body["llm_pool"]["pace"];
    assert_eq!(pace["allowance_tokens"], 16_620_054, "{body}");
    assert_eq!(pace["spent_tokens"], 4_000_000, "{body}");
    assert_eq!(pace["reserve_tokens"], 358_943, "{body}");
    assert_eq!(
        pace["reserve_population"], 533,
        "一份预留量要带着它测自几个整活，否则读者分不清五个样本和五百个: {body}"
    );
    assert_eq!(pace["window_secs"], 2_592_000, "{body}");
    assert!(
        pace["window_start_at"].is_string(),
        "窗起点是读者算出「还有多久换窗」的唯一依据: {body}"
    );

    // 没有量过整窗的快照：默认值解析成 `measured_upstreams: 0`。
    let unmeasured = Arc::new(FakeKeyValue::default());
    let snapshot = serde_json::json!({
        "unavailable": false,
        "unavailable_upstreams": [],
        "pool_size": 1,
        "evidenced_recovery_unix": 0,
        "next_attempt_unix": 0,
        "quota_window_secs": 0,
        "last_success_unix": 0,
    });
    unmeasured
        .set(cog_core::LLM_POOL_STATUS_KEY, &snapshot.to_string())
        .await
        .expect("the stand-in takes the snapshot");

    let (status, body) = overview(state_with(None, Some(unmeasured))).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["llm_pool"]["pace"].is_null(),
        "没量过就是没有读数，不能是一窗零额度: {body}"
    );
}
