//! End-to-end integration tests for the cogneva operational pipeline.
//! These tests exercise the full stack:
//!   HTTP → Gateway → Auth → Quota → DagExecutor → SquadExecutor
//!   → RawLogger → TierMigrator → RawLogIndexStore → MetricsBackend
//! Each test constructs a fresh `GatewayState` backed by memory-only
//! implementations so no external DB or filesystem persistence is required
//! (except for the transient raw-log temp dir).

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use chrono::Utc;
use redis::AsyncCommands;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tower::ServiceExt;

use cog_auth::jwt::{JwtConfig, JwtManager};
use cog_core::{
    GatewayConfig, MetricsBackend, ObjectBackend, RawLogger, ShutdownSignal, TierPolicy, User,
    UserStatus, UserType,
};
use cog_gateway::{create_router, GatewayState};
use cog_memory::{
    IngestionPipeline, MemoryMemoryBackend, MetricsInstrumentedMemoryBackend, RuleBasedExtractor,
};
use cog_quota::QuotaManager;
use cog_storage::{
    MemoryMetricsBackend, MemoryObjectBackend, MemoryRawLogIndexStore, MemoryRawLogger,
    TierMigrator,
};

// ─── Test harness ────────────────────────────────────────────────────

/// A fully wired test app. Lives inside the same process so we can
/// inspect internal state (logger records, metrics samples, …).
struct TestApp {
    state: Arc<GatewayState>,
    raw_logger: Arc<MemoryRawLogger>,
    metrics: Arc<dyn MetricsBackend>,
    #[allow(dead_code)]
    index_store: Arc<MemoryRawLogIndexStore>,
    #[allow(dead_code)]
    object_backend: Arc<dyn ObjectBackend>,
    shutdown: ShutdownSignal,
    addr: SocketAddr,
    _server: tokio::task::JoinHandle<()>,
    _migrator: Option<tokio::task::JoinHandle<()>>,
}

async fn spawn_app(with_tier_migrator: bool, raw_log_dir: Option<&std::path::Path>) -> TestApp {
    spawn_app_full(with_tier_migrator, raw_log_dir, true).await
}

async fn spawn_app_full(
    with_tier_migrator: bool,
    raw_log_dir: Option<&std::path::Path>,
    with_raw_log_index: bool,
) -> TestApp {
    // Redis is required for the quota manager.
    let redis_client = redis::Client::open("redis://127.0.0.1:6379").expect("redis open");
    let redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");

    let jwt_manager: Arc<dyn cog_core::AuthProvider> =
        Arc::new(JwtManager::new(JwtConfig::default()));
    // Generous default so a single request doesn't exhaust quota during E2E.
    let quota_manager = Arc::new(QuotaManager::new(redis_conn, 1_000_000_000));

    let raw_logger = Arc::new(MemoryRawLogger::new());
    let raw_logger_clone: Arc<dyn RawLogger> = raw_logger.clone();

    let metrics: Arc<dyn MetricsBackend> = Arc::new(MemoryMetricsBackend::new());
    let inner_mem: Arc<dyn cog_core::MemoryBackend> = Arc::new(MemoryMemoryBackend::new());
    let memory_backend = Arc::new(MetricsInstrumentedMemoryBackend::new(
        inner_mem,
        metrics.clone(),
    ));

    let index_store = Arc::new(MemoryRawLogIndexStore::new());

    let dag_executor = Arc::new(cog_orchestrator::DagExecutor::new("test-workspace".into()));
    let orchestrator = Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag_executor));

    let (task_event_tx, _task_event_rx) = broadcast::channel::<cog_core::TaskEvent>(16);

    let gateway_state = Arc::new(GatewayState {
        data_dir: "/tmp".into(),
        config: std::sync::RwLock::new(GatewayConfig {
            http_port: 0, // random
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
        task_event_tx,
        jwt_manager: jwt_manager.clone(),
        quota_manager: quota_manager.clone(),
        hierarchy_manager: None,
        action_plan_store: Default::default(),
        collaboration_graph: None,
        raw_logger: raw_logger_clone,
        memory_backend: Some(memory_backend.clone()),
        memory_ingestor: Some(Arc::new(IngestionPipeline::new(RuleBasedExtractor::new()))),
        metrics_backend: Some(metrics.clone()),
        metrics_exporter: None,
        search_backend: None,
        raw_log_index_store: if with_raw_log_index {
            Some(index_store.clone())
        } else {
            None
        },
        hook_archive: None,
        hook_engine: None,

        orchestrator: orchestrator.clone(),
        task_executors: Arc::new(cog_orchestrator::TaskExecutorRouter::new()),
        agent_registry: None,
        observability_gateway: None,
        connection_manager: None,
        wiki_adapter: None,
        user_store: None,
        login_rate_limiter: None,
        session_manager: None,
        heartbeat_history: None,
        snapshot_store: None,
        object_backend: None,
        notification_store: None,
        supervisor: None,
        alert_store: None,
        backend_health_probe: None,
        trace_store: None,
        replay_engine: None,
        sandbox_backend: None,
        plugin_registry: None,
        guardrail: None,
        eval_service: None,
        observables: Vec::new(),
        mcp_client: None,
        workspace_store: None,
        external_skill_registry: None,
        notification_dispatcher: None,
        notification_tx: broadcast::channel(16).0,
        agent_pool: None,
        event_publisher: None,
        media_backend: None,
        websocket_client: None,
        evolution_admin: None,
        audit_stream: None,
        llm_client: std::sync::Arc::new(std::sync::RwLock::new(None)),
        chat_sessions: std::sync::Arc::new(tokio::sync::Mutex::new(
            std::collections::HashMap::new(),
        )),
    });

    let app = create_router(gateway_state.clone());

    // Bind a random localhost port so we can test via real HTTP.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let shutdown = ShutdownSignal::new();
    let shutdown_signal = shutdown.clone();

    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown_signal.wait().await })
            .await
            .unwrap();
    });

    // Optionally start TierMigrator pointed at a temp dir.
    let _migrator = if with_tier_migrator {
        let base_dir = raw_log_dir.unwrap().to_path_buf();
        let object_backend: Arc<dyn ObjectBackend> = Arc::new(MemoryObjectBackend::new());
        let migrator = TierMigrator::new(
            &base_dir,
            TierPolicy {
                hot_duration: Duration::from_secs(1), // fast for tests
                warm_duration: Duration::from_secs(60),
                scan_interval: Duration::from_secs(1),
                ..TierPolicy::default()
            },
            object_backend.clone(),
            index_store.clone(),
        )
        .with_metrics(metrics.clone());
        Some(Arc::new(migrator).spawn(shutdown.clone()))
    } else {
        None
    };

    TestApp {
        state: gateway_state,
        raw_logger,
        metrics,
        index_store,
        object_backend: Arc::new(MemoryObjectBackend::new()),
        shutdown,
        addr,
        _server: server,
        _migrator,
    }
}

impl TestApp {
    async fn bearer_token(&self) -> String {
        let user = User {
            id: uuid::Uuid::new_v4(),
            phone: None,
            email: None,
            username: "e2e-tester".into(),
            display_name: None,
            avatar_url: None,
            status: UserStatus::Active,
            user_type: UserType::Admin,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let token_pair = self
            .state
            .jwt_manager
            .generate_token(&user, vec![], vec![])
            .await
            .unwrap();
        let access = token_pair.access_token;
        format!("Bearer {}", access)
    }
}

// Helper: send an HTTP request via oneshot (no real TCP, uses the router).
// Slightly faster than real TCP and works when the listener is not yet ready.
async fn req_oneshot(
    state: Arc<GatewayState>,
    method: &str,
    uri: &str,
    body: Body,
    auth: Option<String>,
) -> (StatusCode, serde_json::Value) {
    let app = create_router(state);
    let mut builder = Request::builder().method(method).uri(uri);
    if method == "POST" || method == "PUT" || method == "PATCH" {
        builder = builder.header("Content-Type", "application/json");
    }
    if let Some(token) = auth {
        builder = builder.header("Authorization", token);
    }
    let res = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or_else(|_| {
        serde_json::Value::String(String::from_utf8_lossy(&bytes).into_owned())
    });
    (status, json)
}

// ─── Test 1: Health + Liveness (baseline) ────────────────────────────

#[tokio::test]
async fn e2e_health_endpoints() {
    let app = spawn_app(false, None).await;

    let client = reqwest::Client::new();
    let res = client
        .get(format!("http://{}/health", app.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "OK");

    let res = client
        .get(format!("http://{}/health/ready", app.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["status"], "ready");

    app.shutdown.trigger();
}

// ─── Test 2: Auth → Quota → Task creation flow ───────────────────────

#[tokio::test]
async fn e2e_task_create_then_list() {
    let app = spawn_app(false, None).await;

    let (status, body) = req_oneshot(
        app.state.clone(),
        "POST",
        "/api/v1/tasks",
        Body::from(
            serde_json::json!({
                "goal": "e2e integration",
                "tasks": [
                    {
                        "id": "e2e-task-1",
                        "task_type": "echo",
                        "input": {"payload": "hello e2e"},
                        "blocked_by": [],
                        "priority": 1
                    }
                ]
            })
            .to_string(),
        ),
        Some(app.bearer_token().await),
    )
    .await;

    assert!(
        status.is_success(),
        "expected success status, got {}",
        status.as_u16()
    );
    assert_eq!(body["goal"], "e2e integration");

    // List tasks
    let (status, body) = req_oneshot(
        app.state.clone(),
        "GET",
        "/api/v1/tasks/list",
        Body::empty(),
        Some(app.bearer_token().await),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let tasks = body.as_array().expect("tasks array");
    assert!(
        tasks.iter().any(|t| t["id"] == "e2e-task-1"),
        "e2e-task-1 should appear in list"
    );

    app.shutdown.trigger();
}

// ─── Test 3: Raw logger records HTTP traffic ─────────────────────────

#[tokio::test]
async fn e2e_raw_logger_records_http_requests() {
    let app = spawn_app(false, None).await;

    // Trigger some HTTP traffic.
    let _ = req_oneshot(app.state.clone(), "GET", "/health", Body::empty(), None).await;

    // The RawLogger is MemoryRawLogger; inspect its records.
    let records = app.raw_logger.all_records().unwrap();
    assert!(
        !records.is_empty(),
        "expected at least one raw record from the http middleware"
    );
    let http_records: Vec<_> = records
        .into_iter()
        .filter(|r| r.meta.stream == "transport_raw")
        .collect();
    assert!(
        !http_records.is_empty(),
        "expected transport_raw stream record"
    );

    app.shutdown.trigger();
}

// ─── Test 4: TierMigrator promotes + /raw_logs query ─────────────────

#[tokio::test]
async fn e2e_tier_migration_and_raw_logs_query() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = spawn_app(true, Some(dir.path())).await;

    // The tier migrator is already running on a 1-second scan interval.
    // Manually drop a "hot" file that is older than hot_duration (1s).
    let stream_dir = dir.path().join("transport_raw");
    std::fs::create_dir_all(&stream_dir).unwrap();
    let log_path = stream_dir.join("2026-03-01.jsonl");
    std::fs::write(
        &log_path,
        b"{\"meta\":{\"version\":\"1.0\",\"stream\":\"transport_raw\",\"recorded_at\":\"2026-03-01T00:00:00Z\",\"recorded_by\":\"cog-gateway\",\"sequence\":0,\"trace_id\":\"test\"},\"context\":{},\"payload\":{\"direction\":\"inbound\",\"transport\":\"http\"}}\n",
    ).unwrap();
    let old = SystemTime::now() - Duration::from_secs(5);
    filetime::set_file_mtime(&log_path, filetime::FileTime::from_system_time(old)).unwrap();

    // Wait for the migrator to pick it up.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Query via the gateway API.
    let client = reqwest::Client::new();
    let res = client
        .get(format!(
            "http://{}/api/v1/raw_logs?stream=transport_raw&start=2026-02-28T00:00:00Z",
            app.addr
        ))
        .header("Authorization", app.bearer_token().await)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let json: serde_json::Value = res.json().await.unwrap();
    assert_eq!(json["count"], 1, "expected one warm-tier entry: {}", json);

    app.shutdown.trigger();
}

// ─── Test 5: Metrics counter for tier migration ──────────────────────

#[tokio::test]
async fn e2e_tier_migration_increments_prometheus_counter() {
    let dir = tempfile::TempDir::new().unwrap();
    let app = spawn_app(true, Some(dir.path())).await;

    // Seed an old file to trigger migration.
    let stream_dir = dir.path().join("transport_raw");
    std::fs::create_dir_all(&stream_dir).unwrap();
    let log_path = stream_dir.join("2026-03-01.jsonl");
    std::fs::write(&log_path, b"{\"test\":true}\n").unwrap();
    let old = SystemTime::now() - Duration::from_secs(5);
    filetime::set_file_mtime(&log_path, filetime::FileTime::from_system_time(old)).unwrap();

    tokio::time::sleep(Duration::from_secs(2)).await;

    // Query the MemoryMetricsBackend directly — the /metrics handler only
    // renders a fixed set of series, whereas the backend stores everything.
    let from = Utc::now() - chrono::Duration::seconds(60);
    let to = Utc::now() + chrono::Duration::seconds(60);
    let samples = app
        .metrics
        .query_counter_range("tier_migration_total", from, to)
        .await
        .unwrap();
    let total: f64 = samples.iter().map(|s| s.value).sum();
    assert!(
        total >= 1.0,
        "expected tier_migration_total >= 1 in backend, got {}",
        total
    );

    app.shutdown.trigger();
}

// ─── Test 6: Quota exceeded returns 429 ──────────────────────────────

#[tokio::test]
async fn e2e_quota_exceeded_returns_429() {
    let app = spawn_app(false, None).await;

    // Drain the specific user's quota by directly setting the Redis key to 0.
    // (Using pre_check to drain is unreliable because Redis DECR can produce
    // negative values that break the u64 read-back in the manager.)
    let user_id = uuid::Uuid::new_v4().to_string();
    {
        let redis_client = redis::Client::open("redis://127.0.0.1:6379").unwrap();
        let mut conn = redis_client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let key = format!("quota:remaining:{}", user_id);
        let _: () = conn.set_ex(&key, 0u64, 60u64).await.unwrap();
    }

    // Build a token for that exact user so the middleware matches user_id.
    let user = User {
        id: uuid::Uuid::parse_str(&user_id).unwrap(),
        phone: None,
        email: None,
        username: "e2e-tester".into(),
        display_name: None,
        avatar_url: None,
        status: UserStatus::Active,
        user_type: UserType::Standard,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let token_pair = app
        .state
        .jwt_manager
        .generate_token(&user, vec![], vec![])
        .await
        .unwrap();
    let access = token_pair.access_token;
    let token = format!("Bearer {}", access);

    let (status, _) = req_oneshot(
        app.state.clone(),
        "GET",
        "/api/v1/quota",
        Body::empty(),
        Some(token),
    )
    .await;

    assert_eq!(
        status,
        StatusCode::TOO_MANY_REQUESTS,
        "expected 429 when quota exhausted"
    );

    app.shutdown.trigger();
}

// ─── Test 7: Memory ingest round-trip ────────────────────────────────

#[tokio::test]
async fn e2e_memory_ingest_and_search() {
    let app = spawn_app(false, None).await;

    let token = app.bearer_token().await;
    let client = reqwest::Client::new();

    // Ingest
    let res = client
        .post(format!("http://{}/api/v1/memory/ingest", app.addr))
        .header("Authorization", &token)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({
                "id": "e2e-conv-1",
                "text": "Meeting notes:\n@entity:Alice\n@entity:Bob",
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let ingest_status = res.status();
    let ingest_body = res.text().await.unwrap();
    assert_eq!(
        ingest_status,
        StatusCode::OK,
        "ingest failed: status={} body={}",
        ingest_status,
        ingest_body
    );

    // Search
    let res = client
        .get(format!(
            "http://{}/api/v1/memory/schema?query=alice",
            app.addr
        ))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    let search_status = res.status();
    let search_body = res.text().await.unwrap();
    assert_eq!(
        search_status,
        StatusCode::OK,
        "search failed: status={} body={}",
        search_status,
        search_body
    );
    let json: serde_json::Value = serde_json::from_str(&search_body).unwrap();
    let results = json["results"].as_array().unwrap();
    assert_eq!(results.len(), 1);

    app.shutdown.trigger();
}

// ─── Test 8: Cold tier upload fails → local retained ─────────────────

#[tokio::test]
async fn e2e_cold_upload_failure_retains_local() {
    // Build an ObjectBackend that always fails on `put` but succeeds on
    // existence checks (so the migrator hits the failure path and bails).
    #[derive(Debug)]
    struct FailingPutBackend;
    #[async_trait::async_trait]
    impl ObjectBackend for FailingPutBackend {
        async fn put(&self, _key: &str, _data: &[u8]) -> cog_core::SFResult<String> {
            Err(cog_core::SFError::IO("simulated put failure".into()))
        }
        async fn get(&self, _key: &str) -> cog_core::SFResult<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn delete(&self, _key: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn presign_url(&self, _key: &str, _expiry_secs: u64) -> cog_core::SFResult<String> {
            Err(cog_core::SFError::IO("no presign".into()))
        }
        async fn exists(&self, _key: &str) -> cog_core::SFResult<bool> {
            Ok(false)
        }
        async fn list(&self, _prefix: Option<&str>) -> cog_core::SFResult<Vec<String>> {
            Ok(vec![])
        }
    }

    let dir = tempfile::TempDir::new().unwrap();
    let index = Arc::new(MemoryRawLogIndexStore::new());
    let backend: Arc<dyn ObjectBackend> = Arc::new(FailingPutBackend);
    let migrator = TierMigrator::new(
        dir.path(),
        TierPolicy {
            hot_duration: Duration::from_secs(1),
            warm_duration: Duration::from_secs(1),
            ..TierPolicy::default()
        },
        backend,
        index.clone(),
    );

    let stream_dir = dir.path().join("agent_raw");
    std::fs::create_dir_all(&stream_dir).unwrap();
    let log_path = stream_dir.join("2026-01-01.jsonl");
    std::fs::write(&log_path, b"data").unwrap();
    let old = SystemTime::now() - Duration::from_secs(5);
    filetime::set_file_mtime(&log_path, filetime::FileTime::from_system_time(old)).unwrap();

    let stats = migrator.run_once().await.unwrap();
    assert_eq!(stats.errors, 1, "expected one upload failure");
    assert!(
        log_path.exists(),
        "local file must be retained on cold-tier upload failure"
    );
    assert_eq!(index.len(), 0, "no index row on failed upload");
}

// ─── Test 9: Raw logs endpoint returns 503 when unconfigured ─────────

#[tokio::test]
async fn e2e_raw_logs_503_when_store_unconfigured() {
    // Build a GatewayState with raw_log_index_store = None.
    let app = spawn_app_full(false, None, false).await;
    let client = reqwest::Client::new();

    let res = client
        .get(format!("http://{}/api/v1/raw_logs", app.addr))
        .header("Authorization", app.bearer_token().await)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);

    app.shutdown.trigger();
}

// ─── Test 10: Prometheus /metrics aggregates counters ────────────────

#[tokio::test]
async fn e2e_metrics_endpoint_surfaces_http_and_memory_counters() {
    let app = spawn_app(false, None).await;

    // Generate a request so the http counter increments.
    let client = reqwest::Client::new();
    let _ = client
        .get(format!("http://{}/health", app.addr))
        .send()
        .await
        .unwrap();

    // Scrape metrics.
    let res = client
        .get(format!("http://{}/metrics", app.addr))
        .send()
        .await
        .unwrap();
    let body = res.text().await.unwrap();

    assert!(
        body.contains("http_requests_total") || body.contains("memory_operations_total"),
        "expected at least one known counter in /metrics: got {} chars",
        body.len()
    );

    app.shutdown.trigger();
}

// ─── Test 11: Memory batch ingest → list → delete round-trip ─────────

#[tokio::test]
async fn e2e_memory_batch_ingest_and_list() {
    let app = spawn_app(false, None).await;
    let token = app.bearer_token().await;
    let client = reqwest::Client::new();

    // Batch ingest
    let res = client
        .post(format!("http://{}/api/v1/memory/ingest/batch", app.addr))
        .header("Authorization", &token)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({
                "items": [
                    {"id": "batch-1", "text": "@entity:Alice\n@entity:Bob\n@relation:Alice->Bob"},
                    {"id": "batch-2", "text": "@event:ProjectAlphaLaunch"},
                ]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["processed"], 2);
    assert!(body["errors"].as_array().unwrap().is_empty());

    // List schema
    let res = client
        .get(format!("http://{}/api/v1/memory/schema/list", app.addr))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let schema_list: serde_json::Value = res.json().await.unwrap();
    let schemas = schema_list["results"].as_array().unwrap_or(&vec![]).clone();
    assert!(
        !schemas.is_empty(),
        "schema list should not be empty after batch ingest"
    );

    // List summary
    let res = client
        .get(format!("http://{}/api/v1/memory/summary/list", app.addr))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let summary_list: serde_json::Value = res.json().await.unwrap();
    let summaries = summary_list["results"]
        .as_array()
        .unwrap_or(&vec![])
        .clone();
    assert!(
        !summaries.is_empty(),
        "summary list should not be empty after batch ingest"
    );

    // Delete raw
    let res = client
        .delete(format!("http://{}/api/v1/memory/raw/batch-1", app.addr))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    app.shutdown.trigger();
}

// ─── Test 12: Memory unified search across schema + summary ──────────

#[tokio::test]
async fn e2e_memory_unified_search() {
    let app = spawn_app(false, None).await;
    let token = app.bearer_token().await;
    let client = reqwest::Client::new();

    // Ingest a document
    let _res = client
        .post(format!("http://{}/api/v1/memory/ingest", app.addr))
        .header("Authorization", &token)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({"id": "unified-doc-1", "text": "Charlie deploys to staging."})
                .to_string(),
        )
        .send()
        .await
        .unwrap();

    // Unified search
    let res = client
        .post(format!("http://{}/api/v1/memory/search", app.addr))
        .header("Authorization", &token)
        .header("Content-Type", "application/json")
        .body(serde_json::json!({"query": "Charlie", "top_k": 5}).to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    let results = body["results"].as_array().unwrap();
    assert!(
        !results.is_empty(),
        "unified search should return at least one result"
    );

    app.shutdown.trigger();
}

// ─── Test 13: Memory stats and metrics endpoints ─────────────────────

#[tokio::test]
async fn e2e_memory_stats_and_metrics() {
    let app = spawn_app(false, None).await;
    let token = app.bearer_token().await;
    let client = reqwest::Client::new();

    // Ingest something so stats are non-zero
    let _res = client
        .post(format!("http://{}/api/v1/memory/ingest", app.addr))
        .header("Authorization", &token)
        .header("Content-Type", "application/json")
        .body(
            serde_json::json!({"id": "stats-doc-1", "text": "Performance tuning notes."})
                .to_string(),
        )
        .send()
        .await
        .unwrap();

    // Stats endpoint
    let res = client
        .get(format!("http://{}/api/v1/memory/stats", app.addr))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let stats: serde_json::Value = res.json().await.unwrap();
    assert!(
        stats.get("raw_archived").is_some() || stats.get("schema_stored").is_some(),
        "stats should contain raw_archived or schema_stored"
    );

    // Metrics endpoint
    let res = client
        .get(format!("http://{}/api/v1/memory/metrics", app.addr))
        .header("Authorization", &token)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let metrics: serde_json::Value = res.json().await.unwrap();
    assert!(
        metrics.get("counters").is_some() || metrics.get("histograms").is_some(),
        "metrics endpoint should return counters or histograms"
    );

    app.shutdown.trigger();
}

// ─── Test 14: Distributed trace context propagation ──────────────────

#[tokio::test]
async fn e2e_trace_context_propagation() {
    let app = spawn_app(false, None).await;
    let client = reqwest::Client::new();

    let injected_trace_id = "aabbccdd11223344";
    let injected_span_id = "eeff55667788";

    // Call /health with injected trace headers.
    let res = client
        .get(format!("http://{}/health", app.addr))
        .header("x-trace-id", injected_trace_id)
        .header("x-span-id", injected_span_id)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    // Response must echo the trace headers back.
    let resp_trace_id = res
        .headers()
        .get("x-trace-id")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let resp_span_id = res
        .headers()
        .get("x-span-id")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    assert_eq!(
        resp_trace_id, injected_trace_id,
        "response x-trace-id must match injected value"
    );
    assert!(
        !resp_span_id.is_empty(),
        "response x-span-id must be present"
    );

    // Verify the RawLogger captured the trace context.
    let records = app.raw_logger.all_records().unwrap();
    let matched = records.iter().any(|r| {
        r.meta.trace_id == injected_trace_id && r.meta.span_id.as_deref() == Some(injected_span_id)
    });
    assert!(
        matched,
        "RawLogger should contain a record with the injected trace_id and span_id"
    );

    app.shutdown.trigger();
}
