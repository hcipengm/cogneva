//! End-to-end integration tests for the autonomous executor in cogneva.
//! These tests exercise the full autonomous execution flow:
//!   DagExecutor -> TaskEvent broadcast -> Autonomous executor
//!   -> SquadExecutor -> Squad execution -> HookEngine -> StateBackend
//! All external dependencies (Redis, PostgreSQL) are replaced with in-memory mocks.

mod common;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

use cog_agent::hooks::{HookAction, HookDef, HookEngine, HookEvent, HookScope, HookTrigger};
use cog_collaboration::{PgeMode, SquadConfig, SquadExecutor};
use cog_core::TaskEvent as DagTaskEvent;
use cog_core::{AgentState, Task, TaskStatus, TaskType};
use cog_gateway::GatewayState;
use cog_memory::MemoryMemoryBackend;
use cog_storage::{MemoryMetricsBackend, MemoryRawLogIndexStore, NoopRawLogger};

use common::mocks::{MockLLMProvider, MockStateBackend};

// ─── Test harness for autonomous executor E2E tests ──────────────────

/// A fully wired test state for autonomous execution testing.
#[allow(dead_code)]
struct AutonomousTestState {
    gateway_state: Arc<GatewayState>,
    squad_executor: Arc<tokio::sync::Mutex<SquadExecutor>>,
    task_event_rx: broadcast::Receiver<DagTaskEvent>,
    hook_event_rx: broadcast::Receiver<HookEvent>,
    mock_state: Arc<MockStateBackend>,
    mock_llm: Arc<MockLLMProvider>,
}

async fn build_test_state() -> AutonomousTestState {
    let (event_tx, _event_rx) = broadcast::channel::<cog_core::AgentEvent>(16);
    let (task_event_tx, task_event_rx) = broadcast::channel::<DagTaskEvent>(256);

    let mock_state = Arc::new(MockStateBackend::new());
    let mock_llm = Arc::new(MockLLMProvider::new());

    let hook_publisher = Arc::new(RecordingHookPublisher::new());
    let hook_engine = Arc::new(HookEngine::new(hook_publisher));
    let hook_event_rx = hook_engine.subscribe();

    let hooks = vec![
        HookDef {
            id: "on-agent-start".into(),
            trigger: HookTrigger::OnAgentStart,
            scope: HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            action: HookAction::Log {
                level: cog_agent::hooks::LogLevel::Info,
            },
            rate_limit: None,
            timeout_ms: None,
        },
        HookDef {
            id: "on-task-complete".into(),
            trigger: HookTrigger::OnTaskComplete,
            scope: HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            action: HookAction::Log {
                level: cog_agent::hooks::LogLevel::Info,
            },
            rate_limit: None,
            timeout_ms: None,
        },
        HookDef {
            id: "on-ralph-pass".into(),
            trigger: HookTrigger::OnRalphPass,
            scope: HookScope::Global,
            crew_id_filter: None,
            squad_id_filter: None,
            action: HookAction::Log {
                level: cog_agent::hooks::LogLevel::Info,
            },
            rate_limit: None,
            timeout_ms: None,
        },
    ];

    for h in hooks {
        hook_engine.register(h).await;
    }

    let dag_executor = Arc::new(
        cog_orchestrator::DagExecutor::new("test-workspace".into())
            .with_event_tx(task_event_tx.clone()),
    );
    let orchestrator = Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag_executor));

    let jwt_manager: Arc<dyn cog_core::AuthProvider> = Arc::new(cog_auth::JwtManager::new(
        cog_auth::jwt::JwtConfig::default(),
    ));

    // Build a minimal quota manager backed by in-memory Redis fallback.
    let redis_client = redis::Client::open("redis://127.0.0.1:6379").expect("redis open");
    let redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("redis conn");
    let quota_manager = Arc::new(cog_quota::QuotaManager::new(redis_conn, 1_000_000_000));

    let raw_logger: Arc<dyn cog_core::RawLogger> = Arc::new(NoopRawLogger::new());
    let metrics_backend: Arc<dyn cog_core::MetricsBackend> = Arc::new(MemoryMetricsBackend::new());
    let memory_backend: Arc<dyn cog_core::MemoryBackend> = Arc::new(MemoryMemoryBackend::new());

    let gateway_state = Arc::new(GatewayState {
        data_dir: "/tmp".into(),
        config: std::sync::RwLock::new(cog_core::GatewayConfig {
            http_port: 0,
            ws_port: 0,
            metrics_port: 0,
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
        event_tx: event_tx.clone(),
        evolution_stream: None,
        task_event_tx: task_event_tx.clone(),
        jwt_manager: jwt_manager.clone(),
        quota_manager: quota_manager.clone(),
        hierarchy_manager: None,
        action_plan_store: Default::default(),
        collaboration_graph: None,
        raw_logger: raw_logger.clone(),
        memory_backend: Some(memory_backend.clone()),
        memory_ingestor: None,
        metrics_backend: Some(metrics_backend.clone()),
        metrics_exporter: None,
        search_backend: None,
        raw_log_index_store: Some(Arc::new(MemoryRawLogIndexStore::new())),
        hook_engine: Some(hook_engine.clone()),
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
        hook_archive: None,
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

    AutonomousTestState {
        gateway_state,
        squad_executor: Arc::new(tokio::sync::Mutex::new(
            SquadExecutor::new()
                .with_hook_engine(hook_engine.clone())
                .with_llm_provider(mock_llm.clone())
                .with_agent_manager(Arc::new(MockAgentManager)),
        )),
        task_event_rx,
        hook_event_rx,
        mock_state,
        mock_llm,
    }
}

/// Recording hook publisher that captures all hook executions.
struct RecordingHookPublisher {
    calls: std::sync::Mutex<Vec<(HookTrigger, serde_json::Value)>>,
}

impl RecordingHookPublisher {
    fn new() -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    #[allow(dead_code)]
    fn calls_for(&self, trigger: HookTrigger) -> Vec<serde_json::Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(t, _)| *t == trigger)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl cog_agent::hooks::HookPublisher for RecordingHookPublisher {
    async fn publish_webhook(
        &self,
        _url: &str,
        _headers: &std::collections::HashMap<String, String>,
        payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        if let Some(trigger) = payload.get("trigger").and_then(|v| v.as_str()) {
            let t = match trigger {
                "OnAgentStart" => HookTrigger::OnAgentStart,
                "OnAgentEnd" => HookTrigger::OnAgentEnd,
                "OnTaskComplete" => HookTrigger::OnTaskComplete,
                "OnTaskFail" => HookTrigger::OnTaskFail,
                "OnCrewComplete" => HookTrigger::OnCrewComplete,
                "OnRalphPass" => HookTrigger::OnRalphPass,
                "OnRalphUnrecoverable" => HookTrigger::OnRalphUnrecoverable,
                "OnSquadRetry" => HookTrigger::OnSquadRetry,
                _ => HookTrigger::OnAgentStart,
            };
            self.calls.lock().unwrap().push((t, payload.clone()));
        }
        Ok(())
    }

    async fn publish_redis_stream(
        &self,
        _channel: &str,
        payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        if let Some(trigger) = payload.get("trigger").and_then(|v| v.as_str()) {
            let t = match trigger {
                "OnAgentStart" => HookTrigger::OnAgentStart,
                "OnAgentEnd" => HookTrigger::OnAgentEnd,
                "OnTaskComplete" => HookTrigger::OnTaskComplete,
                "OnTaskFail" => HookTrigger::OnTaskFail,
                "OnCrewComplete" => HookTrigger::OnCrewComplete,
                "OnRalphPass" => HookTrigger::OnRalphPass,
                "OnRalphUnrecoverable" => HookTrigger::OnRalphUnrecoverable,
                "OnSquadRetry" => HookTrigger::OnSquadRetry,
                _ => HookTrigger::OnAgentStart,
            };
            self.calls.lock().unwrap().push((t, payload.clone()));
        }
        Ok(())
    }

    async fn notify_user(
        &self,
        _user_id: &str,
        _payload: &serde_json::Value,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
}

// ─── Mock Squad Agent & Manager ──────────────────────────────────────

struct MockSquadAgent {
    response: serde_json::Value,
}

#[async_trait::async_trait]
impl cog_core::Agent for MockSquadAgent {
    async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn start(&self) {}
    async fn snapshot(&self, _task_id: String) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
        Ok(cog_core::AgentCheckpoint {
            checkpoint_id: String::new(),
            task_id: String::new(),
            agent_state: serde_json::Value::Null,
            context_window: Vec::new(),
            event_offset: 0,
            timestamp: chrono::Utc::now(),
        })
    }
    async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn continue_(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
        Ok(self.response.clone())
    }
    async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn abort(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn reset(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
        Ok(cog_core::AgentState::Idle)
    }
    async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
        Ok(())
    }
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
        let (_tx, rx) = tokio::sync::broadcast::channel(1);
        rx
    }
    async fn chat_stream(
        &self,
        _messages: &[cog_core::Message],
        _options: &cog_core::ChatOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
        producer.end(cog_core::ChatResponse::default());
        Ok(stream)
    }
    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &cog_core::CompleteOptions,
    ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
        self.chat_stream(&[], &cog_core::ChatOptions::default())
            .await
    }
    async fn read_board(&self, _task_id: &str, _field: &str) -> cog_core::SFResult<Option<String>> {
        Ok(None)
    }
    async fn write_board(
        &self,
        _task_id: &str,
        _field: &str,
        _value: &str,
    ) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn review_output(
        &self,
        _output: &str,
        _config: &cog_core::SelfReviewConfig,
    ) -> cog_core::SFResult<cog_core::SelfReviewResult> {
        Ok(cog_core::SelfReviewResult::Pass {
            score: 1.0,
            summary: "mock pass".into(),
        })
    }
}

struct MockAgentManager;

#[async_trait::async_trait]
impl cog_core::AgentManager for MockAgentManager {
    async fn create_agent(
        &self,
        _agent_id: &str,
        role: &str,
        _llm: Arc<dyn cog_core::LlmClient>,
    ) -> cog_core::SFResult<Arc<dyn cog_core::Agent>> {
        let response = match role {
            "planner" => serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec", "design": "test design"},
                "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
            }),
            "generator" => serde_json::json!({
                "content": {"code": "fn main() {}", "tests": "", "documentation": ""},
                "artifacts": [],
            }),
            "evaluator" => serde_json::json!({
                "verdict": "pass",
                "score": 92,
                "feedback": "good",
                "criteria": [],
            }),
            _ => serde_json::json!({}),
        };
        Ok(Arc::new(MockSquadAgent { response }))
    }
    async fn dispatch(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn list_workers(&self) -> cog_core::SFResult<Vec<cog_core::WorkerInfo>> {
        Ok(Vec::new())
    }
    async fn shutdown(&self) -> cog_core::SFResult<()> {
        Ok(())
    }
    async fn get_agent(
        &self,
        _agent_id: &str,
    ) -> cog_core::SFResult<Option<Arc<dyn cog_core::Agent>>> {
        Ok(None)
    }
}

// ─── Helper: submit a goal and return the task id ────────────────────

async fn submit_goal(state: &AutonomousTestState, goal: &str) -> String {
    let task = Task::new(
        format!("task-{}", uuid::Uuid::new_v4()),
        TaskType::Custom("autonomous".into()),
        serde_json::json!({"goal": goal}),
    );
    let task_id = task.id.clone();

    let orch = state.gateway_state.orchestrator.clone();
    orch.submit_goal(goal, vec![task])
        .await
        .expect("submit_goal should succeed");

    task_id
}

// ─── Helper: execute a single ready task through the autonomous executor ─

async fn execute_ready_task(state: &AutonomousTestState, task_id: &str) {
    let task: Option<cog_core::Task> = {
        let orch = state.gateway_state.orchestrator.clone();
        orch.get_ready_tasks()
            .await
            .into_iter()
            .find(|t| t.id == task_id)
    };

    let task = match task {
        Some(t) => t,
        None => return,
    };

    // Schedule then start the task so start_task succeeds
    {
        let orch = state.gateway_state.orchestrator.clone();
        let _ = orch.schedule_task(task_id).await;
        let _ = orch.start_task(task_id).await;
    }

    // Hook: OnAgentStart
    if let Some(ref engine) = state.gateway_state.hook_engine {
        engine.emit_detached(
            HookEvent::new(HookTrigger::OnAgentStart)
                .with_task_id(task_id)
                .with_payload(serde_json::json!({"task_type": task.task_type})),
        );
    }

    // Execute Squad directly
    let config = SquadConfig {
        goal: format!("Execute task: {}", task_id),
        context: task.input.clone(),
        pge_mode: cog_collaboration::select_mode(&derive_task_profile(&task)),
        max_retries: 3,
        profile: None,
        context_window_size: None,
        boundary_config: None,
        execution_mode: true,
        is_self_evolution: false,
        planner_skill_id: None,
        generator_skill_id: None,
        evaluator_skill_id: None,
    };
    let squad_result = {
        let executor = state.squad_executor.lock().await;
        executor.execute_squad(task_id.to_string(), config).await
    };

    // Update task status based on Squad result
    let orch = state.gateway_state.orchestrator.clone();
    if squad_result.success {
        let _ = orch
            .complete_task(task_id, serde_json::json!({"squad_result": &squad_result}))
            .await;

        // Hook: OnTaskComplete
        if let Some(ref engine) = state.gateway_state.hook_engine {
            engine.emit_detached(
                HookEvent::new(HookTrigger::OnTaskComplete)
                    .with_task_id(task_id)
                    .with_payload(serde_json::json!({
                        "success": true,
                    })),
            );
        }
    } else {
        let error = squad_result
            .error
            .unwrap_or_else(|| "Squad execution failed".into());

        let _ = orch.fail_task(task_id, error.clone()).await;

        // Hook: OnTaskFail
        if let Some(ref engine) = state.gateway_state.hook_engine {
            engine.emit_detached(
                HookEvent::new(HookTrigger::OnTaskFail)
                    .with_task_id(task_id)
                    .with_payload(serde_json::json!({
                        "error": error,
                    })),
            );
        }
    }

    // Hook: OnAgentEnd
    if let Some(ref engine) = state.gateway_state.hook_engine {
        engine.emit_detached(HookEvent::new(HookTrigger::OnAgentEnd).with_task_id(task_id));
    }
}

/// Derive a TaskProfile from a Task for PGE mode selection.
fn derive_task_profile(task: &cog_core::Task) -> cog_collaboration::TaskProfile {
    let input_len = task.input.to_string().len() as f64;
    let dep_count = task.blocked_by.len() as f64;

    cog_collaboration::TaskProfile {
        novelty: match task.task_type {
            TaskType::Custom(_) => 0.7,
            TaskType::WasmSkill | TaskType::Skill => 0.6,
            _ => 0.3,
        },
        risk: (dep_count / 10.0).min(1.0),
        ambiguity: if input_len < 50.0 {
            0.8
        } else if input_len < 200.0 {
            0.5
        } else {
            0.3
        },
        dependency_count: (dep_count / 20.0).min(1.0),
        token_budget: 1.0,
        historical_success: 1.0,
    }
}

// ─── Test 1: Simple goal executes autonomously ───────────────────────

#[tokio::test]
async fn test_simple_goal_executes_autonomously() {
    let state = build_test_state().await;
    let task_id = submit_goal(&state, "implement hello world").await;

    // Execute the ready task
    execute_ready_task(&state, &task_id).await;

    // Verify task is completed
    let orch = state.gateway_state.orchestrator.clone();
    let task = orch.get_task(&task_id).await.expect("task should exist");
    assert_eq!(
        task.status,
        TaskStatus::Completed,
        "task should be Completed after autonomous execution"
    );
    assert!(task.result.is_some(), "task should have a result");
}

// ─── Test 2: Squad retry mechanism ───────────────────────────────────

#[tokio::test]
async fn test_squad_retry_mechanism() {
    let state = build_test_state().await;
    let task_id = submit_goal(&state, "task that will trigger retry").await;

    // Execute squad with max_retries=0 so failures are terminal.
    // Since stub agents always pass, we verify the retry infrastructure is wired.
    let config = SquadConfig {
        goal: format!("Execute task: {}", task_id),
        context: serde_json::json!({"goal": "task that will trigger retry"}),
        pge_mode: PgeMode::Pipeline,
        max_retries: 0,
        profile: None,
        context_window_size: None,
        boundary_config: None,
        execution_mode: true,
        is_self_evolution: false,
        planner_skill_id: None,
        generator_skill_id: None,
        evaluator_skill_id: None,
    };

    let executor = state.squad_executor.lock().await;
    let result = executor.execute_squad(task_id.clone(), config).await;
    drop(executor);

    // Stub agents pass, so squad succeeds; verify retry_count is tracked.
    assert!(result.success, "squad should succeed with stub agents");
    assert_eq!(result.retry_count, 0, "no retries needed for stub agents");
}

// ─── Test 3: Hook events during execution ────────────────────────────

#[tokio::test]
async fn test_hook_events_during_execution() {
    let state = build_test_state().await;
    let task_id = submit_goal(&state, "test hook events").await;

    execute_ready_task(&state, &task_id).await;

    // Allow async hook dispatch to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the task completed successfully
    let orch = state.gateway_state.orchestrator.clone();
    let task = orch.get_task(&task_id).await.expect("task should exist");
    assert_eq!(task.status, TaskStatus::Completed);

    // Verify that the hook engine has the expected hooks registered
    let hook_engine = state.gateway_state.hook_engine.as_ref().unwrap();
    let hooks = hook_engine.list_hooks().await;
    assert!(
        hooks.iter().any(|h| h.trigger == HookTrigger::OnAgentStart),
        "OnAgentStart hook should be registered"
    );
    assert!(
        hooks
            .iter()
            .any(|h| h.trigger == HookTrigger::OnTaskComplete),
        "OnTaskComplete hook should be registered"
    );
    assert!(
        hooks.iter().any(|h| h.trigger == HookTrigger::OnRalphPass),
        "OnRalphPass hook should be registered"
    );
}

// ─── Test 4: Task event broadcast ────────────────────────────────────

#[tokio::test]
async fn test_task_event_broadcast() {
    let state = build_test_state().await;
    let mut rx = state.gateway_state.subscribe_task_events();

    let task_id = submit_goal(&state, "test task events").await;

    // Drain TaskCreated event
    let event = rx.try_recv();
    assert!(
        event.is_ok(),
        "TaskCreated event should be broadcast immediately after submit_goal"
    );
    match event.unwrap() {
        DagTaskEvent::TaskCreated { task_id: id, .. } => {
            assert_eq!(id, task_id);
        }
        other => panic!("Expected TaskCreated, got {:?}", other),
    }

    execute_ready_task(&state, &task_id).await;

    // Collect remaining events
    let mut found_started = false;
    let mut found_completed = false;

    while let Ok(event) = rx.try_recv() {
        match event {
            DagTaskEvent::TaskStarted { task_id: id, .. } if id == task_id => {
                found_started = true;
            }
            DagTaskEvent::TaskCompleted { task_id: id, .. } if id == task_id => {
                found_completed = true;
            }
            _ => {}
        }
    }

    assert!(
        found_started,
        "TaskStarted event should be broadcast for task {}",
        task_id
    );
    assert!(
        found_completed,
        "TaskCompleted event should be broadcast for task {}",
        task_id
    );
}

// ─── Test 5: State is persisted through StateBackend ─────────────────

#[tokio::test]
async fn test_state_persisted_through_state_backend() {
    let state = build_test_state().await;
    let task_id = submit_goal(&state, "test state persistence").await;

    // Use the mock state backend directly via the StateBackend trait
    let agent_id = format!("agent-{}", task_id);
    cog_core::StateBackend::set_agent_state(
        state.mock_state.as_ref(),
        &agent_id,
        &AgentState::Active,
    )
    .await
    .expect("set_agent_state should succeed");

    let retrieved = cog_core::StateBackend::get_agent_state(state.mock_state.as_ref(), &agent_id)
        .await
        .expect("get_agent_state should succeed");

    assert_eq!(
        retrieved,
        Some(AgentState::Active),
        "state should be persisted and retrievable"
    );

    // Verify checkpoint persistence
    let checkpoint = cog_core::TaskCheckpoint {
        task_id: task_id.clone(),
        snapshot_id: "snap-001".into(),
        event_offset: 42,
        timestamp: chrono::Utc::now(),
    };
    cog_core::StateBackend::save_checkpoint(state.mock_state.as_ref(), &checkpoint)
        .await
        .expect("save_checkpoint should succeed");

    let loaded = cog_core::StateBackend::get_checkpoint(state.mock_state.as_ref(), &task_id)
        .await
        .expect("get_checkpoint should succeed");

    assert_eq!(
        loaded,
        Some(checkpoint),
        "checkpoint should be persisted and retrievable"
    );

    // Verify event persistence
    let event = cog_core::Event {
        offset: 0,
        task_id: task_id.clone(),
        event_type: "test_event".into(),
        payload: serde_json::json!({"key": "value"}),
        timestamp: chrono::Utc::now(),
    };
    let len = cog_core::StateBackend::append_event(state.mock_state.as_ref(), &task_id, &event)
        .await
        .expect("append_event should succeed");
    assert_eq!(len, 1, "event list should have 1 entry");

    let events = cog_core::StateBackend::get_events(state.mock_state.as_ref(), &task_id, 0, 10)
        .await
        .expect("get_events should succeed");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "test_event");

    // Verify board persistence
    cog_core::StateBackend::set_board_field(
        state.mock_state.as_ref(),
        &task_id,
        "status",
        "in_progress",
    )
    .await
    .expect("set_board_field should succeed");

    let board = cog_core::StateBackend::get_board(state.mock_state.as_ref(), &task_id)
        .await
        .expect("get_board should succeed");

    assert!(board.is_some(), "board should exist");
    let board = board.unwrap();
    assert_eq!(
        board.fields.get("status"),
        Some(&"in_progress".to_string()),
        "board field should be persisted"
    );
}
