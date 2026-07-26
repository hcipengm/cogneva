use chrono::{Duration as ChronoDuration, Utc};
use cog_core::{
    ObservabilityGateway, OrchestratorControl, StateBackend, SupervisorEvent, Task, TaskType,
};
use cog_storage::{MemoryObservabilityGateway, MemoryStateBackend};
use cog_supervisor::registry::AgentRegistry;
use cog_supervisor::{
    AutonomousConfig, HealthCheckerConfig, SchedulerGate, Supervisor, SupervisorConfig,
    TaskRebalancerConfig,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};

fn make_orchestrator() -> Arc<dyn OrchestratorControl> {
    let dag = Arc::new(cog_orchestrator::DagExecutor::new("ws".into()));
    Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag))
}

fn fake_quota_source(remaining: u64) -> Arc<dyn cog_core::WorkspaceQuotaSource> {
    struct StaticQuota(AtomicU64);
    #[async_trait::async_trait]
    impl cog_core::WorkspaceQuotaSource for StaticQuota {
        async fn workspace_remaining(&self, _ws: &str) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    Arc::new(StaticQuota(AtomicU64::new(remaining)))
}

fn build_supervisor(
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    orchestrator: Arc<dyn OrchestratorControl>,
    quota_source: Arc<dyn cog_core::WorkspaceQuotaSource>,
    gateway: Arc<dyn ObservabilityGateway>,
    gate: Arc<SchedulerGate>,
) -> Arc<Supervisor> {
    build_supervisor_with_config_rx(
        registry,
        state_backend,
        orchestrator,
        quota_source,
        gateway,
        gate,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_supervisor_with_config_rx(
    registry: Arc<AgentRegistry>,
    state_backend: Arc<dyn StateBackend>,
    orchestrator: Arc<dyn OrchestratorControl>,
    quota_source: Arc<dyn cog_core::WorkspaceQuotaSource>,
    gateway: Arc<dyn ObservabilityGateway>,
    gate: Arc<SchedulerGate>,
    config_rx: Option<watch::Receiver<SupervisorConfig>>,
) -> Arc<Supervisor> {
    let config = SupervisorConfig {
        health_interval: Duration::from_millis(50),
        quota_interval: Duration::from_millis(50),
        rebalance_interval: Duration::from_millis(50),
        event_window: Duration::from_millis(50),
        broadcast_capacity: 64,
        health_checker: HealthCheckerConfig::default(),
        task_rebalancer: TaskRebalancerConfig::default(),
        autonomous: AutonomousConfig::default(),
        quota_threshold: 1_000,
        control_plane_interval: Duration::from_secs(30),
        control_plane_url: None,
        behavior_history_max: 20,
        heartbeat_history_max: 1_000,
        alert_history_max: 10_000,
    };

    let (_agent_event_tx, agent_event_rx) = broadcast::channel(64);
    let mut supervisor = Supervisor::new(
        config,
        registry,
        state_backend,
        orchestrator,
        quota_source,
        gateway,
        gate,
        agent_event_rx,
        None,
    );
    if let Some(rx) = config_rx {
        supervisor = supervisor.with_config_watch(rx);
    }
    Arc::new(supervisor)
}

#[tokio::test]
async fn health_pass_emits_unhealthy_event() {
    let registry = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orchestrator = make_orchestrator();
    let gateway: Arc<dyn ObservabilityGateway> =
        Arc::new(MemoryObservabilityGateway::new(backend.clone()));
    let gate = Arc::new(SchedulerGate::new());

    // Insert an agent whose heartbeat is well in the past.
    let agent = cog_supervisor::registry::AgentInfo {
        agent_id: "a-stuck".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now() - ChronoDuration::seconds(120),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    };
    registry.register_agent(agent);

    let supervisor = build_supervisor(
        registry,
        backend,
        orchestrator.clone(),
        fake_quota_source(2_000),
        gateway,
        gate,
    );
    let mut rx = supervisor.subscribe();
    supervisor.run_health_pass().await.unwrap();

    let mut saw = false;
    for _ in 0..8 {
        if let Ok(event) = rx.try_recv() {
            if matches!(event, SupervisorEvent::AgentUnhealthy { .. }) {
                saw = true;
                break;
            }
        } else {
            break;
        }
    }
    assert!(saw, "expected an AgentUnhealthy event");
}

#[tokio::test]
async fn quota_pass_pauses_scheduler_on_breach() {
    let registry = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orchestrator = make_orchestrator();
    let gateway: Arc<dyn ObservabilityGateway> =
        Arc::new(MemoryObservabilityGateway::new(backend.clone()));
    let gate = Arc::new(SchedulerGate::new());

    let supervisor = build_supervisor(
        registry,
        backend,
        orchestrator.clone(),
        fake_quota_source(500),
        gateway,
        gate.clone(),
    );
    supervisor.track_workspace("ws-a");

    supervisor.run_quota_pass().await.unwrap();
    assert!(gate.is_paused());
}

#[tokio::test]
async fn rebalance_pass_emits_event_when_ready_tasks_exist() {
    let registry = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orchestrator = make_orchestrator();
    orchestrator
        .submit_goal(
            "g",
            vec![Task::new("t-1", TaskType::Planner, serde_json::json!({}))],
        )
        .await
        .unwrap();

    let gateway: Arc<dyn ObservabilityGateway> =
        Arc::new(MemoryObservabilityGateway::new(backend.clone()));
    let gate = Arc::new(SchedulerGate::new());
    let supervisor = build_supervisor(
        registry,
        backend,
        orchestrator.clone(),
        fake_quota_source(2_000),
        gateway,
        gate,
    );

    let mut rx = supervisor.subscribe();
    supervisor.run_rebalance_pass().await.unwrap();
    let mut saw = false;
    for _ in 0..4 {
        match rx.try_recv() {
            Ok(SupervisorEvent::Rebalance { ready_tasks, .. }) => {
                assert_eq!(ready_tasks, 1);
                saw = true;
                break;
            }
            _ => continue,
        }
    }
    assert!(saw, "expected a Rebalance event");
}

#[tokio::test]
async fn config_reload_updates_state() {
    let registry = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orchestrator = make_orchestrator();
    let gateway: Arc<dyn ObservabilityGateway> =
        Arc::new(MemoryObservabilityGateway::new(backend.clone()));
    let gate = Arc::new(SchedulerGate::new());

    let initial_config = SupervisorConfig {
        health_interval: Duration::from_secs(3600),
        quota_interval: Duration::from_secs(3600),
        rebalance_interval: Duration::from_secs(3600),
        event_window: Duration::from_secs(3600),
        broadcast_capacity: 64,
        health_checker: HealthCheckerConfig::default(),
        task_rebalancer: TaskRebalancerConfig::default(),
        autonomous: AutonomousConfig::default(),
        quota_threshold: 1_000,
        control_plane_interval: Duration::from_secs(3600),
        control_plane_url: None,
        behavior_history_max: 10,
        heartbeat_history_max: 1_000,
        alert_history_max: 10_000,
    };

    let (config_tx, config_rx) = watch::channel(initial_config.clone());

    let supervisor = build_supervisor_with_config_rx(
        registry,
        backend,
        orchestrator.clone(),
        fake_quota_source(2_000),
        gateway,
        gate,
        Some(config_rx),
    );

    let sup_clone = supervisor.clone();
    let handle = tokio::spawn(async move {
        let shutdown = async {
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        sup_clone.run(shutdown).await;
    });

    // Allow run() to enter the select loop.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut new_config = initial_config.clone();
    new_config.behavior_history_max = 42;
    new_config.health_interval = Duration::from_secs(1800);
    let _ = config_tx.send(new_config);

    // Wait for the config change to be processed.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let current = supervisor.config.read().unwrap().clone();
    assert_eq!(current.behavior_history_max, 42);
    assert_eq!(current.health_interval, Duration::from_secs(1800));

    handle.abort();
}
