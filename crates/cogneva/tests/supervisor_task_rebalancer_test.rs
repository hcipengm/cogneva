use chrono::Utc;
use cog_core::{AgentState, OrchestratorControl, StateBackend, Task, TaskCheckpoint, TaskType};
use cog_storage::MemoryStateBackend;
use cog_supervisor::lifecycle_coordinator::RecoveredCheckpoint;
use cog_supervisor::registry::{AgentInfo, AgentRegistry};
use cog_supervisor::task_rebalancer::{TaskRebalancer, TaskRebalancerConfig};
use std::collections::HashMap;
use std::sync::Arc;

fn make_orchestrator() -> Arc<dyn OrchestratorControl> {
    let dag = Arc::new(cog_orchestrator::DagExecutor::new("ws".into()));
    Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag))
}

fn make_agent(id: &str) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: Vec::new(),
        last_heartbeat: now,
        state_since: now,
        registered_at: now,
    }
}

#[tokio::test]
async fn empty_orchestrator_yields_empty_plan() {
    let reg = Arc::new(AgentRegistry::new());
    let backend = Arc::new(MemoryStateBackend::new());
    let orch = make_orchestrator();
    let rebalancer = TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default());
    let plan = rebalancer.plan().await.unwrap();
    assert!(plan.is_empty());
    assert_eq!(plan.ready_tasks, 0);
}

#[tokio::test]
async fn ready_tasks_get_distributed_across_agents() {
    let reg = Arc::new(AgentRegistry::new());
    reg.register_agent(make_agent("agent-1"));
    reg.register_agent(make_agent("agent-2"));

    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("agent-1", &AgentState::Idle)
        .await
        .unwrap();
    backend
        .set_agent_state("agent-2", &AgentState::Idle)
        .await
        .unwrap();

    let orch = make_orchestrator();
    let mut tasks = Vec::new();
    for i in 0..4 {
        tasks.push(Task::new(
            format!("t-{i}"),
            TaskType::Planner,
            serde_json::json!({}),
        ));
    }
    orch.submit_goal("g", tasks).await.unwrap();

    let rebalancer = TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default());
    let plan = rebalancer.plan().await.unwrap();
    assert_eq!(plan.ready_tasks, 4);
    assert_eq!(plan.available_agents, 2);
    assert_eq!(plan.assignments.len(), 4);

    // Each agent should receive 2 tasks for fair round-robin.
    let mut load = HashMap::new();
    for (agent_id, _) in &plan.assignments {
        *load.entry(agent_id.clone()).or_insert(0_usize) += 1;
    }
    assert_eq!(load.get("agent-1").copied().unwrap_or(0), 2);
    assert_eq!(load.get("agent-2").copied().unwrap_or(0), 2);
}

#[tokio::test]
async fn dead_agent_is_skipped() {
    let reg = Arc::new(AgentRegistry::new());
    reg.register_agent(make_agent("agent-1"));

    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("agent-1", &AgentState::Dead)
        .await
        .unwrap();

    let orch = make_orchestrator();
    orch.submit_goal(
        "g",
        vec![Task::new("t-1", TaskType::Planner, serde_json::json!({}))],
    )
    .await
    .unwrap();

    let rebalancer = TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default());
    let plan = rebalancer.plan().await.unwrap();
    assert_eq!(plan.ready_tasks, 1);
    assert_eq!(plan.available_agents, 0);
    assert!(plan.assignments.is_empty());
}

#[tokio::test]
async fn overloaded_agent_is_flagged() {
    let reg = Arc::new(AgentRegistry::new());
    reg.register_agent(make_agent("agent-1"));

    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("agent-1", &AgentState::Active)
        .await
        .unwrap();

    let orch = make_orchestrator();
    let mut tasks = Vec::new();
    for i in 0..6 {
        let mut t = Task::new(format!("t-{i}"), TaskType::Planner, serde_json::json!({}));
        t.agent_id = Some("agent-1".into());
        tasks.push(t);
    }
    orch.submit_goal("g", tasks).await.unwrap();
    // Drive each task to Scheduled / Running so they count toward load.
    for i in 0..6 {
        let id = format!("t-{i}");
        orch.schedule_task(&id).await.unwrap();
        orch.start_task(&id).await.unwrap();
    }

    let rebalancer = TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default());
    let plan = rebalancer.plan().await.unwrap();
    assert!(!plan.overloaded_agents.is_empty());
    assert_eq!(plan.overloaded_agents[0].0, "agent-1");
    // Overloaded agents are not used for new assignments.
    assert!(plan.assignments.is_empty());
}

#[tokio::test]
async fn recovery_plan_includes_relevant_checkpoints() {
    let reg = Arc::new(AgentRegistry::new());
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let orch = make_orchestrator();
    orch.submit_goal(
        "g",
        vec![
            Task::new("t-1", TaskType::Planner, serde_json::json!({})),
            Task::new("t-2", TaskType::Planner, serde_json::json!({})),
        ],
    )
    .await
    .unwrap();

    let rebalancer = TaskRebalancer::new(reg, backend, orch, TaskRebalancerConfig::default());
    let recovered = vec![
        RecoveredCheckpoint {
            agent_id: "a-1".into(),
            task_id: "t-1".into(),
            checkpoint: TaskCheckpoint {
                task_id: "t-1".into(),
                snapshot_id: "snap-1".into(),
                event_offset: 10,
                timestamp: Utc::now(),
            },
        },
        RecoveredCheckpoint {
            agent_id: "a-1".into(),
            task_id: "t-2".into(),
            checkpoint: TaskCheckpoint {
                task_id: "t-2".into(),
                snapshot_id: "snap-2".into(),
                event_offset: 20,
                timestamp: Utc::now(),
            },
        },
        // A checkpoint for a task that does not exist in the orchestrator.
        RecoveredCheckpoint {
            agent_id: "a-1".into(),
            task_id: "t-missing".into(),
            checkpoint: TaskCheckpoint {
                task_id: "t-missing".into(),
                snapshot_id: "snap-m".into(),
                event_offset: 0,
                timestamp: Utc::now(),
            },
        },
    ];

    let plan = rebalancer.recovery_plan(&recovered).await.unwrap();
    assert_eq!(plan.checkpoint_recoveries.len(), 2);
    let ids: Vec<&str> = plan
        .checkpoint_recoveries
        .iter()
        .map(|(id, _)| id.as_str())
        .collect();
    assert!(ids.contains(&"t-1"));
    assert!(ids.contains(&"t-2"));
}
