use chrono::Duration as ChronoDuration;
use cog_core::{OrchestratorControl, Task, TaskType};
use cog_supervisor::{AgentRegistry, CrewInfo, Respawner};
use std::sync::Arc;

fn make_orchestrator() -> Arc<dyn OrchestratorControl> {
    let dag = Arc::new(cog_orchestrator::DagExecutor::new("ws".into()));
    Arc::new(cog_orchestrator::OrchestratorControlImpl::new(dag))
}

fn fake_registered_crew(reg: &AgentRegistry, crew_id: &str, agent_id: &str, tasks: &[&str]) {
    let mut crew = CrewInfo::new(crew_id);
    crew.agent_ids = vec![agent_id.into()];
    crew.task_ids = tasks.iter().map(|s| s.to_string()).collect();
    reg.register_crew(crew);

    let now = chrono::Utc::now();
    reg.register_agent(cog_supervisor::registry::AgentInfo {
        agent_id: agent_id.into(),
        role: Some("planner".into()),
        crew_id: Some(crew_id.into()),
        squad_id: None,
        task_ids: tasks.iter().map(|s| s.to_string()).collect(),
        last_heartbeat: now - ChronoDuration::seconds(120),
        state_since: now,
        registered_at: now,
    });
}

#[tokio::test]
async fn no_dead_agents_returns_empty_report() {
    let reg = Arc::new(AgentRegistry::new());
    let orch = make_orchestrator();
    let respawner = Respawner::new(reg, orch);
    let report = respawner.handle_dead_agents(&[]).await.unwrap();
    assert!(report.retried_crews.is_empty());
    assert!(report.respawn_requested.is_empty());
}

#[tokio::test]
async fn dead_agent_with_retryable_tasks_triggers_retry() {
    let reg = Arc::new(AgentRegistry::new());
    let orch = make_orchestrator();
    fake_registered_crew(&reg, "crew-1", "agent-1", &["t-1", "t-2"]);

    let t1 = Task::new("t-1", TaskType::Planner, serde_json::json!({}));
    let t2 = Task::new("t-2", TaskType::Planner, serde_json::json!({}));
    orch.submit_goal("g", vec![t1, t2]).await.unwrap();

    for id in ["t-1", "t-2"] {
        for _ in 0..3 {
            orch.schedule_task(id).await.unwrap();
            orch.start_task(id).await.unwrap();
            let (retried, _, _) = orch.fail_task(id, "boom".into()).await.unwrap();
            assert!(retried, "expected retry within budget");
        }
        orch.schedule_task(id).await.unwrap();
        orch.start_task(id).await.unwrap();
        let (retried, _, _) = orch.fail_task(id, "boom".into()).await.unwrap();
        assert!(
            !retried,
            "expected permanent failure after exhausting retries"
        );
    }

    let respawner = Respawner::new(reg.clone(), orch.clone());
    let report = respawner
        .handle_dead_agents(&["agent-1".into()])
        .await
        .unwrap();

    assert_eq!(report.retried_crews.len(), 0);
    assert_eq!(report.respawn_requested.len(), 1);
    assert_eq!(report.respawn_requested[0].crew_id, "crew-1");
}

#[tokio::test]
async fn crew_with_no_tasks_requests_respawn() {
    let reg = Arc::new(AgentRegistry::new());
    let orch = make_orchestrator();
    fake_registered_crew(&reg, "crew-1", "agent-1", &[]);

    let respawner = Respawner::new(reg.clone(), orch);
    let report = respawner
        .handle_dead_agents(&["agent-1".into()])
        .await
        .unwrap();
    assert_eq!(report.respawn_requested.len(), 1);
    assert!(report.respawn_requested[0]
        .reason
        .contains("no tracked tasks"));
}

#[tokio::test]
async fn retryable_failed_tasks_trigger_crew_retry() {
    let reg = Arc::new(AgentRegistry::new());
    let orch = make_orchestrator();
    fake_registered_crew(&reg, "crew-1", "agent-1", &["t-1"]);

    let task = Task::new("t-1", TaskType::Planner, serde_json::json!({}));
    orch.submit_goal("g", vec![task]).await.unwrap();
    for _ in 0..4 {
        orch.schedule_task("t-1").await.unwrap();
        orch.start_task("t-1").await.unwrap();
        let _ = orch.fail_task("t-1", "boom".into()).await.unwrap();
    }
    assert_eq!(
        orch.get_task("t-1").await.unwrap().status,
        cog_core::TaskStatus::Failed
    );

    let respawner = Respawner::new(reg.clone(), orch.clone());
    let report = respawner
        .handle_dead_agents(&["agent-1".into()])
        .await
        .unwrap();
    assert_eq!(report.respawn_requested.len(), 1);
}
