use chrono::Utc;
use cog_core::{AgentState, StateBackend};
use cog_storage::MemoryStateBackend;
use cog_supervisor::health_checker::{HealthChecker, HealthCheckerConfig};
use cog_supervisor::registry::{AgentInfo, AgentRegistry};
use std::sync::Arc;

fn fresh_registry() -> Arc<AgentRegistry> {
    Arc::new(AgentRegistry::new())
}

fn fresh_backend() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

fn make_agent(id: &str, secs_since_heartbeat: i64) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: Vec::new(),
        last_heartbeat: now - chrono::Duration::seconds(secs_since_heartbeat),
        state_since: now,
        registered_at: now,
    }
}

#[tokio::test]
async fn healthy_agent_yields_clean_report() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 1));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert!(report.is_clean(), "{:?}", report);
    assert_eq!(report.healthy, vec!["a-1".to_string()]);
}

#[tokio::test]
async fn suspect_threshold_triggers() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 20));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.suspect.len(), 1);
    assert!(report.dead.is_empty());
}

#[tokio::test]
async fn dead_threshold_triggers() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 90));
    let checker = HealthChecker::new(reg, fresh_backend(), HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.dead.len(), 1);
    assert!(report.suspect.is_empty());
}

#[tokio::test]
async fn backend_dead_state_short_circuits() {
    let reg = fresh_registry();
    reg.register_agent(make_agent("a-1", 1));
    let backend = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("a-1", &AgentState::Dead)
        .await
        .unwrap();
    let checker = HealthChecker::new(reg, backend, HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.dead.len(), 1);
}

#[tokio::test]
async fn stuck_active_agent_detected() {
    let reg = fresh_registry();
    let mut agent = make_agent("a-1", 1);
    agent.state_since = Utc::now() - chrono::Duration::seconds(700);
    reg.register_agent(agent);

    let backend = Arc::new(MemoryStateBackend::new());
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let checker = HealthChecker::new(reg, backend, HealthCheckerConfig::default());
    let report = checker.check().await.unwrap();
    assert_eq!(report.stuck.len(), 1);
}
