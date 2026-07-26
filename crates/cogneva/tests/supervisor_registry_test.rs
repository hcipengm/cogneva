use chrono::{Duration as ChronoDuration, Utc};
use cog_core::{AgentState, StateBackend};
use cog_storage::MemoryStateBackend;
use cog_supervisor::registry::{
    AgentInfo, AgentRegistry, CrewInfo, HeartbeatRecord, HeartbeatStatus,
    DEFAULT_HEARTBEAT_TTL_SECONDS,
};
use std::sync::Arc;

fn fake_agent(id: &str) -> AgentInfo {
    let now = Utc::now();
    AgentInfo {
        agent_id: id.into(),
        role: Some("planner".into()),
        crew_id: Some("crew-1".into()),
        squad_id: Some("squad-1".into()),
        task_ids: vec!["task-a".into()],
        last_heartbeat: now,
        state_since: now,
        registered_at: now,
    }
}

#[test]
fn register_and_lookup() {
    let reg = AgentRegistry::new();
    reg.register_agent(fake_agent("a-1"));
    assert_eq!(reg.agent_count(), 1);
    assert!(reg.get_agent("a-1").is_some());
    assert!(reg.get_agent("missing").is_none());
}

#[test]
fn unregister_removes_from_crews() {
    let reg = AgentRegistry::new();
    let mut crew = CrewInfo::new("crew-1");
    crew.agent_ids = vec!["a-1".into(), "a-2".into()];
    reg.register_crew(crew);
    reg.register_agent(fake_agent("a-1"));

    reg.unregister_agent("a-1");
    let updated = reg.get_crew("crew-1").unwrap();
    assert_eq!(updated.agent_ids, vec!["a-2".to_string()]);
}

#[test]
fn touch_updates_heartbeat() {
    let reg = AgentRegistry::new();
    reg.register_agent(fake_agent("a-1"));
    let before = reg.get_agent("a-1").unwrap().last_heartbeat;
    std::thread::sleep(std::time::Duration::from_millis(5));
    reg.touch("a-1");
    let after = reg.get_agent("a-1").unwrap().last_heartbeat;
    assert!(after > before);
}

#[test]
fn touch_inserts_unknown_agent() {
    let reg = AgentRegistry::new();
    reg.touch("ghost");
    assert!(reg.get_agent("ghost").is_some());
}

#[test]
fn set_agent_tasks_updates_listing() {
    let reg = AgentRegistry::new();
    reg.register_agent(fake_agent("a-1"));
    reg.set_agent_tasks("a-1", vec!["t-1".into(), "t-2".into()]);
    assert_eq!(reg.get_agent("a-1").unwrap().task_ids.len(), 2);
}

#[test]
fn record_crew_retry_increments() {
    let reg = AgentRegistry::new();
    reg.register_crew(CrewInfo::new("crew-1"));
    let count = reg.record_crew_retry("crew-1");
    assert_eq!(count, 1);
    let count = reg.record_crew_retry("crew-1");
    assert_eq!(count, 2);
}

// -- Heartbeat tests --

#[tokio::test]
async fn heartbeat_inserts_and_updates_timestamp() {
    let reg = AgentRegistry::new();
    reg.heartbeat("a-1").await.unwrap();
    let first = reg.get_heartbeat("a-1").expect("heartbeat present");
    assert_eq!(first.status, HeartbeatStatus::Healthy);

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    reg.heartbeat("a-1").await.unwrap();
    let second = reg.get_heartbeat("a-1").unwrap();
    assert!(second.timestamp > first.timestamp);
}

#[tokio::test]
async fn record_heartbeat_persists_full_record() {
    let reg = AgentRegistry::new();
    let record = HeartbeatRecord {
        agent_id: "a-1".into(),
        timestamp: Utc::now(),
        status: HeartbeatStatus::Degraded,
        load_score: 0.75,
        task_count: 3,
    };
    reg.record_heartbeat(record.clone()).await.unwrap();
    let stored = reg.get_heartbeat("a-1").unwrap();
    assert_eq!(stored, record);
    // Last heartbeat on the AgentInfo is bumped too.
    assert_eq!(
        reg.get_agent("a-1").unwrap().last_heartbeat,
        record.timestamp
    );
}

#[tokio::test]
async fn record_heartbeat_mirrors_state_into_backend() {
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let reg = AgentRegistry::new().with_state_backend(backend.clone());
    let record = HeartbeatRecord {
        agent_id: "a-1".into(),
        timestamp: Utc::now(),
        status: HeartbeatStatus::Unhealthy,
        load_score: 0.95,
        task_count: 7,
    };
    reg.record_heartbeat(record).await.unwrap();
    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Suspect));
}

#[tokio::test]
async fn heartbeat_mirrors_active_into_backend() {
    let backend: Arc<dyn StateBackend> = Arc::new(MemoryStateBackend::new());
    let reg = AgentRegistry::new().with_state_backend(backend.clone());
    reg.heartbeat("a-1").await.unwrap();
    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));
}

#[tokio::test]
async fn check_expired_returns_old_heartbeats() {
    let reg = AgentRegistry::new();
    // Inject a stale heartbeat directly.
    let stale_ts = Utc::now() - ChronoDuration::seconds(120);
    let stale = HeartbeatRecord {
        agent_id: "a-stale".into(),
        timestamp: stale_ts,
        status: HeartbeatStatus::Healthy,
        load_score: 0.0,
        task_count: 0,
    };
    reg.record_heartbeat(stale).await.unwrap();
    // Inject a fresh heartbeat for a different agent.
    let fresh = HeartbeatRecord::now("a-fresh");
    reg.record_heartbeat(fresh).await.unwrap();
    let expired = reg.check_expired(DEFAULT_HEARTBEAT_TTL_SECONDS);
    assert_eq!(expired, vec!["a-stale".to_string()]);
}

#[test]
fn check_expired_falls_back_to_agent_info() {
    let reg = AgentRegistry::new();
    // Agent registered with a stale last_heartbeat and no record.
    let mut info = fake_agent("a-1");
    info.last_heartbeat = Utc::now() - ChronoDuration::seconds(120);
    reg.register_agent(info);
    let expired = reg.check_expired(DEFAULT_HEARTBEAT_TTL_SECONDS);
    assert_eq!(expired, vec!["a-1".to_string()]);
}

#[tokio::test]
async fn health_score_decays_linearly() {
    let reg = AgentRegistry::new();
    // Fresh heartbeat -> ~1.0
    reg.heartbeat("a-1").await.unwrap();
    let fresh = reg.health_score("a-1").unwrap();
    assert!(fresh > 0.99, "expected ~1.0, got {fresh}");

    // Manually backdate to half-way through the TTL window.
    {
        let mut record = reg.get_heartbeat("a-1").unwrap();
        record.timestamp =
            Utc::now() - ChronoDuration::seconds(DEFAULT_HEARTBEAT_TTL_SECONDS as i64 / 2);
        reg.record_heartbeat(record).await.unwrap();
    }
    let mid = reg.health_score("a-1").unwrap();
    assert!(mid > 0.45 && mid < 0.55, "expected ~0.5, got {mid}");

    // Beyond TTL -> 0.0
    {
        let mut record = reg.get_heartbeat("a-1").unwrap();
        record.timestamp =
            Utc::now() - ChronoDuration::seconds(DEFAULT_HEARTBEAT_TTL_SECONDS as i64 + 5);
        reg.record_heartbeat(record).await.unwrap();
    }
    let expired = reg.health_score("a-1").unwrap();
    assert_eq!(expired, 0.0);
}

#[test]
fn health_score_returns_none_for_unknown_agent() {
    let reg = AgentRegistry::new();
    assert!(reg.health_score("missing").is_none());
}

#[tokio::test]
async fn unregister_clears_heartbeat() {
    let reg = AgentRegistry::new();
    reg.heartbeat("a-1").await.unwrap();
    assert!(reg.get_heartbeat("a-1").is_some());
    reg.unregister_agent("a-1");
    assert!(reg.get_heartbeat("a-1").is_none());
}
