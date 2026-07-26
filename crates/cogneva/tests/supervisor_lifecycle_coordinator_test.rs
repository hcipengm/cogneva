use chrono::Utc;
use cog_core::SupervisorEvent;
use cog_core::{AgentState, StateBackend, TaskCheckpoint};
use cog_storage::MemoryStateBackend;
use cog_supervisor::health_checker::HealthReport;
use cog_supervisor::lifecycle_coordinator::LifecycleCoordinator;
use cog_supervisor::registry::{AgentInfo, AgentRegistry};
use std::sync::Arc;
use tokio::sync::broadcast;

fn mk_backend() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

fn mk_registry() -> Arc<AgentRegistry> {
    Arc::new(AgentRegistry::new())
}

fn mk_coordinator(
    registry: Arc<AgentRegistry>,
    backend: Arc<dyn StateBackend>,
) -> (LifecycleCoordinator, broadcast::Receiver<SupervisorEvent>) {
    let (tx, rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry, backend, tx);
    (coord, rx)
}

#[tokio::test]
async fn suspect_agent_transitioned_to_suspect() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, _rx) = mk_coordinator(registry, backend.clone());
    let report = HealthReport {
        suspect: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 2 },
        )],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();
    assert_eq!(result.transitioned.len(), 1);
    assert_eq!(result.transitioned[0].2, AgentState::Suspect);

    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Suspect));
}

#[tokio::test]
async fn dead_agent_transitioned_and_checkpoint_recovered() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    backend
        .save_checkpoint(&TaskCheckpoint {
            task_id: "t-1".into(),
            snapshot_id: "snap-1".into(),
            event_offset: 42,
            timestamp: Utc::now(),
        })
        .await
        .unwrap();

    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec!["t-1".into()],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, _rx) = mk_coordinator(registry, backend.clone());
    let report = HealthReport {
        dead: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
        )],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();
    assert_eq!(result.transitioned.len(), 1);
    assert_eq!(result.transitioned[0].2, AgentState::Dead);
    assert_eq!(result.recovered_checkpoints.len(), 1);
    assert_eq!(result.recovered_checkpoints[0].task_id, "t-1");

    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Dead));
}

#[tokio::test]
async fn invalid_transition_is_rejected() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Dead)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, _rx) = mk_coordinator(registry, backend.clone());
    let report = HealthReport {
        suspect: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 1 },
        )],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();
    assert_eq!(result.failed.len(), 1);
    assert!(result.failed[0].1.contains("invalid transition"));

    // State must remain Dead.
    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Dead));
}

#[tokio::test]
async fn stuck_agent_marked_suspect() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, _rx) = mk_coordinator(registry, backend.clone());
    let report = HealthReport {
        stuck: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Stuck { stuck_seconds: 700 },
        )],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();
    assert_eq!(result.transitioned.len(), 1);
    assert_eq!(result.transitioned[0].2, AgentState::Suspect);
}

#[tokio::test]
async fn lifecycle_event_broadcasted() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, mut rx) = mk_coordinator(registry, backend);
    let report = HealthReport {
        suspect: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 1 },
        )],
        ..Default::default()
    };

    coord.handle_health_report(&report).await.unwrap();

    let event = rx.try_recv();
    assert!(event.is_ok(), "expected lifecycle event broadcast");
    assert!(
        matches!(event.unwrap(), SupervisorEvent::AgentUnhealthy { .. }),
        "expected AgentUnhealthy event"
    );
}
