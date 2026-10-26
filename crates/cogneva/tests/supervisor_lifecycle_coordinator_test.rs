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
    // "From" is read before the CAS; read after and it is already the target,
    // so every row of this log would read Suspect -> Suspect and the frame on
    // the bus would report a transition out of a state the agent never held.
    assert_eq!(result.transitioned[0].1, AgentState::Active);
    assert_eq!(result.transitioned[0].2, AgentState::Suspect);

    let state = backend.get_agent_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Suspect));
}

/// The roster's state cell has no source but frames -- `AgentInfo` carries no
/// state -- and nothing in the tree ever built a `StateChange`. So this is the
/// one place that turns a stored transition into something a screen can move
/// on, and the values it writes are what the gateway's level filter and the
/// roster's label compare against.
#[tokio::test]
async fn a_transition_that_landed_is_announced_on_the_agent_bus() {
    let backend = mk_backend();
    backend
        .set_agent_state("a-1", &AgentState::Active)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "a-1".into(),
        role: None,
        crew_id: Some("c-1".into()),
        squad_id: Some("s-1".into()),
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (agent_tx, mut agent_rx) = broadcast::channel(64);
    let (supervisor_tx, _supervisor_rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry, backend.clone(), supervisor_tx)
        .with_agent_events(agent_tx);

    let report = HealthReport {
        dead: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
        )],
        ..Default::default()
    };
    coord.handle_health_report(&report).await.unwrap();

    match agent_rx
        .try_recv()
        .expect("the transition was not announced")
    {
        cog_core::AgentEvent::StateChange {
            agent_id,
            from,
            to,
            crew_id,
            squad_id,
            ..
        } => {
            assert_eq!(agent_id, "a-1");
            assert_eq!(from, "active");
            assert_eq!(to, "dead");
            assert_eq!(crew_id.as_deref(), Some("c-1"));
            assert_eq!(squad_id.as_deref(), Some("s-1"));
        }
        other => panic!("expected a state change, got {other:?}"),
    }
}

/// A refused transition is not announced: the store still holds the old state,
/// and a frame saying otherwise would make the screen the optimistic reader.
#[tokio::test]
async fn a_refused_transition_is_not_announced() {
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

    let (agent_tx, mut agent_rx) = broadcast::channel(64);
    let (supervisor_tx, _supervisor_rx) = broadcast::channel(64);
    let coord = LifecycleCoordinator::new(registry, backend.clone(), supervisor_tx)
        .with_agent_events(agent_tx);

    let report = HealthReport {
        suspect: vec![(
            "a-1".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 2 },
        )],
        ..Default::default()
    };
    let result = coord.handle_health_report(&report).await.unwrap();

    assert_eq!(result.transitioned.len(), 0);
    assert!(
        agent_rx.try_recv().is_err(),
        "a refused transition was announced"
    );
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
    // A transition emits exactly that one event. A `Tick` used to follow it as
    // a stand-in heartbeat, carrying a hardcoded `cycle: 0`; but `Tick.cycle`
    // is the health-check loop's cycle number, so a lifecycle transition
    // writing 0 there read to every cache as "no health-check has run yet" --
    // a reading that moves backwards whenever an agent goes suspect, dies or
    // gets stuck. Nothing consumed that Tick, so this asserts the transition
    // stays a single, truthful event.
    assert!(
        rx.try_recv().is_err(),
        "a lifecycle transition must emit one event, not a heartbeat that \
         overwrites the health-check cycle with 0"
    );
}

/// 离队的 id 在这一条路上也不许发事件。
///
/// 这条通道有两个写者：监督自己收尾时的 `announce_verdicts`，和这里的生命周期
/// 协调器，而这里**先跑**。只在收尾那处加过滤器，等于给一条有两扇门的路只锁了
/// 一扇——现网读数会一动不动。两处读同一个 `HealthReport::announces`。
///
/// 门的两半：事件一条都不许出来（判决不算「不健康」），而状态迁移照旧（离队的
/// agent 的任务还要有人接手，走了不等于不用管）。
#[tokio::test]
async fn a_departed_id_emits_no_lifecycle_event_but_still_transitions() {
    let backend = mk_backend();
    backend
        .set_agent_state("squad:one-shot-planner", &AgentState::Active)
        .await
        .unwrap();
    let registry = mk_registry();
    registry.register_agent(AgentInfo {
        agent_id: "squad:one-shot-planner".into(),
        role: None,
        crew_id: None,
        squad_id: None,
        task_ids: vec![],
        last_heartbeat: Utc::now(),
        state_since: Utc::now(),
        registered_at: Utc::now(),
    });

    let (coord, mut rx) = mk_coordinator(registry, Arc::clone(&backend));
    let report = HealthReport {
        dead: vec![(
            "squad:one-shot-planner".into(),
            cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
        )],
        departed: vec!["squad:one-shot-planner".into()],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();

    assert!(
        rx.try_recv().is_err(),
        "名单里已经没有它了：这条判决不是「不健康的 agent」"
    );
    assert_eq!(result.events_emitted, 0);
    assert_eq!(
        result.transitioned.len(),
        1,
        "迁移照旧：离队的 agent 的任务还要有人接手"
    );
    assert_eq!(
        backend
            .get_agent_state("squad:one-shot-planner")
            .await
            .unwrap(),
        Some(AgentState::Dead)
    );
}

/// The three branches that emit `AgentUnhealthy` are three separate guards, so
/// each needs its own leg. A file-level test would stay green while two of the
/// three were deleted, because the surviving one still names the judgement.
#[tokio::test]
async fn a_departed_suspect_emits_no_lifecycle_event_but_still_transitions() {
    let backend = mk_backend();
    backend
        .set_agent_state("squad:one-shot-suspect", &AgentState::Active)
        .await
        .unwrap();

    let (coord, mut rx) = mk_coordinator(mk_registry(), Arc::clone(&backend));
    let report = HealthReport {
        suspect: vec![(
            "squad:one-shot-suspect".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 3 },
        )],
        departed: vec!["squad:one-shot-suspect".into()],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();

    assert!(
        rx.try_recv().is_err(),
        "名单里已经没有它了：suspect 那一支也不能报"
    );
    assert_eq!(result.events_emitted, 0);
    assert_eq!(result.transitioned.len(), 1);
    assert_eq!(
        backend
            .get_agent_state("squad:one-shot-suspect")
            .await
            .unwrap(),
        Some(AgentState::Suspect)
    );
}

#[tokio::test]
async fn a_departed_stuck_agent_emits_no_lifecycle_event_but_still_transitions() {
    let backend = mk_backend();
    backend
        .set_agent_state("squad:one-shot-stuck", &AgentState::Active)
        .await
        .unwrap();

    let (coord, mut rx) = mk_coordinator(mk_registry(), Arc::clone(&backend));
    let report = HealthReport {
        stuck: vec![(
            "squad:one-shot-stuck".into(),
            cog_core::HealthIssue::Stuck { stuck_seconds: 900 },
        )],
        departed: vec!["squad:one-shot-stuck".into()],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();

    assert!(
        rx.try_recv().is_err(),
        "名单里已经没有它了：stuck 那一支也不能报"
    );
    assert_eq!(result.events_emitted, 0);
    assert_eq!(result.transitioned.len(), 1);
    assert_eq!(
        backend
            .get_agent_state("squad:one-shot-stuck")
            .await
            .unwrap(),
        Some(AgentState::Suspect)
    );
}

/// The other half, so the three above are not passed by silencing every
/// branch: an agent that is still on the roster must still be announced.
#[tokio::test]
async fn a_listed_agent_in_any_branch_is_still_announced() {
    let backend = mk_backend();
    for id in ["a-suspect", "a-dead", "a-stuck"] {
        backend
            .set_agent_state(id, &AgentState::Active)
            .await
            .unwrap();
    }

    let (coord, mut rx) = mk_coordinator(mk_registry(), Arc::clone(&backend));
    let report = HealthReport {
        suspect: vec![(
            "a-suspect".into(),
            cog_core::HealthIssue::Suspect { missed_beats: 3 },
        )],
        dead: vec![(
            "a-dead".into(),
            cog_core::HealthIssue::Dead {
                last_seen: Utc::now(),
            },
        )],
        stuck: vec![(
            "a-stuck".into(),
            cog_core::HealthIssue::Stuck { stuck_seconds: 900 },
        )],
        departed: vec![],
        ..Default::default()
    };

    let result = coord.handle_health_report(&report).await.unwrap();

    let mut announced = Vec::new();
    while let Ok(SupervisorEvent::AgentUnhealthy { agent_id, .. }) = rx.try_recv() {
        announced.push(agent_id);
    }
    announced.sort();
    assert_eq!(announced, vec!["a-dead", "a-stuck", "a-suspect"]);
    assert_eq!(result.events_emitted, 3);
}
