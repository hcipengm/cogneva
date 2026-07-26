use cog_core::{
    AgentCheckpoint, AgentRegistration, CheckpointStore, Event, MessageBackend, ResourceInfo,
    StateBackend,
};
use cog_orchestrator::dag_executor::task_transfer::{
    StaleTaskDetector, TaskTransferCoordinator, TaskTransferEvent, TransferReason,
};
use cog_storage::{MemoryAgentRegistry, MemorySnapshotStore, MemoryStateBackend};
use cog_stream::MemoryMessageBackend;
use futures::StreamExt;
use std::sync::Arc;

fn make_backend() -> Arc<dyn MessageBackend> {
    Arc::new(MemoryMessageBackend::new())
}

fn make_state() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

fn make_checkpoints() -> Arc<dyn CheckpointStore> {
    Arc::new(MemorySnapshotStore::new())
}

#[tokio::test]
async fn publish_transfer_lands_on_stream() {
    let backend = make_backend();
    let coordinator =
        TaskTransferCoordinator::new(backend.clone(), make_checkpoints(), make_state());

    // Subscribe before publishing so the in-memory backend buffers nothing.
    let mut sub = backend
        .subscribe(coordinator.stream_name(), "test-group")
        .await
        .unwrap();

    let event = coordinator
        .transfer_task("t-1", "agent-A", TransferReason::GracefulShutdown, None, 0)
        .await
        .unwrap();
    assert_eq!(event.task_id, "t-1");

    let (_, payload) = sub.next().await.unwrap().unwrap();
    let received: TaskTransferEvent = serde_json::from_slice(&payload).unwrap();
    assert_eq!(received.task_id, "t-1");
    assert_eq!(received.from_agent, "agent-A");
    assert_eq!(received.reason, TransferReason::GracefulShutdown);
}

#[tokio::test]
async fn recover_loads_snapshot_and_replays_events() {
    let snapshots = make_checkpoints();
    let state = make_state();
    let coordinator =
        TaskTransferCoordinator::new(make_backend(), snapshots.clone(), state.clone());

    // Persist a snapshot at offset = 2.
    let snap = AgentCheckpoint {
        checkpoint_id: "snap-1".into(),
        task_id: "t-7".into(),
        agent_state: serde_json::json!({"step": 2}),
        context_window: vec![],
        event_offset: 2,
        timestamp: chrono::Utc::now(),
    };
    snapshots.save(&snap).await.unwrap();

    // Persist 5 events; only the ones at offset >= 2 should replay.
    for i in 0..5u64 {
        let event = Event {
            offset: i,
            task_id: "t-7".into(),
            event_type: "step".into(),
            payload: serde_json::json!({"i": i}),
            timestamp: chrono::Utc::now(),
        };
        state.append_event("t-7", &event).await.unwrap();
    }

    let recovered = coordinator.recover("t-7", Some("snap-1")).await.unwrap();
    assert_eq!(recovered.task_id, "t-7");
    assert_eq!(
        recovered.checkpoint.as_ref().unwrap().checkpoint_id,
        "snap-1"
    );
    // 5 total events, replay starts at offset 2 → 3 events
    assert_eq!(recovered.events.len(), 3);
    assert_eq!(recovered.events[0].payload["i"], 2);
}

#[tokio::test]
async fn recover_without_snapshot_starts_from_zero() {
    let state = make_state();
    let coordinator =
        TaskTransferCoordinator::new(make_backend(), make_checkpoints(), state.clone());

    let event = Event {
        offset: 0,
        task_id: "t-9".into(),
        event_type: "init".into(),
        payload: serde_json::json!({}),
        timestamp: chrono::Utc::now(),
    };
    state.append_event("t-9", &event).await.unwrap();

    let recovered = coordinator.recover("t-9", None).await.unwrap();
    assert!(recovered.checkpoint.is_none());
    assert_eq!(recovered.events.len(), 1);
}

#[tokio::test]
async fn stale_task_detector_transfers_dead_owners() {
    let registry: Arc<dyn cog_core::AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
    let backend = make_backend();
    let coordinator = Arc::new(TaskTransferCoordinator::new(
        backend.clone(),
        make_checkpoints(),
        make_state(),
    ));

    let alive = AgentRegistration::new(
        "agent-alive",
        "host",
        "10.0.0.1",
        "planner",
        "ws",
        vec![],
        ResourceInfo::default(),
    );
    registry.register(&alive).await.unwrap();

    let detector = StaleTaskDetector::new(registry.clone(), coordinator.clone());
    // Track one task owned by an alive agent — should NOT transfer.
    detector.track("task-alive", &alive.agent_id, None, 0).await;
    // Track one task owned by a dead agent — should transfer.
    detector
        .track("task-dead", "dead-agent-id", Some("snap-7".into()), 5)
        .await;

    let transferred = detector.sweep().await.unwrap();
    assert_eq!(transferred.len(), 1);
    assert_eq!(transferred[0].task_id, "task-dead");
    assert_eq!(transferred[0].reason, TransferReason::DeadAgent);
    assert_eq!(transferred[0].checkpoint_id.as_deref(), Some("snap-7"));
    assert_eq!(transferred[0].checkpoint_version, 5);

    // Second sweep is idempotent — already-transferred tasks are skipped.
    let again = detector.sweep().await.unwrap();
    assert!(again.is_empty());
}

#[tokio::test]
async fn stale_task_detector_untrack_removes_task() {
    let registry: Arc<dyn cog_core::AgentRegistry> = Arc::new(MemoryAgentRegistry::new());
    let coordinator = Arc::new(TaskTransferCoordinator::new(
        make_backend(),
        make_checkpoints(),
        make_state(),
    ));
    let detector = StaleTaskDetector::new(registry, coordinator);
    detector.track("t-1", "owner", None, 0).await;
    detector.untrack("t-1").await;
    let transferred = detector.sweep().await.unwrap();
    assert!(transferred.is_empty());
}
