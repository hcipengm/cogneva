use cog_core::{
    AgentCheckpoint, AgentState, CheckpointStore, ContextBoard, Event, StateBackend, TaskCheckpoint,
};
use cog_storage::{MemorySnapshotStore, MemoryStateBackend};

#[tokio::test]
async fn test_memory_agent_state() {
    let backend = MemoryStateBackend::new();

    // Initially absent
    let state = backend.get_agent_state("agent-1").await.unwrap();
    assert!(state.is_none());

    // Set and retrieve
    backend
        .set_agent_state("agent-1", &AgentState::Active)
        .await
        .unwrap();
    let state = backend.get_agent_state("agent-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));

    // Update
    backend
        .set_agent_state("agent-1", &AgentState::Idle)
        .await
        .unwrap();
    let state = backend.get_agent_state("agent-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Idle));
}

#[tokio::test]
async fn test_memory_checkpoint() {
    let backend = MemoryStateBackend::new();

    let cp = TaskCheckpoint {
        task_id: "task-1".into(),
        snapshot_id: "snap-a".into(),
        event_offset: 42,
        timestamp: chrono::Utc::now(),
    };

    backend.save_checkpoint(&cp).await.unwrap();
    let fetched = backend.get_checkpoint("task-1").await.unwrap();
    assert_eq!(fetched, Some(cp));

    let missing = backend.get_checkpoint("task-2").await.unwrap();
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_memory_events() {
    let backend = MemoryStateBackend::new();

    let e1 = Event {
        offset: 0,
        task_id: "task-1".into(),
        event_type: "start".into(),
        payload: serde_json::json!({"hello": "world"}),
        timestamp: chrono::Utc::now(),
    };
    let e2 = Event {
        offset: 1,
        task_id: "task-1".into(),
        event_type: "end".into(),
        payload: serde_json::json!({"status": "ok"}),
        timestamp: chrono::Utc::now(),
    };

    let len = backend.append_event("task-1", &e1).await.unwrap();
    assert_eq!(len, 1);
    let len = backend.append_event("task-1", &e2).await.unwrap();
    assert_eq!(len, 2);

    let events = backend.get_events("task-1", 0, 10).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event_type, "start");
    assert_eq!(events[1].event_type, "end");

    let paginated = backend.get_events("task-1", 1, 1).await.unwrap();
    assert_eq!(paginated.len(), 1);
    assert_eq!(paginated[0].event_type, "end");

    let empty = backend.get_events("task-1", 5, 10).await.unwrap();
    assert!(empty.is_empty());
}

#[tokio::test]
async fn test_memory_board() {
    let backend = MemoryStateBackend::new();

    let board = backend.get_board("task-1").await.unwrap();
    assert!(board.is_none());

    backend
        .set_board_field("task-1", "key1", "value1")
        .await
        .unwrap();
    backend
        .set_board_field("task-1", "key2", "value2")
        .await
        .unwrap();

    let board = backend.get_board("task-1").await.unwrap().unwrap();
    assert_eq!(board.task_id, "task-1");
    assert_eq!(board.fields.get("key1"), Some(&"value1".to_string()));
    assert_eq!(board.fields.get("key2"), Some(&"value2".to_string()));

    // Update existing field
    backend
        .set_board_field("task-1", "key1", "updated")
        .await
        .unwrap();
    let board = backend.get_board("task-1").await.unwrap().unwrap();
    assert_eq!(board.fields.get("key1"), Some(&"updated".to_string()));
}

#[tokio::test]
async fn test_agent_state_variants() {
    let backend = MemoryStateBackend::new();

    for state in [
        AgentState::Init,
        AgentState::Registered,
        AgentState::Active,
        AgentState::Idle,
        AgentState::Completing,
        AgentState::Inactive,
        AgentState::Suspect,
        AgentState::Dead,
    ] {
        let id = format!("agent-{:?}", state);
        backend.set_agent_state(&id, &state).await.unwrap();
        let got = backend.get_agent_state(&id).await.unwrap();
        assert_eq!(got, Some(state));
    }
}

#[tokio::test]
async fn test_context_board_default() {
    let board = ContextBoard {
        task_id: "t".into(),
        ..Default::default()
    };
    assert!(board.fields.is_empty());
}

// ---------------------------------------------------------------------------
// SnapshotStore tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_memory_snapshot_store_roundtrip() {
    let store = MemorySnapshotStore::new();

    let snap = AgentCheckpoint {
        checkpoint_id: "snap-1".into(),
        task_id: "task-1".into(),
        agent_state: serde_json::json!({"state": "active"}),
        context_window: Vec::new(),
        event_offset: 42,
        timestamp: chrono::Utc::now(),
    };

    let id = store.save(&snap).await.unwrap();
    assert_eq!(id, "snap-1");

    let loaded = store.load("snap-1").await.unwrap();
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().checkpoint_id, "snap-1");

    let missing = store.load("snap-missing").await.unwrap();
    assert!(missing.is_none());
}
