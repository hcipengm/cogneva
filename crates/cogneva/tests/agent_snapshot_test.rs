use cog_agent::{Agent, AgentRuntime, RuntimeState};
use cog_core::RuntimeConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use cog_agent::wal::AgentWal;
use cog_storage::wal::MemoryWalBackend;

mod mock_provider;
use mock_provider::MockProvider;

#[tokio::test]
async fn test_agent_loop_restore_preserves_context() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "restore-agent".into(),
            role: "planner".to_string(),
            max_iterations: 3,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    // Run the agent once to populate context
    let llm = Arc::new(MockProvider::new("analysis complete"));
    let _ = agent_loop
        .run(serde_json::json!({"query": "test"}), llm.as_ref())
        .await
        .unwrap();

    assert_eq!(agent_loop.state(), RuntimeState::Complete);
    let original_message_count = agent_loop.get_context().messages().len();
    assert!(original_message_count > 1); // system + user + assistant

    // Capture snapshot
    let snap = agent_loop.checkpoint("task-restore").unwrap();
    assert_eq!(snap.task_id, "task-restore");
    assert_eq!(snap.context_window.len(), original_message_count);

    // Create a fresh agent loop and restore
    let (event_tx2, _event_rx2) = mpsc::channel(128);
    let mut restored_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "restore-agent".into(),
            role: "planner".to_string(),
            max_iterations: 3,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx2,
    );

    restored_loop.restore(&snap).unwrap();

    // Context should be restored
    assert_eq!(
        restored_loop.get_context().messages().len(),
        original_message_count
    );
    assert_eq!(restored_loop.state(), RuntimeState::Complete);
}

#[tokio::test]
async fn test_agent_loop_restore_changes_role_and_state() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "role-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let snap = agent_loop.checkpoint("task-role").unwrap();

    // Create a fresh loop with different role and restore
    let (event_tx2, _event_rx2) = mpsc::channel(128);
    let mut restored_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "role-agent".into(),
            role: "generator".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx2,
    );

    restored_loop.restore(&snap).unwrap();

    // Role should be restored to Planner from snapshot
    assert_eq!(restored_loop.role(), "planner");
}

#[tokio::test]
async fn test_agent_snapshot_and_restore() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "wrapper-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        Arc::new(MockProvider::new("result")),
    );

    // Run the agent
    let result = agent
        .prompt(serde_json::json!({"goal": "test"}))
        .await
        .expect("prompt failed");
    assert!(result.get("result").is_some() || result.get("status").is_some());

    // Take a snapshot
    let snap = timeout(Duration::from_secs(2), agent.snapshot("task-1"))
        .await
        .expect("snapshot timed out")
        .expect("snapshot failed");

    assert_eq!(snap.task_id, "task-1");
    assert!(!snap.context_window.is_empty());

    // Restore from snapshot
    agent.restore(&snap).await.expect("restore failed");

    // After restore, agent should be idle and ready to run
    let state = agent.state().await;
    assert!(
        matches!(state, cog_core::AgentState::Idle),
        "Expected Idle after restore, got {:?}",
        state
    );
}

#[tokio::test]
async fn test_agent_restore_allows_continue() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "continue-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        Arc::new(MockProvider::new("acknowledged")),
    );

    // First prompt
    let result1 = agent
        .prompt(serde_json::json!({"goal": "first"}))
        .await
        .expect("first prompt failed");
    assert!(result1.get("result").is_some() || result1.get("status").is_some());

    // Snapshot
    let snap = agent
        .snapshot("task-continue")
        .await
        .expect("snapshot failed");

    // Restore
    agent.restore(&snap).await.expect("restore failed");

    // Should be able to continue after restore
    let result2 = timeout(
        Duration::from_secs(5),
        agent.continue_(serde_json::json!({"goal": "second"})),
    )
    .await
    .expect("continue timed out")
    .expect("continue failed");

    assert!(result2.get("result").is_some() || result2.get("status").is_some());
}

#[tokio::test]
async fn test_snapshot_event_replay_with_wal() {
    let wal_backend = Arc::new(MemoryWalBackend::new());
    let agent_wal = AgentWal::new(wal_backend.clone(), "session-replay")
        .await
        .expect("wal init failed");

    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "replay-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    )
    .with_wal(Arc::new(agent_wal));

    // Run to generate WAL entries
    let llm = Arc::new(MockProvider::new("done"));
    let _ = agent_loop
        .run(serde_json::json!({"query": "test"}), llm.as_ref())
        .await;

    // Capture snapshot (will record current WAL offset)
    let snap = agent_loop.checkpoint("task-replay").unwrap();
    let offset_before = snap.event_offset;

    // Run again to generate more WAL entries after the snapshot
    let _ = agent_loop
        .run(serde_json::json!({"query": "again"}), llm.as_ref())
        .await;

    // Create fresh loop with same WAL and restore
    let agent_wal2 = AgentWal::new(wal_backend, "session-replay")
        .await
        .expect("wal2 init failed");
    let (event_tx2, mut event_rx2) = mpsc::channel(128);
    let mut restored_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "replay-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx2,
    )
    .with_wal(Arc::new(agent_wal2));

    restored_loop.restore(&snap).unwrap();

    // Replay events from the snapshot offset
    let replayed = restored_loop
        .replay_events(offset_before)
        .await
        .expect("replay failed");

    // Should have replayed some events (at least the second run's events)
    assert!(replayed > 0, "Expected replayed events, got {replayed}");

    // Collect replayed events
    let mut replayed_events = Vec::new();
    while let Ok(event) = event_rx2.try_recv() {
        replayed_events.push(event);
    }
    assert!(
        !replayed_events.is_empty(),
        "Should receive replayed events through channel"
    );
}

#[tokio::test]
async fn test_snapshot_without_wal_has_zero_offset() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "no-wal-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let snap = agent_loop.checkpoint("task-no-wal").unwrap();
    assert_eq!(snap.event_offset, 0);
}

#[tokio::test]
async fn test_restore_resets_steps() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "steps-agent".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let llm = Arc::new(MockProvider::new("done"));
    let _ = agent_loop
        .run(serde_json::json!({"query": "test"}), llm.as_ref())
        .await;

    assert!(!agent_loop.steps().is_empty(), "Should have recorded steps");

    let snap = agent_loop.checkpoint("task-steps").unwrap();

    // Restore should clear steps (they are ephemeral)
    agent_loop.restore(&snap).unwrap();
    assert!(
        agent_loop.steps().is_empty(),
        "Steps should be cleared after restore"
    );
}
