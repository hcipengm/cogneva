use cog_agent::lifecycle::{HeartbeatConfig, LifecycleManager, StateTransitionHook};
use cog_core::{AgentState, StateBackend};
use cog_storage::MemoryStateBackend;
use std::sync::Arc;

fn mk_backend() -> Arc<dyn StateBackend> {
    Arc::new(MemoryStateBackend::new())
}

#[tokio::test]
async fn test_register_and_get_state() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_duplicate_register_is_idempotent() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    assert!(mgr.register("a-1").await.is_ok());
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_valid_transitions() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Completing).await.unwrap();
    mgr.transition("a-1", AgentState::Inactive).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
}

#[tokio::test]
async fn test_invalid_transition_rejected() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    // Dead is terminal
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());
}

#[tokio::test]
async fn test_transition_hook_fired() {
    let backend = mk_backend();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let calls2 = calls.clone();

    let hook: StateTransitionHook = Arc::new(move |id, from, to| {
        let mut v = calls2.lock().unwrap();
        v.push((id.to_string(), from, to));
    });

    let mgr = LifecycleManager::new(backend).with_transition_hook(hook);
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    let v = calls.lock().unwrap();
    assert_eq!(v.len(), 1);
    assert_eq!(
        v[0],
        ("a-1".to_string(), AgentState::Init, AgentState::Registered)
    );
}

#[tokio::test]
async fn test_heartbeat_marks_suspect_then_dead() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend.clone()).with_heartbeat_config(HeartbeatConfig {
        interval_ms: 50,
        suspect_threshold: 1,
        dead_threshold: 2,
    });

    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.start_heartbeat("a-1").await;

    // Wait for heartbeat to mark suspect then dead
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Dead));

    mgr.stop_heartbeat("a-1").await;
}

#[tokio::test]
async fn test_idempotent_same_state_transition() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Init).await.unwrap();
    mgr.transition("a-1", AgentState::Init).await.unwrap();
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Init));
}

#[tokio::test]
async fn test_cas_prevents_concurrent_overwrite() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend.clone());
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Directly mutate the backend to simulate a concurrent change
    backend
        .set_agent_state("a-1", &AgentState::Dead)
        .await
        .unwrap();

    // Transition should fail because the backend state no longer matches
    let result = mgr.transition("a-1", AgentState::Active).await;
    assert!(
        result.is_err(),
        "Expected transition to fail after concurrent state change"
    );

    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Dead));
}

#[tokio::test]
async fn test_cas_retry_succeeds_on_stale_read() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend.clone());
    mgr.register("a-1").await.unwrap();

    // Transition to Registered
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Manually set state to Idle (simulating another process moving it forward)
    backend
        .set_agent_state("a-1", &AgentState::Idle)
        .await
        .unwrap();

    // Transition Registered -> Active should retry, see Idle, and fail
    // because Idle -> Active IS valid, but our first read was Registered.
    // Wait, with CAS: read=Registered, CAS fails because current=Idle,
    // re-read=Idle, check Idle->Active is valid, CAS succeeds.
    let result = mgr.transition("a-1", AgentState::Active).await;
    assert!(
        result.is_ok(),
        "Retry should succeed when re-read state allows transition"
    );
    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));
}

#[tokio::test]
async fn test_all_valid_transitions() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);

    // Init -> Registered -> Active -> Idle -> Completing -> Inactive -> Active -> Idle -> Suspect -> Active -> Dead
    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Completing).await.unwrap();
    mgr.transition("a-1", AgentState::Inactive).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Idle).await.unwrap();
    mgr.transition("a-1", AgentState::Suspect).await.unwrap();
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
}

#[tokio::test]
async fn test_invalid_transitions_rejected() {
    let backend = mk_backend();
    let mgr = LifecycleManager::new(backend);
    mgr.register("a-1").await.unwrap();

    // Init -> Active (must go through Registered)
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());

    // Init -> Idle
    assert!(mgr.transition("a-1", AgentState::Idle).await.is_err());

    // Registered -> Suspect
    mgr.transition("a-1", AgentState::Registered).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Suspect).await.is_err());

    // Dead -> anything
    mgr.transition("a-1", AgentState::Active).await.unwrap();
    mgr.transition("a-1", AgentState::Dead).await.unwrap();
    assert!(mgr.transition("a-1", AgentState::Init).await.is_err());
    assert!(mgr.transition("a-1", AgentState::Registered).await.is_err());
    assert!(mgr.transition("a-1", AgentState::Active).await.is_err());
}

#[tokio::test]
async fn test_concurrent_transition_race() {
    let backend = mk_backend();
    let mgr = Arc::new(LifecycleManager::new(backend));

    mgr.register("a-1").await.unwrap();
    mgr.transition("a-1", AgentState::Registered).await.unwrap();

    // Spawn two concurrent transitions from Registered -> Active
    let mgr1 = mgr.clone();
    let mgr2 = mgr.clone();

    let t1 = tokio::spawn(async move { mgr1.transition("a-1", AgentState::Active).await });
    let t2 = tokio::spawn(async move { mgr2.transition("a-1", AgentState::Active).await });

    let r1 = t1.await.unwrap();
    let r2 = t2.await.unwrap();

    // At least one must succeed; the other may succeed (idempotent same-state)
    // or fail due to CAS race.
    assert!(
        r1.is_ok() || r2.is_ok(),
        "At least one concurrent transition should succeed"
    );

    let state = mgr.get_state("a-1").await.unwrap();
    assert_eq!(state, Some(AgentState::Active));
}
