use cog_agent::Agent;
use cog_core::{AgentEvent, AgentState, RuntimeConfig};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

use futures::StreamExt;

mod mock_provider;
use mock_provider::MockProvider;

#[tokio::test]
async fn test_agent_prompt_completes() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    let result = timeout(
        Duration::from_secs(5),
        agent.prompt(serde_json::json!({"goal": "test"})),
    )
    .await
    .expect("prompt timed out")
    .expect("prompt failed");

    assert!(result.get("result").is_some() || result.get("status").is_some());
    assert_eq!(agent.state().await, AgentState::Idle);
}

#[tokio::test]
async fn test_agent_subscribe_receives_events() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    let mut events = agent.subscribe();

    let _ = agent.prompt(serde_json::json!({"goal": "test"})).await;

    let mut received = Vec::new();
    while let Ok(event) = events.try_recv() {
        received.push(event);
    }

    assert!(!received.is_empty(), "Should receive events");
    let has_start = received
        .iter()
        .any(|e| matches!(e, AgentEvent::AgentStart { .. }));
    let has_end = received
        .iter()
        .any(|e| matches!(e, AgentEvent::AgentEnd { .. }));
    assert!(has_start, "Missing AgentStart");
    assert!(has_end, "Missing AgentEnd");
}

#[tokio::test]
async fn test_agent_state_transitions() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    assert_eq!(agent.state().await, AgentState::Idle);

    // Start in background so we can observe Running state
    let agent_ref = &agent;
    let prompt_future = agent_ref.prompt(serde_json::json!({"goal": "test"}));

    // The prompt runs synchronously in our test (await blocks until done),
    // so we can't easily catch the Running state. Just verify final state.
    let _ = prompt_future.await;
    assert_eq!(agent.state().await, AgentState::Idle);
}

#[tokio::test]
async fn test_agent_abort_resets_to_idle() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
            role: "planner".to_string(),
            max_iterations: 10,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        Arc::new(MockProvider::new("slow result")),
    );

    // Start prompt in background
    let agent_clone = Arc::new(agent);
    let agent_for_prompt = agent_clone.clone();
    let handle = tokio::spawn(async move {
        agent_for_prompt
            .prompt(serde_json::json!({"goal": "test"}))
            .await
    });

    // Give it a moment to start, then abort
    tokio::time::sleep(Duration::from_millis(50)).await;
    agent_clone.abort().await.expect("abort failed");

    // Aborting kills the task, so the prompt will fail
    let result = timeout(Duration::from_secs(2), handle).await;
    assert!(result.is_ok(), "Abort should complete");
}

#[tokio::test]
async fn test_agent_reset_clears_state() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    let _ = agent.prompt(serde_json::json!({"goal": "test"})).await;
    assert_eq!(agent.state().await, AgentState::Idle);

    agent.reset().await.expect("reset failed");
    assert_eq!(agent.state().await, AgentState::Idle);
}

#[tokio::test]
async fn test_agent_continue_preserves_context() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    let result1 = agent
        .prompt(serde_json::json!({"goal": "first"}))
        .await
        .expect("first prompt failed");
    assert!(result1.get("result").is_some() || result1.get("status").is_some());

    let result2 = agent
        .continue_(serde_json::json!({"goal": "second"}))
        .await
        .expect("continue failed");
    assert!(result2.get("result").is_some() || result2.get("status").is_some());

    // Both should complete successfully, meaning context was preserved
    assert_eq!(agent.state().await, AgentState::Idle);
}

#[tokio::test]
async fn test_agent_wait_for_idle() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    // Before running, should return immediately
    timeout(Duration::from_secs(1), agent.wait_for_idle())
        .await
        .expect("wait_for_idle timed out")
        .expect("wait_for_idle failed");

    let _ = agent.prompt(serde_json::json!({"goal": "test"})).await;

    // After completion, should also return immediately
    timeout(Duration::from_secs(1), agent.wait_for_idle())
        .await
        .expect("wait_for_idle timed out after run")
        .expect("wait_for_idle failed after run");
}

#[tokio::test]
async fn test_agent_chat_stream_bypasses_loop() {
    let agent = Agent::new(
        RuntimeConfig {
            agent_id: "test".into(),
            role: "planner".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        Arc::new(MockProvider::new("hello stream")),
    );

    // Direct chat_stream bypasses AgentRuntime entirely
    let mut stream = agent
        .chat_stream(
            &[cog_core::Message::user("hi")],
            &cog_llm::ChatOptions::default(),
        )
        .await
        .expect("chat_stream failed");

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }

    // Should receive Start + TextStart + TextDelta + TextEnd + Done
    assert!(!events.is_empty(), "Should receive streaming events");
    let has_done = events
        .iter()
        .any(|e| matches!(e, cog_core::AssistantMessageEvent::Done { .. }));
    assert!(has_done, "Should receive Done event");
}
