use cog_agent::{AgentRuntime, ReActLoop, RuntimeState};
use cog_core::{AgentEvent, RuntimeConfig, ToolRegistry};
use std::sync::Arc;
use tokio::sync::mpsc;

mod mock_provider;
use mock_provider::MockProvider;

#[tokio::test]
async fn test_agent_loop_completes_without_tools() {
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "test".into(),
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

    let llm = Arc::new(MockProvider::new("Analysis complete. Result: 42"));
    let result = agent_loop
        .run(serde_json::json!({"query": "test"}), llm.as_ref())
        .await
        .unwrap();

    assert!(result.get("result").is_some() || result.get("status").is_some());
    assert_eq!(agent_loop.state(), RuntimeState::Complete);

    // Drain events
    let mut events = Vec::new();
    while let Ok(event) = event_rx.try_recv() {
        events.push(event);
    }
    assert!(!events.is_empty());

    // Verify lifecycle events
    let has_start = events
        .iter()
        .any(|e| matches!(e, AgentEvent::AgentStart { .. }));
    let has_end = events
        .iter()
        .any(|e| matches!(e, AgentEvent::AgentEnd { .. }));
    assert!(has_start, "Missing AgentStart event");
    assert!(has_end, "Missing AgentEnd event");
}

#[tokio::test]
async fn test_agent_loop_state_transitions() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
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
        event_tx,
    );

    assert_eq!(agent_loop.state(), RuntimeState::Idle);

    let llm = Arc::new(MockProvider::new("done"));
    let _ = agent_loop.run(serde_json::json!({}), llm.as_ref()).await;

    assert_eq!(agent_loop.state(), RuntimeState::Complete);
}

#[tokio::test]
async fn test_agent_loop_steps_recorded() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "test".into(),
            role: "generator".to_string(),
            max_iterations: 2,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let llm = Arc::new(MockProvider::new("result"));
    let _ = agent_loop
        .run(serde_json::json!({"spec": "test"}), llm.as_ref())
        .await;

    let steps = agent_loop.steps();
    assert!(!steps.is_empty(), "Steps should be recorded");
}

#[tokio::test]
async fn test_agent_loop_max_iterations() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "test".into(),
            role: "planner".to_string(),
            max_iterations: 1,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    // Provide a tool call so the loop wants to continue, but max_iterations=1
    // forces it to bail out with max_iterations_reached.
    let llm = Arc::new(
        MockProvider::new("need tool").with_tool_call(cog_core::ToolCall {
            id: "tc-1".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path": "/tmp/test"}),
        }),
    );
    let result = agent_loop
        .run(serde_json::json!({}), llm.as_ref())
        .await
        .unwrap();

    assert_eq!(
        result.get("status").and_then(|v| v.as_str()),
        Some("max_iterations_reached")
    );
}

#[tokio::test]
async fn test_agent_loop_chat_stream_returns_raw_stream() {
    use futures::StreamExt;

    let (event_tx, _event_rx) = mpsc::channel(128);
    let mut agent_loop = AgentRuntime::new(
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
        event_tx,
    );

    let llm = Arc::new(MockProvider::new("raw stream response"));
    let mut stream = agent_loop
        .chat_stream(llm.as_ref())
        .await
        .expect("chat_stream failed");

    let mut events = Vec::new();
    while let Some(event) = stream.next().await {
        events.push(event);
    }

    assert!(!events.is_empty(), "Should receive streaming events");
    let has_done = events
        .iter()
        .any(|e| matches!(e, cog_core::AssistantMessageEvent::Done { .. }));
    assert!(has_done, "Should receive Done event");

    // The loop's context should not have been modified (no user message added)
    // because chat_stream is a read-only operation on context.
    // Context window starts empty — no system prompt is injected.
    assert_eq!(agent_loop.get_context().messages().len(), 0);
}

#[tokio::test]
async fn test_agent_loop_checkpoint() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "checkpoint-agent".into(),
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

    let snap = agent_loop.checkpoint("task-42").unwrap();

    assert_eq!(snap.task_id, "task-42");
    assert!(snap.checkpoint_id.starts_with("snap-checkpoint-agent-"));
    assert_eq!(snap.context_window.len(), 0); // no system prompt injected
    assert_eq!(snap.event_offset, 0);

    // Verify agent_state JSON payload
    assert_eq!(snap.agent_state["agent_id"], "checkpoint-agent");
    assert_eq!(snap.agent_state["role"], "planner");
    assert_eq!(snap.agent_state["state"], "idle");
}

#[tokio::test]
async fn test_react_loop_wraps_agent_loop() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "react-test".into(),
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

    let react_loop = ReActLoop::new(agent_loop);
    assert_eq!(react_loop.state(), RuntimeState::Idle);
    assert_eq!(react_loop.react_iteration_count(), 0);
}

#[tokio::test]
async fn test_react_loop_emits_react_events() {
    let (event_tx, mut event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "react-events".into(),
            role: "generator".to_string(),
            max_iterations: 3,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let mut react_loop = ReActLoop::new(agent_loop);

    // Use a mock that emits a tool call to trigger a full ReAct cycle
    let llm = Arc::new(
        MockProvider::new("need tool").with_tool_call(cog_core::ToolCall {
            id: "tc-1".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path": "/tmp/test"}),
        }),
    );

    let _ = react_loop
        .run(serde_json::json!({"spec": "test"}), llm.as_ref())
        .await;

    // Drain events and look for ReAct-specific events
    let mut has_react_start = false;
    let mut has_react_end = false;
    while let Ok(event) = event_rx.try_recv() {
        match event {
            AgentEvent::ReActStepStart { .. } => has_react_start = true,
            AgentEvent::ReActStepEnd { .. } => has_react_end = true,
            _ => {}
        }
    }

    assert!(
        has_react_start,
        "ReActStepStart event should be emitted during tool-call iteration"
    );
    assert!(
        has_react_end,
        "ReActStepEnd event should be emitted after tool results collected"
    );
}

#[tokio::test]
async fn test_react_loop_steps_extracted_correctly() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let agent_loop = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "react-steps".into(),
            role: "evaluator".to_string(),
            max_iterations: 3,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    );

    let mut react_loop = ReActLoop::new(agent_loop);

    let llm = Arc::new(
        MockProvider::new("need tool").with_tool_call(cog_core::ToolCall {
            id: "tc-1".into(),
            name: "read_file".into(),
            arguments: serde_json::json!({"path": "/tmp/test"}),
        }),
    );

    let _ = react_loop
        .run(serde_json::json!({"spec": "test"}), llm.as_ref())
        .await;

    let react_steps = react_loop.react_steps();
    assert!(
        !react_steps.is_empty(),
        "ReAct steps should be extracted from tool-call iterations"
    );

    for step in &react_steps {
        assert!(
            !step.thought.is_empty(),
            "Each ReAct step should have a thought"
        );
        assert!(
            !step.actions.is_empty(),
            "Each ReAct step should have at least one action"
        );
    }
}

#[tokio::test]
async fn test_agent_runtime_discovers_dynamically_registered_tool() {
    let (event_tx, _event_rx) = mpsc::channel(128);
    let registry = cog_agent::ToolRegistry::new();
    let runtime = AgentRuntime::new(
        RuntimeConfig {
            agent_id: "test".into(),
            role: "planner".to_string(),
            max_iterations: 3,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        },
        event_tx,
    )
    .with_tools(registry.clone());

    // Dynamically register a tool AFTER the runtime was created.
    registry.register(cog_core::Tool {
        name: "dynamic_tool".into(),
        description: "A dynamically registered tool".into(),
        parameters: serde_json::json!({
            "type": "object",
            "properties": {
                "input": { "type": "string" }
            }
        }),
        implementation: cog_core::ToolImplementation::Native(Arc::new(|args| {
            Box::pin(async move {
                let input = args["input"].as_str().unwrap_or("default");
                Ok(serde_json::json!({ "output": input }))
            })
        })),
    });

    // Verify the runtime can discover the tool.
    let tools = runtime.get_tools();
    let tool = tools.get("dynamic_tool");
    assert!(
        tool.is_some(),
        "AgentRuntime should discover dynamically registered tool"
    );

    // Verify the runtime can execute the tool.
    let result = tools
        .execute("dynamic_tool", serde_json::json!({"input": "hello"}))
        .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap()["output"], "hello");
}
