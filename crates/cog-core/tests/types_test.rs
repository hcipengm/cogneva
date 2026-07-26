use cog_core::{AgentEvent, Message, StreamEvent, Task, TaskStatus, TaskType};

#[test]
fn test_message_system() {
    let msg = Message::system("test");
    assert_eq!(msg.role(), "system");
    assert_eq!(msg.content(), "test");
}

#[test]
fn test_message_user() {
    let msg = Message::user("hello");
    assert_eq!(msg.role(), "user");
    assert_eq!(msg.content(), "hello");
}

#[test]
fn test_message_assistant() {
    let msg = Message::assistant_text("response");
    assert_eq!(msg.role(), "assistant");
    assert_eq!(msg.content(), "response");
}

#[test]
fn test_message_serialization() {
    let msg = Message::user("test content");
    let json = serde_json::to_string(&msg).unwrap();
    assert!(json.contains("user"));
    assert!(json.contains("test content"));
}

#[test]
fn test_stream_event_text_delta() {
    let event = StreamEvent::TextDelta {
        delta: "hello".into(),
        timestamp: chrono::Utc::now(),
    };
    match event {
        StreamEvent::TextDelta { delta, .. } => assert_eq!(delta, "hello"),
        _ => panic!("Expected TextDelta"),
    }
}

#[test]
fn test_agent_event_state_change() {
    let event = AgentEvent::StateChange {
        agent_id: "agent-1".into(),
        from: "idle".into(),
        to: "thinking".into(),
        crew_id: None,
        squad_id: None,
        timestamp: chrono::Utc::now(),
    };
    match event {
        AgentEvent::StateChange {
            agent_id, from, to, ..
        } => {
            assert_eq!(agent_id, "agent-1");
            assert_eq!(from, "idle");
            assert_eq!(to, "thinking");
        }
        _ => panic!("Expected StateChange"),
    }
}

#[test]
fn test_task_creation() {
    let task = Task::new(
        "task-1",
        TaskType::Planner,
        serde_json::json!({"key": "value"}),
    );
    assert_eq!(task.id, "task-1");
    assert_eq!(task.status, TaskStatus::Pending);
    assert_eq!(task.task_type, TaskType::Planner);
}
