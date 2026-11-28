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

/// 执行器与调用方是分开升级的：新版执行器报得出树从哪来，旧版报不出。
/// 这个字段缺席时事件仍要读得懂，读成「不知道」——调用方宁可少说一句，
/// 也不能替执行器断言一棵刚切的树。
#[test]
fn a_workdir_event_from_an_executor_that_does_not_say_where_the_tree_came_from_still_reads() {
    let old = serde_json::json!({"type": "workdir", "path": "/srv/workspaces/a"});
    let event: cog_core::CommandEvent = serde_json::from_value(old).unwrap();
    match event {
        cog_core::CommandEvent::Workdir { path, origin } => {
            assert_eq!(path, "/srv/workspaces/a");
            assert_eq!(origin, cog_core::WorktreeOrigin::Unknown);
        }
        other => panic!("旧版执行器的首帧读不成工作目录：{other:?}"),
    }
}

#[test]
fn a_workdir_event_says_where_the_tree_came_from() {
    let event = cog_core::CommandEvent::Workdir {
        path: "/srv/workspaces/a".into(),
        origin: cog_core::WorktreeOrigin::Warm,
    };
    let line = serde_json::to_string(&event).unwrap();
    let back: cog_core::CommandEvent = serde_json::from_str(&line).unwrap();
    match back {
        cog_core::CommandEvent::Workdir { origin, .. } => {
            assert_eq!(origin, cog_core::WorktreeOrigin::Warm)
        }
        other => panic!("读回来的不是工作目录事件：{other:?}"),
    }
    assert!(line.contains("\"warm\""), "线上写法是 lowercase：{line}");
}
