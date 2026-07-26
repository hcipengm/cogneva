use chrono::{NaiveDate, Utc};
use cog_core::{ClusterOverview, EventFilter, LogEntry, RawLogIndex, TaskMetrics};

#[test]
fn test_event_filter_default() {
    let f = EventFilter::default();
    assert!(f.agent_id.is_none());
    assert!(f.task_id.is_none());
    assert!(f.event_types.is_none());
    assert!(f.since.is_none());
}

#[test]
fn test_task_metrics_serialization() {
    let m = TaskMetrics {
        task_id: "t-1".into(),
        total_tokens: 1000,
        prompt_tokens: 400,
        completion_tokens: 600,
        tool_calls: 3,
        iterations: 2,
        duration_ms: 1234,
        timestamp: Utc::now(),
    };
    let json = serde_json::to_string(&m).unwrap();
    assert!(json.contains("\"task_id\":\"t-1\""));
    let recovered: TaskMetrics = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.total_tokens, 1000);
}

#[test]
fn test_log_entry_serialization() {
    let e = LogEntry {
        timestamp: Utc::now(),
        level: "INFO".into(),
        source: "cog-agents".into(),
        message: "agent started".into(),
        metadata: serde_json::json!({"agent_id": "a-1"}),
    };
    let json = serde_json::to_string(&e).unwrap();
    assert!(json.contains("INFO"));
    let recovered: LogEntry = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.source, "cog-agents");
}

#[test]
fn test_raw_log_index_serialization() {
    let idx = RawLogIndex {
        stream: "session_raw".into(),
        date: NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
        file_path: "/data/session_raw/2026-04-26.pb".into(),
        encoding: "protobuf".into(),
        record_count: 42,
        byte_size: 8192,
        created_at: Utc::now(),
    };
    let json = serde_json::to_string(&idx).unwrap();
    assert!(json.contains("session_raw"));
    let recovered: RawLogIndex = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.record_count, 42);
}

#[test]
fn test_cluster_overview_serialization() {
    let o = ClusterOverview {
        total_agents: 10,
        active_agents: 7,
        total_tasks: 100,
        active_tasks: 20,
        queued_tasks: 5,
        failed_tasks: 2,
        avg_task_duration_ms: 1500,
        cluster_health: "healthy".into(),
        timestamp: Utc::now(),
        total_squads: 6,
        active_squads: 4,
    };
    let json = serde_json::to_string(&o).unwrap();
    assert!(json.contains("healthy"));
    let recovered: ClusterOverview = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.active_tasks, 20);
    assert_eq!(recovered.total_squads, 6);
    assert_eq!(recovered.active_squads, 4);
}
