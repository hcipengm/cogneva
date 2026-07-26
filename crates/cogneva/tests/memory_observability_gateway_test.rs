use chrono::{NaiveDate, Utc};
use cog_core::{
    AgentEvent, EventFilter, LogEntry, ObservabilityGateway, RawLogIndex, SquadState, SquadStatus,
    TaskMetrics,
};
use cog_storage::{MemoryObservabilityGateway, MemoryStateBackend};
use futures::StreamExt;
use std::sync::Arc;

fn mk_gateway() -> MemoryObservabilityGateway {
    MemoryObservabilityGateway::new(Arc::new(MemoryStateBackend::new()))
}

#[tokio::test]
async fn test_gateway_event_subscription() {
    let gw = mk_gateway();
    let mut stream = gw
        .subscribe_events(EventFilter {
            agent_id: Some("a-1".into()),
            ..Default::default()
        })
        .await
        .unwrap();

    gw.publish_event(AgentEvent::AgentStart {
        agent_id: "a-1".into(),
        crew_id: None,
        squad_id: None,
        timestamp: Utc::now(),
    });
    gw.publish_event(AgentEvent::AgentStart {
        agent_id: "a-2".into(),
        crew_id: None,
        squad_id: None,
        timestamp: Utc::now(),
    });

    let event = stream.next().await.unwrap().unwrap();
    match event {
        AgentEvent::AgentStart { agent_id, .. } => assert_eq!(agent_id, "a-1"),
        _ => panic!("expected AgentStart"),
    }
}

#[tokio::test]
async fn test_gateway_metrics_and_logs() {
    let gw = mk_gateway();

    gw.record_metrics(TaskMetrics {
        task_id: "t-1".into(),
        total_tokens: 100,
        prompt_tokens: 40,
        completion_tokens: 60,
        tool_calls: 2,
        iterations: 3,
        duration_ms: 500,
        timestamp: Utc::now(),
    });

    gw.record_log(
        "t-1",
        LogEntry {
            timestamp: Utc::now(),
            level: "INFO".into(),
            source: "test".into(),
            message: "started".into(),
            metadata: serde_json::json!(null),
        },
    );

    let metrics = gw.get_task_metrics("t-1").await.unwrap();
    assert_eq!(metrics.total_tokens, 100);

    let logs = gw.get_task_logs("t-1", 10).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].message, "started");
}

#[tokio::test]
async fn test_gateway_snapshot_and_raw_index() {
    let gw = mk_gateway();

    gw.register_snapshot("snap-1", "https://cos.example.com/snap-1.json");
    let url = gw.get_snapshot_url("snap-1").await.unwrap();
    assert_eq!(url, "https://cos.example.com/snap-1.json");

    gw.register_raw_index(RawLogIndex {
        stream: "session_raw".into(),
        date: NaiveDate::from_ymd_opt(2026, 4, 26).unwrap(),
        file_path: "/data/2026-04-26.pb".into(),
        encoding: "protobuf".into(),
        record_count: 10,
        byte_size: 1024,
        created_at: Utc::now(),
    });

    let idx = gw
        .get_raw_log_index("session_raw", NaiveDate::from_ymd_opt(2026, 4, 26).unwrap())
        .await
        .unwrap();
    assert_eq!(idx.len(), 1);
    assert_eq!(idx[0].record_count, 10);
}

#[tokio::test]
async fn test_gateway_cluster_overview() {
    let gw = mk_gateway();
    gw.record_metrics(TaskMetrics {
        task_id: "t-1".into(),
        total_tokens: 100,
        prompt_tokens: 40,
        completion_tokens: 60,
        tool_calls: 1,
        iterations: 2,
        duration_ms: 1000,
        timestamp: Utc::now(),
    });

    let overview = gw.get_cluster_overview().await.unwrap();
    assert_eq!(overview.total_tasks, 1);
    assert_eq!(overview.avg_task_duration_ms, 1000);
}

#[tokio::test]
async fn test_gateway_event_subscription_with_event_types_filter() {
    let gw = mk_gateway();
    let mut stream = gw
        .subscribe_events(EventFilter {
            event_types: Some(vec!["agent_start".into()]),
            ..Default::default()
        })
        .await
        .unwrap();

    gw.publish_event(AgentEvent::AgentStart {
        agent_id: "a-1".into(),
        crew_id: None,
        squad_id: None,
        timestamp: Utc::now(),
    });
    gw.publish_event(AgentEvent::AgentEnd {
        agent_id: "a-1".into(),
        messages: vec![],
        crew_id: None,
        squad_id: None,
        timestamp: Utc::now(),
    });

    let event = stream.next().await.unwrap().unwrap();
    match event {
        AgentEvent::AgentStart { agent_id, .. } => assert_eq!(agent_id, "a-1"),
        _ => panic!("expected AgentStart"),
    }
}

#[tokio::test]
async fn test_gateway_squad_state() {
    let gw = mk_gateway();

    let squad = SquadState {
        squad_id: "squad-1".into(),
        task_id: "task-1".into(),
        status: SquadStatus::Running,
        agents: vec![],
        completion_pct: 0.0,
        retry_count: 0,
        snapshot_id: None,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    gw.update_squad_state("squad-1", squad.clone());

    let fetched_squad = gw.get_squad_state("squad-1").await.unwrap();
    assert_eq!(fetched_squad.squad_id, "squad-1");
    assert_eq!(fetched_squad.task_id, "task-1");

    // Missing squad returns error
    assert!(gw.get_squad_state("missing").await.is_err());
}

#[tokio::test]
async fn test_gateway_cluster_overview_with_squads() {
    let gw = mk_gateway();

    gw.record_metrics(TaskMetrics {
        task_id: "t-1".into(),
        total_tokens: 100,
        prompt_tokens: 40,
        completion_tokens: 60,
        tool_calls: 1,
        iterations: 2,
        duration_ms: 1000,
        timestamp: Utc::now(),
    });

    gw.update_squad_state(
        "squad-1",
        SquadState {
            squad_id: "squad-1".into(),
            task_id: "task-1".into(),
            status: SquadStatus::Running,
            agents: vec![],
            completion_pct: 0.0,
            retry_count: 0,
            snapshot_id: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        },
    );

    let overview = gw.get_cluster_overview().await.unwrap();
    assert_eq!(overview.total_tasks, 1);
    assert_eq!(overview.total_squads, 1);
    assert_eq!(overview.active_squads, 1);
}
