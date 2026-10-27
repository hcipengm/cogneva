use chrono::{NaiveDate, Utc};
use cog_core::{ClusterHealth, ClusterOverview, EventFilter, LogEntry, RawLogIndex, TaskMetrics};

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
        total_agents: Some(10),
        active_agents: Some(7),
        total_tasks: Some(100),
        active_tasks: Some(20),
        queued_tasks: Some(5),
        failed_tasks: Some(2),
        avg_task_duration_ms: Some(1500),
        cluster_health: ClusterHealth::Healthy,
        timestamp: Utc::now(),
        total_squads: Some(6),
        active_squads: Some(4),
    };
    let json = serde_json::to_string(&o).unwrap();
    assert!(json.contains("healthy"));
    let recovered: ClusterOverview = serde_json::from_str(&json).unwrap();
    assert_eq!(recovered.active_tasks, Some(20));
    assert_eq!(recovered.total_squads, Some(6));
    assert_eq!(recovered.active_squads, Some(4));

    // 「没有读数」必须能穿过序列化，不能被折叠成 0。
    let unknown = ClusterOverview {
        total_agents: None,
        active_agents: None,
        total_tasks: None,
        active_tasks: None,
        queued_tasks: None,
        failed_tasks: None,
        avg_task_duration_ms: None,
        cluster_health: ClusterHealth::Unknown,
        timestamp: Utc::now(),
        total_squads: None,
        active_squads: None,
    };
    let recovered: ClusterOverview =
        serde_json::from_str(&serde_json::to_string(&unknown).unwrap()).unwrap();
    assert_eq!(recovered.total_tasks, None);
    assert_eq!(recovered.avg_task_duration_ms, None);
    assert_ne!(recovered.total_tasks, Some(0));
}

/// Every declared verdict survives the wire as its own word, and no other word
/// is accepted on the way back.
///
/// A reader that selected rows by these words (`healthy`, `degraded`,
/// `unknown`) was comparing against three strings written at each site; a third
/// spelling that drifted would still parse as a valid overview and simply draw
/// nothing. So the round trip covers every word the declaration has, and a word
/// outside it is an error rather than a fallback — folding an unknown word into
/// `unknown` would report "nobody has this word" as "nobody looked".
#[test]
fn every_cluster_verdict_survives_the_wire_and_no_other_word_does() {
    for health in ClusterHealth::ALL {
        let json = serde_json::to_string(&health).unwrap();
        assert_eq!(json, format!("\"{}\"", health.as_str()));
        let recovered: ClusterHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, health);
        assert_eq!(ClusterHealth::parse(health.as_str()), Some(health));
    }

    // Distinct words: two variants sharing a spelling would make `parse` pick
    // whichever came first and silently retire the other.
    let words: std::collections::BTreeSet<&str> =
        ClusterHealth::ALL.iter().map(|h| h.as_str()).collect();
    assert_eq!(words.len(), ClusterHealth::ALL.len());

    assert_eq!(
        ClusterHealth::parse("HEALTHY"),
        Some(ClusterHealth::Healthy)
    );
    assert_eq!(ClusterHealth::parse("ok"), None);
    assert_eq!(ClusterHealth::parse("healthy "), None);

    let err = serde_json::from_str::<ClusterHealth>("\"ok\"").unwrap_err();
    assert!(
        err.to_string().contains("unknown cluster health"),
        "a word outside the vocabulary must name itself: {err}"
    );
}

// ─── traceparent: the id that survives a process boundary ──────────────────

#[test]
fn a_trace_context_survives_a_process_boundary_as_a_traceparent_header() {
    // The whole point of the carrier is that the receiving process can join
    // the same chain: a context rendered to headers, shipped, and read back
    // must name the same trace and the same span.
    let ctx = cog_core::TraceContext::generate();
    let headers = ctx.to_headers();
    let tp = headers
        .get("traceparent")
        .expect("a freshly generated context must be renderable");
    assert_eq!(tp.split('-').count(), 4);

    let back = cog_core::TraceContext::from_headers(&headers).expect("headers must parse");
    assert_eq!(back.trace_id, ctx.trace_id);
    assert_eq!(back.span_id, ctx.span_id);
    assert_eq!(back.sampled, ctx.sampled);
}

#[test]
fn a_traceparent_from_another_process_is_read_before_the_bare_id() {
    // Two hops, each writing its own bare x-trace-id, is exactly how a chain
    // splits: the header written by the far process has to win, or the id that
    // arrives is the local one and the remote half becomes unreachable.
    let mut headers = std::collections::HashMap::new();
    headers.insert(
        "traceparent".to_string(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
    );
    headers.insert("x-trace-id".to_string(), "local-only".to_string());
    headers.insert("x-span-id".to_string(), "local-span".to_string());

    let ctx = cog_core::TraceContext::from_headers(&headers).unwrap();
    assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    assert_eq!(ctx.span_id, "00f067aa0ba902b7");
    assert!(ctx.sampled);
}

#[test]
fn a_malformed_traceparent_is_refused_rather_than_half_read() {
    // Each of these is a header a lenient parser would accept and then key a
    // trace on — an id nobody else can join, or the all-zero id the spec
    // forbids. Refusing sends the caller down the generate-a-new-trace path,
    // which is visible; accepting produces a silently orphaned chain.
    for bad in [
        "",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7", // no flags
        "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01", // short trace
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01", // short span
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // zero trace
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // zero span
        "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // future version
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
    ] {
        assert!(
            cog_core::TraceContext::from_traceparent(bad).is_none(),
            "accepted malformed traceparent: {bad}"
        );
    }
}

#[test]
fn a_context_whose_ids_are_the_wrong_width_emits_no_traceparent() {
    // Emitting a header a spec-compliant receiver rejects splits the chain at
    // that hop with no error; emitting nothing is the visible failure.
    let ctx = cog_core::TraceContext::new("a".repeat(32), "b".repeat(32));
    assert!(ctx.to_traceparent().is_none());
    assert!(!ctx.to_headers().contains_key("traceparent"));
}
