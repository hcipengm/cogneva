use cog_agent::AgentWal;
use cog_core::{AgentEvent, WalBackend, WalEventType, WalRecord};
use cog_protocol::convert::wal_codec;
use cog_protocol::wal::WalEventType as ProtoWalEventType;
use cog_storage::wal::{FileWalBackend, MemoryWalBackend};
use std::sync::Arc;

#[tokio::test]
async fn test_memory_wal_backend_append_and_read() {
    let backend = MemoryWalBackend::new();
    let record = WalRecord::new(0, "sess-1", WalEventType::AgentStart, serde_json::json!({}));
    let seq = backend.append(record).await.unwrap();
    assert_eq!(seq, 0);

    let records = backend.read_since("sess-1", 0).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 0);
    assert_eq!(records[0].session_id, "sess-1");
}

#[tokio::test]
async fn test_memory_wal_backend_read_since_filters() {
    let backend = MemoryWalBackend::new();
    for i in 0..5 {
        let record = WalRecord::new(i, "sess-1", WalEventType::TurnStart, serde_json::json!({}));
        backend.append(record).await.unwrap();
    }

    let records = backend.read_since("sess-1", 2).await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].seq, 2);
}

#[tokio::test]
async fn test_memory_wal_backend_read_latest() {
    let backend = MemoryWalBackend::new();
    for i in 0..10 {
        let record = WalRecord::new(i, "sess-1", WalEventType::TurnStart, serde_json::json!({}));
        backend.append(record).await.unwrap();
    }

    let records = backend.read_latest("sess-1", 3).await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].seq, 7);
    assert_eq!(records[2].seq, 9);
}

#[tokio::test]
async fn test_memory_wal_backend_truncate_before() {
    let backend = MemoryWalBackend::new();
    for i in 0..5 {
        let record = WalRecord::new(i, "sess-1", WalEventType::TurnStart, serde_json::json!({}));
        backend.append(record).await.unwrap();
    }

    backend.truncate_before("sess-1", 3).await.unwrap();
    let records = backend.read_since("sess-1", 0).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].seq, 3);
}

#[tokio::test]
async fn test_memory_wal_backend_next_seq() {
    let backend = MemoryWalBackend::new();
    assert_eq!(backend.next_seq("sess-1").await.unwrap(), 0);

    let record = WalRecord::new(0, "sess-1", WalEventType::AgentStart, serde_json::json!({}));
    backend.append(record).await.unwrap();
    assert_eq!(backend.next_seq("sess-1").await.unwrap(), 1);
}

#[tokio::test]
async fn test_memory_wal_backend_isolation_between_sessions() {
    let backend = MemoryWalBackend::new();
    backend
        .append(WalRecord::new(
            0,
            "sess-a",
            WalEventType::AgentStart,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    backend
        .append(WalRecord::new(
            0,
            "sess-b",
            WalEventType::AgentEnd,
            serde_json::json!({}),
        ))
        .await
        .unwrap();

    let a = backend.read_since("sess-a", 0).await.unwrap();
    let b = backend.read_since("sess-b", 0).await.unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    assert!(matches!(a[0].event_type, WalEventType::AgentStart));
    assert!(matches!(b[0].event_type, WalEventType::AgentEnd));
}

#[tokio::test]
async fn test_file_wal_backend_persists_and_reads() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));
    let record = WalRecord::new(
        0,
        "sess-1",
        WalEventType::AgentStart,
        serde_json::json!({"agent_id": "a1"}),
    );
    backend.append(record).await.unwrap();

    let records = backend.read_since("sess-1", 0).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 0);
    assert_eq!(records[0].payload["agent_id"], "a1");
}

#[tokio::test]
async fn test_file_wal_backend_read_latest_and_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));
    for i in 0..5 {
        backend
            .append(WalRecord::new(
                i,
                "sess-1",
                WalEventType::TurnStart,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
    }

    let latest = backend.read_latest("sess-1", 2).await.unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest[1].seq, 4);

    backend.truncate_before("sess-1", 3).await.unwrap();
    let remaining = backend.read_since("sess-1", 0).await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0].seq, 3);
}

#[tokio::test]
async fn test_file_wal_backend_next_seq() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));
    assert_eq!(backend.next_seq("sess-1").await.unwrap(), 0);

    backend
        .append(WalRecord::new(
            0,
            "sess-1",
            WalEventType::AgentStart,
            serde_json::json!({}),
        ))
        .await
        .unwrap();
    assert_eq!(backend.next_seq("sess-1").await.unwrap(), 1);
}

#[tokio::test]
async fn test_agent_wal_appends_and_increments_seq() {
    let backend = Arc::new(MemoryWalBackend::new());
    let wal = AgentWal::new(backend, "sess-1").await.unwrap();

    let event = AgentEvent::AgentStart {
        agent_id: "agent-1".into(),
        crew_id: None,
        squad_id: None,
        timestamp: chrono::Utc::now(),
    };
    let seq1 = wal.append(&event).await.unwrap();
    let seq2 = wal.append(&event).await.unwrap();

    assert_eq!(seq1, 0);
    assert_eq!(seq2, 1);
    assert_eq!(wal.current_seq(), 2);
}

#[tokio::test]
async fn test_agent_wal_event_conversion() {
    let backend = Arc::new(MemoryWalBackend::new());
    let wal = AgentWal::new(backend, "sess-1").await.unwrap();

    let event = AgentEvent::StateChange {
        agent_id: "agent-1".into(),
        from: "idle".into(),
        to: "running".into(),
        crew_id: None,
        squad_id: None,
        timestamp: chrono::Utc::now(),
    };
    wal.append(&event).await.unwrap();

    let records = wal.read_since(0).await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].event_type, WalEventType::StateChange));
    assert_eq!(records[0].payload["from"], "idle");
    assert_eq!(records[0].payload["to"], "running");
}

// ---------------------------------------------------------------------------
// Protobuf WAL format tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_file_wal_backend_uses_protobuf_format() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));
    let record = WalRecord::new(
        0,
        "sess-proto",
        WalEventType::AgentStart,
        serde_json::json!({"agent_id": "a1"}),
    );
    backend.append(record).await.unwrap();

    // The file must be `.wal.bin` (protobuf), not `.wal.jsonl`
    let bin_path = tmp.path().join("sess-proto.wal.bin");
    assert!(bin_path.exists(), "expected .wal.bin file to exist");

    let jsonl_path = tmp.path().join("sess-proto.wal.jsonl");
    assert!(!jsonl_path.exists(), "expected no .wal.jsonl file");

    // Verify we can read it back correctly
    let records = backend.read_since("sess-proto", 0).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].seq, 0);
    assert_eq!(records[0].payload["agent_id"], "a1");
}

#[tokio::test]
async fn test_file_wal_backend_backward_compatible_jsonl_read() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));

    // Write a legacy JSONL file directly
    let jsonl_path = tmp.path().join("sess-legacy.wal.jsonl");
    let record = WalRecord::new(
        0,
        "sess-legacy",
        WalEventType::StateChange,
        serde_json::json!({"from": "idle", "to": "running"}),
    );
    let line = record.encode_to_json_line().unwrap();
    std::fs::write(&jsonl_path, format!("{}\n", line)).unwrap();

    // FileWalBackend should read the legacy JSONL file
    let records = backend.read_since("sess-legacy", 0).await.unwrap();
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].event_type, WalEventType::StateChange));
    assert_eq!(records[0].payload["from"], "idle");
    assert_eq!(records[0].payload["to"], "running");
}

#[tokio::test]
async fn test_file_wal_backend_protobuf_truncate() {
    let tmp = tempfile::tempdir().unwrap();
    let backend = FileWalBackend::new(tmp.path(), Arc::new(cog_protocol::convert::ProtoCodec));
    for i in 0..5 {
        backend
            .append(WalRecord::new(
                i,
                "sess-trunc",
                WalEventType::TurnStart,
                serde_json::json!({}),
            ))
            .await
            .unwrap();
    }

    backend.truncate_before("sess-trunc", 3).await.unwrap();
    let records = backend.read_since("sess-trunc", 0).await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].seq, 3);
    assert_eq!(records[1].seq, 4);
}

#[test]
fn test_wal_record_proto_roundtrip() {
    let record = WalRecord::new(
        42,
        "session-abc",
        WalEventType::ToolExecutionEnd,
        serde_json::json!({"tool": "git", "status": "ok"}),
    );

    // Roundtrip via raw protobuf bytes
    let bytes = wal_codec::encode(&record);
    let decoded = wal_codec::decode(&bytes).unwrap();

    assert_eq!(decoded.seq, record.seq);
    assert_eq!(decoded.session_id, record.session_id);
    assert!(matches!(decoded.event_type, WalEventType::ToolExecutionEnd));
    assert_eq!(decoded.payload, record.payload);

    // Roundtrip via length-delimited framing
    let framed = wal_codec::encode_length_delimited(&record).unwrap();
    let (decoded2, consumed) = wal_codec::decode_length_delimited(&framed).unwrap();
    assert_eq!(consumed, framed.len());
    assert_eq!(decoded2.seq, record.seq);
    assert_eq!(decoded2.session_id, record.session_id);
    assert_eq!(decoded2.payload, record.payload);
}

#[test]
fn test_wal_record_proto_is_smaller_than_json() {
    let record = WalRecord::new(
        7,
        "sess-compare",
        WalEventType::AgentStart,
        serde_json::json!({"agent_id": "agent-1", "model": "claude-sonnet-4-6"}),
    );

    let proto_bytes = cog_protocol::convert::wal_codec::encode(&record);
    let json_bytes = record.encode_to_json_line().unwrap().into_bytes();

    assert!(
        proto_bytes.len() < json_bytes.len(),
        "protobuf ({}) should be smaller than JSON ({})",
        proto_bytes.len(),
        json_bytes.len()
    );
}

#[test]
fn test_wal_event_type_proto_mapping() {
    // Verify every Rust WalEventType maps to a distinct proto value and back
    let types = vec![
        WalEventType::AgentStart,
        WalEventType::AgentEnd,
        WalEventType::TurnStart,
        WalEventType::TurnEnd,
        WalEventType::MessageStart,
        WalEventType::MessageDelta,
        WalEventType::MessageEnd,
        WalEventType::ToolExecutionStart,
        WalEventType::ToolExecutionDelta,
        WalEventType::ToolExecutionEnd,
        WalEventType::StateChange,
        WalEventType::TaskStatusChange,
        WalEventType::Custom { name: "foo".into() },
    ];

    for ty in types {
        let proto_ty = ProtoWalEventType::from(&ty);
        let back = WalEventType::from(proto_ty);
        // Custom names are not preserved in proto (stored in payload), so skip name check
        match (&ty, &back) {
            (WalEventType::Custom { .. }, WalEventType::Custom { .. }) => {}
            _ => assert_eq!(std::mem::discriminant(&ty), std::mem::discriminant(&back)),
        }
    }
}

// ---------------------------------------------------------------------------
// AgentRuntime WAL integration tests
// ---------------------------------------------------------------------------

use async_trait::async_trait;
use cog_agent::AgentRuntime;
use cog_core::{AssistantMessageEvent, LlmClient as LLMProvider, Message, RuntimeConfig};
use cog_llm::{ChatOptions, ChatResponse, CompleteOptions, EventStream};

struct DummyProvider;

#[async_trait]
impl LLMProvider for DummyProvider {
    async fn chat(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> cog_core::SFResult<ChatResponse> {
        Ok(ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _messages: &[Message],
        _options: &ChatOptions,
    ) -> cog_core::SFResult<cog_llm::AssistantMessageEventStream> {
        let (stream, mut producer) = EventStream::with_capacity(1);
        tokio::spawn(async move {
            let _ = producer
                .push(AssistantMessageEvent::Done {
                    reason: cog_core::StopReason::Stop,
                    message: Message::assistant_text("hello"),
                    timestamp: chrono::Utc::now(),
                })
                .await;
            producer.end(ChatResponse::default());
        });
        Ok(stream)
    }

    async fn complete_stream(
        &self,
        _prompt: &str,
        _options: &CompleteOptions,
    ) -> cog_core::SFResult<cog_llm::AssistantMessageEventStream> {
        let (stream, _) = EventStream::with_capacity(1);
        Ok(stream)
    }

    async fn health_check(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn test_agent_loop_with_wal_persists_events() {
    let backend = Arc::new(MemoryWalBackend::new());
    let wal = Arc::new(AgentWal::new(backend.clone(), "sess-loop").await.unwrap());

    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(16);
    let config = RuntimeConfig {
        agent_id: "test-agent".into(),
        role: "planner".to_string(),
        max_iterations: 1,
        context_window_size: 4000,
        skill_cache_ttl_secs: 30,
        skill_config: None,
        crew_id: None,
        squad_id: None,
    };
    let mut agent_loop = AgentRuntime::new(config, event_tx).with_wal(wal);

    let result = agent_loop
        .run(serde_json::json!({"test": true}), &DummyProvider)
        .await;
    assert!(result.is_ok());

    let records = backend.read_since("sess-loop", 0).await.unwrap();
    assert!(!records.is_empty());
    assert!(records
        .iter()
        .any(|r| matches!(r.event_type, WalEventType::AgentStart)));
    assert!(records
        .iter()
        .any(|r| matches!(r.event_type, WalEventType::AgentEnd)));
}
