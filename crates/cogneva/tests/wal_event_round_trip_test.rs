//! principle: feedback_no_hardcoding_config_surface
//! The WAL is written through one table and read back through another, and the
//! two tables name the same twenty events independently. Nothing makes them
//! agree: the write side turns an `AgentEvent` into a `WalEventType` plus a
//! JSON payload, the read side turns the record back by matching on that type,
//! and a name the read side does not know answers `Err`. `replay_events` then
//! logs a warning and moves on, so the record is simply gone from the replay —
//! no panic, no failed restore, just a shorter history than the disk holds.
//!
//! This was not hypothetical. `ResourceChanged` is written as
//! `Custom { name: "resource_changed" }` because a write has no agent and no
//! task and `WalEventType` has no variant for it; the read side's `Custom` arm
//! listed seven other names and no `resource_changed`, so every resource change
//! ever recorded was dropped on replay. The comment next to that arm already
//! described the loss for the seven names it did know — the list had merely
//! fallen behind the writer.
//!
//! So this test writes one of every variant through the public `AgentWal` and
//! reads them all back through the public `replay_events`, comparing the wire
//! names. A variant added to the enum without a specimen fails the census
//! below; a name the read table lacks fails the round trip.

use crate::common;

use common::routes::repo_root;

use cog_agent::{AgentRuntime, AgentWal};
use cog_core::{
    AgentEvent, AssistantMessageEvent, ErrorSeverity, Message, RuntimeConfig, ToolCall,
};
use cog_storage::wal::MemoryWalBackend;
use serde_json::json;
use std::collections::BTreeSet;
use std::sync::Arc;

/// The variant names the enum itself declares, read from the source text.
///
/// Kept as a text census rather than a hand-written list for the same reason
/// the specimens below are: a list that is only ever edited by the person who
/// remembered to edit it stops being a census the first time someone forgets.
fn declared_variants(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut inside = false;
    for line in text.lines() {
        if !inside {
            if line.starts_with("pub enum AgentEvent") {
                inside = true;
            }
            continue;
        }
        if line == "}" {
            break;
        }
        // 变体行恰好在四个空格缩进处；八个空格是变体里的字段，`#[..]` 是属性。
        let Some(rest) = line.strip_prefix("    ") else {
            continue;
        };
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if name.starts_with(|c: char| c.is_ascii_uppercase()) {
            out.push(name);
        }
    }
    out
}

/// The name this event puts on the wire, which is also the name the read table
/// has to recognize.
fn wire_name(event: &AgentEvent) -> String {
    serde_json::to_value(event)
        .expect("event serializes")
        .get("type")
        .and_then(|v| v.as_str())
        .expect("every variant tags itself with a type")
        .to_string()
}

/// One of every variant. Written as literals so the compiler type-checks each
/// field; the census test below catches a variant that has no line here.
fn one_of_each() -> Vec<AgentEvent> {
    let now = chrono::Utc::now();
    let message = Message::user("x");
    vec![
        AgentEvent::AgentStart {
            agent_id: "a".into(),
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::AgentEnd {
            agent_id: "a".into(),
            messages: vec![message.clone()],
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::TurnStart {
            agent_id: "a".into(),
            timestamp: now,
        },
        AgentEvent::TurnEnd {
            agent_id: "a".into(),
            message: message.clone(),
            tool_results: vec![],
            timestamp: now,
        },
        AgentEvent::MessageStart {
            agent_id: "a".into(),
            message: message.clone(),
            timestamp: now,
        },
        AgentEvent::MessageUpdate {
            agent_id: "a".into(),
            assistant_event: AssistantMessageEvent::TextDelta {
                delta: "d".into(),
                content_index: 0,
                timestamp: now,
            },
            timestamp: now,
        },
        AgentEvent::MessageEnd {
            agent_id: "a".into(),
            message: message.clone(),
            timestamp: now,
        },
        AgentEvent::ToolExecutionStart {
            agent_id: "a".into(),
            tool_call_id: "c".into(),
            tool_name: "t".into(),
            args: json!({}),
            timestamp: now,
        },
        AgentEvent::ToolExecutionUpdate {
            agent_id: "a".into(),
            tool_call_id: "c".into(),
            partial_result: json!({}),
            timestamp: now,
        },
        AgentEvent::ToolExecutionEnd {
            agent_id: "a".into(),
            tool_call_id: "c".into(),
            result: json!({}),
            is_error: false,
            timestamp: now,
        },
        AgentEvent::StateChange {
            agent_id: "a".into(),
            from: "x".into(),
            to: "y".into(),
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::TaskStatusChange {
            task_id: "t".into(),
            status: "running".into(),
            agent_id: None,
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::SelfReview {
            agent_id: "a".into(),
            status: "PASS".into(),
            score: 1.0,
            summary: None,
            critique: None,
            suggestions: None,
            timestamp: now,
        },
        AgentEvent::ReActStepStart {
            agent_id: "a".into(),
            iteration: 1,
            timestamp: now,
        },
        AgentEvent::ReActStepEnd {
            agent_id: "a".into(),
            iteration: 1,
            thought: "t".into(),
            tool_calls: vec![ToolCall {
                id: "c".into(),
                name: "n".into(),
                arguments: json!({}),
            }],
            observations: vec![],
            timestamp: now,
        },
        AgentEvent::AgentError {
            agent_id: "a".into(),
            error_code: "E".into(),
            severity: ErrorSeverity::Warning,
            details: "d".into(),
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::ResourceAlert {
            agent_id: "a".into(),
            metric: "m".into(),
            threshold: 1.0,
            current: 2.0,
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::Heartbeat {
            agent_id: "a".into(),
            timestamp: now,
        },
        AgentEvent::CheckpointSaved {
            agent_id: "a".into(),
            checkpoint_id: "c".into(),
            task_id: "t".into(),
            crew_id: None,
            squad_id: None,
            timestamp: now,
        },
        AgentEvent::ResourceChanged {
            domain: "settings".into(),
            timestamp: now,
        },
    ]
}

#[test]
fn every_variant_has_a_specimen() {
    let path = repo_root().join("crates/cog-core/src/types/event.rs");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let declared = declared_variants(&text);
    assert!(
        declared.len() >= 15,
        "只从 enum 里读出 {} 个变体，这条判据没有题目：{declared:?}",
        declared.len()
    );
    assert!(
        declared.iter().any(|name| name == "ResourceChanged"),
        "census 没读到 ResourceChanged，说明它读错了段落：{declared:?}"
    );

    let specimens = one_of_each();
    assert_eq!(
        specimens.len(),
        declared.len(),
        "enum 里声明了 {} 个变体，这里只有 {} 个标本。少的那一个在重放里\
         没人试过——补一条标本，再补上读回表里对应的名字：\n{declared:#?}",
        declared.len(),
        specimens.len()
    );

    let names: BTreeSet<String> = specimens.iter().map(wire_name).collect();
    assert_eq!(
        names.len(),
        specimens.len(),
        "两个变体在线上的名字相同，重放时无法区分：{names:?}"
    );
}

#[tokio::test]
async fn every_written_event_is_read_back_by_replay() {
    let backend = Arc::new(MemoryWalBackend::new());
    let wal = Arc::new(
        AgentWal::new(backend.clone(), "round-trip")
            .await
            .expect("内存后端建得出 WAL"),
    );

    let specimens = one_of_each();
    for event in &specimens {
        wal.append(event).await.expect("WAL 写得出这一条");
    }

    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(16);
    let runtime = AgentRuntime::new(RuntimeConfig::default(), event_tx).with_wal(wal.clone());
    let (sink, mut rx) = tokio::sync::broadcast::channel(64);

    let replayed = runtime
        .replay_events(0, &sink)
        .await
        .expect("重放本身不该失败");

    let mut seen = BTreeSet::new();
    while let Ok(event) = rx.try_recv() {
        seen.insert(wire_name(&event));
    }
    let written: BTreeSet<String> = specimens.iter().map(wire_name).collect();

    let missing: Vec<&String> = written.difference(&seen).collect();
    assert!(
        missing.is_empty(),
        "写进 WAL 的 {} 条事件里，重放只读回 {} 条。缺的这几类在写盘那一侧有\
         对应分支，读回那一侧的 `match` 却不认它们的名字，于是 `Err` 被\
         `replay_events` 当成坏记录跳过——恢复后的历史比盘上短，而且只在日志里\
         留一行 warn。给这些名字在读回表里补上分支：{missing:#?}",
        written.len(),
        replayed
    );
    assert_eq!(replayed, specimens.len(), "重放条数与写进去的条数对不上");
}
