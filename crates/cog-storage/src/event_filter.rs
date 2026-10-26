//! Event-filter logic moved from `cog-core` so the domain-kernel
//! stays free of business-rule implementations.

use cog_core::{AgentEvent, EventFilter};

/// Check whether an [`AgentEvent`] matches the given [`EventFilter`].
pub fn event_matches(filter: &EventFilter, event: &AgentEvent) -> bool {
    if let Some(ref agent_id) = filter.agent_id {
        let ev_agent = match event {
            AgentEvent::AgentStart { agent_id: id, .. }
            | AgentEvent::AgentEnd { agent_id: id, .. }
            | AgentEvent::TurnStart { agent_id: id, .. }
            | AgentEvent::TurnEnd { agent_id: id, .. }
            | AgentEvent::MessageStart { agent_id: id, .. }
            | AgentEvent::MessageUpdate { agent_id: id, .. }
            | AgentEvent::MessageEnd { agent_id: id, .. }
            | AgentEvent::ToolExecutionStart { agent_id: id, .. }
            | AgentEvent::ToolExecutionUpdate { agent_id: id, .. }
            | AgentEvent::ToolExecutionEnd { agent_id: id, .. }
            | AgentEvent::StateChange { agent_id: id, .. }
            | AgentEvent::SelfReview { agent_id: id, .. }
            | AgentEvent::ReActStepStart { agent_id: id, .. }
            | AgentEvent::ReActStepEnd { agent_id: id, .. }
            | AgentEvent::AgentError { agent_id: id, .. }
            | AgentEvent::ResourceAlert { agent_id: id, .. }
            | AgentEvent::Heartbeat { agent_id: id, .. }
            | AgentEvent::CheckpointSaved { agent_id: id, .. } => id,
            AgentEvent::TaskStatusChange { agent_id: id, .. } => {
                if let Some(id) = id {
                    id
                } else {
                    return false;
                }
            }
            // 一次写不属于任何一个执行者：按身份筛的时候它不是任何人的。
            AgentEvent::ResourceChanged { .. } => return false,
        };
        if ev_agent != agent_id {
            return false;
        }
    }
    if let Some(ref task_id) = filter.task_id {
        match event {
            AgentEvent::TaskStatusChange { task_id: id, .. } => {
                if id != task_id {
                    return false;
                }
            }
            _ => return false,
        }
    }
    if let Some(ref squad_id) = filter.squad_id {
        let ev_squad = match event {
            AgentEvent::AgentStart { squad_id: id, .. }
            | AgentEvent::AgentEnd { squad_id: id, .. }
            | AgentEvent::StateChange { squad_id: id, .. }
            | AgentEvent::TaskStatusChange { squad_id: id, .. }
            | AgentEvent::CheckpointSaved { squad_id: id, .. } => id.clone(),
            _ => {
                return false;
            }
        };
        if ev_squad.as_ref() != Some(squad_id) {
            return false;
        }
    }
    if let Some(ref event_types) = filter.event_types {
        let ev_type = event_type_name(event);
        if !event_types.iter().any(|t| t == ev_type) {
            return false;
        }
    }
    if let Some(ref since) = filter.since {
        let ts = match event {
            AgentEvent::AgentStart { timestamp, .. }
            | AgentEvent::AgentEnd { timestamp, .. }
            | AgentEvent::TurnStart { timestamp, .. }
            | AgentEvent::TurnEnd { timestamp, .. }
            | AgentEvent::MessageStart { timestamp, .. }
            | AgentEvent::MessageUpdate { timestamp, .. }
            | AgentEvent::MessageEnd { timestamp, .. }
            | AgentEvent::ToolExecutionStart { timestamp, .. }
            | AgentEvent::ToolExecutionUpdate { timestamp, .. }
            | AgentEvent::ToolExecutionEnd { timestamp, .. }
            | AgentEvent::StateChange { timestamp, .. }
            | AgentEvent::TaskStatusChange { timestamp, .. }
            | AgentEvent::SelfReview { timestamp, .. }
            | AgentEvent::ReActStepStart { timestamp, .. }
            | AgentEvent::ReActStepEnd { timestamp, .. }
            | AgentEvent::AgentError { timestamp, .. }
            | AgentEvent::ResourceAlert { timestamp, .. }
            | AgentEvent::Heartbeat { timestamp, .. }
            | AgentEvent::CheckpointSaved { timestamp, .. }
            | AgentEvent::ResourceChanged { timestamp, .. } => *timestamp,
        };
        if ts < *since {
            return false;
        }
    }
    true
}

/// Return the snake_case type name for an [`AgentEvent`] variant.
pub fn event_type_name(event: &AgentEvent) -> &str {
    match event {
        AgentEvent::AgentStart { .. } => "agent_start",
        AgentEvent::AgentEnd { .. } => "agent_end",
        AgentEvent::TurnStart { .. } => "turn_start",
        AgentEvent::TurnEnd { .. } => "turn_end",
        AgentEvent::MessageStart { .. } => "message_start",
        AgentEvent::MessageUpdate { .. } => "message_update",
        AgentEvent::MessageEnd { .. } => "message_end",
        AgentEvent::ToolExecutionStart { .. } => "tool_execution_start",
        AgentEvent::ToolExecutionUpdate { .. } => "tool_execution_update",
        AgentEvent::ToolExecutionEnd { .. } => "tool_execution_end",
        AgentEvent::StateChange { .. } => "state_change",
        AgentEvent::TaskStatusChange { .. } => "task_status_change",
        AgentEvent::SelfReview { .. } => "self_review",
        AgentEvent::ReActStepStart { .. } => "react_step_start",
        AgentEvent::ReActStepEnd { .. } => "react_step_end",
        AgentEvent::AgentError { .. } => "agent_error",
        AgentEvent::ResourceAlert { .. } => "resource_alert",
        AgentEvent::Heartbeat { .. } => "heartbeat",
        AgentEvent::CheckpointSaved { .. } => "checkpoint_saved",
        AgentEvent::ResourceChanged { .. } => "resource_changed",
    }
}

#[cfg(test)]
mod tests {
    use super::event_type_name;
    use chrono::Utc;
    use cog_core::{AgentEvent, AssistantMessageEvent, ErrorSeverity, Message, ToolCall};
    use serde_json::json;

    /// One of every variant. Kept as a plain function rather than a list of
    /// literals so the compiler catches a variant added without a specimen.
    fn one_of_each() -> Vec<AgentEvent> {
        let now = Utc::now();
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
        ]
    }

    /// The name on the wire and the name in storage are one name.
    ///
    /// `AgentEvent` derives `rename_all = "snake_case"`, which splits `ReAct`
    /// into `re_act`, while `event_type_name` — the name written to the event
    /// table and matched by every filter — says `react_step_start`. A client
    /// subscribing to the stored name therefore never saw a live ReAct frame,
    /// and neither side looked wrong on its own. Pinning the two together here
    /// is what makes that class of drift fail a build instead of a demo.
    #[test]
    fn the_wire_name_of_every_event_equals_its_stored_name() {
        for event in one_of_each() {
            let wire = serde_json::to_value(&event).expect("event serializes");
            let wire_name = wire
                .get("type")
                .and_then(|v| v.as_str())
                .expect("every variant tags itself with a type");
            assert_eq!(
                wire_name,
                event_type_name(&event),
                "wire name and stored name disagree for {event:?}"
            );
        }
    }
}
