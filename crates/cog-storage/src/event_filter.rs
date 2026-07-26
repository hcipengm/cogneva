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
            | AgentEvent::CheckpointSaved { timestamp, .. } => *timestamp,
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
    }
}
