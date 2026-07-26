use axum::{
    extract::{Path, State, WebSocketUpgrade},
    response::Response,
};
use std::sync::Arc;
use tokio::time::{interval, Duration};

use crate::GatewayState;

// ─── Agent State Stream ───

pub async fn agent_stream_handler(
    State(state): State<Arc<GatewayState>>,
    Path(agent_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_agent_stream(socket, state, agent_id))
}

use axum::extract::ws::{Message as WsMessage, WebSocket};

async fn handle_agent_stream(mut socket: WebSocket, state: Arc<GatewayState>, agent_id: String) {
    let mut event_rx = state.event_tx.subscribe();
    let gateway = match state.observability_gateway.clone() {
        Some(g) => g,
        None => {
            let _ = socket
                .send(WsMessage::Text(
                    r#"{"error":"observability gateway not configured"}"#.into(),
                ))
                .await;
            return;
        }
    };

    let mut tick = interval(Duration::from_secs(
        state.config.read().unwrap().websocket_tick_secs,
    ));

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                if event_matches_agent(&event, &agent_id) {
                    let payload = match serde_json::to_string(&event) {
                        Ok(j) => j,
                        Err(e) => {
                            tracing::warn!("agent stream serialization error: {e}");
                            continue;
                        }
                    };
                    if socket.send(WsMessage::Text(format!(
                        "{{\"type\":\"event\",\"agent_id\":\"{}\",\"payload\":{}}}",
                        agent_id, payload
                    ).into())).await.is_err() {
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                match gateway.get_agent_state(&agent_id).await {
                    Ok(agent_state) => {
                        let snapshot = serde_json::json!({
                            "type": "snapshot",
                            "agent_id": agent_id,
                            "state": agent_state,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                        });
                        if socket.send(WsMessage::Text(snapshot.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("agent stream snapshot error: {e}");
                    }
                }
            }
        }
    }
}

fn event_matches_agent(event: &cog_core::AgentEvent, agent_id: &str) -> bool {
    match event {
        cog_core::AgentEvent::AgentStart { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::AgentEnd { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::TurnStart { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::TurnEnd { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::MessageStart { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::MessageUpdate { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::MessageEnd { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ToolExecutionStart { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ToolExecutionUpdate { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ToolExecutionEnd { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::StateChange { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::TaskStatusChange { agent_id: id, .. } => {
            id.as_deref() == Some(agent_id)
        }
        cog_core::AgentEvent::SelfReview { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ReActStepStart { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ReActStepEnd { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::AgentError { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::ResourceAlert { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::Heartbeat { agent_id: id, .. } => id == agent_id,
        cog_core::AgentEvent::CheckpointSaved { agent_id: id, .. } => id == agent_id,
    }
}

// ─── Task Progress Stream ───

pub async fn task_stream_handler(
    State(state): State<Arc<GatewayState>>,
    Path(task_id): Path<String>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_task_stream(socket, state, task_id))
}

async fn handle_task_stream(mut socket: WebSocket, state: Arc<GatewayState>, task_id: String) {
    let mut task_rx = state.subscribe_task_events();
    let gateway = match state.observability_gateway.clone() {
        Some(g) => g,
        None => {
            let _ = socket
                .send(WsMessage::Text(
                    r#"{"error":"observability gateway not configured"}"#.into(),
                ))
                .await;
            return;
        }
    };

    let mut tick = interval(Duration::from_secs(
        state.config.read().unwrap().websocket_tick_secs,
    ));

    loop {
        tokio::select! {
            Ok(event) = task_rx.recv() => {
                if task_event_matches(&event, &task_id) {
                    let payload = match serde_json::to_string(&event) {
                        Ok(j) => j,
                        Err(e) => {
                            tracing::warn!("task stream serialization error: {e}");
                            continue;
                        }
                    };
                    if socket.send(WsMessage::Text(format!(
                        "{{\"type\":\"event\",\"task_id\":\"{}\",\"payload\":{}}}",
                        task_id, payload
                    ).into())).await.is_err() {
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                let metrics = gateway.get_task_metrics(&task_id).await.ok();
                let logs = gateway.get_task_logs(&task_id, 10).await.ok();
                let snapshot = serde_json::json!({
                    "type": "snapshot",
                    "task_id": task_id,
                    "metrics": metrics,
                    "logs": logs,
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                });
                if socket.send(WsMessage::Text(snapshot.to_string().into())).await.is_err() {
                    break;
                }
            }
        }
    }
}

fn task_event_matches(event: &cog_core::TaskEvent, task_id: &str) -> bool {
    match event {
        cog_core::TaskEvent::TaskCreated { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskScheduled { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskStarted { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskCompleted { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskFailed { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskCancelled { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskRetried { task_id: id, .. } => id == task_id,
        cog_core::TaskEvent::TaskTimeout { task_id: id, .. } => id == task_id,
    }
}

// ─── Cluster Overview Stream ───

pub async fn cluster_stream_handler(
    State(state): State<Arc<GatewayState>>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |socket| handle_cluster_stream(socket, state))
}

async fn handle_cluster_stream(mut socket: WebSocket, state: Arc<GatewayState>) {
    let mut event_rx = state.event_tx.subscribe();
    let gateway = match state.observability_gateway.clone() {
        Some(g) => g,
        None => {
            let _ = socket
                .send(WsMessage::Text(
                    r#"{"error":"observability gateway not configured"}"#.into(),
                ))
                .await;
            return;
        }
    };

    let mut tick = interval(Duration::from_secs(
        state.config.read().unwrap().websocket_tick_secs,
    ));

    loop {
        tokio::select! {
            Ok(event) = event_rx.recv() => {
                // Only forward cluster-relevant events (state changes, errors, alerts)
                let is_cluster_relevant = matches!(
                    event,
                    cog_core::AgentEvent::StateChange { .. }
                        | cog_core::AgentEvent::AgentError { .. }
                        | cog_core::AgentEvent::ResourceAlert { .. }
                        | cog_core::AgentEvent::TaskStatusChange { .. }
                );
                if is_cluster_relevant {
                    let payload = match serde_json::to_string(&event) {
                        Ok(j) => j,
                        Err(e) => {
                            tracing::warn!("cluster stream serialization error: {e}");
                            continue;
                        }
                    };
                    if socket.send(WsMessage::Text(format!(
                        "{{\"type\":\"event\",\"payload\":{}}}", payload
                    ).into())).await.is_err() {
                        break;
                    }
                }
            }
            _ = tick.tick() => {
                match gateway.get_cluster_overview().await {
                    Ok(overview) => {
                        let snapshot = serde_json::json!({
                            "type": "snapshot",
                            "overview": overview,
                            "timestamp": chrono::Utc::now().to_rfc3339(),
                        });
                        if socket.send(WsMessage::Text(snapshot.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("cluster stream snapshot error: {e}");
                    }
                }
            }
        }
    }
}
