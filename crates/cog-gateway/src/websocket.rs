use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

use crate::websocket_protocol::{event_channels, ClientMessage, ConnectionManager, ServerMessage};
use crate::GatewayState;
use cog_core::TraceContext;
use cog_core::{RawContext, RawMeta, RawPayload, RawRecord};

#[derive(Debug, Deserialize)]
#[serde(tag = "cmd")]
pub enum WsCommand {
    #[serde(rename = "ping")]
    Ping { id: Option<String> },
    #[serde(rename = "abort")]
    Abort { task_id: Option<String> },
    #[serde(rename = "status")]
    Status,
    #[serde(rename = "list")]
    List {
        #[serde(default)]
        status: Option<String>,
        #[serde(default)]
        task_type: Option<String>,
        #[serde(default)]
        agent_id: Option<String>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
pub enum WsResponse {
    #[serde(rename = "pong")]
    Pong { id: Option<String> },
    #[serde(rename = "ack")]
    Ack { cmd: String },
    #[serde(rename = "error")]
    Error { message: String },
}

fn websocket_record(
    stream: &str,
    direction: &str,
    raw: Value,
    trace_ctx: Option<&TraceContext>,
) -> RawRecord {
    let (trace_id, span_id) = match trace_ctx {
        Some(ctx) => (ctx.trace_id.clone(), Some(ctx.span_id.clone())),
        None => (uuid::Uuid::new_v4().to_string(), None),
    };
    RawRecord {
        meta: RawMeta {
            version: "1.0".into(),
            stream: stream.into(),
            recorded_at: chrono::Utc::now(),
            recorded_by: "cog-gateway".into(),
            sequence: 0,
            trace_id,
            span_id,
        },
        context: RawContext::default(),
        payload: RawPayload {
            direction: direction.into(),
            transport: "websocket".into(),
            format: Some("json".into()),
            raw,
        },
    }
}

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

async fn send_server_message(
    socket: &mut WebSocket,
    msg: &ServerMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let json = serde_json::to_string(msg)?;
    socket.send(WsMessage::Text(json.into())).await?;
    Ok(())
}

async fn send_connected_event(
    socket: &mut WebSocket,
    connection_id: &str,
    conn_manager: &ConnectionManager,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let missed = conn_manager.get_missed_event_ids().await;
    let msg = ServerMessage::Connected {
        connection_id: connection_id.into(),
        server_time: now_iso(),
        missed_events: missed,
    };
    send_server_message(socket, &msg).await
}

async fn handle_type_message(
    socket: &mut WebSocket,
    state: &Arc<GatewayState>,
    connection_id: &str,
    text: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_msg: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            // Not a type-based message; fall through silently so the
            // caller can try cmd-based parsing.
            return Err("not a type-based message".into());
        }
    };

    match client_msg {
        ClientMessage::Ping { timestamp, seq } => {
            let resp = ServerMessage::Pong {
                timestamp,
                seq,
                server_time: now_iso(),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::Subscribe { channels, .. } => {
            let mgr = state
                .connection_manager
                .as_ref()
                .ok_or("connection manager not available")?;
            let subscribed = mgr.subscribe(connection_id, &channels).await;
            let resp = ServerMessage::Subscribed {
                channels: subscribed,
                server_time: now_iso(),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::Unsubscribe { channels, .. } => {
            let mgr = state
                .connection_manager
                .as_ref()
                .ok_or("connection manager not available")?;
            let unsubscribed = mgr.unsubscribe(connection_id, &channels).await;
            let resp = ServerMessage::Unsubscribed {
                channels: unsubscribed,
                server_time: now_iso(),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::Ack { event_ids, .. } => {
            let resp = ServerMessage::Acknowledged {
                event_ids,
                server_time: now_iso(),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::Typing { .. } => {
            // Typing indicator is accepted but produces no response.
        }
    }

    Ok(())
}

async fn handle_command(
    socket: &mut WebSocket,
    state: &Arc<GatewayState>,
    text: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cmd: WsCommand = match serde_json::from_str(text) {
        Ok(c) => c,
        Err(_) => {
            tracing::debug!("WebSocket text is not a command: {}", text);
            return Ok(());
        }
    };

    match cmd {
        WsCommand::Ping { id } => {
            let resp = WsResponse::Pong { id };
            let json = serde_json::to_string(&resp)?;
            socket.send(WsMessage::Text(json.into())).await?;
        }
        WsCommand::Status => {
            let all = state.orchestrator.get_all_tasks().await;
            let mut pending = 0usize;
            let mut scheduled = 0usize;
            let mut running = 0usize;
            let mut completed = 0usize;
            let mut failed = 0usize;
            let mut cancelled = 0usize;
            for task in &all {
                match task.status {
                    cog_core::TaskStatus::Pending => pending += 1,
                    cog_core::TaskStatus::Scheduled => scheduled += 1,
                    cog_core::TaskStatus::Running => running += 1,
                    cog_core::TaskStatus::Completed => completed += 1,
                    cog_core::TaskStatus::Failed => failed += 1,
                    cog_core::TaskStatus::Cancelled => cancelled += 1,
                }
            }
            let ready = state.orchestrator.get_ready_tasks().await.len();
            let total = all.len();
            let all_completed = !all.is_empty()
                && all
                    .iter()
                    .all(|t| t.status == cog_core::TaskStatus::Completed);
            let resp = serde_json::json!({
                "type": "status_summary",
                "total": total,
                "pending": pending,
                "scheduled": scheduled,
                "running": running,
                "completed": completed,
                "failed": failed,
                "cancelled": cancelled,
                "ready": ready,
                "all_completed": all_completed,
            });
            socket
                .send(WsMessage::Text(resp.to_string().into()))
                .await?;
        }
        WsCommand::List {
            status,
            task_type,
            agent_id,
        } => {
            let mut tasks: Vec<crate::tasks::TaskView> = state
                .orchestrator
                .get_all_tasks()
                .await
                .into_iter()
                .map(|t| t.into())
                .collect();

            if let Some(ref s) = status {
                tasks.retain(|t| t.status == *s);
            }
            if let Some(ref tt) = task_type {
                tasks.retain(|t| t.task_type == *tt);
            }
            if let Some(ref a) = agent_id {
                tasks.retain(|t| t.agent_id.as_ref() == Some(a));
            }

            let resp = serde_json::json!({
                "type": "task_list",
                "tasks": tasks,
            });
            socket
                .send(WsMessage::Text(resp.to_string().into()))
                .await?;
        }
        WsCommand::Abort { task_id } => {
            tracing::info!("WebSocket abort received: task_id={:?}", task_id);

            if let Some(ref tid) = task_id {
                if let Err(e) = state.orchestrator.cancel_task(tid).await {
                    tracing::warn!("Abort failed to cancel task {}: {}", tid, e);
                    let resp = WsResponse::Error {
                        message: format!("abort failed: {}", e),
                    };
                    let json = serde_json::to_string(&resp)?;
                    socket.send(WsMessage::Text(json.into())).await?;
                    return Ok(());
                }
            }

            let resp = WsResponse::Ack {
                cmd: "abort".into(),
            };
            let json = serde_json::to_string(&resp)?;
            socket.send(WsMessage::Text(json.into())).await?;
        }
    }

    Ok(())
}

pub async fn handle_socket(
    mut socket: WebSocket,
    state: Arc<GatewayState>,
    user_id: String,
    device_id: Option<String>,
    platform: Option<String>,
    app_version: Option<String>,
    trace_ctx: TraceContext,
) {
    let connection_id = uuid::Uuid::new_v4().to_string();
    let mut rx = state.event_tx.subscribe();
    let mut notif_rx = state.notification_tx.subscribe();

    // Register connection and send "connected" event.
    if let Some(ref mgr) = state.connection_manager {
        mgr.register(
            connection_id.clone(),
            user_id,
            device_id,
            platform,
            app_version,
        )
        .await;

        if let Err(e) = send_connected_event(&mut socket, &connection_id, mgr).await {
            tracing::warn!(
                "Failed to send connected event for {}: {}",
                connection_id,
                e
            );
        }
    }

    // Server-side heartbeat: send ping every websocket_timeout_secs to keep connection alive
    // and detect dead peers.  First tick is delayed so it does not race
    // with the initial "connected" event.
    let ws_timeout =
        std::time::Duration::from_secs(state.config.read().unwrap().websocket_timeout_secs);
    let mut heartbeat =
        tokio::time::interval_at(tokio::time::Instant::now() + ws_timeout, ws_timeout);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    // Client inactivity timeout: disconnect after websocket_inactivity_timeout_secs of no messages
    // (spec section 2.1).
    let inactivity_timeout = std::time::Duration::from_secs(
        state
            .config
            .read()
            .unwrap()
            .websocket_inactivity_timeout_secs,
    );
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            Ok(event) = rx.recv() => {
                last_activity = tokio::time::Instant::now();

                // Channel filtering
                if let Some(ref mgr) = state.connection_manager {
                    let channels = event_channels(&event);
                    let should = mgr.should_deliver(&connection_id, &channels).await;
                    if !should {
                        continue;
                    }
                }

                let payload = match serde_json::to_string(&event) {
                    Ok(json) => json,
                    Err(_) => continue,
                };

                if let Ok(value) = serde_json::to_value(&event) {
                    // Record in global missed-events cache
                    if let Some(ref mgr) = state.connection_manager {
                        let event_id = uuid::Uuid::new_v4().to_string();
                        mgr.record_event(event_id.clone(), value.clone()).await;
                    }

                    let record = websocket_record("session_raw", "outbound", value, Some(&trace_ctx));
                    if let Err(e) = state.raw_logger.write(record).await {
                        tracing::warn!("RawLogger write failed (outbound): {}", e);
                    }
                }

                if socket.send(WsMessage::Text(payload.into())).await.is_err() {
                    break;
                }
            }
            Ok(notification) = notif_rx.recv() => {
                last_activity = tokio::time::Instant::now();
                let msg = crate::websocket_protocol::ServerMessage::Notification {
                    event_id: notification.id.clone(),
                    payload: serde_json::to_value(&notification).unwrap_or_default(),
                    server_time: now_iso(),
                };
                if let Err(e) = send_server_message(&mut socket, &msg).await {
                    tracing::warn!(
                        "Failed to send notification over WebSocket for {}: {}",
                        connection_id, e
                    );
                    break;
                }
            }
            _ = heartbeat.tick() => {
                if socket.send(WsMessage::Ping(vec![].into())).await.is_err() {
                    tracing::debug!("Heartbeat ping failed for {}; closing.", connection_id);
                    break;
                }
            }
            _ = tokio::time::sleep_until(last_activity + inactivity_timeout) => {
                tracing::info!("Connection {} inactive for 90s; closing.", connection_id);
                break;
            }
            msg = socket.recv() => {
                match msg {
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(WsMessage::Ping(data)))
                        if socket.send(WsMessage::Pong(data.clone())).await.is_err() =>
                    {
                        break;
                    }
                    Some(Ok(WsMessage::Text(text))) => {
                        tracing::debug!("WebSocket received: {}", text);

                        last_activity = tokio::time::Instant::now();

                        let raw = match serde_json::from_str::<Value>(&text) {
                            Ok(json) => json,
                            Err(_) => Value::String(text.to_string()),
                        };
                        let record = websocket_record("transport_raw", "inbound", raw, Some(&trace_ctx));
                        if let Err(e) = state.raw_logger.write(record).await {
                            tracing::warn!("RawLogger write failed (inbound): {}", e);
                        }

                        // Try type-based protocol first, then cmd-based.
                        if handle_type_message(&mut socket, &state, &connection_id, &text).await.is_err() {
                            if let Err(e) = handle_command(&mut socket, &state, &text).await {
                                tracing::warn!("WebSocket command handling error: {}", e);
                            }
                        }
                    }
                    Some(Ok(WsMessage::Pong(_))) => {
                        last_activity = tokio::time::Instant::now();
                    }
                    _ => {}
                }
            }
        }
    }

    // Graceful close: attempt to send close frame before dropping.
    let _ = socket.close().await;

    if let Some(ref mgr) = state.connection_manager {
        mgr.unregister(&connection_id).await;
    }
}
