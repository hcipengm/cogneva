use axum::extract::ws::{Message as WsMessage, WebSocket};
use futures::SinkExt;
use serde_json::Value;
use std::sync::Arc;

use crate::websocket_protocol::{event_channels, ClientMessage, ConnectionManager, ServerMessage};
use crate::GatewayState;
use cog_core::TraceContext;
use cog_core::{RawContext, RawMeta, RawPayload, RawRecord};

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

/// 一条连接的来历：它自己叫什么、以谁的身份连着、从哪个客户端连上来。
///
/// 三格回答的是同一个问题，所以一起走——分行传只是把参数个数往上加，加到第八个
/// 时 clippy 会以「太多」判红，而它说的正是这件事：这些格子属于一个整体。三格里
/// 没有一格是多出来的：`id` 是这条连接自己的名字（订阅与事件分发按它找），
/// `user_id` 是握手时认出来的人（聊天花销记在他身上），`origin` 是客户端自报的
/// 我在哪儿（只回答名单上「这一行是谁开出来的会话」，不是认证主体）。
#[derive(Debug, Clone, Copy)]
struct Connection<'a> {
    id: &'a str,
    /// The person this connection authenticated as, from the verified token
    /// taken at upgrade. Chat spend is charged to it: the socket is the only
    /// place on the platform where a human's identity is established, so it is
    /// the only place a per-person ceiling can be enforced from.
    user_id: &'a str,
    origin: crate::chat::ClientOrigin<'a>,
}

/// 把这一轮放到后台跑起来。
///
/// 一轮对话可能要几十秒——绝不能挡住套接字这一圈（心跳与事件转发都在这条路上）。
/// 一句话与一条命令走的是同一条路：命令只是多带了一样东西（点了哪一个名字），
/// 回声、记录、停止与问人的通道都是共用的，所以这里是同一个出口。
#[allow(clippy::too_many_arguments)]
fn start_chat_turn(
    state: &Arc<GatewayState>,
    out_tx: &tokio::sync::mpsc::Sender<String>,
    conn: Connection<'_>,
    permissions: &Arc<crate::chat::PermissionChannel>,
    session_id: Option<String>,
    content: String,
    command: Option<crate::commands::Invocation>,
    attachments: Vec<cog_core::ChatAttachment>,
    addressed_to: String,
) {
    let state = state.clone();
    let session = session_id.unwrap_or_else(|| "default".into());
    let owner = conn.user_id.to_string();
    // 借用版活不过 spawn：来历里那两格先拷成自己的字符串，闭包里再借回去。
    let platform = conn.origin.platform.map(str::to_string);
    let device_id = conn.origin.device_id.map(str::to_string);
    // 这一轮说的是这一段对话的流；停止与续流都按会话找它（见 `chat_stream`）。
    let stream = state.chat_stream.clone();
    // 这一轮的问人通道就是这条连接：问句从这里出去，答案从 `permission_response`
    // 回来。模式取这一段对话当前那一格。
    let mode = permissions.mode(&session);
    let bridge: Arc<dyn crate::chat::PermissionBridge> =
        Arc::new(crate::chat::SocketPermissionBridge::new(
            session.clone(),
            out_tx.clone(),
            Arc::clone(permissions),
        ));
    tokio::spawn(async move {
        let origin = crate::chat::ClientOrigin {
            platform: platform.as_deref(),
            device_id: device_id.as_deref(),
        };
        crate::chat::run_chat_turn(
            state,
            stream,
            session,
            content,
            command,
            attachments,
            owner,
            addressed_to,
            origin,
            mode,
            Some(bridge),
        )
        .await;
    });
}

/// 这一句能不能落进 `session_id` 那段对话。不能时把原因作为一条回答发回这条
/// 连接——静默丢掉会让人以为说过了。
async fn refuses_session(
    state: &Arc<GatewayState>,
    out_tx: &tokio::sync::mpsc::Sender<String>,
    conn: Connection<'_>,
    session_id: Option<&str>,
    content: &str,
) -> bool {
    let Some(session) = session_id.filter(|s| !s.trim().is_empty()) else {
        // 没有会话号的那一句落进 `default`，与 `start_chat_turn` 用的是同一个默认。
        return false;
    };
    let Err(reason) = crate::sessions::speaks_here(state, session, conn.user_id).await else {
        return false;
    };
    // 回声与回答都发在这一段会话上：界面已经把这句画在屏幕上了，答在这里，
    // 读的人才知道它为什么没有下文。
    let user_mid = format!("user-{}", uuid::Uuid::new_v4());
    let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
    for (kind, inner) in [
        (
            "message.start",
            serde_json::json!({ "message_id": user_mid, "role": "user", "attachment": serde_json::Value::Null }),
        ),
        (
            "message.text_delta",
            serde_json::json!({ "message_id": user_mid, "delta": content }),
        ),
        ("message.end", serde_json::json!({ "message_id": user_mid })),
        (
            "message.start",
            serde_json::json!({ "message_id": assistant_mid, "role": "assistant", "attachment": serde_json::Value::Null }),
        ),
        (
            "message.text_delta",
            serde_json::json!({ "message_id": assistant_mid, "delta": reason }),
        ),
        (
            "message.end",
            serde_json::json!({ "message_id": assistant_mid }),
        ),
    ] {
        let frame = crate::websocket_protocol::ServerMessage::AgentEvent {
            event_id: uuid::Uuid::new_v4().to_string(),
            session_id: Some(session.to_string()),
            task_id: None,
            payload: serde_json::json!({
                "id": uuid::Uuid::new_v4().to_string(),
                "type": kind,
                "timestamp": now_iso(),
                "payload": inner,
            }),
            server_time: now_iso(),
        };
        let _ = out_tx
            .send(serde_json::to_string(&frame).unwrap_or_default())
            .await;
    }
    true
}

/// 把这条 socket 接上这一段对话的流（第一次接时顺带收一份快照）。
///
/// 没给会话号的那一句落进 `default`，与 [`start_chat_turn`] 用的是同一个默认——
/// 两处各算一遍，接上的那一段与跑起来的那一段就有可能不是一个。
fn watch_session(
    state: &Arc<GatewayState>,
    watched: &mut std::collections::HashSet<String>,
    session_id: Option<&str>,
    out_tx: &tokio::sync::mpsc::Sender<String>,
) {
    let session = session_id
        .filter(|s| !s.trim().is_empty())
        .unwrap_or("default");
    if watched.insert(session.to_string()) {
        state.chat_stream.watch(session, out_tx.clone());
    }
}

async fn handle_type_message(
    socket: &mut WebSocket,
    state: &Arc<GatewayState>,
    conn: Connection<'_>,
    text: &str,
    out_tx: &tokio::sync::mpsc::Sender<String>,
    // 这条连接已经接上哪几段对话的流。接上就是「这一段的帧发到这条 socket」；
    // 同一条连接重复接同一段会多一条转发，每一帧都到两次，所以按会话去重。
    watched: &mut std::collections::HashSet<String>,
    // 这条连接上的权限通道：问句出去、答案回来都走它，模式按会话记在这里。
    permissions: &Arc<crate::chat::PermissionChannel>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client_msg: ClientMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => {
            // 不是这一族的上行消息。套接字上只有这一族：曾经还有一套 `{"cmd": …}`
            // 的命令协议（ping / abort / status / list），两条路都在同一条连接上试，
            // 而那一套没有任何客户端发过——它要的四个能力 REST 全有。留着它，就等于
            // 让每条认不出的消息都多一次解析、多一条永远不会有人读的回执。
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
            let subscribed = mgr.subscribe(conn.id, &channels).await;
            let resp = ServerMessage::Subscribed {
                channels: subscribed,
                server_time: now_iso(),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::Typing { .. } => {
            // Typing indicator is accepted but produces no response.
        }
        ClientMessage::ChatMessage {
            session_id,
            content,
            addressed_to,
            attachments,
            ..
        } => {
            let content = content.trim().to_string();
            // 带附件的这一句可以没有正文：一张截图配一句「这里错了」，图才是要说的
            // 那部分。两样都空才是没说话。
            if content.is_empty() && attachments.is_empty() {
                return Ok(());
            }
            // 别人的对话不进：这一句连循环都不交出去，只在屏幕上说明原因。
            if refuses_session(state, out_tx, conn, session_id.as_deref(), &content).await {
                return Ok(());
            }
            watch_session(state, watched, session_id.as_deref(), out_tx);
            start_chat_turn(
                state,
                out_tx,
                conn,
                permissions,
                session_id,
                content,
                None,
                attachments,
                addressed_to.unwrap_or_default(),
            );
        }
        ClientMessage::Command {
            session_id,
            name,
            arguments,
            attachments,
            ..
        } => {
            let invocation = crate::commands::Invocation { name, arguments };
            // 空名字不是一条命令，也不是一句话：什么都不说就是不说话。
            if invocation.line().trim() == "/" {
                return Ok(());
            }
            if refuses_session(
                state,
                out_tx,
                conn,
                session_id.as_deref(),
                &invocation.line(),
            )
            .await
            {
                return Ok(());
            }
            watch_session(state, watched, session_id.as_deref(), out_tx);
            // 命令这一路没有收件人：命令是这一屏自己的动作，不是对某个人说的话。
            start_chat_turn(
                state,
                out_tx,
                conn,
                permissions,
                session_id,
                invocation.line(),
                Some(invocation),
                attachments,
                String::new(),
            );
        }
        ClientMessage::PermissionResponse {
            request_id,
            allow,
            reason,
            remember,
            ..
        } => {
            let answer = if allow {
                crate::chat::PermissionAnswer::Allow { always: remember }
            } else {
                crate::chat::PermissionAnswer::Deny {
                    reason: reason.unwrap_or_else(|| "人在对话框里拒绝了这一次执行。".into()),
                    always: remember,
                }
            };
            // 没人等这一问（重复的答案、或者那一轮已经被停掉）不是错误：那一问
            // 已经不在了，答案落地与否不改变任何结果。
            let delivered = permissions.answer(&request_id, answer);
            tracing::debug!(request = %request_id, delivered, "permission answer");
        }
        ClientMessage::PermissionMode {
            session_id, mode, ..
        } => {
            permissions.set_mode(&session_id, &mode);
        }
        ClientMessage::PermissionRules { session_id, .. } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            if let Some(conversation) = state.conversation.get().cloned() {
                let rules = conversation.permission_rules(conn.user_id, &session).await;
                let resp = ServerMessage::PermissionRules {
                    session_id: session,
                    rules,
                };
                send_server_message(socket, &resp).await?;
            }
        }
        ClientMessage::PermissionRuleRevoke {
            session_id,
            rule_id,
            ..
        } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            if let Some(conversation) = state.conversation.get().cloned() {
                let rules = conversation
                    .revoke_permission_rule(conn.user_id, &session, &rule_id)
                    .await;
                let resp = ServerMessage::PermissionRules {
                    session_id: session,
                    rules,
                };
                send_server_message(socket, &resp).await?;
            }
        }
        ClientMessage::OwnerMemory { .. } => {
            if let Some(conversation) = state.conversation.get().cloned() {
                let entries = conversation.owner_memory(conn.user_id).await;
                send_server_message(socket, &ServerMessage::OwnerMemory { entries }).await?;
            }
        }
        ClientMessage::OwnerMemoryWrite { text, .. } => {
            if let Some(conversation) = state.conversation.get().cloned() {
                let entries = conversation
                    .remember_owner_memory(conn.user_id, &text)
                    .await;
                send_server_message(socket, &ServerMessage::OwnerMemory { entries }).await?;
            }
        }
        ClientMessage::OwnerMemoryForget { id, .. } => {
            if let Some(conversation) = state.conversation.get().cloned() {
                let entries = conversation.forget_owner_memory(conn.user_id, &id).await;
                send_server_message(socket, &ServerMessage::OwnerMemory { entries }).await?;
            }
        }
        ClientMessage::ChatModel {
            session_id, model, ..
        } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            let catalogue = crate::chat_models::catalogue().await;
            let answer = |model: Option<String>, reason: Option<String>| ServerMessage::ChatModel {
                session_id: session.clone(),
                model,
                models: catalogue.models.clone(),
                configured: catalogue.configured,
                reason,
            };
            let Some(conversation) = state.conversation.get().cloned() else {
                let resp = answer(None, Some("这套部署没有接上对话面，换不了模型。".into()));
                send_server_message(socket, &resp).await?;
                return Ok(());
            };

            // 不带 `model` 是只读：界面上打开下拉框先问一次「现在用哪只、能选
            // 哪些」。读失败要说出来——画成「没选过」等于替人换了一只。
            let Some(wanted) = model.as_deref().map(str::trim) else {
                let resp = match conversation.chosen_model(conn.user_id, &session).await {
                    Ok(chosen) => answer(chosen, catalogue.reason.clone()),
                    Err(error) => answer(None, Some(format!("读不到这一段选的模型：{error}"))),
                };
                send_server_message(socket, &resp).await?;
                return Ok(());
            };

            // 换的是这一段对话的属性，所以先判这一段能不能由这个人改。
            if let Err(reason) = crate::sessions::speaks_here(state, &session, conn.user_id).await {
                let resp = answer(None, Some(reason));
                send_server_message(socket, &resp).await?;
                return Ok(());
            }

            // 空串是「改回默认」：清单列不出来的那一只不该靠它恢复，所以这一支
            // 不看清单。真要挑一只时，清单外的一律不收——收下来，下一轮就会用
            // 一只平台根本没配的模型发请求。
            let target = (!wanted.is_empty()).then_some(wanted);
            if let Some(target) = target {
                if !catalogue.offers(target) {
                    let reason = if catalogue.configured {
                        format!(
                            "清单里没有 `{target}` 这只模型，能选的是：{}。",
                            if catalogue.models.is_empty() {
                                "（清单是空的）".to_string()
                            } else {
                                catalogue.models.join("、")
                            }
                        )
                    } else {
                        catalogue
                            .reason
                            .clone()
                            .unwrap_or_else(|| "平台还没有配置任何模型。".into())
                    };
                    let resp = answer(None, Some(reason));
                    send_server_message(socket, &resp).await?;
                    return Ok(());
                }
            }

            let resp = match conversation
                .choose_model(conn.user_id, &session, target)
                .await
            {
                Ok(chosen) => answer(chosen, catalogue.reason.clone()),
                Err(error) => answer(None, Some(error)),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::ChatCheckpoint {
            session_id,
            turn,
            apply,
            ..
        } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            let reply =
                |rows: Vec<crate::chat::CheckpointView>,
                 hidden: usize,
                 rewind: Option<crate::chat::RewindReport>,
                 reason: Option<String>| ServerMessage::ChatCheckpoint {
                    session_id: session.clone(),
                    rows,
                    hidden,
                    rewind,
                    reason,
                };
            // 读的人得是这一段的读者：检查点的清单里有路径，退回改的是那棵树。
            // 与说话、换模型、续流同一把尺子。
            if let Err(reason) = crate::sessions::speaks_here(state, &session, conn.user_id).await {
                send_server_message(socket, &reply(Vec::new(), 0, None, Some(reason))).await?;
                return Ok(());
            }
            let Some(conversation) = state.conversation.get().cloned() else {
                let reason = "这套部署没有接上对话面，看不见检查点。".to_string();
                send_server_message(socket, &reply(Vec::new(), 0, None, Some(reason))).await?;
                return Ok(());
            };
            let resp = match turn {
                None => match conversation.checkpoints(conn.user_id, &session).await {
                    Ok(list) => reply(list.rows, list.hidden, None, None),
                    Err(reason) => reply(Vec::new(), 0, None, Some(reason)),
                },
                Some(turn) => match conversation
                    .rewind(conn.user_id, &session, turn, apply)
                    .await
                {
                    Ok(report) => reply(Vec::new(), 0, Some(report), None),
                    Err(reason) => reply(Vec::new(), 0, None, Some(reason)),
                },
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::ChatSandboxBoundary { session_id, .. } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            let reply = |boundary: Option<cog_core::SandboxBoundary>, reason: Option<String>| {
                ServerMessage::ChatSandboxBoundary {
                    session_id: session.clone(),
                    boundary,
                    reason,
                }
            };
            // 边界里有工作树路径，属于这一段对话的家底：与说话、列检查点同一把尺子。
            if let Err(reason) = crate::sessions::speaks_here(state, &session, conn.user_id).await {
                send_server_message(socket, &reply(None, Some(reason))).await?;
                return Ok(());
            }
            let Some(conversation) = state.conversation.get().cloned() else {
                let reason = "这套部署没有接上对话面，说不清命令跑在哪里。".to_string();
                send_server_message(socket, &reply(None, Some(reason))).await?;
                return Ok(());
            };
            let resp = match conversation.sandbox_boundary(conn.user_id, &session).await {
                Ok(boundary) => reply(Some(boundary), None),
                Err(reason) => reply(None, Some(reason)),
            };
            send_server_message(socket, &resp).await?;
        }
        ClientMessage::ChatResume { session_id, .. } => {
            let session = session_id.unwrap_or_else(|| "default".into());
            // 续的是一段对话的流，所以先判这一段能不能给这个人看：不能就不接，
            // 也不回快照——别人的对话里在说什么，与他无关。
            if let Err(reason) = crate::sessions::speaks_here(state, &session, conn.user_id).await {
                tracing::debug!(session = %session, %reason, "chat resume refused");
                return Ok(());
            }
            // 这一条连接可能已经接在这一段上了（重发一次 resume 不是错）：
            // `watch_session` 按会话去重，避免同一条 socket 收到两份帧。
            watch_session(state, watched, Some(&session), out_tx);
        }
        ClientMessage::ChatCancel { session_id, .. } => {
            // 没有在跑的那一段上按停止不是错：用户可能刚好在上一轮结束的那一瞬
            // 按下去。这时什么都不发生，也不该有一条「取消失败」的帧——他真正
            // 关心的是屏幕上那一轮停了没有，而它已经停了。
            // 别人的对话停不了：停按会话寻址，而名册是整台部署共用的（见 E3），
            // 没有这一步，猜一个会话号就能掐掉别人正在跑的那一轮。
            match crate::sessions::speaks_here(state, &session_id, conn.user_id).await {
                Ok(()) => {
                    let stopped = state.chat_stream.running().cancel(&session_id);
                    tracing::debug!(session = %session_id, stopped, "chat cancel");
                }
                Err(reason) => {
                    tracing::debug!(session = %session_id, %reason, "chat cancel refused");
                }
            }
        }
    }

    Ok(())
}

/// Filter, record, and forward one agent event to this connection.
/// Returns false only when the socket itself is broken.
async fn forward_agent_event(
    socket: &mut WebSocket,
    state: &Arc<GatewayState>,
    connection_id: &str,
    trace_ctx: &TraceContext,
    event: &cog_core::AgentEvent,
) -> bool {
    // Channel filtering
    if let Some(ref mgr) = state.connection_manager {
        let channels = event_channels(event);
        if !mgr.should_deliver(connection_id, &channels).await {
            return true;
        }
    }

    let payload = match serde_json::to_string(event) {
        Ok(json) => json,
        Err(_) => return true,
    };

    // message_update fires per token delta; recording and raw-logging each one
    // would write-amplify the audit path by orders of magnitude. Stream
    // details are observable live and worthless as replay/audit entries.
    let is_stream_detail = matches!(event, cog_core::AgentEvent::MessageUpdate { .. });
    if !is_stream_detail {
        if let Ok(value) = serde_json::to_value(event) {
            // Record in global missed-events cache
            if let Some(ref mgr) = state.connection_manager {
                let event_id = uuid::Uuid::new_v4().to_string();
                mgr.record_event(event_id.clone(), value.clone()).await;
            }

            let record = websocket_record("session_raw", "outbound", value, Some(trace_ctx));
            if let Err(e) = state.raw_logger.write(record).await {
                tracing::warn!("RawLogger write failed (outbound): {}", e);
            }
        }
    }

    socket.send(WsMessage::Text(payload.into())).await.is_ok()
}

/// 这条通知是不是发给这条连接的。
///
/// 通知带收件人（`user_id`），而 `notification_tx` 是广播：每个打开的连接都
/// 会收到每一条。缺这一步，站内通知就成了把别人的通知推给所有人。空身份
/// 的连接一条都收不到——没有身份就没有收件人可以对。
fn addressed_to(notification: &cog_core::Notification, user_id: &str) -> bool {
    !user_id.is_empty() && notification.user_id == user_id
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
    let mut task_rx = state.subscribe_task_events();
    let mut notif_rx = state.notification_tx.subscribe();
    // Outbound queue for messages produced outside the event broadcast (e.g.
    // chat replies from spawned LLM tasks).
    let (out_tx, mut out_rx) = tokio::sync::mpsc::channel::<String>(32);
    // 这条连接接上了哪几段对话的流。断线重连上来的是一条新连接，它的这一格是
    // 空的——所以它会重新接一次，并且先收到一份快照（见 `chat_stream`）。
    let mut watched: std::collections::HashSet<String> = std::collections::HashSet::new();
    // 这条连接上的权限通道：一段对话一格模式，一行问题一个等待位。
    let permissions = Arc::new(crate::chat::PermissionChannel::new());

    // Register connection and send "connected" event.
    if let Some(ref mgr) = state.connection_manager {
        mgr.register(
            connection_id.clone(),
            user_id.clone(),
            device_id.clone(),
            platform.clone(),
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
    // 0 在这里不是「不心跳」：零周期的定时器构造即 panic，所以读侧把 0 换成内置值。
    let ws_timeout = std::time::Duration::from_secs(
        state
            .config
            .read()
            .unwrap()
            .effective_websocket_heartbeat_secs(),
    );
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
            .effective_websocket_inactivity_timeout_secs(),
    );
    let mut last_activity = tokio::time::Instant::now();

    loop {
        tokio::select! {
            Ok(event) = rx.recv() => {
                last_activity = tokio::time::Instant::now();
                if !forward_agent_event(&mut socket, &state, &connection_id, &trace_ctx, &event).await {
                    break;
                }
            }
            // Task lifecycle events are translated at this edge (same shape
            // and channel filtering as agent events) — no re-injection into
            // the agent bus.
            Ok(task_event) = task_rx.recv() => {
                last_activity = tokio::time::Instant::now();
                let event = crate::websocket_protocol::task_event_as_status_change(&task_event);
                if !forward_agent_event(&mut socket, &state, &connection_id, &trace_ctx, &event).await {
                    break;
                }
            }
            Ok(notification) = notif_rx.recv() => {
                // 广播通道发给每条连接，收件人写在通知里；不过滤就是把所有人
                // 的通知群发给每个登录用户。别人的通知不算这条连接的活跃。
                if !addressed_to(&notification, &user_id) {
                    continue;
                }
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
            Some(outbound) = out_rx.recv() => {
                last_activity = tokio::time::Instant::now();
                if socket.send(WsMessage::Text(outbound.into())).await.is_err() {
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

                        if let Err(e) =
                            handle_type_message(
                                &mut socket,
                                &state,
                                Connection {
                                    id: &connection_id,
                                    user_id: &user_id,
                                    origin: crate::chat::ClientOrigin {
                                        platform: platform.as_deref(),
                                        device_id: device_id.as_deref(),
                                    },
                                },
                                &text,
                                &out_tx,
                                &mut watched,
                                &permissions,
                            )
                            .await
                        {
                            tracing::debug!("WebSocket message not handled: {}", e);
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

#[cfg(test)]
mod tests {
    use super::addressed_to;
    use chrono::Utc;

    fn notification_for(user_id: &str) -> cog_core::Notification {
        cog_core::Notification {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            ntype: cog_core::NotificationType::System,
            title: "t".into(),
            body: "b".into(),
            is_read: false,
            created_at: Utc::now(),
            read_at: None,
        }
    }

    #[test]
    fn a_notification_reaches_only_its_recipient() {
        let alice = notification_for("alice");
        assert!(addressed_to(&alice, "alice"));
        assert!(!addressed_to(&alice, "bob"));
        // 身份为空的连接不在收件人名单上：它对不上任何人，也不该收到所有人。
        assert!(!addressed_to(&alice, ""));
        assert!(!addressed_to(&notification_for(""), "alice"));
    }
}
