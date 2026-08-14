//! Web UI chat turn — bridges the WebSocket `chat_message` client message to
//! the LLM and streams the reply back to the requesting connection as
//! `agent_event` envelopes (message.start / message.text_delta / message.end),
//! matching what the SPA's `useEventStream` store renders.

use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::mpsc;

use crate::websocket_protocol::ServerMessage;
use crate::GatewayState;

/// Per-session history cap (user + assistant messages combined).
const SESSION_HISTORY_CAP: usize = 40;
/// Hard bound on concurrent session histories; oldest-touched evicted first.
const MAX_SESSIONS: usize = 256;
/// Connecting to the LLM backend should be fast; the failover pool handles
/// slow backends by switching.
const CONNECT_TIMEOUT_SECS: u64 = 30;
/// A reasoning model may think for a long while between deltas; only an idle
/// gap beyond this aborts the turn. Matches the provider's own in-stream cap.
const STREAM_IDLE_TIMEOUT_SECS: u64 = 180;

/// Persona for the Web UI chat. Raw history + user text without a system
/// prompt makes coding-tuned endpoints answer terse and generic.
const CHAT_SYSTEM_PROMPT: &str =
    "你是 Cogneva 多智能体协作平台的内置助手，正在通过平台 Live 面板与用户对话。\
用用户的语言回答（中文提问用中文，英文提问用英文）。\
回答要具体、有实质内容，适当展开但不要啰嗦；不知道就直说不知道，不要编造。";

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn envelope(session_id: &str, event_type: &str, inner: serde_json::Value) -> String {
    let msg = ServerMessage::AgentEvent {
        event_id: uuid::Uuid::new_v4().to_string(),
        session_id: Some(session_id.to_string()),
        task_id: None,
        payload: serde_json::json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "type": event_type,
            "timestamp": now_iso(),
            "payload": inner,
        }),
        server_time: now_iso(),
    };
    serde_json::to_string(&msg).unwrap_or_default()
}

async fn emit(
    out: &mpsc::Sender<String>,
    session_id: &str,
    event_type: &str,
    inner: serde_json::Value,
) {
    // Receiver only disappears when the socket loop ended — nothing to do then.
    let _ = out.send(envelope(session_id, event_type, inner)).await;
}

async fn emit_message_start(
    out: &mpsc::Sender<String>,
    session_id: &str,
    message_id: &str,
    role: &str,
) {
    emit(
        out,
        session_id,
        "message.start",
        serde_json::json!({ "message_id": message_id, "role": role }),
    )
    .await;
}

async fn emit_delta(out: &mpsc::Sender<String>, session_id: &str, message_id: &str, delta: &str) {
    emit(
        out,
        session_id,
        "message.text_delta",
        serde_json::json!({ "message_id": message_id, "delta": delta }),
    )
    .await;
}

async fn emit_message_end(out: &mpsc::Sender<String>, session_id: &str, message_id: &str) {
    emit(
        out,
        session_id,
        "message.end",
        serde_json::json!({ "message_id": message_id }),
    )
    .await;
}

/// Run one chat turn: echo the user message, stream the LLM reply delta by
/// delta (typewriter style — the routing layer still fails over transparently
/// if a backend errors before the first content delta), and record the turn in
/// the session history.
pub async fn run_chat_turn(
    state: Arc<GatewayState>,
    out: mpsc::Sender<String>,
    session_id: String,
    content: String,
) {
    // 1. Echo the user message so the UI renders it (the real transport has
    //    no local echo, unlike the mock client).
    let user_mid = format!("user-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &user_mid, "user").await;
    emit_delta(&out, &session_id, &user_mid, &content).await;
    emit_message_end(&out, &session_id, &user_mid).await;

    let assistant_mid = format!("assistant-{}", uuid::Uuid::new_v4());
    emit_message_start(&out, &session_id, &assistant_mid, "assistant").await;

    let llm = state
        .llm_client
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone();

    let Some(llm) = llm else {
        emit_delta(
            &out,
            &session_id,
            &assistant_mid,
            "LLM 未配置或未就绪，无法处理聊天消息。请先在右上角完成 LLM 设置。",
        )
        .await;
        emit_message_end(&out, &session_id, &assistant_mid).await;
        return;
    };

    // 2. Build the request from session history + the new user message.
    let mut messages = {
        let sessions = state.chat_sessions.lock().await;
        sessions
            .get(&session_id)
            .map(|(msgs, _)| msgs.clone())
            .unwrap_or_default()
    };
    messages.insert(0, cog_core::Message::system(CHAT_SYSTEM_PROMPT));
    messages.push(cog_core::Message::user(content.clone()));

    let options = cog_core::ChatOptions {
        response_format: cog_core::ResponseFormat::Text,
        ..Default::default()
    };

    // 3. Stream the reply: deltas are forwarded as they arrive.
    let mut stream = match tokio::time::timeout(
        std::time::Duration::from_secs(CONNECT_TIMEOUT_SECS),
        llm.chat_stream(&messages, &options),
    )
    .await
    {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            emit_delta(
                &out,
                &session_id,
                &assistant_mid,
                &format!("LLM 调用失败：{e}"),
            )
            .await;
            emit_message_end(&out, &session_id, &assistant_mid).await;
            return;
        }
        Err(_) => {
            emit_delta(
                &out,
                &session_id,
                &assistant_mid,
                &format!("LLM 连接超时（>{CONNECT_TIMEOUT_SECS}s），请稍后重试。"),
            )
            .await;
            emit_message_end(&out, &session_id, &assistant_mid).await;
            return;
        }
    };

    let mut reply = String::new();
    let mut failure: Option<String> = None;
    loop {
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
            stream.next(),
        )
        .await;
        match next {
            Ok(Some(cog_core::AssistantMessageEvent::TextDelta { delta, .. })) => {
                reply.push_str(&delta);
                emit_delta(&out, &session_id, &assistant_mid, &delta).await;
            }
            // Reasoning models (thinking forced on at some endpoints) can
            // think for tens of seconds before the first text delta. Forward
            // the thinking stream so the UI shows progress instead of an
            // empty bubble for the whole reasoning window.
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingStart { .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_start",
                    serde_json::json!({ "message_id": assistant_mid }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingDelta { delta, .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_delta",
                    serde_json::json!({ "message_id": assistant_mid, "delta": delta }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::ThinkingEnd { .. })) => {
                emit(
                    &out,
                    &session_id,
                    "message.thinking_end",
                    serde_json::json!({ "message_id": assistant_mid }),
                )
                .await;
            }
            Ok(Some(cog_core::AssistantMessageEvent::Error { error, .. })) => {
                failure = Some(error.content());
                break;
            }
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                failure = Some(format!(
                    "LLM 响应超时（>{STREAM_IDLE_TIMEOUT_SECS}s 无输出）"
                ));
                break;
            }
        }
    }

    // Reasoning-only replies can arrive with no text deltas at all; fall back
    // to the final response payload so the user never sees an empty bubble.
    if failure.is_none() && reply.is_empty() {
        let final_resp = stream.result().await;
        if let Some(err) = final_resp.error_message {
            failure = Some(err);
        } else {
            reply = final_resp
                .content
                .iter()
                .filter_map(|b| b.as_text())
                .collect::<Vec<_>>()
                .join("");
            if !reply.is_empty() {
                emit_delta(&out, &session_id, &assistant_mid, &reply).await;
            }
        }
    }

    if let Some(err) = &failure {
        emit_delta(
            &out,
            &session_id,
            &assistant_mid,
            &format!("LLM 调用失败：{err}"),
        )
        .await;
    }
    emit_message_end(&out, &session_id, &assistant_mid).await;

    // 4. Record the turn (skip failed/timeout replies — they are not useful
    //    context for later turns).
    if failure.is_none() && !reply.is_empty() {
        let mut sessions = state.chat_sessions.lock().await;
        if sessions.len() >= MAX_SESSIONS && !sessions.contains_key(&session_id) {
            if let Some(oldest) = sessions
                .iter()
                .min_by_key(|(_, (_, ts))| *ts)
                .map(|(k, _)| k.clone())
            {
                sessions.remove(&oldest);
            }
        }
        let entry = sessions
            .entry(session_id)
            .or_insert_with(|| (Vec::new(), chrono::Utc::now()));
        entry.0.push(cog_core::Message::user(content));
        entry.0.push(cog_core::Message::assistant_text(reply));
        if entry.0.len() > SESSION_HISTORY_CAP {
            let drop = entry.0.len() - SESSION_HISTORY_CAP;
            entry.0.drain(..drop);
        }
        entry.1 = chrono::Utc::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_matches_frontend_contract() {
        let raw = envelope(
            "s1",
            "message.start",
            serde_json::json!({"message_id": "m1", "role": "assistant"}),
        );
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(v["type"], "agent_event");
        assert_eq!(v["session_id"], "s1");
        assert_eq!(v["payload"]["type"], "message.start");
        assert_eq!(v["payload"]["payload"]["message_id"], "m1");
        assert_eq!(v["payload"]["payload"]["role"], "assistant");
    }
}
