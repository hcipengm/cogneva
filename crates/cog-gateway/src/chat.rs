//! Web UI chat turn — bridges the WebSocket `chat_message` client message to
//! the LLM and streams the reply back to the requesting connection as
//! `agent_event` envelopes (message.start / message.text_delta / message.end),
//! matching what the SPA's `useEventStream` store renders.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::websocket_protocol::ServerMessage;
use crate::GatewayState;

/// Per-session history cap (user + assistant messages combined).
const SESSION_HISTORY_CAP: usize = 40;
/// Hard bound on concurrent session histories; oldest-touched evicted first.
const MAX_SESSIONS: usize = 256;
/// A chat turn may wait on a reasoning model for well over a minute.
const CHAT_TIMEOUT_SECS: u64 = 180;

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

/// Run one chat turn: echo the user message, call the LLM with session
/// history, stream the assistant reply back as a single delta, and record the
/// turn in the session history.
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
    messages.push(cog_core::Message::user(content.clone()));

    let options = cog_core::ChatOptions {
        response_format: cog_core::ResponseFormat::Text,
        ..Default::default()
    };

    // 3. Call the LLM (non-streaming: the full failover pool applies).
    let reply = match tokio::time::timeout(
        std::time::Duration::from_secs(CHAT_TIMEOUT_SECS),
        llm.chat(&messages, &options),
    )
    .await
    {
        Ok(Ok(resp)) => resp
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<Vec<_>>()
            .join(""),
        Ok(Err(e)) => format!("LLM 调用失败：{e}"),
        Err(_) => format!("LLM 响应超时（>{CHAT_TIMEOUT_SECS}s），请稍后重试。"),
    };

    emit_delta(&out, &session_id, &assistant_mid, &reply).await;
    emit_message_end(&out, &session_id, &assistant_mid).await;

    // 4. Record the turn (skip failed/timeout replies — they are not useful
    //    context for later turns).
    if !reply.starts_with("LLM 调用失败") && !reply.starts_with("LLM 响应超时") {
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
