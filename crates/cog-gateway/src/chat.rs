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
/// Intent classification is a one-word answer; anything slower means the
/// backend is struggling and we fall back to the plain chat path.
const CLASSIFY_TIMEOUT_SECS: u64 = 20;

/// Intent router prompt. The Web UI chat is an entry point into the system,
/// not a side channel: actionable messages must reach the orchestrator so
/// they flow through decompose → DAG → squad like every other intent, with
/// progress coming back on the event stream. Pure Q&A keeps the direct
/// streaming path.
const INTENT_CLASSIFY_PROMPT: &str =
    "判断用户消息属于哪一类。\
ACT = 可执行意图：要求系统做事情（构建、修改、修复、实现、运行、部署、排查、分析、优化等会产生实际动作或代码变更的请求）。\
CHAT = 纯问答：聊天、知识提问、解释概念、询问系统状态等只需要文字回答的消息。\
只回答一个词：ACT 或 CHAT，不要任何其他内容。";

/// Persona for the Web UI chat. Identity follows the project definition:
/// Cogneva is a distributed AI multi-agent autonomous system, not a Q&A
/// assistant — the configured upstream model only supplies the language
/// capability. Raw history + user text without a system prompt makes
/// coding-tuned endpoints answer terse, generic, and clueless about the
/// platform.
const CHAT_SYSTEM_PROMPT: &str =
    "你是 Cogneva——分布式 AI 多智能体自治系统（元启动·真自治·全进化），一个能自己站起来、自己运转、自己成长的数字生命体。用户正通过系统的 Live 面板与你对话。\
身份口径：你就是 Cogneva 系统本身，不是客服也不是问答助手；语言模型由管理员在设置向导中配置的上游提供，那只是你的语言能力来源，不是你的身份。被问及身份时按此口径回答，可简要介绍三大支柱（元启动解决系统起源、真自治解决系统意志、全进化解决系统成长）。\
用用户的语言回答（中文提问用中文，英文提问用英文）。回答要具体、有实质内容，适当展开但不要啰嗦；不知道就直说不知道，不要编造。";

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Route a chat message: ACT (system should do something) or CHAT (answer
/// directly). Classification failure defaults to CHAT — answering is always
/// safe, silently dropping an intent into a goal is not.
async fn is_actionable_intent(llm: &Arc<dyn cog_core::LlmClient>, content: &str) -> bool {
    let messages = [
        cog_core::Message::system(INTENT_CLASSIFY_PROMPT),
        cog_core::Message::user(content),
    ];
    let options = cog_core::ChatOptions::default();
    let res = tokio::time::timeout(
        std::time::Duration::from_secs(CLASSIFY_TIMEOUT_SECS),
        llm.chat(&messages, &options),
    )
    .await;
    match res {
        Ok(Ok(resp)) => resp
            .content
            .iter()
            .filter_map(|b| b.as_text())
            .collect::<String>()
            .trim()
            .to_ascii_uppercase()
            .starts_with("ACT"),
        _ => false,
    }
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

    // 2. Route the intent. Actionable messages go to the orchestrator as a
    //    goal (decompose → DAG → squad); progress returns on the event
    //    stream, which the UI renders live. Decomposition takes 50-90s of
    //    LLM planning, so it runs detached and reports back on this
    //    connection when it lands.
    if is_actionable_intent(&llm, &content).await {
        emit_delta(
            &out,
            &session_id,
            &assistant_mid,
            "收到，这是可执行意图。已作为目标提交给编排器：分解为任务后由 squad 接管执行，进度会实时出现在右侧目标流水线；分解完成或失败我都会在这里回报。",
        )
        .await;
        emit_message_end(&out, &session_id, &assistant_mid).await;

        let orchestrator = state.orchestrator.clone();
        let out2 = out.clone();
        let sid = session_id.clone();
        let goal = content.clone();
        tokio::spawn(async move {
            // message.start is emitted only when there is content — the
            // decomposition takes minutes, and an empty bubble that whole
            // time reads as broken.
            let text = match orchestrator.submit_goal_auto(&goal, Vec::new()).await {
                Ok(ids) => format!(
                    "目标分解完成，已注入 {} 个任务进入执行图。squad 正在执行，进度见右侧目标流水线。",
                    ids.len()
                ),
                Err(e) => format!("目标分解失败：{e}。可以换个更具体的描述再试一次。"),
            };
            let mid = format!("assistant-{}", uuid::Uuid::new_v4());
            emit_message_start(&out2, &sid, &mid, "assistant").await;
            emit_delta(&out2, &sid, &mid, &text).await;
            emit_message_end(&out2, &sid, &mid).await;
        });
        return;
    }

    // 3. Build the request from session history + the new user message.
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

    // 4. Stream the reply: deltas are forwarded as they arrive.
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

    // 5. Record the turn (skip failed/timeout replies — they are not useful
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
