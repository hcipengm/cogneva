//! These endpoints expose the **conversation session** concept (a continuous
//! AI chat thread) to mobile / web clients. Persistent storage and message
//! history will be wired up once the `sf-session` adapter lands; for now
//! these handlers validate auth, accept the documented request shapes, and
//! return canned responses so the client SDKs can develop against the real
//! routing surface.
//! Routes wired in [`crate::create_router`]:
//! - `GET    /api/v1/sessions`                 — list sessions
//! - `POST   /api/v1/sessions`                 — create session
//! - `GET    /api/v1/sessions/{id}`            — get session detail
//! - `PUT    /api/v1/sessions/{id}`            — update session
//! - `DELETE /api/v1/sessions/{id}`            — delete (soft) session
//! - `GET    /api/v1/sessions/{id}/messages`   — fetch message history
//! - `POST   /api/v1/sessions/{id}/messages`   — send message (stream task)
//! - `POST   /api/v1/sessions/{id}/messages/{message_id}/regenerate`
//!
//! Session management handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

use crate::GatewayState;

/// 谁能读一段对话、谁能在里面说话。
///
/// 三格是闭集，判据只有一条：这一段记下的那个人是不是读的人。
///
/// - `Shared`：这一段没有记下人——写它的时候那条连接没有登录身份。没有人可
///   保护的对话对这台部署上的所有人都开放，而返回里会明说它是共享的（见
///   [`shared_marking`]），不把「没有属主」画成「属于你」。
/// - `Own`：记下的就是读的人。
/// - `SomebodyElses`：记下的是别人。对读的人来说它与「这一段不存在」是同一个
///   答案——告诉猜会话号的人「这一段存在、但不是你的」，等于把「谁的会话号是
///   这个」告诉了他；而会话号是可以猜的。
///
/// 空名字的连接落在 `SomebodyElses`：没有身份的人对一段有主的对话没有话语权。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAccess {
    Shared,
    Own,
    SomebodyElses,
}

/// 判一段对话能不能交给这个人。看的是这一段记下的那个人，不是请求里带了什么。
pub fn access(owner_of_session: Option<&str>, caller: &str) -> SessionAccess {
    let caller = caller.trim();
    match owner_of_session.map(str::trim).filter(|o| !o.is_empty()) {
        None => SessionAccess::Shared,
        Some(owner) if !caller.is_empty() && owner == caller => SessionAccess::Own,
        Some(_) => SessionAccess::SomebodyElses,
    }
}

/// 返回体里标「这一段是共享的」的那一格。
///
/// 只有 `Shared` 为真：一段有主的对话不给读的人看它属于谁——那是另一个人，
/// 而这一段能不能读已经在 [`access`] 里判过了。
fn shared_marking(access: SessionAccess) -> bool {
    matches!(access, SessionAccess::Shared)
}

/// 一条连接能不能在 `session_id` 里说话。
///
/// 与读面同一把尺子：共享的、自己的都能说；别人的不能说。没有登录身份的连接
/// 也走这一条，所以它只能在共享的对话里说话——它没有名字，而一段记录了名字的
/// 对话不是它的。
///
/// 读不到「这一段属于谁」时按拒绝处理：拿不到判据的那一次不放进任何人的对话，
/// 一个猜出来的会话号不该因为存储抖了一下就变成可写的。返回的是给界面看的那句话。
pub async fn speaks_here(
    state: &GatewayState,
    session_id: &str,
    caller: &str,
) -> Result<(), String> {
    let Some(store) = state.chat_turns.as_ref() else {
        // 没有记录库：这一段不会留下任何人的话，也就没有别人可保护的东西。
        return Ok(());
    };
    let owner = store
        .owner_of(session_id)
        .await
        .map_err(|e| format!("判不了这一段对话属于谁，所以这一句没有发出去（{e}）。"))?;
    match access(owner.as_deref(), caller) {
        SessionAccess::Shared | SessionAccess::Own => Ok(()),
        SessionAccess::SomebodyElses => Err(
            "这一段对话属于另一个人，这一句没有发出去。要开一段自己的，换一个新的会话号。".into(),
        ),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ListSessionsQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    /// 只在某一段话里找过某几个字时给。给的是字面，不是通配符：搜索框里打的
    /// `%` 是用户在找 `%`，不是「什么都算」。
    #[serde(default)]
    pub search: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateSessionPayload {
    #[serde(default)]
    pub title: Option<String>,
    pub workspace_id: String,
    #[serde(default)]
    pub agent_id: Option<String>,
    #[serde(default)]
    pub context: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSessionPayload {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
pub struct ListMessagesQuery {
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub direction: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SendMessagePayload {
    pub content: String,
    #[serde(default)]
    pub attachments: Vec<String>,
    #[serde(default)]
    pub options: Option<serde_json::Value>,
}

/// One person's conversations, newest activity first.
///
/// This used to answer `items: []` unconditionally while the transcript held
/// every session the platform had ever recorded: the one endpoint named "list
/// sessions" could not name a single one, so a page that lost its dock (a new
/// browser, cleared storage) had no way back to a conversation -- the record was
/// kept and unreachable.
///
/// The listing is scoped to the caller. The transcript holds every person's
/// conversations, so an unscoped list would be a list of other people's
/// questions; a caller with no established identity gets a refusal instead of an
/// empty page, because "you have no conversations" and "we do not know who you
/// are" are different answers and only one of them is true.
pub async fn list_sessions_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Query(q): Query<ListSessionsQuery>,
) -> Response {
    let Some(owner) = claims
        .as_ref()
        .map(|axum::Extension(c)| c.sub.trim())
        .filter(|sub| !sub.is_empty())
    else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "code": crate::error::simple_code::UNAUTHORIZED,
                "error": "需要先登录才能列出自己的会话",
            })),
        )
            .into_response();
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 200) as usize;
    let Some(store) = state.chat_turns.as_ref() else {
        // 这套部署没有聊天记录库：空名单是真话，与 `list_messages` 同一种答法。
        return (
            StatusCode::OK,
            Json(json!({
                "code": crate::error::simple_code::OK,
                "data": {
                    "items": [],
                    "next_cursor": null,
                    "has_more": false,
                }
            })),
        )
            .into_response();
    };
    match store.sessions(owner, limit, q.search.as_deref()).await {
        Ok(sessions) => {
            let items: Vec<serde_json::Value> = sessions
                .into_iter()
                .map(|session| {
                    json!({
                        "session_id": session.session_id,
                        "title": session.title,
                        "last_activity_at": session.last_activity_at,
                        "turns": session.turns,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "code": crate::error::simple_code::OK,
                    "data": {
                        "items": items,
                        "next_cursor": null,
                        "has_more": false,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": "error",
                "message": format!("读不到会话名单：{e}"),
            })),
        )
            .into_response(),
    }
}

pub async fn create_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Json(payload): Json<CreateSessionPayload>,
) -> Response {
    let session_id = uuid::Uuid::new_v4().to_string();
    (
        StatusCode::CREATED,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "session_id": session_id,
                "title": payload.title.unwrap_or_else(|| "新对话".to_string()),
                "workspace_id": payload.workspace_id,
                "agent_id": payload.agent_id,
                "created_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn get_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "code": crate::error::simple_code::NOT_FOUND,
            "error": "session_not_found",
            "session_id": session_id,
        })),
    )
        .into_response()
}

pub async fn update_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<UpdateSessionPayload>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "session_id": session_id,
                "title": payload.title,
                "status": payload.status,
                "updated_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn delete_session_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
) -> Response {
    (
        StatusCode::OK,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "session_id": session_id,
                "deleted_at": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

/// The lines of one conversation, oldest first.
///
/// This used to answer `items: []` unconditionally while the chat socket was
/// happily streaming deltas: the transcript existed in `chat_turns` and the one
/// endpoint named "message history" could not see it. A reopened page showed an
/// empty conversation and the platform looked like it had forgotten what it was
/// told. The rows are read here instead of answered from a canned body.
///
/// `failure` rides on the row rather than being folded into `content`: a turn
/// whose answer never arrived is still a turn, and a reader that cannot tell
/// "no answer yet" from "no answer ever" re-asks a question that already failed.
pub async fn list_messages_handler(
    State(state): State<Arc<GatewayState>>,
    claims: Option<axum::Extension<cog_core::Claims>>,
    Path(session_id): Path<String>,
    Query(q): Query<ListMessagesQuery>,
) -> Response {
    // 读的人是谁先定下来，再去看那一段记下的是谁。没有身份就没有「只属于你」
    // 这回事可判：一张空记录在说「这一段没有说过话」，而实话是「不知道你是谁」。
    let Some(caller) = claims
        .as_ref()
        .map(|axum::Extension(c)| c.sub.trim())
        .filter(|sub| !sub.is_empty())
    else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({
                "code": crate::error::simple_code::UNAUTHORIZED,
                "error": "需要先登录才能读一段对话的记录",
            })),
        )
            .into_response();
    };
    let limit = q.limit.unwrap_or(200).clamp(1, 500) as usize;
    let Some(store) = state.chat_turns.as_ref() else {
        // No transcript store in this deployment: an empty list is the truthful
        // answer, and the client still has the live socket.
        return (
            StatusCode::OK,
            Json(json!({
                "code": crate::error::simple_code::OK,
                "data": {
                    "session_id": session_id,
                    "shared": true,
                    "items": [],
                    "next_cursor": null,
                    "has_more": false,
                }
            })),
        )
            .into_response();
    };

    // 这一段属于谁。判不了就拒（503）：把「判不了」画成「没有主人」等于在存储
    // 抖一下的时候把别人的对话交给猜中号的人。
    let access = match store.owner_of(&session_id).await {
        Ok(owner) => access(owner.as_deref(), caller),
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "code": "error",
                    "message": format!("判不了这一段对话属于谁：{e}"),
                })),
            )
                .into_response()
        }
    };
    // 别人的对话只有一句 `session_not_found`：另起一种文案（「存在，但不是你的」）
    // 就是给猜会话号的人一条「这一段有人在写」的读数。一个谁都没写过字的名字不走
    // 这一支——它的属主是空的（公共场所），下面按共享读回一张空记录；新开一段会话时
    // 页面读的就是那条路。
    if access == SessionAccess::SomebodyElses {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "code": crate::error::simple_code::NOT_FOUND,
                "error": "session_not_found",
                "session_id": session_id,
            })),
        )
            .into_response();
    }

    match store.transcript(&session_id, limit).await {
        Ok(turns) => {
            let items: Vec<serde_json::Value> = turns
                .into_iter()
                .map(|turn| {
                    json!({
                        "id": format!("turn-{}", turn.id),
                        "role": turn.role.as_str(),
                        "content": turn.content,
                        "failure": turn.failure,
                        // 交出去的这一轮带着目标身份回来：重新打开这段对话时，
                        // 「这条是能关掉的问答，还是一条还在服务器上跑的目标」
                        // 只能从这一格读——实时流在页面关掉时就断了。
                        "goal_id": turn.goal,
                        // 名字与类型，不带字节：重开页面要能把「这条话说过一张图」
                        // 画出来，而那张图本身不在这一趟返回里——它随记录落库，
                        // 由下一轮装进提示词。把 base64 也发给浏览器，是让每一次
                        // 重开都把整段对话的附件重新下载一遍。
                        "attachments": turn.attachments,
                        // 这一轮调过什么、谁答的。实时帧只活在连接里，页面关掉
                        // 之后「这一段对话的待办清单」与「回答是谁写的」都只剩这一
                        // 格还写着；不带回去，重开就是一段只剩一问一答的对话。
                        "tool_calls": turn.tool_calls,
                        "agent_id": turn.agent,
                        "created_at": turn.created_at,
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({
                    "code": crate::error::simple_code::OK,
                    "data": {
                        "session_id": session_id,
                        // 共享场所单独标出来：这一段没有属主，谁都读得到它，而
                        // 读的人有权知道这一点。
                        "shared": shared_marking(access),
                        "items": items,
                        "next_cursor": null,
                        "has_more": false,
                    }
                })),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "code": "error",
                "message": format!("读不到这段对话的记录：{e}"),
            })),
        )
            .into_response(),
    }
}

pub async fn send_message_handler(
    State(_state): State<Arc<GatewayState>>,
    Path(session_id): Path<String>,
    Json(payload): Json<SendMessagePayload>,
) -> Response {
    let message_id = format!("msg-{}", uuid::Uuid::new_v4());
    let task_id = format!("task-{}", uuid::Uuid::new_v4());
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "message_id": message_id,
                "session_id": session_id,
                "role": "user",
                "content": payload.content,
                "task_id": task_id,
                "timestamp": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

pub async fn regenerate_message_handler(
    State(_state): State<Arc<GatewayState>>,
    Path((session_id, message_id)): Path<(String, String)>,
) -> Response {
    let task_id = format!("task-{}", uuid::Uuid::new_v4());
    (
        StatusCode::ACCEPTED,
        Json(json!({
            "code": crate::error::simple_code::OK,
            "data": {
                "session_id": session_id,
                "source_message_id": message_id,
                "task_id": task_id,
                "timestamp": chrono::Utc::now(),
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::{access, shared_marking, SessionAccess};

    /// 两个身份互查：各自读到自己的，读不到对方的。
    #[test]
    fn two_people_cannot_read_each_others_conversations() {
        assert_eq!(access(Some("alice"), "alice"), SessionAccess::Own);
        assert_eq!(access(Some("alice"), "bob"), SessionAccess::SomebodyElses);
        assert_eq!(access(Some("bob"), "alice"), SessionAccess::SomebodyElses);
    }

    /// 猜一个会话号读不到：记下的那个名字不是读的人，答案与「不存在」同一格。
    #[test]
    fn a_guessed_session_id_reads_as_somebody_elses() {
        assert_eq!(access(Some("mira"), "guess"), SessionAccess::SomebodyElses);
        // 空名字的连接也一样：没有身份的人对一段有主的对话没有话语权。
        assert_eq!(access(Some("mira"), ""), SessionAccess::SomebodyElses);
    }

    /// 没有属主的一段是共享的，而且返回里明说这一点。
    #[test]
    fn a_conversation_nobody_owns_is_shared_and_says_so() {
        assert_eq!(access(None, "alice"), SessionAccess::Shared);
        assert_eq!(access(Some(""), "alice"), SessionAccess::Shared);
        assert_eq!(access(Some("   "), "alice"), SessionAccess::Shared);
        assert!(shared_marking(SessionAccess::Shared));
        assert!(!shared_marking(SessionAccess::Own));
        assert!(!shared_marking(SessionAccess::SomebodyElses));
    }

    /// 名字两边的空白不算另一个名字，也不让一个空名字认领一段有主的话。
    #[test]
    fn whitespace_does_not_make_a_second_identity() {
        assert_eq!(access(Some(" alice "), "alice"), SessionAccess::Own);
        assert_eq!(access(Some("alice"), " alice"), SessionAccess::Own);
        assert_eq!(access(Some("alice"), "   "), SessionAccess::SomebodyElses);
    }
}
