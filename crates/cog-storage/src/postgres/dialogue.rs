//! 一段会话在流上的样子：谁说的、什么时刻、正文。
//!
//! 会话是投影：属主那一句在 `intent_received` 上（到达即落），助手的回答在
//! `message_sent(kind=answer)` 上；两条都带 `session`。这张读法只读事实，不读会话表
//! ——表坏了会话仍拼得回来，反过来不行。

use chrono::{DateTime, Utc};
use sqlx::PgPool;

use cog_core::SFError;

/// 会话里的一句话。
#[derive(Debug, Clone, PartialEq)]
pub struct DialogueLine {
    /// 什么时候说的。
    pub at: DateTime<Utc>,
    /// 谁说的（属主的登录身份，或员工 id）。
    pub said_by: String,
    /// 原话。
    pub text: String,
}

/// 按会话读出一段对话，按发生时刻排。
///
/// 事件的 `attributes` 里找不到 `session` 的行不属于任何会话，不返回：它们不是
/// 「这段对话的一部分」，是别的入口（外部情报、A2A 的任务提交）。
pub async fn dialogue_for_session(
    pool: &PgPool,
    session: &str,
) -> Result<Vec<DialogueLine>, SFError> {
    let rows: Vec<(DateTime<Utc>, serde_json::Value)> = sqlx::query_as(
        "SELECT at, attributes::jsonb FROM cogneva_event \
          WHERE kind IN ('intent_received', 'message_sent') \
            AND session_id = $1 \
          ORDER BY seq",
    )
    .bind(session)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the dialogue of {session}: {e}")))?;
    Ok(rows
        .into_iter()
        .filter_map(|(at, attributes)| {
            let text = attributes.get("text")?.as_str()?.to_string();
            let said_by = attributes
                .get("said_by")
                .or_else(|| attributes.get("from"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            Some(DialogueLine { at, said_by, text })
        })
        .collect())
}
