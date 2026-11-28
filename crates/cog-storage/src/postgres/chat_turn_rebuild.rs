//! 从流重建一段会话的 `chat_turns`。
//!
//! 轮的根是意图事件：入口先把属主那一句落成 `intent_received`，同一轮随后的
//! 附件、工具调用、回答、失败都把信封的 `causation_id` 指向那条意图事件。于是
//! 「哪几格属于同一轮」不是一个要额外记的字段，读因果链就是分组。
//!
//! 两个端口由装配层给：对象存储按 key 取附件字节，`artifact://` 按引用取工具
//! 正文。重建命令不知道这两样存在哪，只按接口取。

use base64::Engine as _;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::{
    ArchivedBodyReader, AttachmentBytes, ChatAttachment, ChatToolCall, ChatToolCallStatus,
    ChatTurnStore, SFError, SFResult,
};

use super::chat_turn_store::PostgresChatTurnStore;

/// 一轮在流上的四类格子。
struct TurnFacts {
    /// 这一轮折到流上的哪个位置：意图与它名下每条事实里最深的那个 `seq`。
    max_seq: i64,
    /// 这是哪一条意图事件（也是这一轮的因果根）。
    intent_event_id: String,
    /// 意图身份（planner 派生任务时把它写进 `intent_id`）。
    intent_identity: String,
    at: DateTime<Utc>,
    owner: String,
    addressed_to: String,
    text: String,
    model_text: Option<String>,
    attachments: Vec<ChatAttachment>,
    answer: Option<String>,
    agent: Option<String>,
    failure: Option<String>,
    tool_calls: Vec<ChatToolCall>,
}

/// 按会话重建。返回写回的轮数（一次问答算一轮）。
///
/// 幂等性交给调用方：这条命令**追加**行。用一个空的会话名或新的 session 跑，
/// 不要在已有行上重跑——表里还没有按轮去重的列，重跑就是把同一段再写一遍。
pub async fn rebuild_chat_turns(
    pool: &PgPool,
    session: &str,
    attachments: &dyn AttachmentBytes,
    artifacts: &dyn ArchivedBodyReader,
) -> Result<u64, SFError> {
    let intents = sqlx::query(
        "SELECT event_id, subject_id, seq, attributes::jsonb, at FROM cogneva_event \
          WHERE kind = 'intent_received' AND session_id = $1 \
          ORDER BY seq",
    )
    .bind(session)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the intents of {session}: {e}")))?;
    if intents.is_empty() {
        return Ok(0);
    }

    // 折到哪儿了：按会话记。没有水位的旧会话（这一格是后加的）先整段重建一次——
    // 增量在旧行上说不清哪些轮已经折过，猜一次就会写重复的轮。
    let watermark: Option<i64> =
        sqlx::query_scalar("SELECT folded_seq FROM chat_turn_watermarks WHERE session_id = $1")
            .bind(session)
            .fetch_optional(pool)
            .await
            .map_err(|e| SFError::Database(format!("read the watermark of {session}: {e}")))?;
    let existing: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM chat_turns WHERE session_id = $1")
            .bind(session)
            .fetch_one(pool)
            .await
            .map_err(|e| SFError::Database(format!("count the turns of {session}: {e}")))?;
    let full_rebuild = watermark.is_none() && existing > 0;
    if full_rebuild {
        sqlx::query("DELETE FROM chat_turns WHERE session_id = $1")
            .bind(session)
            .execute(pool)
            .await
            .map_err(|e| SFError::Database(format!("clear the old turns of {session}: {e}")))?;
    }
    let floor = watermark.unwrap_or(0);

    let roots: Vec<String> = intents
        .iter()
        .map(|row| row.get::<String, _>("event_id"))
        .collect();
    let facts = sqlx::query(
        "SELECT kind, causation_id, seq, attributes::jsonb, at FROM cogneva_event \
          WHERE causation_id = ANY($1) AND kind IN ('message_sent', 'tool_call') \
          ORDER BY seq",
    )
    .bind(&roots)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the facts of {session}: {e}")))?;

    // 每个因果根上挂着的那几格。
    let mut turns: std::collections::HashMap<String, TurnFacts> = intents
        .iter()
        .map(|row| {
            let attributes: serde_json::Value = row.get("attributes");
            let text = |key: &str| {
                attributes
                    .get(key)
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            };
            let id = row.get::<String, _>("event_id");
            (
                id.clone(),
                TurnFacts {
                    max_seq: row.get::<i64, _>("seq"),
                    intent_event_id: id,
                    intent_identity: row.get::<String, _>("subject_id"),
                    at: row.get("at"),
                    owner: text("said_by"),
                    addressed_to: text("addressed_to"),
                    text: text("text"),
                    model_text: attributes
                        .get("model_text")
                        .and_then(|v| v.as_str())
                        .map(str::to_string),
                    attachments: Vec::new(),
                    answer: None,
                    agent: None,
                    failure: None,
                    tool_calls: Vec::new(),
                },
            )
        })
        .collect();

    for row in facts {
        let Some(root) = row.get::<Option<String>, _>("causation_id") else {
            continue;
        };
        let Some(turn) = turns.get_mut(&root) else {
            continue;
        };
        let kind: String = row.get("kind");
        turn.max_seq = turn.max_seq.max(row.get::<i64, _>("seq"));
        let attributes: serde_json::Value = row.get("attributes");
        let text = |key: &str| {
            attributes
                .get(key)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        // 正文外置时 `text` 为空、`artifact` 指向存放处：重建按引用回读，
        // 读不回来就留空并把原因写进日志——事件仍然点着那一份，投影不替它编。
        let mut body = text("text");
        if body.is_empty() {
            if let Some(uri) = attributes.get("artifact").and_then(|v| v.as_str()) {
                match artifacts.read(uri).await {
                    Ok(bytes) => body = bytes,
                    Err(e) => {
                        tracing::warn!(uri = %uri, error = %e, "message body could not be read back from its artifact");
                    }
                }
            }
        }
        match kind.as_str() {
            "message_sent" => match attributes
                .get("kind")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
            {
                "answer" => {
                    turn.answer = Some(body.clone());
                    turn.agent = Some(text("from"));
                }
                "failure" => turn.failure = Some(body.clone()),
                "attachment" => {
                    // 指针那一格是 JSON：key、名字、类型。字节按 key 从不对象存储取。
                    let Ok(pointer) = serde_json::from_str::<serde_json::Value>(&text("text"))
                    else {
                        continue;
                    };
                    let key = pointer
                        .get("key")
                        .and_then(|v| v.as_str())
                        .unwrap_or(Default::default());
                    if key.is_empty() {
                        continue;
                    }
                    let bytes = attachments.get(key).await?;
                    turn.attachments.push(ChatAttachment {
                        name: pointer
                            .get("name")
                            .and_then(|v| v.as_str())
                            .map(str::to_string),
                        mime_type: pointer
                            .get("mime_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                        data: base64::engine::general_purpose::STANDARD.encode(bytes),
                    });
                }
                _ => {}
            },
            "tool_call" => {
                let uri = text("artifact");
                if uri.is_empty() {
                    continue;
                }
                let body = artifacts.read(&uri).await?;
                let record: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
                    SFError::Database(format!("tool call body {uri} is not JSON: {e}"))
                })?;
                let field = |key: &str| {
                    record
                        .get(key)
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string()
                };
                let is_error = record
                    .get("is_error")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let at: DateTime<Utc> = record
                    .get("at")
                    .and_then(|v| v.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                    .map(|t| t.with_timezone(&Utc))
                    .unwrap_or_else(|| row.get("at"));
                turn.tool_calls.push(ChatToolCall {
                    tool_call_id: field("tool_call_id"),
                    tool_name: field("tool"),
                    args: record
                        .get("arguments")
                        .map(|v| v.to_string())
                        .unwrap_or_default(),
                    result: record.get("result").map(|v| v.to_string()),
                    status: if is_error {
                        ChatToolCallStatus::Error
                    } else {
                        ChatToolCallStatus::Ok
                    },
                    started_at: at,
                    finished_at: Some(at),
                });
            }
            _ => {}
        }
    }

    // 这一轮派出的目标：任务行上 `intent_id` 指回意图身份。
    let identities: Vec<String> = turns
        .values()
        .map(|turn| turn.intent_identity.clone())
        .collect();
    let goals = sqlx::query(
        "SELECT DISTINCT ON (intent_id) intent_id, goal_id FROM task_defined \
          WHERE intent_id = ANY($1) ORDER BY intent_id, rev DESC",
    )
    .bind(&identities)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the goals of {session}: {e}")))?;
    let mut goal_of: std::collections::HashMap<String, String> = Default::default();
    for row in goals {
        let identity: Option<String> = row.get("intent_id");
        let goal: Option<String> = row.get("goal_id");
        if let (Some(identity), Some(goal)) = (identity, goal) {
            goal_of.insert(identity, goal);
        }
    }

    let store = PostgresChatTurnStore::new(pool.clone());
    // 下一次的水位：每一轮的最深一条事实的最大值。没答的那几轮也在其中——它们写下
    // 来的意图已经折过，等回答到了（seq 更大）那一轮会被重新折。
    let next_watermark = turns
        .values()
        .map(|turn| turn.max_seq)
        .max()
        .unwrap_or(floor)
        .max(floor);
    let mut ordered: Vec<TurnFacts> = turns
        .into_values()
        .filter(|turn| full_rebuild || turn.max_seq > floor)
        .collect();
    ordered.sort_by_key(|turn| turn.at);
    let mut written = 0u64;
    for turn in ordered {
        let agent = turn.agent.as_deref().filter(|name| !name.is_empty());
        let tool_calls = turn.tool_calls.as_slice();
        match (&turn.failure, &turn.answer) {
            (Some(failure), _) => {
                store
                    .record_failed_turn(
                        session,
                        &turn.owner,
                        &turn.text,
                        &turn.attachments,
                        failure,
                        tool_calls,
                        agent,
                    )
                    .await?;
            }
            (None, Some(answer)) => match goal_of.get(&turn.intent_identity) {
                Some(goal) => {
                    store
                        .record_dispatched_turn(
                            session,
                            &turn.owner,
                            &turn.text,
                            turn.model_text.as_deref(),
                            &turn.attachments,
                            answer,
                            goal,
                            tool_calls,
                            agent,
                        )
                        .await?;
                }
                None => {
                    store
                        .record_turn(
                            session,
                            &turn.owner,
                            &turn.text,
                            turn.model_text.as_deref(),
                            &turn.attachments,
                            answer,
                            tool_calls,
                            agent,
                        )
                        .await?;
                }
            },
            (None, None) => {
                // 这一轮还没答（进程死在回答之前）：不写一行，重跑重建会把它补上。
                continue;
            }
        }
        written += 1;
    }
    sqlx::query(
        "INSERT INTO chat_turn_watermarks (session_id, folded_seq) VALUES ($1, $2) \
         ON CONFLICT (session_id) DO UPDATE SET \
             folded_seq = GREATEST(chat_turn_watermarks.folded_seq, EXCLUDED.folded_seq), \
             folded_at = NOW()",
    )
    .bind(session)
    .bind(next_watermark)
    .execute(pool)
    .await
    .map_err(|e| SFError::Database(format!("write the watermark of {session}: {e}")))?;
    Ok(written)
}
