//! 「这段正文被谁在什么时候因为什么擦掉了」——从流上读擦除记录。
//!
//! 擦除只做两件事：销毁正文本身，再追加一条 `redacted`。历史记录不被改写，所以
//! 这个问题永远答得出来；正文本身再也取不回来，那是另一件事。

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::SFError;

/// 一条擦除记录。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionRecord {
    pub event_id: String,
    pub at: DateTime<Utc>,
    pub actor: String,
    pub body_oid: u32,
    pub sha256: String,
    pub bytes: u64,
    pub reason: String,
}

/// 按正文哈希读擦除记录，按发生次序。
pub async fn redactions_for(pool: &PgPool, sha256: &str) -> Result<Vec<RedactionRecord>, SFError> {
    let rows = sqlx::query(
        "SELECT event_id, at, actor, attributes::jsonb AS attributes \
           FROM cogneva_event \
          WHERE kind = 'redacted' AND attributes::jsonb ->> 'sha256' = $1 \
          ORDER BY seq",
    )
    .bind(sha256)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read redactions of {sha256}: {e}")))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let attributes: serde_json::Value = row.get("attributes");
            let text = |key: &str| {
                attributes
                    .get(key)
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string()
            };
            RedactionRecord {
                event_id: row.get("event_id"),
                at: row.get("at"),
                actor: row.get("actor"),
                body_oid: attributes
                    .get("body_oid")
                    .and_then(|value| value.as_u64())
                    .unwrap_or_default() as u32,
                sha256: text("sha256"),
                bytes: attributes
                    .get("bytes")
                    .and_then(|value| value.as_u64())
                    .unwrap_or_default(),
                reason: text("reason"),
            }
        })
        .collect())
}
