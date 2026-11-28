//! 判据修订的写者与读面：`task` 这一族里 `terminal_revised` 那一步。
//!
//! 修订是一条事实，不是一个开关：写下之后，后面每一圈对着的那段话就是它写的那一段，
//! 而「为什么换」和「换之前是哪一段」都在这一条里。读的人按待办标记取最近一条——
//! 同一个标记换过两次时，对着说话的是最后一次。

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;

use cog_core::contract::terminal_revision::{check_revision, TerminalRevision, TerminalRevisions};
use cog_core::{EventPayload, EventStore, FactEvent, SFError, SFResult};

/// 修订的写入口：落一条 `task`／`terminal_revised` 事实。
pub struct PostgresTerminalRevisions {
    pool: PgPool,
    store: Arc<dyn EventStore>,
}

impl PostgresTerminalRevisions {
    pub fn new(pool: PgPool, store: Arc<dyn EventStore>) -> Self {
        Self { pool, store }
    }
}

#[async_trait::async_trait]
impl TerminalRevisions for PostgresTerminalRevisions {
    async fn record(&self, revision: &TerminalRevision) -> SFResult<()> {
        check_revision(revision).map_err(SFError::Validation)?;
        let mut event = FactEvent::new(EventPayload::TerminalRevised {
            item_id: revision.item_id.clone(),
            previous: revision.previous.clone(),
            terminal: revision.terminal.clone(),
            reason: revision.reason.clone(),
            actor: revision.actor.clone(),
        });
        event.at = Some(revision.at);
        self.store.append(event).await
    }

    async fn latest_for(&self, item_ids: &[String]) -> SFResult<Vec<TerminalRevision>> {
        terminal_revisions_for(&self.pool, item_ids).await
    }
}

/// 这几条待办各自最近一次的修订；没有修订的条目不出现。
///
/// 空名单是一条不发出去的查询而不是「全表」：调用方问的是「这几条换过没有」，
/// 把它读成「所有待办的修订」会让一次没有待办的轮次拿到别人的判据。
pub async fn terminal_revisions_for(
    pool: &PgPool,
    item_ids: &[String],
) -> SFResult<Vec<TerminalRevision>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, DateTime<Utc>, String)> = sqlx::query_as(
        "SELECT subject_id, at, attributes FROM cogneva_event \
          WHERE kind = 'task' AND step = 'terminal_revised' AND subject_id = ANY($1) \
          ORDER BY seq",
    )
    .bind(item_ids)
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read terminal revisions: {e}")))?;
    let mut latest: std::collections::BTreeMap<String, TerminalRevision> =
        std::collections::BTreeMap::new();
    for (item_id, at, attributes) in rows {
        let value: serde_json::Value = serde_json::from_str(&attributes).map_err(|e| {
            SFError::Database(format!("terminal revision of {item_id} is not JSON: {e}"))
        })?;
        let text = |key: &str| {
            value
                .get(key)
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .unwrap_or_default()
        };
        latest.insert(
            item_id.clone(),
            TerminalRevision {
                item_id,
                previous: text("previous"),
                terminal: text("terminal"),
                reason: text("reason"),
                actor: text("actor"),
                at,
            },
        );
    }
    Ok(latest.into_values().collect())
}
