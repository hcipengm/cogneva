//! 续跑日志（[`cog_core::attempt_journal`]）的 PostgreSQL 落点。
//!
//! 表是窄的：一次尝试的对话就是若干行 `(agent_id, seq) -> message`，没有索引之外
//! 的结构，也没有 json/jsonb 列——消息原样以文本存，读回来按 `serde_json` 还原。
//!
//! 键是**agent**，不是任务：`agent_id` 由任务与角色算出来（`agent_id_for`），
//! 同一件任务再派一次还是同一个名字，续跑才找得回上一次那一段。列名就叫
//! `agent_id`——叫 `task_id` 会让下一个人以为这里存的是任务 id，而它从来不是。
//!
//! 不设保留期清扫在这里：**已关闭**的尝试永远不再被读，它们唯一的读者是事后
//! 查一件事的人。清扫按「关闭时间早于保留窗口」删，见 [`SWEEP_SQL`]。
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;

use cog_core::attempt_journal::{AttemptJournal, JournalEntry, JournalKind};
use cog_core::{SFError, SFResult};

/// 一行日志。`message` 是 `Message` 的 JSON，原样。
pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cog_attempt_journal (
    agent_id     TEXT NOT NULL,
    seq          BIGINT NOT NULL,
    kind         TEXT NOT NULL,
    message      TEXT,
    tool_call_id TEXT,
    tool         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (agent_id, seq)
);
-- 扫「哪些 agent 还开着」只读最后一条，走同一个索引不必另建。
"#;

/// 一个 agent 追加一行，`seq` 在一条语句里算出来。
///
/// 两个写者同时追加同一个 agent 会拿到同一个 `seq`，于是主键冲突——那是正确的
/// 结局：拼接顺序由发生顺序决定，两个写者本来就不该在同一个 agent 上并发写。
/// 调用方重试一次即可，而重试读到的 `MAX(seq)` 已经把它算进去。
const APPEND_SQL: &str = r#"
INSERT INTO cog_attempt_journal (agent_id, seq, kind, message, tool_call_id, tool)
SELECT $1,
       COALESCE((SELECT MAX(seq) FROM cog_attempt_journal WHERE agent_id = $1), 0) + 1,
       $2, $3, $4, $5
"#;

const LOAD_SQL: &str = r#"
SELECT seq, kind, message, tool_call_id, tool, created_at
FROM cog_attempt_journal
WHERE agent_id = $1
ORDER BY seq ASC
"#;

/// 已关闭且关闭时间早于 `now - keep` 的整个 agent，连同日志一起删。
///
/// 判据落在**关闭那一刻**，不是最后一条消息那一刻：一个 agent 开着的时候，
/// 无论它多久没动，它都有可能被下一次派发接着跑。
pub const SWEEP_SQL: &str = r#"
DELETE FROM cog_attempt_journal
WHERE agent_id IN (
    SELECT agent_id FROM cog_attempt_journal
    WHERE kind = 'closed' AND created_at < NOW() - make_interval(secs => $1)
      AND seq = (SELECT MAX(seq) FROM cog_attempt_journal j
                 WHERE j.agent_id = cog_attempt_journal.agent_id)
)
"#;

/// 已关闭的日志留多久。
///
/// 已关闭的尝试永远不会再被读回来——续跑只读还开着的那一段——所以这个窗口
/// 量的是「事后还想翻多久」，不是「还能不能用」。七天：一次运行的细节在
/// `cog_events` 的事件流里有一份更完整的，这里留着的是同一段对话的另一面，
/// 隔一周还在追问它的情形极少，而每一天它都按调用量长一份。
// cap: bound — 只清已关闭的；开着的无论多久都不动。七天的理由写在正文，不是量出来的
pub const DEFAULT_KEEP_DAYS: i64 = 7;

/// 承载保留期的环境变量，与同族的两条（事件流、用量账）同一个形状。
pub const KEEP_DAYS_ENV: &str = "COGNEVA_ATTEMPT_JOURNAL_KEEP_DAYS";

/// 从环境读保留期；没设或读不成就是默认值。
pub fn keep_days_from_env() -> i64 {
    std::env::var(KEEP_DAYS_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|d| *d >= 1)
        .unwrap_or(DEFAULT_KEEP_DAYS)
}

pub struct PostgresAttemptJournal {
    pool: PgPool,
}

impl PostgresAttemptJournal {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn init_schema(&self) -> SFResult<()> {
        crate::schema_ddl::apply(&self.pool, DDL).await
    }

    /// 删掉已关闭且过了保留窗口的整个 agent，返回删掉的行数。
    pub async fn sweep_closed_older_than(&self, keep_seconds: i64) -> SFResult<u64> {
        let done = sqlx::query(SWEEP_SQL)
            .bind(keep_seconds as f64)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("attempt journal sweep failed: {e}")))?;
        Ok(done.rows_affected())
    }
}

/// 每两分钟一趟清扫，与它旁边那两条保留期循环同一个节奏。
///
/// 「还没关闭的永远不动」这条判据在 SQL 里，不在这个循环里：一个循环只要
/// 按时醒来就够，它不需要知道哪一条能删。
// cap: bound — 同族的两条保留期清扫都是两分钟；它量的「年龄」以天计
pub const SWEEP_PERIOD: std::time::Duration = std::time::Duration::from_secs(120);

/// 起一趟清扫。启动时先扫一遍——停机期间过期的那些要有人收。
pub fn spawn_sweeper(
    journal: Arc<PostgresAttemptJournal>,
    keep_seconds: i64,
    shutdown: cog_core::ShutdownSignal,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match journal.sweep_closed_older_than(keep_seconds).await {
                Ok(0) => {}
                Ok(rows) => tracing::info!(
                    rows,
                    "attempt journal: retired closed attempts past the retention"
                ),
                Err(e) => {
                    tracing::warn!(error = %e, "attempt journal sweep failed; the next pass retries")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(SWEEP_PERIOD) => {}
                _ = shutdown.wait() => break,
            }
        }
    })
}

#[async_trait]
impl AttemptJournal for PostgresAttemptJournal {
    async fn append(&self, agent_id: &str, entry: &JournalEntry) -> Result<(), String> {
        let message = match &entry.message {
            Some(m) => Some(
                serde_json::to_string(m)
                    .map_err(|e| format!("attempt journal: a message did not serialize: {e}"))?,
            ),
            None => None,
        };
        let result = sqlx::query(APPEND_SQL)
            .bind(agent_id)
            .bind(entry.kind.as_str())
            .bind(message)
            .bind(entry.tool_call_id.as_deref())
            .bind(entry.tool.as_deref())
            .execute(&self.pool)
            .await;
        match result {
            Ok(_) => Ok(()),
            // 主键冲突：另一个写者刚占了同一个 seq。重读一次再追加。
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => sqlx::query(APPEND_SQL)
                .bind(agent_id)
                .bind(entry.kind.as_str())
                .bind(
                    entry
                        .message
                        .as_ref()
                        .and_then(|m| serde_json::to_string(m).ok()),
                )
                .bind(entry.tool_call_id.as_deref())
                .bind(entry.tool.as_deref())
                .execute(&self.pool)
                .await
                .map(|_| ())
                .map_err(|e| format!("attempt journal append failed twice: {e}")),
            Err(e) => Err(format!("attempt journal append failed: {e}")),
        }
    }

    async fn load(&self, agent_id: &str) -> Result<Vec<JournalEntry>, String> {
        let rows: Vec<(
            i64,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
            DateTime<Utc>,
        )> = sqlx::query_as(LOAD_SQL)
            .bind(agent_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| format!("attempt journal load failed: {e}"))?;

        let mut out = Vec::with_capacity(rows.len());
        for (seq, kind, message, tool_call_id, tool, created_at) in rows {
            let kind = JournalKind::parse(&kind)
                .ok_or_else(|| format!("attempt journal holds an unknown row kind: {kind}"))?;
            let message = match message {
                Some(text) => Some(serde_json::from_str::<cog_core::Message>(&text).map_err(
                    |e| format!("attempt journal row {seq} does not parse as a message: {e}"),
                )?),
                None => None,
            };
            out.push(JournalEntry {
                seq: seq as u64,
                kind,
                message,
                tool_call_id,
                tool,
                created_at: created_at.into(),
            });
        }
        Ok(out)
    }
}
