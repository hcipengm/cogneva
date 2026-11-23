//! 意图进流程那一刻的那一条事件：`intent_received`。
//!
//! 二十六处入口（对话、外部仓库、外部世界、系统自发现、账本自己的结构来源）交上来
//! 的东西，先落成**同一种行**：来源、外部身份、原文、因果父行。判官随后按机械判据把
//! 它分成回答／任务／变更／原则——分类不在入口做，入口只翻译。
//!
//! 落在事件流上而不是另起一张表：事件流已经是唯一正本（每一件事先落一条事件），
//! 再建一张「意图表」就是给同一件事开第二份记录。`writer` 与幂等键沿用同一个信封，
//! 于是「这条意图收到过没有」由库自己回答，不由调用方记。

use async_trait::async_trait;
use chrono::Utc;
use sqlx::{PgPool, Row};

use cog_core::contract::intent_events::{
    IntentEvents, IntentReader, IntentRow, INTENT_RECEIVED,
};
use cog_core::{SFError, SFResult};

const INSERT_SQL: &str = "\
INSERT INTO cogneva_event (\
    event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
    idempotency_key, causation_id, batch_id, actor, writer, scope, payload_version, \
    attributes, at) \
VALUES ($1, 'intent_received', 'intent', $2, $2, 'receive', 'ok', 1, $3, NULL, NULL, $4, $5, \
        'intent', 1, $6, $7) \
ON CONFLICT (writer, idempotency_key) DO NOTHING";

/// 意图的写入口。只写这一种 kind，其余列照信封写死。
pub struct IntentEventStore {
    pool: PgPool,
    writer: String,
}

impl IntentEventStore {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        Self {
            pool,
            writer: writer.into(),
        }
    }

    /// 还没有被派生成任务的意图：流里收到过 `intent_received`，而任务行里没有一条带着
    /// 它的身份。读不到就是读不到（`Err`），不返回空表——空表与「全都处理完了」同形。
    pub async fn undispatched_intents(&self, limit: i64) -> SFResult<Vec<IntentRow>> {
        // 两处查询面都是**列**，不是载荷里的键：
        // - `cogneva_event` 的四格由迁移 127 升成生成列（`intent_source` 等），
        //   与 124 对 `llm_call` 那几格是同一个做法；
        // - 「有没有一条任务带着这条身份」读的是 `task_defined.intent_id`，同样是
        //   迁移 127 升出来的生成列，带条件索引。
        // 查询里不 cast 载荷（那条门 `no_production_query_reads_into_the_payload`
        // 盯着），也不碰 `cog_dag_tasks`——那是台账改版前的 JSONB 投影，真库里早就不存在。
        //
        // 2026-11-24 实测：改版前的写法在真库上两条都不成立（`text ->> unknown`、
        // 表不存在），而树上没有一条用例对着真库问过它。
        //
        // 已派生的身份集合算**一次**（`NOT IN`），不是每条事件各扫一遍定义表；
        // 集合里过滤掉 NULL——`NOT IN` 碰到一个 NULL 会把整张收件台读成空的。
        let rows = sqlx::query(
            "SELECT e.intent_source AS source, e.intent_external_id AS external_id, \
                    e.intent_text AS text, coalesce(e.intent_parent, '') AS parent \
               FROM cogneva_event e \
              WHERE e.kind = 'intent_received' \
                AND e.subject_id NOT IN ( \
                    SELECT d.intent_id FROM task_defined d \
                     WHERE d.intent_id IS NOT NULL) \
              ORDER BY e.at ASC, e.event_id ASC \
              LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("undispatched intents: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|row| IntentRow {
                source: row
                    .try_get::<Option<String>, _>("source")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                external_id: row
                    .try_get::<Option<String>, _>("external_id")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                text: row
                    .try_get::<Option<String>, _>("text")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
                parent: row
                    .try_get::<Option<String>, _>("parent")
                    .ok()
                    .flatten()
                    .unwrap_or_default(),
            })
            .collect())
    }
}

#[async_trait]
impl IntentEvents for IntentEventStore {
    /// 记下一条意图。重投同一个 (writer, identity) 不写第二行——意图的「收到过」
    /// 由库回答，不由调用方记。
    async fn record(&self, row: &IntentRow, actor: &str) -> SFResult<()> {
        let identity = row.identity();
        let idempotency_key = format!("{identity}:received");
        let event_id = format!("intent-{}", uuid_like(&identity));
        let attributes = serde_json::json!({
            "source": row.source,
            "external_id": row.external_id,
            "text": row.text,
            "parent": row.parent,
        })
        .to_string();
        sqlx::query(INSERT_SQL)
            .bind(&event_id)
            .bind(&identity)
            .bind(&idempotency_key)
            .bind(actor)
            .bind(&self.writer)
            .bind(&attributes)
            .bind(Utc::now())
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(format!("{INTENT_RECEIVED}: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl IntentReader for IntentEventStore {
    async fn undispatched(&self, limit: i64) -> SFResult<Vec<IntentRow>> {
        self.undispatched_intents(limit).await
    }
}

/// 由行身份派生一个稳定的十六进制后缀：同一句意图重投时事件 id 不变，
/// 两个不同的意图几乎不可能撞上（FNV-1a，够用且不引依赖）。
fn uuid_like(identity: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in identity.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 同一条意图的行身份只取决于来源与外部身份：原文改了不换行，重投不写第二行。
    #[test]
    fn the_row_identity_is_source_and_external_id() {
        let a = IntentRow {
            source: "webui".into(),
            external_id: "session-1".into(),
            text: "先看日志".into(),
            parent: String::new(),
        };
        let mut b = a.clone();
        b.text = "换成看指标".into();
        assert_eq!(a.identity(), b.identity());
        assert_ne!(uuid_like(&a.identity()), uuid_like("webui:session-2"));
    }
}
