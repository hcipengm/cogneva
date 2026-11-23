//! 意图进流程那一刻的那一条事件：`intent_received`。
//!
//! 二十六处入口（对话、外部仓库、外部世界、系统自发现、账本自己的结构来源）交上来
//! 的东西，先落成**同一种行**：来源、外部身份、原文、因果父行。判官随后按机械判据
//! 把它分成回答／任务／变更／原则——分类不在入口做，入口只翻译。
//!
//! 落在事件流上而不是另起一张表：事件流已经是唯一正本（每一件事先落一条事件），
//! 再建一张「意图表」就是给同一件事开第二份记录。`writer` 与幂等键沿用同一个信封，
//! 于是「这条意图收到过没有」由库自己回答，不由调用方记。

use async_trait::async_trait;
use chrono::Utc;
use sqlx::PgPool;

use cog_core::{SFError, SFResult};

/// 意图这一族的 kind。闭集里的一个词，写死在这一句里。
pub const INTENT_RECEIVED: &str = "intent_received";

/// 一条进门时的意图行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntentRow {
    /// 谁把它带进来的：`webui`、`cli`、`github`、`outward`、`self_signal`、`ledger`…
    pub source: String,
    /// 来源那边的身份（会话 id、issue 号、URL、任务 id）。同一个来源里唯一。
    pub external_id: String,
    /// 原文。**不摘要、不改写**：判官要判的就是这段话本身。
    pub text: String,
    /// 因果父行：这一条是被哪一条引出来的（上一轮、上一条事件）。空表示没有父。
    pub parent: String,
}

impl IntentRow {
    /// 这条意图的行身份：来源＋外部身份。幂等键由它派生，重投不会写第二行。
    pub fn identity(&self) -> String {
        format!("{}:{}", self.source, self.external_id)
    }
}

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

    /// 记下一条意图。重投同一个 (writer, identity) 不写第二行——意图的「收到过」
    /// 由库回答，不由调用方记。
    pub async fn record(&self, row: &IntentRow, actor: &str) -> SFResult<()> {
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
            .map_err(|e| SFError::Database(format!("intent_received: {e}")))?;
        Ok(())
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
