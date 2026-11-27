//! 用量行与调用事实的同事务落点。
//!
//! 两样东西在同一个库、同一张事件流旁边；把它们放在一个事务里，是为了让「行进了、
//! 事件没进」这个中间态不存在。网关只拿到 `dyn UsageLanding`，看不到两张表。

use std::sync::Arc;

use cog_core::{EventPayload, LlmUsageRecord, UsageLanding};
use sqlx::PgPool;

/// 一个写者：一个库、一个部署名。
pub struct PostgresUsageLanding {
    pool: PgPool,
    writer: String,
}

impl PostgresUsageLanding {
    pub fn new(pool: PgPool, writer: impl Into<String>) -> Self {
        Self {
            pool,
            writer: writer.into(),
        }
    }

    /// 事件 id：由调用身份派生，重放同一笔写同一个主键。
    fn event_id(call_id: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(format!("llm_call:{call_id}").as_bytes());
        let hex: String = digest.iter().map(|b| format!("{b:02X}")).collect();
        hex.chars().take(26).collect()
    }
}

#[async_trait::async_trait]
impl UsageLanding for PostgresUsageLanding {
    async fn land(&self, record: &LlmUsageRecord) -> Result<(), String> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| format!("begin usage landing: {e}"))?;
        // 停写行之后只剩事件这一份：读口已经全部从流上取数，行是纯重复。
        // 由部署声明（`COGNEVA_LLM_USAGE_ROWS=stop`）逐级切换；未声明时仍写。
        let row_id = if usage_rows_stopped() {
            uuid::Uuid::new_v4().to_string()
        } else {
            cog_observability::usage_store::LlmUsageStore::record_on(&mut tx, record)
                .await
                .map_err(|e| format!("usage row: {e}"))?
        };
        let call = record.as_call(&row_id);
        let payload = EventPayload::LlmCall(call.clone());
        let event_id = Self::event_id(&call.call_id);
        let fields = payload.envelope(&event_id, &self.writer);
        let at = call.at;
        cog_storage::postgres::event_log::append_on(&mut tx, &fields, at)
            .await
            .map_err(|e| format!("usage event: {e}"))?;
        tx.commit()
            .await
            .map_err(|e| format!("commit usage landing: {e}"))
    }
}

/// 部署是否已停止写用量行：`COGNEVA_LLM_USAGE_ROWS=stop` 是唯一的词，别的值都按
/// 「还写」读——一个读不懂的值不该悄悄停掉一份记录。
pub fn usage_rows_stopped() -> bool {
    rows_stopped_from(std::env::var("COGNEVA_LLM_USAGE_ROWS").ok().as_deref())
}

/// 判定与读环境分开：一个读不懂的值不该悄悄停掉一份记录，这条要能不碰环境地测。
pub fn rows_stopped_from(raw: Option<&str>) -> bool {
    raw.map(|value| value.trim().eq_ignore_ascii_case("stop"))
        .unwrap_or(false)
}

/// 装配入口：把同一个池交给这个落点。
pub fn postgres_usage_landing(pool: PgPool, writer: impl Into<String>) -> Arc<dyn UsageLanding> {
    Arc::new(PostgresUsageLanding::new(pool, writer))
}

#[cfg(test)]
mod tests {
    use super::rows_stopped_from;

    #[test]
    fn only_the_word_stop_stops_the_rows() {
        assert!(rows_stopped_from(Some("stop")));
        assert!(rows_stopped_from(Some(" STOP ")));
        assert!(!rows_stopped_from(None));
        assert!(!rows_stopped_from(Some("write")));
        assert!(!rows_stopped_from(Some("")));
    }
}
