//! 落地账的读面：每条变更最后一条 `change_landed` 事实。
//!
//! 落地账原来只有磁盘上每个变更一份 JSON。那一份是缓存：从这条流上折得出同一份状态，
//! 而文件丢了、机器换了之后，读的人仍然答得出「这条变更落到哪、CI 怎么看、为什么退」。

use sqlx::PgPool;

use cog_core::{SFError, SFResult};

/// 一条变更此刻的落地状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LandingStateRow {
    pub change_id: String,
    pub base: String,
    pub landed_rev: String,
    pub state: String,
    pub retired_reason: Option<String>,
}

/// 每条变更最后一条落地事实，按变更 id 排。
pub async fn landing_states(pool: &PgPool) -> SFResult<Vec<LandingStateRow>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT subject_id, attributes FROM cogneva_event \
          WHERE kind = 'change_landed' ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read landing states: {e}")))?;
    let mut latest: std::collections::BTreeMap<String, LandingStateRow> =
        std::collections::BTreeMap::new();
    for (change_id, attributes) in rows {
        let value: serde_json::Value = serde_json::from_str(&attributes).map_err(|e| {
            SFError::Database(format!("landing fact of {change_id} is not JSON: {e}"))
        })?;
        let text = |key: &str| {
            value
                .get(key)
                .and_then(|found| found.as_str())
                .map(str::to_string)
        };
        let closed = value
            .get("closed")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if closed {
            // 已经离开通道的一条：读面与盘上那份文件被删时同形。
            latest.remove(&change_id);
            continue;
        }
        latest.insert(
            change_id.clone(),
            LandingStateRow {
                change_id,
                base: text("base").unwrap_or_default(),
                landed_rev: text("landed_rev").unwrap_or_default(),
                state: text("state").unwrap_or_default(),
                retired_reason: text("retired_reason"),
            },
        );
    }
    Ok(latest.into_values().collect())
}

/// 落地事实的读口：每条变更最后一条 `change_landed` 的属性 JSON，按 seq 升序。
///
/// 与 [`landing_states`] 同一个查询的两种投影：那一个是给人看的五行，
/// 这一个把整条属性原样交给落地通道，由它重建 `LandingRecord`。
pub async fn latest_landing_attributes(pool: &PgPool) -> SFResult<Vec<String>> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT subject_id, attributes FROM cogneva_event \
          WHERE kind = 'change_landed' ORDER BY seq",
    )
    .fetch_all(pool)
    .await
    .map_err(|e| SFError::Database(format!("read landing facts: {e}")))?;
    let mut latest: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for (change_id, attributes) in rows {
        latest.insert(change_id, attributes);
    }
    Ok(latest.into_values().collect())
}

/// 把读口装成一个服务，供装配处接给落地账（cog-github 不依赖存储层）。
pub struct PostgresLandingFacts {
    pool: PgPool,
}

impl PostgresLandingFacts {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl cog_core::LandingFactReader for PostgresLandingFacts {
    async fn latest_landing_attributes(&self) -> SFResult<Vec<String>> {
        latest_landing_attributes(&self.pool).await
    }
}
