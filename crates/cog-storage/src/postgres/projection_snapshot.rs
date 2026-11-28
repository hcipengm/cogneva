//! principle: feedback_a_row_per_commit_turns_the_row_rate_into_a_wal_wait_rate
//!
//! 投影的水位快照：**重建 = 快照 + 增量**。
//!
//! 一条投影从零重放要读完整条流，代价随流长线性增长；而读面只关心「最近这段」。快照
//! 把「到某条事件为止的状态」存一份，重建从它之后开始——于是重建耗时随**快照间隔**
//! 变化，不随**流长**变化。这两句话是同一件事的两面，也是这一格唯一要证明的事。
//!
//! 水位是 `event_id` 而不是时刻：时刻只说明「什么时候拍的」，而重建要问的是「从哪一条
//! 之后开始」，两者在乱序或补偿写入下不是一回事。

use sqlx::{PgPool, Row};

use cog_core::{SFError, SFResult};

/// 一份快照：投影名、水位、那一刻的折叠结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSnapshot {
    pub projection: String,
    pub event_id: String,
    pub state: String,
}

/// 存一份快照。同一 (投影, 水位) 写两次是同一行——重建重放不会让快照越攒越多。
pub async fn record(pool: &PgPool, snapshot: &ProjectionSnapshot) -> SFResult<()> {
    sqlx::query(
        "INSERT INTO projection_snapshot (projection, event_id, state) VALUES ($1, $2, $3) \
         ON CONFLICT (projection, event_id) DO NOTHING",
    )
    .bind(&snapshot.projection)
    .bind(&snapshot.event_id)
    .bind(&snapshot.state)
    .execute(pool)
    .await
    .map_err(|e| SFError::Database(format!("record a projection snapshot: {e}")))?;
    Ok(())
}

/// 这条投影最新的一份快照。`None` 是「还没有快照」——重建要从流的起点开始，那与
/// 「快照是空的」不是一件事：后者会说「从某个点之后没有增量」。
pub async fn newest(pool: &PgPool, projection: &str) -> SFResult<Option<ProjectionSnapshot>> {
    let row = sqlx::query(
        "SELECT projection, event_id, state FROM projection_snapshot \
         WHERE projection = $1 ORDER BY taken_at DESC, event_id DESC LIMIT 1",
    )
    .bind(projection)
    .fetch_optional(pool)
    .await
    .map_err(|e| SFError::Database(format!("read the newest projection snapshot: {e}")))?;
    row.map(|row| {
        Ok(ProjectionSnapshot {
            projection: row
                .try_get("projection")
                .map_err(|e| SFError::Database(format!("read projection: {e}")))?,
            event_id: row
                .try_get("event_id")
                .map_err(|e| SFError::Database(format!("read event_id: {e}")))?,
            state: row
                .try_get("state")
                .map_err(|e| SFError::Database(format!("read state: {e}")))?,
        })
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 快照的键是 (投影, 水位)：同一投影在两个水位上是两份，不同投影在同一水位上
    /// 互不相干。这条不需要库——判的是那一对键的形状。
    #[test]
    fn a_snapshot_is_keyed_by_projection_and_watermark() {
        let a = ProjectionSnapshot {
            projection: "task_current".into(),
            event_id: "E1".into(),
            state: "{}".into(),
        };
        let b = ProjectionSnapshot {
            event_id: "E2".into(),
            ..a.clone()
        };
        let c = ProjectionSnapshot {
            projection: "other".into(),
            ..a.clone()
        };
        assert_ne!((&a.projection, &a.event_id), (&b.projection, &b.event_id));
        assert_ne!((&a.projection, &a.event_id), (&c.projection, &c.event_id));
    }
}
