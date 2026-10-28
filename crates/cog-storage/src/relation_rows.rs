//! 一条关系有多少行——四个清扫器共用的一把尺子。
//!
//! checkpoint 清扫、指标样本清扫、可观测事件清扫与任务事件归档各自发布一条
//! 行数读数，四条线在同一个面板上被并排比较（还有 cog-observability 的交换
//! 归档在旁）。它们必须是同一把尺子量出来的：三份 `SELECT count(*) FROM {}`
//! 里任一份被改动——比如加上 `WHERE` 只数某一分区——另外两条不会失败，面板上
//! 只是有一条线的口径悄悄变了，而「这类日志比那一类多」这句话从此不成立。
//!
//! 表名拼进语句之前的引号由 [`cog_core::sql::quote_relation`] 给：那是每个把名字
//! 写进 SQL 的地方共用的同一份拼法，不在这里再写一遍。

use cog_core::sql::quote_relation;
use cog_core::{SFError, SFResult};
use sqlx::PgPool;

/// 一条关系里有多少行。
///
/// 这是一个**读数**，不是决策的输入：它是全表的，不随分区、不随 age 策略，
/// 按它决定删多少会让「这张表很大」与「这张表里全是旧行」两件事同形。
pub(crate) async fn relation_rows(pool: &PgPool, relation: &str) -> SFResult<i64> {
    let sql = format!("SELECT count(*) FROM {}", quote_relation(relation));
    sqlx::query_scalar(&sql)
        .fetch_one(pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
}
