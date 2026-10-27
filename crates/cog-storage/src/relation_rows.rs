//! 一条关系有多少行——四个清扫器共用的一把尺子。
//!
//! checkpoint 清扫、指标样本清扫、可观测事件清扫与任务事件归档各自发布一条
//! 行数读数，四条线在同一个面板上被并排比较（还有 cog-observability 的交换
//! 归档在旁）。它们必须是同一把尺子量出来的：三份 `SELECT count(*) FROM {}`
//! 里任一份被改动——比如加上 `WHERE` 只数某一分区——另外两条不会失败，面板上
//! 只是有一条线的口径悄悄变了，而「这类日志比那一类多」这句话从此不成立。
//!
//! 表名拼进语句之前引一遍。名字来自各文件自己的常量、不是运行时输入，但拼进
//! SQL 的形状在这里检查，而不是靠「它来自常量」这个约定。

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

/// 把一个表名引成标识符，允许 `schema.table` 这种限定写法。
///
/// 字母表之外的字符被丢弃而不是带进语句。丢弃对正确名字是无损的（名字来自
/// 常量），对一个被写坏的名字则是把它变成另一张表——调用方那一侧的表名清单
/// 与这里的分段规则是同一条约定的两半，改一边要两边一起改。
pub(crate) fn quote_relation(name: &str) -> String {
    name.split('.')
        .map(|part| {
            let cleaned: String = part
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            format!("\"{cleaned}\"")
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_schema_qualified_name_is_quoted_in_parts() {
        assert_eq!(
            quote_relation("public.cog_events"),
            "\"public\".\"cog_events\""
        );
    }

    #[test]
    fn a_plain_name_is_one_quoted_identifier() {
        assert_eq!(quote_relation("cog_checkpoints"), "\"cog_checkpoints\"");
    }
}
