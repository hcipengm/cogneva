//! 一条关系在盘上占多少——三个容量清扫器共用的一把尺子。
//!
//! checkpoint 清扫、指标样本清扫与可观测事件清扫各发布一条字节读数
//! （`CHECKPOINTS_BYTES` / `METRICS_SAMPLES_BYTES` / `OBSERVABILITY_EVENTS_BYTES`），
//! 三条线在同一个面板上被并排看。它们必须是同一把尺子量出来的：三份拷贝各改一处
//! ——比如从含 TOAST 的 `pg_total_relation_size` 换成不含的 `pg_table_size`——不
//! 会编译失败，也不会有哪条测试发现；面板上只是有一条线的量纲悄悄变了，而「这一
//! 类日志比那一类占得少」这句话从此不成立，且没有任何东西会说不成立。
//!
//! 还有一处同形的静默：表还没建出来时 `to_regclass` 返回 NULL，
//! `pg_total_relation_size(NULL)` 也是 NULL，按 `i64` 解出来是一个解码错误。三份
//! 拷贝各自把它记成「拿不到这张表的大小」——而真正的原因（这张表还不存在）在读数
//! 里一个字都没有，于是「名字写错了」与「还没建表」与「库连不上」三种现场同形。

use cog_core::{SFError, SFResult};
use sqlx::PgPool;

/// 一条关系在盘上的字节数：索引与 TOAST 都算在内。
///
/// 这是一个**读数**，不是决策的输入：`DELETE` 腾出来的空间要等 vacuum 才归还，
/// 所以它比行数回落得慢，按它决定删多少就会在真空之前反复删同一批。
///
/// 语句本身在 [`cog_core::sql::RELATION_BYTES_SQL`]：同一张面板上的交换归档清扫
/// 量的是同一件事，两处各写一份 SQL 就是两条口径，而 SQL 只要改一个字就不再是
/// 同一把尺子。这里只重新导出，让既有调用点继续读这个名字。
pub use cog_core::sql::RELATION_BYTES_SQL;

/// 那条读数，或者「这张表还不存在」——两者不许同形。
pub(crate) async fn relation_bytes(pool: &PgPool, relation: &str) -> SFResult<i64> {
    let bytes: Option<i64> = sqlx::query_scalar(RELATION_BYTES_SQL)
        .bind(relation)
        .fetch_one(pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    bytes.ok_or_else(|| missing_relation(relation))
}

/// 读不到大小的第一种可能：这条关系不在库里。
///
/// 单独一个函数是为了能在没有数据库的时候把措辞钉住——这条错误是三个清扫器唯一
/// 能说明「没量到」的地方，措辞里少了名字，读者就只知道少了一个数，不知道少的是
/// 哪张表的。
fn missing_relation(relation: &str) -> SFError {
    SFError::Database(format!(
        "relation {relation} does not exist: to_regclass answered NULL, so its size is \
         unmeasured rather than zero"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unmeasured_relation_is_named_in_the_error() {
        let rendered = format!("{}", missing_relation("cog_checkpoints"));
        assert!(
            rendered.contains("cog_checkpoints"),
            "读不到大小要点名是哪张表：{rendered}"
        );
        assert!(
            rendered.contains("unmeasured rather than zero"),
            "缺表不是 0 字节：{rendered}"
        );
    }
}
