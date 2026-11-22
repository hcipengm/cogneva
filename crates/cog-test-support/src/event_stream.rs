//! 事件流的库形状：几条迁移，读者要的生成列在最后一条里。
//!
//! 两份真库用例（「一次打包花多少钱」与「行与流两个数相等」）原先各抄了一段同样的
//! 应用步骤。抄件之间没有东西比较，只会分开长：一处多读一条迁移，两份就关于两个
//! 形状，而两份都绿。判据在
//! `crates/cogneva/tests/the_fixture_reading_steps_are_written_once_test.rs`。

/// 建出事件流：`117` 建表、`119` 把载荷变成 JSON、`124` 建出读者按名取的生成列。
pub async fn event_stream(pool: &sqlx::PgPool) {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../cog-storage/migrations/postgres");
    for file in [
        "117_event_stream.up.sql",
        "119_event_payload_is_json.up.sql",
        // 读者按生成列取 token 与花费；缺这一条，流上那些列名根本不存在。
        "124_event_payload_query_columns.up.sql",
    ] {
        let sql = std::fs::read_to_string(dir.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply {file}: {e}"));
    }
}
