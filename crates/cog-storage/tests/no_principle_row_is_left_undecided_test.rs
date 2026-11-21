//! 原则举证欠账：`change_ledger` 里每一条 `principle:` 行都必须有结论。
//!
//! 1709 条去重欠账、树里 312 条原则标记、交集 0——不是账陈旧，是真没有证据。每条的
//! 结论只有三种：已成立（补标记并指向证据）、不成立（去实现）、不适用（退役并写理由）。
//! 停在 `pending` 的那一行等于「有人看见过、没人决定过」，而它在账上与「正在做」同形。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use sqlx::PgPool;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn no_principle_row_is_left_undecided() {
    let url = database_url();
    let schema = own_schema(&url, "principle_undecided").await;
    let pool = schema_pool(&url, &schema).await;

    // 账的形状由迁移给；这一条只问「还有没有停在 pending 的 principle 行」。
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS change_ledger (kind TEXT NOT NULL, scope TEXT NOT NULL, \
         literal TEXT NOT NULL, state TEXT NOT NULL)",
    )
    .execute(&pool)
    .await
    .expect("a ledger table for the probe");

    // 探针：一条已决定、一条未决定——后者正是这一条要红的那一行。
    sqlx::raw_sql(
        "INSERT INTO change_ledger (kind, scope, literal, state) VALUES \
         ('principle', 'crates/a.rs', 'landed one', 'landed'), \
         ('principle', 'crates/b.rs', 'undecided one', 'pending')",
    )
    .execute(&pool)
    .await
    .expect("the probe rows");

    let undecided: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM change_ledger WHERE kind = 'principle' AND state = 'pending'",
    )
    .fetch_one(&pool)
    .await
    .expect("count the undecided principle rows");

    assert_eq!(
        undecided, 0,
        "{undecided} 条 principle 行停在 pending：每一条都要有结论（已成立/不成立/不适用）"
    );

    pool.close().await;
    drop_schema(&url, &schema).await;
}
