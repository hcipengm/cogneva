//! 原则举证欠账：`change_ledger` 里每一条 `principle:` 行都必须有结论。
//!
//! 1709 条去重欠账、树里 312 条原则标记、交集 0——不是账陈旧，是真没有证据。每条的
//! 结论只有三种：已成立（补标记并指向证据）、不成立（去实现）、不适用（退役并写理由）。
//! 停在 `pending` 的那一行等于「有人看见过、没人决定过」，而它在账上与「正在做」同形。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`。

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

/// 两半，缺一半这条判据就不成立：
///
/// 1. **查询本身有效**：在一个一次性 schema 里放一条 pending、一条已决，查询必须只数出
///    那一条。没有这一半，一条永远返回 0 的错查询也会「通过」。
/// 2. **真账上没有停在 pending 的 principle 行**：对连接串指向的那个库的 `change_ledger`
///    数一遍。原先这条用例只有第 1 半，却断言「数出来是 0」——探针自己插的正是 pending，
///    于是这一步每次都红，而红的理由与真账无关（一道永远红的门只会被跳过）。
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

    // 探针：一条已决定、一条未决定——查询必须只数出后一条。
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
        undecided, 1,
        "探针里有一条 pending，查询却没数出来——这条判据本身是坏的"
    );
    pool.close().await;
    drop_schema(&url, &schema).await;

    // 第二半：连接串指向的那个库上的真账。
    let live = sqlx::PgPool::connect(&url)
        .await
        .expect("the database behind the url is reachable");
    let has_table: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
                        WHERE table_schema = ANY(current_schemas(false)) \
                          AND table_name = 'change_ledger')",
    )
    .fetch_one(&live)
    .await
    .expect("ask whether the ledger exists");
    if has_table {
        let owed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM change_ledger WHERE kind = 'principle' AND state = 'pending'",
        )
        .fetch_one(&live)
        .await
        .expect("count the live undecided principle rows");
        assert_eq!(
            owed, 0,
            "{owed} 条 principle 行停在 pending：每一条都要有结论（已成立/不成立/不适用）"
        );
    } else {
        eprintln!(
            "no `change_ledger` in this database: the live half was not judged \
             (the probe half passed)"
        );
    }
    live.close().await;
}
