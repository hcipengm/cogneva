//! 擦除的两条判据，对着真 PostgreSQL 问：
//!
//! - **因果不断**：被擦的那条事件（这里是记它哈希的那条 `llm_call`）**还在**，字段一个
//!   没改。擦除不动历史——动历史等于让回放读到一件从没发生过的事。
//! - **字节读不回来**：它 `attributes` 里那个哈希指向的对象，`lo_get` 读不到了。
//!
//! 两条合起来才是「擦除」：只有第二条是丢字节，只有第一条是假装擦过。
//!
//! 另有一条：同一段正文擦两次写的是同一条 `redacted` 事件（键是 oid + 哈希），
//! 而不是两条——重放不重复记账。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`（一次性库）。

use cog_core::KINDS;
use cog_storage::postgres::{large_object, redaction};
use sqlx::PgPool;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

/// 事件流那张表属于迁移，不属于任何一个 store。
async fn apply_event_stream_migration(pool: &PgPool) {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations/postgres");
    for file in [
        "117_event_stream.up.sql",
        "119_event_payload_is_json.up.sql",
    ] {
        let sql = std::fs::read_to_string(dir.join(file))
            .unwrap_or_else(|e| panic!("read the migration {file}: {e}"));
        sqlx::raw_sql(&sql)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply the migration {file}: {e}"));
    }
}

/// 一段正文：按块写进去、封口，返回 (oid, sha256, bytes)。
async fn sealed_body(pool: &PgPool, bytes: &[u8]) -> (u32, String, u64) {
    let oid = large_object::open(pool).await.expect("open a body");
    large_object::write_block(pool, oid, 0, bytes)
        .await
        .expect("write the body");
    let (sha256, len) = large_object::seal(pool, oid).await.expect("seal");
    (oid, sha256, len)
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn erasing_a_body_keeps_the_event_that_named_it_and_makes_its_bytes_unreadable() {
    let url = database_url();
    let schema = own_schema(&url, "erasure_keeps_the_event").await;
    let pool = schema_pool(&url, &schema).await;
    apply_event_stream_migration(&pool).await;

    assert!(
        KINDS.contains(&redaction::KIND),
        "`redacted` 必须在这个闭集里，否则写入口会拒它"
    );

    let body = b"the bytes a person asked to have erased".to_vec();
    let (oid, sha256, bytes) = sealed_body(&pool, &body).await;

    // 记下这一段的那条事件：它带着哈希，正是擦除之后还要在的那一条。
    sqlx::query(
        "INSERT INTO cogneva_event (\
            event_id, kind, subject_kind, subject_id, unit_of_work, step, outcome, attempt, \
            idempotency_key, actor, writer, scope, payload_version, attributes) \
         VALUES ('EVENT00000000000000000001', 'llm_call', 'llm_call', 'call-1', 'call-1', 'call', \
                 'ok', 1, 'call-1', 'actor-1', 'writer-1', 'test', 1, $1::text)",
    )
    .bind(serde_json::json!({ "response_body_oid": oid, "sha256": sha256 }).to_string())
    .execute(&pool)
    .await
    .expect("the event that names the body");

    let first = redaction::redact_body(
        &pool,
        "writer-1",
        "actor-1",
        oid,
        &sha256,
        bytes,
        "a person asked for it",
    )
    .await
    .expect("the erase runs");

    // 一：那条事件还在，哈希还写着。
    let kept: Option<String> = sqlx::query_scalar(
        "SELECT attributes::jsonb->>'sha256' FROM cogneva_event WHERE event_id = \
                            'EVENT00000000000000000001'",
    )
    .fetch_one(&pool)
    .await
    .expect("read the event back");
    assert_eq!(
        kept.as_deref(),
        Some(sha256.as_str()),
        "被擦的那条事件被改过了：历史不许改写"
    );

    // 二：它指向的字节读不回来了。
    let read: Result<Vec<u8>, _> = sqlx::query_scalar("SELECT lo_get($1::oid)")
        .bind(oid as i64)
        .fetch_one(&pool)
        .await;
    assert!(read.is_err(), "正文还在库里：擦除没销毁字节（{read:?}）");

    // 三：事件流里留下了一条说得清理由的 `redacted`。
    let (kind, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT kind, attributes::jsonb->>'reason' FROM cogneva_event WHERE event_id = $1",
    )
    .bind(&first.event_id)
    .fetch_one(&pool)
    .await
    .expect("the redacted event exists");
    assert_eq!(kind, "redacted");
    assert_eq!(reason.as_deref(), Some("a person asked for it"));

    // 四：再擦一次写的是同一条事件，不是两条。
    let again = redaction::redact_body(
        &pool,
        "writer-1",
        "actor-1",
        oid,
        &sha256,
        bytes,
        "a person asked for it",
    )
    .await
    .expect("erasing twice is safe");
    assert_eq!(again.event_id, first.event_id);
    let redacted_rows: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cogneva_event WHERE kind = 'redacted'")
            .fetch_one(&pool)
            .await
            .expect("count the redacted events");
    assert_eq!(redacted_rows, 1, "重放写了两条：幂等键没起作用");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
