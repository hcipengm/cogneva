//! 定批的写入与读取，对着真 PostgreSQL 问三件事。
//!
//! - **一轮只有一条开着的**：最后一条没有配对 `round_closed` 的 `round_opened` 就是它。
//! - **批次是钉住的那一份**：读回来的清单与写进去的逐条相同，不随后来的账本变化。
//! - **重放不写第二条**：同一轮开工两次，流里仍是一条——幂等键是这一轮的身份。
//!
//! 第三件事是这一族存在的前提：平台的进化循环会重试一次开工，而重试写第二条会让
//! 「这一轮开了哪些条」有两个答案。
//!
//! 默认忽略，要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/throwaway \
//!   cargo test -p cog-storage --test round_events -- --ignored
//! ```

use chrono::Utc;
use sqlx::PgPool;

use cog_core::{RoundEvents, RoundOpened};
use cog_storage::RoundEventStore;

/// 这一份用例自己的 schema，跑完丢掉。
struct Scratch {
    pool: PgPool,
    schema: String,
}

impl Scratch {
    async fn new(case: &str) -> Self {
        let schema = cog_test_support::own_schema(
            &cog_test_support::database_url(),
            &format!("probe_round_{case}"),
        )
        .await;
        let pool =
            cog_test_support::schema_pool_with(&cog_test_support::database_url(), &schema, 2).await;
        // 与真表同形状的最小列集：写入口点的是这些列，少一列就不是同一个问题。
        sqlx::query(
            "CREATE TABLE cogneva_event (
                 event_id        TEXT PRIMARY KEY,
                 at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                 kind            TEXT NOT NULL,
                 subject_kind    TEXT NOT NULL,
                 subject_id      TEXT NOT NULL,
                 unit_of_work    TEXT NOT NULL,
                 step            TEXT NOT NULL,
                 outcome         TEXT NOT NULL,
                 attempt         INTEGER NOT NULL DEFAULT 1,
                 idempotency_key TEXT NOT NULL,
                 causation_id    TEXT,
                 batch_id        TEXT,
                 actor           TEXT NOT NULL,
                 writer          TEXT NOT NULL,
                 scope           TEXT NOT NULL,
                 payload_version INTEGER NOT NULL DEFAULT 1,
                 attributes      TEXT NOT NULL,
                 UNIQUE (writer, idempotency_key)
             )",
        )
        .execute(&pool)
        .await
        .expect("the event table must be creatable");
        Self { pool, schema }
    }

    async fn store(&self) -> RoundEventStore {
        RoundEventStore::new(self.pool.clone(), "probe")
    }

    async fn done(self) {
        cog_test_support::drop_schema(&cog_test_support::database_url(), &self.schema).await;
    }
}

fn round(id: &str, batch: &[&str]) -> RoundOpened {
    RoundOpened {
        round: id.to_string(),
        ledger_rev: "rev-1".into(),
        code_rev: "rev-1".into(),
        batch: batch.iter().map(|s| s.to_string()).collect(),
        actor: "probe-actor".into(),
        at: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_open_round_is_the_one_with_no_close() {
    let scratch = Scratch::new("open").await;
    let store = scratch.store().await;

    store
        .open(&round("r1", &["S1", "S2"]))
        .await
        .expect("open r1");
    let current = store
        .current()
        .await
        .expect("reading the open round must not fail")
        .expect("r1 is open");
    assert_eq!(current.round, "r1");
    assert_eq!(current.batch, vec!["S1".to_string(), "S2".to_string()]);

    store.close("r1", "probe").await.expect("close r1");
    store.close("r1", "probe").await.expect("replay the close");
    let closes: i64 =
        sqlx::query_scalar("SELECT count(*) FROM cogneva_event WHERE kind = 'round_closed'")
            .fetch_one(&scratch.pool)
            .await
            .expect("count the closes");
    assert_eq!(closes, 1, "关同一轮两次写了两条：幂等键没起作用");
    assert!(
        store.current().await.expect("read").is_none(),
        "关掉之后就没有开着的一轮了"
    );
    scratch.done().await;
}

/// 重放不写第二条：幂等键是这一轮的身份。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn replaying_an_open_writes_one_event() {
    let scratch = Scratch::new("replay").await;
    let store = scratch.store().await;

    store.open(&round("r1", &["S1"])).await.expect("first open");
    store.open(&round("r1", &["S1"])).await.expect("replay");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM cogneva_event")
        .fetch_one(&scratch.pool)
        .await
        .expect("count");
    assert_eq!(rows, 1, "同一次开工写了两条");
    scratch.done().await;
}

/// 旧的一轮关掉不影响新的一轮：配对按这一轮的身份，不按时间远近猜。
#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn closing_one_round_does_not_touch_another() {
    let scratch = Scratch::new("pair").await;
    let store = scratch.store().await;

    store.open(&round("r1", &["S1"])).await.expect("open r1");
    store
        .open(&round("r2", &["S2", "S3"]))
        .await
        .expect("open r2");
    store.close("r1", "probe").await.expect("close r1");

    let current = store.current().await.expect("read").expect("r2 is open");
    assert_eq!(current.round, "r2", "关掉 r1 之后当前那一轮该是 r2");
    assert_eq!(current.batch, vec!["S2".to_string(), "S3".to_string()]);
    scratch.done().await;
}
