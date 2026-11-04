//! 一串事件要花掉几次提交，而不是几条事件就几次提交。
//!
//! 事件落库一开始是一条一次 INSERT、一条起一个任务，于是「每秒多少条事件」直接
//! 变成「每秒多少次提交」。每次提交都要等 WAL 落盘；宿主同时跑着构建与集群时，
//! 等待被拉长到秒级，池里的连接全被提交占住，读的人拿一条连接要等十秒
//! （2026-11-04 现场：`sqlx::pool::acquire` 反复报 10 秒，`pg_stat_activity`
//! 里活着的语句全在等 `WALWrite`/`WALSync`）。
//!
//! 这个判据只能对着真库问：语句时间在两种写法下都一样，差别在**提交次数**上，
//! 而提交次数不在任何一条语句的返回里。`pg_stat_database.xact_commit` 是本库
//! 提交的累计数，是这个差别唯一读得到的地方：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test observability_events_batch_writes -- --ignored
//! ```
//!
//! 门槛取四分之一：批量写时提交数是个位数（加上轮询读的几次），逐条写时不少于
//! 事件条数。中间那一大片空白让这条判据不会因为轮询多了几次就翻红。

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway};
use cog_storage::PostgresObservabilityGateway;
use sqlx::PgPool;

use cog_test_support::database_url;

/// 这一轮发多少条。要明显大于一批的上限（64），才看得出「按批提交」与
/// 「按条提交」的差别；又要小到一次轮询能在几秒内等完。
// cap: bound — 远大于批上限（64）以显出差别，又小到能在几秒内写完
const BURST: usize = 300;

async fn commits_on_this_database(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>(
        "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .expect("read xact_commit")
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_burst_of_events_does_not_cost_one_commit_each() {
    let pool = PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "event-batch-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );

    let before = commits_on_this_database(&pool).await;

    // 走 trait 对象，与生产调用同一条路。
    let publisher: &dyn ObservabilityGateway = &gateway;
    for i in 0..BURST {
        publisher.publish_event(AgentEvent::TurnStart {
            agent_id: format!("{marker}:{i}"),
            timestamp: Utc::now(),
        });
    }

    // 等最后一批落下去，再读数：先写、后读，不然读到的是还没提交的那一半。
    let mut landed = 0i64;
    for _ in 0..200 {
        landed = sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM cog_observability_events WHERE agent_id LIKE $1",
        )
        .bind(format!("{marker}:%"))
        .fetch_one(&pool)
        .await
        .expect("count the rows");
        if landed == BURST as i64 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        landed, BURST as i64,
        "批写没有把每一条都写进表：{landed} / {BURST}"
    );

    let after = commits_on_this_database(&pool).await;
    let commits = after - before;

    // 先清理，再断言：判据红了也要把库留在接手时的样子。
    sqlx::query("DELETE FROM cog_observability_events WHERE agent_id LIKE $1")
        .bind(format!("{marker}:%"))
        .execute(&pool)
        .await
        .expect("clean up the rows this run wrote");

    assert!(
        (commits as usize) < BURST / 4,
        "写 {BURST} 条事件花了 {commits} 次提交（上限 {}）：这条通路又退回成\
         一条事件一次提交，事件速率就是提交速率，提交又要等 WAL",
        BURST / 4
    );
}
