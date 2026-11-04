//! 队列满的时候，剩下的那些事件不许每条各要一条连接。
//!
//! 写者拿不到连接时，队列先替它把事件堆着；堆满了说明这条路此刻写不进去。曾经
//! 在这里的处置是「立刻单独写这一条」——每一条溢出的事件起一个任务去等连接，
//! 而它们等的是同一个被占住的池。集群库实测（2026-11-04）十分钟里三千多条
//! `pool timed out` 全是这些救兵：它们一条事件也没救回来，只把池压得更死。
//!
//! 判据落在**提交次数**上，和批量写那条同一个仪器：救兵写法下，每条溢出事件都是
//! 一次自己的连接、一次自己的提交；不救的写法下，溢出的事件被记进失败计数就到此
//! 为止，队列里那些照旧按批提交。两种写法写进表的行数差得不多，提交次数差一个
//! 数量级——这就是为什么这条判据读 `pg_stat_database.xact_commit`。
//!
//! 把写者挡住的办法是给池一条连接，由测试自己占住它：写者的 `begin()` 因此排队，
//! 队列在发布期间必然堆满。
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test a_full_write_queue_does_not_give_each_event_its_own_connection -- --ignored
//! ```

use chrono::Utc;
use cog_core::{AgentEvent, ObservabilityGateway};
use cog_storage::PostgresObservabilityGateway;
use sqlx::postgres::PgPoolOptions;

use cog_test_support::database_url;

mod common;

/// 发多少条。要明显多过写队列的长度，才能保证发布期间队列一定满；队列长度是
/// 实现细节，这里不抄它的值，只为「一定溢出」取个足够大的数。
// cap: bound — 明显大于写队列长度以保证溢出，又小到一次测试能跑完
const BURST: usize = 12000;

/// 提交次数的上限。按批写时它是个位数加轮询那几次；按条救的写法下，光溢出那部分
/// 就不少于几千次。中间这一大片空白让判据不必贴着实现调。
// cap: bound — 两种写法差一个数量级，取在中间
const MAX_COMMITS: i64 = 1000;

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_stalled_writer_does_not_turn_each_surplus_event_into_a_connection_wait() {
    // 一条连接：测试占住它，写者的 `begin()` 就只能排队。
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_secs(60))
        .connect(&database_url())
        .await
        .expect("connect to the test database");
    let gateway = PostgresObservabilityGateway::new(pool.clone());
    gateway.init_schema().await.expect("init the schema");

    let marker = format!(
        "full-queue-test:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let before = common::commits_on_this_database(&pool).await;

    // 占住唯一那条连接：发布期间写者拿不到它，队列因此堆满。
    let held = pool.acquire().await.expect("hold the only connection");

    let publisher: &dyn ObservabilityGateway = &gateway;
    for i in 0..BURST {
        publisher.publish_event(AgentEvent::TurnStart {
            agent_id: format!("{marker}:{i}"),
            timestamp: Utc::now(),
        });
    }

    drop(held);

    // 放开之后等写者把队列排空。这里不轮询：轮询自己也是一次提交，会把判据抬起来。
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let landed = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM cog_observability_events WHERE agent_id LIKE $1",
    )
    .bind(format!("{marker}:%"))
    .fetch_one(&pool)
    .await
    .expect("count the rows");
    let commits = common::commits_on_this_database(&pool).await - before;

    // 先清理，再断言：判据红了也要把库留在接手时的样子。
    sqlx::query("DELETE FROM cog_observability_events WHERE agent_id LIKE $1")
        .bind(format!("{marker}:%"))
        .execute(&pool)
        .await
        .expect("clean up the rows this run wrote");

    // 队列里那些必须照常落库：这条通路不能为了省提交而一条都不写。溢出那部分被
    // 丢掉是本轮的取舍，丢多少由队列长度决定，所以这里只判「写了」，不判写了几成。
    assert!(
        landed > 0,
        "一条也没落库：写者一放开就该把队列里的写完，而不是整段丢掉"
    );
    assert!(
        commits < MAX_COMMITS,
        "写者被挡住期间发了 {BURST} 条事件，落库花了 {commits} 次提交（上限 {MAX_COMMITS}）：\
         队列满之后的每条事件又在各要一条连接、各提交一次——那不是丢掉溢出的事件，\
         是给它们每条配了一个等连接的任务"
    );
}
