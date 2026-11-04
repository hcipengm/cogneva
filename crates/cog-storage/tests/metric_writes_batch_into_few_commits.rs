//! 一串观测要花掉几次提交，而不是一条观测就一次提交。
//!
//! 指标落库此前是一条观测两条（或三条）语句、每条语句自己一次提交：样本日志
//! 一行、计数总量一次 upsert、直方图桶与和再各一次。于是「每秒多少条观测」
//! 直接变成「每秒多少次提交」。每次提交都要等 WAL 落盘；宿主同时跑着构建与
//! 集群时，等待被拉长到秒级，池里的连接全被提交占住（2026-11-04 现场：
//! `sqlx::pool::acquire` 反复报 10 秒超时，`pg_stat_activity` 里活着的语句全在等
//! `WALWrite`/`WALSync`，本库累计 2.2 亿次提交）。
//!
//! 这个判据只能对着真库问：语句时间在两种写法下都一样，差别在**提交次数**上，
//! 而提交次数不在任何一条语句的返回里。`pg_stat_database.xact_commit` 是本库
//! 提交的累计数，是这个差别唯一读得到的地方：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cog-storage --test metric_writes_batch_into_few_commits -- --ignored
//! ```
//!
//! 门槛取四分之一：批量写时提交数是个位数（加上轮询读的几次），逐条写时不少于
//! 观测条数。中间那一大片空白让这条判据不会因为轮询多了几次就翻红。

use std::collections::HashMap;

use chrono::Utc;
use cog_core::{MetricName, MetricsBackend};
use cog_storage::PostgresMetricsBackend;

use cog_test_support::database_url;

mod common;

/// 探针写的名字。没有任何构建会产出它，所以清理按名字删得干净。
const PROBE: &str = "probe_metric_batch_writes";

/// 这一轮发多少条。要明显大于一批的上限（256）才看得出「按批提交」与「按条
/// 提交」的差别，又要小到一次轮询能在几秒内写完。
// cap: bound — 远大于批上限（256）以显出差别，又小到能在几秒内写完
const BURST: usize = 600;

fn labels(series: usize) -> HashMap<String, String> {
    HashMap::from([("series".to_string(), series.to_string())])
}

async fn clean(pool: &sqlx::PgPool, suffix: &str) {
    let like = format!("{suffix}%");
    for table in [
        "cog_metrics_samples",
        "cog_metric_counter_totals",
        "cog_metric_histogram_buckets",
        "cog_metric_histogram_sums",
    ] {
        let sql = format!("DELETE FROM {table} WHERE name = $1 OR name LIKE $2");
        let _ = sqlx::query(&sql)
            .bind(PROBE)
            .bind(&like)
            .execute(pool)
            .await;
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL via COGNEVA_TEST_DATABASE_URL"]
async fn a_burst_of_observations_does_not_cost_one_commit_each() {
    let pool = sqlx::PgPool::connect(&database_url())
        .await
        .expect("connect to the test database");
    let backend = PostgresMetricsBackend::new(pool.clone());
    backend.init_schema().await.expect("init the schema");

    let marker = format!(
        "{PROBE}:{}:{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    clean(&pool, &marker).await;

    let before = common::commits_on_this_database(&pool).await;

    // 走 trait 对象，与生产调用同一条路。
    let metrics: &dyn MetricsBackend = &backend;
    for i in 0..BURST {
        let name = MetricName::for_tests_only(PROBE);
        let series = i % 8;
        // 三种观测都走一遍：它们各自落在不同的累积表上，批写要一起省下来。
        match i % 3 {
            0 => metrics.record_counter(name, 1.0, labels(series)).await,
            1 => metrics.record_gauge(name, i as f64, labels(series)).await,
            _ => metrics.record_histogram(name, 0.5, labels(series)).await,
        }
        .expect("record an observation");
    }

    // 读会先把队列里剩下的写下去，所以这条读回来时这一轮已经全部落库。
    let landed = metrics
        .query_counter_totals(PROBE)
        .await
        .expect("read the counter totals back");
    assert!(
        !landed.is_empty(),
        "这一轮一条计数观测都没落库：批写把整批丢了"
    );

    let after = common::commits_on_this_database(&pool).await;
    let commits = after - before;

    clean(&pool, &marker).await;

    assert!(
        (commits as usize) < BURST / 4,
        "写 {BURST} 条观测花了 {commits} 次提交（上限 {}）：这条通路又退回成\
         一条观测一次提交，观测速率就是提交速率，提交又要等 WAL",
        BURST / 4
    );
}
