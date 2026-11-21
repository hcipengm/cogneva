//! 三张指标累加表是**投影**：它们上面每一行都能从真源（Prometheus）重算出来。
//!
//! 「能重算」这句声明如果不带一条真的重算测试，就只是声明——而投影最坏的失败不是
//! 算错，是**没人知道它该跟谁说一致**：一张算歪了的投影与一张对的投影在库里同形，
//! 读的人分不出来。这条测试跑一遍 `rebuild`，判三件事：
//!
//! - 表里一个过时/陈旧的行会被清掉（整表重写，不是合并）；
//! - 重写出来的行与真源给的数一致（计数器原值、直方图**累积桶还原成每档观测数**、
//!   直方图的和）；
//! - 水位 `as_of` 被写成这一次重建的时刻。
//!
//! 默认忽略，需要 `COGNEVA_TEST_DATABASE_URL`：
//!
//! ```text
//! COGNEVA_TEST_DATABASE_URL=postgres://user:pw@127.0.0.1:5432/probe \
//!   cargo test -p cogneva --test metric_projections_rebuild_from_prometheus -- --ignored
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::{HttpClient, HttpRequest, HttpResponse, SFResult};
use cog_observability::metric_projection::{rebuild, Tables};
use cog_storage::PostgresMetricsBackend;

use cog_test_support::{database_url, drop_schema, own_schema, schema_pool};

const COUNTER: &str = "cogneva_probe_total";
const HISTOGRAM: &str = "cogneva_probe_seconds";

/// 一个只回答两条 PromQL 的客户端：真源在这里是几个写死的数，测试判的是
/// 「重建有没有把这些数落成行」，不是 Prometheus 本身对不对。
#[derive(Debug)]
struct FakePrometheus;

#[async_trait]
impl HttpClient for FakePrometheus {
    async fn execute(&self, req: HttpRequest) -> SFResult<HttpResponse> {
        // URL 长这样：<base>/api/v1/query?query=<encoded promql>
        let query = req
            .url
            .split("query=")
            .nth(1)
            .unwrap_or_default()
            .replace("%7B", "{")
            .replace("%7D", "}")
            .replace("%5B", "[")
            .replace("%5D", "]");
        let body = if query.contains(&format!("{HISTOGRAM}_bucket")) {
            serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [
                    // 累积桶：≤0.1 有 3 次，≤1 有 10 次，+Inf 10 次。
                    { "metric": {"__name__": format!("{HISTOGRAM}_bucket"), "stream": "local", "le": "0.1"},
                      "value": [1_700_000_000u64, "3"] },
                    { "metric": {"__name__": format!("{HISTOGRAM}_bucket"), "stream": "local", "le": "1"},
                      "value": [1_700_000_000u64, "10"] },
                    { "metric": {"__name__": format!("{HISTOGRAM}_bucket"), "stream": "local", "le": "+Inf"},
                      "value": [1_700_000_000u64, "10"] },
                ]}
            })
        } else if query.contains(&format!("{HISTOGRAM}_sum")) {
            serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [
                    { "metric": {"__name__": format!("{HISTOGRAM}_sum"), "stream": "local"},
                      "value": [1_700_000_000u64, "4.5"] },
                ]}
            })
        } else if query.contains(COUNTER) {
            serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [
                    { "metric": {"__name__": COUNTER, "stream": "local"},
                      "value": [1_700_000_000u64, "42"] },
                ]}
            })
        } else {
            serde_json::json!({
                "status": "success",
                "data": { "resultType": "vector", "result": [] }
            })
        };
        Ok(HttpResponse {
            status: 200,
            headers: HashMap::new(),
            body: serde_json::to_vec(&body).expect("the canned answer serialises"),
        })
    }
}

#[tokio::test]
#[ignore = "needs COGNEVA_TEST_DATABASE_URL"]
async fn the_three_metric_projections_rebuild_from_the_source() {
    let url = database_url();
    let schema = own_schema(&url, "metric_projection_rebuild").await;
    let pool = schema_pool(&url, &schema).await;

    let backend = PostgresMetricsBackend::new(pool.clone());
    backend
        .init_schema()
        .await
        .expect("the metric tables exist");

    // 真源里有的那一个名字要在表里，重建才会去问它（名字从表里读，不从注册表猜）。
    sqlx::query(
        "INSERT INTO cog_metric_counter_totals (name, labels, upstream, as_of, value) \
         VALUES ($1, '{}', NULL, NOW(), 0)",
    )
    .bind(COUNTER)
    .execute(&pool)
    .await
    .expect("the counter name is present");

    // 一个**过时**的行：它不在真源里，重建之后必须消失。整表重写与「合并写」在这
    // 一格上分得开——后者会把它留着，而读数从此多出一行没人能解释的序列。
    sqlx::query(
        "INSERT INTO cog_metric_counter_totals (name, labels, upstream, as_of, value) \
         VALUES ($1, '{}', NULL, NOW(), 7)",
    )
    // 这个名字**不能**包含 `COUNTER`：假的真源按子串回答，含它的名字也会被答成
    // 42，于是重建把它当成真源里有的一行重新写回去——那测的就不是「整表重写」了。
    .bind("probe_stale_counter_total")
    .execute(&pool)
    .await
    .expect("the stale row is written");

    // 重建**从表里读名字**问真源：直方图那一族要有名字才会被问。这里放一条旧的
    // 和值，既给名字，也让「整表重写」这一格有东西可换掉。
    sqlx::query(
        "INSERT INTO cog_metric_histogram_sums (name, labels, upstream, as_of, sum) \
         VALUES ($1, '{}', NULL, NOW(), 999)",
    )
    .bind(HISTOGRAM)
    .execute(&pool)
    .await
    .expect("the histogram name is present");

    let http: Arc<dyn HttpClient> = Arc::new(FakePrometheus);
    let report = rebuild(
        &pool,
        &http,
        "http://prometheus.invalid",
        Tables {
            counters: "cog_metric_counter_totals",
            buckets: "cog_metric_histogram_buckets",
            sums: "cog_metric_histogram_sums",
        },
    )
    .await
    .expect("the rebuild runs");

    // 真源只回答了这一次重建要问的两个名字（计数器与直方图各一条序列），所以
    // 报告里的行数就是这两族各自落下的行数。
    assert_eq!(report.counters, 1, "计数器一族落了一行：{:?}", report);
    assert_eq!(
        report.histogram_buckets, 3,
        "三档累积桶 → 三行：{:?}",
        report
    );
    assert_eq!(report.histogram_sums, 1, "直方图的和落了一行：{:?}", report);
    assert!(report.as_of.is_some(), "重建之后水位写上了：{:?}", report);

    let stale: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM cog_metric_counter_totals WHERE name = 'probe_stale_counter_total'",
    )
    .fetch_one(&pool)
    .await
    .expect("read back the stale row");
    assert_eq!(stale, 0, "过时的行没被清掉：整表重写没做到");

    let counter: f64 =
        sqlx::query_scalar("SELECT value FROM cog_metric_counter_totals WHERE name = $1")
            .bind(COUNTER)
            .fetch_one(&pool)
            .await
            .expect("read the rebuilt counter");
    assert_eq!(counter, 42.0, "计数器的值与真源不一致");

    // 累积桶 → 每档观测数：3、7、0。最后一档（+Inf）减完是零，与写入端的算法一致。
    let buckets: Vec<i64> = sqlx::query_scalar(
        "SELECT observations FROM cog_metric_histogram_buckets WHERE name = $1 ORDER BY bucket",
    )
    .bind(HISTOGRAM)
    .fetch_all(&pool)
    .await
    .expect("read the rebuilt buckets");
    assert_eq!(buckets, vec![3, 7, 0], "累积桶没有还原成每档的观测数");

    let sum: f64 = sqlx::query_scalar("SELECT sum FROM cog_metric_histogram_sums WHERE name = $1")
        .bind(HISTOGRAM)
        .fetch_one(&pool)
        .await
        .expect("read the rebuilt histogram sum");
    assert_eq!(sum, 4.5, "直方图的和与真源不一致");

    pool.close().await;
    drop_schema(&url, &schema).await;
}
