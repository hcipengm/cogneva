//! `cogneva rebuild-metric-projections`：把三张指标累加表按 Prometheus 重写一遍。
//!
//! 这三张表是投影，真源是 Prometheus；这一步是它们的「重建命令」。没有它，三张表只是
//! 一份没人能重算的副本——样本日志不是来源（保留窗口会把旧样本清掉，实测 500 条累加里
//! 469 条已经对不上日志）。
//!
//! 环境变量：
//!
//! * `COGNEVA_DATABASE_URL`：要重建的库；
//! * `COGNEVA_PROMETHEUS_URL`：真源。没有它就没有重建，所以缺了直接退出非零，而不是
//!   把三张表清空之后才发现没有答案可写。
//!
//! 退出码就是判词：0 表示三张表都按当前答案重写完了；非零带一行原因。

use std::sync::Arc;

use cog_core::HttpClient;

/// 读环境、建连接、重建、印一行。
pub async fn run_from_env() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::var(cog_core::env::DATABASE_URL).map_err(|_| {
        "COGNEVA_DATABASE_URL 没有设置：不知道要重建哪个库，这一跑没有去处".to_string()
    })?;
    let prometheus = std::env::var(cog_core::env::PROMETHEUS_URL).map_err(|_| {
        "COGNEVA_PROMETHEUS_URL 没有设置：这三张表的真源是 Prometheus，没有它就没有答案可写"
            .to_string()
    })?;

    let pool = sqlx::PgPool::connect(&database).await?;
    let config =
        cog_net::HttpClientConfig::load().map_err(|e| format!("HTTP 客户端配置读不出来: {e}"))?;
    let client = cog_net::ReqwestHttpClient::from_config(&config)
        .map_err(|e| format!("HTTP 客户端造不出来: {e}"))?;
    let http: Arc<dyn HttpClient> = Arc::new(client);

    let tables = cog_observability::metric_projection::Tables {
        counters: cog_storage::postgres::metrics_backend::COUNTER_TOTALS_TABLE,
        buckets: cog_storage::postgres::metrics_backend::HISTOGRAM_BUCKETS_TABLE,
        sums: cog_storage::postgres::metrics_backend::HISTOGRAM_SUMS_TABLE,
    };
    let report = cog_observability::metric_projection::rebuild(&pool, &http, &prometheus, tables)
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    println!(
        "metric projections rebuilt: counters={} buckets={} sums={} as_of={:?}",
        report.counters, report.histogram_buckets, report.histogram_sums, report.as_of
    );
    Ok(())
}
