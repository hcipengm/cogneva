//! 指标三张累加表的**重建**：从 Prometheus 把投影重算一遍。
//!
//! 这三张表是投影，而它们的真源只能是 Prometheus。样本日志不是来源：实测 2026-11-16，
//! 500 条累加里 469 条对应的样本已经被保留窗口清掉——日志按容量轮转，累加要跨越它。
//!
//! 重建的形状是「先问有哪些名字，再逐族取当前值，最后整表重写」：
//!
//! * 名字从表里读，不从注册表猜：表里有什么，就重建什么；一张空表重建之后还是空的，
//!   那是对的答案，不是漏了一步。
//! * 计数器取 `<name>` 的即时向量；直方图取 `<name>_bucket` 与 `<name>_sum`，把
//!   Prometheus 的**累积**桶值还原成这一平台存的那一份（每档的观测数 + 总的和）。
//! * 整表重写在一条事务里：读的人要么看到旧的那一版，要么看到新的一版，不会看到
//!   一半。
//!
//! 水位 `as_of` 写成这一次查询的时刻——它回答的是「这一行是哪个时刻的读数」。
//! 重建只碰这三张表：样本日志是追加表，不是投影。

use std::collections::BTreeMap;
use std::sync::Arc;

use sqlx::PgPool;

use crate::infra_watch::{query_prometheus, SeriesSample};

/// 重建一族的写法：怎么问 Prometheus，怎么把答案落到行上。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    Counter,
    HistogramBuckets,
    HistogramSums,
}

impl Family {
    /// 重建要问的 PromQL，按名字给。
    pub fn query(self, name: &str) -> String {
        match self {
            Family::Counter => name.to_string(),
            Family::HistogramBuckets => format!("{name}_bucket"),
            Family::HistogramSums => format!("{name}_sum"),
        }
    }
}

/// 一行，落库之前的样子。
#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedRow {
    pub name: String,
    /// 规范拼写：键排序、紧凑 JSON。与写入端交给服务端规整出来的那一份相同。
    pub labels: String,
    pub upstream: Option<String>,
    pub bucket: Option<i32>,
    pub value: f64,
}

/// 把一次即时向量查询的答案落成行。
///
/// `le` 是 Prometheus 直方图的分档标签，不是这个平台的标签集的一部分：它被读成
/// `bucket` 那一列（`+Inf` 是 `i32::MAX`），剩下的标签才是标签集。
pub fn rows_from_vector(name: &str, family: Family, samples: &[SeriesSample]) -> Vec<ProjectedRow> {
    samples
        .iter()
        .map(|sample| {
            let mut labels: BTreeMap<String, String> = BTreeMap::new();
            let mut bucket = None;
            for (key, value) in &sample.labels {
                if key == "__name__" {
                    continue;
                }
                if key == "le" && family == Family::HistogramBuckets {
                    bucket = Some(le_to_bucket(name, value));
                    continue;
                }
                labels.insert(key.clone(), value.clone());
            }
            ProjectedRow {
                name: name.to_string(),
                upstream: labels
                    .get(cog_core::observability_text::UPSTREAM_LABEL)
                    .cloned(),
                labels: serde_json::to_string(&labels).unwrap_or_else(|_| "{}".to_string()),
                bucket,
                value: sample.value,
            }
        })
        .collect()
}

/// `le` 的分档值换成本平台那一列的编号。
///
/// 编号的定义只有一份（`cog_core::histogram_bucket_bounds` + 写入端的
/// `position(|bound| value <= *bound)`）：Prometheus 的 `le` 是那个上界，所以下标就是
/// 界列表里第一个不小于它的位置；`+Inf` 落在最后一档（`bounds.len()`）。
///
/// 读不出数字的分档记成最后一档：那是它在直方图里的位置（最上面一档），而且
/// 「读不懂」与「无穷」在这一列上本来就该落到同一处。
fn le_to_bucket(name: &str, le: &str) -> i32 {
    let bounds = cog_core::histogram_bucket_bounds(name);
    let top = bounds.len() as i32;
    if le == "+Inf" || le == "Inf" {
        return top;
    }
    match le.parse::<f64>() {
        Ok(bound) => bounds
            .iter()
            .position(|edge| bound <= *edge)
            .map(|index| index as i32)
            .unwrap_or(top),
        Err(_) => top,
    }
}

/// 把 Prometheus 的累积桶值还原成每档的观测数。
///
/// Prometheus 的 `_bucket` 是**累积**的（≤ 上界），而这个平台的列存的是这一档自己的
/// 观测数。两者之差就是那一档的数；最后一档（`+Inf`）减完是零，与写入端的算法一致。
pub fn bucket_observations(rows: &[ProjectedRow]) -> Vec<ProjectedRow> {
    let mut out = rows.to_vec();
    out.sort_by_key(|row| row.bucket.unwrap_or(i32::MAX));
    let mut previous = 0.0;
    for row in out.iter_mut() {
        let cumulative = row.value;
        row.value = (cumulative - previous).max(0.0);
        previous = cumulative;
    }
    out
}

/// 要重写的三张表，由调用方给：表名在 `cog-storage` 里声明（SQL 住在那里），
/// 而这个 crate 不依赖它——依赖方向是「读数不依赖存储实现」。
#[derive(Debug, Clone, Copy)]
pub struct Tables<'a> {
    pub counters: &'a str,
    pub buckets: &'a str,
    pub sums: &'a str,
}

/// 重建报告：每一族重写了多少行，水位是什么。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct RebuildReport {
    pub counters: usize,
    pub histogram_buckets: usize,
    pub histogram_sums: usize,
    pub as_of: Option<chrono::DateTime<chrono::Utc>>,
}

/// 从 Prometheus 把三张累加表重写一遍。
///
/// 名字从表里读：表里有什么，就重建什么。整表重写在一条事务里。
pub async fn rebuild(
    pool: &PgPool,
    http: &Arc<dyn cog_core::HttpClient>,
    prometheus: &str,
    tables: Tables<'_>,
) -> Result<RebuildReport, String> {
    let counter_names: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT name FROM cog_metric_counter_totals ORDER BY name")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("read the counter names: {e}"))?;
    let histogram_names: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT name FROM cog_metric_histogram_sums ORDER BY name")
            .fetch_all(pool)
            .await
            .map_err(|e| format!("read the histogram names: {e}"))?;

    let mut counters = Vec::new();
    for name in &counter_names {
        let samples = query_prometheus(http, prometheus, &Family::Counter.query(name)).await?;
        counters.extend(rows_from_vector(name, Family::Counter, &samples));
    }
    let mut buckets = Vec::new();
    let mut sums = Vec::new();
    for name in &histogram_names {
        let raw = query_prometheus(http, prometheus, &Family::HistogramBuckets.query(name)).await?;
        buckets.extend(bucket_observations(&rows_from_vector(
            name,
            Family::HistogramBuckets,
            &raw,
        )));
        let raw = query_prometheus(http, prometheus, &Family::HistogramSums.query(name)).await?;
        sums.extend(rows_from_vector(name, Family::HistogramSums, &raw));
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| format!("begin the rebuild: {e}"))?;
    for table in [tables.counters, tables.buckets, tables.sums] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut *tx)
            .await
            .map_err(|e| format!("clear {table}: {e}"))?;
    }
    for row in &counters {
        sqlx::query(
            "INSERT INTO cog_metric_counter_totals (name, labels, upstream, as_of, value) \
             VALUES ($1, $2::jsonb::text, $3, NOW(), $4)",
        )
        .bind(&row.name)
        .bind(&row.labels)
        .bind(&row.upstream)
        .bind(row.value)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("write a counter: {e}"))?;
    }
    for row in &buckets {
        sqlx::query(
            "INSERT INTO cog_metric_histogram_buckets \
                 (name, labels, upstream, as_of, bucket, observations) \
             VALUES ($1, $2::jsonb::text, $3, NOW(), $4, $5)",
        )
        .bind(&row.name)
        .bind(&row.labels)
        .bind(&row.upstream)
        .bind(row.bucket.unwrap_or(i32::MAX))
        .bind(row.value as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("write a histogram bucket: {e}"))?;
    }
    for row in &sums {
        sqlx::query(
            "INSERT INTO cog_metric_histogram_sums (name, labels, upstream, as_of, sum) \
             VALUES ($1, $2::jsonb::text, $3, NOW(), $4) \
             ON CONFLICT (name, labels) DO UPDATE SET sum = EXCLUDED.sum, as_of = EXCLUDED.as_of",
        )
        .bind(&row.name)
        .bind(&row.labels)
        .bind(&row.upstream)
        .bind(row.value)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("write a histogram sum: {e}"))?;
    }
    // 三张累加表被整表重写了，标签侧表也要跟着重写：只写累加行会让重建出来的序列在
    // `metric_labels` 上一条标签都没有——按标签筛的读者（第 10 环的 endpoint/status）
    // 会读成「这个端点一条请求都没有」。侧表是累加行的导数，重写就得一起重写。
    sqlx::query("DELETE FROM metric_labels")
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("clear metric_labels: {e}"))?;
    sqlx::query(
        "INSERT INTO metric_labels (name, labels, key, value) \
         SELECT t.name, t.labels, e.key, e.value \
           FROM (SELECT DISTINCT name, labels FROM cog_metric_counter_totals \
                 UNION SELECT DISTINCT name, labels FROM cog_metric_histogram_buckets \
                 UNION SELECT DISTINCT name, labels FROM cog_metric_histogram_sums) t, \
                LATERAL jsonb_each_text(t.labels::jsonb) e \
         ON CONFLICT (name, labels, key) DO NOTHING",
    )
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("rebuild metric_labels: {e}"))?;

    let as_of: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT max(as_of) FROM cog_metric_counter_totals")
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| format!("read the watermark back: {e}"))?;
    tx.commit()
        .await
        .map_err(|e| format!("commit the rebuild: {e}"))?;

    Ok(RebuildReport {
        counters: counters.len(),
        histogram_buckets: buckets.len(),
        histogram_sums: sums.len(),
        as_of,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 标签集的拼写只能是写入端与服务端规整出来的那一份：键排序、紧凑 JSON。
    /// 拼错一次，同一个序列在重建之后就成了新的一行——`ON CONFLICT` 认不出它，
    /// 计数从此分成两半。
    #[test]
    fn a_rebuilt_row_carries_the_canonical_label_spelling() {
        let rows = rows_from_vector(
            cog_core::metric_names::READY_ADMISSION_HELD_TOTAL.as_str(),
            Family::Counter,
            &[SeriesSample {
                labels: [
                    ("__name__".to_string(), "x".to_string()),
                    ("stream".to_string(), "local".to_string()),
                    ("reason".to_string(), "pool".to_string()),
                ]
                .into(),
                value: 7.0,
            }],
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].labels, r#"{"reason":"pool","stream":"local"}"#);
        assert_eq!(rows[0].upstream, None);
        assert_eq!(rows[0].value, 7.0);
    }

    /// 名册那一步读的是 `upstream` 这一列，重建必须把它填上——否则名册会把这些
    /// 序列读成「没有上游」，而那个方向是「不删」，看起来一切正常。
    #[test]
    fn a_rebuilt_row_keeps_the_upstream_column() {
        let rows = rows_from_vector(
            cog_core::metric_names::READY_ADMISSION_HELD_TOTAL.as_str(),
            Family::Counter,
            &[SeriesSample {
                labels: [(
                    cog_core::observability_text::UPSTREAM_LABEL.to_string(),
                    "a.example".to_string(),
                )]
                .into(),
                value: 1.0,
            }],
        );
        assert_eq!(rows[0].upstream.as_deref(), Some("a.example"));
    }

    /// Prometheus 的桶是累积的，这个平台的列是每档自己的数。两者的差就是那一档。
    #[test]
    fn cumulative_buckets_become_per_bucket_counts() {
        let rows = vec![
            ProjectedRow {
                name: "probe_ms".into(),
                labels: "{}".into(),
                upstream: None,
                bucket: Some(0),
                value: 1.0,
            },
            ProjectedRow {
                name: "probe_ms".into(),
                labels: "{}".into(),
                upstream: None,
                bucket: Some(1),
                value: 4.0,
            },
            ProjectedRow {
                name: "probe_ms".into(),
                labels: "{}".into(),
                upstream: None,
                bucket: Some(i32::MAX),
                value: 6.0,
            },
        ];
        let got: Vec<f64> = bucket_observations(&rows).iter().map(|r| r.value).collect();
        assert_eq!(got, vec![1.0, 3.0, 2.0]);
    }

    /// `le` 换成的编号必须与写入端 `histogram_bucket` 用的是同一个映射：上界落在哪一档，
    /// 就记成哪一档。`+Inf` 是最上面那一档。
    #[test]
    fn a_boundary_is_the_bucket_the_writer_would_have_used() {
        let name = "probe_operation_ms";
        let bounds = cog_core::histogram_bucket_bounds(name);
        for (index, bound) in bounds.iter().enumerate() {
            assert_eq!(
                le_to_bucket(name, &bound.to_string()),
                index as i32,
                "bound {bound} did not land in its own bucket"
            );
        }
        assert_eq!(le_to_bucket(name, "+Inf"), bounds.len() as i32);
    }
}
