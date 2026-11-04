use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::PgPool;
use std::collections::HashMap;

use cog_core::{
    histogram_bucket_bounds, HistogramTotals, MetricName, MetricSample, MetricType, MetricsBackend,
    SFError, SFResult,
};

/// The identity of one series, from the only thing that identifies it.
///
/// A row per series comes back from the store, but each row's labels are read
/// into their own `HashMap`, and a map's iteration order is drawn per instance
/// rather than from its contents. Keying on the map — or on serializing it —
/// therefore splits one series across as many keys as the map has orders, and
/// redraws the split on every scrape, because the maps are rebuilt each time.
/// The canonical label-set form is a function of the contents alone, and it is
/// the same one the render side groups by, so the two ends cannot come to
/// different conclusions about what one series is.
fn series_key(labels: &HashMap<String, String>) -> String {
    cog_core::observability_text::format_labels(labels)
}

/// The read a scrape makes of one gauge: its newest sample per label set.
///
/// Public because it is half of a pair with the index that carries it (see
/// `idx_cog_metrics_latest_series` in [`PostgresMetricsBackend::init_schema`]),
/// and the gate that holds the two together has to explain the statement the
/// backend actually runs rather than a copy of it. One hand-written copy of a
/// statement is how a gate comes to certify something else.
///
/// Two probes per series, both against that index: one that walks the label
/// sets in the index's own order (`labels >` the one just read, `LIMIT 1`), and
/// one that reads the newest sample of the label set it landed on (`labels =`
/// exact, `ORDER BY timestamp DESC LIMIT 1`). The recursion stops on the row
/// the walk returns past the last label set, whose NULL the outer query drops,
/// so the walk costs one descent per series and the answer costs one row per
/// series. `jsonb` defines an ordering but no `min()`/`max()` aggregate, hence
/// the `ORDER BY labels LIMIT 1` probes rather than an aggregate: an aggregate
/// over the label sets would also have to read every sample behind them.
///
/// A single `DISTINCT ON (labels) ... ORDER BY labels, timestamp DESC` asks for
/// the same rows and reads the same index, but it reaches its bound only if the
/// planner hands it the series in group order, and whether the planner does so
/// depends on how dense that index physically is. After half a million sample
/// deletions (the retention path trims this log continuously) the live index
/// carried two pages per entry, which put the estimated cost of the ordered
/// walk above the estimated cost of reading the same rows out of the heap and
/// sorting them: 120,457 rows sorted into a 9,344 kB temporary file, 848 ms per
/// scrape, to return 32 rows -- index present and unused. Cost here is bounded
/// by the answer whatever the planner believes about the index, because no node
/// in this plan can see more rows than there are series.
pub const GAUGE_LATEST_SQL_TEMPLATE: &str = r#"
            WITH RECURSIVE series(labels) AS (
                SELECT (SELECT s.labels FROM cog_metrics_samples s
                        WHERE s.metric_type = '{metric:gauge}' AND s.name = $1
                        ORDER BY s.labels LIMIT 1)
              UNION ALL
                SELECT (SELECT s.labels FROM cog_metrics_samples s
                        WHERE s.metric_type = '{metric:gauge}' AND s.name = $1
                          AND s.labels > series.labels
                        ORDER BY s.labels LIMIT 1)
                FROM series WHERE series.labels IS NOT NULL
            )
            SELECT latest.value, series.labels, latest.timestamp
            FROM series
            CROSS JOIN LATERAL (
                SELECT s.value, s.timestamp FROM cog_metrics_samples s
                WHERE s.metric_type = '{metric:gauge}' AND s.name = $1 AND s.labels = series.labels
                ORDER BY s.timestamp DESC LIMIT 1
            ) AS latest
            WHERE series.labels IS NOT NULL
            "#;

/// The newest sample per label set, with the kind word filled in.
///
/// `gauge` is a word [`MetricType::as_str`] decides, and the samples table is
/// read back by kind. A predicate that spells the kind itself keeps matching
/// the old value after a rename and returns no rows -- which every caller reads
/// as "this series has no samples yet", the one answer a metrics reader is
/// least likely to question.
pub fn gauge_latest_sql() -> String {
    super::fill_slots(
        GAUGE_LATEST_SQL_TEMPLATE,
        &[("metric", MetricType::Gauge.as_str())],
    )
}

/// This store's schema, and the one a restore replays.
pub const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS cog_metrics_samples (
                id SERIAL PRIMARY KEY,
                metric_type TEXT NOT NULL,
                name TEXT NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
;
CREATE INDEX IF NOT EXISTS idx_cog_metrics_name_type ON cog_metrics_samples(name, metric_type)
;
CREATE INDEX IF NOT EXISTS idx_cog_metrics_timestamp ON cog_metrics_samples(timestamp)
;
CREATE INDEX IF NOT EXISTS idx_cog_metrics_latest_series
                ON cog_metrics_samples(name, metric_type, labels, timestamp DESC)
                INCLUDE (value)
;
CREATE TABLE IF NOT EXISTS cog_metric_counter_totals (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                value DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (name, labels)
            )
;
CREATE TABLE IF NOT EXISTS cog_metric_histogram_buckets (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                bucket INTEGER NOT NULL,
                observations BIGINT NOT NULL,
                PRIMARY KEY (name, labels, bucket)
            )
;
CREATE TABLE IF NOT EXISTS cog_metric_histogram_sums (
                name TEXT NOT NULL,
                labels JSONB NOT NULL DEFAULT '{}',
                sum DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (name, labels)
            );
"#;

/// One observation on its way to the store.
///
/// The labels are carried as the canonical JSON text rather than as a map so
/// that two observations of the same series are equal by their bytes: a map's
/// iteration order is drawn per instance, and grouping a batch on the map would
/// split one series into as many rows as it has orders — which the upsert then
/// refuses ("cannot affect row a second time") instead of accumulating.
#[derive(Debug, Clone, PartialEq)]
enum PendingWrite {
    Gauge {
        name: String,
        value: f64,
        labels: String,
    },
    Counter {
        name: String,
        value: f64,
        labels: String,
    },
    Histogram {
        name: String,
        value: f64,
        labels: String,
        bucket: i32,
    },
}

impl PendingWrite {
    fn labels(&self) -> &str {
        match self {
            PendingWrite::Gauge { labels, .. }
            | PendingWrite::Counter { labels, .. }
            | PendingWrite::Histogram { labels, .. } => labels,
        }
    }

    fn name(&self) -> &str {
        match self {
            PendingWrite::Gauge { name, .. }
            | PendingWrite::Counter { name, .. }
            | PendingWrite::Histogram { name, .. } => name,
        }
    }
}

/// What the writer is asked to do: write this, or say that everything queued so
/// far has landed.
///
/// The barrier is what keeps a read-after-write honest. A caller that records a
/// value and then reads it back is asking about the value it just wrote, and a
/// queue nobody drains before the read would answer `None` for a series that is
/// merely still in memory.
enum Command {
    Write(PendingWrite),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// 一批最多写几条。
///
/// 由「一次提交的固定成本摊到多少条上才不亏」定，不是从任何容量读数导出：
/// 一次提交的 WAL 页与提交记录摊到 256 条上已低到可以忽略，再往上加行省下的
/// 提交成本小于整批变慢的方差。
// cap: bound — 使一次提交的固定成本摊到 256 条上可忽略，不是从任何容量读数导出
const METRIC_BATCH_MAX: usize = 256;

/// 第一条到了之后最多再等多久凑批。
///
/// 指标是按拍刷的（每次抓取、每个事件循环各一轮），凑够一批通常不用等；
/// 这个等待只对安静时段起作用，让单条观测不因为「等下一批」而迟到。
// cap: bound — 与指标到达间隔（毫秒级）相比足够短，只用来收尾一批
const METRIC_BATCH_LINGER_MS: u64 = 50;

/// 队列能排多少条。满了不丢观测，改为立刻单独写一条。
///
/// 按「数据库停住多久还不丢」定：实测本平台每秒数十到数百条观测，8192 条约是
/// 十几秒到几分钟的积压。
// cap: bound — 按观测速率（每秒数十到数百条）乘一段可等待的数据库停顿估出
const METRIC_WRITE_QUEUE: usize = 8192;

/// 读之前等写者确认的上限。
///
/// 确认是一次队列内的屏障，正常在一批之内（几十毫秒）就回来；超过这个界说明
/// 写者不在了，读继续走——读到的是还没落库的观测缺席，那是对的答案，而不是
/// 一次永远不返回的读。
// cap: bound — 屏障正常在毫秒级返回，这个界只用来结束一个已经不存在的写者
const METRIC_FLUSH_ACK_TIMEOUT_SECS: u64 = 5;

pub struct PostgresMetricsBackend {
    pool: PgPool,
    /// The writer's queue. Created on the first write, because the writer is a
    /// task and a backend built outside a runtime has none to start it on.
    queue: tokio::sync::OnceCell<tokio::sync::mpsc::Sender<Command>>,
}

impl PostgresMetricsBackend {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            queue: tokio::sync::OnceCell::new(),
        }
    }

    /// Auto-create the required table and indexes if they do not exist.
    pub async fn init_schema(&self) -> SFResult<()> {
        crate::schema_ddl::apply(&self.pool, DDL).await
    }

    /// The bucket index a value falls in.
    ///
    /// A value above the top finite bound lands in bucket index `bounds.len()`,
    /// which is the `+Inf` bucket — the same index the read side treats as the
    /// overflow, so the two ends agree without a separate overflow column.
    fn histogram_bucket(name: &str, value: f64) -> i32 {
        let bounds = histogram_bucket_bounds(name);
        bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(bounds.len()) as i32
    }

    /// Hand one observation to the writer, starting it if this is the first.
    ///
    /// A full queue is not a dropped observation: the caller writes it itself,
    /// which is slower and is the point — a database that cannot keep up should
    /// slow its writers down, not silently lose what they said.
    async fn enqueue(&self, pending: PendingWrite) -> SFResult<()> {
        let queue = self
            .queue
            .get_or_init(|| async {
                let (tx, rx) = tokio::sync::mpsc::channel(METRIC_WRITE_QUEUE);
                tokio::spawn(run_metric_writer(self.pool.clone(), rx));
                tx
            })
            .await;

        match queue.try_send(Command::Write(pending)) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(Command::Write(pending)))
            | Err(tokio::sync::mpsc::error::TrySendError::Closed(Command::Write(pending))) => {
                write_batch(&self.pool, std::slice::from_ref(&pending)).await
            }
            // The command is only ever a write here; a barrier cannot be
            // constructed on this path.
            Err(_) => Ok(()),
        }
    }

    /// Wait until everything recorded before this call has been written.
    ///
    /// Reads call this first. A reader asking for a value it just recorded is
    /// asking a question about the store, and answering it out of a queue that
    /// has not landed yet would say the series is missing. Public for the same
    /// reason: a caller that reads the tables itself has taken over the read
    /// the backend would have done, and the queue is invisible from out there.
    pub async fn flush_pending(&self) -> SFResult<()> {
        let Some(queue) = self.queue.get() else {
            return Ok(());
        };
        let (ack, landed) = tokio::sync::oneshot::channel();
        if queue.send(Command::Flush(ack)).await.is_err() {
            return Ok(());
        }
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(METRIC_FLUSH_ACK_TIMEOUT_SECS),
            landed,
        )
        .await;
        Ok(())
    }

    fn labels_json(labels: &HashMap<String, String>) -> SFResult<String> {
        // 一个标签集只能有一种拼写。`HashMap` 的迭代顺序每份 map 都不同，直接
        // 序列化会让同一个标签集写出字节不同的两个 JSON——批内求和不认得它们是
        // 同一个序列，写入端也就把一条序列拆成两行。这里按 key 排序后再序列化，
        // 与分组用的 [`series_key`] 是同一个次序。
        let ordered: std::collections::BTreeMap<&String, &String> = labels.iter().collect();
        serde_json::to_string(&ordered).map_err(SFError::Serialization)
    }

    async fn record(
        &self,
        metric_type: MetricType,
        name: &str,
        value: f64,
        labels: &HashMap<String, String>,
    ) -> SFResult<()> {
        let labels = Self::labels_json(labels)?;
        let pending = match metric_type {
            MetricType::Gauge => PendingWrite::Gauge {
                name: name.to_string(),
                value,
                labels,
            },
            MetricType::Histogram => PendingWrite::Histogram {
                name: name.to_string(),
                value,
                labels,
                bucket: Self::histogram_bucket(name, value),
            },
            MetricType::Counter => PendingWrite::Counter {
                name: name.to_string(),
                value,
                labels,
            },
        };
        self.enqueue(pending).await
    }

    async fn query_range(
        &self,
        metric_type: MetricType,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.flush_pending().await?;
        let rows: Vec<(f64, serde_json::Value, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT value, labels, timestamp
            FROM cog_metrics_samples
            WHERE metric_type = $1 AND name = $2 AND timestamp >= $3 AND timestamp <= $4
            ORDER BY timestamp ASC
            "#,
        )
        .bind(metric_type.as_str())
        .bind(name)
        .bind(start)
        .bind(end)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut samples = Vec::with_capacity(rows.len());
        for (value, labels_json, timestamp) in rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            samples.push(MetricSample {
                timestamp,
                value,
                labels,
            });
        }
        Ok(samples)
    }
}

/// One series' increments, summed, ready to become one upsert row.
///
/// Duplicates are collapsed here because `ON CONFLICT DO UPDATE` refuses a
/// statement that would touch the same row twice, and a batch carrying a
/// hundred ticks of one counter is exactly that. Summing them first is what
/// accumulating a counter means anyway.
#[derive(Default)]
struct Accumulated {
    rows: Vec<(String, String, f64)>,
    index: HashMap<(String, String), usize>,
}

impl Accumulated {
    fn add(&mut self, name: &str, labels: &str, value: f64) {
        let key = (name.to_string(), labels.to_string());
        if let Some(at) = self.index.get(&key) {
            self.rows[*at].2 += value;
            return;
        }
        self.index.insert(key, self.rows.len());
        self.rows
            .push((name.to_string(), labels.to_string(), value));
    }

    fn columns(&self) -> (Vec<String>, Vec<String>, Vec<f64>) {
        let mut names = Vec::with_capacity(self.rows.len());
        let mut labels = Vec::with_capacity(self.rows.len());
        let mut values = Vec::with_capacity(self.rows.len());
        for (name, label_set, value) in &self.rows {
            names.push(name.clone());
            labels.push(label_set.clone());
            values.push(*value);
        }
        (names, labels, values)
    }
}

/// Histogram buckets of a batch: one observation per entry, summed by bucket.
#[derive(Default)]
struct BucketAccumulated {
    rows: Vec<(String, String, i32, i64)>,
    index: HashMap<(String, String, i32), usize>,
}

impl BucketAccumulated {
    fn add(&mut self, name: &str, labels: &str, bucket: i32) {
        let key = (name.to_string(), labels.to_string(), bucket);
        if let Some(at) = self.index.get(&key) {
            self.rows[*at].3 += 1;
            return;
        }
        self.index.insert(key, self.rows.len());
        self.rows
            .push((name.to_string(), labels.to_string(), bucket, 1));
    }

    fn columns(&self) -> (Vec<String>, Vec<String>, Vec<i32>, Vec<i64>) {
        let mut names = Vec::with_capacity(self.rows.len());
        let mut labels = Vec::with_capacity(self.rows.len());
        let mut buckets = Vec::with_capacity(self.rows.len());
        let mut observations = Vec::with_capacity(self.rows.len());
        for (name, label_set, bucket, count) in &self.rows {
            names.push(name.clone());
            labels.push(label_set.clone());
            buckets.push(*bucket);
            observations.push(*count);
        }
        (names, labels, buckets, observations)
    }
}

/// The sample log's rows, written as one statement per batch.
///
/// The arrays arrive as `text[]` and are cast on the way in: the driver sends
/// both a JSON text and a `jsonb` the same way, and casting on the server keeps
/// the client side to types every caller already has.
const SAMPLE_INSERT_SQL: &str = r#"
    INSERT INTO cog_metrics_samples (metric_type, name, value, labels, timestamp)
    SELECT t.kind, t.name, t.value, t.labels::jsonb, NOW()
    FROM unnest($1::text[], $2::text[], $3::float8[], $4::text[]) AS t(kind, name, value, labels)
"#;

/// `SUM` over the batch, so one series is one row for the upsert to touch.
const COUNTER_TOTAL_UPSERT_SQL: &str = r#"
    INSERT INTO cog_metric_counter_totals (name, labels, value)
    SELECT t.name, t.labels::jsonb, SUM(t.value)
    FROM unnest($1::text[], $2::text[], $3::float8[]) AS t(name, labels, value)
    GROUP BY t.name, t.labels
    ON CONFLICT (name, labels)
    DO UPDATE SET value = cog_metric_counter_totals.value + EXCLUDED.value,
                  updated_at = NOW()
"#;

const HISTOGRAM_BUCKET_UPSERT_SQL: &str = r#"
    INSERT INTO cog_metric_histogram_buckets (name, labels, bucket, observations)
    SELECT t.name, t.labels::jsonb, t.bucket, SUM(t.observations)
    FROM unnest($1::text[], $2::text[], $3::int4[], $4::int8[]) AS t(name, labels, bucket, observations)
    GROUP BY t.name, t.labels, t.bucket
    ON CONFLICT (name, labels, bucket)
    DO UPDATE SET observations = cog_metric_histogram_buckets.observations + EXCLUDED.observations
"#;

const HISTOGRAM_SUM_UPSERT_SQL: &str = r#"
    INSERT INTO cog_metric_histogram_sums (name, labels, sum)
    SELECT t.name, t.labels::jsonb, SUM(t.value)
    FROM unnest($1::text[], $2::text[], $3::float8[]) AS t(name, labels, value)
    GROUP BY t.name, t.labels
    ON CONFLICT (name, labels)
    DO UPDATE SET sum = cog_metric_histogram_sums.sum + EXCLUDED.sum,
                  updated_at = NOW()
"#;

/// Write a batch of observations in one transaction.
///
/// One commit for the batch rather than one per observation is the whole point:
/// every commit waits for the WAL, and at the platform's write rate those waits
/// were what filled the pool and pushed every other statement behind them.
async fn write_batch(pool: &PgPool, batch: &[PendingWrite]) -> SFResult<()> {
    if batch.is_empty() {
        return Ok(());
    }

    let mut kinds = Vec::with_capacity(batch.len());
    let mut sample_names = Vec::with_capacity(batch.len());
    let mut sample_values = Vec::with_capacity(batch.len());
    let mut sample_labels = Vec::with_capacity(batch.len());
    let mut counters = Accumulated::default();
    let mut buckets = BucketAccumulated::default();
    let mut sums = Accumulated::default();

    for pending in batch {
        let name = pending.name();
        let labels = pending.labels();
        match pending {
            PendingWrite::Gauge { value, .. } => {
                kinds.push(MetricType::Gauge.as_str().to_string());
                sample_names.push(name.to_string());
                sample_values.push(*value);
                sample_labels.push(labels.to_string());
            }
            PendingWrite::Counter { value, .. } => {
                kinds.push(MetricType::Counter.as_str().to_string());
                sample_names.push(name.to_string());
                sample_values.push(*value);
                sample_labels.push(labels.to_string());
                counters.add(name, labels, *value);
            }
            PendingWrite::Histogram { value, bucket, .. } => {
                kinds.push(MetricType::Histogram.as_str().to_string());
                sample_names.push(name.to_string());
                sample_values.push(*value);
                sample_labels.push(labels.to_string());
                buckets.add(name, labels, *bucket);
                sums.add(name, labels, *value);
            }
        }
    }

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

    sqlx::query(SAMPLE_INSERT_SQL)
        .bind(&kinds)
        .bind(&sample_names)
        .bind(&sample_values)
        .bind(&sample_labels)
        .execute(&mut *tx)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

    if !counters.rows.is_empty() {
        let (names, labels, values) = counters.columns();
        sqlx::query(COUNTER_TOTAL_UPSERT_SQL)
            .bind(&names)
            .bind(&labels)
            .bind(&values)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
    }

    if !buckets.rows.is_empty() {
        let (names, labels, bucket_ids, observations) = buckets.columns();
        sqlx::query(HISTOGRAM_BUCKET_UPSERT_SQL)
            .bind(&names)
            .bind(&labels)
            .bind(&bucket_ids)
            .bind(&observations)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
    }

    if !sums.rows.is_empty() {
        let (names, labels, values) = sums.columns();
        sqlx::query(HISTOGRAM_SUM_UPSERT_SQL)
            .bind(&names)
            .bind(&labels)
            .bind(&values)
            .execute(&mut *tx)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
    }

    tx.commit()
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;
    Ok(())
}

/// 写一批，失败只记日志。
///
/// 指标不是事实的正本：一批写不进去不该让调用方以为自己的业务写入失败了，也不该
/// 让写者退出——退出的写者会把后面所有观测都变成「无人认领」，而数据库往往是
/// 短暂停顿而不是永久消失。日志里带上这一批的条数，否则读的人只知道「写失败
/// 过」，不知道丢了多少。
async fn write_batch_logged(pool: &PgPool, batch: &[PendingWrite]) {
    if let Err(e) = write_batch(pool, batch).await {
        tracing::warn!(
            observations = batch.len(),
            error = %e,
            "Failed to write a batch of metric observations"
        );
    }
}

/// 批量落库的写者：收够一批（或等够 linger）就写一次。
///
/// 一次写是一个事务；批里同一个序列的多条观测先求和（见 [`Accumulated`]），
/// 所以一条 upsert 不会撞上自己的第二行。队列满由写入方就地写单条兜住，这里
/// 不需要为满队列另开一条路；写失败只记日志——指标不是事实的正本，一条写不
/// 进去不该让调用方以为自己的业务写入失败了。
async fn run_metric_writer(pool: PgPool, mut rx: tokio::sync::mpsc::Receiver<Command>) {
    let linger = std::time::Duration::from_millis(METRIC_BATCH_LINGER_MS);
    let mut batch: Vec<PendingWrite> = Vec::with_capacity(METRIC_BATCH_MAX);

    loop {
        let Some(first) = rx.recv().await else {
            if !batch.is_empty() {
                write_batch_logged(&pool, &batch).await;
            }
            return;
        };

        let mut barrier = match first {
            Command::Write(pending) => {
                batch.push(pending);
                None
            }
            Command::Flush(ack) => {
                if !batch.is_empty() {
                    write_batch_logged(&pool, &batch).await;
                    batch.clear();
                }
                let _ = ack.send(());
                continue;
            }
        };

        let deadline = tokio::time::Instant::now() + linger;
        while batch.len() < METRIC_BATCH_MAX {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(Command::Write(pending))) => batch.push(pending),
                Ok(Some(Command::Flush(ack))) => {
                    barrier = Some(ack);
                    break;
                }
                // The senders are gone: this batch is the last thing to write.
                Ok(None) => break,
                // The linger expired with nothing else to say.
                Err(_) => break,
            }
        }

        write_batch_logged(&pool, &batch).await;
        batch.clear();

        if let Some(ack) = barrier {
            let _ = ack.send(());
        }
    }
}

#[async_trait]
impl MetricsBackend for PostgresMetricsBackend {
    async fn record_gauge(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.record(MetricType::Gauge, name.as_str(), value, &labels)
            .await
    }

    async fn record_counter(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        // Both the sample log and the running total are kept: the log answers
        // range queries, the total answers the scrape endpoint, and callers of
        // either must keep working. One observation is one pending write; the
        // two rows it produces are written together, in the same transaction
        // as every other observation of its batch.
        self.record(MetricType::Counter, name.as_str(), value, &labels)
            .await
    }

    async fn record_histogram(
        &self,
        name: MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) -> SFResult<()> {
        self.record(MetricType::Histogram, name.as_str(), value, &labels)
            .await
    }

    async fn query_gauge_latest(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        self.flush_pending().await?;
        // Newest row per label set: the statement walks the label sets through
        // the index and probes each one for its newest sample, so what it reads
        // is one row per series rather than the history behind it. The index
        // that carries both probes is created in `init_schema`, and
        // [`gauge_latest_sql`] is the statement the two are held together by.
        let rows: Vec<(f64, serde_json::Value, DateTime<Utc>)> =
            sqlx::query_as(&gauge_latest_sql())
                .bind(name)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| SFError::Database(e.to_string()))?;

        let mut samples = Vec::with_capacity(rows.len());
        for (value, labels_json, timestamp) in rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            samples.push(MetricSample {
                timestamp,
                value,
                labels,
            });
        }
        Ok(samples)
    }

    async fn query_gauge_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range(MetricType::Gauge, name, start, end).await
    }

    async fn query_counter_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range(MetricType::Counter, name, start, end)
            .await
    }

    async fn query_counter_totals(&self, name: &str) -> SFResult<Vec<MetricSample>> {
        self.flush_pending().await?;
        let rows: Vec<(serde_json::Value, f64, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT labels, value, updated_at
            FROM cog_metric_counter_totals
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut samples = Vec::with_capacity(rows.len());
        for (labels_json, value, timestamp) in rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            samples.push(MetricSample {
                timestamp,
                value,
                labels,
            });
        }
        Ok(samples)
    }

    async fn query_histogram_totals(&self, name: &str) -> SFResult<Vec<HistogramTotals>> {
        self.flush_pending().await?;
        let bounds = histogram_bucket_bounds(name);

        let bucket_rows: Vec<(serde_json::Value, i32, i64)> = sqlx::query_as(
            r#"
            SELECT labels, bucket, observations
            FROM cog_metric_histogram_buckets
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let sum_rows: Vec<(serde_json::Value, f64, DateTime<Utc>)> = sqlx::query_as(
            r#"
            SELECT labels, sum, updated_at
            FROM cog_metric_histogram_sums
            WHERE name = $1
            "#,
        )
        .bind(name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))?;

        let mut series: HashMap<String, HistogramTotals> = HashMap::new();

        for (labels_json, sum, updated_at) in sum_rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            let key = series_key(&labels);
            series.insert(
                key,
                HistogramTotals {
                    labels,
                    buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                    overflow: 0,
                    count: 0,
                    sum,
                    updated_at,
                },
            );
        }

        for (labels_json, bucket, observations) in bucket_rows {
            let labels: HashMap<String, String> =
                serde_json::from_value(labels_json).map_err(SFError::Serialization)?;
            let key = series_key(&labels);
            let observations = observations.max(0) as u64;
            let totals = series.entry(key).or_insert_with(|| HistogramTotals {
                labels,
                buckets: bounds.iter().map(|b| (*b, 0)).collect(),
                overflow: 0,
                count: 0,
                sum: 0.0,
                // A bucket row without a sums row is not a series this backend
                // can time; the sums row is written by the same call, so the
                // only way to be here is a partially written observation.
                updated_at: Utc::now(),
            });

            if let Some(slot) = totals.buckets.get_mut(bucket.max(0) as usize) {
                slot.1 = observations;
            } else {
                totals.overflow = observations;
            }
            totals.count += observations;
        }

        Ok(series.into_values().collect())
    }

    async fn query_histogram_range(
        &self,
        name: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> SFResult<Vec<MetricSample>> {
        self.query_range(MetricType::Histogram, name, start, end)
            .await
    }

    async fn list_metric_names(&self, metric_type: MetricType) -> SFResult<Vec<String>> {
        self.flush_pending().await?;
        // A name is listed from wherever its value is read, so that listing and
        // reading cannot disagree. Counters and histograms keep their current
        // value in their own accumulation tables, and reading the name out of
        // the sample log instead answers a different question: it offers names
        // whose accumulation row does not exist yet (the read that follows comes
        // back empty), and it drops a name whose log rows the capacity sweep
        // pruned while its accumulation — the thing actually served — is still
        // there. Only gauges are read through the log, because for a gauge the
        // log is the value.
        let rows: Vec<(String,)> = match metric_type {
            MetricType::Counter => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metric_counter_totals ORDER BY name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
            MetricType::Histogram => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metric_histogram_sums ORDER BY name",
            )
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
            _ => sqlx::query_as(
                "SELECT DISTINCT name FROM cog_metrics_samples WHERE metric_type = $1 ORDER BY name",
            )
            .bind(metric_type.as_str())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?,
        };
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    async fn health_check(&self) -> SFResult<()> {
        sqlx::query("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一批里同一个序列的多条观测必须先求和，再交给 upsert。
    ///
    /// `ON CONFLICT DO UPDATE` 拒绝一条语句动同一行两次（"cannot affect row a
    /// second time"），所以不先求和的批次不是慢，是整批写失败——而失败的那一批
    /// 正是最忙的时候攒下来的。
    #[test]
    fn a_batch_sums_one_series_before_the_upsert_sees_it() {
        let mut counters = Accumulated::default();
        counters.add("ops_total", "{\"k\":\"1\"}", 2.0);
        counters.add("ops_total", "{\"k\":\"1\"}", 3.0);
        counters.add("ops_total", "{\"k\":\"2\"}", 7.0);

        let (names, labels, values) = counters.columns();
        assert_eq!(names.len(), 2, "一个序列一行：{names:?}");
        assert_eq!(
            labels,
            vec!["{\"k\":\"1\"}".to_string(), "{\"k\":\"2\"}".to_string()]
        );
        assert_eq!(values, vec![5.0, 7.0], "同序列求和，异序列分开");
    }

    /// 桶按 (name, labels, bucket) 分，只按名字分会把不同桶折成一个。
    #[test]
    fn histogram_buckets_of_one_series_stay_apart() {
        let mut buckets = BucketAccumulated::default();
        buckets.add("latency", "{}", 3);
        buckets.add("latency", "{}", 3);
        buckets.add("latency", "{}", 4);

        let (names, _labels, ids, observations) = buckets.columns();
        assert_eq!(names.len(), 2);
        assert_eq!(ids, vec![3, 4]);
        assert_eq!(observations, vec![2, 1]);
    }

    /// 标签集的书写形式来自内容本身，所以同一个标签集在一批里只有一种拼写。
    /// 两处各搭一份 JSON 就成了两个序列，upsert 于是各加一次，总数对、序列数错。
    #[test]
    fn one_label_set_has_one_spelling_in_a_batch() {
        let mut counters = Accumulated::default();
        for _ in 0..8 {
            let labels: HashMap<String, String> = [("a", "1"), ("b", "2")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            counters.add(
                "probe",
                &PostgresMetricsBackend::labels_json(&labels).unwrap(),
                1.0,
            );
        }
        let (_, labels, values) = counters.columns();
        assert_eq!(labels.len(), 1, "一个标签集只能有一个拼写：{labels:?}");
        assert_eq!(values, vec![8.0]);
    }

    /// The kind word is filled, not copied, and nothing is left for the database
    /// to trip over: a template whose slot nobody fills is a parse error at the
    /// scrape, which is the moment a reader is least able to do without it.
    #[test]
    fn the_gauge_reading_is_rendered_from_the_declaration() {
        let sql = gauge_latest_sql();
        assert!(!sql.contains('{'), "槽没填完：{sql}");
        let gauge = MetricType::Gauge.as_str();
        assert_eq!(
            sql.matches(&format!("metric_type = '{gauge}'")).count(),
            3,
            "三处探针都要按 kind 选行：{sql}"
        );
        assert!(
            !sql.contains("'counter'") && !sql.contains("'histogram'"),
            "gauge 读数里出现了别的 kind：{sql}"
        );
    }

    /// 一个标签集的每一行都由它自己那份 map 承载，而迭代顺序是按实例取的，
    /// 与内容无关。键若取自 map 本身（或它的序列化文本），同一个序列就会按
    /// 排列数碎成多份，且每次抓取的碎法都不同——渲染端于是拿到半个序列的桶，
    /// 与另一个键下的 `_sum`，两边都对不上。内容相同必须得出同一个键。
    #[test]
    fn one_label_set_is_one_series_key_however_their_maps_iterate() {
        let mut keys = std::collections::HashSet::new();
        for _ in 0..32 {
            // 每轮重建一份 map：内容一样，迭代顺序各不同。
            let labels: HashMap<String, String> = [
                ("endpoint", "/api/v1/tasks"),
                ("method", "GET"),
                ("status", "200"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
            keys.insert(series_key(&labels));
        }
        assert_eq!(keys.len(), 1, "一个标签集只能有一个键：{keys:?}")
    }

    /// 分键用的是渲染端分组用的那个形式：两端若各有一套身份，「一个序列」在
    /// 存储端和抓取端就会是不同的东西。
    #[test]
    fn the_series_key_is_the_form_the_render_side_groups_by() {
        let labels: HashMap<String, String> = [("b", "2"), ("a", "1")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(series_key(&labels), "a=\"1\",b=\"2\"");
    }
}
