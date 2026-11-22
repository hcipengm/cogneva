//! principle: feedback_a_bound_reading_is_not_the_population_it_governs
//! 指标样本日志的规模是一条**读数**，不是一个到点执行的删除循环。
//!
//! `cog_metrics_samples` 是 append-only 的：一次观察一行。从前的做法是给它一个行数上限，
//! 超了就从最老的开始删——这条路上每一格都试过了（租约选一个副本删、按填充速率自适应
//! 调整扫的节拍、给每个 gauge 系列留最新一行当地板、删不动就说「触底」），而它们治的
//! 是同一个病：**删除是一种判断，而这里没有人做那个判断**。
//!
//! 三个问题在删除循环里都答不上来：
//!
//! 1. 删谁？按行龄删会删掉正在被读的，留着没人问的；按行数删要先知道谁在增长，而增长
//!    本身就是读数。地板（每个 gauge 留最新一行）承认了这一点：它承认「删到什么程度会
//!    让读数消失」，却只能为 gauge 补一格，counter/histogram 的当前值根本不在这张表里。
//! 2. 谁删？多个副本共享一个库，谁都可能删，于是要有租约；租约要有属主、要有续期、
//!    要有「持有者死了」的判定——一整套只为了让一件事成立：恰好一个人删。
//! 3. 删得对不对？删掉的字节没有任何地方留痕：事后问「上个月的某个读数还在不在」，
//!    答案只能是「按当时的界算应该不在了」，那是推断，不是读数。
//!
//! 所以这一族改成**只测量、不删除**：每一拍把「现在有多少行、字节多少、声明的容量是多少、
//! 超了没有」四个读数一起发出去。超了就是超了——让那个数在面板上红着，由人去决定是抬容量、
//! 是给这个序列换一种记法、还是承认它就该长。这不是把问题往后推：读数带标签、带时间戳，
//! 「超了多久、超了多少」是能被追着看的，而删除循环给出的只是「已经处理过了」。
//!
//! 退役（[`crate::MetricsRetirement`]）仍然挂在这个循环的节拍上，但它不是容量那一半：
//! 退役判的是「这个名字不再有生产者」，与日志有多满无关，所以它的形状是「按名字搬走」，
//! 不是「按时间或行数删掉」。
//!
//! 容量本身仍是部署声明的一个数（chart 的 `sample_max_rows`），语义是「这个部署声明容忍
//! 它长到多少」；`0` 表示这个部署不声明容量，读数里 `budget` 那一格就不发——不声明与
//! 声明为 0 在读数上必须分得开。

use std::collections::HashMap;
use std::sync::Arc;

use std::time::Duration;

use sqlx::PgPool;
use tracing::warn;

use cog_core::{MetricsBackend, SFResult, ShutdownSignal};

/// The append-only sample log written by [`crate::PostgresMetricsBackend`].
pub const SAMPLES_TABLE: &str = "cog_metrics_samples";

/// The supervised loop this module registers under. Unchanged from the pruning
/// era on purpose: it names the same loop in `loop_health`, and a rename would
/// start a second series that says nothing about the first.
pub const LOOP: &str = "storage_metrics_sample_cap";

/// The label naming which deployment declared the capacity it reports.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// How often the log is measured.
///
/// 一分钟是五分钟这个**约定**的五分之一：Prometheus 的默认 lookback delta 是
/// 五分钟，超过它这一条序列就从抓取面上消失——不是变成旧值，是不存在。所以一个
/// 「现在有多大」的读数必须比那个窗口刷新得更勤，留出余量给一次迟到的抓取。这个数
/// 不是谁挑的节拍，它就是那条约定除以一个整数；换算成界写在下面一行。
// cap: fixed — Prometheus 的默认 lookback delta = 5 分钟，读数须在其内至少刷新一次；这里取 1/5
pub const LOOP_PERIOD: Duration = Duration::from_secs(60);

/// 读一次日志得到的那几个数。**一个都不来自删除**。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SampleReading {
    /// Rows the log holds.
    pub held: i64,
    /// The capacity this deployment declares, or `None` when it declares none.
    ///
    /// `None` and `Some(0)` are different statements and the readings keep them
    /// apart: nobody declared a capacity here, versus somebody declared zero.
    pub budget: Option<i64>,
    /// Bytes the table occupies, when the size could be read.
    pub bytes: Option<i64>,
    /// `held > budget`, decided only when there is a budget to compare to.
    ///
    /// Published rather than left to the reader: the two numbers it is made of
    /// are published beside it, and a reader that has to compute it can get the
    /// comparison backwards without anything noticing.
    pub over_capacity: bool,
}

impl SampleReading {
    /// The same reading with what the table costs on disk attached.
    fn with_bytes(mut self, bytes: Option<i64>) -> Self {
        self.bytes = bytes;
        self
    }
}

/// Measures the metrics sample log. Removes nothing from it.
pub struct MetricsSampleLog {
    pool: PgPool,
    table: String,
    /// The declared capacity; `0` means this deployment declares none.
    budget: i64,
    metrics: Option<Arc<dyn MetricsBackend>>,
    retirement: Option<Arc<crate::metrics_retirement::RetirementPass>>,
    /// The deployment this process is, for the readings below. `None` publishes
    /// them unlabelled, which is what a process whose platform never told it
    /// which deployment it belongs to can honestly do.
    deployment: Option<String>,
}

impl MetricsSampleLog {
    /// `budget` is the row capacity this deployment declares; `0` declares none.
    pub fn new(pool: PgPool, budget: u64) -> Self {
        Self {
            pool,
            table: SAMPLES_TABLE.to_string(),
            budget: budget as i64,
            metrics: None,
            retirement: None,
            deployment: None,
        }
    }

    /// Name this deployment on the readings this loop publishes.
    ///
    /// Every one of them is a claim made by one deployment — the capacity it
    /// declares for a log every deployment writes to — and they land in a log
    /// every deployment shares. Without a name they are served by whichever
    /// process a reader scrapes as though the value had come from there, which
    /// is how a deployment that declares no budget at all comes to answer with
    /// the one the deployment beside it declared.
    ///
    /// A blank name is treated as none: an empty label value is a series of its
    /// own that says nothing, and it would split every reader's series set
    /// without answering the question the label was added for.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        let deployment = deployment.into();
        self.deployment = (!deployment.trim().is_empty()).then_some(deployment);
        self
    }

    /// Spawn the measuring loop under this module's own name.
    ///
    /// Registered from here rather than by the caller so the name the reading
    /// reports and the name the loop is declared under cannot become two
    /// spellings of the same loop.
    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            LOOP,
            cog_core::loop_health::Cadence::Periodic(LOOP_PERIOD),
            shutdown.clone(),
            move |beat| {
                let log = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move { log.run(beat, shutdown).await }
            },
        )
    }

    /// Measure a table other than the live one. Exists so a test can point the
    /// loop at a throwaway table.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    /// Run the retirement pass every cycle.
    ///
    /// The release is attached here rather than given a loop of its own because
    /// this loop is the only thing that visits the metric store on a period, and
    /// a second period would be a second constant to keep right. Being attached
    /// does not make it conditional on the capacity: a retired name's rows have
    /// to go whether or not any deployment ever declares a capacity.
    pub fn with_retirement(
        mut self,
        retirement: Arc<crate::metrics_retirement::RetirementPass>,
    ) -> Self {
        self.retirement = Some(retirement);
        self
    }

    /// Attach a metrics backend so each pass publishes the size it read. Without
    /// it the loop still measures; the log simply has no published size.
    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Measure once immediately, then once per [`LOOP_PERIOD`], until shutdown.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        loop {
            beat.beat();
            // Before the measurement, so a retired name's rows are already gone
            // when the size is read; otherwise the reading counts rows the
            // release just decided nobody will ever read again.
            if let Some(retirement) = self.retirement.clone() {
                retirement.run_once().await;
            }
            match self.measure_once().await {
                Ok(reading) => {
                    if reading.over_capacity {
                        warn!(
                            table = self.table,
                            held = reading.held,
                            budget = reading.budget,
                            "Metrics sample log is over the capacity this deployment declares; \
                             nothing deletes it, so this reading stays red until somebody \
                             decides what to do with it"
                        );
                    }
                    self.report(&reading).await;
                    self.report_pass_outcome(false).await;
                }
                Err(e) => {
                    // A failed pass leaves all four readings frozen, and the
                    // capacity rules trust them inside their companion-clock
                    // age — a log that stopped being measured and one that
                    // stopped growing then read the same. This cell is that
                    // fact's own reading.
                    self.report_pass_outcome(true).await;
                    warn!(table = self.table, error = %e, "Metrics sample log measurement failed")
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(LOOP_PERIOD) => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// Read the log: how many rows, what it costs, what this deployment declared.
    ///
    /// The bytes are read separately and their absence is carried as `None`
    /// rather than as a zero: a size that could not be read and a table that
    /// occupies nothing are different facts, and only one of them is good news.
    pub async fn measure_once(&self) -> SFResult<SampleReading> {
        let held = self.row_count().await?;
        let budget = (self.budget > 0).then_some(self.budget);
        let bytes = match crate::relation_bytes::relation_bytes(&self.pool, &self.table).await {
            Ok(bytes) => Some(bytes),
            Err(e) => {
                warn!(table = self.table, error = %e, "sample log size unavailable");
                None
            }
        };
        Ok(SampleReading {
            held,
            budget,
            bytes: None,
            over_capacity: budget.is_some_and(|budget| held > budget),
        }
        .with_bytes(bytes))
    }

    /// 发布「这一趟有没有量到」。
    ///
    /// 行数、容量、超没超、字节四个读数只在 `Ok` 分支盖章，所以一趟量失败的 pass 会让
    /// 它们全停在上一笔值上，而容量规则按伴生钟年龄在界内还信它们——量不动与不再增长
    /// 于是在读数上同形。这一格是那件事自己的读数：量到记 0、取不到记 1，每一趟都写。
    async fn report_pass_outcome(&self, failed: bool) {
        let Some(ref mb) = self.metrics else { return };
        publish_pass_outcome(mb, &self.labels(), failed).await;
    }

    /// Publish what the log holds, what it is declared to be allowed to hold, and
    /// what it costs on disk, so its growth is visible from the scrape instead of
    /// only through a query against the database.
    async fn report(&self, reading: &SampleReading) {
        let Some(ref mb) = self.metrics else { return };

        self.emit(
            mb,
            cog_core::metric_names::METRICS_SAMPLES_ROWS,
            reading.held as f64,
        )
        .await;
        if let Some(budget) = reading.budget {
            self.emit(
                mb,
                cog_core::metric_names::METRICS_SAMPLES_BUDGET_ROWS,
                budget as f64,
            )
            .await;
            // The verdict is published under the same condition as the capacity
            // because it is the same claim: a deployment that declares no
            // capacity has none to be over, and a zero written from here would
            // say "this log is fine" on behalf of a process that declared
            // nothing.
            self.emit(
                mb,
                cog_core::metric_names::METRICS_SAMPLES_OVER_CAPACITY,
                reading.over_capacity as u8 as f64,
            )
            .await;
        }
        if let Some(bytes) = reading.bytes {
            self.emit(
                mb,
                cog_core::metric_names::METRICS_SAMPLES_BYTES,
                bytes as f64,
            )
            .await;
        }
    }

    /// The labels every reading this loop publishes carries.
    ///
    /// One place, because the readings are read beside each other and a reader
    /// that has to tell "this deployment declared nothing" from "another
    /// deployment declared something" needs them to agree on who is speaking.
    fn labels(&self) -> HashMap<String, String> {
        match self.deployment {
            Some(ref deployment) => {
                HashMap::from([(DEPLOYMENT_LABEL.to_string(), deployment.clone())])
            }
            None => HashMap::new(),
        }
    }

    async fn emit(&self, mb: &Arc<dyn MetricsBackend>, name: cog_core::MetricName, value: f64) {
        if let Err(e) = mb.record_gauge(name, value, self.labels()).await {
            warn!(error = %e, metric = %name, "metrics sample log gauge emit failed");
        }
    }

    async fn row_count(&self) -> SFResult<i64> {
        crate::relation_rows::relation_rows(&self.pool, &self.table).await
    }
}

/// Publish whether a measuring pass returned a result, under the same labels as
/// the readings that pass produced.
///
/// Free rather than a method so a test can drive it against a recording backend
/// and assert both outcomes without standing up a pool.
async fn publish_pass_outcome(
    metrics: &Arc<dyn MetricsBackend>,
    labels: &HashMap<String, String>,
    failed: bool,
) {
    let value = if failed { 1.0 } else { 0.0 };
    if let Err(e) = metrics
        .record_gauge(
            cog_core::metric_names::METRICS_SAMPLE_SWEEP_FAILED,
            value,
            labels.clone(),
        )
        .await
    {
        warn!(error = %e, "metrics sample sweep outcome emit failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reading(held: i64, budget: Option<i64>) -> SampleReading {
        SampleReading {
            held,
            budget,
            bytes: None,
            over_capacity: budget.is_some_and(|budget| held > budget),
        }
    }

    /// A log past the capacity it declares reads as over it; one at the capacity
    /// does not. The boundary is the whole reading: off by one here and a
    /// dashboard says "over" for a log that is exactly at its limit.
    #[test]
    fn over_capacity_is_strictly_above_the_declared_capacity() {
        assert!(!reading(10, Some(10)).over_capacity);
        assert!(reading(11, Some(10)).over_capacity);
        assert!(!reading(9, Some(10)).over_capacity);
    }

    /// A deployment that declares no capacity is not over one.
    ///
    /// This is the reading that keeps "nobody declared a limit" from being
    /// reported as "the limit is zero, you are over it" — which is how an
    /// operator who never set the knob gets paged about it.
    #[test]
    fn no_declared_capacity_is_not_a_capacity_of_zero() {
        assert!(!reading(5, None).over_capacity);
        assert!(!reading(5, Some(5)).over_capacity);
        assert!(reading(5, Some(4)).over_capacity);
    }

    /// 读回一段时间里某一格 gauge 的样本：(名字, 值, 标签)。
    async fn published(
        metrics: &Arc<crate::MemoryMetricsBackend>,
        name: &str,
    ) -> Vec<(String, f64, HashMap<String, String>)> {
        let end = chrono::Utc::now() + chrono::Duration::seconds(1);
        let start = end - chrono::Duration::seconds(60);
        metrics
            .query_gauge_range(name, start, end)
            .await
            .expect("the in-memory face is readable")
            .into_iter()
            .map(|sample| (name.to_string(), sample.value, sample.labels))
            .collect()
    }

    /// The four readings are published every pass, from one measurement.
    #[tokio::test]
    async fn every_reading_of_a_pass_is_written_from_the_same_measurement() {
        let metrics = Arc::new(crate::MemoryMetricsBackend::new());
        let mb: Arc<dyn MetricsBackend> = metrics.clone();
        let mut log = MetricsSampleLog::new(PgPool::connect_lazy("postgres://x/y").unwrap(), 100)
            .with_deployment("d1");
        log.metrics = Some(mb);
        log.report(&reading(150, Some(100))).await;

        let rows = published(
            &metrics,
            cog_core::metric_names::METRICS_SAMPLES_ROWS.as_str(),
        )
        .await;
        assert_eq!(
            rows.len(),
            1,
            "the size is published once per pass: {rows:?}"
        );
        assert_eq!(rows[0].1, 150.0);
        assert_eq!(
            rows[0].2.get(DEPLOYMENT_LABEL).map(String::as_str),
            Some("d1"),
            "a reading that does not say which deployment declared the capacity is served by \
             whichever deployment the scraper happens to hit"
        );
        let budget = published(
            &metrics,
            cog_core::metric_names::METRICS_SAMPLES_BUDGET_ROWS.as_str(),
        )
        .await;
        assert_eq!(budget.len(), 1);
        assert_eq!(budget[0].1, 100.0);
        let over = published(
            &metrics,
            cog_core::metric_names::METRICS_SAMPLES_OVER_CAPACITY.as_str(),
        )
        .await;
        assert_eq!(over.len(), 1);
        assert_eq!(over[0].1, 1.0, "150 rows against a declared 100 is over");
    }

    /// A deployment that declares no capacity publishes no capacity and no
    /// verdict — silence, not zeros.
    #[tokio::test]
    async fn a_deployment_that_declares_nothing_publishes_no_capacity() {
        let metrics = Arc::new(crate::MemoryMetricsBackend::new());
        let mb: Arc<dyn MetricsBackend> = metrics.clone();
        let mut log = MetricsSampleLog::new(PgPool::connect_lazy("postgres://x/y").unwrap(), 0);
        log.metrics = Some(mb);
        log.report(&reading(10, None)).await;

        assert_eq!(
            published(
                &metrics,
                cog_core::metric_names::METRICS_SAMPLES_ROWS.as_str()
            )
            .await
            .len(),
            1,
            "the size is still a reading when nobody declared a capacity"
        );
        assert!(
            published(
                &metrics,
                cog_core::metric_names::METRICS_SAMPLES_BUDGET_ROWS.as_str()
            )
            .await
            .is_empty(),
            "a deployment that declared nothing published a capacity"
        );
        assert!(
            published(
                &metrics,
                cog_core::metric_names::METRICS_SAMPLES_OVER_CAPACITY.as_str()
            )
            .await
            .is_empty(),
            "a deployment that declared nothing published a verdict"
        );
    }

    /// A pass that could not measure says so, on its own gauge.
    ///
    /// The four readings keep their last value when a pass fails, so this cell is
    /// the only thing that tells a frozen reading from a quiet log.
    #[tokio::test]
    async fn a_pass_that_could_not_measure_says_so_on_both_branches() {
        let metrics = Arc::new(crate::MemoryMetricsBackend::new());
        let mb: Arc<dyn MetricsBackend> = metrics.clone();
        let labels = HashMap::from([(DEPLOYMENT_LABEL.to_string(), "d1".to_string())]);
        publish_pass_outcome(&mb, &labels, false).await;
        publish_pass_outcome(&mb, &labels, true).await;

        let values: Vec<f64> = published(
            &metrics,
            cog_core::metric_names::METRICS_SAMPLE_SWEEP_FAILED.as_str(),
        )
        .await
        .into_iter()
        .map(|(_, value, _)| value)
        .collect();
        assert_eq!(
            values,
            vec![0.0, 1.0],
            "both branches have to be written, or a pass that never returned reads as one that \
             returned nothing to report"
        );
    }

    /// Nothing in this module deletes. The reading is the whole mechanism.
    ///
    /// This is the shape of the change and the thing that must not come back: a
    /// capacity that deletes what it measures cannot be read as a reading, because
    /// the number it reports has already been acted on by the time anybody sees it.
    #[test]
    fn the_module_carries_no_delete_statement() {
        let source = include_str!("metrics_sample_cap.rs");
        // The word is assembled rather than written, so this assertion does not
        // match itself — it lives in this same file.
        let forbidden = ["DE", "LETE FROM"].join("");
        assert!(
            !source.to_uppercase().contains(&forbidden),
            "a delete statement came back into the measuring loop"
        );
    }
}
