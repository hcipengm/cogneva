//! Retires the prompt-and-response archive at the age this deployment promises.
//!
//! `gateway_llm_exchange` keeps the bodies of what the platform sent upstream
//! and what came back, so a later question about one call ("did the prompt we
//! re-sent match the one we paid for?") is answerable without buying it again.
//! The bodies are the only part of that ledger whose size is set by the caller
//! rather than by the call count, so the table is only bounded by its age
//! policy: 30 days is 307 MB on 2026-10-27, measured on this deployment, and it
//! grows with the size of what people paste into a prompt rather than with how
//! often they do it.
//!
//! Until this loop existed the age was enforced in one place: a delete at the
//! gateway's startup. That is a bound on how long a row lives only while the
//! gateway is rolled more often than the retention is wide, which is an
//! assumption about deployment habits and not something the table can state.
//! The loop runs in the main application instead — beside the usage rollup and
//! for the same reason: it must not be the process that restarts on every
//! rollout that decides what is old.
//!
//! Two readings make the promise checkable rather than merely declared. The age
//! of the oldest row is the retention seen from the reader's side: whatever the
//! policy says, this is how far back the archive actually reaches. The
//! retention this process enforces is published beside it so the rule compares
//! two readings rather than a constant written into the rules file — a
//! deployment that shortens the retention moves its own judgement with it.
//!
//! No lease guards the pass. Two replicas deleting rows past the same cutoff
//! take the same rows and the second finds nothing; correctness comes from the
//! deletion being a set defined by the clock, not from a single writer.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use sqlx::{PgPool, Row};
use tracing::warn;

use cog_core::metric_names;
use cog_core::retention::{keep_days_against_volume, RetentionReadings, UNIT_DAYS};
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

/// The registry key this loop's target answers to.
pub const KNOB: &str = "llmExchangeRetention.keepDays";

/// The table this loop retires rows from.
pub const TABLE: &str = "gateway_llm_exchange";

/// The loop, as the liveness readings name it.
pub const LOOP: &str = "observability_exchange_retention";

/// The label that names the deployment a reading below came from: the table is
/// shared, and a reading without a name is served by whichever process the
/// scrape happens to reach.
pub const DEPLOYMENT_LABEL: &str = "deployment";

/// How long a body is kept by default.
///
/// Thirty days: long enough to answer a dispute about a call made last month,
/// short enough that the archive is not the platform's largest table. It is a
/// default for the deployment that does not say otherwise, not a policy — a
/// deployment with stricter promises about what it holds shortens it, and the
/// readings move with it because the number is published rather than assumed.
// cap: bound — 对「存了些什么」许下更紧承诺的部署会缩短它，读数跟着动，因为那个数是发布出去的而不是假定的
pub const DEFAULT_KEEP_DAYS: i64 = 30;

/// The shortest retention this loop will accept.
///
/// One day. A zero or negative day count would erase the archive every pass,
/// which reads as "the retention works" while the table it exists to bound is
/// empty by construction — the promise to the reader is what has to survive a
/// misconfiguration, so the value falls back rather than being honoured.
// cap: bound — 承诺是许给读的人的，它必须扛过一次配置错误，所以低于它的值回退而不是被采纳
pub const MIN_KEEP_DAYS: i64 = 1;

/// The environment variable that carries the retention.
pub const KEEP_DAYS_ENV: &str = "COGNEVA_LLM_EXCHANGE_KEEP_DAYS";

/// How often the loop looks for rows past the retention.
///
/// Five minutes: the readings this loop publishes are also what the alert rules
/// read, and Prometheus drops a series it has not seen for five minutes. A
/// shorter period would spend durable rows to restate a table whose age changes
/// by days; a longer one would let the published size go stale between scrapes.
// cap: bound — 更短就为一门年龄以天变的表反复重述花掉持久行，更长就让发布出去的大小在两次抓取之间过期
pub const PERIOD: Duration = Duration::from_secs(300);

/// 两次采样之间，这张表每天涨多少行。
///
/// 纯函数：速率是这一族里唯一能被穷举测试的部分，把它从 `pass` 里拿出来，判据就不必
/// 连一个真库才问得出来。
///
/// 三条口径：
/// - **没有上一次采样 ⇒ `None`**：那是一次「量不到」，不是零。
/// - **两次采样同一时刻 ⇒ `None`**：除数不是正的，速率没有定义。
/// - **可以是负数**：表变小了（真空或搬走一部分）也要如实报，那是读数不是错误。
fn rows_per_day(
    previous: Option<(i64, std::time::Instant)>,
    now_held: i64,
    now: std::time::Instant,
) -> Option<f64> {
    let (rows_before, at_before) = previous?;
    let days = now.saturating_duration_since(at_before).as_secs_f64() / 86_400.0;
    (days > 0.0).then(|| (now_held - rows_before) as f64 / days)
}

/// What one pass did, so a caller can assert on it rather than infer it.
///
/// 不含 `Eq`：速率是浮点数，而 `Eq` 要求全序。判等用 `PartialEq`。
#[derive(Debug, Clone, PartialEq)]
pub struct RetentionOutcome {
    /// Rows the archive held after the pass.
    pub held: i64,
    /// Rows the archive gained per day, from the previous pass to this one.
    ///
    /// `None` on the first pass and whenever the previous one is too old or too
    /// close to divide by: a rate nobody measured is not a rate of zero, and a
    /// zero here would say the table is not growing while nothing had compared
    /// two readings yet.
    pub rows_per_day: Option<f64>,
    /// The age of the oldest surviving row, in seconds; zero for an empty
    /// archive.
    pub oldest_age_secs: i64,
    /// The retention this pass enforced, in seconds.
    pub keep_secs: i64,
    /// Whether `keep_secs` came from this beat's measurements rather than from
    /// the deployment's seed.
    pub keep_derived: bool,
}

/// The retention sweep: the archive's table, the pool it lives on, and the age
/// this deployment promises its readers.
#[derive(Clone)]
pub struct ExchangeRetention {
    pool: PgPool,
    /// The retention this deployment declared. It is what a process with no
    /// measured write volume keeps.
    keep: Duration,
    /// The declared size of the database volume; with it and the measured cost
    /// of a day's writes, every pass recomputes the retention.
    volume_bytes: Option<u64>,
    table: String,
    /// 上一次采样：(行数, 时刻)。速率要两次读数才成立，所以它跟着这个对象走。
    ///
    /// 用 `Mutex` 是因为 `pass` 拿的是 `&self`：读数是这一趟自己的事，不改变
    /// 「这个对象是什么」，而速率需要跨趟记忆。
    last_sample: Arc<std::sync::Mutex<Option<(i64, std::time::Instant)>>>,
    metrics: Option<Arc<dyn MetricsBackend>>,
    readings: Option<Arc<RetentionReadings>>,
    deployment: String,
    /// 正文的归档层。没有它就没有「已归档」这回事，于是这一遍**一行也不删**：
    /// 删一份只有本表持有、别处没有副本的正文，就是把那次调用从留底里抹掉。
    archive: Option<Arc<dyn cog_core::ObjectBackend>>,
}

impl ExchangeRetention {
    pub fn new(pool: PgPool, keep: Duration) -> Self {
        Self {
            pool,
            keep,
            volume_bytes: None,
            table: TABLE.to_string(),
            last_sample: Arc::new(std::sync::Mutex::new(None)),
            metrics: None,
            readings: None,
            deployment: String::new(),
            archive: None,
        }
    }

    /// The declared size of the volume this archive lives on.
    pub fn with_database_volume(mut self, bytes: Option<u64>) -> Self {
        self.volume_bytes = bytes;
        self
    }

    /// Attach the reading face that carries this loop's target and beat count.
    pub fn with_readings(mut self, readings: Arc<RetentionReadings>) -> Self {
        self.readings = Some(readings);
        self
    }

    /// 正文归档层：这一遍只删「副本已在归档里、且能读到」的行。
    pub fn with_archive(mut self, archive: Arc<dyn cog_core::ObjectBackend>) -> Self {
        self.archive = Some(archive);
        self
    }

    /// Point the sweep at another table. Only a test does this, and it is what
    /// lets the suite run against its own schema instead of the live archive.
    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = table.into();
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<dyn MetricsBackend>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Name the deployment these readings belong to.
    pub fn with_deployment(mut self, deployment: impl Into<String>) -> Self {
        self.deployment = deployment.into();
        self
    }

    /// The retention this sweep enforces.
    pub fn keep(&self) -> Duration {
        self.keep
    }

    /// Spawn the sweep as a supervised loop under this file's own name.
    pub fn spawn(self: Arc<Self>, shutdown: ShutdownSignal) -> tokio::task::JoinHandle<()> {
        cog_core::loop_health::spawn(
            LOOP,
            cog_core::loop_health::Cadence::Periodic(PERIOD),
            shutdown.clone(),
            move |beat| {
                let sweep = Arc::clone(&self);
                let shutdown = shutdown.clone();
                async move { sweep.run(beat, shutdown).await }
            },
        )
    }

    /// Sweep once immediately, then every [`PERIOD`], until shutdown.
    pub async fn run(&self, beat: cog_core::loop_health::Beat, shutdown: ShutdownSignal) {
        let mut interval = tokio::time::interval(PERIOD);
        // A pass that overran its period resumes from now rather than firing a
        // backlog of catch-ups: what it missed is found by the cutoff, not by
        // the timer.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            beat.beat();
            let pass = self.measure_once().await;
            match pass {
                Ok(outcome) => {
                    self.report(&outcome).await;
                    self.report_pass_failed(false).await;
                }
                Err(e) => {
                    // A failed pass leaves every value reading at its last
                    // value, and the rules keep trusting them while the
                    // companion clock is inside its age bound — so an archive
                    // that cannot be swept and one that is not growing look
                    // the same. This cell says the pass itself did not answer.
                    self.report_pass_failed(true).await;
                    warn!(table = self.table, error = %e, "LLM exchange retention pass failed");
                }
            }
            tokio::select! {
                _ = interval.tick() => {}
                _ = shutdown.wait() => break,
            }
        }
    }

    /// 读一次留底表：多少行、最老一行多老、占多少字节、这一拍声明的查询窗口。
    ///
    /// 一行都不删。从前这一遍要做的事情是「删掉过了保留期、而且正文在归档里读得回来
    /// 的那些行」——那条两段条件回答的是「删掉会不会丢掉唯一一份正文」。删界之后这个
    /// 问题不存在了：没有任何东西会删，于是「哪些行的正文没有可读副本」也不再是这个
    /// 循环要回答的问题（它是审计与备份演练的事，那边本来就逐表走一遍）。
    pub async fn measure_once(&self) -> SFResult<RetentionOutcome> {
        let oldest_before = self.oldest_age_secs().await?;
        let bytes = self
            .table_bytes()
            .await
            .ok()
            .map(|bytes| bytes.max(0) as u64);
        let (keep, keep_derived) = self.keep_for(oldest_before, bytes);
        let held = self.row_count().await?;
        // 速率：与上一次采样比。第一次没有上一次，所以是「量不到」，不是 0。
        let now = std::time::Instant::now();
        let rows_per_day = {
            let mut slot = self.last_sample.lock().map_err(|_| {
                SFError::Database("exchange retention: sample lock poisoned".into())
            })?;
            // 同一段算法只有一份：这里问 `rows_per_day`，不在调用点再写一遍。
            let rate = rows_per_day(*slot, held, now);
            *slot = Some((held, now));
            rate
        };
        let oldest_age_secs = self.oldest_age_secs().await?;
        Ok(RetentionOutcome {
            held,
            rows_per_day,
            oldest_age_secs,
            keep_secs: keep.as_secs() as i64,
            keep_derived,
        })
    }

    /// The archive's own size and the age of its oldest row give the daily
    /// write volume directly: what survived the last pass over the span it
    /// covers is what a day costs. The volume's declared share then buys a
    /// number of days; when any input is missing the deployment's seed stands.
    fn keep_for(&self, oldest_age_secs: i64, bytes: Option<u64>) -> (Duration, bool) {
        let (days, derived) = keep_days_against_volume(
            self.keep.as_secs() as i64 / 86_400,
            self.volume_bytes,
            oldest_age_secs,
            bytes,
        );
        (
            Duration::from_secs((days.max(0) as u64).saturating_mul(86_400)),
            derived,
        )
    }

    /// Rows in the archive, read with the same statement as the capacity sweeps'
    /// own count: the two land on the same panel, and a second spelling of "how
    /// many rows does this relation hold" is a second unit on that panel.
    async fn row_count(&self) -> SFResult<i64> {
        let sql = cog_core::sql::relation_rows_sql(&self.table);
        sqlx::query_scalar(&sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
    }

    /// The age of the oldest row in the archive, in seconds.
    ///
    /// Zero for an empty archive, and that is the reading rather than an
    /// absence: a table nobody has written to is not a retention that has
    /// fallen behind, and a series that disappeared would make the rule
    /// comparing it to the retention go silent exactly while the archive is
    /// clean.
    async fn oldest_age_secs(&self) -> SFResult<i64> {
        sqlx::query(&cog_core::sql::relation_oldest_age_sql(&self.table, "ts"))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
            .map(|row| row.get::<Option<i64>, _>("age").unwrap_or(0))
    }

    /// What the archive costs on disk, in bytes.
    ///
    /// The statement is the contract's, because the capacity sweeps measure the
    /// same thing on the same panel. It reads a missing relation as NULL rather
    /// than raising, so `None` here is a named answer: this table is not in the
    /// database. Reporting that as a bare database error would make "the
    /// archive has no size yet" and "the name does not resolve" the same
    /// reading.
    async fn table_bytes(&self) -> SFResult<i64> {
        let bytes: Option<i64> = sqlx::query_scalar(cog_core::sql::RELATION_BYTES_SQL)
            .bind(&self.table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?;
        bytes.ok_or_else(|| {
            SFError::Database(format!(
                "relation {} is not in the database, so its size was not read",
                self.table
            ))
        })
    }

    /// Publish what the archive holds, how far back it reaches, and what that
    /// costs on disk.
    async fn report(&self, outcome: &RetentionOutcome) {
        if let Some(ref readings) = self.readings {
            readings.evaluate(
                KNOB,
                outcome.keep_secs as f64 / 86_400.0,
                UNIT_DAYS,
                outcome.keep_derived,
            );
        }
        let Some(ref mb) = self.metrics else { return };
        let labels = self.labels();
        let mut pairs = vec![
            (metric_names::LLM_EXCHANGE_ROWS, outcome.held as f64),
            (
                metric_names::LLM_EXCHANGE_OLDEST_AGE_SECONDS,
                outcome.oldest_age_secs as f64,
            ),
            (
                metric_names::LLM_EXCHANGE_KEEP_SECONDS,
                outcome.keep_secs as f64,
            ),
        ];
        // 速率只在量得到的时候发布：量不到就不发，而不是发一个 0——一个 0 会被读成
        // 「表不再增长」，而那时还没有任何两次读数比过。
        if let Some(rate) = outcome.rows_per_day {
            pairs.push((metric_names::LLM_EXCHANGE_ROWS_PER_DAY, rate));
        }
        if let Ok(bytes) = self.table_bytes().await {
            pairs.push((metric_names::LLM_EXCHANGE_BYTES, bytes as f64));
        }
        for (name, value) in pairs {
            self.emit(mb, name, value, labels.clone()).await;
        }
    }

    async fn report_pass_failed(&self, failed: bool) {
        let Some(ref mb) = self.metrics else { return };
        self.emit(
            mb,
            metric_names::LLM_EXCHANGE_SWEEP_FAILED,
            if failed { 1.0 } else { 0.0 },
            self.labels(),
        )
        .await;
    }

    fn labels(&self) -> HashMap<String, String> {
        if self.deployment.is_empty() {
            return HashMap::new();
        }
        HashMap::from([(DEPLOYMENT_LABEL.to_string(), self.deployment.clone())])
    }

    async fn emit(
        &self,
        mb: &Arc<dyn MetricsBackend>,
        name: cog_core::MetricName,
        value: f64,
        labels: HashMap<String, String>,
    ) {
        if let Err(e) = mb.record_gauge(name, value, labels).await {
            warn!(error = %e, metric = %name, "LLM exchange retention gauge emit failed");
        }
    }
}

/// The retention this process was told, or the default.
///
/// A value that cannot be read, or that is below [`MIN_KEEP_DAYS`], falls back
/// rather than being honoured: shortening the archive to nothing turns the
/// table into one that is trivially bounded and the promise to its readers into
/// one that is trivially broken.
pub fn keep_days_from_env() -> i64 {
    keep_days_from(std::env::var(KEEP_DAYS_ENV).ok().as_deref())
}

/// See [`keep_days_from_env`].
///
/// Zero falls back to the default rather than meaning "stop sweeping": a
/// zero-day retention would empty the archive every pass, and an empty table
/// reads the same as one the policy is keeping bounded. There is no reading in
/// which "keep nothing" is what a deployment meant by `0`.
pub fn keep_days_from(raw: Option<&str>) -> i64 {
    cog_core::retention::keep_days_or_default(raw, DEFAULT_KEEP_DAYS, MIN_KEEP_DAYS)
}

/// The deployment this process belongs to, as the platform names it.
///
/// The same production the capacity loops in `cog-storage` use, and read for
/// the same reason: it is the name the manifest already gives the workload, and
/// a pod name would be an identity that changes on every rollout.
pub fn deployment_name() -> String {
    std::env::var(cog_core::env::DEPLOYMENT_NAME_ENV).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_retention_that_cannot_be_read_falls_back() {
        assert_eq!(keep_days_from(None), DEFAULT_KEEP_DAYS);
        assert_eq!(keep_days_from(Some("nope")), DEFAULT_KEEP_DAYS);
        assert_eq!(
            keep_days_from(Some("0")),
            DEFAULT_KEEP_DAYS,
            "a zero-day retention would empty the archive every pass"
        );
        assert_eq!(keep_days_from(Some("-3")), DEFAULT_KEEP_DAYS);
        assert_eq!(
            keep_days_from(Some("7")),
            MIN_KEEP_DAYS.max(7),
            "a deployment that says seven days means seven"
        );
    }

    /// 保留天数是当拍实测的函数；量不到时 seed 顶着。
    #[tokio::test]
    async fn the_retention_follows_the_measured_daily_write() {
        let sweep = ExchangeRetention::new(
            PgPool::connect_lazy("postgres://example").expect("a lazy pool"),
            Duration::from_secs(30 * 86_400),
        )
        .with_database_volume(Some(10 * 1024u64.pow(3)));
        assert_eq!(
            sweep.keep_for(10 * 86_400, Some(512 * 1024 * 1024)),
            (Duration::from_secs(10 * 86_400), true)
        );
        assert_eq!(
            sweep.keep_for(10 * 86_400, Some(1024 * 1024 * 1024)),
            (Duration::from_secs(5 * 86_400), true)
        );
        assert_eq!(
            sweep.keep_for(0, Some(1024)),
            (Duration::from_secs(30 * 86_400), false)
        );
        assert_eq!(
            sweep.keep_for(10 * 86_400, None),
            (Duration::from_secs(30 * 86_400), false)
        );
    }

    /// 目标不变的一拍仍然是一拍：求值计数往上走，目标以天为单位登记。
    #[tokio::test]
    async fn a_beat_whose_target_does_not_move_still_counts() {
        let readings = Arc::new(RetentionReadings::new());
        let sweep = ExchangeRetention::new(
            PgPool::connect_lazy("postgres://example").expect("a lazy pool"),
            Duration::from_secs(30 * 86_400),
        )
        .with_readings(Arc::clone(&readings));
        let outcome = RetentionOutcome {
            rows_per_day: None,
            held: 100,
            oldest_age_secs: 86_400,
            keep_secs: 7 * 86_400,
            keep_derived: true,
        };
        for expected in 1..=2 {
            sweep.report(&outcome).await;
            let (target, beats, derived) = readings.reading(KNOB).expect("a beat");
            assert_eq!(target, 7.0);
            assert_eq!(beats, expected, "a steady target is still a beat");
            assert_eq!(readings.evaluations(KNOB), expected);
            assert!(derived);
        }
    }

    #[tokio::test]
    async fn the_sweep_names_the_size_and_the_retention_it_enforces() {
        let sweep = ExchangeRetention::new(
            PgPool::connect_lazy("postgres://example").expect("a lazy pool"),
            Duration::from_secs(7 * 86_400),
        );
        assert_eq!(sweep.keep(), Duration::from_secs(7 * 86_400));
        assert_eq!(sweep.table, TABLE);
    }
}

#[cfg(test)]
mod rate_tests {
    use super::rows_per_day;
    use std::time::{Duration, Instant};

    /// 一天的差：涨了多少行，速率就是多少。
    #[test]
    fn a_day_of_growth_is_the_rate() {
        let t0 = Instant::now();
        let rate = rows_per_day(Some((100, t0)), 300, t0 + Duration::from_secs(86_400));
        assert_eq!(rate, Some(200.0));
    }

    /// 半天涨 100 行：每天 200 行。**换输入值，答案跟着变**——这是「derived」那条判据。
    #[test]
    fn halving_the_interval_doubles_the_rate() {
        let t0 = Instant::now();
        let rate = rows_per_day(Some((100, t0)), 200, t0 + Duration::from_secs(43_200));
        assert_eq!(rate, Some(200.0));
    }

    /// 没有上一次采样：量不到，不是零。
    #[test]
    fn a_first_pass_has_no_rate() {
        assert_eq!(rows_per_day(None, 500, Instant::now()), None);
    }

    /// 同一时刻两次采样：除数不是正的。
    #[test]
    fn two_samples_at_one_instant_have_no_rate() {
        let t0 = Instant::now();
        assert_eq!(rows_per_day(Some((10, t0)), 20, t0), None);
    }

    /// 表变小了也如实报：负数是读数，不是错误。
    #[test]
    fn a_shrinking_table_reports_a_negative_rate() {
        let t0 = Instant::now();
        let rate = rows_per_day(Some((300, t0)), 100, t0 + Duration::from_secs(86_400));
        assert_eq!(rate, Some(-200.0));
    }
}
