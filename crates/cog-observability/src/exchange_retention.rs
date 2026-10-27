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
use tracing::{info, warn};

use cog_core::metric_names;
use cog_core::{MetricsBackend, SFError, SFResult, ShutdownSignal};

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
pub const DEFAULT_KEEP_DAYS: i64 = 30;

/// The shortest retention this loop will accept.
///
/// One day. A zero or negative day count would erase the archive every pass,
/// which reads as "the retention works" while the table it exists to bound is
/// empty by construction — the promise to the reader is what has to survive a
/// misconfiguration, so the value falls back rather than being honoured.
pub const MIN_KEEP_DAYS: i64 = 1;

/// The environment variable that carries the retention.
pub const KEEP_DAYS_ENV: &str = "COGNEVA_LLM_EXCHANGE_KEEP_DAYS";

/// How often the loop looks for rows past the retention.
///
/// Five minutes: the readings this loop publishes are also what the alert rules
/// read, and Prometheus drops a series it has not seen for five minutes. A
/// shorter period would spend durable rows to restate a table whose age changes
/// by days; a longer one would let the published size go stale between scrapes.
pub const PERIOD: Duration = Duration::from_secs(300);

/// How many rows one statement may delete.
///
/// Two thousand rows of bodies is tens of megabytes, which bounds both the
/// statement's work and the locks it holds. The pas repeats until it finds
/// nothing past the cutoff, so the count of batches follows the backlog rather
/// than the batch size.
const BATCH_ROWS: i64 = 2_000;

/// What one pass did, so a caller can assert on it rather than infer it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionOutcome {
    /// Rows the archive held after the pass.
    pub held: i64,
    /// Rows this pass retired.
    pub removed: u64,
    /// The age of the oldest surviving row, in seconds; zero for an empty
    /// archive.
    pub oldest_age_secs: i64,
    /// The retention this pass enforced, in seconds.
    pub keep_secs: i64,
}

/// The retention sweep: the archive's table, the pool it lives on, and the age
/// this deployment promises its readers.
#[derive(Clone)]
pub struct ExchangeRetention {
    pool: PgPool,
    keep: Duration,
    table: String,
    metrics: Option<Arc<dyn MetricsBackend>>,
    deployment: String,
}

impl ExchangeRetention {
    pub fn new(pool: PgPool, keep: Duration) -> Self {
        Self {
            pool,
            keep,
            table: TABLE.to_string(),
            metrics: None,
            deployment: String::new(),
        }
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
            let pass = self.retire_once(|| beat.beat()).await;
            match pass {
                Ok(outcome) => {
                    if outcome.removed > 0 {
                        info!(
                            table = self.table,
                            rows = outcome.removed,
                            held = outcome.held,
                            "Retired LLM exchanges past the retention"
                        );
                    }
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

    /// Retire every row older than the retention, calling `progress` before
    /// each batch.
    ///
    /// The loop hands `beat.beat` in here: the work between two beats would
    /// otherwise be the whole pass, and a first run against an archive that
    /// grew while nothing retired it deletes for as long as the backlog lasts.
    /// That is exactly the shape that made a working sweep read as a stopped
    /// loop on the event log, so the bound published here is one batch.
    pub async fn retire_once(&self, progress: impl Fn()) -> SFResult<RetentionOutcome> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(self.keep).unwrap_or_else(|_| chrono::Duration::days(30));
        let mut removed = 0u64;
        loop {
            progress();
            let batch = sqlx::query(&format!(
                "DELETE FROM {table} WHERE id IN (
                     SELECT id FROM {table} WHERE ts < $2 ORDER BY ts LIMIT $1
                 )",
                table = quote_ident(&self.table),
            ))
            .bind(BATCH_ROWS)
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))?
            .rows_affected();
            removed += batch;
            if batch == 0 {
                break;
            }
        }

        let held = self.row_count().await?;
        let oldest_age_secs = self.oldest_age_secs().await?;
        Ok(RetentionOutcome {
            held,
            removed,
            oldest_age_secs,
            keep_secs: self.keep.as_secs() as i64,
        })
    }

    async fn row_count(&self) -> SFResult<i64> {
        sqlx::query(&format!(
            "SELECT count(*) AS n FROM {}",
            quote_ident(&self.table)
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
        .map(|row| row.get::<i64, _>("n"))
    }

    /// The age of the oldest row in the archive, in seconds.
    ///
    /// Zero for an empty archive, and that is the reading rather than an
    /// absence: a table nobody has written to is not a retention that has
    /// fallen behind, and a series that disappeared would make the rule
    /// comparing it to the retention go silent exactly while the archive is
    /// clean.
    async fn oldest_age_secs(&self) -> SFResult<i64> {
        sqlx::query(&format!(
            "SELECT EXTRACT(EPOCH FROM (NOW() - min(ts)))::bigint AS age FROM {}",
            quote_ident(&self.table)
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| SFError::Database(e.to_string()))
        .map(|row| row.get::<Option<i64>, _>("age").unwrap_or(0))
    }

    /// What the archive costs on disk, in bytes.
    async fn table_bytes(&self) -> SFResult<i64> {
        sqlx::query("SELECT pg_total_relation_size($1::regclass)::bigint AS bytes")
            .bind(&self.table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| SFError::Database(e.to_string()))
            .map(|row| row.get::<i64, _>("bytes"))
    }

    /// Publish what the archive holds, how far back it reaches, and what that
    /// costs on disk.
    async fn report(&self, outcome: &RetentionOutcome) {
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

/// Quote a table name, allowing a schema-qualified one.
///
/// The name is never a value the sweep takes from a caller at runtime — it is
/// this file's constant or a test's own table — but it is interpolated into the
/// statement, so the shape is checked here rather than trusted: anything outside
/// the identifier alphabet is dropped instead of being carried into the SQL.
fn quote_ident(name: &str) -> String {
    name.split('.')
        .map(|part| {
            let cleaned: String = part
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            format!("\"{cleaned}\"")
        })
        .collect::<Vec<_>>()
        .join(".")
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
pub fn keep_days_from(raw: Option<&str>) -> i64 {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|d| *d >= MIN_KEEP_DAYS)
        .unwrap_or(DEFAULT_KEEP_DAYS)
}

/// The deployment this process belongs to, as the platform names it.
///
/// The same production the capacity loops in `cog-storage` use, and read for
/// the same reason: it is the name the manifest already gives the workload, and
/// a pod name would be an identity that changes on every rollout.
pub fn deployment_name() -> String {
    std::env::var("COGNEVA_DEPLOYMENT_NAME").unwrap_or_default()
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
