//! The host producers that feed the platform's accounts, and when each of them
//! last delivered.
//!
//! A producer is a host process on a timer: it renders a reading of the host's
//! corpus and pushes it into an intake table, which the account's own resident
//! loop folds in. The push is idempotent per row, so a cycle that finds nothing
//! new writes nothing -- and that is the whole difficulty. Every trace the
//! account keeps is a trace of *content that arrived*: a row's `first_seen_at`
//! dates the first time that principle was ever described, not the last time
//! the producer ran. A producer that stopped yesterday and one that is running
//! exactly as designed over an unchanged corpus leave the same account, the same
//! queue depth, and the same counts.
//!
//! So the producer says so itself. Once per successful cycle it writes one row
//! here, whether or not that cycle had anything new to queue, and this module
//! serves that row to Prometheus. Three decisions are load-bearing:
//!
//! - **The moment is the platform's own `now()`, stamped on receipt**, not a
//!   timestamp the host computes and sends. The age a rule reads is then the
//!   difference between two readings of one clock; a host whose clock drifts
//!   would otherwise make the producer look late (or early) by exactly that
//!   drift, and the drift is invisible in the reading it corrupts.
//! - **The period is the producer's declaration, not a constant here.** It
//!   arrives on the same row, measured from the timer that drives the producer,
//!   so a schedule changed on the host moves the alert's threshold with it. A
//!   period written into this repository instead would keep firing on the old
//!   cadence -- the failure mode is silence, not noise: the rule waits longer
//!   than the producer is allowed to take and reports a stopped producer as a
//!   healthy one.
//! - **Nothing is published before the first delivery.** A reading that has
//!   never been taken is not a reading of zero; serving the epoch here would
//!   say the producer last delivered in 1970. The rule that catches the
//!   never-delivered state reads the absence itself.
//!
//! The reading is taken at scrape time rather than published once per pass by
//! the account's resident loop, and that is the point of it: what this measures
//! is whether the *host* is still feeding the platform, and a loop that
//! published its own view of that would go quiet with the loop instead of with
//! the producer. The loop's own liveness is a separate reading
//! (`cogneva_loop_tick_age_seconds`), and the two are meant to be read together:
//! a fresh push with a stalled loop is a backlog, and a stalled producer with a
//! fresh loop is an account nobody is feeding.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};

use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{SFError, SFResult};

/// The table the heartbeats are kept in: one row per producer, rewritten in
/// place.
///
/// One row per producer rather than one per delivery: this holds the *latest*
/// delivery, and a row per delivery would make the reading a query over an
/// append-only log that grows for as long as the deployment lives. What a
/// history would answer -- how often the gaps happen -- is answered by the
/// alert's own series, which Prometheus already keeps.
pub const HEARTBEAT_TABLE: &str = "principle_producer_pushes";

/// The producer this reading is about, as it names itself in the pushed row.
///
/// The source is a column rather than an assumption because the row is written
/// by the producer, not by this crate: a second producer of the same account
/// needs no change here.
pub const PRINCIPLE_PRODUCER_SOURCE: &str = "principle-ledger-tsv.sh";

/// When the named producer last completed a delivery, as the platform's clock
/// read at the moment it was received.
pub const PRODUCER_LAST_PUSH_TIMESTAMP: &str =
    "cogneva_principle_producer_last_push_timestamp_seconds";

/// How often the named producer intends to deliver, as it declared on its last
/// delivery.
///
/// A separate series rather than a label on the one above: it is the operand
/// the alert's threshold is computed from, and a reader has to be able to see
/// the number the judgement was made against.
pub const PRODUCER_PUSH_PERIOD: &str = "cogneva_principle_producer_push_period_seconds";

/// The row as the reader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerHeartbeat {
    pub source: String,
    pub pushed_at: DateTime<Utc>,
    pub period_seconds: i64,
}

/// The producer heartbeats, read from the platform's own database.
pub struct ProducerHeartbeatStore {
    pool: PgPool,
}

impl ProducerHeartbeatStore {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let pool = PgPool::connect(database_url).await?;
        Ok(Self { pool })
    }

    /// Create the table this reads.
    ///
    /// This process is not the writer -- the producer is, from the host -- and it
    /// creates the table anyway, for the same reason the LLM usage store does:
    /// the reader would otherwise work only on a database the producer had
    /// already run against, and a reading that silently has nothing to read on a
    /// fresh deployment is indistinguishable from a producer that never ran.
    /// `IF NOT EXISTS` is what makes two creators agree rather than race.
    pub async fn init_schema(&self) -> anyhow::Result<()> {
        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
                source         TEXT PRIMARY KEY,
                pushed_at      TIMESTAMPTZ NOT NULL,
                period_seconds BIGINT NOT NULL
            )
            "#
        ))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The latest delivery of one producer, or `None` if it has never delivered.
    ///
    /// `None` and a row from long ago are different findings and are not
    /// collapsed: the first is a producer that has never run, the second one
    /// that ran and stopped.
    pub async fn latest(&self, source: &str) -> SFResult<Option<ProducerHeartbeat>> {
        let row = sqlx::query(&format!(
            "SELECT source, pushed_at, period_seconds FROM {HEARTBEAT_TABLE} WHERE source = $1"
        ))
        .bind(source)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("read {HEARTBEAT_TABLE}: {e}")))?;
        Ok(row.map(|r| ProducerHeartbeat {
            source: r.get("source"),
            pushed_at: r.get("pushed_at"),
            period_seconds: r.get("period_seconds"),
        }))
    }
}

/// Serves the producers' last deliveries to Prometheus.
pub struct ProducerHeartbeatObservable {
    store: ProducerHeartbeatStore,
    source: String,
}

impl ProducerHeartbeatObservable {
    pub fn new(store: ProducerHeartbeatStore, source: impl Into<String>) -> Self {
        Self {
            store,
            source: source.into(),
        }
    }
}

/// The two readings, from the row, or nothing at all.
///
/// A free function rather than a method because the row already carries the
/// name the label is taken from: which producer this is comes off the row, not
/// off the handle that asked for it. That also keeps the whole of the shape
/// testable without a database -- the pool a handle needs cannot even be built
/// outside a runtime.
///
/// The age is deliberately not computed here. Prometheus already knows the
/// time, and a rule that subtracts the stored moment from `time()` reads the
/// same number without this process having to decide when "now" was -- a
/// decision that would freeze into the sample and make the age a function of
/// the scrape interval rather than of the producer.
fn readings(row: Option<ProducerHeartbeat>) -> Vec<RawMetric> {
    match row {
        Some(row) => vec![
            RawMetric::new(
                PRODUCER_LAST_PUSH_TIMESTAMP,
                row.pushed_at.timestamp() as f64,
            )
            .with_label("source", row.source.clone()),
            RawMetric::new(PRODUCER_PUSH_PERIOD, row.period_seconds as f64)
                .with_label("source", row.source),
        ],
        None => Vec::new(),
    }
}

#[async_trait]
impl Observable for ProducerHeartbeatObservable {
    /// A read that fails answers with nothing rather than with an error, which
    /// is not a quiet failure: the series for this producer disappears from the
    /// exposition, and the rule that reads it fires on the absence. Failing the
    /// whole scrape instead would take every other reading on this endpoint down
    /// with a fault that is about one table.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let row = match self.store.latest(&self.source).await {
            Ok(row) => row,
            Err(e) => {
                tracing::warn!(
                    source = %self.source,
                    error = %e,
                    "producer heartbeat could not be read; its series is absent from this \
                     scrape, and the rule that watches it reads that absence as a producer \
                     that has delivered nothing"
                );
                return Ok(Vec::new());
            }
        };
        Ok(readings(row))
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The same for every dimension: the row is one producer's latest delivery,
    /// and which task asked does not change it.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stamped(seconds: i64, period: i64) -> ProducerHeartbeat {
        ProducerHeartbeat {
            source: PRINCIPLE_PRODUCER_SOURCE.to_string(),
            pushed_at: DateTime::from_timestamp(seconds, 0).expect("a valid instant"),
            period_seconds: period,
        }
    }

    /// The moment is served as the instant it happened, in seconds, so that a
    /// rule can subtract it from the platform's own `time()`.
    #[test]
    fn the_moment_is_served_as_the_instant_and_the_period_beside_it() {
        let out = readings(Some(stamped(1_760_000_000, 1_832)));
        let by_name = |name: &str| {
            out.iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("{name} missing"))
        };
        assert_eq!(by_name(PRODUCER_LAST_PUSH_TIMESTAMP).value, 1_760_000_000.0);
        assert_eq!(by_name(PRODUCER_PUSH_PERIOD).value, 1_832.0);
        for metric in &out {
            assert_eq!(
                metric.labels.get("source").map(String::as_str),
                Some(PRINCIPLE_PRODUCER_SOURCE),
                "{} carries no source label",
                metric.name
            );
        }
    }

    /// A producer that has never delivered answers with no series at all, which
    /// is the whole reason the rule for that state reads an absence: publishing
    /// the epoch here would say it delivered in 1970, and publishing a zero
    /// period would make the stopped rule divide by a number nothing declared.
    #[test]
    fn a_producer_that_has_never_delivered_publishes_nothing() {
        assert!(readings(None).is_empty());
    }

    /// The two series are published together or not at all: a moment without a
    /// period leaves the stopped rule with no threshold, and a period without a
    /// moment leaves it with nothing to measure.
    #[test]
    fn the_pair_is_never_split() {
        let out = readings(Some(stamped(1, 1)));
        assert_eq!(out.len(), 2);
        assert!(readings(None).is_empty());
    }
}
