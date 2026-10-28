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
//! - **The row says who started the run.** The same producer runs on its timer
//!   and, sometimes, by hand. When both write one row, a manual run overwrites
//!   the timer's evidence: the stopped rule then reads a fresh moment while the
//!   timer has been dead for hours, and the proof of a stall can be supplied by
//!   the very act that should not count as one. So a row is identified by
//!   `(source, trigger)`, both rows are served, and the rules judge only
//!   `trigger="timer"` -- a manual delivery stays visible without standing in
//!   for a scheduled one.
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
use cog_core::{fill_slots, SFError, SFResult};

/// The table the heartbeats are kept in: one row per producer and trigger,
/// rewritten in place.
///
/// One row per producer and trigger rather than one per delivery: this holds the
/// *latest* delivery, and a row per delivery would make the reading a query over
/// an append-only log that grows for as long as the deployment lives. What a
/// history would answer -- how often the gaps happen -- is answered by the
/// alert's own series, which Prometheus already keeps.
pub const HEARTBEAT_TABLE: &str = "principle_producer_pushes";

/// What started the run that wrote a row: the producer's timer, or a person.
///
/// The values are a closed set because the rules select on the column: a typo
/// would not fail anything, it would create a third trigger whose rows no rule
/// reads -- a producer that looks alive only to itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerTrigger {
    /// The producer's own schedule.
    Timer,
    /// A person ran the producer by hand.
    Manual,
}

impl ProducerTrigger {
    /// Every trigger, in the order the schema names them.
    ///
    /// The table's `DEFAULT` and its `CHECK` both spell one of these words. A
    /// default on a word the enum no longer produces writes rows under a
    /// trigger nobody selects, which reads as a producer whose timer stopped.
    pub const ALL: [ProducerTrigger; 2] = [ProducerTrigger::Timer, ProducerTrigger::Manual];

    /// The word this trigger is stored under.
    pub fn as_str(self) -> &'static str {
        match self {
            ProducerTrigger::Timer => "timer",
            ProducerTrigger::Manual => "manual",
        }
    }

    /// Read one back. A value outside the set is refused rather than mapped to
    /// a neighbour: an unknown word means the writer and this build disagree.
    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }
}

/// The name the principle ledger's producer gives itself in the row it writes.
///
/// The source is a column rather than an assumption because the row is written
/// by the producer, not by this crate: a second producer of the same account
/// needs no change here, and this crate never filters the table by name.
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

/// This store's schema, and the one a restore replays.
///
/// The `DEFAULT` and the `CHECK` are rendered from [`ProducerTrigger`] rather
/// than written out: the table is created by this store on first start, so a
/// stale word here refuses the producer's own rows on its next delivery, and
/// the rules that read them select by the word the enum declares.
const DDL_TEMPLATE: &str = r#"
CREATE TABLE IF NOT EXISTS principle_producer_pushes (
                source         TEXT NOT NULL,
                trigger        TEXT NOT NULL DEFAULT '{trigger:timer}'
                               CHECK (trigger IN ('{trigger:timer}', '{trigger:manual}')),
                pushed_at      TIMESTAMPTZ NOT NULL,
                period_seconds BIGINT NOT NULL,
                PRIMARY KEY (source, trigger)
            );
"#;

/// The schema with the trigger words filled in from the declaration.
pub fn ddl() -> String {
    fill_slots(
        DDL_TEMPLATE,
        &ProducerTrigger::ALL.map(|trigger| ("trigger", trigger.as_str())),
    )
}

/// The row as the reader sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerHeartbeat {
    pub source: String,
    pub trigger: String,
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
    ///
    /// On a database that already holds the old one-column key, this statement
    /// is a no-op; the `(source, trigger)` key and the column that goes with it
    /// are applied by migration 061 in `cog-storage`, which also runs at
    /// startup. A fresh database gets the final shape from here, and an existing
    /// one gets it from there; neither path drops a row.
    pub async fn init_schema(&self) -> anyhow::Result<()> {
        cog_storage::schema_ddl::apply(&self.pool, &ddl())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(())
    }

    /// The latest delivery of every producer and every trigger, or an empty set
    /// if nothing has ever delivered.
    ///
    /// An empty set and a row from long ago are different findings and are not
    /// collapsed: the first is a producer that has never run, the second one
    /// that ran and stopped.
    ///
    /// Every row, not one row per source: a source has one row per trigger, and
    /// a reader that picked one of them by source would silently drop the other
    /// -- exactly the row the stopped rule needs when the timer is the one that
    /// went quiet.
    pub async fn latest_all(&self) -> SFResult<Vec<ProducerHeartbeat>> {
        let rows = sqlx::query(&format!(
            "SELECT source, \"trigger\", pushed_at, period_seconds FROM {HEARTBEAT_TABLE} \
             ORDER BY source, \"trigger\""
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| SFError::Database(format!("read {HEARTBEAT_TABLE}: {e}")))?;
        Ok(rows
            .into_iter()
            .map(|r| ProducerHeartbeat {
                source: r.get("source"),
                trigger: r.get(crate::LABEL_TRIGGER),
                pushed_at: r.get("pushed_at"),
                period_seconds: r.get("period_seconds"),
            })
            .collect())
    }
}

/// Serves the producers' last deliveries to Prometheus.
pub struct ProducerHeartbeatObservable {
    store: ProducerHeartbeatStore,
}

impl ProducerHeartbeatObservable {
    pub fn new(store: ProducerHeartbeatStore) -> Self {
        Self { store }
    }
}

/// The two readings per row, or nothing at all.
///
/// A free function rather than a method because the row already carries the
/// names the labels are taken from: which producer this is, and what started
/// it, come off the row, not off the handle that asked for it. That also keeps
/// the whole of the shape testable without a database -- the pool a handle
/// needs cannot even be built outside a runtime.
///
/// The age is deliberately not computed here. Prometheus already knows the
/// time, and a rule that subtracts the stored moment from `time()` reads the
/// same number without this process having to decide when "now" was -- a
/// decision that would freeze into the sample and make the age a function of
/// the scrape interval rather than of the producer.
fn readings(rows: Vec<ProducerHeartbeat>) -> Vec<RawMetric> {
    let mut out = Vec::with_capacity(rows.len() * 2);
    for row in rows {
        for (name, value) in [
            (
                PRODUCER_LAST_PUSH_TIMESTAMP,
                row.pushed_at.timestamp() as f64,
            ),
            (PRODUCER_PUSH_PERIOD, row.period_seconds as f64),
        ] {
            out.push(
                RawMetric::new(name, value)
                    .with_label(crate::alerts::LABEL_SOURCE, row.source.clone())
                    .with_label(crate::LABEL_TRIGGER, row.trigger.clone()),
            );
        }
    }
    out
}

#[async_trait]
impl Observable for ProducerHeartbeatObservable {
    /// A read that fails answers with nothing rather than with an error, which
    /// is not a quiet failure: the series for this producer disappears from the
    /// exposition, and the rule that reads it fires on the absence. Failing the
    /// whole scrape instead would take every other reading on this endpoint down
    /// with a fault that is about one table.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let rows = match self.store.latest_all().await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "producer heartbeat could not be read; its series is absent from this \
                     scrape, and the rule that watches it reads that absence as a producer \
                     that has delivered nothing"
                );
                return Ok(Vec::new());
            }
        };
        Ok(readings(rows))
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

    fn stamped(trigger: &str, seconds: i64, period: i64) -> ProducerHeartbeat {
        ProducerHeartbeat {
            source: PRINCIPLE_PRODUCER_SOURCE.to_string(),
            trigger: trigger.to_string(),
            pushed_at: DateTime::from_timestamp(seconds, 0).expect("a valid instant"),
            period_seconds: period,
        }
    }

    /// The moment is served as the instant it happened, in seconds, so that a
    /// rule can subtract it from the platform's own `time()`.
    #[test]
    fn the_moment_is_served_as_the_instant_and_the_period_beside_it() {
        let out = readings(vec![stamped(
            ProducerTrigger::Timer.as_str(),
            1_760_000_000,
            1_832,
        )]);
        let by_name = |name: &str| {
            out.iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("{name} missing"))
        };
        assert_eq!(by_name(PRODUCER_LAST_PUSH_TIMESTAMP).value, 1_760_000_000.0);
        assert_eq!(by_name(PRODUCER_PUSH_PERIOD).value, 1_832.0);
        for metric in &out {
            assert_eq!(
                metric
                    .labels
                    .get(crate::alerts::LABEL_SOURCE)
                    .map(String::as_str),
                Some(PRINCIPLE_PRODUCER_SOURCE),
                "{} carries no source label",
                metric.name
            );
            assert_eq!(
                metric.labels.get(crate::LABEL_TRIGGER).map(String::as_str),
                Some(ProducerTrigger::Timer.as_str()),
                "{} carries no trigger label",
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
        assert!(readings(Vec::new()).is_empty());
    }

    /// The two series are published together or not at all: a moment without a
    /// period leaves the stopped rule with no threshold, and a period without a
    /// moment leaves it with nothing to measure.
    #[test]
    fn the_pair_is_never_split() {
        let out = readings(vec![stamped(ProducerTrigger::Timer.as_str(), 1, 1)]);
        assert_eq!(out.len(), 2);
        assert!(readings(Vec::new()).is_empty());
    }

    /// A manual delivery and a scheduled one are two findings, and the later
    /// manual row must not take the timer's place: the stopped rule subtracts
    /// from the timer's moment, so replacing it would let a person keep a dead
    /// timer looking alive by running the producer by hand.
    #[test]
    fn a_manual_delivery_stands_beside_the_timers_instead_of_replacing_it() {
        let out = readings(vec![
            stamped(ProducerTrigger::Timer.as_str(), 1_000, 1_800),
            stamped(ProducerTrigger::Manual.as_str(), 2_000, 1_800),
        ]);
        assert_eq!(out.len(), 4, "both rows must be served");
        let mut timed: Vec<(String, f64)> = out
            .iter()
            .filter(|m| m.name == PRODUCER_LAST_PUSH_TIMESTAMP)
            .map(|m| {
                (
                    m.labels
                        .get(crate::LABEL_TRIGGER)
                        .cloned()
                        .unwrap_or_default(),
                    m.value,
                )
            })
            .collect();
        timed.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            timed,
            vec![
                ("manual".to_string(), 2_000.0),
                ("timer".to_string(), 1_000.0)
            ],
            "the timer's moment must survive a later manual push"
        );
    }

    /// 建表语句里的默认值与 CHECK 必须逐字带着枚举声明的词。
    ///
    /// 这一句由本进程在首次启动时执行：写着一个不再产出的词，生产的第一次投递
    /// 就被自己的表拒掉；而默认值写错的后果更安静——行落在一个没人选的 trigger
    /// 上，判停摆的规则查那个词，读到的是"定时器停了"。槽没填则整段 DDL 拒解析。
    #[test]
    fn the_schema_names_the_triggers_the_enum_declares() {
        let schema = ddl();
        assert!(!schema.contains("{trigger:"), "trigger 槽没有填上");
        for trigger in ProducerTrigger::ALL {
            assert!(
                schema.contains(&format!("'{}'", trigger.as_str())),
                "建表语句里没有 {} 这个词",
                trigger.as_str()
            );
        }
        assert!(schema.contains(&format!("DEFAULT '{}'", ProducerTrigger::Timer.as_str())));
    }
}
