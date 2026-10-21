//! The quota's own reading — what it decided, at the one face that needs no
//! credentials.
//!
//! The quota refuses requests, and a refusal reaches only the caller it refused:
//! the 429 carries the reason back to that one request, while how many were
//! refused — and of what — reaches nobody.
//! The account it spends is one key per user in the key-value store, and that
//! contract has no enumeration — [`cog_core::KeyValueStore`] reads a key you
//! already know (`get`, `hget`, `hgetall`) — so a scrape cannot walk the users
//! to report `quota_remaining{user_id}`. What it can report is this process's
//! own decisions, which is the question the enforcement raised and could not
//! answer: is anyone being refused, and how often.
//!
//! Both cells are published always, the zero one included. "No refusals" is a
//! reading — it is what tells an operator the quota is quiet rather than
//! unwired — and a series that appears only once the first refusal lands reads
//! as a producer that arrived late.
//!
//! The amount charged is deliberately not published. A token-amount series is
//! only meaningful per account, and per-account keys are unbounded, which this
//! platform's [`DimensionSpec::unbounded`] forbids on a periodic scrape;
//! aggregated across accounts it measures the scrape's own throughput rather
//! than a fault. A reading with no reader is a name nobody sees, so the family
//! stops at the decisions.
//!
//! No rule reads this family today, and that is the honest shape of it: the one
//! caller that charged the manager was a middleware on the gateway's management
//! route groups, removing it left nothing that can reach a refusal, and a rule
//! over a family that cannot move would fire never and mean nothing when it did.
//! The family stays published at zero — both cells, every scrape — so that the
//! vocabulary a refusal would use already exists, and so that the day
//! enforcement lands on a surface that actually spends tokens, the only thing
//! missing is the reader.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Checks the quota has run, by how they were decided.
///
/// One family with an `allowed` cell rather than two names: the two counts are
/// only readable against each other — a hundred refusals out of a hundred and
/// one checks is a cap doing its job, the same hundred out of a million is a
/// defect — and a reader that has to remember to ask for two names is one that
/// will read half of an answer.
pub const PRE_CHECKS_TOTAL: &str = "quota_pre_checks_total";

/// Which way a check was decided. The values are the series' identity.
const ALLOWED: &str = "allowed";

/// What the quota decided since this process started.
///
/// Kept beside the decisions themselves ([`crate::QuotaManager::pre_check`])
/// rather than derived at scrape time: the account is spent through a store
/// this process shares with every other replica, so a scrape that tried to
/// count decisions by reading the store would be reporting the fleet's traffic
/// under one pod's name.
#[derive(Debug, Default)]
pub struct QuotaReadings {
    allowed: AtomicU64,
    refused: AtomicU64,
}

impl QuotaReadings {
    /// The check was admitted; `tokens` were deducted from the account.
    pub fn record_allowed(&self) {
        self.allowed.fetch_add(1, Ordering::Relaxed);
    }

    /// The check was refused. A caller that refuses turns `allowed == false`
    /// into an answer that carries the reason back to that one caller, so what
    /// this counter adds is not the reason but the aggregate: every refusal
    /// lands on the caller it refused and nowhere else, and "how many were
    /// refused over a window" is not recoverable from any single response.
    pub fn record_refused(&self) {
        self.refused.fetch_add(1, Ordering::Relaxed);
    }

    /// Checks admitted since this process started.
    pub fn allowed(&self) -> u64 {
        self.allowed.load(Ordering::Relaxed)
    }

    /// Checks refused since this process started.
    pub fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }
}

/// [`QuotaReadings`] as the scrape asks for them.
#[derive(Debug)]
pub struct QuotaObservable {
    readings: Arc<QuotaReadings>,
}

impl QuotaObservable {
    /// Build the reading over a manager's own counters.
    pub fn new(readings: Arc<QuotaReadings>) -> Self {
        Self { readings }
    }
}

#[async_trait]
impl Observable for QuotaObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(vec![
            RawMetric::new(PRE_CHECKS_TOTAL, self.readings.allowed() as f64)
                .with_label(ALLOWED, "true"),
            RawMetric::new(PRE_CHECKS_TOTAL, self.readings.refused() as f64)
                .with_label(ALLOWED, "false"),
        ])
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// No dimension: every cell of this family is a process-wide count, the
    /// same number whichever dimension is asked for. Declaring one would have
    /// the collector ask this observable once per dimension and publish the
    /// same sample under each — which the exposition rejects as a duplicate,
    /// costing the series its reading in that scrape.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}
