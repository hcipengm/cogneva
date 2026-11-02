//! How full each PostgreSQL pool is, as a reading rather than as a timeout.
//!
//! Four pools open against one server, each given the same share of it. When one
//! of them runs out, the call that needed a connection does not report
//! "saturated" — it waits out its acquire timeout and fails with
//! `pool timed out while waiting for an open connection`, which names the
//! symptom and not the resource. The only series this process published about a
//! pool was the ceiling it was given, and a ceiling is a number, not a load: the
//! readings cannot say which pool is out of its share, or whether anything is
//! using it at all.
//!
//! Three series, all from one read of the pool itself:
//!
//! - `cogneva_pg_pool_connections{state="in_use"}` — connections checked out.
//!   Equality with `max` is the state that makes the next caller wait.
//! - `cogneva_pg_pool_connections{state="idle"}` — connections held and unused.
//!   Beside `in_use` it separates two failures that look the same from a
//!   timeout: a pool whose share is all taken, and one that holds no connection
//!   at all because every attempt to open one is failing.
//! - `cogneva_pg_pool_connections{state="max"}` — this pool's own ceiling, as
//!   `sqlx` will enforce it. The ceiling was published once at startup through
//!   the metric store, and the metric store writes through the `explain` pool;
//!   a rule that paired the load against it would go quiet exactly when the pool
//!   it is watching stops answering. Rendering both from this process keeps the
//!   comparison available while the store is the thing that is unwell.
//!
//! Rendered rather than stored on purpose: these are point-in-time loads of a
//! resource that dies with the process. A stored row would outlive its producer
//! and be served as "the pool is full" long after the pool was gone.
//!
//! Nothing is published before the pools exist. A build with no database has no
//! pool to describe, and a zero under a `pool` label would claim there is one.

use async_trait::async_trait;
use cog_core::{DimensionSpec, Observable, RawMetric, SFResult, TraceFragment};

/// Connections held by a PostgreSQL pool, by which state of the connection they
/// are in.
pub const PG_POOL_CONNECTIONS: &str = "cogneva_pg_pool_connections";
/// The label carrying which connection state a value counts.
pub const POOL_STATE_LABEL: &str = "state";
/// Connections checked out to a caller. Equal to [`POOL_STATE_MAX`] is the
/// state in which the next caller waits out its acquire timeout.
pub const POOL_STATE_IN_USE: &str = "in_use";
/// Connections held and not checked out.
pub const POOL_STATE_IDLE: &str = "idle";
/// The pool's configured ceiling, as the driver enforces it.
pub const POOL_STATE_MAX: &str = "max";

/// The logical pools the storage plugin opens, in the order it opens them.
///
/// A pool that failed to connect and one that succeeded are the same logical
/// pool here: the plugin reuses whichever pool opened, so a label names the role
/// the configuration declares rather than a distinct driver instance.
pub const POOL_ROLES: [&str; 4] = ["users", "messages", "config", "explain"];

/// The plugin pairs this list with one array literal of pool options, and `zip`
/// stops at the shorter side: a fifth role added here alone would be dropped
/// without a word. Pin the length at compile time so the two have to move
/// together.
const _: () = assert!(
    POOL_ROLES.len() == 4,
    "POOL_ROLES must match the plugin's pool list"
);

/// One pool's connection census at the instant it was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolCensus {
    /// Connections the pool currently holds: checked out plus idle.
    pub open: u32,
    /// Of those, the ones nothing is using.
    pub idle: u32,
    /// The pool's configured maximum.
    pub ceiling: u32,
}

impl PoolCensus {
    /// Connections currently checked out.
    ///
    /// `open` and `idle` are two reads of two counters, not one atomic snapshot:
    /// a caller returning its connection between them can make `idle` exceed the
    /// `open` read that preceded it. The subtraction saturates because a
    /// negative load is not a reading — the answer in that race is zero, which
    /// is what the pool was a moment later anyway.
    pub fn in_use(self) -> u32 {
        self.open.saturating_sub(self.idle)
    }
}

/// What the pools are doing, read on every scrape.
pub struct PoolReadings {
    pools: Vec<(&'static str, sqlx::PgPool)>,
}

impl PoolReadings {
    /// One reading per role, for as many roles as actually opened a pool.
    pub fn new(pools: Vec<(&'static str, sqlx::PgPool)>) -> Self {
        Self { pools }
    }

    fn census(pool: &sqlx::PgPool) -> PoolCensus {
        PoolCensus {
            open: pool.size(),
            idle: pool.num_idle() as u32,
            ceiling: pool.options().get_max_connections(),
        }
    }
}

/// The three rows one pool contributes.
///
/// Every state is written even when it is zero: a scrape that skipped the idle
/// row would read as absent, and absent is what a rule compares nothing to.
fn rows_for(pool: &'static str, census: PoolCensus) -> Vec<RawMetric> {
    [
        (POOL_STATE_IN_USE, f64::from(census.in_use())),
        (POOL_STATE_IDLE, f64::from(census.idle)),
        (POOL_STATE_MAX, f64::from(census.ceiling)),
    ]
    .into_iter()
    .map(|(state, value)| {
        RawMetric::new(PG_POOL_CONNECTIONS, value)
            .with_label(crate::LABEL_POOL, pool)
            .with_label(POOL_STATE_LABEL, state)
    })
    .collect()
}

#[async_trait]
impl Observable for PoolReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(self
            .pools
            .iter()
            .flat_map(|(role, pool)| rows_for(role, Self::census(pool)))
            .collect())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The census is one question about one server, not a per-dimension
    /// reading: every dimension would get the same answer.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn values(rows: &[RawMetric], state: &str) -> f64 {
        rows.iter()
            .find(|row| row.labels.get(POOL_STATE_LABEL).map(String::as_str) == Some(state))
            .unwrap_or_else(|| panic!("{state} row: zero, not absent"))
            .value
    }

    #[test]
    fn a_pool_at_its_ceiling_reads_as_fully_in_use() {
        let census = PoolCensus {
            open: 4,
            idle: 0,
            ceiling: 4,
        };
        let rows = rows_for("explain", census);
        assert_eq!(values(&rows, POOL_STATE_IN_USE), 4.0);
        assert_eq!(values(&rows, POOL_STATE_MAX), 4.0);
    }

    #[test]
    fn idle_connections_are_not_counted_as_work_in_flight() {
        let census = PoolCensus {
            open: 4,
            idle: 3,
            ceiling: 4,
        };
        assert_eq!(census.in_use(), 1);
    }

    /// The two counters are read one after the other; a connection returned
    /// between the reads makes the later idle count larger than the earlier
    /// open count. The answer is zero — the load was zero by the time the
    /// second read landed — not a negative number the exposition would print.
    #[test]
    fn a_connection_returned_between_the_two_reads_does_not_go_negative() {
        let census = PoolCensus {
            open: 1,
            idle: 2,
            ceiling: 4,
        };
        assert_eq!(census.in_use(), 0);
    }

    #[test]
    fn every_state_is_written_even_when_the_pool_holds_nothing() {
        let rows = rows_for(
            "users",
            PoolCensus {
                open: 0,
                idle: 0,
                ceiling: 4,
            },
        );
        assert_eq!(rows.len(), 3, "three states, none of them omitted at zero");
        assert_eq!(values(&rows, POOL_STATE_IN_USE), 0.0);
        assert_eq!(values(&rows, POOL_STATE_MAX), 4.0);
    }

    #[test]
    fn the_role_is_carried_on_every_row() {
        let rows = rows_for(
            "messages",
            PoolCensus {
                open: 1,
                idle: 1,
                ceiling: 4,
            },
        );
        assert!(rows
            .iter()
            .all(|row| row.labels.get(crate::LABEL_POOL).map(String::as_str) == Some("messages")));
    }

    #[test]
    fn every_role_is_named_once() {
        let unique: std::collections::BTreeSet<&str> = POOL_ROLES.into_iter().collect();
        assert_eq!(
            unique.len(),
            POOL_ROLES.len(),
            "two roles sharing a name publish the same label set twice"
        );
    }

    #[tokio::test]
    async fn a_build_with_no_pool_publishes_nothing() {
        let readings = PoolReadings::new(Vec::new());
        assert!(readings
            .collect_metrics("")
            .await
            .expect("collect")
            .is_empty());
    }
}
