//! Where the birth footprint went: one resident-memory snapshot per named
//! startup phase, published as a gauge labelled by phase.
//!
//! The application is born holding most of what it will ever hold — measured over
//! the twenty-four hours to 2026-10-23, generations that lived long enough to be
//! sampled were born at 4821 to 5051 MiB and moved at most 112 MiB (measured
//! 2026-10-23) inside a generation. That makes the birth footprint, not the
//! runtime, the quantity that decides whether the container has room. The band
//! rule above it can see the footprint and the bound can see it drift, but
//! neither can say *which* part of startup the footprint belongs to: the only
//! way to attribute it was to replay the startup by hand, and the eight days
//! (measured to 2026-10-23) in which it rose by about 300 MiB were attributed by
//! nobody.
//!
//! So a handful of points in startup each take one snapshot: process entry,
//! config loaded, each model session built, all plugins initialised, serving.
//! The value is this process's `VmRSS` at that moment, in bytes.
//!
//! Two things about what a value means, because they decide how it is read:
//!
//! - **A phase's value is cumulative**, not that phase's own cost. The snapshot
//!   is the whole process at that instant, so the cost of one phase is the
//!   difference from the phase before it. Publishing the raw snapshot rather
//!   than the delta is deliberate: a delta is a derived number, and the
//!   derivation changes when a phase is added between two others. Any reader
//!   that wants deltas can subtract two snapshots; a reader handed deltas
//!   cannot recover what the process actually held.
//! - **The first mark of a phase is the one kept.** A phase is an instant, and
//!   a second call would record the process much later under a name that says
//!   "at startup". A phase that never marks — embeddings switched off, startup
//!   that died before serving — is absent rather than zero, because zero would
//!   read as "this phase allocates nothing" instead of "this process never got
//!   there".
//!
//! The phase set is a closed list for the same reason a label is ever closed:
//! the series' cardinality is decided by it, and a free-form phase name would
//! let one caller's typo become a second series that no rule reads.

use std::sync::Mutex;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Resident bytes of this process at a named startup phase.
pub const BOOT_RSS_METRIC: &str = "cogneva_boot_rss_bytes";

/// The rule that reads the series, named here so the pairing is asserted
/// rather than left to whoever edits the chart.
pub const BOOT_RSS_METRIC_RULE: &str = "boot_phase_footprint_grew";

/// The startup phases this build can mark, in the order they happen.
///
/// Adding one is a decision about where a snapshot is worth taking; the list
/// is what keeps the series' cardinality fixed. Names are hyphenated so they
/// read the same in a label selector and in a log line.
pub const PHASES: &[&str] = &[
    "process-start",
    "config-loaded",
    "embedding-dense",
    "embedding-sparse",
    "reranker",
    "plugins-initialized",
    "serving",
];

/// One snapshot per phase, in phase order, first mark wins.
static MARKS: Mutex<Vec<(&'static str, u64)>> = Mutex::new(Vec::new());

/// Records this process's resident memory at `phase`.
///
/// Silently does nothing when `phase` is not in [`PHASES`] or has already been
/// marked — but says so at debug level, because a phase that never lands is a
/// snapshot the reader will wait for and never get.
pub fn mark(phase: &'static str) {
    if !PHASES.contains(&phase) {
        tracing::debug!(
            phase,
            "boot footprint: phase is not in the closed set; not recorded"
        );
        return;
    }
    let Some(rss) = resident_bytes() else {
        tracing::debug!(
            phase,
            "boot footprint: resident memory is not readable here"
        );
        return;
    };
    let mut marks = match MARKS.lock() {
        Ok(m) => m,
        Err(e) => e.into_inner(),
    };
    if marks.iter().any(|(p, _)| *p == phase) {
        return;
    }
    marks.push((phase, rss));
    marks.sort_by_key(|(p, _)| PHASES.iter().position(|x| x == p).unwrap_or(usize::MAX));
    tracing::debug!(phase, rss, "boot footprint: marked");
}

/// The snapshots taken so far, in phase order.
pub fn marks() -> Vec<(&'static str, u64)> {
    match MARKS.lock() {
        Ok(m) => m.clone(),
        Err(e) => e.into_inner().clone(),
    }
}

/// Forgets every mark. Tests only: the registry is per process and a test that
/// marks must not decide what the next test reads.
#[doc(hidden)]
pub fn clear_for_tests() {
    let mut marks = match MARKS.lock() {
        Ok(m) => m,
        Err(e) => e.into_inner(),
    };
    marks.clear();
}

/// This process's resident bytes from `/proc/self/status`, or `None` where that
/// file does not exist or does not carry `VmRSS`.
fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_vm_rss(&status).map(|kb| kb.saturating_mul(1024))
}

/// `VmRSS:` from `/proc/<pid>/status`, in kilobytes.
///
/// Split from the read so the parse is testable without a procfs: the field is
/// the process's resident set, which is the same quantity the container
/// readings use as their numerator.
pub fn parse_vm_rss(status: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let rest = line.strip_prefix("VmRSS:")?;
        rest.split_whitespace().next()?.parse::<u64>().ok()
    })
}

/// Publishes one `cogneva_boot_rss_bytes` series per recorded phase.
pub struct BootFootprintObservable;

#[async_trait]
impl Observable for BootFootprintObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        Ok(marks()
            .into_iter()
            .map(|(phase, bytes)| {
                RawMetric::new(BOOT_RSS_METRIC, bytes as f64).with_label("phase", phase)
            })
            .collect())
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The reading is the same for every dimension: it describes this process,
    /// and which task asked does not change that.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry is one per process, so tests that mark have to take turns:
    /// otherwise each one reads the marks another one left behind.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn one_at_a_time() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[test]
    fn vm_rss_is_read_from_the_status_line_not_from_any_number_in_it() {
        let status =
            "Name:\tcogneva\nVmPeak:\t 9000000 kB\nVmRSS:\t  5251384 kB\nRssAnon:\t 5154420 kB\n";
        assert_eq!(parse_vm_rss(status), Some(5251384));
    }

    #[test]
    fn a_status_without_vm_rss_is_unreadable_rather_than_zero() {
        assert_eq!(
            parse_vm_rss("Name:\tcogneva\nRssAnon:\t 5154420 kB\n"),
            None
        );
        assert_eq!(parse_vm_rss(""), None);
    }

    #[test]
    fn a_phase_is_marked_once_and_an_unknown_phase_is_not_a_new_series() {
        let _guard = one_at_a_time();
        clear_for_tests();
        mark("serving");
        let first = marks();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].0, "serving");

        // A second mark of the same phase keeps the first snapshot: the name
        // says "at startup", and a later value would be the process at some
        // unrelated moment.
        mark("serving");
        assert_eq!(marks(), first);

        // A phase outside the closed set does not become a series of its own.
        mark("not-a-phase");
        assert_eq!(marks(), first);
        clear_for_tests();
    }

    #[test]
    fn marks_come_out_in_the_order_the_phases_happen() {
        let _guard = one_at_a_time();
        clear_for_tests();
        mark("serving");
        mark("process-start");
        let order: Vec<&str> = marks().into_iter().map(|(p, _)| p).collect();
        assert_eq!(order, vec!["process-start", "serving"]);
        clear_for_tests();
    }

    // Driven from a sync test on purpose: the guard this suite serializes on is
    // a std mutex, and holding one across an await is both a lint and a real
    // hazard -- another test running on the same thread would wait for a lock
    // whose holder is parked in the same executor. Here the guard is held for
    // the whole body and the await happens inside a runtime this test owns.
    #[test]
    fn every_marked_phase_is_a_series_carrying_its_own_name() {
        let _guard = one_at_a_time();
        clear_for_tests();
        mark("process-start");
        mark("plugins-initialized");
        let metrics = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime for one reading")
            .block_on(BootFootprintObservable.collect_metrics(""))
            .expect("the reading never fails");
        let phases: Vec<String> = metrics
            .iter()
            .filter(|m| m.name == BOOT_RSS_METRIC)
            .map(|m| m.labels.get("phase").cloned().unwrap_or_default())
            .collect();
        assert_eq!(phases, vec!["process-start", "plugins-initialized"]);
        assert!(
            metrics.iter().all(|m| m.value > 0.0),
            "a live process has resident memory"
        );
        clear_for_tests();
    }

    #[test]
    fn the_rule_this_series_exists_for_is_named_here() {
        // The pairing is asserted where the series is declared: a rule renamed
        // in the chart without this constant, or the reverse, is what leaves a
        // published series with no reader.
        assert_eq!(BOOT_RSS_METRIC_RULE, "boot_phase_footprint_grew");
    }
}
