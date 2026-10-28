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
//! The same points also publish **when** they were reached, in seconds counted
//! from the first mark. Startup time could only be attributed by replaying the
//! log by hand before this: the process-internal work behind a slow start
//! writes nothing a database can query, and the only trace of it was the gap
//! between two adjacent log lines. Two numbers taken at the same instants as
//! the memory snapshots cost nothing extra to read, and one phase's own cost is
//! the difference from the phase before it -- exactly the same arithmetic as
//! the memory series, for the same reason.
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
//! - **A phase is printed as it is marked, not only published.** The series
//!   reaches a reader through the process's own exposition, and a boot that
//!   stalls before `serving` is exactly the boot whose exposition does not
//!   exist yet: the reading that would say where it stopped is served by the
//!   step it never reached. A log line at the moment of the mark is the only
//!   account of the stall that survives it, and the last phase named before a
//!   silent stretch is the phase to look at.
//!
//! The phase set is a closed list for the same reason a label is ever closed:
//! the series' cardinality is decided by it, and a free-form phase name would
//! let one caller's typo become a second series that no rule reads.

use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Resident bytes of this process at a named startup phase.
pub const BOOT_RSS_METRIC: &str = "cogneva_boot_rss_bytes";

/// Seconds from the first mark to a named startup phase.
pub const BOOT_ELAPSED_METRIC: &str = "cogneva_boot_phase_elapsed_seconds";

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

/// One phase as it stood when it was reached.
#[derive(Clone, Debug, PartialEq)]
struct Mark {
    phase: &'static str,
    /// Resident bytes at that instant, or `None` where the read failed.
    rss: Option<u64>,
    /// Seconds from the first mark to this one.
    elapsed_seconds: f64,
}

/// One snapshot per phase, in phase order, first mark wins.
static MARKS: Mutex<Vec<Mark>> = Mutex::new(Vec::new());

/// The instant elapsed time is counted from: the first phase marked, which in a
/// real boot is `process-start`. A mutex rather than a `OnceLock` so a test can
/// forget the origin along with the marks it left behind.
static ORIGIN: Mutex<Option<Instant>> = Mutex::new(None);

/// Records this process's resident memory at `phase`.
///
/// Does nothing when `phase` is not in [`PHASES`] or has already been marked.
/// Both are said at debug level, because neither is startup progress: the first
/// is a caller naming a phase that has no series, the second is the same phase
/// arriving twice.
///
/// A phase that does land is announced at info level, before anything else can
/// go wrong with the snapshot. The published series cannot stand in for this
/// line: it is served over the HTTP endpoint, so a boot stuck earlier in
/// startup has no series at all, and the log is then the only place the last
/// phase reached is written down. When the resident read fails the phase is
/// still announced -- where a boot got to is worth knowing even without the
/// number -- and it still carries its elapsed time, which costs no read to take.
pub fn mark(phase: &'static str) {
    if !PHASES.contains(&phase) {
        tracing::debug!(
            phase,
            "boot footprint: phase is not in the closed set; not recorded"
        );
        return;
    }
    let mut marks = match MARKS.lock() {
        Ok(m) => m,
        Err(e) => e.into_inner(),
    };
    if marks.iter().any(|m| m.phase == phase) {
        return;
    }
    // Time before memory: the resident read opens a file, and a phase's position
    // in startup must not move because procfs was slow or missing.
    let elapsed_seconds = {
        let mut origin = match ORIGIN.lock() {
            Ok(o) => o,
            Err(e) => e.into_inner(),
        };
        origin
            .get_or_insert_with(Instant::now)
            .elapsed()
            .as_secs_f64()
    };
    let rss = resident_bytes();
    marks.push(Mark {
        phase,
        rss,
        elapsed_seconds,
    });
    marks.sort_by_key(|m| {
        PHASES
            .iter()
            .position(|x| *x == m.phase)
            .unwrap_or(usize::MAX)
    });
    match rss {
        Some(rss) => {
            tracing::info!(phase, rss, elapsed_seconds, "boot footprint: phase reached")
        }
        None => tracing::info!(
            phase,
            elapsed_seconds,
            "boot footprint: phase reached, but resident memory could not be read here"
        ),
    }
}

/// The memory snapshots taken so far, in phase order.
pub fn marks() -> Vec<(&'static str, u64)> {
    snapshot()
        .into_iter()
        .filter_map(|m| m.rss.map(|rss| (m.phase, rss)))
        .collect()
}

/// Seconds from the first mark to every phase reached so far, in phase order.
pub fn marks_elapsed() -> Vec<(&'static str, f64)> {
    snapshot()
        .into_iter()
        .map(|m| (m.phase, m.elapsed_seconds))
        .collect()
}

fn snapshot() -> Vec<Mark> {
    match MARKS.lock() {
        Ok(m) => m.clone(),
        Err(e) => e.into_inner().clone(),
    }
}

/// Forgets every mark. Tests only: the registry is per process and a test that
/// marks must not decide what the next test reads.
#[doc(hidden)]
pub fn clear_for_tests() {
    match MARKS.lock() {
        Ok(mut m) => m.clear(),
        Err(e) => e.into_inner().clear(),
    }
    match ORIGIN.lock() {
        Ok(mut o) => *o = None,
        Err(e) => *e.into_inner() = None,
    }
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

/// Publishes one `cogneva_boot_rss_bytes` and one
/// `cogneva_boot_phase_elapsed_seconds` series per recorded phase.
pub struct BootFootprintObservable;

#[async_trait]
impl Observable for BootFootprintObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        for mark in snapshot() {
            metrics.push(
                RawMetric::new(BOOT_ELAPSED_METRIC, mark.elapsed_seconds)
                    .with_label(crate::LABEL_PHASE, mark.phase),
            );
            if let Some(bytes) = mark.rss {
                metrics.push(
                    RawMetric::new(BOOT_RSS_METRIC, bytes as f64)
                        .with_label(crate::LABEL_PHASE, mark.phase),
                );
            }
        }
        Ok(metrics)
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
    use std::sync::Arc;

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

    /// One event as it reaches a log backend: its level and the phase it named.
    #[derive(Clone, Debug)]
    struct Logged {
        level: tracing::Level,
        phase: Option<String>,
    }

    /// A log backend that only records what it was handed.
    struct Capture(Arc<Mutex<Vec<Logged>>>);

    impl<S: tracing::Subscriber> tracing_subscriber::layer::Layer<S> for Capture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut logged = Logged {
                level: *event.metadata().level(),
                phase: None,
            };
            let mut fields = PhaseField(&mut logged.phase);
            event.record(&mut fields);
            self.0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(logged);
        }
    }

    struct PhaseField<'a>(&'a mut Option<String>);

    impl tracing::field::Visit for PhaseField<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "phase" {
                *self.0 = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "phase" {
                *self.0 = Some(format!("{value:?}"));
            }
        }
    }

    #[test]
    fn a_phase_that_is_reached_is_announced_at_the_level_a_log_keeps() {
        use tracing_subscriber::prelude::*;

        let _guard = one_at_a_time();
        clear_for_tests();
        let logged = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Capture(Arc::clone(&logged)));
        tracing::subscriber::with_default(subscriber, || {
            mark("embedding-dense");
            // A phase outside the closed set is a caller's mistake, not startup
            // progress: it must stay at debug rather than read as a phase the
            // boot actually reached.
            mark("not-a-phase");
        });

        let events = logged.lock().unwrap_or_else(|e| e.into_inner()).clone();
        let reached: Vec<&Logged> = events
            .iter()
            .filter(|e| e.phase.as_deref() == Some("embedding-dense"))
            .collect();
        assert_eq!(
            reached.len(),
            1,
            "reaching a phase printed {} lines naming it: {events:?}",
            reached.len()
        );
        assert_eq!(
            reached[0].level,
            tracing::Level::INFO,
            "a phase the boot reached has to survive the level a deployment logs at; \
             the series that carries it is served by the endpoint a stall prevents, \
             so a debug line is a progress report nobody receives"
        );

        let refused: Vec<&Logged> = events
            .iter()
            .filter(|e| e.phase.as_deref() == Some("not-a-phase"))
            .collect();
        assert!(
            refused.iter().all(|e| e.level == tracing::Level::DEBUG),
            "a phase outside the closed set reached the log at {refused:?}"
        );
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

    #[test]
    fn the_second_phase_carries_the_time_the_first_one_did_not() {
        let _guard = one_at_a_time();
        clear_for_tests();
        mark("process-start");
        std::thread::sleep(std::time::Duration::from_millis(30));
        mark("config-loaded");

        let elapsed = marks_elapsed();
        assert_eq!(
            elapsed.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            vec!["process-start", "config-loaded"],
            "the time series has to name the same phases, in the same order, as the memory series"
        );
        assert!(
            elapsed[0].1 < 0.030,
            "the first phase is the origin of the count and must come out at about zero, got {:?}",
            elapsed
        );
        assert!(
            elapsed[1].1 >= 0.020,
            "30 ms passed between the two marks and the second has to carry it, got {:?}",
            elapsed
        );

        // First mark wins here too: a later call is the process at some
        // unrelated moment under a name that says "at startup".
        std::thread::sleep(std::time::Duration::from_millis(10));
        mark("config-loaded");
        assert_eq!(
            marks_elapsed(),
            elapsed,
            "re-marking a phase moved the time it was reached at"
        );
        clear_for_tests();
    }

    #[test]
    fn clearing_forgets_the_origin_along_with_the_marks() {
        let _guard = one_at_a_time();
        clear_for_tests();
        mark("process-start");
        std::thread::sleep(std::time::Duration::from_millis(10));
        clear_for_tests();

        // A second test's first phase is its own origin, not the time since
        // whatever the previous test happened to mark.
        mark("serving");
        let elapsed = marks_elapsed();
        assert_eq!(elapsed.len(), 1);
        assert!(
            elapsed[0].1 < 0.010,
            "the origin survived a clear, so the next test reads the previous one's clock: {:?}",
            elapsed
        );
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
            .map(|m| {
                m.labels
                    .get(crate::LABEL_PHASE)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(phases, vec!["process-start", "plugins-initialized"]);
        assert!(
            metrics
                .iter()
                .filter(|m| m.name == BOOT_RSS_METRIC)
                .all(|m| m.value > 0.0),
            "a live process has resident memory"
        );

        // The time series is published for the same phases by the same reading:
        // a reader that has to ask a second endpoint for it is a reader that
        // compares two scrapes of different instants.
        let timed: Vec<String> = metrics
            .iter()
            .filter(|m| m.name == BOOT_ELAPSED_METRIC)
            .map(|m| {
                m.labels
                    .get(crate::LABEL_PHASE)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(
            timed, phases,
            "the reading published a time series for a different set of phases than the memory series"
        );
        assert!(
            metrics
                .iter()
                .filter(|m| m.name == BOOT_ELAPSED_METRIC)
                .all(|m| m.value >= 0.0),
            "elapsed time cannot run backwards from the origin"
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
