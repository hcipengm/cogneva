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
//!
//! Each of those lines also carries **`cpu_seconds`**, **`minor_faults`**,
//! **`major_faults`** and **`read_bytes`** for the same instant. The CPU
//! reading is published as `cogneva_boot_phase_cpu_seconds` beside the elapsed
//! one, so the comparison that says whether a phase was working or waiting can
//! be made by something other than a person reading two adjacent log lines. The
//! fault and disk counters stay fields on the line: their meaning is a
//! difference between adjacent phases either way, and the question they answer
//! -- was this phase's missing CPU spent waiting to be scheduled or waiting for
//! the disk -- is asked once, by whoever is looking at a slow boot, not
//! continuously. Elapsed
//! time alone cannot say whether a slow phase was working or waiting: the same
//! work costs about the same CPU time however long it is made to wait, so a
//! phase whose elapsed time rose while its CPU time did not had time pass with
//! no thread running. That still leaves the two ways time passes without a
//! thread: waiting to be scheduled, and waiting on the disk. `major_faults`
//! separates fault-driven paging from the rest -- a phase that page-faulted its
//! way through the weights shows about one fault per page of the file, and one
//! that was merely not scheduled shows almost none -- but it cannot see a
//! loader that reads its weights with `read()` into a buffer: the process waits
//! inside that call, with no fault raised and no CPU spent, which is the same
//! shape as never being scheduled. `read_bytes` closes that gap: the bytes the
//! kernel fetched off the block layer on this process's behalf, whether the
//! process then mapped them or copied them.
//!
//! The fault and disk counters are not published as series because a phase's
//! value is cumulative and its meaning is the difference from the phase before
//! it, which is the reader's arithmetic either way; keeping them in the line
//! costs no cardinality and answers questions asked once, of one slow boot,
//! rather than continuously.

use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

/// Resident bytes of this process at a named startup phase.
pub const BOOT_RSS_METRIC: &str = "cogneva_boot_rss_bytes";

/// Seconds from the first mark to a named startup phase.
pub const BOOT_ELAPSED_METRIC: &str = "cogneva_boot_phase_elapsed_seconds";

/// Seconds of CPU this process had spent, summed over every thread, by the time
/// a named startup phase was reached.
///
/// Published beside the elapsed time so that the pair can be compared by
/// something other than a person reading log lines: a phase whose wall clock
/// grew while its CPU time did not had time pass with no thread running.
pub const BOOT_CPU_METRIC: &str = "cogneva_boot_phase_cpu_seconds";

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
    /// CPU seconds this process had spent by this instant, or `None` where the
    /// clock could not be read.
    cpu_seconds: Option<f64>,
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
    // The CPU and fault counters are taken beside the wall clock and for the same
    // reason: they describe this instant, and they are only comparable with it
    // when they were read at the same instant.
    let vitals = process_vitals();
    let rss = resident_bytes();
    marks.push(Mark {
        phase,
        rss,
        elapsed_seconds,
        cpu_seconds: vitals.map(|v| v.cpu_seconds),
    });
    marks.sort_by_key(|m| {
        PHASES
            .iter()
            .position(|x| *x == m.phase)
            .unwrap_or(usize::MAX)
    });
    announce(phase, rss, elapsed_seconds, vitals);
}

/// Prints the line a reached phase is announced with.
///
/// One place prints it, so a field added here reaches every shape the line
/// takes -- with or without a resident reading, with or without the process
/// counters -- rather than the one an edit happened to touch. A field that
/// reaches only some of the shapes is read as absent by whoever greps the
/// others.
fn announce(phase: &'static str, rss: Option<u64>, elapsed_seconds: f64, vitals: Option<Vitals>) {
    match (rss, vitals) {
        (Some(rss), Some(vitals)) => tracing::info!(
            phase,
            rss,
            elapsed_seconds,
            cpu_seconds = vitals.cpu_seconds,
            minor_faults = vitals.minor_faults,
            major_faults = vitals.major_faults,
            read_bytes = vitals.read_bytes,
            "boot footprint: phase reached"
        ),
        (Some(rss), None) => {
            tracing::info!(phase, rss, elapsed_seconds, "boot footprint: phase reached")
        }
        (None, Some(vitals)) => tracing::info!(
            phase,
            elapsed_seconds,
            cpu_seconds = vitals.cpu_seconds,
            minor_faults = vitals.minor_faults,
            major_faults = vitals.major_faults,
            read_bytes = vitals.read_bytes,
            "boot footprint: phase reached, but resident memory could not be read here"
        ),
        (None, None) => tracing::info!(
            phase,
            elapsed_seconds,
            "boot footprint: phase reached, but resident memory could not be read here"
        ),
    }
}

/// This module's [`mark`], handed out as a contract.
///
/// A crate that loads a model does not depend on this crate: it reports the
/// phase through `cog_core::BootPhaseMarker`, and the host publishes this
/// implementation into it. The type exists so that publishing is one line and
/// has no state of its own — the registry the call writes into is the same one
/// a mark from this crate lands in.
pub struct BootMarks;

impl cog_core::BootPhaseMarker for BootMarks {
    fn mark(&self, phase: &'static str) {
        mark(phase)
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

/// What this process has spent so far, as the kernel counts it.
///
/// Kept in one struct so a phase's line has one reading to carry rather than
/// three that can each be missing: they are taken from the same two syscalls at
/// the same instant, and an instant with only some of them is an instant that
/// answers no question.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Vitals {
    /// CPU seconds, summed over every thread.
    cpu_seconds: f64,
    /// Minor page faults: pages that had to be mapped in, served from memory.
    minor_faults: u64,
    /// Major page faults: pages that had to be read from disk.
    major_faults: u64,
    /// Bytes this process has made the kernel fetch from the block layer, or
    /// `None` where `/proc/self/io` could not be read.
    read_bytes: Option<u64>,
}

/// The process counters at this instant, or `None` where they cannot be read.
fn process_vitals() -> Option<Vitals> {
    let (minor_faults, major_faults) = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|stat| parse_faults(&stat))?;
    Some(Vitals {
        cpu_seconds: process_cpu_seconds()?,
        minor_faults,
        major_faults,
        read_bytes: std::fs::read_to_string("/proc/self/io")
            .ok()
            .and_then(|io| parse_read_bytes(&io)),
    })
}

/// `read_bytes` from `/proc/<pid>/io`: bytes this process caused to be fetched
/// from the block layer.
///
/// The fault counters say whether a file came in through the page tables; this
/// says whether it came off the disk at all. A loader that reads its weights
/// with `read()` into a buffer waits on the disk inside that call, which costs
/// no CPU and raises no major fault -- so a phase whose wall clock grew while
/// its CPU time and its major faults did not is indistinguishable from one that
/// was never scheduled, until this number is looked at.
fn parse_read_bytes(io: &str) -> Option<u64> {
    io.lines().find_map(|line| {
        let rest = line.strip_prefix("read_bytes:")?;
        rest.trim().parse().ok()
    })
}

/// `minflt` and `majflt` from `/proc/<pid>/stat`: pages mapped in from memory,
/// and pages that had to be read from disk.
///
/// The programme name in that line is parenthesised and may itself contain
/// spaces and parentheses, so the fields are counted from the last `)` rather
/// than from the start: from there the first field is the state, and `minflt`
/// and `majflt` are the seventh and ninth.
fn parse_faults(stat: &str) -> Option<(u64, u64)> {
    let tail = &stat[stat.rfind(')')? + 1..];
    let f: Vec<&str> = tail.split_whitespace().collect();
    Some((f.get(7)?.parse().ok()?, f.get(9)?.parse().ok()?))
}

/// Seconds of CPU this process has been scheduled for so far, summed over
/// every thread, or `None` where the clock cannot be read.
///
/// `CLOCK_PROCESS_CPUTIME_ID` is the quantity the scheduler itself accounts,
/// already in seconds and already summed over threads, so it needs neither the
/// platform's tick rate nor a walk over per-thread readings. `None` rather than
/// zero on failure: zero would read as "this phase did no work", which is the
/// exact opposite of what an unreadable clock means.
fn process_cpu_seconds() -> Option<f64> {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a live `timespec` the call fills in, and the clock id is a
    // constant this platform defines.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut ts) };
    if rc != 0 {
        return None;
    }
    Some(ts.tv_sec as f64 + ts.tv_nsec as f64 / 1e9)
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

/// Publishes one `cogneva_boot_rss_bytes`, one
/// `cogneva_boot_phase_elapsed_seconds` and, where the CPU clock was read, one
/// `cogneva_boot_phase_cpu_seconds` series per recorded phase.
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
            if let Some(cpu_seconds) = mark.cpu_seconds {
                metrics.push(
                    RawMetric::new(BOOT_CPU_METRIC, cpu_seconds)
                        .with_label(crate::LABEL_PHASE, mark.phase),
                );
            }
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

    /// The phases a named series was published for, in the order published.
    fn series_phases(metrics: &[RawMetric], name: &str) -> Vec<String> {
        metrics
            .iter()
            .filter(|m| m.name == name)
            .map(|m| {
                m.labels
                    .get(crate::LABEL_PHASE)
                    .cloned()
                    .unwrap_or_default()
            })
            .collect()
    }

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

    /// One event as it reaches a log backend: its level and the fields the
    /// reader of this line is meant to find.
    #[derive(Clone, Debug)]
    struct Logged {
        level: tracing::Level,
        phase: Option<String>,
        rss: Option<u64>,
        cpu_seconds: Option<f64>,
        minor_faults: Option<u64>,
        major_faults: Option<u64>,
        read_bytes: Option<u64>,
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
                rss: None,
                cpu_seconds: None,
                minor_faults: None,
                major_faults: None,
                read_bytes: None,
            };
            let mut fields = Fields(&mut logged);
            event.record(&mut fields);
            self.0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(logged);
        }
    }

    /// What the shape test reads off each line: resident bytes, CPU seconds,
    /// minor faults, major faults.
    type Counters = (
        Option<u64>,
        Option<f64>,
        Option<u64>,
        Option<u64>,
        Option<u64>,
    );

    struct Fields<'a>(&'a mut Logged);

    impl tracing::field::Visit for Fields<'_> {
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "phase" {
                self.0.phase = Some(value.to_string());
            }
        }

        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "phase" {
                self.0.phase = Some(format!("{value:?}"));
            }
        }

        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            match field.name() {
                "rss" => self.0.rss = Some(value),
                "minor_faults" => self.0.minor_faults = Some(value),
                "major_faults" => self.0.major_faults = Some(value),
                "read_bytes" => self.0.read_bytes = Some(value),
                _ => {}
            }
        }

        fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
            if field.name() == "cpu_seconds" {
                self.0.cpu_seconds = Some(value);
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
        assert!(
            reached[0].cpu_seconds.is_some(),
            "the line a reached phase prints carries no cpu_seconds, so a slow phase \
             still cannot be told apart from a starved one: {reached:?}"
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

    /// What this clock has to do, and the one thing it is not asserted to do
    /// here: a phase that sleeps shows no CPU *in a process with only itself
    /// running*, and `cargo test` never gives it that -- the sibling tests run as
    /// threads of the same process, so the process's CPU time keeps rising while
    /// this test sleeps. Asserting the sleeping half went red at 0.05 s the first
    /// time it ran beside the rest of this crate's suite, which is the shape of a
    /// gate that fails for a reason unrelated to the code. So the test holds the
    /// half that is this process's own: work done here moves the clock by about
    /// the work done.
    #[test]
    fn the_cpu_clock_advances_with_the_work_this_process_does() {
        let before = process_cpu_seconds().expect("the process CPU clock is readable here");
        let start = Instant::now();
        let mut spin = 0u64;
        while start.elapsed() < std::time::Duration::from_millis(120) {
            spin = spin.wrapping_add(1);
        }
        std::hint::black_box(spin);
        let after = process_cpu_seconds().expect("the process CPU clock is readable here");
        assert!(
            after >= before,
            "the process CPU clock ran backwards: {before} -> {after}"
        );
        assert!(
            after - before >= 0.08,
            "120 ms of spinning moved the CPU clock by only {:.3} s: a phase that computed \
             would read the same as one that waited",
            after - before
        );
    }

    #[test]
    fn a_phase_announced_without_its_memory_still_carries_its_counters() {
        use tracing_subscriber::prelude::*;

        let vitals = Vitals {
            cpu_seconds: 1.5,
            minor_faults: 11,
            major_faults: 3,
            read_bytes: Some(77),
        };
        let logged = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(Capture(Arc::clone(&logged)));
        tracing::subscriber::with_default(subscriber, || {
            announce("serving", Some(4096), 0.0, Some(vitals));
            announce("serving", None, 0.0, Some(vitals));
            announce("serving", Some(4096), 0.0, None);
            announce("serving", None, 0.0, None);
        });

        let fields: Vec<Counters> = logged
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|e| {
                (
                    e.rss,
                    e.cpu_seconds,
                    e.minor_faults,
                    e.major_faults,
                    e.read_bytes,
                )
            })
            .collect();
        assert_eq!(
            fields,
            vec![
                (Some(4096), Some(1.5), Some(11), Some(3), Some(77)),
                (None, Some(1.5), Some(11), Some(3), Some(77)),
                (Some(4096), None, None, None, None),
                (None, None, None, None, None),
            ],
            "every shape of the line has to carry the counters it has: the shape that drops \
             the resident number is the one the reader greps when procfs is missing, and a \
             field present in only some shapes reads as absent in the others"
        );
    }

    #[test]
    fn faults_are_read_from_the_fields_after_the_programme_name() {
        // A real line's programme name is parenthesised and may itself contain
        // spaces and parentheses, which is why the fields are counted from the
        // last `)` rather than from the start of the line.
        let stat = "1234 (cog neva (x)) S 1 1234 1234 0 -1 4194560 111 0 0 0 5 6 0 0 20 0 7 0 99 1000 200 0 0 0 0 0 0 0 0 0 0 0";
        assert_eq!(parse_faults(stat), Some((111, 0)));

        assert_eq!(parse_faults("no parenthesis here"), None);
        assert_eq!(parse_faults("1234 (short) S 1"), None);
    }

    #[test]
    fn bytes_read_from_the_block_layer_are_read_from_the_line_that_names_them() {
        // `rchar` counts bytes handed back from the page cache too, so it is not
        // this number: a loader whose file is already cached reads gigabytes
        // without touching the disk, and that is exactly the case this counter
        // has to keep apart from one that waited on the disk.
        let io = "rchar: 999999999\n\
                  wchar: 12\n\
                  syscr: 40\n\
                  syscw: 0\n\
                  read_bytes: 1572864\n\
                  write_bytes: 0\n\
                  cancelled_write_bytes: 0\n";
        assert_eq!(parse_read_bytes(io), Some(1_572_864));

        assert_eq!(parse_read_bytes("rchar: 1\n"), None);
        assert_eq!(parse_read_bytes("read_bytes: not-a-number\n"), None);
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
        let timed = series_phases(&metrics, BOOT_ELAPSED_METRIC);
        assert_eq!(
            timed, phases,
            "the reading published a time series for a different set of phases than the memory series"
        );

        // The CPU series has to cover the same phases for the same reason. It is
        // absent as a whole on a platform whose CPU clock cannot be read, which
        // is why the empty case is allowed and a partial one is not: a reading
        // that covers some phases and not others would make a boot look like it
        // spent no CPU where the clock happened to fail.
        let cpu = series_phases(&metrics, BOOT_CPU_METRIC);
        assert!(
            cpu.is_empty() || cpu == phases,
            "the CPU series covers {cpu:?} while the phases are {phases:?}"
        );
        for sibling in [BOOT_ELAPSED_METRIC, BOOT_CPU_METRIC] {
            assert!(
                metrics
                    .iter()
                    .filter(|m| m.name == sibling)
                    .all(|m| m.value >= 0.0),
                "{sibling} cannot run backwards from the origin"
            );
        }
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
