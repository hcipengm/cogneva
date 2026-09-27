//! A change inside the apply/test flight, as a reading.
//!
//! Verifying one change is the longest stretch of the evolution cycle that
//! publishes nothing. `apply_and_test_in` logs that it is applying the change
//! and then runs the promotion gate, the apply, the format check, the wait for a
//! build slot and `cargo test --workspace` before it says anything else, and
//! every reading about that work is derived after it ends:
//! `cogneva_verification_last_run_seconds` is written when a run ends,
//! `cogneva_evolution_build_outcomes_total` when the release build after it
//! ends, the change's status when the pipeline returns. In between, a process
//! sitting inside the flight and a process whose cycle has stopped at the flight
//! have the same face: nothing is moving.
//!
//! That is not hypothetical. On 2026-09-27 a change spent 40 minutes between
//! `Applying evolution change` and its landing commit, and the strongest reading
//! anyone could take from the deployment was that no file had been written for
//! the last 18 of them. The flight was healthy -- a full workspace test on this
//! host takes that long -- but "healthy and slow" and "stopped" were one face,
//! and the investigation had to reconstruct the timeline from PVC mtimes.
//!
//! So the flight publishes itself while it runs:
//!
//! - `cogneva_evolution_change_flight_seconds{dir}` -- how long the change now in
//!   the flight has been in it. Computed when the scrape arrives, from a stamp
//!   the flight leaves behind when it starts, so nothing has to publish it: a
//!   publisher task is one more thing that can die silently, and its silence is
//!   the state this reading exists to report. Absent while no flight runs, which
//!   on this face is a reading rather than a silence -- the value is derived at
//!   scrape time from the process's own state, not left over from the last write.
//! - `cogneva_evolution_change_flight_budget_seconds{dir}` -- the wall the flight
//!   is judged against: the two bounds of its two slowest steps, added. Published
//!   on the same axis so a rule compares the age against this deployment's own
//!   numbers rather than a constant that goes stale when a budget is configured
//!   differently.
//!
//! The wall is the *sum* because both steps are inside the flight: the wait for a
//! build slot is bounded by the gate's wait budget and the test run by the
//! verification budget, and a flight that waits out the first and then runs the
//! second is doing exactly what it is allowed to do. Both numbers are read from
//! the objects that enforce them (the verification budget the pipeline holds and
//! the build gate configuration the same process installed), never re-derived
//! from the configuration document by a second path that could disagree with
//! what is being enforced. The steps that are neither -- `git apply`, the format
//! check, the rollback after a refusal -- take seconds to a minute, so a flight
//! past the wall is one whose bounded steps can no longer still be running.
//!
//! A process that runs no flights publishes nothing here. The question "is any
//! process even set up to verify a change" is answered one family over, by
//! `cogneva_evolution_change_queue_owner`, and both are the same decision made
//! once (`executor_enabled`): a second role flag here would be a second home for
//! that fact, and the two could drift into "the flights are idle" and "nobody
//! verifies anything" being told apart by neither.
//!
//! The change's id is deliberately not a label. It is a per-object value whose
//! domain grows with every change the deployment ever processes, and a series
//! keyed by it has no reclaim path -- the shape the identity-key rule exists to
//! keep out of the series face. It travels on the log line the flight starts
//! with, which is where an operator who sees this reading goes next.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::SFResult;

use crate::evolution_queue_readings::{resolve_change_dir, DIR_LABEL};

/// How long the change now in the flight has been in it, in seconds.
pub const FLIGHT_SECONDS_METRIC: &str = "cogneva_evolution_change_flight_seconds";

/// The wall the flight is judged against, in seconds.
pub const FLIGHT_BUDGET_SECONDS_METRIC: &str = "cogneva_evolution_change_flight_budget_seconds";

/// Where a change is in its apply/test flight, as one process sees it.
pub struct EvolutionFlightReadings {
    /// The queue this process reads, resolved the way the pipeline resolves it,
    /// so a rule groups the flight by the same directory the queue readings
    /// report and the two can be joined.
    dir: PathBuf,
    /// The wall, in seconds. `None` on a process that runs no flights: it
    /// publishes nothing in this family rather than a zero, which on the axis a
    /// rule compares would read as a flight bounded at zero seconds -- every
    /// flight over its wall before it starts.
    budget_secs: Option<u64>,
    /// When the flight in progress started it, or `None` when no flight is.
    ///
    /// A monotone instant rather than a wall-clock stamp, so a step of the host
    /// clock is not a flight that has been running since before it started. It
    /// is `None` and not a zero stamp on purpose: a zero would be a value the
    /// first millisecond of a flight also produces, and the reading would then
    /// have to guess which of the two it was looking at.
    started: Mutex<Option<Instant>>,
}

impl EvolutionFlightReadings {
    /// The readings of a process that runs flights, with the wall those flights
    /// are bounded by.
    pub fn new(dir: impl Into<PathBuf>, wall_secs: u64) -> Self {
        Self {
            dir: resolve_change_dir(&dir.into()),
            budget_secs: Some(wall_secs),
            started: Mutex::new(None),
        }
    }

    /// The readings of a process that runs no flights, which publishes nothing.
    pub fn none(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: resolve_change_dir(&dir.into()),
            budget_secs: None,
            started: Mutex::new(None),
        }
    }

    /// Run one flight under the reading.
    ///
    /// The stamp is set before the work is polled and cleared when the guard
    /// drops, which is the moment the flight ends -- including when it ends by
    /// unwinding, where a `clear` at the end of the block would be skipped and a
    /// panicked flight would go on reading as an in-flight one.
    pub async fn cover<F: std::future::Future>(self: &Arc<Self>, work: F) -> F::Output {
        let _flight = self.begin();
        work.await
    }

    /// Begin a flight, taking the guard that ends it.
    pub fn begin(self: &Arc<Self>) -> Flight {
        self.stamp(Some(Instant::now()));
        Flight {
            readings: Arc::clone(self),
        }
    }

    /// How long the flight in progress has been running; `None` when none is.
    fn age_secs(&self) -> Option<u64> {
        self.held()
            .as_ref()
            .map(|started| started.elapsed().as_secs())
    }

    /// The start of the flight in progress, if one is.
    fn held(&self) -> std::sync::MutexGuard<'_, Option<Instant>> {
        self.started.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Set or clear the stamp; the two happen under one lock, so a scrape never
    /// sees a half-updated pair.
    fn stamp(&self, at: Option<Instant>) {
        *self.held() = at;
    }
}

/// The reading a flight leaves behind while it runs; dropping it ends the flight.
pub struct Flight {
    readings: Arc<EvolutionFlightReadings>,
}

impl Drop for Flight {
    fn drop(&mut self) {
        self.readings.stamp(None);
    }
}

#[async_trait]
impl Observable for EvolutionFlightReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let Some(wall) = self.budget_secs else {
            return Ok(Vec::new());
        };
        let dir = self.dir.display().to_string();
        let mut out = vec![RawMetric::new(FLIGHT_BUDGET_SECONDS_METRIC, wall as f64)
            .with_label(DIR_LABEL, dir.clone())];
        if let Some(age) = self.age_secs() {
            out.push(RawMetric::new(FLIGHT_SECONDS_METRIC, age as f64).with_label(DIR_LABEL, dir));
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The flight is not a per-dimension question: every dimension reads the
    /// same process, so this is pulled once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    async fn metrics(readings: &EvolutionFlightReadings) -> Vec<RawMetric> {
        readings.collect_metrics("").await.unwrap()
    }

    async fn value(readings: &EvolutionFlightReadings, name: &str) -> Option<f64> {
        metrics(readings)
            .await
            .into_iter()
            .find(|m| m.name == name)
            .map(|m| m.value)
    }

    /// The flight publishes its age *and* the wall on the same axis, and the
    /// label that joins them to the queue readings is the resolved directory --
    /// the same string `EvolutionQueueReadings` reports.
    #[tokio::test]
    async fn an_in_flight_change_publishes_its_age_and_the_wall() {
        let dir = tempfile::tempdir().unwrap();
        let readings = Arc::new(EvolutionFlightReadings::new(dir.path(), 5400));
        let flight = readings.begin();
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, Some(0.0));
        assert_eq!(
            value(&readings, FLIGHT_BUDGET_SECONDS_METRIC).await,
            Some(5400.0)
        );
        let labels = metrics(&readings)
            .await
            .into_iter()
            .find(|m| m.name == FLIGHT_SECONDS_METRIC)
            .unwrap()
            .labels;
        assert_eq!(
            labels.get(DIR_LABEL),
            Some(&dir.path().display().to_string())
        );
        drop(flight);
        // The flight is over: the age series is gone, and what remains is the
        // wall, which is a property of the process rather than of the flight.
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, None);
        assert_eq!(
            value(&readings, FLIGHT_BUDGET_SECONDS_METRIC).await,
            Some(5400.0)
        );
    }

    /// The age is derived when the scrape arrives, from a stamp the flight left
    /// behind: nobody publishes it, and it keeps growing while the flight runs.
    #[tokio::test]
    async fn the_age_is_computed_at_the_scrape() {
        let dir = tempfile::tempdir().unwrap();
        let readings = Arc::new(EvolutionFlightReadings::new(dir.path(), 5400));
        let flight = readings.begin();
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, Some(0.0));
        tokio::time::sleep(Duration::from_millis(1_100)).await;
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, Some(1.0));
        drop(flight);
    }

    /// A flight that ends by unwinding still ends. The stamp is cleared by the
    /// guard rather than by the code after the work, so a panic inside the
    /// verification cannot leave a reading that goes on growing forever -- which
    /// is the reading a rule would then alert on, for a flight that is gone.
    #[tokio::test]
    async fn a_flight_that_unwinds_is_cleared() {
        let dir = tempfile::tempdir().unwrap();
        let readings = Arc::new(EvolutionFlightReadings::new(dir.path(), 5400));
        let panicking = Arc::clone(&readings);
        let joined = tokio::spawn(async move {
            panicking
                .cover(async { panic!("the verification panicked") })
                .await
        })
        .await;
        assert!(joined.is_err(), "the flight was meant to unwind");
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, None);
    }

    /// A process that runs no flights publishes nothing at all, rather than a
    /// zero wall that would read as a flight bounded at zero seconds.
    #[tokio::test]
    async fn a_process_that_runs_no_flights_publishes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let readings = EvolutionFlightReadings::none(dir.path());
        assert!(metrics(&readings).await.is_empty());
    }

    /// A flight that just started reads as running for zero seconds, and not as
    /// no flight at all. The two are the states a rule tells apart -- "over its
    /// wall" versus "nothing is being verified" -- so the reading has to have a
    /// distinct value for each from the first instant of the flight.
    #[tokio::test]
    async fn a_flight_at_its_first_instant_is_not_absent() {
        let dir = tempfile::tempdir().unwrap();
        let readings = Arc::new(EvolutionFlightReadings::new(dir.path(), 3600));
        let entered = readings.begin();
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, Some(0.0));
        drop(entered);
        assert_eq!(value(&readings, FLIGHT_SECONDS_METRIC).await, None);
    }

    /// A relative `change_dir` resolves against this process's working
    /// directory, the way the pipeline and the queue readings resolve it: a
    /// reader joins the two families by this label, so the two must not resolve
    /// it differently.
    #[test]
    fn the_directory_is_resolved_the_way_the_queue_reads_it() {
        let readings = EvolutionFlightReadings::new("./evolution-changes", 3600);
        assert_eq!(
            readings.dir,
            resolve_change_dir(std::path::Path::new("./evolution-changes"))
        );
    }
}
