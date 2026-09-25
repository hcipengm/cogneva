//! Which process drains the change queue, and how deep that queue is.
//!
//! The queue is not one directory shared by the deployment. `change_dir` is a
//! relative path in the shipped configuration, so it resolves against each
//! process's own working directory, and the two deployments mount different
//! volumes at those paths. The process that generates changes and the process
//! that lists them for the console therefore read different files, and a listing
//! served by the one that does not drain the queue is empty for a reason no
//! reading anywhere could tell apart from "the queue is empty" -- which is what
//! this module exists to make readable.
//!
//! One series is published by every process and the rest by the owner alone, the
//! same division the build cache uses:
//!
//! - `cogneva_evolution_change_queue_owner{dir}` -- 1 on the process that owns
//!   the executor role, 0 on the ones that do not. Published everywhere, because
//!   the question it answers is about the deployment rather than about this
//!   process: `1 - sum by (dir) (owner) > 0` says no process anywhere is set up
//!   to drain that queue, which is a state an operator has to be able to see
//!   whether or not this process is the one missing. A queue whose executor was
//!   disabled, or whose executor pod is gone, reads as a sum of zero rather than
//!   as a series that stopped being scraped.
//! - `cogneva_evolution_change_queue_pending{dir}` -- how many changes the
//!   pipeline would pick up, judged by the same predicate it acts on. Only the
//!   owner publishes it: a process that does not read the directory has nothing
//!   to report, and reporting zero would be the empty-queue reading this module
//!   is about.
//! - `cogneva_evolution_change_queue_oldest_seconds{dir}` -- how long the oldest
//!   waiting change has been waiting. This is the difference between a queue
//!   that is being worked through and one that is not being touched at all, and
//!   the depth alone cannot tell them apart.
//! - `cogneva_evolution_change_queue_poll_interval_seconds{dir}` -- the cycle
//!   interval this process consumes the queue on, so a rule can say "waiting
//!   more than six cycles" without carrying a copy of the interval that goes
//!   stale when it is configured differently.
//!
//! The wait is measured from the artifact's own modification time, not from the
//! `created_at` the listing carries: that one is stamped when the directory is
//! read, so every change in a queue reads as brand new and a queue nobody has
//! touched for a week would still report an age of zero.
//!
//! The queue directory is published as a path rather than as a directory
//! identity, unlike the build cache: it is usually not on a volume at all (it
//! lives in the process's working directory), so `dev:ino` would name a new
//! directory on every pod restart while naming the same queue. The path is
//! stable across restarts, and it is the same string the change listing reports
//! as the queue it read, so the two readings can be joined by a reader.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use cog_core::observability::{DimensionSpec, Observable, RawMetric, TraceFragment};
use cog_core::{EvolutionQueueView, SFResult};
use tracing::warn;

use crate::change_pipeline::ChangePipeline;
use crate::EvolutionEngine;

/// 1 on the process that owns the change queue, 0 on the ones that do not.
pub const QUEUE_OWNER_METRIC: &str = "cogneva_evolution_change_queue_owner";

/// Changes waiting in the queue, judged by the pipeline's own predicate.
pub const QUEUE_PENDING_METRIC: &str = "cogneva_evolution_change_queue_pending";

/// How long the oldest waiting change has been waiting, in seconds.
pub const QUEUE_OLDEST_METRIC: &str = "cogneva_evolution_change_queue_oldest_seconds";

/// The interval this process consumes the queue on, in seconds.
pub const QUEUE_POLL_INTERVAL_METRIC: &str = "cogneva_evolution_change_queue_poll_interval_seconds";

/// The label naming which queue a reading belongs to.
pub const DIR_LABEL: &str = "dir";

/// The queue one process reads, plus whether that process is the one meant to
/// drain it.
pub struct EvolutionQueueReadings {
    /// The directory as this process reads it, resolved to an absolute path.
    dir: PathBuf,
    /// Whether this process holds the executor role for that queue.
    owner: bool,
    /// The cycle interval this process consumes the queue on.
    poll_interval_secs: u64,
    /// The pipeline whose predicate decides what is pending, so the depth and
    /// the work the pipeline does are one answer rather than two.
    pipeline: ChangePipeline,
    /// The engine that knows each change's status; absent in a process that has
    /// no LLM, where the directory alone still says what is waiting.
    engine: Option<Arc<EvolutionEngine>>,
}

impl EvolutionQueueReadings {
    pub fn new(
        dir: impl Into<PathBuf>,
        owner: bool,
        poll_interval_secs: u64,
        pipeline: ChangePipeline,
        engine: Option<Arc<EvolutionEngine>>,
    ) -> Self {
        Self {
            dir: resolve_change_dir(&dir.into()),
            owner,
            poll_interval_secs,
            pipeline,
            engine,
        }
    }

    /// The queue this process reads, for a listing that has to say where it came
    /// from. Same object the metrics are published from, so the directory a
    /// reader sees in the API and the one a rule groups by cannot be two
    /// directories.
    pub fn view(&self) -> EvolutionQueueView {
        EvolutionQueueView {
            dir: self.dir.display().to_string(),
            owner: self.owner,
        }
    }

    pub fn owner(&self) -> bool {
        self.owner
    }

    /// The depth and age of the queue, from one pass over it.
    ///
    /// `None` when the pass could not be completed, which is published as
    /// nothing rather than as an empty queue: a queue whose depth could not be
    /// read and a queue that is empty call for opposite responses. The two
    /// numbers come from one pass because a count from one moment and an age
    /// from another could describe a queue that never existed.
    async fn depth(&self) -> Option<QueueDepth> {
        let pending = match self.pipeline.pending_changes(self.engine.as_deref()).await {
            Ok(pending) => pending,
            Err(e) => {
                warn!(
                    dir = %self.dir.display(),
                    error = %e,
                    "could not read the change queue; publishing no depth this pass"
                );
                return None;
            }
        };
        let oldest = oldest_age_secs(&self.dir, pending.iter().map(|p| p.artifact_id.as_str()))?;
        Some(QueueDepth {
            pending: pending.len(),
            oldest_secs: oldest,
        })
    }
}

/// A queue's depth and how long its oldest entry has been waiting.
struct QueueDepth {
    pending: usize,
    oldest_secs: u64,
}

/// The age of the oldest `.diff` in the queue, or `None` when the pending set
/// and the files on disk disagree.
///
/// An entry the listing calls pending but that cannot be stat'ed has no age, and
/// a queue whose entries all have no age is not a queue with an age of zero.
/// Refusing the pass is safe: the next one re-reads both from the directory.
fn oldest_age_secs<'a>(dir: &Path, ids: impl Iterator<Item = &'a str>) -> Option<u64> {
    let now = SystemTime::now();
    let mut ages = Vec::new();
    for id in ids {
        let modified = std::fs::metadata(dir.join(format!("{id}.diff")))
            .and_then(|m| m.modified())
            .ok()?;
        ages.push(
            now.duration_since(modified)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        );
    }
    // An empty queue has nothing waiting: zero is the age of a wait that has not
    // started, and the rule pairs it with the depth so it cannot be read as a
    // queue that has just been drained.
    Some(ages.into_iter().max().unwrap_or(0))
}

/// Where the process actually reads the queue, resolved the way it does.
///
/// The shipped configuration carries a relative path, and the pipeline reads it
/// as one, so the reference point is this process's working directory. Reported
/// resolved so a reader does not have to know which process is answering to know
/// which directory was read.
fn resolve_change_dir(dir: &Path) -> PathBuf {
    if dir.is_absolute() {
        return dir.to_path_buf();
    }
    let relative = dir.strip_prefix(".").unwrap_or(dir);
    match std::env::current_dir() {
        Ok(cwd) => cwd.join(relative),
        Err(_) => dir.to_path_buf(),
    }
}

#[async_trait]
impl Observable for EvolutionQueueReadings {
    /// The owner flag is published before anything is read, so the series a rule
    /// asks about exists even when the reading below it fails.
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let dir = self.dir.display().to_string();
        let mut out = vec![
            RawMetric::new(QUEUE_OWNER_METRIC, if self.owner { 1.0 } else { 0.0 })
                .with_label(DIR_LABEL, dir.clone()),
        ];
        if !self.owner {
            return Ok(out);
        }
        out.push(
            RawMetric::new(QUEUE_POLL_INTERVAL_METRIC, self.poll_interval_secs as f64)
                .with_label(DIR_LABEL, dir.clone()),
        );
        let Some(depth) = self.depth().await else {
            return Ok(out);
        };
        out.push(
            RawMetric::new(QUEUE_PENDING_METRIC, depth.pending as f64)
                .with_label(DIR_LABEL, dir.clone()),
        );
        out.push(
            RawMetric::new(QUEUE_OLDEST_METRIC, depth.oldest_secs as f64)
                .with_label(DIR_LABEL, dir),
        );
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The queue depth is not a per-dimension metric: every dimension reads the
    /// same directory, so declaring no dimension is what tells the collector to
    /// pull this observable once rather than once per question.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn readings(dir: &Path, owner: bool) -> EvolutionQueueReadings {
        let pipeline = ChangePipeline::new(std::env::current_dir().unwrap(), dir, true);
        EvolutionQueueReadings::new(dir, owner, 60, pipeline, None)
    }

    async fn value(readings: &EvolutionQueueReadings, name: &str) -> Option<f64> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.name == name)
            .map(|m| m.value)
    }

    async fn labels_of(
        readings: &EvolutionQueueReadings,
        name: &str,
    ) -> Option<Vec<(String, String)>> {
        readings
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .find(|m| m.name == name)
            .map(|m| m.labels.into_iter().collect())
    }

    /// A process that does not drain the queue reports that it does not, and
    /// reports nothing about a directory it does not read. The second half is
    /// the point: a zero depth from a process with no queue of its own reads
    /// exactly like an empty queue.
    #[tokio::test]
    async fn a_non_owner_reports_the_role_and_no_depth() {
        let dir = tempfile::tempdir().unwrap();
        tokio::fs::write(
            dir.path().join("waiting.diff"),
            "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n",
        )
        .await
        .unwrap();
        let readings = readings(dir.path(), false);
        assert_eq!(value(&readings, QUEUE_OWNER_METRIC).await, Some(0.0));
        assert_eq!(value(&readings, QUEUE_PENDING_METRIC).await, None);
        assert_eq!(value(&readings, QUEUE_OLDEST_METRIC).await, None);
        assert_eq!(value(&readings, QUEUE_POLL_INTERVAL_METRIC).await, None);
    }

    /// The owner reports the depth the pipeline would act on, the interval it
    /// acts on, and the label that says which queue both belong to.
    #[tokio::test]
    async fn the_owner_reports_its_depth_and_interval() {
        let dir = tempfile::tempdir().unwrap();
        for id in ["first", "second"] {
            tokio::fs::write(
                dir.path().join(format!("{id}.diff")),
                "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n",
            )
            .await
            .unwrap();
        }
        // Not a change: the queue is the `.diff` files in the directory, so
        // anything else in there must not be counted as waiting work.
        tokio::fs::write(dir.path().join("notes.txt"), "scratch\n")
            .await
            .unwrap();
        let readings = readings(dir.path(), true);
        assert_eq!(value(&readings, QUEUE_OWNER_METRIC).await, Some(1.0));
        assert_eq!(value(&readings, QUEUE_PENDING_METRIC).await, Some(2.0));
        assert_eq!(
            value(&readings, QUEUE_POLL_INTERVAL_METRIC).await,
            Some(60.0)
        );
        let labels = labels_of(&readings, QUEUE_PENDING_METRIC).await.unwrap();
        let dir_label = labels
            .iter()
            .find(|(k, _)| k == DIR_LABEL)
            .map(|(_, v)| v.clone());
        assert_eq!(dir_label, Some(dir.path().display().to_string()));
    }

    /// The age is the artifact's own, taken from the file, because the listing
    /// stamps every row with the moment it was read. A queue nothing has touched
    /// has to age.
    #[tokio::test]
    async fn the_age_comes_from_the_artifact_not_from_the_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("old.diff");
        tokio::fs::write(&path, "--- a/x.txt\n+++ b/x.txt\n@@ -0,0 +1 @@\n+one\n")
            .await
            .unwrap();
        let readings = readings(dir.path(), true);
        let fresh = value(&readings, QUEUE_OLDEST_METRIC).await.unwrap();
        assert!(fresh < 5.0, "a change just written is not old: {fresh}");

        // Backdate the artifact the way a queue left standing overnight would
        // look, without waiting for the age to pass.
        let two_hours_ago = SystemTime::now() - std::time::Duration::from_secs(7200);
        let file = std::fs::File::options().write(true).open(&path).unwrap();
        file.set_modified(two_hours_ago).unwrap();
        let aged = value(&readings, QUEUE_OLDEST_METRIC).await.unwrap();
        assert!(
            (7195.0..=7210.0).contains(&aged),
            "the age has to follow the artifact: {aged}"
        );
    }

    /// An empty queue is a depth of zero and an age of zero, and both series
    /// exist: absence would otherwise be indistinguishable from a reading that
    /// failed, which is the state the depth series must not be able to hide in.
    #[tokio::test]
    async fn an_empty_queue_still_reports_both_series() {
        let dir = tempfile::tempdir().unwrap();
        let readings = readings(dir.path(), true);
        assert_eq!(value(&readings, QUEUE_PENDING_METRIC).await, Some(0.0));
        assert_eq!(value(&readings, QUEUE_OLDEST_METRIC).await, Some(0.0));
    }

    /// A queue directory that is not there is a reading that failed, not an
    /// empty queue: nothing is published below the role flag.
    #[tokio::test]
    async fn an_unreadable_queue_publishes_no_depth() {
        let dir = tempfile::tempdir().unwrap();
        let readings = readings(&dir.path().join("gone"), true);
        assert_eq!(value(&readings, QUEUE_OWNER_METRIC).await, Some(1.0));
        assert_eq!(value(&readings, QUEUE_PENDING_METRIC).await, None);
        assert_eq!(value(&readings, QUEUE_OLDEST_METRIC).await, None);
    }

    /// The view a listing reports is the same directory the metrics are
    /// published under, so a reader can join "which queue did this listing read"
    /// to "what does that queue hold".
    #[tokio::test]
    async fn the_view_names_the_directory_the_metrics_name() {
        let dir = tempfile::tempdir().unwrap();
        let readings = readings(dir.path(), true);
        let view = readings.view();
        assert!(view.owner);
        assert_eq!(view.dir, dir.path().display().to_string());
        let labels = labels_of(&readings, QUEUE_OWNER_METRIC).await.unwrap();
        assert_eq!(
            labels
                .iter()
                .find(|(k, _)| k == DIR_LABEL)
                .map(|(_, v)| v.clone()),
            Some(view.dir)
        );
    }

    /// A relative `change_dir` is read against the process's working directory,
    /// which is what the pipeline does with it.
    #[test]
    fn a_relative_change_dir_is_resolved_against_the_working_directory() {
        let resolved = resolve_change_dir(Path::new("./evolution-changes"));
        assert!(resolved.is_absolute());
        assert!(resolved.ends_with("evolution-changes"));
        assert!(
            !resolved.to_string_lossy().contains("/./"),
            "the reported path should not carry the dot segment: {}",
            resolved.display()
        );
    }
}
