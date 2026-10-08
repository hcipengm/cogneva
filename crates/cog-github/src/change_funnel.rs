//! Where every change the platform still holds is sitting, by entry point.
//!
//! Generation, verification and landing each record what they did, and none of
//! them answers the question the loop is judged by: whether intents are
//! turning into commits. That answer existed only as a directory listing —
//! which is how "36 retired, 0 landed" was found at all — and a reading that
//! needs someone to run `ls` is not one the system can act on.
//!
//! The census counts the durable records instead of incrementing counters as
//! stages pass. A counter is absent until its first event, so a stage that has
//! never once happened publishes nothing, and "never happened" then looks
//! exactly like "never wired up" — the shape of the fault this exists to
//! expose. Derived from the records, every stage has a value from the first
//! scrape, zeros included, and the reading cannot drift from the records
//! because it is computed out of them.

use std::collections::{BTreeMap, HashMap};

use cog_core::{EvolutionIntent, GeneratedChange};

use crate::landing::{load_records, LandingRecord, LandingState};

/// How many changes sit at each stage, by the entry point that produced them.
pub use cog_core::metric_names::CHANGE_FUNNEL as CHANGE_FUNNEL_METRIC;

/// How long a census cell may stand unchanged before it is stamped again.
///
/// The store hands back every gauge series' newest row forever, so a cell's
/// value alone cannot tell a reader whether its writer is quiet or gone —
/// nothing in the number moves in either case. The
/// `_observed_timestamp_seconds` companion is the reading that separates them,
/// and it separates them only while the writer keeps stamping a cell that is
/// not moving. So a reader that says "the writer behind this cell is still
/// there" has to look back further than this: a window no longer than the gap
/// is satisfied by a live writer sitting between two of its own stamps, which
/// is how a rule fires on a working system.
///
/// The number matches the durable pool gauges' own heartbeat for the same
/// reason it exists there: one cadence for "unchanged but still observed" is
/// easier to hold than two, and the readers of both are compared to it.
pub const CENSUS_HEARTBEAT: std::time::Duration = std::time::Duration::from_secs(1800);

/// Whether a census cell needs a row now.
///
/// The decision is split out of the pass so it can be read and exercised on its
/// own: the pass also talks to the backend, and a rule that can only be checked
/// through a store is a rule nobody checks. `last` is what this process last
/// got through for that cell — count and when — and is `None` for a cell this
/// process has never written, which is always written so a cell is never absent
/// from a process's first scrape.
pub(crate) fn census_needs_write(
    last: Option<(u64, std::time::Instant)>,
    count: u64,
    now: std::time::Instant,
    heartbeat: std::time::Duration,
) -> bool {
    match last {
        None => true,
        Some((written, at)) => written != count || now.duration_since(at) >= heartbeat,
    }
}

/// How many changes have ended each way, by the entry point that produced them.
///
/// The census cannot answer this one, and the reason is worth stating: a record
/// that lands is removed once its CI verdict comes back green, so a landing
/// leaves the directory and the census's `landed` cell drops back to zero. A
/// rate read off the census would therefore be wrong in the direction of
/// looking healthy. Fate is cumulative and outlives the record.
///
/// Labeled `fate` rather than `stage`, although the two overlap on the terminal
/// values: a counter labelled `stage` would accept `stage="staged"` and answer
/// nothing, which reads exactly like "nothing was ever staged". The label names
/// the question — which way did it end — and its values are the only ones it
/// can take.
pub use cog_core::metric_names::CHANGE_FATE_TOTAL as CHANGE_FATE_METRIC;

/// A way a change has ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunnelFate {
    /// Verified and committed to the base branch.
    Landed,
    /// Verification settled it as never landing.
    Retired,
}

impl FunnelFate {
    /// Every value. A producer that publishes one series per class needs the
    /// classes it has none of as well, or an empty class reads exactly like a
    /// class that was never wired up. The list is the enum, so adding a
    /// variant cannot leave a reader silently short of a series.
    pub const ALL: [FunnelFate; 2] = [FunnelFate::Landed, FunnelFate::Retired];

    /// The label spelling. These are the only values a change can end with
    /// here: every other stage is a place to wait, not a way to finish.
    pub fn as_str(self) -> &'static str {
        match self {
            FunnelFate::Landed => "landed",
            FunnelFate::Retired => "retired",
        }
    }
}

/// A stage a produced change can be sitting in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FunnelStage {
    /// Produced and withheld: the contribution policy waits for the owner to
    /// approve publication, so the change is not in the landing channel yet.
    Staged,
    /// In the landing channel, not yet verified by a sandbox.
    Unverified,
    /// Verified and committed to the base branch.
    Landed,
    /// Verification settled it as never landing. Terminal.
    Retired,
}

impl FunnelStage {
    /// Every stage, because the census has to publish the empty ones too.
    pub const ALL: [FunnelStage; 4] = [
        FunnelStage::Staged,
        FunnelStage::Unverified,
        FunnelStage::Landed,
        FunnelStage::Retired,
    ];

    /// The label spelling. The variants are the whole domain, so these are too.
    pub fn as_str(self) -> &'static str {
        match self {
            FunnelStage::Staged => "staged",
            FunnelStage::Unverified => "unverified",
            FunnelStage::Landed => "landed",
            FunnelStage::Retired => "retired",
        }
    }
}

/// One cell of the census.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FunnelPoint {
    /// The entry point that produced the changes counted here.
    pub intent: EvolutionIntent,
    /// Where they are sitting.
    pub stage: FunnelStage,
    /// How many are there.
    pub count: u64,
}

/// Count the held changes by entry point and stage.
///
/// Pure, and deliberately total: one point for every pair the two enums allow,
/// zeros included. Returning only the non-empty cells would rebuild the
/// problem the census exists to solve, because the empty cells are the
/// interesting ones — an entry point whose every change was retired shows up
/// as a zero under `landed`, and a reader has to be able to see that zero.
///
/// The two inputs are the two places a change can be waiting: the landing
/// channel's records, and the staging directory the contribution policy uses
/// when it is not publishing automatically. A change is in one or the other,
/// never both, so the cells add up to everything the platform is holding.
pub fn census(records: &[LandingRecord], staged: &[GeneratedChange]) -> Vec<FunnelPoint> {
    let mut counts: BTreeMap<(EvolutionIntent, FunnelStage), u64> = BTreeMap::new();
    for intent in EvolutionIntent::ALL {
        for stage in FunnelStage::ALL {
            counts.insert((intent, stage), 0);
        }
    }

    for record in records {
        let stage = match record.state {
            LandingState::Unverified => FunnelStage::Unverified,
            LandingState::Landed => FunnelStage::Landed,
            LandingState::Retired => FunnelStage::Retired,
        };
        *counts
            .entry((intent_of(&record.change), stage))
            .or_insert(0) += 1;
    }
    for change in staged {
        *counts
            .entry((intent_of(change), FunnelStage::Staged))
            .or_insert(0) += 1;
    }

    counts
        .into_iter()
        .map(|((intent, stage), count)| FunnelPoint {
            intent,
            stage,
            count,
        })
        .collect()
}

/// Every (entry point, fate) pair the fate counter can take.
///
/// A counter has no series until its first increment, so with nothing landed
/// and nothing retired yet, "no change has ended this way since this process
/// started" and "this counter was never wired up" are the same reading: both
/// are the absence of a series. The publisher walks this list once per process
/// and records a zero for each pair, which is what creates the series — a
/// counter's `inc_by(0)` adds nothing to the value and everything to the
/// series. The census already publishes its empty cells for the same reason;
/// this is the half the census cannot carry, because a landing takes its
/// record with it when it succeeds.
pub fn fate_domain() -> Vec<(EvolutionIntent, FunnelFate)> {
    let mut domain = Vec::with_capacity(EvolutionIntent::ALL.len() * FunnelFate::ALL.len());
    for intent in EvolutionIntent::ALL {
        for fate in FunnelFate::ALL {
            domain.push((intent, fate));
        }
    }
    domain
}

/// Whether a landing is the one that counts, given the record already on
/// disk.
///
/// The landing path is idempotent: an attempt that finds the change already on
/// the branch returns without committing again. Counting that as a landing
/// would turn a retry into a second success and inflate the rate the fate
/// counter feeds, so the count is taken on the edge — a record that is already
/// `Landed` is the idempotency check answering, not a new arrival.
pub(crate) fn is_first_landing(existing: Option<&LandingRecord>) -> bool {
    existing
        .map(|r| r.state != LandingState::Landed)
        .unwrap_or(true)
}

/// The entry point a change came from. A record written before the intent was
/// carried on the change has none, and the census says so instead of guessing
/// from the goal text: an unattributed change is a fact about the record, and
/// spreading it across the named classes would make each of them wrong.
pub(crate) fn intent_of(change: &GeneratedChange) -> EvolutionIntent {
    change.intent.unwrap_or(EvolutionIntent::Unattributed)
}

impl crate::landing::MainChannel {
    /// Count one change arriving at a terminal stage.
    ///
    /// Called on the edge only — the call sites check the record's previous
    /// state first — because a counter that re-counts a retried or already
    /// settled change would turn one landing into two and make the rate it
    /// feeds meaningless. A failure to record is logged and dropped, like the
    /// landing failure counter's: losing the reading must not turn one outcome
    /// into another.
    pub(crate) async fn note_change_fate(&self, change: &GeneratedChange, fate: FunnelFate) {
        let Some(metrics) = self.metrics_handle() else {
            return;
        };
        let labels = HashMap::from([
            ("intent".to_string(), intent_of(change).as_str().to_string()),
            ("fate".to_string(), fate.as_str().to_string()),
        ]);
        if let Err(e) = metrics
            .record_counter(CHANGE_FATE_METRIC, 1.0, labels)
            .await
        {
            tracing::warn!(fate = fate.as_str(), error = %e, "cannot record a change's fate");
        }
    }

    /// Publish the census, one gauge series per (entry point, stage), and seed
    /// the fate counter's whole domain — the latter once per process.
    ///
    /// Called on the landing channel's own tick. Failures are logged and the
    /// pass is abandoned: the backend being unreachable is one fact, and
    /// reporting it once per cell per tick would drown the log line that
    /// matters.
    pub async fn publish_funnel(&self) {
        let points = census(
            &load_records().await,
            &crate::pending_changes::load_pending().await,
        );
        if !self.publish_census(&points).await {
            return;
        }
        let Some(metrics) = self.metrics_handle() else {
            return;
        };
        // The fate counter is seeded so that a class nothing has reached yet
        // is a zero rather than a missing series. Recording a zero creates the
        // series without moving its value, but it is not free: the backend
        // appends a sample row for every record, zero or not, and the domain
        // is 18 series wide. Doing this on each tick therefore wrote about
        // 52k rows a day into a store that is capped and shared with every
        // other reading, and each of those rows evicted somebody else's. Once
        // per process is what the seed needs — the series has to exist from
        // this process's first tick, and it does — so the flag is set only
        // after the whole domain has been accepted, and a pass that failed
        // halfway retries on the next tick.
        if !self
            .fate_domain_seeded
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            for (intent, fate) in fate_domain() {
                let labels = HashMap::from([
                    ("intent".to_string(), intent.as_str().to_string()),
                    ("fate".to_string(), fate.as_str().to_string()),
                ]);
                if let Err(e) = metrics
                    .record_counter(CHANGE_FATE_METRIC, 0.0, labels)
                    .await
                {
                    tracing::warn!(error = %e, "cannot seed the change fate counter");
                    return;
                }
            }
            self.fate_domain_seeded
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Write the census cells that moved, and the ones that have stood still
    /// long enough to need a stamp, and report whether the pass went through.
    ///
    /// Split out of the tick so the rule it follows can be read on its own:
    /// a cell is written when its count moved, or when this process last wrote
    /// it more than [`CENSUS_HEARTBEAT`] ago — never merely because a tick
    /// happened.
    ///
    /// Returns `false` when a write failed and the pass was abandoned — the
    /// backend being unreachable is one fact, and the caller has to stop for
    /// the same reason rather than because a cell was skipped.
    pub(crate) async fn publish_census(&self, points: &[FunnelPoint]) -> bool {
        let Some(metrics) = self.metrics_handle() else {
            return true;
        };
        let now = std::time::Instant::now();
        for point in points {
            let key = (
                point.intent.as_str().to_string(),
                point.stage.as_str().to_string(),
            );
            // Two reasons to write, and they carry different readings. A count
            // that moved is the reading the census exists for. A count that
            // stood still past the heartbeat is the stamp that says its writer
            // is still there: the store hands back each series' newest row
            // forever, so an unstamped cell reads the same whether the producer
            // is quiet or gone, and `changes(<cell>_observed_timestamp_seconds)`
            // — the only reading that separates the two — would say "gone" for
            // a census that is merely not converting anything. What the
            // heartbeat costs is bounded and small: a cell that never moves is
            // one row per heartbeat rather than the 36-per-tick flood that made
            // this a write-on-move in the first place, and the sweep still
            // exempts every gauge series' newest row, so the value stays put.
            let last = {
                let written = self
                    .census_written
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                written.get(&key).copied()
            };
            if !census_needs_write(last, point.count, now, CENSUS_HEARTBEAT) {
                continue;
            }
            let labels = HashMap::from([
                ("intent".to_string(), key.0.clone()),
                ("stage".to_string(), key.1.clone()),
            ]);
            if let Err(e) = metrics
                .record_gauge(CHANGE_FUNNEL_METRIC, point.count as f64, labels)
                .await
            {
                tracing::warn!(error = %e, "cannot record the change funnel census");
                return false;
            }
            let mut written = self
                .census_written
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            written.insert(key, (point.count, std::time::Instant::now()));
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn record(intent: Option<EvolutionIntent>, state: LandingState) -> LandingRecord {
        LandingRecord {
            change: GeneratedChange {
                change_id: format!("c-{:?}-{:?}", intent, state),
                intent,
                ..Default::default()
            },
            base: "main".into(),
            landed_rev: String::new(),
            state,
            failure_recorded: false,
            redriven: false,
            unlanded_reported: false,
            inherited_ci_reported: false,
            retired_reason: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn changed(intent: Option<EvolutionIntent>) -> GeneratedChange {
        GeneratedChange {
            change_id: "staged".into(),
            intent,
            ..Default::default()
        }
    }

    fn count(points: &[FunnelPoint], intent: EvolutionIntent, stage: FunnelStage) -> u64 {
        points
            .iter()
            .find(|p| p.intent == intent && p.stage == stage)
            .map(|p| p.count)
            .unwrap_or_else(|| panic!("census has no cell for {intent:?}/{stage:?}"))
    }

    /// The whole point of the census: a stage nothing has reached yet is
    /// published as a zero rather than omitted. Absent and zero read the same
    /// way to a scraper, and the zero is the reading that says the loop is not
    /// converting.
    #[test]
    fn an_empty_funnel_still_publishes_every_cell() {
        let points = census(&[], &[]);
        assert_eq!(
            points.len(),
            EvolutionIntent::ALL.len() * FunnelStage::ALL.len()
        );
        for point in &points {
            assert_eq!(
                point.count, 0,
                "{:?}/{:?} is not zero",
                point.intent, point.stage
            );
        }
    }

    /// The two reasons a cell gets a row, and the one that does not.
    ///
    /// The heartbeat half is the one worth pinning: a census that stops
    /// converting anything stops moving its counts, and a writer that stops
    /// stamping on that account leaves the companion reading the same for a
    /// live cell as for an abandoned one — so the rule that would ask whether
    /// the cell's writer is still there has nothing to ask.
    #[test]
    fn a_cell_is_stamped_when_it_moves_or_when_the_heartbeat_is_due() {
        let heartbeat = std::time::Duration::from_secs(1800);
        let t0 = std::time::Instant::now();

        // Never written by this process: always, so a process's first scrape
        // carries every cell and no stage is absent.
        assert!(census_needs_write(None, 3, t0, heartbeat));
        // The count moved: the reading the census exists for.
        assert!(census_needs_write(Some((3, t0)), 4, t0, heartbeat));
        // Unchanged, inside the heartbeat: no row says anything that another
        // row did not already say.
        assert!(!census_needs_write(
            Some((3, t0)),
            3,
            t0 + std::time::Duration::from_secs(1799),
            heartbeat
        ));
        // Unchanged, heartbeat due: not a reading of the count, a reading of
        // the writer.
        assert!(census_needs_write(
            Some((3, t0)),
            3,
            t0 + heartbeat,
            heartbeat
        ));
    }

    /// The seed has to cover the same pairs the counter can take, and each of
    /// them exactly once: a class left out of the domain is a series that
    /// stays absent until it happens to fire, which is the reading the seed
    /// exists to remove.
    #[test]
    fn the_fate_domain_is_every_pair_the_counter_can_take() {
        let domain = fate_domain();
        assert_eq!(
            domain.len(),
            EvolutionIntent::ALL.len() * FunnelFate::ALL.len()
        );
        for intent in EvolutionIntent::ALL {
            for fate in FunnelFate::ALL {
                assert!(
                    domain.contains(&(intent, fate)),
                    "{intent:?}/{fate:?} is not in the domain"
                );
            }
        }
        let mut seen: Vec<(EvolutionIntent, FunnelFate)> = Vec::new();
        for pair in &domain {
            assert!(!seen.contains(pair), "{pair:?} appears twice");
            seen.push(*pair);
        }
    }

    /// A change retired by verification and a change landed are different
    /// fates, and the census has to keep them apart: folded into one "handled"
    /// count, a funnel that only ever rejects would look like one that works.
    #[test]
    fn the_stages_count_the_records_that_are_actually_in_them() {
        let points = census(
            &[
                record(Some(EvolutionIntent::SelfSignal), LandingState::Retired),
                record(Some(EvolutionIntent::SelfSignal), LandingState::Retired),
                record(Some(EvolutionIntent::CiFix), LandingState::Unverified),
                record(Some(EvolutionIntent::CiFix), LandingState::Landed),
            ],
            &[changed(Some(EvolutionIntent::CiFix))],
        );

        assert_eq!(
            count(&points, EvolutionIntent::SelfSignal, FunnelStage::Retired),
            2
        );
        assert_eq!(
            count(&points, EvolutionIntent::SelfSignal, FunnelStage::Landed),
            0
        );
        assert_eq!(
            count(&points, EvolutionIntent::CiFix, FunnelStage::Unverified),
            1
        );
        assert_eq!(
            count(&points, EvolutionIntent::CiFix, FunnelStage::Landed),
            1
        );
        assert_eq!(
            count(&points, EvolutionIntent::CiFix, FunnelStage::Staged),
            1
        );
    }

    /// A record from before the intent travelled on the change is reported as
    /// unattributed, not spread over the named classes. Attributing it to one
    /// would make that class's numbers wrong in a direction nobody can see.
    #[test]
    fn a_change_without_an_intent_is_counted_as_unattributed() {
        let points = census(&[record(None, LandingState::Retired)], &[changed(None)]);
        assert_eq!(
            count(&points, EvolutionIntent::Unattributed, FunnelStage::Retired),
            1
        );
        assert_eq!(
            count(&points, EvolutionIntent::Unattributed, FunnelStage::Staged),
            1
        );
        assert_eq!(
            count(&points, EvolutionIntent::SelfSignal, FunnelStage::Retired),
            0
        );
    }

    /// A repeat landing call is the idempotency check answering, and it must
    /// not count. A rate inflated by retries would hide exactly the funnel it
    /// is meant to show.
    #[test]
    fn only_the_edge_counts_as_a_landing() {
        assert!(is_first_landing(None));
        assert!(is_first_landing(Some(&record(
            Some(EvolutionIntent::CiFix),
            LandingState::Unverified
        ))));
        assert!(is_first_landing(Some(&record(
            Some(EvolutionIntent::SelfSignal),
            LandingState::Retired
        ))));
        // A retired change that lands anyway is a landing: the landing won the
        // race against the verdict, and the change is on the branch.
        assert!(!is_first_landing(Some(&record(
            Some(EvolutionIntent::CiFix),
            LandingState::Landed
        ))));
    }

    /// Records already on disk were written without the intent field, and they
    /// are the whole reason the census can be read on the day it ships: a
    /// deserialization failure here would empty the funnel exactly when there
    /// is history behind it.
    #[test]
    fn a_record_written_before_the_intent_field_loads_and_counts() {
        let old = serde_json::json!({
            "change": {
                "change_id": "task-abc-012345",
                "goal": "fix the thing",
                "content": "--- a/x\n+++ b/x\n@@ -1 +1 @@\n-old\n+new\n",
                "affected_files": ["crates/x/src/lib.rs"],
                "rationale": null,
                "pge_mode": "squad",
                "self_review_score": 0.9,
                "issue_number": null,
            },
            "base": "main",
            "landed_rev": "",
            "state": "retired",
            "created_at": "2026-09-23T10:00:00Z",
            "updated_at": "2026-09-23T10:05:00Z",
        });
        let record: LandingRecord =
            serde_json::from_value(old).expect("a record without the intent field still loads");
        assert_eq!(record.change.intent, None);
        assert_eq!(record.state, LandingState::Retired);

        let points = census(&[record], &[]);
        assert_eq!(
            count(&points, EvolutionIntent::Unattributed, FunnelStage::Retired),
            1
        );
    }
}
