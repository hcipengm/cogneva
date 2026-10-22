//! What the inbox retirement leaves behind, as a reading rather than a log line.
//!
//! The retirement pass removes an inbox only when its agent is gone from the
//! registry **and** the stream is empty. That second condition is deliberate: an
//! entry in a stream nobody owns is a command somebody was told had been
//! delivered, and dropping it would be this pass deciding to lose accepted work.
//! The consequence is a population nothing reports on — an inbox whose owner is
//! gone but which still holds entries is never removed, never delivered and
//! never counted. The pass's own output says how many it removed; the entries it
//! *refused* to remove have no reading at all, so "the store is clean" and "the
//! store holds work nobody can reach" look the same from outside.
//!
//! Three gauges, all from one walk:
//!
//! - `agent_inbox_orphaned_streams{state="empty"}` — unowned inboxes with no
//!   entries. This is the retirement pass's work in progress; a value that stops
//!   falling is a pass that is not running.
//! - `agent_inbox_orphaned_streams{state="holding"}` — unowned inboxes that still
//!   carry entries. Nothing will ever read them: no live agent has that id, and
//!   retirement will not touch a non-empty stream. This is the leak.
//! - `agent_inbox_orphaned_entries` — how many entries sit in that second set.
//!   The stream count alone cannot tell one stranded command from ten thousand.
//!
//! Nothing is published before the first walk completes. A zero that means "the
//! scan has not run" and a zero that means "there is nothing there" are two
//! different statements, and only one of them is a claim this module can make.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use cog_core::{DimensionSpec, Observable, RawMetric, SFResult, TraceFragment};

use crate::inbox_retirement::OrphanScan;

/// Unowned inboxes, split by whether they can be removed.
pub const INBOX_ORPHANED_STREAMS: &str = "agent_inbox_orphaned_streams";
/// Entries sitting in unowned inboxes that retirement will not remove.
pub const INBOX_ORPHANED_ENTRIES: &str = "agent_inbox_orphaned_entries";
/// The label carrying which half of the population a stream count describes.
pub const INBOX_ORPHAN_STATE_LABEL: &str = "state";
/// Unowned and empty: the retirement pass may remove it.
pub const INBOX_ORPHAN_STATE_EMPTY: &str = "empty";
/// Unowned and non-empty: no reader will ever take these entries.
pub const INBOX_ORPHAN_STATE_HOLDING: &str = "holding";

/// The last walk's inventory, published on every scrape.
///
/// Written by the retirement loop, read by the metrics endpoint; both are in the
/// same process, so the values are plain atomics rather than a shared map.
#[derive(Debug, Default)]
pub struct InboxOrphanReadings {
    empty: AtomicU64,
    holding: AtomicU64,
    entries: AtomicU64,
    measured: AtomicBool,
}

impl InboxOrphanReadings {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the reading with what this walk saw.
    ///
    /// The values are written before `measured` is set: a scrape that arrives
    /// mid-write then reads either the previous tuple or this one, never a
    /// half-written mix presented as a completed walk.
    pub fn record(&self, scan: &OrphanScan) {
        self.empty.store(scan.empty.len() as u64, Ordering::Relaxed);
        self.holding
            .store(scan.holding.len() as u64, Ordering::Relaxed);
        self.entries.store(scan.held_entries(), Ordering::Relaxed);
        self.measured.store(true, Ordering::Release);
    }
}

#[async_trait]
impl Observable for InboxOrphanReadings {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        // Before the first walk there is nothing to say. Zero would read as
        // "the walk found nothing", which is exactly the confusion this module
        // exists to remove.
        if !self.measured.load(Ordering::Acquire) {
            return Ok(Vec::new());
        }
        Ok(vec![
            RawMetric::new(
                INBOX_ORPHANED_STREAMS,
                self.empty.load(Ordering::Relaxed) as f64,
            )
            .with_label(INBOX_ORPHAN_STATE_LABEL, INBOX_ORPHAN_STATE_EMPTY),
            RawMetric::new(
                INBOX_ORPHANED_STREAMS,
                self.holding.load(Ordering::Relaxed) as f64,
            )
            .with_label(INBOX_ORPHAN_STATE_LABEL, INBOX_ORPHAN_STATE_HOLDING),
            RawMetric::new(
                INBOX_ORPHANED_ENTRIES,
                self.entries.load(Ordering::Relaxed) as f64,
            ),
        ])
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// The inventory is one question about one store, not a per-dimension
    /// reading: every dimension would get the same walk's answer.
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn nothing_is_published_before_the_first_walk() {
        let readings = InboxOrphanReadings::new();
        assert!(readings
            .collect_metrics("")
            .await
            .expect("collect")
            .is_empty());
    }

    #[tokio::test]
    async fn both_halves_are_written_after_a_walk_even_when_one_is_empty() {
        let readings = InboxOrphanReadings::new();
        let scan = OrphanScan {
            empty: vec![("orchestrator:agent:a:inbox".into(), 0)],
            holding: Vec::new(),
        };
        readings.record(&scan);
        let metrics = readings.collect_metrics("").await.expect("collect");
        assert_eq!(metrics.len(), 3, "both states and the entry count");
        let empty = metrics
            .iter()
            .find(|m| {
                m.name == INBOX_ORPHANED_STREAMS
                    && m.labels.get(INBOX_ORPHAN_STATE_LABEL)
                        == Some(&INBOX_ORPHAN_STATE_EMPTY.to_string())
            })
            .expect("empty half");
        let holding = metrics
            .iter()
            .find(|m| {
                m.name == INBOX_ORPHANED_STREAMS
                    && m.labels.get(INBOX_ORPHAN_STATE_LABEL)
                        == Some(&INBOX_ORPHAN_STATE_HOLDING.to_string())
            })
            .expect("holding half: zero, not absent");
        assert_eq!(empty.value, 1.0);
        assert_eq!(holding.value, 0.0);
    }

    #[tokio::test]
    async fn the_entry_count_is_the_sum_of_the_holding_lengths() {
        let readings = InboxOrphanReadings::new();
        let scan = OrphanScan {
            empty: Vec::new(),
            holding: vec![
                ("orchestrator:agent:x:inbox".into(), 2),
                ("orchestrator:agent:y:inbox".into(), 3),
            ],
        };
        readings.record(&scan);
        let metrics = readings.collect_metrics("").await.expect("collect");
        let entries = metrics
            .iter()
            .find(|m| m.name == INBOX_ORPHANED_ENTRIES)
            .expect("entry count");
        assert_eq!(entries.value, 5.0);
    }

    #[tokio::test]
    async fn a_later_walk_replaces_the_earlier_one() {
        let readings = InboxOrphanReadings::new();
        readings.record(&OrphanScan {
            empty: Vec::new(),
            holding: vec![("orchestrator:agent:x:inbox".into(), 4)],
        });
        readings.record(&OrphanScan::default());
        let metrics = readings.collect_metrics("").await.expect("collect");
        let entries = metrics
            .iter()
            .find(|m| m.name == INBOX_ORPHANED_ENTRIES)
            .expect("entry count");
        assert_eq!(entries.value, 0.0, "the stranded work was drained");
    }
}
