//! What the inbox retirement leaves behind, as a reading rather than a log line.
//!
//! The retirement pass removes an inbox only when its agent is gone from the
//! registry **and** the stream owes nothing to a reader. That second condition is
//! deliberate: an entry no consumer has taken is a command somebody was told had
//! been delivered, and dropping it would be this pass deciding to lose accepted
//! work. The consequence is a population nothing reports on — an inbox whose
//! owner is gone but which still owes entries is never delivered and never
//! counted. The pass's own output says how many it removed; the entries it
//! *refused* to remove have no reading at all, so "the store is clean" and "the
//! store holds work nobody can reach" look the same from outside.
//!
//! Length is not that test. A stream whose group read and acknowledged every
//! entry still reports `XLEN > 0`, and counting those entries as stranded work
//! makes a queue that did its job look like a leak — the whole reason the walk
//! asks the group state instead of the length.
//!
//! Four series, all from one walk:
//!
//! - `agent_inbox_orphaned_streams{state="empty"}` — unowned inboxes with no
//!   entries. This is the retirement pass's work in progress; a value that stops
//!   falling is a pass that is not running.
//! - `agent_inbox_orphaned_streams{state="consumed"}` — unowned inboxes whose
//!   entries have all been taken and acknowledged. Also retirement's work; their
//!   contents are history, not work, so they add nothing to the entry count.
//! - `agent_inbox_orphaned_streams{state="holding"}` — unowned inboxes that still
//!   owe a reader something. Nothing will ever read them: no live agent has that
//!   id. This is the leak.
//! - `agent_inbox_orphaned_entries` — how many entries are still owed across that
//!   last set. The stream count alone cannot tell one stranded command from ten
//!   thousand.
//!
//! Nothing is published before the first walk completes. A zero that means "the
//! scan has not run" and a zero that means "there is nothing there" are two
//! different statements, and only one of them is a claim this module can make.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use async_trait::async_trait;
use cog_core::{DimensionSpec, Observable, RawMetric, SFResult, TraceFragment};

use crate::inbox_retirement::OrphanScan;

/// Unowned inboxes, split by what they still owe a reader.
pub const INBOX_ORPHANED_STREAMS: &str = "agent_inbox_orphaned_streams";
/// Entries still owed by unowned inboxes that retirement will not remove.
pub const INBOX_ORPHANED_ENTRIES: &str = "agent_inbox_orphaned_entries";
/// The label carrying which state of the population a stream count describes.
pub const INBOX_ORPHAN_STATE_LABEL: &str = "state";
/// Unowned and empty: the retirement pass may remove it.
pub const INBOX_ORPHAN_STATE_EMPTY: &str = "empty";
/// Unowned, every entry taken and acknowledged: the pass may remove it, and its
/// contents are history rather than work.
pub const INBOX_ORPHAN_STATE_CONSUMED: &str = "consumed";
/// Unowned and still owing entries: no reader will ever take them.
pub const INBOX_ORPHAN_STATE_HOLDING: &str = "holding";

/// The last walk's inventory, published on every scrape.
///
/// Written by the retirement loop, read by the metrics endpoint; both are in the
/// same process, so the values are plain atomics rather than a shared map.
#[derive(Debug, Default)]
pub struct InboxOrphanReadings {
    empty: AtomicU64,
    consumed: AtomicU64,
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
        self.consumed
            .store(scan.consumed.len() as u64, Ordering::Relaxed);
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
                self.consumed.load(Ordering::Relaxed) as f64,
            )
            .with_label(INBOX_ORPHAN_STATE_LABEL, INBOX_ORPHAN_STATE_CONSUMED),
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
    async fn every_state_is_written_after_a_walk_even_when_some_are_empty() {
        let readings = InboxOrphanReadings::new();
        let scan = OrphanScan {
            empty: vec![("orchestrator:agent:a:inbox".into(), 0)],
            consumed: Vec::new(),
            holding: Vec::new(),
        };
        readings.record(&scan);
        let metrics = readings.collect_metrics("").await.expect("collect");
        assert_eq!(metrics.len(), 4, "three states and the entry count");
        let state = |wanted: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == INBOX_ORPHANED_STREAMS
                        && m.labels.get(INBOX_ORPHAN_STATE_LABEL) == Some(&wanted.to_string())
                })
                .unwrap_or_else(|| panic!("{wanted} row: zero, not absent"))
        };
        assert_eq!(state(INBOX_ORPHAN_STATE_EMPTY).value, 1.0);
        assert_eq!(state(INBOX_ORPHAN_STATE_CONSUMED).value, 0.0);
        assert_eq!(state(INBOX_ORPHAN_STATE_HOLDING).value, 0.0);
    }

    #[tokio::test]
    async fn the_entry_count_is_the_sum_of_the_unread_lengths() {
        let readings = InboxOrphanReadings::new();
        let scan = OrphanScan {
            empty: Vec::new(),
            consumed: Vec::new(),
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

    /// A stream whose group read and acknowledged everything keeps its entries
    /// on disk, but owes nothing: counting them as stranded work is the reading
    /// saying "six commands nobody will get" about three queues that were drained.
    #[tokio::test]
    async fn a_consumed_inbox_is_reported_but_its_history_is_not_stranded_work() {
        let readings = InboxOrphanReadings::new();
        let scan = OrphanScan {
            empty: Vec::new(),
            consumed: vec![("orchestrator:agent:y:inbox".into(), 7)],
            holding: vec![("orchestrator:agent:x:inbox".into(), 2)],
        };
        readings.record(&scan);
        let metrics = readings.collect_metrics("").await.expect("collect");
        let entries = metrics
            .iter()
            .find(|m| m.name == INBOX_ORPHANED_ENTRIES)
            .expect("entry count");
        assert_eq!(
            entries.value, 2.0,
            "history a group already read is not work"
        );
        let consumed = metrics
            .iter()
            .find(|m| {
                m.name == INBOX_ORPHANED_STREAMS
                    && m.labels.get(INBOX_ORPHAN_STATE_LABEL)
                        == Some(&INBOX_ORPHAN_STATE_CONSUMED.to_string())
            })
            .expect("consumed state");
        assert_eq!(consumed.value, 1.0);
    }

    #[tokio::test]
    async fn a_later_walk_replaces_the_earlier_one() {
        let readings = InboxOrphanReadings::new();
        readings.record(&OrphanScan {
            empty: Vec::new(),
            consumed: Vec::new(),
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
