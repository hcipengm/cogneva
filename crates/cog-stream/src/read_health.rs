//! What the stream consumers can say about themselves.
//!
//! A consumer whose connection died keeps trying: the read loop retries with
//! backoff and logs a warning, so the process stays up, the health checks stay
//! green, and the only symptom is that the queue behind it stops moving. The
//! cluster ran that way for hours — every consumer group of one pod repeating
//! "broken pipe" at a steady cadence — while the observation surface said
//! nothing about the bus. What it did show was a consequence two steps away
//! ("pending state not measured"), which names the measurement rather than the
//! transport.
//!
//! So the read loop records the one thing only it knows: how long the server
//! has been silent to it. A healthy consumer gets an answer every block, so the
//! silence of a working consumer is bounded by its block period; a consumer that
//! is failing drifts past it. Kept per stream and group, because one stalled
//! group among many is exactly what a single aggregate would hide.
//!
//! A name is not a loop. Several loops read the same stream under the same group
//! — measured at eighteen or more on one name — so a record keyed by the name
//! alone answers for whichever loop registered last: a sibling's registration
//! zeroed the counts, a sibling's exit deleted the row out from under the loops
//! still burning, and the published value read the same whether one loop or
//! eighteen were behind it. The identity therefore lives where the loop does:
//! one row per registration, addressed by the handle the loop holds, and the
//! name is an aggregation over its rows. Which aggregation it is matters and is
//! deliberate — see [`ReadFamily`].
//!
//! principle: feedback_a_name_is_not_an_identity_the_reading_aggregates_over_its_loops
//! principle: feedback_an_aggregate_follows_the_signal_and_the_population_gets_its_own_reading

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Why a read came back refused, as far as the loop can tell from the error.
///
/// Two causes share one code and mean opposite things. `XREADGROUP` answers
/// `NOGROUP` when the group has not been created yet — a race this loop loses
/// once at startup and the next attempt wins — and again when the group is gone
/// for good: deleted, or the stream flushed under a live consumer, which leaves
/// it retrying against nothing for as long as the pod lives. Pooled into one
/// number neither can be read. A rate loose enough to ignore the startup race
/// is loose enough to miss the consumer that will never recover, and a rate
/// tight enough to catch that one fires on every restart. Which of the two a
/// series is showing is decided by the reading beside it (how long the consumer
/// has gone unanswered), not by this label: the label is what makes the pair
/// readable at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRefusal {
    /// The server answered that the group does not exist.
    NoGroup,
    /// Everything else: the transport, the server, the shape of the answer.
    Other,
}

impl ReadRefusal {
    /// The label the counter is published under. A reader that wants the pooled
    /// number sums over this label rather than reading one series of it.
    pub fn label(self) -> &'static str {
        match self {
            ReadRefusal::NoGroup => "nogroup",
            ReadRefusal::Other => "other",
        }
    }

    /// Every cause a series is published for, so a cause added here cannot be
    /// counted and left unpublished.
    pub const ALL: [ReadRefusal; 2] = [ReadRefusal::NoGroup, ReadRefusal::Other];
}

/// One read loop, as the loop itself observes it.
///
/// Private, and deliberately so: a loop's identity is the process's business,
/// and the unit anything outside this module acts on is the name. Handing out
/// per-loop rows would invite a second reader answering a question the
/// aggregation is the single answer to.
#[derive(Debug, Clone, Copy, Default)]
struct ReadSlot {
    /// Epoch seconds when this consumer's loop started. Silence is measured
    /// from here for a consumer that has never been answered, so a loop that
    /// cannot connect at all is as visible as one that lost its connection.
    since_seconds: u64,
    /// Epoch seconds of the last read the server answered — a message and an
    /// empty reply both count: the question is whether the connection works,
    /// not whether there was work to do.
    last_ok_seconds: u64,
    /// Reads refused because the server answered `NOGROUP`.
    failures_no_group: u64,
    /// Reads refused for any other reason.
    failures_other: u64,
}

impl ReadSlot {
    /// How long this loop has been without an answer.
    fn silent_seconds(&self, now: u64) -> u64 {
        let anchor = self.last_ok_seconds.max(self.since_seconds);
        now.saturating_sub(anchor)
    }
}

/// Every loop reading one stream under one group, as one reading.
///
/// The name is the unit a reader acts on: the fix for any of these is on the
/// bus, the group or the process, none of which is per-loop, and a series per
/// loop would be a name nobody acts on and one that disappears in the moment it
/// is most wanted — when the loop that is burning exits.
///
/// The three fields aggregate differently on purpose:
///
/// - failures are **summed**: they are work the loops did, and a name's cost is
///   what all of its loops paid;
/// - silence is the **worst** of the loops: it is a state, and the question is
///   whether anything under this name is stuck. A sum would rise with the
///   population, and an average would let a broken loop hide behind healthy
///   ones — worst is the only reading under which "is it moving" has one answer;
/// - `loops` is the **population**, published because the other two do not carry
///   it: one loop failing slowly and eighteen failing the same way are different
///   situations with the same counter, and until this number existed the
///   eighteen were inferred from rates rather than read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadFamily {
    /// How many live read loops are behind this name.
    pub loops: usize,
    /// The longest a loop under this name has gone unanswered.
    pub silent_seconds: u64,
    /// Refused reads of every cause, over every loop under this name.
    pub failures_no_group: u64,
    /// Refused reads of any other cause, over every loop under this name.
    pub failures_other: u64,
}

impl ReadFamily {
    /// Refused reads of one cause, over the family.
    pub fn failures_of(&self, refusal: ReadRefusal) -> u64 {
        match refusal {
            ReadRefusal::NoGroup => self.failures_no_group,
            ReadRefusal::Other => self.failures_other,
        }
    }

    /// Refused reads of every cause, over the family.
    pub fn failures(&self) -> u64 {
        self.failures_no_group.saturating_add(self.failures_other)
    }
}

/// Per-consumer read health for one process.
#[derive(Debug)]
pub struct ReadHealth {
    /// One entry per stream+group, and inside it one row per read loop.
    families: Mutex<BTreeMap<(String, String), BTreeMap<u64, ReadSlot>>>,
    /// Source of the row ids. Process-local and never reused, so a row's
    /// identity cannot be inherited by a later loop the way the name was.
    next_id: AtomicU64,
    /// The block period the reads are actually issued with, so the staleness
    /// bound published beside it cannot drift from the read loop's behaviour.
    block_ms: u64,
}

impl ReadHealth {
    pub fn new(block_ms: u64) -> Self {
        Self {
            families: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(0),
            block_ms,
        }
    }

    /// Start tracking one read loop. The returned handle is the row: it is what
    /// the loop writes through, and dropping it retires that one row — a stream
    /// nobody consumes any more stops reporting instead of sitting at
    /// ever-growing silence and alerting about a consumer that no longer
    /// exists, while the loops still reading keep theirs.
    pub fn track(health: &Arc<Self>, stream: &str, group: &str) -> ReadGuard {
        let name = (stream.to_string(), group.to_string());
        let id = health.next_id.fetch_add(1, Ordering::Relaxed);
        health.with_families(|families| {
            families.entry(name.clone()).or_default().insert(
                id,
                ReadSlot {
                    since_seconds: now_seconds(),
                    ..Default::default()
                },
            );
        });
        ReadGuard {
            health: Arc::clone(health),
            name,
            id,
        }
    }

    /// The block period in use, in seconds.
    pub fn block_seconds(&self) -> f64 {
        (self.block_ms as f64) / 1000.0
    }

    /// One row per live stream+group, ordered so a scrape is stable between
    /// ticks.
    pub fn families(&self, now: u64) -> Vec<((String, String), ReadFamily)> {
        self.with_families(|families| {
            families
                .iter()
                .map(|(name, rows)| {
                    (
                        name.clone(),
                        ReadFamily {
                            loops: rows.len(),
                            silent_seconds: rows
                                .values()
                                .map(|slot| slot.silent_seconds(now))
                                .max()
                                .unwrap_or(0),
                            failures_no_group: rows
                                .values()
                                .map(|slot| slot.failures_no_group)
                                .sum(),
                            failures_other: rows.values().map(|slot| slot.failures_other).sum(),
                        },
                    )
                })
                .collect()
        })
    }

    /// Write to one loop's own row.
    ///
    /// A write can only come through that loop's handle, so there is no path
    /// here that silently drops a count because some other loop's row went
    /// away: the row is removed when this handle is dropped, and no read of
    /// this loop outlives it.
    fn update(&self, name: &(String, String), id: u64, change: impl FnOnce(&mut ReadSlot)) {
        self.with_families(|families| {
            if let Some(slot) = families.get_mut(name).and_then(|rows| rows.get_mut(&id)) {
                change(slot);
            }
        });
    }

    /// Retire one loop's row, and the name with it once nothing is left.
    fn retire(&self, name: &(String, String), id: u64) {
        self.with_families(|families| {
            if let Some(rows) = families.get_mut(name) {
                rows.remove(&id);
                if rows.is_empty() {
                    families.remove(name);
                }
            }
        });
    }

    fn with_families<T>(
        &self,
        apply: impl FnOnce(&mut BTreeMap<(String, String), BTreeMap<u64, ReadSlot>>) -> T,
    ) -> T {
        match self.families.lock() {
            Ok(mut families) => apply(&mut families),
            Err(poisoned) => apply(&mut poisoned.into_inner()),
        }
    }
}

/// One read loop's row, and the handle that loop writes through.
///
/// Dropping it retires that row and nothing else: the name stays as long as any
/// loop is still reading under it.
#[derive(Debug)]
pub struct ReadGuard {
    health: Arc<ReadHealth>,
    name: (String, String),
    id: u64,
}

impl ReadGuard {
    /// The server answered this read.
    pub fn note_ok(&self) {
        self.health.update(&self.name, self.id, |slot| {
            slot.last_ok_seconds = now_seconds();
        });
    }

    /// This read failed; the loop is backing off and will try again.
    ///
    /// The cause is the caller's to name, because only the caller holds the
    /// error: by the time it is a count here the code that told the two causes
    /// apart is gone, and a counter that pools them is one no threshold can be
    /// written against.
    pub fn note_failure(&self, refusal: ReadRefusal) {
        self.health.update(&self.name, self.id, |slot| {
            let cell = match refusal {
                ReadRefusal::NoGroup => &mut slot.failures_no_group,
                ReadRefusal::Other => &mut slot.failures_other,
            };
            *cell = cell.saturating_add(1);
        });
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        self.health.retire(&self.name, self.id);
    }
}

pub(crate) fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family_of(health: &Arc<ReadHealth>, stream: &str, now: u64) -> ReadFamily {
        health
            .families(now)
            .into_iter()
            .find(|((name, _), _)| name == stream)
            .map(|(_, family)| family)
            .unwrap_or_else(|| panic!("no family published for {stream}"))
    }

    #[test]
    fn health_is_recorded_per_consumer() {
        let health = Arc::new(ReadHealth::new(5000));
        let goals = ReadHealth::track(&health, "goals", "worker");
        let _results = ReadHealth::track(&health, "results", "worker");
        goals.note_ok();
        goals.note_failure(ReadRefusal::NoGroup);
        goals.note_failure(ReadRefusal::NoGroup);

        let now = now_seconds();
        let rows = health.families(now);
        assert_eq!(rows.len(), 2, "one row per stream+group: {rows:?}");
        let goals = family_of(&health, "goals", now);
        assert_eq!(goals.loops, 1, "one loop is reading this name");
        assert_eq!(goals.failures(), 2, "failures accumulate");
        assert_eq!(
            goals.failures_of(ReadRefusal::NoGroup),
            2,
            "and the cause they are counted under is the one the caller named"
        );
        assert!(goals.silent_seconds <= 1, "it was answered just now");

        let results = family_of(&health, "results", now);
        assert!(
            results.silent_seconds <= 1,
            "a loop that never got an answer counts its silence from the moment \
             it started, not from the epoch: {}",
            results.silent_seconds
        );
        assert_eq!(results.failures(), 0, "and it has refused nothing");
        assert_eq!(health.block_seconds(), 5.0);
    }

    #[test]
    fn an_answered_read_does_not_erase_the_failure_count() {
        // The counter and the silence answer different questions: "has it
        // refused anything" and "is it moving now". Rolling the counter back on
        // success would hide an intermittent consumer that keeps up by
        // retrying.
        let health = Arc::new(ReadHealth::new(1000));
        let guard = ReadHealth::track(&health, "s", "g");
        guard.note_failure(ReadRefusal::NoGroup);
        guard.note_ok();
        let family = family_of(&health, "s", now_seconds());
        assert_eq!(family.failures(), 1);
        assert!(family.silent_seconds <= 1);
    }

    /// The two causes are counted apart, and the two counts are the whole.
    ///
    /// Both halves are load-bearing: without the first, a change that files
    /// every refusal under one label passes every other case here; without the
    /// second, a cause that is counted but never published would read as a
    /// consumer that has refused less than it has.
    #[test]
    fn the_two_causes_are_counted_apart_and_sum_to_the_old_number() {
        let health = Arc::new(ReadHealth::new(1000));
        let guard = ReadHealth::track(&health, "s", "g");
        guard.note_failure(ReadRefusal::NoGroup);
        guard.note_failure(ReadRefusal::Other);
        guard.note_failure(ReadRefusal::Other);

        let family = family_of(&health, "s", now_seconds());
        assert_eq!(family.failures_of(ReadRefusal::NoGroup), 1);
        assert_eq!(family.failures_of(ReadRefusal::Other), 2);
        assert_eq!(family.failures(), 3, "the number this counter used to publish");
    }

    /// A second loop on the same name takes nothing from the first.
    ///
    /// Registration used to be a keyed write: the later loop's row replaced the
    /// earlier one's, so whatever the first had counted was gone and the value
    /// reset without anything saying so.
    #[test]
    fn a_second_loop_on_the_same_name_takes_nothing_from_the_first() {
        let health = Arc::new(ReadHealth::new(1000));
        let first = ReadHealth::track(&health, "s", "g");
        first.note_failure(ReadRefusal::NoGroup);
        first.note_failure(ReadRefusal::NoGroup);
        let second = ReadHealth::track(&health, "s", "g");
        second.note_failure(ReadRefusal::Other);

        let family = family_of(&health, "s", now_seconds());
        assert_eq!(
            family.loops, 2,
            "two loops are reading this name, and that is a reading"
        );
        assert_eq!(
            family.failures(),
            3,
            "the first loop's two refusals survived the second's registration"
        );
        assert_eq!(family.failures_of(ReadRefusal::NoGroup), 2);
        assert_eq!(family.failures_of(ReadRefusal::Other), 1);
    }

    /// A loop leaving stops counting for itself and for nobody else.
    ///
    /// The row used to be the name, so the first sibling to exit deleted it and
    /// every loop still burning under that name stopped being counted — a
    /// reading that fell to zero exactly when the situation got worse.
    #[test]
    fn a_sibling_leaving_neither_silences_nor_hides_the_others() {
        let health = Arc::new(ReadHealth::new(1000));
        let leaving = ReadHealth::track(&health, "s", "g");
        let staying = ReadHealth::track(&health, "s", "g");
        leaving.note_failure(ReadRefusal::NoGroup);
        staying.note_failure(ReadRefusal::Other);
        assert_eq!(family_of(&health, "s", now_seconds()).failures(), 2);

        drop(leaving);
        let after = family_of(&health, "s", now_seconds());
        assert_eq!(after.loops, 1, "the name still has a loop on it");
        assert_eq!(
            after.failures(),
            1,
            "the loop that is still reading keeps its count: {after:?}"
        );
        staying.note_failure(ReadRefusal::Other);
        assert_eq!(
            family_of(&health, "s", now_seconds()).failures(),
            2,
            "and keeps counting after the sibling is gone"
        );
    }

    /// The name leaves the scrape when its last loop does, and not before.
    #[test]
    fn the_name_leaves_the_scrape_with_its_last_loop() {
        let health = Arc::new(ReadHealth::new(1000));
        let first = ReadHealth::track(&health, "s", "g");
        let second = ReadHealth::track(&health, "s", "g");
        assert_eq!(health.families(now_seconds()).len(), 1);
        drop(first);
        assert_eq!(
            health.families(now_seconds()).len(),
            1,
            "a name with one loop left is still a reading"
        );
        drop(second);
        assert!(
            health.families(now_seconds()).is_empty(),
            "a stream nobody consumes must not keep a stale row firing"
        );
    }

    /// The published silence is the worst loop under the name.
    ///
    /// This is the half that keeps an alert up. With the row keyed by name, the
    /// loop that registered last decided the value, and when it exited the row
    /// went with it: the alert resolved while the sibling that had been silent
    /// the longest was still stuck, which is the one moment the reading has to
    /// hold.
    #[test]
    fn the_silence_published_is_the_worst_loop_under_the_name() {
        let health = Arc::new(ReadHealth::new(1000));
        let started_first = now_seconds();
        let worst = ReadHealth::track(&health, "s", "g");
        // The loops are registered a second apart, so "which one has been
        // silent longer" has an answer the assertion can read rather than one
        // the test assumes.
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let started_second = now_seconds();
        let sibling = ReadHealth::track(&health, "s", "g");
        assert!(started_second > started_first, "the two anchors differ");
        worst.note_failure(ReadRefusal::NoGroup);
        sibling.note_failure(ReadRefusal::Other);

        let later = started_second + 600;
        assert_eq!(
            family_of(&health, "s", later).silent_seconds,
            later - started_first,
            "the reading is the loop that has been silent the longest, not the \
             one that registered last"
        );
        assert!(
            later - started_first > later - started_second,
            "and the two loops really do disagree"
        );

        drop(worst);
        let after = family_of(&health, "s", later);
        assert_eq!(
            after.silent_seconds,
            later - started_second,
            "the loop that exited was the one the value came from, and the \
             sibling still has to be visible where it is: {after:?}"
        );
        assert_eq!(after.loops, 1);
        assert_eq!(
            after.failures(),
            1,
            "with the exited loop's refusals gone and the sibling's kept"
        );
    }
}
