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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// One consumer's read loop, as the loop itself observes it.
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadSlot {
    /// Epoch seconds when this consumer's loop started. Silence is measured
    /// from here for a consumer that has never been answered, so a loop that
    /// cannot connect at all is as visible as one that lost its connection.
    pub since_seconds: u64,
    /// Epoch seconds of the last read the server answered — a message and an
    /// empty reply both count: the question is whether the connection works,
    /// not whether there was work to do.
    pub last_ok_seconds: u64,
    /// Reads that came back as errors since this loop started.
    pub failures: u64,
}

impl ReadSlot {
    /// How long this consumer has been without an answer.
    pub fn silent_seconds(&self, now: u64) -> u64 {
        let anchor = self.last_ok_seconds.max(self.since_seconds);
        now.saturating_sub(anchor)
    }
}

/// Per-consumer read health for one process.
#[derive(Debug)]
pub struct ReadHealth {
    slots: Mutex<BTreeMap<(String, String), ReadSlot>>,
    /// The block period the reads are actually issued with, so the staleness
    /// bound published beside it cannot drift from the read loop's behaviour.
    block_ms: u64,
}

impl ReadHealth {
    pub fn new(block_ms: u64) -> Self {
        Self {
            slots: Mutex::new(BTreeMap::new()),
            block_ms,
        }
    }

    /// Start tracking one consumer's loop. The returned guard owns the row: it
    /// removes it when the loop is dropped, so a stream nobody consumes any
    /// more stops reporting instead of sitting at ever-growing silence and
    /// alerting about a consumer that no longer exists.
    pub fn track(health: &Arc<Self>, stream: &str, group: &str) -> ReadGuard {
        let key = (stream.to_string(), group.to_string());
        health.with_map(|map| {
            map.insert(
                key.clone(),
                ReadSlot {
                    since_seconds: now_seconds(),
                    ..Default::default()
                },
            );
        });
        ReadGuard {
            health: Arc::clone(health),
            key,
        }
    }

    /// The server answered this read.
    pub fn note_ok(&self, stream: &str, group: &str) {
        self.update(stream, group, |slot| {
            slot.last_ok_seconds = now_seconds();
        });
    }

    /// This read failed; the loop is backing off and will try again.
    pub fn note_failure(&self, stream: &str, group: &str) {
        self.update(stream, group, |slot| {
            slot.failures = slot.failures.saturating_add(1);
        });
    }

    /// The block period in use, in seconds.
    pub fn block_seconds(&self) -> f64 {
        (self.block_ms as f64) / 1000.0
    }

    /// One row per live consumer, ordered so a scrape is stable between ticks.
    pub fn snapshot(&self) -> Vec<((String, String), ReadSlot)> {
        self.with_map(|map| map.iter().map(|(k, v)| (k.clone(), *v)).collect())
    }

    fn update(&self, stream: &str, group: &str, change: impl FnOnce(&mut ReadSlot)) {
        let key = (stream.to_string(), group.to_string());
        self.with_map(|map| {
            // A read that arrives after its guard is gone (a loop finishing
            // while a read is in flight) has no row to update and no business
            // creating one: nobody is watching that consumer any more.
            if let Some(slot) = map.get_mut(&key) {
                change(slot);
            }
        });
    }

    fn with_map<T>(&self, apply: impl FnOnce(&mut BTreeMap<(String, String), ReadSlot>) -> T) -> T {
        match self.slots.lock() {
            Ok(mut map) => apply(&mut map),
            Err(poisoned) => apply(&mut poisoned.into_inner()),
        }
    }
}

/// Owns one consumer's row; dropping it retires the row.
#[derive(Debug)]
pub struct ReadGuard {
    health: Arc<ReadHealth>,
    key: (String, String),
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        let key = self.key.clone();
        self.health.with_map(|map| {
            map.remove(&key);
        });
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

    #[test]
    fn health_is_recorded_per_consumer() {
        let health = Arc::new(ReadHealth::new(5000));
        let _goals = ReadHealth::track(&health, "goals", "worker");
        let _results = ReadHealth::track(&health, "results", "worker");
        health.note_ok("goals", "worker");
        health.note_failure("goals", "worker");
        health.note_failure("goals", "worker");

        let rows = health.snapshot();
        assert_eq!(rows.len(), 2, "one row per stream+group: {rows:?}");
        let now = now_seconds();
        let goals = rows
            .iter()
            .find(|((stream, _), _)| stream == "goals")
            .map(|(_, slot)| *slot)
            .unwrap();
        assert!(goals.last_ok_seconds > 0, "an answered read is a timestamp");
        assert_eq!(goals.failures, 2, "failures accumulate");
        assert!(goals.silent_seconds(now) <= 1, "it was answered just now");

        let results = rows
            .iter()
            .find(|((stream, _), _)| stream == "results")
            .map(|(_, slot)| *slot)
            .unwrap();
        assert_eq!(results.last_ok_seconds, 0, "never answered");
        assert!(
            results.silent_seconds(results.since_seconds) == 0
                && results.silent_seconds(results.since_seconds + 90) == 90,
            "a consumer that never got an answer counts its silence from the \
             moment it started, not from the epoch"
        );
        assert_eq!(health.block_seconds(), 5.0);
    }

    #[test]
    fn an_answered_read_does_not_erase_the_failure_count() {
        // The counter and the silence answer different questions: "has it
        // refused anything" and "is it moving now". Rolling the counter back on
        // success would hide an intermittent consumer that keeps up by
        // retrying.
        let health = Arc::new(ReadHealth::new(1000));
        let _guard = ReadHealth::track(&health, "s", "g");
        health.note_failure("s", "g");
        health.note_ok("s", "g");
        let rows = health.snapshot();
        assert_eq!(rows[0].1.failures, 1);
        assert!(rows[0].1.last_ok_seconds > 0);
    }

    #[test]
    fn a_retired_consumer_stops_reporting() {
        let health = Arc::new(ReadHealth::new(1000));
        let guard = ReadHealth::track(&health, "s", "g");
        health.note_ok("s", "g");
        assert_eq!(health.snapshot().len(), 1);
        drop(guard);
        assert!(
            health.snapshot().is_empty(),
            "a stream nobody consumes must not keep a stale row firing"
        );
        // A read finishing after the loop was dropped must not resurrect it.
        health.note_ok("s", "g");
        assert!(health.snapshot().is_empty());
    }
}
