//! Today's spend as the ledger last reported it.
//!
//! The ledger, the hourly rollup and window pacing each answer what the world
//! allowed. This module answers only what was spent, and it is deliberately the
//! whole of what it does: the platform does not put a daily ceiling on its own
//! work, so there is no judgement here to publish, only a reading.
//!
//! The figure comes from a warehouse sum taken on the pool's own cadence, not
//! from the request path: summing the ledger per call would buy one PG query per
//! request for a number that is stale the moment the request itself lands. Two
//! rules fall out of reading on a cadence instead of per call:
//!
//! - A failed sum keeps the last good one for the rest of the day, because
//!   dropping to "unmeasured" would erase a reading the writer can still resume.
//! - A successful sum from an earlier day is dropped rather than carried: the
//!   counters it summed have reset, so yesterday's total would read as today's.
//!
//! `measured_at_unix` travels with the figure because a spend without an age is
//! either a quiet day or a dead writer, and those call for opposite repairs.

/// Seconds in a UTC day.
const SECONDS_PER_DAY: i64 = 86_400;

/// Start of the UTC day containing `now_unix`.
///
/// The day is fixed to UTC rather than to a local zone because the figure is
/// summed from a ledger whose timestamps are UTC, and a boundary derived from the
/// host's zone would move the reset hour when the pod moved.
pub fn day_start_unix(now_unix: i64) -> i64 {
    now_unix.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

/// Today's spend as the ledger last reported it.
///
/// `day_start_unix` travels with the figures because they are only true of the
/// day they were summed over: a snapshot taken before midnight and read after it
/// would otherwise stand as the new day's total.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpendSnapshot {
    pub day_start_unix: i64,
    pub total_tokens: u64,
    pub actor_tokens: std::collections::BTreeMap<String, u64>,
}

/// What the last tick could say about today's spend.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reading {
    /// The last successful sum. `None` until a read succeeds on the current day;
    /// a successful sum from an earlier day is dropped rather than carried,
    /// because carrying it would publish yesterday's total as today's.
    pub snapshot: Option<SpendSnapshot>,
    /// When the last successful sum was taken, unix seconds. `None` until one
    /// is. A boolean "measured" would say the same thing on a healthy process
    /// and would stop saying it once the gateway died, which is the state a
    /// reader most needs to see.
    pub measured_at_unix: Option<i64>,
}

impl Reading {
    /// Drop a snapshot that belongs to a day other than `now_unix`'s.
    ///
    /// Called by the refresh on both arms, so that a failed read at 00:00:30
    /// cannot leave yesterday's spend standing as today's.
    pub fn carry_forward(self, now_unix: i64) -> Self {
        let today = day_start_unix(now_unix);
        match self.snapshot.as_ref() {
            Some(snapshot) if snapshot.day_start_unix == today => self,
            _ => Self::default(),
        }
    }

    /// The snapshot a reader may use: today's, or none at all.
    pub fn usable(&self, now_unix: i64) -> Option<&SpendSnapshot> {
        self.snapshot
            .as_ref()
            .filter(|s| s.day_start_unix == day_start_unix(now_unix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOON: i64 = 1_600_000_000 - (1_600_000_000 % SECONDS_PER_DAY) + 12 * 3600;

    fn snapshot(total: u64) -> SpendSnapshot {
        SpendSnapshot {
            day_start_unix: day_start_unix(NOON),
            total_tokens: total,
            actor_tokens: Default::default(),
        }
    }

    #[test]
    fn the_day_boundary_is_utc_midnight() {
        assert_eq!(day_start_unix(0), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY - 1), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY), SECONDS_PER_DAY);
        // Before the epoch the floor has to go down, not toward zero.
        assert_eq!(day_start_unix(-1), -SECONDS_PER_DAY);
    }

    #[test]
    fn yesterdays_spend_does_not_stand_as_todays() {
        let yesterday = SpendSnapshot {
            day_start_unix: day_start_unix(NOON) - SECONDS_PER_DAY,
            ..snapshot(10_000)
        };
        let reading = Reading {
            snapshot: Some(yesterday),
            measured_at_unix: Some(NOON),
        };
        assert!(reading.usable(NOON).is_none());
        assert!(reading.carry_forward(NOON).snapshot.is_none());
    }

    #[test]
    fn a_failed_read_at_midnight_drops_yesterdays_snapshot() {
        let yesterday = Reading {
            snapshot: Some(snapshot(10_000)),
            measured_at_unix: Some(NOON),
        };
        let just_after_midnight = day_start_unix(NOON) + SECONDS_PER_DAY + 30;
        let carried = yesterday.carry_forward(just_after_midnight);
        assert!(carried.snapshot.is_none());
        assert!(carried.measured_at_unix.is_none());
    }

    #[test]
    fn a_failed_read_inside_the_day_keeps_the_last_good_sum() {
        let earlier = Reading {
            snapshot: Some(snapshot(10_000)),
            measured_at_unix: Some(NOON),
        };
        let carried = earlier.clone().carry_forward(NOON + 60);
        assert_eq!(carried, earlier);
    }
}
