//! The UTC day boundary, and the wait until the next one.
//!
//! Daily counters reset at UTC midnight, and every refusal made against one of
//! them has to name that reset, so the two facts are written once here instead
//! of once per caller. The writer and the reader are in different crates over
//! this: one process increments a counter and gives it a lifetime that ends at
//! the boundary, another answers the caller it just refused with how long to
//! wait. If the two spell the boundary themselves they agree until one of them
//! is edited, and the way they disagree is a caller retrying one second before
//! the counter it collided with actually resets.
//!
//! Both take the clock as an argument rather than reading it: a boundary that
//! reads its own clock cannot be tested at the second before midnight, and that
//! second is where the off-by-one lives.
//!
//! The day is fixed to UTC rather than to a local zone because the timestamps
//! being counted are UTC, and a boundary derived from the host's zone would move
//! the reset hour when the process moved.
//!
//! It lives in the contract layer rather than beside either caller because the
//! two callers cannot see each other: the quota crate owns the counters and the
//! gateway owns the request path.

/// Seconds in a UTC day.
// cap: bound — 一个 UTC 日的秒数，定义值，不是这台机器量的
pub const SECONDS_PER_DAY: i64 = 86_400;

/// Start of the UTC day containing `now_unix`.
pub fn day_start_unix(now_unix: i64) -> i64 {
    now_unix.div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY
}

/// Seconds from `now_unix` until the next UTC midnight, at least one.
///
/// At least one because a `retry-after` of zero reads as "retry now", and the
/// call that reads it would be the one refused again.
pub fn seconds_to_next_day(now_unix: i64) -> u64 {
    (day_start_unix(now_unix) + SECONDS_PER_DAY - now_unix).max(1) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOON: i64 = 1_600_000_000 - (1_600_000_000 % SECONDS_PER_DAY) + 12 * 3600;

    #[test]
    fn the_day_boundary_is_utc_midnight() {
        assert_eq!(day_start_unix(0), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY - 1), 0);
        assert_eq!(day_start_unix(SECONDS_PER_DAY), SECONDS_PER_DAY);
        // Before the epoch the floor has to go down, not toward zero.
        assert_eq!(day_start_unix(-1), -SECONDS_PER_DAY);
    }

    #[test]
    fn the_wait_until_reset_is_the_rest_of_the_day() {
        assert_eq!(
            seconds_to_next_day(day_start_unix(NOON)),
            SECONDS_PER_DAY as u64
        );
        assert_eq!(
            seconds_to_next_day(day_start_unix(NOON) + SECONDS_PER_DAY - 1),
            1
        );
        // The last second is still one, never zero: a zero wait would be read
        // as "retry immediately" by the caller that was just refused.
        assert_eq!(
            seconds_to_next_day(day_start_unix(NOON) + SECONDS_PER_DAY),
            SECONDS_PER_DAY as u64
        );
    }
}
