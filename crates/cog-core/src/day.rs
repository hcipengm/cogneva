//! The day boundary of the cluster's clock, and the wait until the next one.
//!
//! Daily counters reset at the reference's midnight, and every refusal made
//! against one of them has to name that reset, so the two facts are written once
//! here instead of once per caller. The writer and the reader are in different
//! crates over this: one process increments a counter and gives it a lifetime
//! that ends at the boundary, another answers the caller it just refused with
//! how long to wait. If the two spell the boundary themselves they agree until
//! one of them is edited, and the way they disagree is a caller retrying one
//! second before the counter it collided with actually resets.
//!
//! The boundary is not fixed to UTC and is not fixed to any zone named here: it
//! is midnight on the clock this process runs on, which the deployment sets once
//! for every part of itself. A day edge that named its own zone would be a
//! second reference — changing the cluster's would move every counter except
//! that one.
//!
//! Every form here takes the instant as an argument rather than reading the
//! clock itself: a boundary that reads its own clock cannot be tested at the
//! second before midnight, and that second is where the off-by-one lives. The
//! offset is an argument for the same reason, which is why the arithmetic has a
//! form that takes it explicitly.
//!
//! What the arithmetic does not model: a zone whose offset changes inside the
//! day it is measuring. The wait is computed from the offset at the instant it
//! was asked, so across a transition it would be off by the change. The
//! deployment's reference does not move within a day, and saying that here is
//! cheaper than arithmetic that pretends to be zone-generic and is wrong twice
//! a year.
//!
//! It lives in the contract layer rather than beside either caller because the
//! two callers cannot see each other: the quota crate owns the counters and the
//! gateway owns the request path.

/// Seconds in a day.
// cap: bound — 一日的秒数，定义值，不是这台机器量的
pub const SECONDS_PER_DAY: i64 = 86_400;

/// How far east of UTC this process's clock is at `now_unix`, in seconds.
///
/// This is the projection: the deployment sets one timezone for every part of
/// the cluster, and this reads what that turned into here. Zoned offsets change
/// over the year, so it is asked at the instant in question rather than cached.
pub fn local_offset_secs(now_unix: i64) -> i64 {
    use chrono::{Local, Offset, TimeZone};
    Local
        .timestamp_opt(now_unix, 0)
        .single()
        .map(|at| i64::from(at.offset().fix().local_minus_utc()))
        .unwrap_or(0)
}

/// Start of the day containing `now_unix`, on a clock `offset_secs` east of UTC.
pub fn day_start_unix_in(now_unix: i64, offset_secs: i64) -> i64 {
    (now_unix + offset_secs).div_euclid(SECONDS_PER_DAY) * SECONDS_PER_DAY - offset_secs
}

/// Start of the day containing `now_unix` on this process's clock.
pub fn day_start_unix(now_unix: i64) -> i64 {
    day_start_unix_in(now_unix, local_offset_secs(now_unix))
}

/// The civil date containing `now_unix`, on a clock `offset_secs` east of UTC.
///
/// The day a counter is filed under and the day its edge falls on have to be the
/// same day, so this starts from the boundary rather than converting the instant
/// separately: two conversions agree until one of them is edited.
pub fn date_at(now_unix: i64, offset_secs: i64) -> chrono::NaiveDate {
    // The edge is an instant; the date is that instant read on the same clock,
    // which is the edge shifted back by the same offset.
    chrono::DateTime::from_timestamp(day_start_unix_in(now_unix, offset_secs) + offset_secs, 0)
        .map(|at| at.date_naive())
        .unwrap_or_else(|| chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch is a date"))
}

/// The civil date containing `now_unix` on this process's clock.
pub fn local_date_naive(now_unix: i64) -> chrono::NaiveDate {
    date_at(now_unix, local_offset_secs(now_unix))
}

/// Seconds from `now_unix` until the next midnight, at least one.
///
/// At least one because a `retry-after` of zero reads as "retry now", and the
/// call that reads it would be the one refused again.
pub fn seconds_to_next_day_in(now_unix: i64, offset_secs: i64) -> u64 {
    (day_start_unix_in(now_unix, offset_secs) + SECONDS_PER_DAY - now_unix).max(1) as u64
}

/// Seconds from `now_unix` until the next midnight on this process's clock.
pub fn seconds_to_next_day(now_unix: i64) -> u64 {
    seconds_to_next_day_in(now_unix, local_offset_secs(now_unix))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An instant that is noon UTC, so a whole day is comfortably on both sides.
    const NOON_UTC: i64 = 1_600_000_000 - (1_600_000_000 % SECONDS_PER_DAY) + 12 * 3600;
    /// Eight hours east, the offset the cluster runs on.
    const EAST: i64 = 8 * 3600;

    #[test]
    fn a_day_starts_at_midnight_on_the_clock_it_is_asked_about() {
        // UTC midnight is the same instant as 08:00 on an eight-hour-east clock,
        // so the day that begins at 0 UTC has not begun yet over there.
        assert_eq!(day_start_unix_in(0, 0), 0);
        assert_eq!(day_start_unix_in(0, EAST), -EAST);
        assert_eq!(day_start_unix_in(NOON_UTC, 0), NOON_UTC - 12 * 3600);
        assert_eq!(day_start_unix_in(NOON_UTC, EAST), NOON_UTC - 20 * 3600);
        assert_eq!(day_start_unix_in(SECONDS_PER_DAY - 1, 0), 0);
        assert_eq!(day_start_unix_in(SECONDS_PER_DAY, 0), SECONDS_PER_DAY);
        // Before the epoch the floor has to go down, not toward zero.
        assert_eq!(day_start_unix_in(-1, 0), -SECONDS_PER_DAY);
        // One second before the UTC epoch is 07:59:59 on the same date over
        // there, so that day's edge is eight hours before the epoch, not a day down.
        assert_eq!(day_start_unix_in(-1, EAST), -EAST);
    }

    #[test]
    fn a_westward_offset_is_not_the_same_shape_as_an_eastward_one() {
        const WEST: i64 = -5 * 3600;
        // 02:00 UTC is still the previous day at five hours west.
        let at = day_start_unix_in(0, 0) + 2 * 3600;
        assert_eq!(day_start_unix_in(at, WEST), -SECONDS_PER_DAY - WEST);
        // 02:00 UTC is 21:00 the day before, three hours short of midnight.
        assert_eq!(seconds_to_next_day_in(at, WEST), 3 * 3600);
    }

    #[test]
    fn the_wait_until_reset_is_the_rest_of_the_day() {
        assert_eq!(
            seconds_to_next_day_in(day_start_unix_in(NOON_UTC, EAST), EAST),
            SECONDS_PER_DAY as u64
        );
        assert_eq!(
            seconds_to_next_day_in(
                day_start_unix_in(NOON_UTC, EAST) + SECONDS_PER_DAY - 1,
                EAST
            ),
            1
        );
        // The last second is still one, never zero: a zero wait would be read
        // as "retry immediately" by the caller that was just refused.
        assert_eq!(
            seconds_to_next_day_in(day_start_unix_in(NOON_UTC, EAST) + SECONDS_PER_DAY, EAST),
            SECONDS_PER_DAY as u64
        );
    }

    #[test]
    fn the_date_a_counter_is_filed_under_is_the_day_its_edge_starts() {
        use chrono::{DateTime, FixedOffset};
        // Independent of the arithmetic under test: chrono's own conversion of the
        // same instant into the same offset. Every candidate day edge and the two
        // seconds around it, so an implementation that ignored the offset would
        // disagree on the ones that straddle UTC midnight.
        for off in [0, EAST, -5 * 3600, 5 * 3600 + 45 * 60] {
            let zone = FixedOffset::east_opt(off as i32).expect("offset in range");
            for now in [
                NOON_UTC,
                NOON_UTC - 12 * 3600,
                NOON_UTC - 13 * 3600,
                NOON_UTC + 12 * 3600,
                NOON_UTC + 11 * 3600,
            ] {
                let expected = DateTime::from_timestamp(now, 0)
                    .expect("representable")
                    .with_timezone(&zone)
                    .date_naive();
                assert_eq!(date_at(now, off), expected, "offset {off} at {now}");
                // The date must be the one whose edge `day_start_unix_in` names.
                let edge = day_start_unix_in(now, off);
                assert_eq!(
                    DateTime::from_timestamp(edge, 0)
                        .expect("representable")
                        .with_timezone(&zone)
                        .date_naive(),
                    expected,
                    "edge agrees with the date at offset {off} and {now}"
                );
            }
        }
    }

    #[test]
    fn the_process_clock_form_agrees_with_the_offset_it_reads() {
        let offset = local_offset_secs(NOON_UTC);
        assert_eq!(
            day_start_unix(NOON_UTC),
            day_start_unix_in(NOON_UTC, offset)
        );
        assert_eq!(
            seconds_to_next_day(NOON_UTC),
            seconds_to_next_day_in(NOON_UTC, offset)
        );
        assert_eq!(local_date_naive(NOON_UTC), date_at(NOON_UTC, offset));
    }
}
