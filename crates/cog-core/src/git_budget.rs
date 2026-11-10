//! When a git lock left behind is a corpse.
//!
//! Two crates run git against a volume they do not control -- the sandbox
//! executor against its task worktrees, the reflection loop against its change
//! workspaces -- and both need the same answer.
//!
//! A git command is no longer bounded by a time: it is stopped when it stops
//! moving (`cog_core::progress`), at the same measured window every other judged
//! child is stopped at. That same judgement is what makes a leftover lock
//! readable as a corpse. A live command beats at least once per window (a byte
//! of output, or CPU time advancing), so a command silent for longer than the
//! window is killed by its own watchdog -- therefore a lock older than that
//! cannot belong to a live command.
//!
//! The arithmetic lives here so the two loops cannot drift apart on it: two
//! copies of one control law is the same defect as two copies of one figure.

use std::time::Duration;

/// How old a git lock has to be before it can only be a corpse: twice the
/// longest silence this deployment has ever seen from a command that was still
/// working.
///
/// Twice rather than once, because a command may have taken the lock just as
/// this pass looked at the clock: a lock that just missed being killed is still
/// live.
///
/// `Duration::MAX` until the deployment has measured a window -- and that is a
/// reading, not a fallback: before anything has been measured, nothing here
/// knows how quiet a live command gets, so no lock can be named a corpse. A
/// substitute figure would be a wall wearing the clothes of a measurement, and
/// this one clears lock files that other processes may be holding right now.
pub fn stale_lock_age() -> Duration {
    match crate::progress::longest_gap() {
        Some(window) => window.saturating_mul(2),
        None => Duration::MAX,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::progress;

    /// The corpse age follows the measured window and is absent until there is
    /// one: a deployment that has not measured a gap yet must not clear any
    /// lock, because it cannot tell a corpse from a command still working.
    #[test]
    fn the_corpse_age_follows_the_measured_window() {
        let _guard = progress::test_lock();
        progress::reset_for_tests();
        // Nothing measured: no lock can be judged a corpse.
        assert_eq!(stale_lock_age(), Duration::MAX);

        progress::seed_gaps_for_tests(Duration::from_secs(40), progress::SAMPLE_FLOOR);
        // Twice the longest gap that was still work in progress.
        assert_eq!(stale_lock_age(), Duration::from_secs(80));

        // A heavier deployment moves it: the same law, a new window.
        progress::reset_for_tests();
        progress::seed_gaps_for_tests(Duration::from_secs(610), progress::SAMPLE_FLOOR);
        assert_eq!(stale_lock_age(), Duration::from_secs(1220));
        progress::reset_for_tests();
    }
}
