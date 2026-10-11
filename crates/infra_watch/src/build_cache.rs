//! The deferral side of the build-cache reclaim watcher.
//!
//! The watcher itself lives in `cog_core::build_cache_readings`: it walks the
//! cache, publishes what the walk found, and runs the pass that pays the cache
//! down to its cap. This file is the reading of that watcher that the stalled
//! alert's window is counted in -- consecutive walks whose pass was deferred
//! (`busy` or `ungated`), so the walk that crosses the window is a fact the
//! watcher can name rather than a shape a reader has to infer from a climbing
//! counter.
//!
//! A deferral is counted in walks, not in passes, matching how the rule ages
//! the run: a pass that freed bytes and left the cache over has not ended the
//! run, but it did run, so it is not a deferral; a walk that found the cache
//! under its cap ends both.

/// Walks in a row whose pass was deferred because no deletion was safe.
///
/// `busy` and `ungated` share the streak because the question it answers --
/// how long has nothing paid the cap down -- does not depend on which of the
/// two held the pass back. Which one dominates is a separate fact, read off
/// the per-walk outcome counters this streak sits beside.
#[derive(Debug, Default, Clone, Copy)]
pub struct DeferralStreak(u64);

impl DeferralStreak {
    pub fn new() -> Self {
        Self(0)
    }

    /// One more walk whose pass was deferred; the streak after the count.
    ///
    /// Saturating: a streak that cannot overflow never has to be reset for
    /// the counter's own sake, only for the cache's.
    pub fn note(&mut self) -> u64 {
        self.0 = self.0.saturating_add(1);
        self.0
    }

    /// A walk found the cache under its cap, or a pass ran: the run of walks
    /// with nothing paying the cap down is over.
    pub fn reset(&mut self) {
        self.0 = 0;
    }

    /// Walks in the current run.
    pub fn walks(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_streak_counts_consecutive_deferrals_and_resets() {
        let mut streak = DeferralStreak::new();
        assert_eq!(streak.walks(), 0);
        assert_eq!(streak.note(), 1);
        assert_eq!(streak.note(), 2);
        streak.reset();
        assert_eq!(streak.walks(), 0);
        assert_eq!(streak.note(), 1, "the reset run counts from its own start");
    }

    #[test]
    fn a_streak_never_overflows() {
        let mut streak = DeferralStreak(u64::MAX);
        assert_eq!(streak.note(), u64::MAX);
    }
}
