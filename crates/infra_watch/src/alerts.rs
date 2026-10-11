//! The constants the `build_cache_reclaim_stalled` alert shares with the
//! watcher it reads.
//!
//! The rule itself is deployment configuration; what has to live beside the
//! watcher is the window the rule is written against -- six of the cache's
//! own scan intervals with no pass paying the cap down -- because the watcher
//! escalates in its own log on the walk that crosses it. A watcher whose only
//! record of a stall is a counter that keeps climbing leaves the crossing to
//! be inferred after the fact, by the alert it ended up firing on itself.

/// Walks a pass may be deferred -- `busy` or `ungated` -- before the stall is
/// the one the alert names: a cache over its cap for more than six of its own
/// scan intervals with no pass paying any of it down.
pub const MAX_DEFERRED_WALKS: u64 = 6;

/// The rule this window belongs to, named so the escalation can say which
/// reading of the cache it is the crossing of.
pub const STALLED_RULE: &str = "build_cache_reclaim_stalled";

/// Whether this deferral is the walk on which the stall is escalated.
///
/// Exactly the walk past the window, and only that one: the crossing is
/// logged once per run, and a streak that keeps climbing past it is read off
/// the outcome counters rather than re-announced every walk.
pub fn stall_escalated(deferred_walks: u64) -> bool {
    deferred_walks == MAX_DEFERRED_WALKS + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_escalation_fires_once_on_the_walk_past_the_window() {
        for walks in 0..=MAX_DEFERRED_WALKS {
            assert!(
                !stall_escalated(walks),
                "walk {walks} is still inside the window"
            );
        }
        assert!(stall_escalated(MAX_DEFERRED_WALKS + 1));
        assert!(
            !stall_escalated(MAX_DEFERRED_WALKS + 2),
            "the crossing is announced once, not every walk after it"
        );
    }
}
