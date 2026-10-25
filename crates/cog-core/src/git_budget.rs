//! How long a git command may run, and when a lock it left behind is a corpse.
//!
//! Two crates run git against a volume they do not control -- the sandbox
//! executor against its task worktrees, the reflection loop against its change
//! workspaces -- and both need the same two answers. A period chosen once is
//! wrong in both directions on a volume that changes: a repository that grew
//! past a full-tree checkout gets killed mid-command, and a volume that got
//! faster sits out a ceiling it no longer needs. Both crates therefore measure
//! the slowest command that has completed and take a multiple of it.
//!
//! The arithmetic lives here so the two loops cannot drift apart on it: two
//! copies of one control law is the same defect as two copies of one figure.

use std::time::Duration;

/// The ceiling before any command has completed here: only this is available.
pub const GIT_TIMEOUT_FLOOR_SECS: u64 = 120;

/// How many times the slowest completed command a command may take.
pub const GIT_TIMEOUT_MULTIPLE: u64 = 10;

/// How long a git command may run, from the slowest one that has completed.
pub fn git_timeout_from(slowest_ms: u64) -> Duration {
    let measured = (slowest_ms / 1000).saturating_mul(GIT_TIMEOUT_MULTIPLE);
    Duration::from_secs(measured.max(GIT_TIMEOUT_FLOOR_SECS))
}

/// How old a git lock has to be before it can only be a corpse.
///
/// Killed git processes (OOM, node restart) leave `.lock` files behind, and
/// every git write in these crates is bounded by [`git_timeout_from`] and
/// kill-on-drop, so a lock older than twice that ceiling cannot belong to a
/// live command: the command that made it would have been killed first. Twice
/// rather than once, because a command may have taken the lock just as this
/// pass looked at the clock.
pub fn stale_lock_age_from(timeout: Duration) -> Duration {
    timeout * 2
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ceiling follows what git has actually cost here, so a volume that
    /// grew or got slower moves it, and the corpse age moves with it.
    #[test]
    fn the_timeout_follows_the_slowest_command_that_finished() {
        // Nothing has completed yet: the floor is the only evidence available.
        assert_eq!(
            git_timeout_from(0),
            Duration::from_secs(GIT_TIMEOUT_FLOOR_SECS)
        );
        // A 41-second full-tree checkout makes the ceiling its ten times.
        assert_eq!(git_timeout_from(41_000), Duration::from_secs(410));
        // Slower than the floor: the floor stops constraining anything.
        assert_eq!(git_timeout_from(120_000), Duration::from_secs(1200));
        // The corpse age is twice whatever ceiling is in force, not a
        // separate figure that can disagree with it.
        assert_eq!(
            stale_lock_age_from(Duration::from_secs(410)),
            Duration::from_secs(820)
        );
        assert_eq!(
            stale_lock_age_from(git_timeout_from(0)),
            Duration::from_secs(GIT_TIMEOUT_FLOOR_SECS * 2)
        );
    }
}
