//! How the executor arms the host-wide build gate (`cog_core::build_gate`)
//! from the configuration it was handed.
//!
//! The gate itself — slot files, flocks, counters — lives in
//! `cog_core::build_gate`. What lives here is the decision about the *numbers*,
//! because the one mis-set number that keeps hurting is the wait budget: with a
//! bound of a single slot, a queued one-shot build (a change verification has no
//! next cycle to come back to) waits behind a holder whose own work is bounded
//! by `build_timeout_secs`. A wait budget shorter than that ceiling refuses the
//! queued build *before the holder is even out of bounds*: the build did not
//! fail, it never ran, and the refusal counter climbs on a host that was never
//! saturated. That is exactly what the `build_gate_refused` alert was reporting
//! — the budget was set below the longest legal hold.
//!
//! [`crate::build_gate::effective_config`] therefore floors a nonzero wait
//! budget at the build timeout plus a release margin. Deriving the floor from
//! `build_timeout_secs` (rather than raising a constant) keeps the two knobs
//! consistent forever: raising the build timeout automatically raises the wait
//! floor, so the misconfiguration cannot silently come back. An explicit
//! `wait_secs = 0` keeps its documented meaning — try once, no queue — and is
//! left alone.

use cog_core::config::BuildGateConfig;

/// Extra seconds past the build timeout that a waiter is given, so a holder
/// that runs right up to its bound still has time to release before the waiter
/// is refused.
pub const WAIT_RELEASE_MARGIN_SECS: u64 = 600;

/// The gate configuration this process should actually arm, given the build
/// timeout its builds are killed at.
///
/// A nonzero wait budget shorter than `build_timeout_secs + margin` is raised
/// to that floor; everything else is passed through unchanged, so a deliberately
/// generous operator setting is never lowered and a deliberate zero stays zero.
pub fn effective_config(cfg: &BuildGateConfig, build_timeout_secs: u64) -> BuildGateConfig {
    let mut out = cfg.clone();
    let floor = build_timeout_secs.saturating_add(WAIT_RELEASE_MARGIN_SECS);
    if out.wait_secs > 0 && out.wait_secs < floor {
        out.wait_secs = floor;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(wait_secs: u64) -> BuildGateConfig {
        BuildGateConfig {
            enabled: true,
            max_concurrent: 1,
            wait_secs,
            lock_dir: "/tmp/build-gate-test".into(),
        }
    }

    /// The regression the alert reported: a default 1800s budget against a
    /// 3600s build timeout refuses every build queued behind a legal
    /// full-length holder. The armed gate must wait at least as long as the
    /// longest legal hold, plus the release margin.
    #[test]
    fn a_budget_below_the_longest_legal_hold_is_raised_to_cover_it() {
        let out = effective_config(&cfg(1800), 3600);
        assert_eq!(out.wait_secs, 3600 + WAIT_RELEASE_MARGIN_SECS);
        // The concurrency bound and the slot directory are the host's own
        // properties; flooring the budget must not touch them.
        assert_eq!(out.max_concurrent, 1);
        assert_eq!(out.lock_dir, "/tmp/build-gate-test");
    }

    /// A budget already past the floor is the operator's own answer and is
    /// kept as-is: the floor exists to stop refusals of legal waits, not to
    /// cap patience.
    #[test]
    fn a_budget_past_the_floor_is_kept() {
        let out = effective_config(&cfg(7200), 3600);
        assert_eq!(out.wait_secs, 7200);
    }

    /// Zero means "try once, never queue"; flooring it into a queue would
    /// silently change what the operator asked for.
    #[test]
    fn an_explicit_no_queue_budget_is_kept() {
        let out = effective_config(&cfg(0), 3600);
        assert_eq!(out.wait_secs, 0);
    }

    /// The floor follows the build timeout: raising the timeout raises the
    /// floor, so the two knobs cannot drift back into budget < longest hold.
    #[test]
    fn the_floor_tracks_the_build_timeout() {
        assert_eq!(
            effective_config(&cfg(100), 7200).wait_secs,
            7200 + WAIT_RELEASE_MARGIN_SECS
        );
        assert_eq!(
            effective_config(&cfg(100), 60).wait_secs,
            60 + WAIT_RELEASE_MARGIN_SECS
        );
    }
}
