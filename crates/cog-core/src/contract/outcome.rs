//! Wire markers that a producer writes in-band onto a failed run's reason, so a
//! consumer that sees only the reason string can tell a deterministic failure
//! from one worth retrying.
//!
//! An empty plan and a plan whose prompt never reached its upstream look
//! identical to every downstream consumer — an empty plan is a valid plan — so
//! the cause has to travel in the reason itself. These prefixes are that
//! carrier.
//!
//! They are defined here, in the shared contract layer, because producers and
//! consumers live in different crates. A consumer that re-types the literal
//! instead of importing it keeps compiling and silently stops matching the day
//! a producer changes the wording, which is how a declared classification ends
//! up permanently empty while the corresponding events keep happening.

/// Prefix marking a run whose failure cause is deterministic (environment or
/// upstream protocol), so retry/upgrade loops can stop instead of re-paying for
/// attempts that must fail again.
pub const TERMINAL_ENV_FAILURE_PREFIX: &str = "terminal_env_failure";

/// Prefix marking a run that spent without making progress: the same failure
/// repeating, or cost rising with nothing to show for it.
pub const DEGENERATE_LOOP_PREFIX: &str = "degenerate_loop";

/// Status the agent runtime reports when its ReAct loop used up its whole
/// iteration budget while tool calls were still pending. The loop stops
/// mid-work, so the result carries neither content nor artifacts.
pub const MAX_ITERATIONS_STATUS: &str = "max_iterations_reached";

/// Cause carried in-band by a run that ended on [`MAX_ITERATIONS_STATUS`].
///
/// Without it the empty result is indistinguishable from a producer that
/// finished and chose to return nothing, so the run gets recorded as a
/// generator defect with the upstream blamed, while what actually ran out was
/// the loop's own budget. Producer and reader are different crates, so the
/// marker lives here with the other wire markers: a re-typed literal would keep
/// compiling after a wording change and silently stop matching.
pub const ITERATION_BUDGET_EXHAUSTED_MARKER: &str = "iteration_budget_exhausted";

/// Whether a failed run's reason declares a cause that re-running it cannot
/// clear, so a retry loop reading only the reason must stop rather than pay for
/// another attempt.
///
/// This is the consumer half of the two prefixes above. Every retry decision
/// asks here instead of re-checking the leading bytes: a second copy keeps
/// compiling after a producer changes the wording and silently starts matching
/// nothing, which reads as "the environment stopped failing" while the failures
/// keep coming — the exact failure mode this module exists to prevent.
///
/// [`DEGENERATE_LOOP_PREFIX`] counts as much as the terminal one. A run that
/// spent without producing anything, or that repeated its own failure, is
/// already the definition of an attempt not worth buying again; the two differ
/// only in whether the cause was known before the first attempt or discovered
/// during it.
pub fn is_deterministic_failure(reason: &str) -> bool {
    reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX) || reason.starts_with(DEGENERATE_LOOP_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_cause_is_recognised_in_both_wire_forms() {
        assert!(is_deterministic_failure(
            "terminal_env_failure: generator produced no artifacts (environment/protocol failure)"
        ));
        assert!(is_deterministic_failure(
            "degenerate_loop: same failure 3 times"
        ));
    }

    /// The predicate reads a declaration, not a mood. A reason that merely talks
    /// about failures staying around is not the same as one that was classified,
    /// and treating it as terminal would quietly end retries for ordinary
    /// transient errors.
    #[test]
    fn an_undeclared_reason_keeps_its_retries() {
        assert!(!is_deterministic_failure("upstream is unreachable"));
        assert!(!is_deterministic_failure(
            "the request failed; terminal_env_failure is not the cause"
        ));
        assert!(!is_deterministic_failure(""));
    }
}
