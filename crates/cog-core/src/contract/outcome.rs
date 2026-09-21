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

/// Cause carried in-band by a generator that returned an envelope with neither
/// content nor artifacts, and named no cause of its own.
///
/// Deliberately not [`TERMINAL_ENV_FAILURE_PREFIX`]: the prompt demonstrably
/// reached the upstream and the model answered with a well-formed envelope, so
/// nothing observed here rules out the next attempt. Filing it as an environment
/// failure ends retries on the strength of a fact that was never seen, and
/// points the reader at the transport while the defect is in what the generator
/// produced. It is the generator's own name for "I produced nothing", which the
/// repair loop is the thing that exists to act on.
pub const EMPTY_GENERATION_PREFIX: &str = "empty_generation";

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
    [TERMINAL_ENV_FAILURE_PREFIX, DEGENERATE_LOOP_PREFIX]
        .into_iter()
        .any(|prefix| declares(reason, prefix))
}

/// Whether `reason` carries `prefix` as a declared label.
///
/// The producer writes the marker first, but the reason then travels through
/// layers that prepend their own context, and this crate's own error type does
/// exactly that when it renders: every wrapping variant of `SFError` joins the
/// inner text as `"<context>: <inner>"`. A bare `starts_with` therefore reads
/// only the outermost wrapper's prose and misses the declaration underneath it —
/// which looks identical to "no cause was declared", so the retry loop buys
/// another attempt for a failure that cannot clear, and the classification
/// counter records the run as unclassified while the corresponding events keep
/// happening.
///
/// The marker is recognised in the two shapes a declaration can take: leading
/// the reason, or opening a clause after a wrapper boundary (`": "`). Requiring
/// the trailing `:` in the wrapped shape keeps a sentence that merely mentions
/// the words — prose, not a classification — from counting as one.
///
/// Every reader of these markers asks here rather than re-implementing the
/// match: a second copy keeps compiling when the shapes drift apart and then
/// silently answers "nothing was declared" for reasons that were.
pub fn declares(reason: &str, prefix: &str) -> bool {
    if reason.starts_with(prefix) {
        return true;
    }
    let mut needle = String::with_capacity(prefix.len() + 3);
    needle.push_str(": ");
    needle.push_str(prefix);
    needle.push(':');
    reason.contains(&needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_declared_cause_is_recognised_in_both_wire_forms() {
        assert!(is_deterministic_failure(
            "terminal_env_failure: generator prompt failed: HTTP 503"
        ));
        assert!(is_deterministic_failure(
            "degenerate_loop: same failure 3 times"
        ));
    }

    /// An empty envelope is a defect in what the generator produced, not a
    /// statement about the transport, so it must keep its retries: the next
    /// attempt may well answer. Reading it as deterministic would end them on
    /// evidence nobody observed and file it under the wrong role.
    #[test]
    fn an_empty_envelope_keeps_its_retries() {
        assert!(!is_deterministic_failure(&format!(
            "{EMPTY_GENERATION_PREFIX}: the generator returned an envelope with no content and no artifacts"
        )));
        // ...including when it arrives wrapped by the error type.
        assert!(!is_deterministic_failure(&format!(
            "Agent execution error: {EMPTY_GENERATION_PREFIX}: nothing produced"
        )));
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

    /// A declaration survives the wrapping that the error type applies on its
    /// way out. `SFError::Agent` renders as `"Agent execution error: {inner}"`,
    /// so a reason that arrives through it begins with the wrapper's prose and
    /// only then carries the marker. Reading position zero alone made this the
    /// common case, not an edge case: the failure that motivated the predicate
    /// reached the retry decision in exactly this shape.
    #[test]
    fn a_wrapper_does_not_hide_the_declaration() {
        assert!(is_deterministic_failure(
            "Agent execution error: terminal_env_failure: generator prompt failed: HTTP 503"
        ));
        assert!(is_deterministic_failure(
            "Dag-executor error: degenerate_loop: same failure 3 times"
        ));
        // Nesting the wrappers must not defeat it either.
        assert!(is_deterministic_failure(
            "LLM provider error: Agent execution error: terminal_env_failure: generator prompt failed: HTTP 503"
        ));
    }

    /// Widening the match to "the words appear somewhere" would end retries for
    /// ordinary failures whose text happens to discuss the marker. Only a label —
    /// the marker opening a clause and terminated by its colon — counts.
    #[test]
    fn a_mention_inside_prose_is_still_not_a_declaration() {
        assert!(!is_deterministic_failure(
            "warning: terminal_env_failure is the name of the marker we look for"
        ));
    }
}
