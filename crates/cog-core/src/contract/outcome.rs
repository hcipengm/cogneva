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

use crate::contract::llm::UpstreamFailure;

/// Prefix marking a run whose failure cause is deterministic (environment or
/// upstream protocol), so retry/upgrade loops can stop instead of re-paying for
/// attempts that must fail again.
pub const TERMINAL_ENV_FAILURE_PREFIX: &str = "terminal_env_failure";

/// Prefix marking a run that spent without making progress: the same failure
/// repeating, or cost rising with nothing to show for it.
pub const DEGENERATE_LOOP_PREFIX: &str = "degenerate_loop";

/// Prefix carried by a policy refusal — a guardrail rule declined the call.
///
/// Not an environment failure and not a fault: the rule is still in force, so
/// the identical call is refused the same way however many times it is bought.
/// It travels in-band because the dispatch layer flattens errors to text before
/// it decides whether to retry; a refusal outside this list would receive the
/// task type's ordinary retry budget and pay for the same answer again.
pub const GUARDRAIL_BLOCKED_PREFIX: &str = "guardrail_blocked";

/// A refusal as the layer that will re-dispatch the work needs it: which tool
/// was refused, by which rule, and what the rule said.
///
/// The producer has this typed ([`crate::SFError::Blocked`]) and the consumer
/// gets a string — the same gap [`UpstreamFailure::named_in`] closes for
/// refusals that come from an upstream. Without it the dispatch layer can only
/// decide "do not retry"; with it, the next attempt can be told what not to
/// propose, which is the one thing that makes a re-dispatch different from a
/// re-run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuardrailRefusal {
    pub tool: String,
    pub rule: String,
    pub reason: String,
}

impl GuardrailRefusal {
    /// The payload planted in a task's input, which is the part of the task the
    /// role actually reads. The instruction rides with the facts: a rule name on
    /// its own tells the model what happened, not what to do about it.
    pub fn payload(&self) -> serde_json::Value {
        serde_json::json!({
            "tool": self.tool,
            "rule": self.rule,
            "reason": self.reason,
            "instruction": "A guardrail rule refused this call on the previous attempt. \
                            Do not propose it again: complete the work by another route, or \
                            report the rule as the blocker.",
        })
    }
}

/// Recover the refusal a reason declares, in the shape
/// [`crate::SFError::Blocked`] renders.
///
/// `None` when the reason declares the marker but not that shape. The caller
/// then still treats it as a refusal — no retry — but plants no payload: a
/// guessed tool or rule would tell the next attempt to avoid the wrong call,
/// which is worse than telling it nothing.
pub fn guardrail_refusal_in(reason: &str) -> Option<GuardrailRefusal> {
    let marker = if reason.starts_with(GUARDRAIL_BLOCKED_PREFIX) {
        0
    } else {
        reason.find(&format!(": {GUARDRAIL_BLOCKED_PREFIX}:"))? + 2
    };
    let rest = reason[marker + GUARDRAIL_BLOCKED_PREFIX.len()..].strip_prefix(": ")?;
    let rest = rest.strip_prefix("Guardrail blocked tool '")?;
    let (tool, rest) = rest.split_once("': ")?;
    // Split from the right: the rule is the last field the renderer writes, and
    // a rule's own reason may itself contain the separator prose.
    let (body, rule) = rest.strip_suffix(')')?.rsplit_once(" (rule: ")?;
    if tool.is_empty() || rule.is_empty() {
        return None;
    }
    Some(GuardrailRefusal {
        tool: tool.to_string(),
        rule: rule.to_string(),
        reason: body.to_string(),
    })
}

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

/// Prefix marking a run whose cause is an upstream refusal the environment can
/// clear on its own — a rate limit, a server error, a broken transport.
///
/// Deliberately not [`TERMINAL_ENV_FAILURE_PREFIX`]: the refusal says the
/// upstream did not serve this call, and nothing about it rules out the next
/// attempt. Producers reach for the terminal marker because a prompt failure
/// they cannot tell apart from a transport that never arrived looks the same
/// from where they stand — but the typed cause travels in the same text and is
/// finer-grained than the blanket declaration around it. Publishing the
/// terminal label anyway puts a fate the cause denies onto a row the retry
/// ladder is scheduling the next attempt for at that very moment, and the
/// reader has to know to distrust it.
pub const UPSTREAM_UNAVAILABLE_PREFIX: &str = "upstream_unavailable";

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
///
/// A terminal declaration is honoured unless the reason it is wrapped around
/// itself names an upstream refusal the environment can clear on its own. A
/// producer writes the terminal marker for any prompt failure it cannot tell
/// apart from a transport that never reached the upstream, so a typed server
/// error or rate limit gets filed as deterministic and the work is dropped with
/// no retry — precisely during the outage a later attempt would have survived.
/// The typed cause is finer-grained than the blanket declaration around it and
/// wins.
pub fn is_deterministic_failure(reason: &str) -> bool {
    if declares(reason, DEGENERATE_LOOP_PREFIX) {
        return true;
    }
    // A refusal is settled by the rule that made it, and a re-run does not
    // change rules. Its budget is zero for the same reason a repeated failure's
    // is: another attempt cannot answer differently.
    if declares(reason, GUARDRAIL_BLOCKED_PREFIX) {
        return true;
    }
    if !declares(reason, TERMINAL_ENV_FAILURE_PREFIX) {
        return false;
    }
    // The iteration-budget marker rides inside the terminal envelope when a
    // role translates its spent ReAct budget into an environment error. It is
    // not a deterministic cause: the per-role adaptive budget is raised from
    // exactly this observation, so the next attempt runs with more room.
    if declares(reason, ITERATION_BUDGET_EXHAUSTED_MARKER) {
        return false;
    }
    match UpstreamFailure::named_in(reason) {
        // A refusal the environment can clear by itself is a failure to come
        // back to, not one to give up on.
        Some(cause) if a_refusal_clears_on_its_own(cause) => false,
        _ => true,
    }
}

/// Whether a typed upstream refusal clears on its own: what changes the answer
/// is the environment (a window resetting, a service coming back, a transport
/// recovering), so re-running the identical request later is what buys
/// something. Its complement — quota, credentials, and the request being
/// malformed — is settled by the refusal itself.
///
/// The one place this is decided, so that a producer choosing which marker to
/// publish a refusal under and a consumer deciding whether to retry it cannot
/// answer differently about the same typed cause.
pub fn a_refusal_clears_on_its_own(cause: UpstreamFailure) -> bool {
    cause.is_environment_failure() && !cause.is_terminal()
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

    /// A guardrail refusal ends the task's retries: the rule that refused the
    /// call is still in force, so another attempt cannot answer differently.
    /// Without the marker the dispatch layer reads the refusal as an ordinary
    /// failure and hands it the task type's budget.
    #[test]
    fn a_guardrail_refusal_keeps_no_retries() {
        assert!(is_deterministic_failure(
            &crate::SFError::Blocked {
                tool: "http_request".into(),
                reason: "not allowed here".into(),
                rule: "no_external_io".into(),
            }
            .to_string()
        ));
    }

    /// The dispatch layer gets a string, so the fields it needs to re-dispatch
    /// — which tool, which rule — have to survive the rendering. Losing them
    /// leaves it able to say "do not retry" and nothing more, which is the
    /// difference between re-planning and re-running.
    #[test]
    fn a_refusal_is_recoverable_from_the_text_the_bus_carries() {
        let rendered = crate::SFError::Blocked {
            tool: "git_commit".into(),
            reason: "this scope may not write to the shared tree".into(),
            rule: "no-shared-tree-writes".into(),
        }
        .to_string();
        let parsed = guardrail_refusal_in(&rendered).expect("the rendered refusal must parse");
        assert_eq!(parsed.tool, "git_commit");
        assert_eq!(parsed.rule, "no-shared-tree-writes");
        assert_eq!(parsed.reason, "this scope may not write to the shared tree");

        // The wrapper shape is the common one: the error type prepends its own
        // prose before the declaration reaches the retry decision.
        let wrapped = format!("Agent execution error: {rendered}");
        assert_eq!(
            guardrail_refusal_in(&wrapped).map(|r| r.rule),
            Some("no-shared-tree-writes".to_string())
        );

        // A reason that itself contains the separator prose must not steal the
        // split: the rule is the last field the renderer writes.
        let nested = crate::SFError::Blocked {
            tool: "http_request".into(),
            reason: "blocked by policy (rule: inner) while talking to a mirror".into(),
            rule: "outer-rule".into(),
        }
        .to_string();
        assert_eq!(
            guardrail_refusal_in(&nested).map(|r| r.rule),
            Some("outer-rule".to_string())
        );
    }

    /// A marker without the rendered fields yields nothing. The caller still
    /// treats it as a refusal, so the safe reading is "no payload", never a
    /// guessed tool name — the next attempt would be told to avoid the wrong
    /// call.
    #[test]
    fn a_refusal_without_its_fields_yields_no_payload() {
        assert!(guardrail_refusal_in(&format!(
            "{GUARDRAIL_BLOCKED_PREFIX}: something refused somewhere"
        ))
        .is_none());
        assert!(guardrail_refusal_in("").is_none());
        assert!(guardrail_refusal_in("nothing declared here").is_none());
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

    /// A terminal declaration that rides on a typed upstream refusal is only as
    /// terminal as that refusal. A producer cannot tell a transport that never
    /// reached the upstream apart from one the upstream refused, so it declares
    /// both terminal — but a server error or a rate limit clears on its own and
    /// must keep its retries, while quota and credentials do not.
    #[test]
    fn a_terminal_declaration_defers_to_the_refusal_it_reports() {
        let declared = |cause: UpstreamFailure| {
            format!(
                "Agent execution error: terminal_env_failure: environment_error: \
                 LLM upstream refused ({cause}): LLM stream error: API error (HTTP detail)"
            )
        };

        for cause in [
            UpstreamFailure::ServerError,
            UpstreamFailure::RateLimited,
            UpstreamFailure::Transport,
        ] {
            assert!(
                !is_deterministic_failure(&declared(cause)),
                "{cause} clears on its own and must keep its retries"
            );
        }
        for cause in [
            UpstreamFailure::QuotaExhausted,
            UpstreamFailure::Auth,
            UpstreamFailure::BadRequest,
        ] {
            assert!(
                is_deterministic_failure(&declared(cause)),
                "{cause} cannot be cleared by retrying the same request"
            );
        }

        // A declaration carrying no typed refusal is still honoured, and a
        // degenerate loop stays terminal regardless of any refusal named inside.
        assert!(is_deterministic_failure(
            "terminal_env_failure: generator produced no artifacts"
        ));
        assert!(is_deterministic_failure(&format!(
            "degenerate_loop: {}",
            declared(UpstreamFailure::ServerError)
        )));
    }

    /// The other half of that declaration: a refusal the environment can clear
    /// on its own is published under [`UPSTREAM_UNAVAILABLE_PREFIX`], which is
    /// not a terminal declaration at all. The producer picking the marker and
    /// the consumer deciding on a retry both ask
    /// [`a_refusal_clears_on_its_own`], so the label and the decision cannot
    /// disagree about the same typed cause.
    #[test]
    fn a_refusal_the_environment_can_clear_is_published_as_unavailable() {
        assert!(!is_deterministic_failure(&format!(
            "{UPSTREAM_UNAVAILABLE_PREFIX}: environment_error: LLM upstream refused (server_error): HTTP 503"
        )));

        for cause in [
            UpstreamFailure::ServerError,
            UpstreamFailure::RateLimited,
            UpstreamFailure::Transport,
        ] {
            assert!(
                a_refusal_clears_on_its_own(cause),
                "{cause} is the environment's to clear"
            );
        }
        for cause in [
            UpstreamFailure::QuotaExhausted,
            UpstreamFailure::Auth,
            UpstreamFailure::BadRequest,
        ] {
            assert!(
                !a_refusal_clears_on_its_own(cause),
                "{cause} is settled by the refusal itself"
            );
        }
    }
}
