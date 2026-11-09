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

/// Prefix carried by a run that stopped because the process running it was told
/// to (a rollout, a node draining): the work reached a turn boundary and left
/// its journal open so the next dispatch continues from there.
///
/// It is not a failure of the work, so it must not be charged to the task's
/// retry budget — a rollout every hour would otherwise spend a task's whole
/// budget on the environment and drop work that never went wrong. Nor is it
/// [`TERMINAL_ENV_FAILURE_PREFIX`]: nothing says a re-run fails again; the next
/// process is exactly the thing that can finish it.
pub const PROCESS_SHUTTING_DOWN_PREFIX: &str = "process_shutting_down";

/// Prefix carried by a round that never began because the pool had no member to
/// hand out.
///
/// Not a failure of the work either: the round did not run, so nothing was
/// learned about the task. Charging it to the task's retry budget is how a
/// single-member pool that refuses twice spends a task's whole budget on
/// rounds that never started — measured 2026-11-09: 42 occurrences in 24 hours,
/// each one charged.
pub const LLM_POOL_PAUSED_PREFIX: &str = "llm_pool_paused";

/// Why a failure happened, typed end to end.
///
/// [`UpstreamFailure`] covers one of the ways the environment denies a round —
/// "the upstream refused this call" — and every variant of it is read off an
/// HTTP status. Two more ways deny a round without ever producing a status: the
/// pool is paused, so no member is handed out, and the process is asked to stop
/// at a turn boundary. Both are "this was not the work's fault", and neither is
/// an upstream refusal, so they live here rather than in `UpstreamFailure`,
/// whose `from_status` is its only producer.
///
/// Why a union rather than a wider list of prefixes: the verdict "who pays"
/// used to read the rendered reason against three prefixes, and a producer that
/// wrote a fourth wording was charged to the work in silence — `llm_pool_paused`
/// was exactly such a wording. A producer here has to name a variant to
/// compile; a reader matches exhaustively, so a new way to be denied cannot be
/// added on one side only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureCause {
    /// The upstream refused this call; the cause came from the transport signal.
    Upstream(UpstreamFailure),
    /// The pool was paused: this round never began.
    PoolPaused,
    /// The process was asked to stop: the run ended at its turn boundary and the
    /// next dispatch continues from there.
    ProcessStopping,
}

impl FailureCause {
    /// Whether re-issuing the identical work could succeed with no external
    /// change. Only the upstream's own terminal causes (quota, credentials) say
    /// no; a paused pool or a stopping process both end on their own.
    pub fn is_terminal(self) -> bool {
        match self {
            Self::Upstream(cause) => cause.is_terminal(),
            Self::PoolPaused | Self::ProcessStopping => false,
        }
    }

    /// Whether the environment failed to serve the work, as opposed to the work
    /// being malformed. A paused pool and a stopping process never reflect on
    /// the work.
    pub fn is_environment_failure(self) -> bool {
        match self {
            Self::Upstream(cause) => cause.is_environment_failure(),
            Self::PoolPaused | Self::ProcessStopping => true,
        }
    }

    /// The name this cause is persisted under and [`Self::from_name`] reads back.
    /// Upstream causes keep their own names so a stored row means the same thing
    /// it meant before this type existed.
    pub fn name(self) -> &'static str {
        match self {
            Self::Upstream(cause) => cause.name(),
            Self::PoolPaused => "pool_paused",
            Self::ProcessStopping => "process_stopping",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        if let Some(cause) = UpstreamFailure::from_name(name) {
            return Some(Self::Upstream(cause));
        }
        [Self::PoolPaused, Self::ProcessStopping]
            .into_iter()
            .find(|cause| cause.name() == name)
    }

    /// Every cause, in the order their names render. The two lists — this one
    /// and [`Self::name`] — are two views of one closed set; the test below
    /// pairs them.
    pub const ALL: [Self; 8] = [
        Self::Upstream(UpstreamFailure::RateLimited),
        Self::Upstream(UpstreamFailure::QuotaExhausted),
        Self::Upstream(UpstreamFailure::Auth),
        Self::Upstream(UpstreamFailure::ServerError),
        Self::Upstream(UpstreamFailure::BadRequest),
        Self::Upstream(UpstreamFailure::Transport),
        Self::PoolPaused,
        Self::ProcessStopping,
    ];

    /// The prefix this cause renders its reason under.
    ///
    /// The in-band declaration stays — layers between the producer and the
    /// retry decision still flatten errors to text, and a reason that carries
    /// nothing would be re-run blind — but it is now derived from the variant
    /// rather than picked per call site, so the two cannot drift.
    pub fn prefix(self) -> &'static str {
        match self {
            Self::Upstream(_) => UPSTREAM_UNAVAILABLE_PREFIX,
            Self::PoolPaused => LLM_POOL_PAUSED_PREFIX,
            Self::ProcessStopping => PROCESS_SHUTTING_DOWN_PREFIX,
        }
    }

    /// The reason text this cause renders for `detail`.
    pub fn render(self, detail: &str) -> String {
        format!("{}: {detail}", self.prefix())
    }
}

impl std::fmt::Display for FailureCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// Serialised through [`FailureCause::name`] for the same reason
/// [`UpstreamFailure`] is: the derive would spell the variant, and a stored row
/// read back under another word reads as "nobody declared a cause".
impl serde::Serialize for FailureCause {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.name())
    }
}

impl<'de> serde::Deserialize<'de> for FailureCause {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = <String as serde::Deserialize>::deserialize(deserializer)?;
        Self::from_name(&name).ok_or_else(|| {
            serde::de::Error::custom(format!(
                "unknown failure cause {name:?}; declared names are {:?}",
                FailureCause::ALL.map(|cause| cause.name())
            ))
        })
    }
}

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

/// Cause a role writes into its own output content when its call failed for an
/// environment reason: `environment_error: <the error>`.
///
/// The role that calls the model and the loop that judges its output are
/// different crates, and what crosses between them is the content string. The
/// judge decides "this role produced nothing because the environment failed"
/// by finding this word in that string — so the word is written by
/// [`environment_error_content`] and read through this constant, never spelled
/// out on either side. A re-typed copy keeps compiling after one side changes
/// its wording and then matches nothing, which reads as "the environment
/// stopped failing" on a run where it failed on the first call.
pub const ENVIRONMENT_ERROR_PREFIX: &str = "environment_error";

/// The in-band content a role publishes when its call failed for an environment
/// reason, carrying the error's own text beside the cause.
///
/// The error goes with the marker rather than after it: a reader deciding
/// whether to retry needs the typed upstream refusal inside that text, and a
/// marker alone would send the next attempt to the same wall.
pub fn environment_error_content(err: impl std::fmt::Display) -> String {
    format!("{ENVIRONMENT_ERROR_PREFIX}: {err}")
}

/// Cause carried in-band by a run that ended on [`MAX_ITERATIONS_STATUS`].
///
/// Without it the empty result is indistinguishable from a producer that
/// finished and chose to return nothing, so the run gets recorded as a
/// generator defect with the upstream blamed, while what actually ran out was
/// the loop's own budget. Producer and reader are different crates, so the
/// marker lives here with the other wire markers: a re-typed literal would keep
/// compiling after a wording change and silently stop matching.
pub const ITERATION_BUDGET_EXHAUSTED_MARKER: &str = "iteration_budget_exhausted";

/// 那一轮的上下文窗口连地板（system 与首条 user）都装不下时的结局字串。
///
/// 与 [`ITERATION_BUDGET_EXHAUSTED_MARKER`] 分开：那一个说「再给几轮就行」，
/// 这一个说「给多少轮都一样」——每条结果进来都在同一拍被裁掉，下一轮发出去的
/// 请求与这一轮逐字节相同。处置也相反：前者抬预算，后者改输入的大小或改窗口。
/// 生产者（agent 的循环）与读者（协作侧的解析）是不同的 crate，所以这个字串
/// 和其它线标一起放在这里，免得两边各拼一份、改一处之后静默不匹配。
pub const CONTEXT_FLOOR_EXCEEDS_WINDOW_STATUS: &str = "context_floor_exceeds_window";

/// 一条运行长时间不改工作树，结局字串。
///
/// 与 [`CONTEXT_FLOOR_EXCEEDS_WINDOW_STATUS`] 和 [`ITERATION_BUDGET_EXHAUSTED_MARKER`]
/// 都分开，因为处置第三种：那两个缺的是窗口或轮数，这一个缺的是「动手」——
/// 它读、它搜、它打印，被提醒过两次仍然不动，多给的时间和轮数只会换来同样的
/// 一批只读调用。集群库实测（2026-11-06）：完成的变更任务从开跑到收尾中位数
/// 11.5 分钟、九成在 43 分钟内结束；失败的 26 条中位数 42.9 分钟、九成撞满
/// 60 分钟的墙，而它们一小时里一次都没改过工作树。
///
/// 生产者（agent 的循环）与读者（协作侧的解析）是不同的 crate，所以与其它
/// 线标一起放在这里。
pub const NO_CHECKOUT_PROGRESS_STATUS: &str = "no_checkout_progress";

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

    /// 每个原因都能从自己渲染出的名字读回来，存盘与读盘走同一条词表。
    #[test]
    fn every_failure_cause_reads_back_from_its_own_name() {
        for cause in FailureCause::ALL {
            assert_eq!(
                FailureCause::from_name(cause.name()),
                Some(cause),
                "{cause} 的名字读不回来，存下去的那一行就成了没人认识的原因"
            );
        }
    }

    /// 原因渲染成错误文本时带上前缀，而每种原因的渲染里那句前缀恰好是它自己的。
    ///
    /// 这是类型与带内声明之间的往返：层与层之间仍然会把错误拍平成文本，判决
    /// 虽然读类型，读日志的人和只拿到文本的那一层读到的必须是同一句话。
    #[test]
    fn every_cause_renders_under_the_prefix_a_text_reader_looks_for() {
        for cause in FailureCause::ALL {
            let rendered = cause.render("why");
            assert!(
                rendered.starts_with(cause.prefix()),
                "{cause} 渲染成 {rendered:?}，没有以它自己的前缀开头"
            );
            assert_eq!(
                rendered.split_once(": ").map(|(p, _)| p),
                Some(cause.prefix()),
                "{cause} 的前缀必须恰好是渲染里冒号前那一段"
            );
        }
        // 两种没有上游参与的原因，前缀就是它们被认出来的那两句。
        assert_eq!(
            FailureCause::PoolPaused.prefix(),
            LLM_POOL_PAUSED_PREFIX,
            "池停供在文本面被认作这一句"
        );
        assert_eq!(
            FailureCause::ProcessStopping.prefix(),
            PROCESS_SHUTTING_DOWN_PREFIX,
            "进程被要求停下在文本面被认作这一句"
        );
    }

    /// 池停供与进程停下不是终态：环境自己会结束，重跑一次就能接着做。
    #[test]
    fn an_environment_pause_is_never_terminal() {
        for cause in [FailureCause::PoolPaused, FailureCause::ProcessStopping] {
            assert!(!cause.is_terminal(), "{cause} 会自己结束");
            assert!(cause.is_environment_failure(), "{cause} 不是这份活的错");
        }
        assert!(FailureCause::Upstream(UpstreamFailure::QuotaExhausted).is_terminal());
    }
}
