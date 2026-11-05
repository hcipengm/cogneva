//! principle: feedback_a_shape_check_bounds_the_form_a_closed_set_bounds_the_vocabulary
//!
//! The actors an LLM call may declare, as a closed set.
//!
//! Every call the platform makes to an upstream carries `x-cogneva-actor` so
//! that the call and token series can be read per actor. The set of values is
//! deliberately closed rather than merely shape-checked: the label is what the
//! cost and quota views group by, so a value accepted without being declared
//! here is a new series nobody decided to carry, and on the reading side it is
//! indistinguishable from a feature someone meant to add. The edge that admits
//! a value is [`claimed`], and it admits only what this module declares.
//!
//! A call that names nobody is refused rather than recorded: the platform does
//! not pay for work it cannot attribute. [`UNKNOWN_ACTOR`] stays in the column
//! because the ledger has to keep reading what it already holds — 987 calls and
//! 28.0M input tokens spent on 2026-10-04..10-19, all of them the evaluation
//! harness before its scaffolds stamped themselves — not as a value a caller may
//! choose.
//!
//! Two of the values are not single strings but families: an agent role and a
//! self-review role are configuration, so they are declared as prefixes and the
//! remainder is the configured role name. The remainder is still bounded -- a
//! configured role cannot introduce arbitrary text into the label.

/// Actors whose whole value is fixed.
pub const ACTOR_LABELS: &[&str] = &["eval", "ralph", "sandbox-document-organizer", "self_review"];

/// Actors that carry a configured role name after the colon.
pub const ACTOR_LABEL_PREFIXES: &[&str] = &["agent:", "self_review:"];

/// Where a caller that declared no usable actor lands.
pub const UNKNOWN_ACTOR: &str = "unknown";

/// A byte a configured role name may use, so that a role taken from
/// configuration cannot widen the label vocabulary beyond these.
pub fn is_role_byte(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b':' | b'_' | b'-')
}

/// Whether `raw` is a value this platform may put on the actor label.
///
/// The role families are checked as `prefix + role` rather than as an
/// unbounded prefix match: `agent:` on its own names no actor, so the
/// remainder has to be there.
pub fn declared(raw: &str) -> bool {
    if raw == UNKNOWN_ACTOR || ACTOR_LABELS.contains(&raw) {
        return true;
    }
    ACTOR_LABEL_PREFIXES.iter().any(|prefix| {
        raw.strip_prefix(prefix)
            .is_some_and(|role| !role.is_empty() && role.bytes().all(is_role_byte))
    })
}

/// Why a request cannot be billed to anyone.
///
/// Two reasons, kept apart because they send the reader to two different
/// places: a missing header is a component that never stamped itself, while an
/// undeclared value is one that stamped the wrong word. Collapsing them into a
/// single "unknown" is what made the earlier bucket unreadable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unidentified {
    /// The request carried no `x-cogneva-actor`, or carried a blank one.
    Missing,
    /// The request named something the closed set does not hold.
    Undeclared,
}

impl Unidentified {
    /// The tag this refusal is counted under.
    pub fn as_str(self) -> &'static str {
        match self {
            Unidentified::Missing => "actor_missing",
            Unidentified::Undeclared => "actor_undeclared",
        }
    }

    /// The ledger result word this refusal is written under.
    ///
    /// The two reasons get two words rather than one because they name two
    /// different call sites to repair, and the ledger is the only durable
    /// surface an operator selecting by state can read: the distinction also
    /// exists in the warning line, but a reader that counts by outcome never
    /// sees a log line.
    pub fn result(self) -> cog_core::UsageResult {
        match self {
            Unidentified::Missing => cog_core::UsageResult::RefusedUnidentifiedMissing,
            Unidentified::Undeclared => cog_core::UsageResult::RefusedUnidentifiedUndeclared,
        }
    }

    /// What the caller is told, built from what it actually sent.
    ///
    /// A refusal that only says "bad identity" leaves the caller guessing which
    /// header to set and what values exist, so it names the header and gives
    /// the shape of a legal value. The value the caller sent is echoed back --
    /// bounded and stripped of control bytes -- because "you sent a typo" is
    /// actionable only next to the typo.
    pub fn message(self, raw: Option<&str>) -> String {
        let what_to_send = "请在请求头 `x-cogneva-actor` 上写清是谁在调用：\
                            `agent:<角色>` 或 `self_review:<角色>`（角色取自平台配置），\
                            或 eval / ralph / memory 这类固定名";
        match self {
            Unidentified::Missing => {
                format!("这次调用没有声明执行者身份，网关不替匿名调用付费。{what_to_send}。")
            }
            Unidentified::Undeclared => format!(
                "这次调用声明的执行者 `{}` 不在平台闭集里，网关不替匿名调用付费。{what_to_send}。",
                echo(raw.unwrap_or_default())
            ),
        }
    }
}

/// `raw` bounded and stripped of control bytes, for echoing back to its author.
fn echo(raw: &str) -> String {
    let kept: String = raw.chars().filter(|c| !c.is_control()).take(64).collect();
    if kept.len() < raw.len() {
        format!("{kept}…")
    } else {
        kept
    }
}

/// The identity a caller claims, or why this call cannot be billed.
///
/// Narrower than [`declared`], which answers "may this string sit in the
/// column". A caller may not claim [`UNKNOWN_ACTOR`]: that value exists so the
/// ledger can hold what history already wrote, not so a caller can opt out of
/// being named. The platform does not pay for anonymous work.
pub fn claimed(raw: Option<&str>) -> Result<&str, Unidentified> {
    match raw {
        None => Err(Unidentified::Missing),
        Some(value) if value.trim().is_empty() => Err(Unidentified::Missing),
        Some(value) if value != UNKNOWN_ACTOR && value.len() <= 32 && declared(value) => Ok(value),
        Some(_) => Err(Unidentified::Undeclared),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_declared_value_is_admitted() {
        for label in ACTOR_LABELS {
            assert!(declared(label), "{label} is declared but not admitted");
        }
        for prefix in ACTOR_LABEL_PREFIXES {
            let sample = format!("{prefix}generator");
            assert!(
                declared(&sample),
                "{sample} is a declared family but not admitted"
            );
        }
        assert!(declared(UNKNOWN_ACTOR));
    }

    #[test]
    fn a_claim_must_name_somebody_the_platform_holds() {
        assert_eq!(claimed(Some("self_review")), Ok("self_review"));
        assert_eq!(claimed(Some("agent:generator")), Ok("agent:generator"));
        assert_eq!(
            claimed(Some("self_review:evaluator")),
            Ok("self_review:evaluator")
        );
        // Nothing at all, and nothing that names nothing.
        assert_eq!(claimed(None), Err(Unidentified::Missing));
        assert_eq!(claimed(Some("")), Err(Unidentified::Missing));
        assert_eq!(claimed(Some("   ")), Err(Unidentified::Missing));
        // `unknown` is admitted as a *column value* so history stays readable,
        // but it is not an identity: letting a caller declare it would let any
        // component opt out of being named by typing the opt-out.
        assert!(declared(UNKNOWN_ACTOR));
        assert_eq!(claimed(Some(UNKNOWN_ACTOR)), Err(Unidentified::Undeclared));
        // Well shaped but never declared, and the shapes the label rule bans.
        for undeclared in ["planner", "Agent", "bad/actor", "agent:", "agent:Planner"] {
            assert_eq!(
                claimed(Some(undeclared)),
                Err(Unidentified::Undeclared),
                "{undeclared} was accepted as an identity"
            );
        }
        // The bound the column always had: a label is at most 32 bytes.
        let longest = format!("agent:{}", "a".repeat(32 - "agent:".len()));
        assert_eq!(claimed(Some(&longest)), Ok(longest.as_str()));
        let too_long = format!("agent:{}", "a".repeat(33 - "agent:".len()));
        assert_eq!(
            claimed(Some(&too_long)),
            Err(Unidentified::Undeclared),
            "a 33-byte label was accepted"
        );
    }

    #[test]
    fn a_refusal_names_the_header_and_echoes_what_arrived() {
        let missing = Unidentified::Missing.message(None);
        assert!(
            missing.contains(cog_core::LLM_ACTOR_HEADER),
            "a refusal that does not name the header leaves the caller guessing: {missing}"
        );
        let undeclared = Unidentified::Undeclared.message(Some("agent:planer"));
        assert!(
            undeclared.contains(cog_core::LLM_ACTOR_HEADER),
            "a refusal that does not name the header leaves the caller guessing: {undeclared}"
        );
        assert!(
            undeclared.contains("agent:planer"),
            "a refusal that does not echo the value cannot show the typo: {undeclared}"
        );
        // The echo is bounded and cannot carry line breaks into a log.
        let long = format!("agent:{}", "z".repeat(200));
        let echoed = Unidentified::Undeclared.message(Some(&long));
        assert!(
            echoed.contains('…'),
            "an over-long value is echoed without saying it was cut: {echoed}"
        );
        assert!(
            !echoed.contains(&long),
            "the refusal echoed a {}-byte value back verbatim",
            long.len()
        );
        let controls = Unidentified::Undeclared.message(Some("agent:a\nb"));
        assert!(!controls.contains('\n'));
    }

    #[test]
    fn a_value_nobody_declared_is_not_admitted() {
        // Well-shaped but not declared: this is the case the closing is for.
        for undeclared in ["planner", "agent", "reviewer", "web-chat", "wiki2"] {
            assert!(!declared(undeclared), "{undeclared} was admitted");
        }
        // Declared prefix with nothing after it names no actor.
        for empty_role in ["agent:", "self_review:"] {
            assert!(!declared(empty_role), "{empty_role} was admitted");
        }
        // A role may not smuggle bytes the label rule does not allow.
        for bad_role in ["agent:Planner", "agent:two words", "agent:planner!"] {
            assert!(!declared(bad_role), "{bad_role} was admitted");
        }
        assert!(!declared(""));
    }
}
