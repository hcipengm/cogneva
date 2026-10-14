//! The actors an LLM call may declare, as a closed set.
//!
//! Every call the platform makes to an upstream carries `x-cogneva-actor` so
//! that the call and token series can be read per actor. The set of values is
//! deliberately closed rather than merely shape-checked: the label is what the
//! cost and quota views group by, so a value accepted without being declared
//! here is a new series nobody decided to carry, and on the reading side it is
//! indistinguishable from a feature someone meant to add. The edge that admits
//! a value is the security gateway's `normalize_actor`, and it admits only what
//! this module declares; anything else lands on [`UNKNOWN_ACTOR`], which is the
//! cell to watch for a caller that did not declare itself at all.
//!
//! Two of the values are not single strings but families: an agent role and a
//! self-review role are configuration, so they are declared as prefixes and the
//! remainder is the configured role name. The remainder is still bounded -- a
//! configured role cannot introduce arbitrary text into the label.

/// Actors whose whole value is fixed.
pub const ACTOR_LABELS: &[&str] = &[
    "eval",
    "evolution",
    "intent_assess",
    "intent_gateway",
    "memory",
    "ralph",
    "sandbox-document-organizer",
    "self_review",
    "skill_extractor",
    "web_chat",
    "wiki",
];

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
