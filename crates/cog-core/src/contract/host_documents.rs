//! The one setting the host-document capability has to agree on across
//! processes.
//!
//! The capability has two halves in two processes. The gateway holds the only
//! mouth that can both carry a body off the cluster and refuse it, so the
//! egress switch is judged there. The executor is where a document body first
//! enters a caller, so the read that feeds that mouth is gated there. Both
//! answer "may this body leave the cluster?" — which means both read the same
//! switch, and a second definition of it is a defect that only shows in the
//! default configuration: one side open and the other refusing, or the reverse,
//! and each side's own reading says the capability is off.
//!
//! So the name and the reading are here, once. What each side *does* with the
//! answer stays with that side.

/// Whether document bodies may leave the cluster. Absent means no.
pub const BODY_EGRESS_ENV: &str = "HOST_DOCS_BODY_EGRESS_ENABLED";

/// Whether the switch is on.
///
/// Only an explicit truthy spelling opens it. Absent, empty, misspelled and
/// `0` all mean off, and that direction is deliberate: the switch decides
/// whether host document text may leave the cluster, so a value nobody can
/// parse has to fail towards keeping it in. A reader that treated "anything
/// but false" as on would open the channel on a typo.
pub fn switch_enabled(raw: Option<&str>) -> bool {
    matches!(
        raw.map(str::trim).map(str::to_ascii_lowercase).as_deref(),
        Some("true") | Some("yes") | Some("on") | Some("1")
    )
}

/// The switch as the process environment spells it.
pub fn switch_enabled_env() -> bool {
    switch_enabled(std::env::var(BODY_EGRESS_ENV).ok().as_deref())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The direction that matters: nothing but an explicit truthy spelling may
    /// open a switch that governs whether document text leaves the cluster.
    /// Both the spellings and the near-misses are pinned here, next to the
    /// definition, so the two sides cannot drift apart on either half.
    #[test]
    fn only_an_explicit_truthy_value_opens_the_switch() {
        for raw in [
            Some("true"),
            Some("TRUE"),
            Some("True"),
            Some(" true "),
            Some("yes"),
            Some("on"),
            Some(" on "),
            Some("1"),
        ] {
            assert!(switch_enabled(raw), "{raw:?} must open the switch");
        }
        for raw in [
            None,
            Some(""),
            Some("  "),
            Some("false"),
            Some("0"),
            Some("no"),
            Some("off"),
            Some("enabled"),
            Some("y"),
            Some("TRUEISH"),
            Some("ollkorrect"),
            Some("2"),
        ] {
            assert!(!switch_enabled(raw), "{raw:?} must not open the switch");
        }
    }
}
