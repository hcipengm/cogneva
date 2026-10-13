//! The environment a call is made from, stamped on every outbound request.
//!
//! Token spend is read along two axes that look alike and are not: the calling
//! component, and the environment it ran in. The same component (a generator,
//! a reviewer, the chat path) runs in more than one environment, so "who spent
//! this" cannot answer "where was it spent", and the answer has to be declared
//! by the caller rather than derived from its name — a table mapping component
//! names to environments goes stale the moment a deployment moves a component.
//!
//! The environment is a property of the process, not of the call, so this is
//! where it is stated: once, for every call the process makes. A caller that
//! belongs to a different environment than the process hosting it says so
//! itself, through [`cog_core::ChatOptions::with_origin`], and that wins.

use crate::ChatOptions;
use cog_core::LLM_ORIGIN_HEADER;
use std::collections::HashMap;
use std::sync::OnceLock;

/// Environment variable a deployment sets to declare which environment the
/// process is.
///
/// It is a deployment's business rather than a config file's: the same binary
/// runs as the control plane, as the evolution executor and as a sandbox
/// executor, and which one it is is decided by where it was deployed, not by
/// the image. Setting it is what makes a process's spend attributable; leaving
/// it unset is a supported state and reads as "nothing was declared".
pub const LLM_ORIGIN_ENV: &str = "COGNEVA_LLM_ORIGIN";

/// The origin this process declared, read once and kept.
///
/// Once, because it cannot change while the process runs, and because a
/// per-call read would let the answer change mid-flight — spend from one
/// process would land in two environments with nothing on the wire to say why.
/// A blank or whitespace-only value counts as undeclared: a variable set to
/// nothing carries no more information than an unset one, and taking the
/// blank as a name would put an empty environment on the ledger.
fn declared() -> Option<&'static str> {
    static DECLARED: OnceLock<Option<String>> = OnceLock::new();
    DECLARED
        .get_or_init(|| normalize(std::env::var(LLM_ORIGIN_ENV).ok().as_deref()))
        .as_deref()
}

/// The name a raw setting amounts to, or `None` when it names nothing.
///
/// Separated from the read so the rule can be judged without a process-global
/// environment: [`declared`] caches, and a test that set the variable would
/// answer differently depending on which test ran first.
fn normalize(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|raw| !raw.is_empty())
        .map(str::to_string)
}

/// The headers one call goes out with: the caller's own, plus this process's
/// declared origin when the caller named none itself.
///
/// This is the one place a provider's outbound headers are assembled, so a
/// process's declaration reaches every call it makes without each caller
/// restating it.
pub fn outbound_headers(options: &ChatOptions) -> HashMap<String, String> {
    headers_with_declared(options, declared())
}

/// [`outbound_headers`] with the process's declaration supplied instead of read
/// from the environment, so the rule can be judged without a process-global.
pub fn headers_with_declared(
    options: &ChatOptions,
    declared: Option<&str>,
) -> HashMap<String, String> {
    let mut headers = options.headers.clone();
    let caller_named_one = headers
        .get(LLM_ORIGIN_HEADER)
        .is_some_and(|named| !named.trim().is_empty());
    // Undeclared stays undeclared. There is no default to fall back to on
    // purpose: the reader counts by this axis, so a process that did not say
    // would have its spend credited to an environment it may not have run in.
    // An absent header reads as "not attributable", which is true, and a wrong
    // answer here is worse than a missing one.
    if !caller_named_one {
        if let Some(origin) = declared {
            headers.insert(LLM_ORIGIN_HEADER.to_string(), origin.to_string());
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{LLM_ACTOR_HEADER, LLM_ORIGIN_HEADER};

    fn origin_of(headers: &HashMap<String, String>) -> Option<&str> {
        headers.get(LLM_ORIGIN_HEADER).map(String::as_str)
    }

    /// A process that declared nothing sends nothing: the ledger must read
    /// "not attributable" rather than a default environment.
    #[test]
    fn an_undeclared_process_names_no_environment() {
        let headers = headers_with_declared(&ChatOptions::default(), None);
        assert_eq!(origin_of(&headers), None);
    }

    /// The declaration reaches a call the caller said nothing about.
    #[test]
    fn a_declared_process_names_its_environment_on_every_call() {
        let options = ChatOptions::default().with_actor("agent:generator");
        let headers = headers_with_declared(&options, Some("gateway_pool"));
        assert_eq!(origin_of(&headers), Some("gateway_pool"));
        // The caller's own actor header is carried through untouched: the two
        // axes are independent, and stamping one must not drop the other.
        assert_eq!(
            headers.get(LLM_ACTOR_HEADER).map(String::as_str),
            Some("agent:generator")
        );
    }

    /// The caller's own declaration wins over the process's — that is what
    /// makes a control plane answering a human land in a different environment
    /// than the pool it drives.
    #[test]
    fn a_callers_own_declaration_wins() {
        let options = ChatOptions::default().with_origin("interactive");
        let headers = headers_with_declared(&options, Some("gateway_pool"));
        assert_eq!(origin_of(&headers), Some("interactive"));
    }

    /// A blank header is a caller that named nothing, not a caller that named
    /// the empty environment; the process's declaration still applies.
    #[test]
    fn a_blank_declaration_is_no_declaration() {
        let options = ChatOptions::default().with_origin("   ");
        let headers = headers_with_declared(&options, Some("sandbox"));
        assert_eq!(origin_of(&headers), Some("sandbox"));
    }

    /// A blank setting is an unset one, and surrounding space is not part of a
    /// name: both are read from a deployment, where a stray newline is the
    /// likeliest way an environment lands on the ledger misspelled.
    #[test]
    fn a_blank_setting_names_nothing_and_a_padded_one_is_trimmed() {
        assert_eq!(normalize(None), None);
        assert_eq!(normalize(Some("")), None);
        assert_eq!(normalize(Some("  \t\n")), None);
        assert_eq!(normalize(Some(" sandbox ")), Some("sandbox".into()));
    }
}
