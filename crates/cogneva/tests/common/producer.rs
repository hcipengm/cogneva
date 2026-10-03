//! Whether a source file really carries a series' producer.
//!
//! Both observability contract tests record, against every series they promise,
//! the file that publishes it, and both have to ask the same question about that
//! file. A producer names its series either by the quoted literal or through the
//! metric registry; those are the same claim, and both are what a rename moves.
//!
//! The quoted form is what makes the match worth anything: a name in prose — a
//! module doc, a help string — reads exactly like a producer if the name alone
//! is searched for, so renaming the constant and leaving the doc behind (or the
//! reverse) would leave every rule and panel reading a series nothing publishes
//! while this check stayed green. Only the literal is a recording site's form.
//!
//! That is the ceiling of what a text match can decide, and worth being explicit
//! about: whether a value is recorded at the line naming the series is not
//! readable from the text, so a file that quotes the name without recording it
//! satisfies this check. What it does decide is the failure that happens in
//! practice — the producer renamed or removed while the rule or the panel that
//! reads the series stayed behind, which is a signal that can never arrive and
//! reads as a metric that went quiet.

use cog_core::metric_names::IDENTIFIED;

/// Whether `text` names the series *and* the site in it produces that series.
///
/// The registry path is checked as the identifier, not the name: a file that
/// merely passes the name around is not publishing this series, and accepting it
/// would turn the entry back into the empty licence the check exists to refuse.
/// The identifier is accepted bare as well as `metric_names::`-qualified,
/// because a file that imports the constant names a real recording site just as
/// directly -- the qualification says where the constant came from, not whether
/// this file records the series -- and refusing the imported form would push a
/// producer site onto a file that does not publish it. The match is bounded on
/// both sides so a longer name that merely contains this identifier is not read
/// as it.
pub fn carries_the_producer(text: &str, name: &str) -> bool {
    text.contains(&format!("\"{name}\""))
        || IDENTIFIED.iter().any(|(ident, metric)| {
            metric.as_str() == name
                && (text.contains(&format!("metric_names::{ident}"))
                    || mentions_identifier(text, ident))
        })
}

/// Whether `ident` appears in `text` as a whole identifier.
///
/// Bounded on both sides: `VERSION_CONTRACT_VIOLATIONS_TOTAL` and
/// `MY_VERSION_CONTRACT_VIOLATIONS` both contain the name and neither is it.
pub fn mentions_identifier(text: &str, ident: &str) -> bool {
    let bytes = text.as_bytes();
    let mut from = 0;
    while let Some(at) = text[from..].find(ident) {
        let start = from + at;
        let end = start + ident.len();
        let before_ok = start == 0 || !is_word_byte(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_word_byte(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

fn is_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}
