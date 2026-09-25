//! Whether a source file really carries a series' producer.
//!
//! Both observability contract tests record, against every series they promise,
//! the file that publishes it, and both have to ask the same question about that
//! file. A producer names its series either by the literal or through the metric
//! registry; those are the same claim, and both are what a rename moves. A file
//! that mentions the name any other way — a help string, a list of the names the
//! exposition carries, a comment — still counts here.
//!
//! That is the ceiling of what a text match can decide, and worth being explicit
//! about: whether a value is recorded at the line naming the series is not
//! readable from the text, so a file that lists the name without recording it
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
pub fn carries_the_producer(text: &str, name: &str) -> bool {
    text.contains(name)
        || IDENTIFIED.iter().any(|(ident, metric)| {
            metric.as_str() == name && text.contains(&format!("metric_names::{ident}"))
        })
}
