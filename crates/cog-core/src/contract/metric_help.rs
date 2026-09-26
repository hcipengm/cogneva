//! What each recorded series means, by kind.
//!
//! A name says what is measured only when it is self-explanatory, and most of
//! these are not: `metrics_samples_over_capacity` is a 1-or-0 reading whose
//! point is *when* it is 1, and a series that is absent on purpose reads as a
//! dead producer to anyone who has not been told. The sentence that fixes that
//! lives here, next to the closed set of names, because the same series is
//! served on more than one exposition: an exposition holding its own copy of the
//! wording would describe one series two ways, and a reader comparing the two
//! would have to guess which of them is the measurement.
//!
//! The rule for membership: something in this codebase records the name. A
//! series nothing produces is not an undocumented measurement, it is an absent
//! one, and describing it here would assert a reading that never arrives.
//!
//! The tables are not a list of what to serve. What a scrape carries is decided
//! by what the storage backend holds; this module only says what a name cannot
//! say about itself.

use crate::MetricType;

/// Descriptions for the counter series.
const COUNTER_HELP: &[(&str, &str)] = &[
    (
        "evolution_generated_change_hunks_total",
        "Hunks carried by generated change artifacts, summed over rounds; \
         the ratio against the faithful count in the same window is generation \
         fidelity, and the unfaithful remainder is what the apply gate throws away",
    ),
    (
        "evolution_generated_change_hunks_faithful",
        "Subset of the above whose context was found in the target tree",
    ),
    (
        "evolution_generated_change_files_total",
        "Files touched by generated change artifacts, summed over rounds; a \
         low faithful ratio here with a high hunk ratio means artifacts aimed \
         at the wrong revision, not artifacts written wrong",
    ),
    (
        "evolution_generated_change_files_faithful",
        "Subset of the above whose whole file patch applied",
    ),
    (
        "memory_operations_total",
        "Total number of memory backend operations",
    ),
    (
        "memory_operation_errors_total",
        "Total number of failed memory backend operations",
    ),
    (
        crate::metric_names::HTTP_REQUESTS_TOTAL.as_str(),
        "Total number of HTTP requests",
    ),
    (
        "tier_migration_total",
        "Total number of storage tier migrations",
    ),
    (
        "llm_calls_total",
        "Total number of LLM upstream calls by result",
    ),
    (
        "llm_tokens_total",
        "Total LLM tokens consumed, split by input and output",
    ),
    (
        "llm_upstream_client_errors_total",
        "Total LLM upstream calls rejected as malformed requests",
    ),
    (
        "llm_upstream_failures_total",
        "Total LLM upstream failures, excluding rate limits",
    ),
];

/// Descriptions for the histogram series. See [`COUNTER_HELP`].
const HISTOGRAM_HELP: &[(&str, &str)] = &[
    (
        "memory_operation_latency_ms",
        "Memory backend operation latency in milliseconds",
    ),
    (
        crate::metric_names::HTTP_REQUEST_DURATION_MS.as_str(),
        "HTTP request duration in milliseconds",
    ),
];

/// Descriptions for the gauge series. See [`COUNTER_HELP`].
const GAUGE_HELP: &[(&str, &str)] = &[
    (
        "memory_unextracted_raw",
        "Archived raw sources still missing a summary, as last scanned",
    ),
    (
        "memory_unextracted_raw_aged_out",
        "Subset of the above that aged past the re-drive window; the system \
         will not pick these up again without a budgeted backfill",
    ),
    (
        "metrics_samples_rows",
        "Rows currently held in the metrics sample log",
    ),
    (
        "metrics_samples_budget_rows",
        "Rows the metrics sample log is allowed to hold; the sweep deletes \
         oldest-first past this, stopping at each gauge series' newest row",
    ),
    (
        "metrics_samples_over_capacity",
        "1 when the sample log is over its row budget and cannot be pruned \
         further without deleting a gauge series' current value, 0 otherwise",
    ),
    (
        "metrics_samples_bytes",
        "On-disk bytes the metrics sample log occupies, including indexes. \
         Lags the row count, since PostgreSQL frees deleted space only when it \
         vacuums, so it is a reading and never the pruning criterion",
    ),
    (
        "metrics_retired_rows_removed",
        "Rows of retired metric names the last release pass deleted, labelled \
         by the table they came from. Reported every pass, so a table whose \
         reading stays non-zero is one the release is not draining",
    ),
    (
        "llm_upstream_healthy",
        "Whether each LLM upstream has no outstanding failure, 1 or 0. \
         Absent for an upstream this process has never sent a call to: \
         'no record' is the shape a recovered upstream has too, so it is \
         reported as no reading rather than as health. A backoff window \
         expiring is not evidence of recovery either, so only a call that \
         actually succeeded clears it — the same rule the pool verdict uses",
    ),
    (
        "llm_pool_available",
        "Whether any LLM upstream is usable, 1 or 0",
    ),
    (
        "llm_pool_evidenced_recovery_unix",
        "Recovery instant an upstream itself reported, as a Unix timestamp; \
         0 when no upstream has given one",
    ),
    (
        "llm_pool_next_attempt_unix",
        "When the next pool probe is due, as a Unix timestamp",
    ),
];

/// The description registered for one series, if it has one.
///
/// `None` means somebody recorded a name and never documented it. Callers
/// serving it should say so rather than drop the series.
pub fn metric_description(kind: MetricType, name: &str) -> Option<&'static str> {
    let table = match kind {
        MetricType::Counter => COUNTER_HELP,
        MetricType::Histogram => HISTOGRAM_HELP,
        MetricType::Gauge => GAUGE_HELP,
    };
    table
        .iter()
        .find(|(known, _)| *known == name)
        .map(|(_, text)| *text)
}

/// Every name described for one kind, in table order.
///
/// For readers that have to fall back to *something* when the storage backend
/// cannot enumerate its names: the described set is what such an exposition
/// served before enumeration existed, so the fallback degrades to the older
/// behaviour rather than to an empty body.
pub fn documented_metric_names(kind: MetricType) -> impl Iterator<Item = &'static str> {
    let table = match kind {
        MetricType::Counter => COUNTER_HELP,
        MetricType::Histogram => HISTOGRAM_HELP,
        MetricType::Gauge => GAUGE_HELP,
    };
    table.iter().map(|(name, _)| *name)
}

/// The description for one series, or a placeholder naming the omission.
///
/// Serving a series with a placeholder is better than the two alternatives:
/// dropping it loses the measurement, and refusing to serve it turns a missing
/// sentence into a missing measurement. The placeholder names the fix so the
/// omission is actionable rather than merely visible.
pub fn metric_help_text(kind: MetricType, name: &str) -> String {
    match metric_description(kind, name) {
        Some(text) => text.to_string(),
        None => format!("Undocumented metric {name}: no description is registered for it"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A name is described once, for one kind. Two entries for the same name
    /// would be two answers to one question, and the lookup returning the first
    /// would hide the second from every reader of the exposition.
    #[test]
    fn no_name_is_described_twice() {
        let mut seen: Vec<&str> = Vec::new();
        for (kind, table) in [
            (MetricType::Counter, COUNTER_HELP),
            (MetricType::Histogram, HISTOGRAM_HELP),
            (MetricType::Gauge, GAUGE_HELP),
        ] {
            for (name, text) in table {
                assert!(!text.trim().is_empty(), "{kind:?} {name} 描述为空");
                assert!(
                    !seen.contains(name),
                    "{name} 被描述了两次（第二次在 {kind:?}）"
                );
                seen.push(name);
            }
        }
    }

    /// The lookup goes through the kind, so a name described as a gauge does
    /// not answer for a counter of the same name.
    #[test]
    fn a_description_belongs_to_its_kind() {
        assert!(metric_description(MetricType::Gauge, "llm_upstream_healthy").is_some());
        assert!(metric_description(MetricType::Counter, "llm_upstream_healthy").is_none());
        assert!(
            metric_help_text(MetricType::Counter, "llm_upstream_healthy")
                .starts_with("Undocumented metric llm_upstream_healthy")
        );
    }
}
