//! Prometheus text rendering for the readings a process publishes itself.
//!
//! More than one process exposes a `/metrics` endpoint and appends its own
//! observables to what its registry exported — the gateway's server, the
//! security gateway, the sandbox executor — and each of them is rendering the
//! same family of readings. A renderer per process would be a spelling per
//! process: the same series could arrive as a counter from one and a gauge from
//! another, and Prometheus takes the type from whichever scrape it saw, so the
//! two spellings would make `rate()` work or fail depending on scrape order.
//! The convention is therefore stated once, here, beside the contract that
//! defines what an observable reading is.
//!
//! The convention has two halves. A series whose name ends in `_total` is
//! cumulative and renders as a counter; everything else is a point-in-time
//! value and renders as a gauge. And a label set is ordered by label *name*,
//! never by the map's iteration order: the same logical series arrives as a
//! fresh map per sample, so an iteration-ordered rendering splits one series into
//! as many series as there are label permutations, each carrying a fraction of
//! the total, and every per-series aggregate downstream is then wrong.
//!
//! One name in that convention is derived rather than recorded: alongside every
//! series a store renders it also renders [`OBSERVED_TIMESTAMP_SUFFIX`]'s
//! companion, carrying when the newest thing behind that series happened. It is
//! not a name any producer writes, so it is not in the registry of writable
//! names — but it *is* published by the build, which is why the spelling lives
//! here beside the other two rather than being re-typed by each renderer and
//! each reader. A reader that spells it itself keeps matching after the renderer
//! changes the suffix, and reports a producer for a series nothing renders.

use std::collections::HashMap;

use crate::RawMetric;

/// The suffix a store appends to every series it renders, to carry the time of
/// the newest observation behind that series.
///
/// It is what separates an abandoned series from a quiet one: a value that has
/// stopped moving is the same shape whether its producer went away or is running
/// and has nothing to count. Declared in the shared contract layer because the
/// process that renders the companion, the rules that may read one, and the
/// checks that judge whether a name has a producer here all have to agree on the
/// spelling; a re-typed literal is how a reader ends up accepting a name the
/// store never renders.
pub const OBSERVED_TIMESTAMP_SUFFIX: &str = "_observed_timestamp_seconds";

/// The companion series name for `base`.
pub fn observed_timestamp_name(base: &str) -> String {
    format!("{base}{OBSERVED_TIMESTAMP_SUFFIX}")
}

/// The series `name` is the companion of, or `None` when `name` is not one.
///
/// Only the spelling is decided here. Whether the base names a series this build
/// publishes is a different question, answered against the registry.
pub fn observed_timestamp_base(name: &str) -> Option<&str> {
    name.strip_suffix(OBSERVED_TIMESTAMP_SUFFIX)
        .filter(|base| !base.is_empty())
}

/// Render raw observable metrics in Prometheus text format.
///
/// Metrics are grouped by name; names ending in `_total` render as counters,
/// everything else as gauges. One sample per distinct label set.
///
/// An empty input renders nothing rather than a `# HELP`/`# TYPE` header on its
/// own: a header with no series under it reads downstream as a metric that is
/// present and idle, which is the opposite of "this process has nothing to
/// report under that name".
pub fn render_raw_metrics(metrics: &[RawMetric]) -> String {
    if metrics.is_empty() {
        return String::new();
    }

    let mut by_name: HashMap<&str, Vec<&RawMetric>> = HashMap::new();
    for m in metrics {
        by_name.entry(m.name.as_str()).or_default().push(m);
    }

    let mut names: Vec<&str> = by_name.keys().copied().collect();
    names.sort_unstable();

    let mut out = String::new();
    for name in names {
        let kind = if name.ends_with("_total") {
            "counter"
        } else {
            "gauge"
        };
        out.push_str(&format!("# TYPE {name} {kind}\n"));
        for m in &by_name[name] {
            let labels = format_labels(&m.labels);
            if labels.is_empty() {
                out.push_str(&format!("{name} {}\n", m.value));
            } else {
                out.push_str(&format!("{name}{{{labels}}} {}\n", m.value));
            }
        }
    }

    out
}

/// Render a label set in a canonical form, ordered by label name.
///
/// The order must come from the names rather than from the map's iteration
/// order: the same logical series arrives as a fresh `HashMap` per sample, and an
/// iteration-ordered rendering fragments it into one series per label
/// permutation.
pub fn format_labels(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let mut names: Vec<&String> = labels.keys().collect();
    names.sort_unstable();
    let pairs: Vec<String> = names
        .into_iter()
        .map(|k| format!("{k}=\"{}\"", escape_label_value(&labels[k])))
        .collect();
    pairs.join(",")
}

/// Escape a label value the way the text exposition format requires. A raw
/// quote or backslash inside a value ends the label early and turns the rest of
/// it into syntax, so the series is either rejected or silently different from
/// the one the producer meant to report.
fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels_render_in_canonical_name_order() {
        let labels: HashMap<String, String> = [
            ("status", "200"),
            ("endpoint", "/metrics"),
            ("method", "GET"),
        ]
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(
            format_labels(&labels),
            "endpoint=\"/metrics\",method=\"GET\",status=\"200\""
        );
    }

    #[test]
    fn render_raw_metrics_groups_by_name_and_labels() {
        let metrics = vec![
            RawMetric::new("ralph_terminations_total", 2.0).with_label("reason", "stagnated"),
            RawMetric::new("collaboration_success_rate", 0.5),
        ];
        let out = render_raw_metrics(&metrics);
        assert!(out.contains("# TYPE ralph_terminations_total counter\n"));
        assert!(out.contains("ralph_terminations_total{reason=\"stagnated\"} 2\n"));
        assert!(out.contains("# TYPE collaboration_success_rate gauge\n"));
        assert!(out.contains("collaboration_success_rate 0.5\n"));
    }

    #[test]
    fn render_raw_metrics_empty_is_empty() {
        assert!(render_raw_metrics(&[]).is_empty());
    }

    #[test]
    fn the_companion_suffix_is_the_one_the_reader_strips() {
        assert_eq!(
            observed_timestamp_name("cogneva_rollout_job_reading_unix"),
            "cogneva_rollout_job_reading_unix_observed_timestamp_seconds"
        );
        assert_eq!(
            observed_timestamp_base("cogneva_rollout_job_reading_unix_observed_timestamp_seconds"),
            Some("cogneva_rollout_job_reading_unix")
        );
    }

    /// The suffix alone is not a base: a bare companion name has no series
    /// behind it, and reading the empty string as one would let a rule name a
    /// series nothing renders.
    #[test]
    fn a_name_that_is_not_a_companion_has_no_base() {
        assert_eq!(
            observed_timestamp_base("cogneva_rollout_job_reading_unix"),
            None
        );
        assert_eq!(observed_timestamp_base(OBSERVED_TIMESTAMP_SUFFIX), None);
        assert_eq!(observed_timestamp_base(""), None);
    }
}
