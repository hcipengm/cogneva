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

use std::collections::HashMap;

use crate::RawMetric;

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
}
