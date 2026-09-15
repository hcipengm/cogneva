use std::collections::HashMap;

use cog_core::MetricSample;

/// Render a set of counter samples in Prometheus text format.
/// Samples are aggregated by their label set and summed.
pub fn render_counters(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} counter\n");

    // Aggregate by label set
    let mut aggregated: HashMap<String, f64> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        *aggregated.entry(key).or_insert(0.0) += s.value;
    }

    for (labels, value) in aggregated {
        out.push_str(&format!("{name}{{{labels}}} {value}\n"));
    }

    out
}

/// Render a set of histogram samples in Prometheus text format as a summary.
/// Produces `_count` and `_sum` series per label set.
pub fn render_histograms(name: &str, help: &str, samples: &[MetricSample]) -> String {
    if samples.is_empty() {
        return String::new();
    }

    let mut out = format!("# HELP {name} {help}\n# TYPE {name} summary\n");

    // Aggregate by label set: count and sum
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut sums: HashMap<String, f64> = HashMap::new();
    for s in samples {
        let key = format_labels(&s.labels);
        *counts.entry(key.clone()).or_insert(0) += 1;
        *sums.entry(key).or_insert(0.0) += s.value;
    }

    for (labels, count) in counts {
        let sum = sums.get(&labels).copied().unwrap_or(0.0);
        out.push_str(&format!("{name}_count{{{labels}}} {count}\n"));
        out.push_str(&format!("{name}_sum{{{labels}}} {sum}\n"));
    }

    out
}

/// Render raw observable metrics (D5/D8 pulls) in Prometheus text format.
/// Metrics are grouped by name; names ending in `_total` render as counters,
/// everything else as gauges. One sample per distinct label set.
pub fn render_raw_metrics(metrics: &[cog_core::RawMetric]) -> String {
    if metrics.is_empty() {
        return String::new();
    }

    let mut by_name: HashMap<&str, Vec<&cog_core::RawMetric>> = HashMap::new();
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

fn format_labels(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", escape_label_value(v)))
        .collect();
    pairs.join(",")
}

fn escape_label_value(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_raw_metrics_groups_by_name_and_labels() {
        let metrics = vec![
            cog_core::RawMetric::new("ralph_terminations_total", 2.0)
                .with_label("reason", "stagnated"),
            cog_core::RawMetric::new("collaboration_success_rate", 0.5),
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
