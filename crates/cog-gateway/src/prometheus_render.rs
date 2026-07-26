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
