//! The Grafana dashboard reads series and labels by name. Nothing else does.
//!
//! A panel whose series nothing produces, or whose legend names a label the
//! series does not carry, renders as an empty panel or a legend of blanks —
//! and an empty panel reads exactly like "the system is idle". Nothing about
//! editing either side makes the other notice, so the two only drift, in one
//! direction: the dashboard keeps promising readings the producer stopped
//! supplying. This test is the thing that notices.
//!
//! The table below states the contract for the series this workspace produces.
//! Series from outside it (cAdvisor's `container_*`) are not covered: their
//! label sets belong to the kubelet, not to this repository.

use std::collections::BTreeSet;
use std::path::PathBuf;

const DASHBOARD: &str = "deploy/k3s/observability/manifests/06-grafana-dashboard-configmap.yaml";

/// Series this workspace produces, with the labels each one carries.
/// Recorded at the call sites: `http_requests_total` / `http_request_duration_ms`
/// share one label map, as do `llm_calls_total` / `llm_call_latency_ms`.
const PRODUCED: &[(&str, &[&str])] = &[
    ("http_requests_total", &["method", "endpoint", "status"]),
    (
        "http_request_duration_ms",
        &["method", "endpoint", "status"],
    ),
    ("llm_calls_total", &["upstream", "model", "result", "actor"]),
    (
        "llm_call_latency_ms",
        &["upstream", "model", "result", "actor"],
    ),
];

/// Series the dashboard may read that this workspace does not produce, with the
/// owner. Listing them keeps the coverage check honest: an unlisted foreign
/// series fails the test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    ("container_cpu_usage_seconds_total", "kubelet/cAdvisor 采集"),
    (
        "container_memory_working_set_bytes",
        "kubelet/cAdvisor 采集",
    ),
    ("ALERTS", "Prometheus 由告警规则合成的序列"),
];

fn dashboard_text() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(DASHBOARD);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("dashboard manifest unreadable at {}: {e}", path.display()))
}

/// Metric names a PromQL expression reads. A name is taken as the token
/// immediately before `{`, `[`, or whitespace in a position that is not a
/// label value or a function name — close enough for the handful of flat
/// expressions this dashboard holds, and it errs toward reporting a name
/// rather than skipping one.
fn metric_names_in(expr: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < bytes.len()
                && ((bytes[i] as char).is_ascii_alphanumeric() || bytes[i] == b'_')
            {
                i += 1;
            }
            let token = &expr[start..i];
            // A name is the token that a `{`, `[`, or the end of the expression
            // follows. That excludes function names (`rate`, `sum`,
            // `histogram_quantile`), label names inside braces, and comparison
            // keywords — all of which are followed by `(` or `=` instead.
            let next = expr[i..].chars().next();
            if matches!(next, Some('{') | Some('[') | None) {
                names.insert(token.to_string());
            }
            continue;
        }
        i += 1;
    }
    names
}

/// Label names a legend template asks for: every `{{...}}` in the format.
fn legend_labels_in(legend: &str) -> BTreeSet<String> {
    legend
        .match_indices("{{")
        .filter_map(|(start, _)| {
            let rest = &legend[start + 2..];
            rest.find("}}").map(|end| rest[..end].trim().to_string())
        })
        .collect()
}

#[test]
fn every_series_the_dashboard_reads_is_one_something_produces() {
    let text = dashboard_text();
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeSet<String> = BTreeSet::new();
    for line in text.lines() {
        let trimmed = line.trim();
        let Some(rest) = trimmed.strip_prefix("\"expr\":") else {
            continue;
        };
        let expr = rest.trim().trim_matches(|c| c == '"' || c == ',').trim();
        if expr.is_empty() {
            continue;
        }
        for name in metric_names_in(expr) {
            if produced.contains(name.as_str()) || foreign.contains(name.as_str()) {
                continue;
            }
            // A classic histogram is exposed as three series derived from the
            // one name the producer records: `_bucket`, `_sum`, `_count`. Read
            // the base as the same series rather than asking the dashboard to
            // pretend otherwise. This over-accepts a `_sum` whose base is a
            // counter, which no panel in this dashboard does.
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|suffix| name.strip_suffix(suffix));
            if base.is_some_and(|b| produced.contains(b)) {
                continue;
            }
            unknown.insert(name);
        }
    }

    assert!(
        unknown.is_empty(),
        "面板读了本仓库不产出、也没登记为外来序列的名字: {unknown:?}\n\
         要么把它接上产出面，要么把它记进 FOREIGN 并写明属主"
    );
}

#[test]
fn every_label_a_legend_names_is_on_the_series_it_describes() {
    let text = dashboard_text();
    let labels_of = |name: &str| -> Option<&'static [&'static str]> {
        PRODUCED
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, labels)| *labels)
    };

    // Panels pair one expr with one legend within the same object; walking the
    // file in order and keeping the most recent expr is enough for a manifest
    // that holds one target per panel.
    let mut current: Option<String> = None;
    let mut mismatches: Vec<String> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("\"expr\":") {
            let expr = rest.trim().trim_matches(|c| c == '"' || c == ',').trim();
            current = metric_names_in(expr).into_iter().next();
        } else if let Some(rest) = trimmed.strip_prefix("\"legendFormat\":") {
            let legend = rest.trim().trim_matches(|c| c == '"' || c == ',').trim();
            let (Some(metric), Some(expected)) = (
                current.as_ref(),
                labels_of(current.as_ref().unwrap().as_str()),
            ) else {
                continue;
            };
            for label in legend_labels_in(legend) {
                if !expected.contains(&label.as_str()) {
                    mismatches.push(format!(
                        "{metric} 的 legend 引用了它没有的标签 {{{{{label}}}}}; 实际标签: {expected:?}"
                    ));
                }
            }
        }
    }

    assert!(
        mismatches.is_empty(),
        "面板图例引用了序列上不存在的标签，图例会渲染成空白:\n{}",
        mismatches.join("\n")
    );
}
