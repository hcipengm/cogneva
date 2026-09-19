//! Every PromQL expression in the deployed alert rules reads series by name.
//!
//! A rule whose expression names a series nothing produces never fires. It does
//! not error, it does not log, it does not show up anywhere except as an alert
//! that stays quiet through exactly the incident it was written for — the same
//! failure mode the dashboard contract covers, on the side that is supposed to
//! wake someone up. The two sides drift for the same reason: nothing about
//! editing one makes the other notice.
//!
//! The dashboard contract test covers panels. This one covers
//! `observability.infra_watch.rules` in the chart, which nothing checked before.
//!
//! The tables below are the contract. A series a rule reads must be either one
//! this workspace publishes (recorded with the file that publishes it, so a
//! deleted producer fails the test) or an explicitly owned foreign series.
//! Anything unclassified fails, because the failure mode here is silence, and
//! silence is what a forgotten classification looks like.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// Series this workspace publishes, with the source file that publishes them.
/// The file is read and searched for the name: an entry here whose producer was
/// renamed or removed fails the test rather than quietly licensing a rule that
/// can never fire.
const PRODUCED: &[(&str, &str)] = &[
    (
        "cogneva_data_volume_used_bytes",
        "crates/cog-observability/src/data_volume.rs",
    ),
    (
        "cogneva_stream_pending_claim_idle_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_failures_total",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_interval_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_measure_last_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_stream_pending_unreclaimed_oldest_idle_seconds",
        "crates/cog-orchestrator/src/observable.rs",
    ),
    (
        "cogneva_trace_tier_last_pass_seconds",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_overdue",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_pass_failures_total",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "cogneva_trace_tier_scan_interval_seconds",
        "crates/cog-observability/src/snapshot.rs",
    ),
    (
        "collab_classification_unreachable",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "ralph_terminations_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "self_evolution_change_yield_total",
        "crates/cog-collaboration/src/observable.rs",
    ),
    (
        "sandbox_oom_kills_total",
        "crates/cog-extension/src/runtime/cgroup.rs",
    ),
];

/// Series the rules read that this workspace does not publish, with the owner.
/// Listing them keeps the check honest: an unlisted foreign series fails the
/// test rather than being silently ignored.
const FOREIGN: &[(&str, &str)] = &[
    ("kube_node_status_condition", "kube-state-metrics"),
    (
        "kube_persistentvolumeclaim_resource_requests_storage_bytes",
        "kube-state-metrics",
    ),
    ("kube_pod_container_info", "kube-state-metrics"),
    (
        "kube_pod_container_status_last_terminated_reason",
        "kube-state-metrics",
    ),
    (
        "kube_pod_container_status_restarts_total",
        "kube-state-metrics",
    ),
    ("kube_pod_owner", "kube-state-metrics"),
    ("kube_pod_status_phase", "kube-state-metrics"),
    ("node_filesystem_avail_bytes", "node-exporter"),
    ("node_filesystem_size_bytes", "node-exporter"),
    ("up", "Prometheus 采集目标存活序列"),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every rule in the chart, name to promql, in file order.
fn chart_rules() -> Vec<(String, String)> {
    let path = repo_root().join(CHART_CONFIG);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("chart config unreadable at {}: {e}", path.display()));
    let value: serde_json::Value =
        serde_json::from_str(&text).expect("chart config is not valid JSON");
    value["observability"]["infra_watch"]["rules"]
        .as_array()
        .expect("observability.infra_watch.rules is not an array")
        .iter()
        .map(|rule| {
            (
                rule["name"].as_str().unwrap_or_default().to_string(),
                rule["promql"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

/// Drop `"..."` and `` `...` `` spans, keeping escapes inside a quoted span.
fn strip_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '"' || c == '`' {
            let quote = c;
            while let Some(c2) = chars.next() {
                if c2 == '\\' && quote == '"' {
                    chars.next();
                    continue;
                }
                if c2 == quote {
                    break;
                }
            }
            out.push(' ');
        } else {
            out.push(c);
        }
    }
    out
}

/// Blank out everything inside `{...}`, nested included. Label matchers hold
/// label names and values, neither of which is a series name.
fn strip_braces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0usize;
    for c in s.chars() {
        match c {
            '{' => {
                depth += 1;
                out.push(' ');
            }
            '}' => {
                depth = depth.saturating_sub(1);
                out.push(' ');
            }
            _ if depth > 0 => out.push(' '),
            _ => out.push(c),
        }
    }
    out
}

/// Blank out `kw(...)`, e.g. `by (stream, pod)`. The parenthesised list holds
/// label names, not series names.
fn strip_keyword_parens(s: &str, kw: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < bytes.len() {
        let boundary = i == 0 || {
            let prev = bytes[i - 1] as char;
            !prev.is_ascii_alphanumeric() && prev != '_'
        };
        if boundary && s[i..].starts_with(kw) {
            let after = i + kw.len();
            let trimmed = s[after..].trim_start();
            if trimmed.starts_with('(') {
                let open = after + (s[after..].len() - trimmed.len());
                let mut depth = 0usize;
                let mut j = open;
                while j < bytes.len() {
                    match bytes[j] as char {
                        '(' => depth += 1,
                        ')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    j += 1;
                }
                out.push(' ');
                i = if j < bytes.len() { j + 1 } else { bytes.len() };
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

/// Aggregation operators and keywords that look like identifiers but name no
/// series.
const NOT_SERIES: &[&str] = &[
    "and",
    "avg",
    "bool",
    "bottomk",
    "by",
    "count",
    "count_values",
    "end",
    "group",
    "group_left",
    "group_right",
    "ignoring",
    "infinity",
    "limit_ratio",
    "limitk",
    "max",
    "min",
    "nan",
    "offset",
    "on",
    "or",
    "quantile",
    "start",
    "stddev",
    "stdvar",
    "sum",
    "topk",
    "unless",
    "without",
];

/// Series names a PromQL expression reads.
///
/// The expression is stripped of the places identifiers are not series names
/// (quoted spans, `{...}` matchers, `by (...)`/`on (...)` lists), then every
/// remaining identifier is taken unless it is a function name (followed by
/// `(`), a keyword or aggregation, or the tail of a duration (`30m`, `[1h]`).
fn metric_names_in(expr: &str) -> BTreeSet<String> {
    let mut s = strip_braces(&strip_quoted(expr));
    for kw in [
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
    ] {
        s = strip_keyword_parens(&s, kw);
    }

    let bytes = s.as_bytes();
    let mut names = BTreeSet::new();
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i] as char;
        if c.is_ascii_alphabetic() || c == '_' || c == ':' {
            let start = i;
            while i < bytes.len() && {
                let ch = bytes[i] as char;
                ch.is_ascii_alphanumeric() || ch == '_' || ch == ':'
            } {
                i += 1;
            }
            let token = &s[start..i];
            let part_of_duration = start > 0 && (bytes[start - 1] as char).is_ascii_digit();
            let is_call = s[i..].trim_start().starts_with('(');
            if !part_of_duration && !is_call && !NOT_SERIES.contains(&token) {
                names.insert(token.to_string());
            }
            continue;
        }
        i += 1;
    }
    names
}

#[test]
fn every_series_an_alert_rule_reads_is_one_something_produces() {
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (rule, promql) in chart_rules() {
        for name in metric_names_in(&promql) {
            if produced.contains(name.as_str()) || foreign.contains(name.as_str()) {
                continue;
            }
            unknown.entry(name).or_default().push(rule.clone());
        }
    }

    assert!(
        unknown.is_empty(),
        "告警规则读了本仓库不产出、也没登记为外来序列的名字（这条规则永远不会触发）:\n{}",
        unknown
            .iter()
            .map(|(name, rules)| format!("  {name} <- {}", rules.join(", ")))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

#[test]
fn every_series_recorded_as_produced_is_still_published_there() {
    let root = repo_root();
    let mut missing: Vec<String> = Vec::new();
    for (name, source) in PRODUCED {
        let text = std::fs::read_to_string(root.join(source))
            .unwrap_or_else(|e| panic!("{source} unreadable: {e}"));
        if !text.contains(name) {
            missing.push(format!("{name} 记在 {source}，该文件里找不到这个名字"));
        }
    }

    assert!(
        missing.is_empty(),
        "PRODUCED 表登记的产出点已经不存在，登记本身成了空头许可:\n{}",
        missing.join("\n")
    );
}

#[test]
fn the_tables_hold_no_series_that_no_rule_reads() {
    let read: BTreeSet<String> = chart_rules()
        .iter()
        .flat_map(|(_, promql)| metric_names_in(promql))
        .collect();

    let stale: Vec<&str> = PRODUCED
        .iter()
        .map(|(n, _)| *n)
        .chain(FOREIGN.iter().map(|(n, _)| *n))
        .filter(|name| !read.contains(*name))
        .collect();

    assert!(
        stale.is_empty(),
        "表里登记的名字没有任何规则在读，说明规则被删或改名后表没跟着收敛: {stale:?}"
    );
}
