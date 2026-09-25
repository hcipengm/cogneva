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

#[path = "common/promql.rs"]
mod promql;

use promql::metric_names_in;

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
    // Agent run counters carry no labels: they are totals for the process, not
    // per-task readings, so nothing distinguishes one sample from the next.
    ("agent_success_count", &[]),
    ("agent_budget_exhausted_count", &[]),
    // The iteration ceiling is derived per role, so which role is being cut off
    // is the question; a single unlabelled total could not answer it.
    ("agent_iteration_budget_exhausted", &["role"]),
    // Pool recovery readings. Three separate series rather than one "recovers
    // at" number: which upstream said it, when we will probe again, and how long
    // the window an upstream stated is are answers of different strength, and
    // collapsing them is how a probe cadence gets read as a promise.
    ("llm_pool_evidenced_recovery_unix", &[]),
    ("llm_pool_next_attempt_unix", &[]),
    ("llm_pool_quota_window_secs", &[]),
    ("cogneva_verification_budget_seconds", &["kind"]),
    ("cogneva_verification_last_run_seconds", &["kind"]),
    ("cogneva_verification_timeouts_total", &["kind"]),
    ("llm_upstream_quota_window_secs", &["upstream"]),
    ("llm_upstream_quota_reset_unix", &["upstream"]),
    ("llm_upstream_consecutive_failures", &["upstream"]),
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

/// Whether an `expr` filters a datasource other than the metric store, i.e. a
/// log query. Those select by label and name no series, so running the PromQL
/// extractor over one invents names — a `$variable` reference reads as an
/// identifier. Only this dashboard holds such panels; the alert rules are all
/// metric queries.
fn is_log_query(expr: &str) -> bool {
    expr.trim_start().starts_with('{')
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
        // A log query selects by label, not by series name; the extractor would
        // read its `$variable` references as metrics.
        if expr.is_empty() || is_log_query(expr) {
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
            current = if is_log_query(expr) {
                None
            } else {
                metric_names_in(expr).into_iter().next()
            };
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

/// The `data:` entries of the dashboard ConfigMap as `(key, value)`.
///
/// Read line by line rather than through a YAML parser: the structure this
/// contract needs is one block scalar per key, and a parser would be a
/// dependency carried by every test in this crate for it.
fn configmap_data_blocks(text: &str) -> Vec<(String, String)> {
    let mut blocks = Vec::new();
    let mut lines = text.lines().peekable();
    let mut in_data = false;
    while let Some(line) = lines.next() {
        if line == "data:" {
            in_data = true;
            continue;
        }
        if !in_data {
            continue;
        }
        if !line.starts_with(' ') {
            in_data = false;
            continue;
        }
        let Some((key, _)) = line.trim().split_once(": |") else {
            continue;
        };
        let indent = line.len() - line.trim_start().len();
        let mut value = String::new();
        while let Some(next) = lines.peek() {
            if !next.trim().is_empty() && next.len() - next.trim_start().len() <= indent {
                break;
            }
            let body = lines.next().unwrap_or_default();
            // The block scalar holds whatever is indented past the key; cutting
            // the same amount off every line is what makes it parse as JSON.
            value.push_str(body.get(indent + 2..).unwrap_or(""));
            value.push('\n');
        }
        blocks.push((key.trim().to_string(), value));
    }
    blocks
}

/// A dashboard a ConfigMap ships has to be one Grafana will load.
///
/// Grafana's file provider reads each file as the panel model itself. The API
/// payload shape that wraps the model in `{"dashboard": ..., "overwrite": true}`
/// parses as JSON and reads as a model with an empty title, so Grafana drops
/// the dashboard and logs one line — every panel in it silently stops existing
/// while the manifest still looks like a dashboard to everyone reading the
/// repository. That is the failure this test exists for: the manifest is the
/// only artifact a reviewer sees, and it was wrong in exactly this way.
#[test]
fn every_dashboard_a_configmap_ships_is_one_grafana_can_load() {
    let text = dashboard_text();
    let blocks = configmap_data_blocks(&text);
    assert!(
        !blocks.is_empty(),
        "机读不到 data 里的面板 JSON，这份门禁会变成空转"
    );

    for (key, value) in blocks {
        let model: serde_json::Value = serde_json::from_str(&value)
            .unwrap_or_else(|e| panic!("{key} 不是合法 JSON，Grafana 也读不了: {e}"));
        assert!(
            model.get("dashboard").is_none() && model.get("overwrite").is_none(),
            "{key} 是 API 的 dashboard/overwrite 包装；\
             文件供给器要的是面板模型本身，包装会让它读到空标题并整张丢弃"
        );
        let title = model.get("title").and_then(|t| t.as_str()).unwrap_or("");
        assert!(!title.is_empty(), "{key} 没有 title，Grafana 会拒绝加载");
        let panels = model
            .get("panels")
            .and_then(|p| p.as_array())
            .unwrap_or_else(|| panic!("{key} 没有 panels 数组"));
        // Grafana 按 id 定位面板，两块面板共用一个 id 只会渲染出一块。
        let mut ids = BTreeSet::new();
        for panel in panels {
            let id = panel
                .get("id")
                .and_then(|i| i.as_i64())
                .unwrap_or_else(|| panic!("{key}: 面板没有 id: {panel}"));
            assert!(ids.insert(id), "{key}: 面板 id {id} 重复");
            let panel_title = panel.get("title").and_then(|t| t.as_str()).unwrap_or("");
            assert!(!panel_title.is_empty(), "{key}: 面板 {id} 没有标题");
            let queries = panel
                .get("targets")
                .and_then(|t| t.as_array())
                .unwrap_or_else(|| panic!("{key}: 面板 {id} 没有 targets，是块装饰"));
            assert!(!queries.is_empty(), "{key}: 面板 {id} 一条查询都没有");
        }
    }
}
