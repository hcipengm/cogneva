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

#[path = "common/producer.rs"]
mod producer;
#[path = "common/promql.rs"]
mod promql;

use producer::carries_the_producer;
use promql::{metric_names_in, shape_complaints};

const DASHBOARD: &str = "deploy/k3s/observability/manifests/06-grafana-dashboard-configmap.yaml";

/// Series this workspace produces: the labels each one carries, and the file
/// that publishes it.
///
/// The labels are recorded at the call sites: `http_requests_total` /
/// `http_request_duration_ms` share one label map, as do `llm_calls_total` /
/// `llm_call_latency_ms`. The file is read and searched for the name, so a
/// producer that was renamed or removed fails the test instead of licensing a
/// panel that can never draw anything.
///
/// The third column is what makes the first two a contract rather than a
/// preference: it is the series' own producer, not another reader of it.
const PRODUCED: &[(&str, &[&str], &str)] = &[
    (
        "http_requests_total",
        &["method", "endpoint", "status"],
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "http_request_duration_ms",
        &["method", "endpoint", "status"],
        "crates/cog-gateway/src/lib.rs",
    ),
    (
        "llm_calls_total",
        &["upstream", "model", "result", "actor"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_call_latency_ms",
        &["upstream", "model", "result", "actor"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // Agent run counters carry no labels: they are totals for the process, not
    // per-task readings, so nothing distinguishes one sample from the next.
    (
        "agent_success_count",
        &[],
        "crates/cog-agent/src/observable.rs",
    ),
    (
        "agent_budget_exhausted_count",
        &[],
        "crates/cog-agent/src/observable.rs",
    ),
    // The iteration ceiling is derived per role, so which role is being cut off
    // is the question; a single unlabelled total could not answer it.
    (
        "agent_iteration_budget_exhausted",
        &["role"],
        "crates/cog-agent/src/observable.rs",
    ),
    // Pool recovery readings. Three separate series rather than one "recovers
    // at" number: which upstream said it, when we will probe again, and how long
    // the window an upstream stated is are answers of different strength, and
    // collapsing them is how a probe cadence gets read as a promise.
    (
        "llm_pool_evidenced_recovery_unix",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_pool_next_attempt_unix",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_pool_quota_window_secs",
        &[],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "cogneva_verification_budget_seconds",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "cogneva_verification_last_run_seconds",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "cogneva_verification_timeouts_total",
        &["kind"],
        "crates/cog-reflection/src/verification_budget.rs",
    ),
    (
        "llm_upstream_quota_window_secs",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_upstream_quota_reset_unix",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    (
        "llm_upstream_consecutive_failures",
        &["upstream"],
        "crates/cog-gateway/src/security_gateway.rs",
    ),
    // The host-wide build bound. Every reading carries the slot directory it is
    // about, because two processes that point at different directories bound
    // only themselves and nothing in the numbers would say so, and the role that
    // published it, because a process that runs no builds publishes the same
    // series at zero on purpose.
    (
        "cogneva_build_gate_slots",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_in_flight",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_waiting",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_acquired_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_refused_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_ms_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_ms_max",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_held_ms_total",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_held_ms_max",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    (
        "cogneva_build_gate_wait_budget_ms",
        &["dir", "role"],
        "crates/cog-core/src/build_gate.rs",
    ),
    // What each change's release build cost the host. Every reading carries the
    // change kind, because the decision these panels feed is which kind of
    // change to spend the next build on, and the outcome, because a deploy
    // budget kill and a build that ended on its own are not the same purchase.
    (
        "cogneva_evolution_build_seconds_total",
        &["intent", "outcome"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    (
        "cogneva_evolution_build_last_seconds",
        &["intent"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
    ),
    (
        "cogneva_evolution_build_outcomes_total",
        &["intent", "outcome"],
        "crates/cog-reflection/src/evolution_build_readings.rs",
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
    ("kube_pod_container_resource_limits", "kube-state-metrics"),
    (
        "kube_pod_container_status_restarts_total",
        "kube-state-metrics",
    ),
    (
        "kube_pod_container_status_last_terminated_reason",
        "kube-state-metrics",
    ),
    ("ALERTS", "Prometheus 由告警规则合成的序列"),
];

fn dashboard_text() -> String {
    let path = repo_root().join(DASHBOARD);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("dashboard manifest unreadable at {}: {e}", path.display()))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Whether an `expr` filters a datasource other than the metric store, i.e. a
/// log query. Those select by label and name no series, so running the PromQL
/// extractor over one invents names — a `$variable` reference reads as an
/// identifier. Only this dashboard holds such panels; the alert rules are all
/// metric queries.
fn is_log_query(expr: &str) -> bool {
    expr.trim_start().starts_with('{')
}

/// The decoded value of one string field on a line of the manifest.
///
/// The dashboard is JSON inside a YAML block scalar, so a label value in it
/// arrives as `\"firing\"`. Every reader that kept the escapes read that span as
/// opened and never closed, and the rest of the line disappeared into it --
/// which is how a check over this file comes to cover less than it looks like
/// it does. Decoding here means what is analysed is the text Prometheus is
/// given, and that text has no backslashes in it.
fn json_field(line: &str, key: &str) -> Option<String> {
    let object = format!("{{{}}}", line.trim().trim_end_matches(','));
    let value: serde_json::Value = serde_json::from_str(&object).ok()?;
    Some(value.get(key)?.as_str()?.to_string())
}

/// Every metric `expr` in the dashboard, with the line it sits on.
///
/// One reader for the name check and the shape check both, so a panel one of
/// them refuses to see is not a panel the other silently stops covering.
fn metric_exprs(text: &str) -> Vec<(usize, String)> {
    text.lines()
        .enumerate()
        .filter_map(|(n, line)| {
            let expr = json_field(line, "expr")?;
            // A log query selects by label, not by series name; the extractor
            // would read its `$variable` references as metrics.
            if expr.is_empty() || is_log_query(&expr) {
                return None;
            }
            Some((n + 1, expr))
        })
        .collect()
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
    let produced: BTreeSet<&str> = PRODUCED.iter().map(|(n, _, _)| *n).collect();
    let foreign: BTreeSet<&str> = FOREIGN.iter().map(|(n, _)| *n).collect();

    let mut unknown: BTreeSet<String> = BTreeSet::new();
    for (_, expr) in metric_exprs(&text) {
        for name in metric_names_in(&expr) {
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

/// A panel whose expression does not parse draws nothing, exactly like a panel
/// whose series nothing produces — and the check above passes it, because every
/// series in it is really produced.
///
/// The shape this catches is the one a hand edit makes: an operator dropped
/// between two operands that a matching clause then sits in front of. Nothing
/// in this repository reads PromQL grammar, so before this test the only thing
/// between such an edit and a blank panel was a person noticing.
#[test]
fn every_expression_the_dashboard_writes_is_one_prometheus_can_parse() {
    let mut complaints: Vec<String> = Vec::new();
    for (line, expr) in metric_exprs(&dashboard_text()) {
        for complaint in shape_complaints(&expr) {
            complaints.push(format!("第 {line} 行: {complaint}\n    {expr}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "面板表达式 Prometheus 解析不了，面板会一直是空的:\n{}",
        complaints.join("\n")
    );
}

/// The other half of the same claim, and the half no other check can make: the
/// table says something records these series, and that is the file it names.
///
/// The check above only compares the panel against the table, and the panel and
/// the table are both edited by whoever changes the dashboard. A producer
/// renamed in its own crate moves neither, so every check here stays green while
/// the panel draws nothing at all — the empty panel this file exists to catch,
/// arrived at from the side nothing was watching. The name in the table is the
/// only place that claim lives, so it is the only place that can be held to it.
#[test]
fn every_series_the_table_claims_is_produced_is_named_by_its_producer() {
    let root = repo_root();
    let mut missing: Vec<String> = Vec::new();
    for (name, _, source) in PRODUCED {
        let text = std::fs::read_to_string(root.join(source))
            .unwrap_or_else(|e| panic!("{source} unreadable: {e}"));
        if !carries_the_producer(&text, name) {
            missing.push(format!("{name} 记在 {source}，该文件里找不到这个名字"));
        }
    }

    assert!(
        missing.is_empty(),
        "PRODUCED 表登记的产出点已经不存在，面板上的这个序列会永远画不出东西:\n{}",
        missing.join("\n")
    );
}

#[test]
fn every_label_a_legend_names_is_on_the_series_it_describes() {
    let text = dashboard_text();
    let labels_of = |name: &str| -> Option<&'static [&'static str]> {
        PRODUCED
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(_, labels, _)| *labels)
    };

    // Panels pair one expr with one legend within the same object; walking the
    // file in order and keeping the most recent expr is enough for a manifest
    // that holds one target per panel.
    let mut current: Option<String> = None;
    let mut mismatches: Vec<String> = Vec::new();
    for line in text.lines() {
        if let Some(expr) = json_field(line, "expr") {
            current = if is_log_query(&expr) {
                None
            } else {
                metric_names_in(&expr).into_iter().next()
            };
        } else if let Some(legend) = json_field(line, "legendFormat") {
            let (Some(metric), Some(expected)) = (
                current.as_ref(),
                labels_of(current.as_ref().unwrap().as_str()),
            ) else {
                continue;
            };
            for label in legend_labels_in(&legend) {
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
