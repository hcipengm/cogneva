//! The series names the Grafana manifest reads.
//!
//! Two contracts ask who reads a published series: the dashboard contract,
//! which wants the panels judged against the producers, and the reader-face
//! census, which wants every name the closed set publishes accounted for by
//! some surface. Both have to read the same panels out of the same file, and a
//! second copy of this extractor would be a second answer to "is this panel
//! covered" — the drift the contracts exist to prevent, in the thing that does
//! the preventing.
//!
//! Included with `#[path]` rather than through `common/mod.rs`, like the other
//! helpers here: it depends only on the PromQL extractor, which the caller
//! declares alongside it.

use std::collections::BTreeSet;

use crate::promql::metric_names_in;

/// The chart-rendered Grafana manifest. The copy under `deploy/k3s/observability/`
/// is the one deployed on this host, and it is this file that both contracts read.
pub const DASHBOARD: &str =
    "deploy/k3s/observability/manifests/06-grafana-dashboard-configmap.yaml";

/// The decoded value of one string field on a line of the manifest.
///
/// The dashboard is JSON inside a YAML block scalar, so a label value in it
/// arrives as `\"firing\"`. Every reader that kept the escapes read that span as
/// opened and never closed, and the rest of the line disappeared into it --
/// which is how a check over this file comes to cover less than it looks like
/// it does. Decoding here means what is analysed is the text Prometheus is
/// given, and that text has no backslashes in it.
pub fn json_field(line: &str, key: &str) -> Option<String> {
    let object = format!("{{{}}}", line.trim().trim_end_matches(','));
    let value: serde_json::Value = serde_json::from_str(&object).ok()?;
    Some(value.get(key)?.as_str()?.to_string())
}

/// Whether an `expr` filters a datasource other than the metric store, i.e. a
/// log query. Those select by label and name no series, so running the PromQL
/// extractor over one invents names — a `$variable` reference reads as an
/// identifier. Only this dashboard holds such panels; the alert rules are all
/// metric queries.
pub fn is_log_query(expr: &str) -> bool {
    expr.trim_start().starts_with('{')
}

/// Every metric `expr` in the dashboard, with the line it sits on.
///
/// One reader for the name check and the shape check both, so a panel one of
/// them refuses to see is not a panel the other silently stops covering.
pub fn metric_exprs(text: &str) -> Vec<(usize, String)> {
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

/// Every series name the panels read, over the manifest as written.
///
/// Used by the alert-rule census, which compiles this module into its own test
/// binary; the dashboard contract reaches the same reading through
/// [`metric_exprs`] because it needs each expression's line for its
/// producer check. Kept here so both faces read the panels through one
/// extractor rather than two that drift.
#[allow(dead_code)]
pub fn series_in(text: &str) -> BTreeSet<String> {
    metric_exprs(text)
        .iter()
        .flat_map(|(_, expr)| metric_names_in(expr))
        .collect()
}

/// The same, read from the repository root.
#[allow(dead_code)]
pub fn series() -> BTreeSet<String> {
    series_in(&text())
}

/// The manifest as written. Panics rather than returning an empty set: a
/// missing dashboard would make every panel read as "reads nothing", which is
/// the reading this helper exists to refuse.
pub fn text() -> String {
    let path = repo_root().join(DASHBOARD);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("dashboard manifest unreadable at {}: {e}", path.display()))
}

pub fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
