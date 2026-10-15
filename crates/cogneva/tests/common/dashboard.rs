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

/// One metric expression as written, and the kind of panel that carries it.
pub struct PanelExpr {
    /// The line the expression sits on, as a complaint points at it.
    ///
    /// Only the dashboard contract reads it: the two census targets reach the
    /// same expressions through [`series_in`], which keeps the names and drops
    /// the position. This module is compiled into each of the three test
    /// binaries on its own, so in those two the field is read by nobody, and
    /// `cargo clippy --workspace --all-targets -- -D warnings` turns that into
    /// an error where plain `cargo test` only warns. Same reason the two
    /// functions below carry the attribute.
    #[allow(dead_code)]
    pub line: usize,
    /// The `type` of the panel this expression belongs to, as the manifest
    /// writes it. Empty when no `"type"` stands at the panel's own depth: that
    /// is a reading the caller has to refuse rather than assume, because a
    /// panel nobody classified and a panel that draws nothing look the same
    /// from here.
    pub panel_type: String,
    pub expr: String,
}

/// Every metric `expr` in the dashboard, with the line it sits on and the kind
/// of panel it is drawn by.
///
/// One reader for the name check, the shape check and the panel-kind check, so
/// a panel one of them refuses to see is not a panel the others silently stop
/// covering.
///
/// The panel's `type` is read by depth: a `"type"` line replaces the one held
/// when it is no deeper than it, and it is attributed to an expression when it
/// is no deeper than *that*. In this file the panel's own fields are written at
/// one depth and each target four spaces deeper, so the guard is what keeps
/// `"datasource": {"type": "prometheus"}` from making every expression read as a
/// panel of its own kind -- the depth-blind version of this walk files all 84
/// under `prometheus`. A kind this walk gets wrong lands outside the caller's
/// table and fails there, which is what the table being closed is for.
pub fn metric_exprs(text: &str) -> Vec<PanelExpr> {
    let mut panel: Option<(usize, String)> = None;
    text.lines()
        .enumerate()
        .filter_map(|(n, line)| {
            let indent = line.len() - line.trim_start().len();
            if let Some(kind) = json_field(line, "type") {
                // Siblings replace each other, so `<=`; a nested object's
                // `type` is deeper and does not.
                if panel.as_ref().is_none_or(|(d, _)| indent <= *d) {
                    panel = Some((indent, kind));
                }
            }
            let expr = json_field(line, "expr")?;
            // A log query selects by label, not by series name; the extractor
            // would read its `$variable` references as metrics.
            if expr.is_empty() || is_log_query(&expr) {
                return None;
            }
            Some(PanelExpr {
                line: n + 1,
                panel_type: match &panel {
                    Some((d, kind)) if *d <= indent => kind.clone(),
                    _ => String::new(),
                },
                expr,
            })
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
        .flat_map(|panel| metric_names_in(&panel.expr))
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The depth guard, on a file shaped like the one this reads: a panel's
    /// fields at one depth, the target's own `datasource` deeper, two sibling
    /// panels, and a templating variable's `type` after them.
    ///
    /// Without the guard every expression is filed under `prometheus` -- the
    /// last `"type"` line above it -- and `prometheus` is a kind no caller's
    /// table lists, so the check that reads this reds on all 84 panels rather
    /// than on none. Which is also why the shape is worth a test of its own:
    /// the reading is not "empty or wrong", it is "wrong in a way the caller
    /// notices".
    #[test]
    fn a_panels_kind_is_the_type_at_or_above_its_expression() {
        let text = r#"
  cogneva-overview.json: |
    {
      "panels": [
        {
          "type": "stat",
          "targets": [
            {
              "expr": "sum(ALERTS{alertstate=\"firing\"})",
              "datasource": { "type": "prometheus" }
            }
          ]
        },
        {
          "type": "timeseries",
          "targets": [
            {
              "expr": "rate(x_total[5m])",
              "datasource": { "type": "prometheus" }
            }
          ]
        }
      ],
      "templating": { "list": [ { "name": "pod", "type": "textbox" } ] }
    }
"#;
        // 解析结果先绑一次：`Vec<PanelExpr>` 里的字段是 String，直接对临时值取
        // 引用会在这一句结束时就悬空，而这个测试是唯一读深度归属的地方。
        let panels = metric_exprs(text);
        let got: Vec<(&str, &str)> = panels
            .iter()
            .map(|p| (p.panel_type.as_str(), p.expr.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("stat", "sum(ALERTS{alertstate=\"firing\"})"),
                // 换了面板就换类型（同级覆盖），不是第一个说了算。
                ("timeseries", "rate(x_total[5m])"),
            ],
            "面板类型按深度归属：目标自带的 `datasource` 更深、不算，模板变量的 `type` 不在面板里"
        );
    }
}
