//! principle: feedback_no_hardcoding_config_surface
//!
//! Gate: the readings that describe the platform's own delivery chain must name
//! a series this repository publishes, a producer file that exists, and — where
//! a bound is claimed — the same bound the alert rule for that series already
//! states.
//!
//! The board had no delivery readings at all until 2026-10-31: on that day the
//! mainline stood still for 2.7 hours while every row of "读数" read 正常,
//! because the six rows there all ask about the platform's surroundings. Adding
//! rows is easy; adding rows that lie is easier. A reading is honest only when
//! three things hold, and each fails silently on its own:
//!
//! * the series has a producer. A reading whose series nobody publishes is not
//!   zero — it is "没有序列", forever, on a page that otherwise looks alive.
//!   Foreign producers can only be vouched for by their documentation; our own
//!   cannot hide behind that, so the series is looked up in the metric registry
//!   this repository compiles in.
//! * `produced_by` names a file. The board prints it as the answer to "who do I
//!   look at when this disappears"; a renamed file turns that answer into a path
//!   nobody can follow, and nothing about the row changes.
//! * a threshold has a source. The four-hour bound on the CI hold is the alert
//!   rule's own number; if the rule moves and the board does not, the pager and
//!   the page tell two stories about the same series, and the one an operator
//!   happens to open decides what they believe.

use crate::common;

use common::routes::{read, rendered_root, repo_root};
use common::rust_production::{production_sources, production_text};

/// 名字的声明面：整个 `cog-core` 源树，不是一个文件。
///
/// 闭集（`metric_names.rs`）登记的是**存储能持有**的序列；抓取面渲染的序列
/// （`RawMetric`）合法地住在同 crate 的别处（`contract/build_gate.rs` 十六条、
/// `build_cache.rs` 的缓存目标一族）。判据要答的问题是「这个名字是不是本仓真的写出来的」，那要读整个
/// 声明面；只读闭集文件会把抓取面的名字判成不存在，而它们一直都在。
const CONTRACT_DIR: &str = "生产树（全仓，不含测试）";
const RENDERED_CONFIG: &str = "deploy/rendered/k3s-single/10-configmap-cogneva-json.yaml";

fn readings() -> &'static [&'static cog_gateway::readings::ReadingSpec] {
    &cog_gateway::readings::READINGS
}

/// The group's rows, or a red gate: an empty group would make every check below
/// pass by reading nothing, and "nothing to check" prints exactly like "all
/// checked".
fn delivery() -> Vec<&'static cog_gateway::readings::ReadingSpec> {
    let rows: Vec<_> = readings()
        .iter()
        .copied()
        .filter(|spec| spec.group == cog_gateway::readings::ReadingGroup::Delivery)
        .collect();
    assert!(
        !rows.is_empty(),
        "no delivery readings: the platform's own chain would be back to having \
         nothing on the board, which is the state that let a 2.7-hour stall read \
         as 正常 everywhere"
    );
    rows
}

/// The bare series name: `llm_upstream_healthy{upstream=~".+"}` is a query, and
/// what the registry knows is the name in front of the brace.
fn series_name(spec: &cog_gateway::readings::ReadingSpec) -> &str {
    spec.series
        .split(|c: char| c == '{' || c.is_whitespace())
        .next()
        .unwrap_or(spec.series)
}

#[test]
fn every_delivery_reading_names_a_series_this_repo_publishes() {
    // 名字的声明面是**整棵生产树**，不是某个目录：闭集在
    // `contract/metric_names.rs`，抓取面的名字在 `contract/build_gate.rs` 与
    // `build_cache.rs`，还有一批在各 crate 自己的 readings 文件里
    // （`discovery_outcome_readings.rs` 这类）。判据要答的是「本仓真的写出来过
    // 这个名字吗」，按目录猜会把合法的名字判成不存在。
    let registry: String = {
        let mut text = String::new();
        for path in production_sources(&repo_root()) {
            if let Ok(source) = std::fs::read_to_string(&path) {
                text.push_str(&production_text(&source));
                text.push('\n');
            }
        }
        text
    };
    let mut checked = 0;
    for spec in delivery() {
        let name = series_name(spec);
        assert!(
            registry.contains(&format!("\"{name}\"")),
            "reading {} names {name}, which no constant in {CONTRACT_DIR} declares; \
             the board would show 没有序列 forever and the row would look like a \
             quiet system rather than a reading nobody wired",
            spec.id
        );
        checked += 1;
    }
    assert!(checked > 0, "this gate read no delivery readings");
}

#[test]
fn every_delivery_reading_names_a_producer_file_that_exists() {
    let root = repo_root();
    for spec in delivery() {
        let path = root.join(spec.produced_by);
        assert!(
            path.is_file(),
            "reading {} says it is produced by {}, which is not a file in this \
             tree; the board prints that as the answer to \"who do I look at when \
             this disappears\", so a stale path is an answer nobody can follow",
            spec.id,
            spec.produced_by
        );
    }
}

/// The rendered rules, as the operator's pager sees them.
fn rules() -> Vec<serde_json::Value> {
    let text = read(&rendered_root().join("k3s-single/10-configmap-cogneva-json.yaml"));
    let at = text
        .find("cogneva.json:")
        .unwrap_or_else(|| panic!("{RENDERED_CONFIG} no longer carries cogneva.json"));
    let brace = at
        + text[at..]
            .find('{')
            .unwrap_or_else(|| panic!("{RENDERED_CONFIG}: cogneva.json is not a JSON body"));
    // 只读第一个值：`cogneva.json` 的正文就是它，后面剩下的字节是 YAML 的下一行。
    let body = serde_json::Deserializer::from_str(&text[brace..])
        .into_iter::<serde_json::Value>()
        .next()
        .unwrap_or_else(|| panic!("{RENDERED_CONFIG}: cogneva.json is empty"))
        .unwrap_or_else(|e| panic!("{RENDERED_CONFIG} is not valid JSON: {e}"));
    let mut found = Vec::new();
    collect_rules(&body, &mut found);
    assert!(
        !found.is_empty(),
        "{RENDERED_CONFIG} carries no rules with a promql"
    );
    found
}

fn collect_rules(value: &serde_json::Value, out: &mut Vec<serde_json::Value>) {
    match value {
        serde_json::Value::Object(map) => {
            if map.get("promql").is_some() && map.get("name").is_some() {
                out.push(value.clone());
            }
            for child in map.values() {
                collect_rules(child, out);
            }
        }
        serde_json::Value::Array(items) => {
            for child in items {
                collect_rules(child, out);
            }
        }
        _ => {}
    }
}

/// The numbers a rule compares against: its `condition`, plus any numeric
/// comparison left in the query.
///
/// `condition` is where a rule states its bound — the process applies it to each
/// value the query returns, and the alert fixture folds it back into the
/// expression before asking promtool. The promql scan stays because a bound can
/// also be a multiple of a measured quantity (`6 * max(scan_interval_seconds)`),
/// which no `condition` can express.
fn rule_bounds(rule: &serde_json::Value) -> Vec<f64> {
    let mut out: Vec<f64> = rule
        .get("condition")
        .and_then(|c| c.as_object())
        .map(|c| c.values().filter_map(|v| v.as_f64()).collect())
        .unwrap_or_default();
    out.extend(comparison_bounds(
        rule.get("promql").and_then(|p| p.as_str()).unwrap_or(""),
    ));
    out
}

/// The numbers a promql compares against: `> 14400`, `>= 1`, `== 0`.
///
/// The comparison is what makes a number a bound. Taken as a bare substring, the
/// `3600` in `(time() - …) < 3600` would pass for a four-hour threshold, and that
/// conjunct is a freshness horizon rather than a bound at all.
fn comparison_bounds(promql: &str) -> Vec<f64> {
    let mut out = Vec::new();
    let mut rest = promql;
    while let Some(at) = rest.find(['>', '<', '=']) {
        // 走完整个算子（`>=`、`==`），不是一格一格地走：一格一格走会让 `== 0`
        // 被读两遍，而「读重了」在这一族检查里是同一条界出现两次，看不出来。
        let operator = rest[at..]
            .chars()
            .take_while(|c| matches!(c, '>' | '<' | '='))
            .count();
        let tail = &rest[at + operator..];
        let number: String = tail
            .trim_start()
            .chars()
            .take_while(|c| c.is_ascii_digit() || *c == '.')
            .collect();
        if let Ok(value) = number.parse::<f64>() {
            out.push(value);
        }
        rest = tail;
    }
    out
}

/// The bound the board and the pager must agree on: the CI hold's four hours.
///
/// Only this pair is checked, because only this pair is the same number on two
/// surfaces today. A general version — "every threshold must appear in a rule" —
/// would pass for the wrong reason on the model-pool reading, whose critical
/// zero shows up in an unrelated rule's `== 0`, and a gate that passes by
/// accident is worse than no gate.
#[test]
fn the_ci_hold_reading_carries_the_bound_its_alert_rule_states() {
    let spec = readings()
        .iter()
        .find(|spec| spec.id == "mainline_ci_hold_seconds")
        .expect("the CI hold reading")
        .to_owned();
    let value = spec
        .crit
        .expect("the CI hold reading must carry a critical bound");
    let naming: Vec<_> = rules()
        .into_iter()
        .filter(|rule| {
            rule.get("promql")
                .and_then(|p| p.as_str())
                .map(|p| p.contains(spec.series))
                .unwrap_or(false)
        })
        .collect();
    assert!(
        !naming.is_empty(),
        "no rendered alert rule names {}, so nothing pages when this reading is \
         critical — the board would be the only surface that knows",
        spec.series
    );
    assert!(
        naming.iter().any(|rule| rule_bounds(rule).contains(&value)),
        "the board calls {value} critical for {} but no rule naming that series \
         compares against it: {:?}. Two surfaces, one series, different bounds — \
         whichever one the operator opens decides what they believe",
        spec.series,
        naming
            .iter()
            .map(|rule| {
                (
                    rule.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                    rule_bounds(rule),
                )
            })
            .collect::<Vec<_>>()
    );
}

/// The bound reader is the half that can go blind: a promql shape it does not
/// split on reads no bounds at all and every check above passes.
#[test]
fn the_bound_reader_takes_comparisons_and_not_the_numbers_beside_them() {
    assert_eq!(
        comparison_bounds(
            "max(cogneva_mainline_ci_hold_seconds and \
             ((time() - cogneva_mainline_ci_hold_seconds_observed_timestamp_seconds) < 3600)) > 14400"
        ),
        vec![3600.0, 14400.0]
    );
    assert_eq!(comparison_bounds("sum(x) >= 1"), vec![1.0]);
    assert_eq!(comparison_bounds("max by (upstream) (y) == 0"), vec![0.0]);
    assert!(comparison_bounds("sum(rate(z[5m]))").is_empty());
}
