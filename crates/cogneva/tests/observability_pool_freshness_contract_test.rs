//! A rule that reads a durable pool gauge has to read the writer too.
//!
//! The pool-state family reaches the shared store only when a value changes or
//! on [`DURABLE_POOL_GAUGE_HEARTBEAT`] (`cog_gateway::security_gateway`), and
//! the store renders the last value it holds on every scrape. So a gateway that
//! stops publishing leaves the family looking current: the samples keep arriving
//! with new timestamps, and nothing about the series says the number inside them
//! is old. The one reading that distinguishes a live writer from a dead one is
//! the family's `_observed_timestamp_seconds` companion, which carries the wall
//! clock of the last write rather than the time of the scrape.
//!
//! A rule that reads one of these gauges without requiring that companion's age
//! to be inside a bound is judging a number that may have been frozen hours or
//! days ago. The failure is not symmetric, and both directions are silent:
//!
//! - a frozen `1` keeps a "the pool is fine" verdict standing while the pool is
//!   not, and no series moves to say so;
//! - a frozen `0` keeps a fault lit forever, and no writer is left to clear it.
//!
//! The judgement itself is not restated here: it is
//! [`promql::unguarded_store_reading_complaints`], the same function the alert
//! contract test applies to the store-served gauges, applied to the family that
//! test excludes. That exclusion carries the window-versus-heartbeat rule, which
//! is a different question -- a window longer than the gap between two writes
//! keeps a healthy writer from looking absent, and says nothing at all about a
//! writer that is gone.
//!
//! [`DURABLE_POOL_GAUGE_HEARTBEAT`]: cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT

use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::Deserialize;

#[path = "common/promql.rs"]
mod promql;

use promql::unguarded_store_reading_complaints;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";
const RULE_MANIFESTS_DIR: &str = "deploy/k3s/observability/manifests";

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()))
}

/// Every rule the chart renders, name to expression, in file order.
fn chart_rules() -> Vec<(String, String)> {
    let value: serde_json::Value =
        serde_json::from_str(&read(CHART_CONFIG)).expect("chart config is not valid JSON");
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

/// Every rule a `PrometheusRule` manifest ships, name to expression.
///
/// The chart is not the only place rules come from, and a gate that reads one
/// source says nothing about the other. A document that will not parse fails
/// rather than being skipped: skipping it would drop whichever rules it carried
/// and leave the gate green on a torn file.
fn manifest_rules() -> Vec<(String, String)> {
    let dir = repo_root().join(RULE_MANIFESTS_DIR);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{} unreadable: {e}", dir.display()))
        .map(|entry| entry.expect("manifest entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml")
        })
        .collect();
    paths.sort();

    let mut rules = Vec::new();
    for path in paths {
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{} unreadable: {e}", path.display()));
        for document in serde_yaml::Deserializer::from_str(&text) {
            let doc = serde_yaml::Value::deserialize(document).unwrap_or_else(|e| {
                panic!("{} has a document that will not parse: {e}", path.display())
            });
            if doc["kind"].as_str() != Some("PrometheusRule") {
                continue;
            }
            for group in doc["spec"]["groups"].as_sequence().into_iter().flatten() {
                for rule in group["rules"].as_sequence().into_iter().flatten() {
                    let name = rule["alert"].as_str().or_else(|| rule["record"].as_str());
                    if let (Some(name), Some(expr)) = (name, rule["expr"].as_str()) {
                        rules.push((name.to_string(), expr.to_string()));
                    }
                }
            }
        }
    }
    rules
}

fn shipped_rules() -> Vec<(String, String)> {
    let mut rules = chart_rules();
    rules.extend(manifest_rules());
    rules
}

/// The family the store publishes on a heartbeat rather than on every change.
///
/// Read from the gateway that declares it, not copied: the list is held to the
/// writer's own `record_gauge` call sites by a unit test there, so this gate
/// follows the code when the family grows instead of going stale beside it.
fn durable_pool_gauges() -> BTreeSet<String> {
    let family: BTreeSet<String> = cog_gateway::security_gateway::DURABLE_POOL_GAUGES
        .iter()
        .map(|name| name.as_str().to_string())
        .collect();
    // The denominator: an empty family would make every complaint loop below
    // run zero times and read as green.
    assert!(
        family.len() >= 8,
        "耐久池读数组只剩 {} 条，这条判据已经形同虚设",
        family.len()
    );
    family
}

/// Rules that read the family without a writer leg, and the reason each one is
/// right to.
///
/// Both are absence readings, not present-tense ones. The distinction decides
/// which way the leg would cut: for a rule that asserts a value *now*, a frozen
/// value is a false statement and the leg silences it; for a rule whose subject
/// is the absence of something, a frozen value keeps the statement true, and
/// demanding that the writer be alive would suppress exactly the fault being
/// reported.
const EXEMPT: &[(&str, &str)] = &[
    (
        "llm_pool_signal_undelivered",
        "the verdict is that the pool's answer did not reach the cross-process \
         key for an hour: a writer that stopped is one more way for it not to \
         arrive, so a frozen 0 is a true statement and this is the rule that \
         reads it. A writer that stopped while the last value was 1 is \
         `gateway_durable_writes_failing`'s subject, not this one's",
    ),
    (
        "llm_spend_reading_is_blind",
        "the reading's value *is* the wall clock of the last successful sum, so \
         the rule's whole subject is that clock's age; freezing it is the fault \
         this rule exists to report, and a writer leg would take the rule down \
         at the moment it is right",
    ),
];

#[test]
fn every_rule_that_reads_a_durable_pool_gauge_reads_its_writer_too() {
    let durable = durable_pool_gauges();

    let shipped = shipped_rules();
    // Every exemption has to name a rule that is still shipped: a rename would
    // otherwise leave the entry licensing nothing, quietly.
    for (name, reason) in EXEMPT {
        assert!(
            shipped.iter().any(|(rule, _)| rule == name),
            "豁免表里的 `{name}` 已经不是一条发布中的规则（理由：{reason}）——改名后这条豁免在给谁放行？"
        );
        assert!(!reason.trim().is_empty(), "`{name}` 的豁免没写理由");
    }

    let mut complaints: Vec<String> = Vec::new();
    let mut reading_rules = 0usize;
    let mut guarded_rules = 0usize;
    for (rule, expr) in shipped {
        if EXEMPT.iter().any(|(name, _)| *name == rule) {
            continue;
        }
        let reads_the_family = promql::metric_names_in(&expr)
            .iter()
            .any(|read| durable.contains(read));
        if !reads_the_family {
            continue;
        }
        reading_rules += 1;
        let found = unguarded_store_reading_complaints(&expr, &durable);
        if found.is_empty() {
            guarded_rules += 1;
            continue;
        }
        for complaint in found {
            complaints.push(format!("{rule}: {complaint}\n    {expr}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "读耐久池读数组的规则没有同时要求写者还在写（`(time() - <伴生钟>) < <界>`，\
         界要宽过 {}s 的心跳）：\n{}",
        cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT.as_secs(),
        complaints.join("\n")
    );
    // The other half of the denominator: a sweep that reads no rule at all is
    // green for the same reason an empty file is.
    assert!(
        reading_rules >= 10 && guarded_rules >= 10,
        "只有 {reading_rules} 条规则读这一族、其中 {guarded_rules} 条带写者判据——\
         题目或名单已经不在了，这条判据在空转"
    );
}

/// The gate has to reject the shape it exists for, and accept the one it asks
/// for. A sweep that returns nothing on both is agreeing with itself.
#[test]
fn the_gate_reports_a_pool_read_without_its_writer() {
    let durable = durable_pool_gauges();

    let without = r#"(sum by (upstream) (changes(llm_upstream_client_errors_total{upstream=~".+"}[1h])) > 0) and on () (max(llm_pool_available) == 1)"#;
    assert!(
        !unguarded_store_reading_complaints(without, &durable).is_empty(),
        "这条规则把池级读数当成此刻的观测，没有要求写者还在写，判据却放过了它: {without}"
    );

    let with = r#"(sum by (upstream) (changes(llm_upstream_client_errors_total{upstream=~".+"}[1h])) > 0) and on () (max(llm_pool_available and ((time() - llm_pool_available_observed_timestamp_seconds) < 3600)) == 1)"#;
    assert!(
        unguarded_store_reading_complaints(with, &durable).is_empty(),
        "带写者判据的形状被误判: {with}"
    );
}
