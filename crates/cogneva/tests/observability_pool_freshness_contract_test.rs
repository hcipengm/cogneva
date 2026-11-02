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
//! Panels are the same question asked of a second reader. A dashboard query
//! draws the value it is handed, and the store hands it the newest row it holds
//! forever: a panel for an upstream that was taken out of the config keeps
//! drawing that upstream's last number, and the count of rows on the panel reads
//! as the size of the pool. There is one shape here that is worse than a stale
//! number and worth naming, because it was live when this gate was written:
//! `clamp_min(<gauge> - time(), 0)` is a countdown, so a writer that stopped
//! days ago leaves the gauge frozen in the past, the subtraction goes negative,
//! and the floor renders it as `0` -- "it happens now", recomputed on every
//! scrape, for a reading nobody is writing. A stale number at least looks old.
//!
//! The panel pass has no exemptions. The two rules exempted below are absence
//! readings: their subject is that something did *not* arrive, and a frozen
//! value keeps that statement true. A panel is not an absence reading -- it
//! draws a quantity as though it were being measured now -- so there is no
//! panel for which "no writer leg" is the right shape.
//!
//! [`DURABLE_POOL_GAUGE_HEARTBEAT`]: cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT

use crate::common;

use common::routes::{read_repo, repo_root};
use common::service_topology;

use std::collections::BTreeSet;

use crate::promql;

use promql::unguarded_store_reading_complaints;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// Every rule the chart renders, name to expression, in file order.
fn chart_rules() -> Vec<(String, String)> {
    let value: serde_json::Value =
        serde_json::from_str(&read_repo(CHART_CONFIG)).expect("chart config is not valid JSON");
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

/// Every rule this repo ships: the chart's, plus the ones a manifest applies.
///
/// The chart is not the only place rules come from, and a gate that reads one
/// source says nothing about the other.
fn shipped_rules() -> Vec<(String, String)> {
    let mut rules = chart_rules();
    rules.extend(service_topology::prometheus_rules(&repo_root()));
    rules
}

/// Every query a provisioned dashboard runs, as `dashboard / panel title` to
/// expression.
///
/// The dashboards reach Grafana as `ConfigMap` data labelled `grafana_dashboard`
/// and are read by file provisioning, so the file is the query text and nothing
/// else stands between it and the panel.
fn panel_expressions() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (file, doc) in service_topology::observability_documents(&repo_root()) {
        if doc["kind"].as_str() != Some("ConfigMap") {
            continue;
        }
        if doc["metadata"]["labels"]["grafana_dashboard"].as_str() != Some("1") {
            continue;
        }
        let Some(data) = doc["data"].as_mapping() else {
            continue;
        };
        for (key, body) in data {
            let Some(dashboard_name) = key.as_str() else {
                continue;
            };
            let Some(body) = body.as_str() else {
                continue;
            };
            let dashboard: serde_json::Value = serde_json::from_str(body)
                .unwrap_or_else(|e| panic!("{file} / {dashboard_name} is not valid JSON: {e}"));
            walk_panels(dashboard_name, &dashboard, &mut out);
        }
    }
    out
}

/// Collect `<dashboard> / <panel title>` to `expr` from a dashboard model.
///
/// Every nested value is walked rather than only `panels`, because the model
/// carries rows, collapses and library panel references, and a query hidden one
/// level deeper is still a query Grafana runs.
fn walk_panels(dashboard: &str, node: &serde_json::Value, out: &mut Vec<(String, String)>) {
    walk_panels_titled(dashboard, node, "<untitled>", out);
}

/// The same walk, carrying the title down from the enclosing panel.
///
/// A query lives in a `targets` entry, and the title that names it sits on the
/// panel one level up. Reporting the target's own map would print `<untitled>`
/// for every query in the file, and a complaint that cannot say which panel is
/// wrong is a complaint someone has to go hunting for.
fn walk_panels_titled(
    dashboard: &str,
    node: &serde_json::Value,
    title: &str,
    out: &mut Vec<(String, String)>,
) {
    match node {
        serde_json::Value::Object(map) => {
            let title = map
                .get("title")
                .and_then(|value| value.as_str())
                .unwrap_or(title);
            if let Some(expr) = map.get("expr").and_then(|value| value.as_str()) {
                out.push((format!("{dashboard} / {title}"), expr.to_string()));
            }
            for value in map.values() {
                walk_panels_titled(dashboard, value, title, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                walk_panels_titled(dashboard, item, title, out);
            }
        }
        _ => {}
    }
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
    let heartbeat = cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT.as_secs();

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
        let mut found = unguarded_store_reading_complaints(&expr, &durable);
        // A leg whose bound is no longer than the heartbeat is not a leg: a
        // healthy writer that publishes on the heartbeat sits at exactly that
        // age between two of its own writes, so the rule drops a live reading
        // and reports it as a stopped writer. The bound has to be strictly
        // greater, and it is the heartbeat that decides how much greater.
        for name in promql::metric_names_in(&expr) {
            if !durable.contains(&name) {
                continue;
            }
            let Some(bound) = promql::companion_age_bound(&expr, &name) else {
                continue;
            };
            if bound <= heartbeat {
                found.push(format!(
                    "`{name}` 的新鲜界 {bound}s 不长于心跳 {heartbeat}s——\
                     健康的写者在两次自己的发布之间正好是这个年龄，会被读成「写者停了」"
                ));
            }
        }
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

#[test]
fn every_panel_that_reads_a_durable_pool_gauge_reads_its_writer_too() {
    let durable = durable_pool_gauges();
    let heartbeat = cog_gateway::security_gateway::DURABLE_GAUGE_HEARTBEAT.as_secs();

    let mut complaints: Vec<String> = Vec::new();
    let mut reading_panels = 0usize;
    let mut guarded_panels = 0usize;
    let mut panels = 0usize;
    for (panel, expr) in panel_expressions() {
        panels += 1;
        let reads_the_family = promql::metric_names_in(&expr)
            .iter()
            .any(|read| durable.contains(read));
        if !reads_the_family {
            continue;
        }
        reading_panels += 1;
        let mut found = unguarded_store_reading_complaints(&expr, &durable);
        for name in promql::metric_names_in(&expr) {
            if !durable.contains(&name) {
                continue;
            }
            let Some(bound) = promql::companion_age_bound(&expr, &name) else {
                continue;
            };
            if bound <= heartbeat {
                found.push(format!(
                    "`{name}` 的新鲜界 {bound}s 不长于心跳 {heartbeat}s——健康的写者在两次自己的发布之间正好是这个年龄，会被读成「写者停了」"
                ));
            }
        }
        if found.is_empty() {
            guarded_panels += 1;
            continue;
        }
        for complaint in found {
            complaints.push(format!("{panel}: {complaint}\n    {expr}"));
        }
    }

    assert!(
        complaints.is_empty(),
        "读耐久池读数组的面板没有同时要求写者还在写——store 会永久送出最后一笔值，\
         于是面板把一个早已没人写的数画成当前值（`clamp_min(<gauge> - time(), 0)` 更糟：它把\
         冻在过去的值渲染成 0，读起来是「就是现在」）：\n{}",
        complaints.join("\n")
    );
    assert!(
        panels >= 100 && reading_panels >= 5 && guarded_panels >= 5,
        "扫到 {panels} 条面板查询，其中 {reading_panels} 条读这一族、{guarded_panels} 条带写者判据——\
         题面或名单已经不在了，这条判据在空转"
    );
}

/// The panel pass has to reject a countdown that draws a dead writer's floor as
/// "now", and accept the same countdown with the writer leg on it. The shape is
/// pinned here because it is the one that reads *better* the longer nobody
/// writes, which makes it the least likely to be noticed by watching a panel.
#[test]
fn the_panel_pass_reports_a_countdown_that_floors_a_frozen_reading() {
    let durable = durable_pool_gauges();

    let bare = "clamp_min(llm_pool_evidenced_recovery_unix - time(), 0)";
    assert!(
        !unguarded_store_reading_complaints(bare, &durable).is_empty(),
        "把冻住的值减成负数再 clamp 到 0，判据却放过了这条倒计时: {bare}"
    );

    let guarded = "clamp_min(llm_pool_evidenced_recovery_unix - time(), 0) and ((time() - llm_pool_evidenced_recovery_unix_observed_timestamp_seconds) < 3600)";
    assert!(
        unguarded_store_reading_complaints(guarded, &durable).is_empty(),
        "带写者判据的倒计时被误判: {guarded}"
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

/// A gauge written with an `upstream` label outlives the upstream it describes.
///
/// The store answers a gauge with the newest row per label set, so a series
/// whose producer stopped keeps answering in the present tense: every count
/// that aggregates by upstream keeps counting a member the configuration no
/// longer has, and it does so forever. Nothing in the writer deletes a label
/// set it stopped publishing — it has no idea anyone else still reads it —
/// so the family needs a declared channel that removes the departed ones.
/// `ROSTERED_LABEL_SERIES` is that declaration, and the roster it reads is
/// `llm_upstream_position`, which the pool loop writes for every configured
/// upstream whether or not this process has ever called it.
///
/// The failure this gate exists for is silent in the direction that hurts: a
/// new per-upstream gauge written in the pool loop and not added to the
/// roster leaves no trace once its upstream goes away. It is not a wrong
/// number, it is a member that never leaves the population, and the rule that
/// reports such a residue would report it for that series forever — a lamp
/// nobody can turn off, which is the same as no lamp.
///
/// Read from the writer's own call sites rather than from
/// `DURABLE_POOL_GAUGES`: that list answers "whose window must outlast the
/// heartbeat", which is a different question, and a per-upstream gauge that is
/// not on it is exactly the case this gate is about.
#[test]
fn every_gauge_written_per_upstream_is_on_the_retirement_roster() {
    // One series per upstream can be left behind on purpose: the ledger
    // projection is written from the ledger, and the ledger still holds rows
    // for an upstream that left the configuration (and a `-` total beside
    // them). Its producer is alive for every one of those label values, so
    // releasing them would delete a total that is still being written.
    const EXEMPT: &[(&str, &str)] = &[(
        "llm_tokens_total",
        "账本投影：它对已经离开配置的上游照旧写（账上还有这家记过的花销，另有一行 `-` \
         的合计），写者对每一个这样的标签值都还活着，退休它就是删一个仍在写的合计",
    )];

    let gateway = read_repo("crates/cog-gateway/src/security_gateway.rs");
    // The test module holds a stub backend with its own `record_gauge`, and a
    // `#[cfg(test)]` helper above it. Cut at the test module's opening line, so
    // the scan reads production call sites only.
    let production = gateway
        .split_once("\n#[cfg(test)]\nmod tests {")
        .map(|(before, _)| before)
        .expect("测试模块的开头变了，这条判据会扫到自己的桩实现");

    let mut per_upstream: BTreeSet<String> = BTreeSet::new();
    let mut from = 0;
    while let Some(rel) = production[from..].find("record_gauge(") {
        let at = from + rel;
        // One past the opening, not past the span: a call inside another one's
        // span is a call too, and a scan that skips it goes green by not
        // looking.
        from = at + 1;
        let Some(call) = balanced_span(&production[at..]) else {
            continue;
        };
        if !call.contains("\"upstream\"") {
            continue;
        }
        for (ident, metric) in cog_core::metric_names::IDENTIFIED {
            if call.contains(&format!("metric_names::{ident}")) {
                per_upstream.insert(metric.as_str().to_string());
            }
        }
    }

    let roster: BTreeSet<&str> = cog_storage::ROSTERED_LABEL_SERIES.iter().copied().collect();
    assert!(
        roster.len() >= 8,
        "退休名册只剩 {} 条——这条判据在拿一份空名单对人口",
        roster.len()
    );
    // The denominator. A scanner that stopped matching would otherwise report
    // every populated family as covered.
    assert!(
        per_upstream.len() >= 10,
        "只扫到 {} 条按上游写的 gauge——写它们的调用点换了形状，这条判据已经看不见它们了",
        per_upstream.len()
    );

    for (name, reason) in EXEMPT {
        assert!(
            per_upstream.contains(*name),
            "豁免表里的 `{name}` 已经不再按上游写（理由：{reason}）——这条豁免在给谁放行？"
        );
        assert!(!reason.trim().is_empty(), "`{name}` 的豁免没写理由");
        assert!(
            !roster.contains(name),
            "`{name}` 同时出现在退休名册和豁免里，两句话互相作废"
        );
    }

    let missing: Vec<&str> = per_upstream
        .iter()
        .map(String::as_str)
        .filter(|name| !roster.contains(name) && !EXEMPT.iter().any(|(exempt, _)| exempt == name))
        .collect();
    assert!(
        missing.is_empty(),
        "这些序列按 upstream 写进耐久面，却不在 `ROSTERED_LABEL_SERIES` 里：上游离开配置后\
         它们的行会留在现在时里，而没有任何东西会把它们删掉。要么登记进名册，要么在豁免里\
         写清为什么它的写者对走掉的那一家还活着：\n  {}",
        missing.join("\n  ")
    );
}

/// The text from its first `(` through the one that closes it.
///
/// Deliberately naive about strings and comments: it is applied to call sites
/// this repo writes, and the gateway's own funnel test reads its source the
/// same way. A span it cannot close is skipped rather than guessed at.
fn balanced_span(text: &str) -> Option<&str> {
    let mut depth = 0usize;
    for (i, c) in text.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[..=i]);
                }
            }
            _ => {}
        }
    }
    None
}
