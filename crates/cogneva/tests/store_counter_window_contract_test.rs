//! principle: feedback_a_whole_tree_census_reads_shapes_so_new_artifacts_must_declare_themselves
//! principle: feedback_a_prohibitive_gate_needs_a_counterexample_census_first
//!
//! A counter rendered from a store row is not a process-local counter.
//!
//! Some of this platform's counters live in the shared store rather than in the
//! process that serves them: every pod reads the row and renders the same
//! value, so the number means the same thing everywhere and a pod that is
//! replaced continues from where the previous one left off. `increase()` and
//! `rate()` were written for the other kind of counter. They add up one series'
//! own life and read a drop as a reset, so on a value that changes its series
//! identity every time the deployment rolls they answer a question nobody
//! asked: the growth since this pod started, not the growth in the window.
//!
//! The error is in one direction and grows with the window. Measured against
//! the store's own record over a day, a twenty-four-hour rule read between four
//! and seventeen percent of the truth. Over half an hour it is accurate on the
//! median and under-reads just after a roll, because thirty minutes is about
//! how long a pod lives; over an hour it is simply wrong more often than not.
//!
//! The read that survives both a roll and the window where one pod has not yet
//! caught up with the other is the distance between the high and the low of the
//! leading value across the window:
//!
//! ```text
//! max_over_time((max by (<labels but pod/instance>) (x))[24h:1m])
//!   - min_over_time((max by (<labels but pod/instance>) (x))[24h:1m])
//! ```
//!
//! The inner `max` takes the leading pod at each instant, which erases the one
//! that is a step behind, and the envelope difference is immune to the series
//! being replaced. The window has to be written as a subquery: `[24h]` on an
//! aggregation is a parse error, and the error only shows up when the rule is
//! loaded, in a deployment, with nobody reading the log.
//!
//! This test is the line that keeps a new rule from being written the old way.
//! It is deliberately not "no rule may ever do this": shipped rules still do,
//! and a gate that goes red on the day it lands is a gate somebody switches
//! off. What it holds instead is a budget -- the rules that are still wrong are
//! named below, a rule not on that list may not join them, and a rule that
//! stops being wrong has to be taken off it. The list may only shrink.
//!
//! The size of that budget is written in its own name and nowhere else: a count
//! spelled out in prose is one more thing that expires when the next rule is
//! fixed, and an expired count reads as a stale gate rather than as a stale
//! sentence.

mod common;

use common::lex::Dialect;
use common::routes::{matching_paren, read_repo};

#[path = "common/derivation.rs"]
mod derivation;
#[path = "common/promql.rs"]
mod promql;

use derivation::{MetricDerivation, METRIC_DERIVATIONS};
use promql::windows_over;

use std::collections::BTreeSet;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// The longest window a store-rendered counter can be rated over and still be
/// read correctly by `increase()`: about one pod's life, which is when the
/// series identity survives the whole window. Beyond this the reading is not
/// merely late, it is a different quantity.
const ONE_POD_LIFE_SECONDS: u64 = 1_800;

/// The rules still read the old way, by name.
///
/// Every name here rates a counter that is rendered from a store row over a
/// window longer than the life of the series it lands in, so the number it
/// prints is the growth since this pod started rather than the growth in the
/// window. The one reason they share: the value is stored, not counted, and the
/// window is longer than the series.
///
/// **This list may only shrink.** Converting one of these rules to the envelope
/// shape removes its name in the same change -- the second assertion below
/// fails until it does, so the list cannot quietly become a record of things
/// that were already fixed. Adding a name is not how a new rule gets in: a new
/// rule is written in the shape above, and the assertion that fails says so.
const STILL_RATED_OVER_A_LONG_WINDOW: &[&str] = &[
    // Twenty-four hours.
    "self_evolution_rework_never_handed_off",
    "generated_change_discarded_in_full",
    "change_funnel_never_lands",
    "redrive_cause_exhausted",
    "redrive_refused_without_evidence",
    // Six hours.
    "self_evolution_yields_no_change",
    "self_evolution_has_no_change_sink",
    "task_checkpoint_not_persisted",
    "task_checkpoint_not_superseded",
    "change_landing_failures",
    "memory_decay_passes_all_failed",
    "version_contract_judgement_stopped",
    "mainline_supersession_question_stopped",
    "policy_evolution_rounds_all_failed",
    "knowledge_retrieval_backend_errored",
    // One hour.
    "dag_task_stalled_after_ready",
    "dag_task_reclaimed_despite_backlog",
    "registry_maintenance_round_incomplete",
    "buildah_store_round_incomplete",
    "mainline_ci_verdict_failed",
    "mainline_ci_verdict_unreadable",
    "mainline_rollout_never_lands",
    "mainline_poll_cycles_all_failed",
    "partition_maintenance_round_came_up_short",
    "memory_schema_repair_passes_all_failed",
    "memory_schema_orphans_unrepairable",
];

fn store_rendered() -> BTreeSet<String> {
    METRIC_DERIVATIONS
        .iter()
        .filter(|(_, derivation)| *derivation == MetricDerivation::Store)
        .map(|(name, _)| name.to_string())
        .collect()
}

/// Every `(name, promql)` pair in the chart's rule document.
///
/// `common::rules` reads the expressions alone, which is what the other gates
/// need; this one has to say *which rule* to convert, so it pairs each
/// expression with the name beside it. The walk is the same one -- descending
/// every key except `promql` itself -- and the two are kept apart deliberately:
/// a name is not part of an expression, and folding it into that reader would
/// hand every other consumer a field none of them asked for.
fn named_rules() -> Vec<(String, String)> {
    fn walk(value: &serde_json::Value, out: &mut Vec<(String, String)>) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(promql) = map.get("promql").and_then(|v| v.as_str()) {
                    let name = map
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("<unnamed>")
                        .to_string();
                    out.push((name, promql.to_string()));
                    return;
                }
                for child in map.values() {
                    walk(child, out);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            _ => {}
        }
    }
    let document: serde_json::Value =
        serde_json::from_str(&read_repo(CHART_CONFIG)).expect("chart config is not valid JSON");
    let mut out = Vec::new();
    walk(&document, &mut out);
    out
}

/// The window each `increase(...)`/`rate(...)` call in `expr` looks back over,
/// as `(series, seconds)`.
///
/// Scoped to the call rather than scanned across the whole expression on
/// purpose: `max_over_time(x[6h])` reads a long window correctly, and an
/// expression that contains both that and a five-minute `increase(y[5m])` would
/// otherwise be reported as rating `x` over six hours.
fn windows_inside_rate_calls(expr: &str, gated: &BTreeSet<String>) -> Vec<(String, u64)> {
    let mut out = Vec::new();
    for (offset, _) in expr.match_indices('(') {
        let head = &expr[..offset];
        let call = if head.ends_with("increase") {
            "increase"
        } else if head.ends_with("rate") {
            "rate"
        } else {
            continue;
        };
        // A longer identifier ending in `rate` -- `separate`, `iterate` -- is a
        // different word, and its own `(` opens an expression this function was
        // not asked about.
        let before = head.len() - call.len();
        if before > 0 {
            let previous = expr[..before].chars().next_back().unwrap_or(' ');
            if previous.is_ascii_alphanumeric() || previous == '_' {
                continue;
            }
        }
        let Some(close) = matching_paren(expr, offset, Dialect::Rust) else {
            continue;
        };
        let inside = &expr[offset + 1..close];
        out.extend(
            windows_over(inside, gated)
                .into_iter()
                .filter(|(_, seconds)| *seconds > ONE_POD_LIFE_SECONDS),
        );
    }
    out
}

fn rules_rated_over_a_long_window() -> BTreeSet<String> {
    let gated = store_rendered();
    let mut out = BTreeSet::new();
    for (name, promql) in named_rules() {
        if !windows_inside_rate_calls(&promql, &gated).is_empty() {
            out.insert(name);
        }
    }
    out
}

#[test]
fn the_census_reads_the_whole_rule_document() {
    // A reader that stops matching takes every assertion below with it and
    // reports an empty population as agreement. The floor is not the count --
    // that moves every time a rule is added -- it is that the document was
    // walked at all.
    let rules = named_rules();
    assert!(
        rules.len() > 300,
        "the rule document has {} expressions with a name; the chart carried more than 300 the \
         day this floor was written, so the walk has stopped finding them rather than the \
         document having shrunk",
        rules.len()
    );
    assert!(
        store_rendered().len() > 50,
        "the derivation table named {} store-rendered counters; it named more than 50 the day \
         this floor was written",
        store_rendered().len()
    );
}

#[test]
fn a_new_rule_does_not_rate_a_stored_counter_over_a_window_longer_than_a_series_lives() {
    let budget: BTreeSet<&str> = STILL_RATED_OVER_A_LONG_WINDOW.iter().copied().collect();
    let offenders = rules_rated_over_a_long_window();

    let unlisted: Vec<&String> = offenders
        .iter()
        .filter(|name| !budget.contains(name.as_str()))
        .collect();
    assert!(
        unlisted.is_empty(),
        "these rules `increase`/`rate` a counter that is rendered from a store row over a window \
         longer than the series carrying it lives, so the number they print is the growth since \
         this pod started rather than the growth in the window: {unlisted:#?}. Read it as the \
         distance between the high and the low of the leading value across the window instead -- \
         `max_over_time((max by (<labels but pod/instance>) (x))[24h:1m]) - min_over_time(...)` -- \
         and do not add it to the list below, which only ever shrinks. If the rule is right and \
         this reader is wrong, fix the reader: `max_over_time(x[24h])` over a stored counter is a \
         correct read and must not be reported"
    );
}

#[test]
fn the_budget_only_shrinks() {
    let offenders = rules_rated_over_a_long_window();
    let already_fixed: Vec<&str> = STILL_RATED_OVER_A_LONG_WINDOW
        .iter()
        .copied()
        .filter(|name| !offenders.contains(*name))
        .collect();
    assert!(
        already_fixed.is_empty(),
        "these names are still on the list of rules that rate a stored counter over a long \
         window, but they no longer do -- either the rule was converted and its name was left \
         behind, or the rule was renamed or removed without the list following it: \
         {already_fixed:#?}. Take them off: the list is a record of what is still wrong, and one \
         that keeps naming fixed rules stops being readable as debt"
    );
}
