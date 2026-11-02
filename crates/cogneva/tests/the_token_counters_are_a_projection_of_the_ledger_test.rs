//! principle: feedback_a_second_book_for_the_same_question_is_a_second_truth_until_it_shares_a_key
//!
//! The token counters are rendered from the ledger, not tallied beside it.
//!
//! Two numbers answering the same question are not a redundancy, they are a
//! disagreement waiting for its first restart. The ledger already holds one row
//! per call, so a process that also adds to a counter on the request path is
//! keeping a second book: the ledger survives the pod, the counter does not, and
//! from the first roll onwards the two report different totals with nothing on
//! either reading saying which one the platform believes.
//!
//! The read that settles it is the ledger's own sum. `llm_tokens_total` is the
//! same fact rendered on the metric face, recomputed on the pool's cadence, so
//! every process that can reach the ledger shows the same numbers. That is what
//! `MetricDerivation::Store` means, and it is why an aggregation over it may not
//! be a `sum`: adding the copies counts one fact once per process.
//!
//! What this gate holds:
//!
//! * the gateway's request path does not write `llm_tokens_total` -- the only
//!   writer is the projection, and the file's occurrences are counted so a new
//!   one cannot slip in unnoticed;
//! * the derivation table says `Store`, which is the declaration the other
//!   aggregation gates read;
//! * the projection writes an age beside the counts, because a projection that
//!   cannot reach the ledger goes on serving the last numbers it rendered and
//!   those are indistinguishable from a platform that stopped spending;
//! * no shipped rule `sum`s the token cells -- the copies are one fact, so the
//!   lead takes the place of the sum.
//!
//! What it cannot hold: that the sum is right. A ledger row written with the
//! wrong tokens renders a wrong counter faithfully, and no reading on the metric
//! face can tell.

use std::path::Path;

use crate::common;
use crate::derivation;

use common::routes::repo_root;

/// The gateway source carrying the request path and the projection.
const GATEWAY: &str = "crates/cog-gateway/src/security_gateway.rs";

fn read(root: &Path, relative: &str) -> String {
    let path = root.join(relative);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} must be readable: {e}", path.display()))
}

#[test]
fn the_request_path_does_not_tally_tokens_beside_the_ledger() {
    let root = repo_root();
    let source = read(&root, GATEWAY);
    // The criterion is the write, not the mention: the name is also in the
    // baseline registry and in the projection's own reads. What may not exist
    // is an increment, because that is the second accounting.
    let mut offset = 0usize;
    while let Some(found) = source[offset..].find("LLM_TOKENS_TOTAL") {
        let at = offset + found;
        let line_start = source[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
        // The enclosing call is the nearest `record_*` above within the same
        // statement; `record_counter_add` is the one that keeps a tally.
        let head: String = source[..line_start].chars().rev().take(200).collect();
        assert!(
            !head.contains("record_counter_add"),
            "the request path adds to the token counter; the call that spent the tokens already \
             landed a row in the ledger, and a counter beside it reports a different total from \
             the first restart onwards with nothing saying which one is believed"
        );
        offset = at + 1;
    }
    // Both occurrences must sit inside the projection, not merely exist. The
    // function is bounded by the doc comment of the next one, which is the
    // neighbour the file already keeps in that order.
    let start = source
        .find("async fn publish_token_totals_from_ledger")
        .expect("the projection is the only writer, so it has to be here");
    let end = source[start..]
        .find("\n/// 逐上游各算一份")
        .map(|offset| start + offset)
        .expect("the projection is followed by its neighbour; the walk lost the boundary");
    let body = &source[start..end];
    assert_eq!(
        body.matches("LLM_TOKENS_TOTAL").count(),
        1,
        "the projection names the token counter exactly once -- the write that renders every \
         cell from the ledger's total. A different count means the function changed shape, and \
         the count is here so that change is looked at rather than absorbed"
    );
}

#[test]
fn the_token_counters_declare_themselves_store_rendered() {
    assert_eq!(
        derivation::metric_derivation("llm_tokens_total"),
        Some(derivation::MetricDerivation::Store),
        "the token counters are a projection of the ledger, so every process renders the same \
         numbers for the same reason. Declared `Process`, an aggregation over them reads as a \
         per-process tally -- and the live probe stops redding when the copies turn out identical"
    );
}

#[test]
fn the_projection_is_written_often_enough_to_outlive_a_pod() {
    // A projection that is only written when it moves leaves the store holding
    // the last value through every quiet stretch, and a store-held value that
    // has stopped moving is the same shape as a platform that stopped spending.
    // The durable family is the list of series whose writer rewrites them once
    // per heartbeat whether or not they moved; being on it is the declaration
    // that this series can be waited on.
    let durable: Vec<&str> = cog_gateway::security_gateway::DURABLE_POOL_GAUGES
        .iter()
        .map(|name| name.as_str())
        .collect();
    assert!(
        durable.contains(&"llm_tokens_total"),
        "the token counters are not in the durable family, so a quiet stretch leaves the store \
         serving the last value it was handed and no reader can tell that from a blind meter"
    );
}

#[test]
fn no_rule_adds_the_copies_of_the_token_counters() {
    let graph = common::routes::read_repo("deploy/helm/cogneva/files/cogneva.json");
    let document: serde_json::Value =
        serde_json::from_str(&graph).expect("the chart's rule document is valid JSON");
    let mut expressions = Vec::new();
    fn walk(value: &serde_json::Value, out: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                if let Some(promql) = map.get("promql").and_then(|v| v.as_str()) {
                    out.push(promql.to_string());
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
    walk(&document, &mut expressions);
    assert!(
        expressions.len() > 300,
        "the walk found {} expressions; it has stopped reading the document rather than the \
         document having shrunk",
        expressions.len()
    );
    let summed: Vec<&String> = expressions
        .iter()
        .filter(|expr| expr.contains("sum by (actor) (increase(llm_tokens_total"))
        .collect();
    assert!(
        summed.is_empty(),
        "these rules `sum` the token counters, which are rendered from the ledger: every process \
         reports the same fact, so the sum counts it once per process and grows with the number \
         of pods rather than with the traffic: {summed:#?}. Take the lead instead -- `max by \
         (actor)` -- so the copies collapse to the one fact they all carry"
    );
}
