//! principle: feedback_no_hardcoding_config_surface
//! The volume readings are only useful once something joins them. Two emitters
//! publish series per claim — the footprint walker and the capacity loop — and
//! the deployed rules divide one by the other joined on
//! `on(namespace, persistentvolumeclaim)`. That join is a string in a PromQL
//! expression and a string in Rust, and nothing compares them.
//!
//! A join that matches nothing produces an empty series. The rule then sees no
//! over-budget claim, which is the same reading as a cluster where every claim
//! is comfortably sized — so a renamed label hides an overrun instead of
//! raising it.
//!
//! This test draws the boundary on the other side: every label the code emits a
//! series under must be one the shipped rules join on. Both emitters are
//! checked, so a second declaration cannot pass by agreeing with the first
//! while disagreeing with the expression that consumes it.

use std::path::PathBuf;

fn rules_document() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/helm/cogneva/files/cogneva.json");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

#[test]
fn every_emitter_labels_the_claim_the_way_the_deployed_rules_join_it() {
    let rules = rules_document();
    let emitters = [
        (
            "cog_core::claim_footprint",
            cog_core::claim_footprint::CLAIM_LABEL,
        ),
        (
            "cog_reflection::volume_capacity",
            cog_reflection::volume_capacity::CLAIM_LABEL,
        ),
    ];
    for (who, label) in emitters {
        let join = format!("on(namespace, {label})");
        assert!(
            rules.contains(&join),
            "{who} 按 `{label}` 发读数，部署里的规则却没有按 `{join}` 连接；\
             对不上时除出来的是一串空值，读起来像「没有卷超预算」"
        );
    }
}
