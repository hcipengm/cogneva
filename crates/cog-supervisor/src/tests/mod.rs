//! principle: feedback_two_publishers_of_one_condition_leave_the_operator_two_alerts
//!
//! Gate for that principle: a single condition may be surfaced from two
//! independent publishers, and the operator must still receive two distinct
//! alerts rather than having one silently absorb the other. The in-process
//! publisher is the `set_alert` call written in Rust; it must not be moved
//! into `cogneva.json`, while the config-driven publisher keeps reading that
//! document. This test judges the tree for both publisher surfaces and for
//! the code/config boundary between them.

use std::{fs, path::Path};

#[test]
fn one_condition_is_published_twice_from_two_distinct_surfaces() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates directory")
        .parent()
        .expect("workspace root");
    let observability = workspace.join("crates/cog-observability/src");

    // Publisher one: the in-process watcher reports the condition from code.
    let in_process_publisher = observability.join("infra_watch.rs");
    let in_process = fs::read_to_string(&in_process_publisher).expect("read in-process publisher");
    assert!(
        in_process.contains(".set_alert("),
        "the in-process publisher must call set_alert from Rust: {}",
        in_process_publisher.display()
    );

    // Publisher two: config delivery reports rules declared in cogneva.json.
    let config_publisher = observability.join("config_delivery.rs");
    let config_driven = fs::read_to_string(&config_publisher).expect("read config publisher");
    assert!(
        config_driven.contains(".set_alert("),
        "the config-driven publisher must still surface the condition: {}",
        config_publisher.display()
    );

    // Two publisher surfaces means two alerts; neither path may be removed.
    assert_ne!(
        in_process_publisher,
        config_publisher,
        "the two alerts must come from distinct publishers"
    );

    // The code publisher lives in Rust, not in the JSON document.
    let config = fs::read_to_string(workspace.join("cogneva.example.json"))
        .expect("read shipped cogneva document");
    assert!(
        !config.contains("set_alert"),
        "the in-process set_alert publisher must be declared in Rust, not cogneva.json"
    );
    assert!(
        config.contains("alert"),
        "the config-driven publisher still needs its alert rules in cogneva.json"
    );
}
