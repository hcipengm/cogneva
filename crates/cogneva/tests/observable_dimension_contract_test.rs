//! Every dimension an observable answers must be one something reads.
//!
//! An observable branches on the dimension string it is handed: ask for a
//! dimension it branches on and it returns metrics, ask for any other and it
//! returns an empty vector with no error. So a dimension that is declared by a
//! producer but asked by no consumer is not a missing metric — the counter is
//! incremented on every check, the observable is registered, and the metric
//! name exists in the code. It simply has no reader. Nothing logs, nothing
//! fails, and no series is ever emitted for a "missing series" rule to trip on.
//!
//! That is why this is a build-time table rather than a runtime check. The
//! tables below are the contract:
//!
//! - a dimension listed in `PRODUCERS` must be read by the scrape endpoint
//!   (the configured dimension set in the chart) or by a `CONSUMERS` entry, or
//!   appear in `UNREAD` with the reason it is deliberately left unread;
//! - an `UNREAD` entry must still name a dimension some observable declares,
//!   so a superseded exemption is deleted instead of quietly licensing the
//!   next gap;
//! - a `PRODUCERS` entry must still be declared in the file it names, so
//!   renaming or deleting an observable fails here rather than leaving a table
//!   that describes a past layout.
//!
//! Unclassified fails. The failure mode here is silence, and silence is what a
//! forgotten classification looks like.

use std::collections::BTreeSet;
use std::path::PathBuf;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// Observable sources and the dimensions each one answers, with the source
/// file that declares them. The file is read and searched for every dimension,
/// so an entry whose declaration moved or vanished fails the test.
const PRODUCERS: &[(&str, &[&str])] = &[
    ("crates/cog-agent/src/observable.rs", &["D1", "D2", "D3"]),
    ("crates/cog-collaboration/src/observable.rs", &["D8"]),
    ("crates/cog-guardrail/src/observable.rs", &["D6"]),
    ("crates/cog-llm/src/observable.rs", &["D9"]),
    ("crates/cog-memory/src/observable.rs", &["D4"]),
    ("crates/cog-observability/src/observable.rs", &["D5"]),
    ("crates/cog-orchestrator/src/observable.rs", &["D1", "D8"]),
];

/// Dimensions read somewhere other than the scrape endpoint, with the file
/// that asks for them. The scrape's own set comes from the chart config.
const CONSUMERS: &[(&str, &[&str])] = &[
    ("crates/cog-eval/src/report.rs", &["D5", "D8", "D9"]),
    ("crates/cog-gateway/src/evolution.rs", &["D5"]),
];

/// Dimensions no consumer asks, and why that is deliberate rather than a gap.
const UNREAD: &[(&str, &str)] = &[
    (
        "D1",
        "the agent observable records a series per step keyed by task_id, so its cardinality is unbounded",
    ),
    (
        "D2",
        "the agent observable records a series per step keyed by task_id, so its cardinality is unbounded",
    ),
    (
        "D3",
        "the agent observable records a series per step keyed by task_id, so its cardinality is unbounded",
    ),
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    let path = repo_root().join(rel);
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("unreadable at {}: {e}", path.display()))
}

/// The dimension set the scrape endpoint asks for, as deployed.
fn scrape_dimensions() -> Vec<String> {
    let value: serde_json::Value =
        serde_json::from_str(&read(CHART_CONFIG)).expect("chart config is not valid JSON");
    value["metrics"]["scrape_dimensions"]
        .as_array()
        .unwrap_or_else(|| panic!("{CHART_CONFIG} has no metrics.scrape_dimensions array"))
        .iter()
        .map(|d| {
            d.as_str()
                .expect("scrape_dimensions entries must be strings")
                .to_string()
        })
        .collect()
}

fn declared() -> BTreeSet<String> {
    PRODUCERS
        .iter()
        .flat_map(|(_, dims)| dims.iter().map(|d| d.to_string()))
        .collect()
}

#[test]
fn every_declared_dimension_has_a_reader_or_a_reason_it_does_not() {
    let mut read: BTreeSet<String> = scrape_dimensions().into_iter().collect();
    for (_, dims) in CONSUMERS {
        read.extend(dims.iter().map(|d| d.to_string()));
    }
    let unread: BTreeSet<&str> = UNREAD.iter().map(|(d, _)| *d).collect();

    let orphans: Vec<String> = declared()
        .into_iter()
        .filter(|d| !read.contains(d) && !unread.contains(d.as_str()))
        .collect();

    assert!(
        orphans.is_empty(),
        "these dimensions are answered by an observable but read by nobody, so their metrics \
         are counted and exported to no endpoint: {orphans:?}. Ask for them from a consumer, or \
         record them in UNREAD with the reason they cannot be read."
    );
}

#[test]
fn an_unread_entry_still_names_a_dimension_something_declares() {
    let declared = declared();
    let stale: Vec<&str> = UNREAD
        .iter()
        .map(|(d, _)| *d)
        .filter(|d| !declared.contains(*d))
        .collect();

    assert!(
        stale.is_empty(),
        "these UNREAD entries no longer match any observable's declared dimensions, so they \
         exempt nothing and would hide a future gap: {stale:?}"
    );
}

#[test]
fn a_recorded_producer_still_declares_what_the_table_claims() {
    for (file, dims) in PRODUCERS {
        let source = read(file);
        for dim in *dims {
            assert!(
                source.contains(dim),
                "{file} no longer mentions {dim}, but the contract table still records it as \
                 declared there — the producer was renamed or moved and this entry now describes \
                 a past layout"
            );
        }
    }
}

#[test]
fn a_recorded_consumer_still_asks_what_the_table_claims() {
    for (file, dims) in CONSUMERS {
        let source = read(file);
        for dim in *dims {
            assert!(
                source.contains(dim),
                "{file} no longer mentions {dim}, but the contract table still records it as \
                 asked there — the consumer was changed and the table now overstates what is read"
            );
        }
    }
}

#[test]
fn the_configured_scrape_set_asks_only_dimensions_that_exist() {
    let declared = declared();
    let unknown: Vec<String> = scrape_dimensions()
        .into_iter()
        .filter(|d| !declared.contains(d))
        .collect();

    assert!(
        unknown.is_empty(),
        "metrics.scrape_dimensions names dimensions no observable declares, so the scrape spends \
         a round asking for metrics that cannot exist: {unknown:?}"
    );
}
