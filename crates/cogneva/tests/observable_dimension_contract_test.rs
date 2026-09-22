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
//! What the scrape may ask is now the producer's answer, not a global list: an
//! observable declares each dimension it branches on as bounded (its series do
//! not grow with traffic, so a periodic scrape can afford them) or unbounded
//! (the series are keyed per object, so the scrape would grow without bound).
//! The configured set narrows that; it cannot widen it. Boundedness has to live
//! on the producer because the same name means different things on different
//! faces — D1 is a process-wide counter set on the orchestrator and a per-step
//! record keyed by `task_id` on the agent — so one global list is guaranteed to
//! be wrong about one of them.
//!
//! That is why this is a build-time table rather than a runtime check. The
//! tables below are the contract:
//!
//! - a `(file, dimension)` pair a producer declares must be read by the scrape
//!   (bounded, and admitted by the configured set) or by a `CONSUMERS` entry,
//!   or appear in `UNREAD` with the reason it is deliberately left unread;
//! - an `UNREAD` entry must still be declared by the producer it names, and
//!   must still be something the scrape does not read, so a superseded
//!   exemption is deleted instead of quietly licensing the next gap;
//! - a `PRODUCERS` entry must still be declared in the file it names, with the
//!   boundedness the table claims, so renaming, deleting, or reclassifying an
//!   observable fails here rather than leaving a table that describes a past
//!   layout.
//!
//! Unclassified fails. The failure mode here is silence, and silence is what a
//! forgotten classification looks like.

use std::collections::BTreeSet;
use std::path::PathBuf;

const CHART_CONFIG: &str = "deploy/helm/cogneva/files/cogneva.json";

/// Observable sources, and each dimension that source declares with whether it
/// declares the dimension's series bounded. The file is read and searched for
/// every pair, so an entry whose declaration moved, vanished, or flipped its
/// boundedness fails the test.
const PRODUCERS: &[(&str, &[(&str, bool)])] = &[
    (
        "crates/cog-agent/src/observable.rs",
        &[("D1", false), ("D2", false), ("D3", false)],
    ),
    (
        "crates/cog-collaboration/src/observable.rs",
        &[("D8", true)],
    ),
    ("crates/cog-guardrail/src/observable.rs", &[("D6", true)]),
    ("crates/cog-llm/src/observable.rs", &[("D9", true)]),
    ("crates/cog-memory/src/observable.rs", &[("D4", true)]),
    (
        "crates/cog-observability/src/observable.rs",
        &[("D5", true)],
    ),
    (
        "crates/cog-orchestrator/src/observable.rs",
        &[("D1", true), ("D8", true)],
    ),
];

/// Dimensions read somewhere other than the scrape endpoint, with the file
/// that asks for them. The scrape's own set comes from the chart config.
const CONSUMERS: &[(&str, &[&str])] = &[
    ("crates/cog-eval/src/report.rs", &["D5", "D8", "D9"]),
    ("crates/cog-gateway/src/evolution.rs", &["D5"]),
];

/// Producer/dimension pairs the scrape will not ask for, and why that is
/// deliberate rather than a gap. Keyed by producer because the same name can be
/// scraped on one face and unreadable on another.
const UNREAD: &[(&str, &str, &str)] = &[
    (
        "crates/cog-agent/src/observable.rs",
        "D1",
        "the agent observable records a series per step keyed by task_id, so its cardinality is unbounded",
    ),
    (
        "crates/cog-agent/src/observable.rs",
        "D2",
        "the agent observable records a series per step keyed by task_id, so its cardinality is unbounded",
    ),
    (
        "crates/cog-agent/src/observable.rs",
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

/// The narrowing the scrape endpoint is configured with, as deployed. Empty
/// means no narrowing: every bounded dimension every producer declares.
fn configured_narrowing() -> Vec<String> {
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

/// Whether the deployed scrape asks a given producer for a given dimension.
fn scrapes(dimension: &str, bounded: bool) -> bool {
    let narrowing = configured_narrowing();
    bounded && (narrowing.is_empty() || narrowing.iter().any(|d| d == dimension))
}

fn declared() -> BTreeSet<String> {
    PRODUCERS
        .iter()
        .flat_map(|(_, dims)| dims.iter().map(|(d, _)| d.to_string()))
        .collect()
}

#[test]
fn every_declared_dimension_has_a_reader_or_a_reason_it_does_not() {
    let read_elsewhere: BTreeSet<String> = CONSUMERS
        .iter()
        .flat_map(|(_, dims)| dims.iter().map(|d| d.to_string()))
        .collect();
    let exempted: BTreeSet<(&str, &str)> = UNREAD.iter().map(|(f, d, _)| (*f, *d)).collect();

    let mut orphans: Vec<String> = Vec::new();
    for (file, dims) in PRODUCERS {
        for (dim, bounded) in *dims {
            if scrapes(dim, *bounded)
                || read_elsewhere.contains(*dim)
                || exempted.contains(&(*file, dim))
            {
                continue;
            }
            let why = if *bounded {
                "the configured narrowing excludes it and no other consumer asks for it"
            } else {
                "the producer declares its series unbounded, so the scrape must not ask"
            };
            orphans.push(format!("{file}:{dim} ({why})"));
        }
    }

    assert!(
        orphans.is_empty(),
        "these dimensions are answered by an observable but read by nobody, so their metrics \
         are counted and exported to no endpoint: {orphans:?}. Ask for them from a consumer, or \
         record them in UNREAD with the reason they cannot be read."
    );
}

#[test]
fn an_unread_entry_is_still_declared_and_still_unread() {
    let mut problems: Vec<String> = Vec::new();
    for (file, dim, _) in UNREAD {
        let claimed = PRODUCERS
            .iter()
            .find(|(f, _)| f == file)
            .and_then(|(_, dims)| dims.iter().find(|(d, _)| d == dim));
        match claimed {
            None => problems.push(format!(
                "{file}:{dim} is exempted but that producer no longer declares it, so the entry \
                 exempts nothing and would hide a future gap"
            )),
            Some((_, bounded)) if scrapes(dim, *bounded) => problems.push(format!(
                "{file}:{dim} is exempted but the scrape already reads it, so the entry no longer \
                 describes why anything is left out"
            )),
            Some(_) => {}
        }
    }

    assert!(problems.is_empty(), "{}", problems.join("; "));
}

#[test]
fn a_recorded_producer_still_declares_what_the_table_claims() {
    for (file, dims) in PRODUCERS {
        let source = read(file);
        for (dim, bounded) in *dims {
            let constructor = if *bounded { "::bounded" } else { "::unbounded" };
            assert!(
                source.contains(&format!("{constructor}(\"{dim}\")")),
                "{file} no longer declares {dim} as {constructor}, but the contract table still \
                 records it that way — the producer was renamed, moved, or reclassified and this \
                 entry now describes a past layout"
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
    let unknown: Vec<String> = configured_narrowing()
        .into_iter()
        .filter(|d| !declared.contains(d))
        .collect();

    assert!(
        unknown.is_empty(),
        "metrics.scrape_dimensions names dimensions no observable declares, so the scrape spends \
         a round asking for metrics that cannot exist: {unknown:?}"
    );
}
