//! The live probe for `sum` over a store-derived series.
//!
//! The tree gate judges the artefact: a query may not aggregate a series the
//! registry calls store-derived. It cannot judge the registry itself, because a
//! series can join that set later — a producer starts rendering a store row, a
//! pod is added, a new process copies the same fact — and the artefact keeps
//! saying what it said. This test is the reader for that direction: it asks the
//! running processes what they report and computes, for every declared series,
//! the ratio `sum(X) / max(X)` across them. Every process rendering the same
//! fact gives 1; two copies give 2; and a window adds the last samples of dead
//! pods on top of the live ones, so the ratio can be larger still.
//!
//! It also reports the inverse direction: a counter family all processes report
//! byte-identically that nobody has declared. That is what a new store-derived
//! series looks like from outside, and the census that seeded the registry found
//! 53 of them.
//!
//! Run it by hand against a live cluster:
//!
//! ```text
//! COGNEVA_METRICS_URLS=http://<pod-a>:8080/metrics,http://<pod-b>:8080/metrics \
//!   cargo test -p cogneva --test store_derived_reading -- --ignored --nocapture
//! ```
//!
//! Inside the cluster the two usual addresses are the app and evolution pods; from
//! a host, port-forward both first. The test fails rather than skipping when the
//! variable is missing, because an `#[ignore]` run is a request for a reading and
//! a run that measured nothing must not look like a run that measured 1.0.

#[path = "common/derivation.rs"]
mod derivation;

use std::collections::BTreeMap;
use std::process::Command;

/// Metric name to its rendered value, per process.
type Readings = BTreeMap<String, BTreeMap<String, f64>>;

fn urls() -> Vec<String> {
    let raw = std::env::var("COGNEVA_METRICS_URLS").unwrap_or_else(|_| {
        panic!(
            "COGNEVA_METRICS_URLS is unset: give the /metrics URLs of at least two \
             processes, comma separated. This probe measures a live system; it has \
             nothing to say without one."
        )
    });
    let urls: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect();
    assert!(
        urls.len() >= 2,
        "the probe compares processes; it needs two URLs, got {}",
        urls.len()
    );
    urls
}

fn scrape(url: &str) -> Readings {
    let output = Command::new("curl")
        .args(["-fsS", "--max-time", "10", url])
        .output()
        .unwrap_or_else(|e| panic!("cannot run curl for {url}: {e}"));
    assert!(
        output.status.success(),
        "curl {url} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse(&String::from_utf8_lossy(&output.stdout))
}

/// Parse the exposition into `name -> (labels -> value)`.
///
/// Only sample lines are read. The label text is kept verbatim so that the same
/// label set on two processes compares as the same cell; a killed pod's series
/// differ in `pod`, which is what makes them extra cells rather than a second
/// opinion on the same one.
fn parse(text: &str) -> Readings {
    let mut out: Readings = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((head, value)) = line.rsplit_once(' ') else {
            continue;
        };
        let Ok(value) = value.parse::<f64>() else {
            continue;
        };
        let (name, labels) = match head.find('{') {
            Some(at) => (head[..at].to_string(), head[at..].to_string()),
            None => (head.to_string(), String::new()),
        };
        out.entry(name).or_default().insert(labels, value);
    }
    out
}

#[test]
#[ignore = "needs two live /metrics URLs via COGNEVA_METRICS_URLS"]
fn the_registry_agrees_with_what_the_processes_report() {
    let urls = urls();
    let processes: Vec<Readings> = urls.iter().map(|url| scrape(url)).collect();

    let mut complaints: Vec<String> = Vec::new();
    let mut measured: Vec<String> = Vec::new();

    for (name, kind) in derivation::METRIC_DERIVATIONS {
        let Some(first) = processes[0].get(*name) else {
            continue;
        };
        let identical_everywhere = processes
            .iter()
            .all(|process| process.get(*name).is_some_and(|cells| cells == first));
        let nonzero = first.values().any(|value| *value >= 10.0);
        let per_process: Vec<f64> = processes
            .iter()
            .map(|process| {
                process
                    .get(*name)
                    .map(|cells| cells.values().sum())
                    .unwrap_or(0.0)
            })
            .collect();
        let summed: f64 = per_process.iter().sum();
        let largest = per_process.iter().cloned().fold(0.0, f64::max);
        let ratio = if largest == 0.0 {
            0.0
        } else {
            summed / largest
        };

        match kind {
            // The copies exist — that is what the classification says — so the
            // ratio above 1 is the measured size of the mistake a `sum` makes.
            // It is reported as evidence, not as a failure: the tree gate turns
            // it into the prohibition, and the number is what a reader needs to
            // know a sum would have been wrong.
            derivation::MetricDerivation::Store => {
                if nonzero {
                    measured.push(format!(
                        "{name}: sum={summed} max={largest} ratio={ratio:.3} \
                         (any aggregation over it reads the fact {ratio:.3} times)"
                    ));
                }
            }
            // Declared per-process, but every process reports the same
            // non-zero cells: the registry has the dangerous half of the
            // distinction backwards, and every rule that sums it is wrong.
            derivation::MetricDerivation::Process
                if identical_everywhere && nonzero && ratio > 1.5 =>
            {
                complaints.push(format!(
                    "{name} is declared process-local but every process reports the same \
                     cells ({ratio:.1} copies of one total): it is store-derived and any \
                     sum of it multiplies the fact"
                ));
            }
            derivation::MetricDerivation::Process => {}
        }
    }

    // The inverse direction: a counter every process reports identically that
    // nobody has classified is a store-derived series the registry has not met.
    for (name, cells) in &processes[0] {
        if !name.ends_with("_total") {
            continue;
        }
        let same_everywhere = processes
            .iter()
            .all(|process| process.get(name).is_some_and(|other| other == cells));
        if !same_everywhere {
            continue;
        }
        if cells.values().all(|value| *value < 10.0) {
            // Coincidence is cheap at small numbers and an unclassified series
            // is not a defect by itself; a large identical total is.
            continue;
        }
        if derivation::metric_derivation(name).is_none() {
            complaints.push(format!(
                "{name} is reported byte-identically by every process and is not declared: \
                 add it to METRIC_DERIVATIONS"
            ));
        }
    }

    println!(
        "measured {} declared store-derived series across {} processes",
        measured.len(),
        processes.len()
    );
    for line in &measured {
        println!("  {line}");
    }
    assert!(
        complaints.is_empty(),
        "读数面与登记不符:\n{}",
        complaints.join("\n")
    );
}
