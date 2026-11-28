//! The live reading the scale row still owes: how much the executor can carry
//! right now, and whether that covers the work that is holding or waiting.
//!
//! The capacity is two readings multiplied inside the scaler -- the slots one
//! replica measured for itself out of its own cgroup, and the replica target
//! the queue bought. Both are published (`cogneva_workload_lane_capacity` and
//! `cogneva_workload_lane_slots`, next to the queue they came from), so a
//! cluster answers this in one scrape. What no fixture can answer is whether a
//! real deployment scales wide enough to cover its queue, which is the part of
//! the scale row that is still open: this probe asks the live system and fails
//! on the shapes that would make the reading lie.
//!
//! - No capacity cell at all: nothing was measured this round. That is not
//!   "capacity is zero" -- it is a seed, or a loop that has not read the queue
//!   yet, and this probe exists to tell those apart from an answer.
//! - `replica_lane_readings` at zero while other cells exist: something
//!   published numbers without a replica behind them.
//! - Capacity below holding plus waiting: the law did not cover its own queue.
//! - Capacity that is not slots-per-replica times the derived target: the
//!   multiplication and the scale decision came from different inputs.
//!
//! Run it by hand against a live cluster:
//!
//! ```text
//! COGNEVA_METRICS_URLS=http://<app-pod>:8080/metrics \
//!   cargo test -p cogneva --test scale_capacity_reading -- --ignored --nocapture
//! ```
//!
//! The URL is the process that runs the workload loop: it calls kubectl and
//! sizes objects, so it is the app pod. From a host, port-forward it first. A
//! missing variable fails rather than skips -- an `#[ignore]` run is a request
//! for a reading, and a run that measured nothing must not look like an answer.

use std::collections::BTreeMap;
use std::process::Command;

use cog_reflection::workload_resources::{
    LANE_CAPACITY_METRIC, LANE_HOLDING_METRIC, LANE_SLOTS_METRIC, LANE_WAITING_METRIC,
    REPLICAS_METRIC, REPLICA_LANE_READINGS_METRIC, SOURCE_DERIVED, SOURCE_LABEL, WORKLOAD_LABEL,
};

/// One sample line: the family, its labels, its value.
struct Sample {
    metric: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

fn urls() -> Vec<String> {
    let raw = std::env::var("COGNEVA_METRICS_URLS").unwrap_or_else(|_| {
        panic!(
            "COGNEVA_METRICS_URLS is unset: give the /metrics URL of the process that runs \
             the workload loop (comma separated for more than one). This probe measures a live \
             deployment; it has nothing to say without one."
        )
    });
    let urls: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(str::to_string)
        .collect();
    assert!(!urls.is_empty(), "COGNEVA_METRICS_URLS names no URL");
    urls
}

fn scrape(url: &str) -> String {
    let output = Command::new("curl")
        .args(["-fsS", "--max-time", "10", url])
        .output()
        .unwrap_or_else(|e| panic!("cannot run curl for {url}: {e}"));
    assert!(
        output.status.success(),
        "curl {url} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Read the exposition into samples, keeping the label text verbatim.
fn parse(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
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
        let (metric, labels) = match head.find('{') {
            Some(at) => (head[..at].to_string(), parse_labels(&head[at..])),
            None => (head.to_string(), BTreeMap::new()),
        };
        out.push(Sample {
            metric,
            labels,
            value,
        });
    }
    out
}

/// `{a="1",b="2"}` into its pairs. The values here never carry a comma.
fn parse_labels(text: &str) -> BTreeMap<String, String> {
    let inner = text
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}')
        .trim();
    inner
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(name, value)| {
            (
                name.trim().to_string(),
                value.trim().trim_matches('"').to_string(),
            )
        })
        .collect()
}

/// One metric family, keyed by its workload label.
///
/// The same cell can appear on more than one process; the first one wins,
/// because these are cluster facts published by the loop that read the cluster
/// and a second copy is the same answer, not a second opinion.
fn by_workload(samples: &[Sample], metric: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for sample in samples.iter().filter(|sample| sample.metric == metric) {
        let Some(workload) = sample.labels.get(WORKLOAD_LABEL) else {
            continue;
        };
        out.entry(workload.clone()).or_insert(sample.value);
    }
    out
}

/// The replica target the queue bought, per workload. Only the derived one
/// counts: a seed is the number that was there before any measurement.
fn derived_targets(samples: &[Sample]) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    for sample in samples
        .iter()
        .filter(|sample| sample.metric == REPLICAS_METRIC)
    {
        if sample.labels.get(SOURCE_LABEL).map(String::as_str) != Some(SOURCE_DERIVED) {
            continue;
        }
        let Some(workload) = sample.labels.get(WORKLOAD_LABEL) else {
            continue;
        };
        out.insert(workload.clone(), sample.value);
    }
    out
}

#[test]
#[ignore = "needs a live cluster's /metrics via COGNEVA_METRICS_URLS"]
fn the_platform_carries_at_least_the_work_that_is_holding_or_waiting() {
    let text = urls()
        .iter()
        .map(|url| scrape(url))
        .collect::<Vec<_>>()
        .join("\n");
    let (report, complaints) = judge(&text);
    for line in &report {
        println!("{line}");
    }
    assert!(
        complaints.is_empty(),
        "the live capacity reading does not hold:\n{}",
        complaints.join("\n")
    );
}

/// Read one exposition and judge it: one report line per workload, and every
/// way the numbers do not hold together.
///
/// Split out of the live probe so the judgments themselves have a fixture: an
/// ignored test that only ever runs against a cluster is a judgment nobody has
/// seen fail.
fn judge(text: &str) -> (Vec<String>, Vec<String>) {
    let samples = parse(text);
    let capacity = by_workload(&samples, LANE_CAPACITY_METRIC);
    let answered = by_workload(&samples, REPLICA_LANE_READINGS_METRIC);
    let holding = by_workload(&samples, LANE_HOLDING_METRIC);
    let waiting = by_workload(&samples, LANE_WAITING_METRIC);
    let slots = by_workload(&samples, LANE_SLOTS_METRIC);
    let targets = derived_targets(&samples);

    let mut report = Vec::new();
    let mut complaints = Vec::new();
    if capacity.is_empty() {
        // 没有这一格不是「容量为零」：这一轮没量到，和量到零是两件事。
        complaints.push(format!(
            "no {} cell on any scraped process: this round measured nothing. Answered replicas \
             ({:?}) tell the two apart -- 0 there means the loop read no queue yet, absent \
             means the loop has not run. Neither is a capacity of zero.",
            LANE_CAPACITY_METRIC, answered
        ));
        return (report, complaints);
    }
    for (workload, capacity) in &capacity {
        let answered = answered.get(workload).copied().unwrap_or(0.0);
        let holding = holding.get(workload).copied().unwrap_or(0.0);
        let waiting = waiting.get(workload).copied().unwrap_or(0.0);
        let slots = slots.get(workload).copied().unwrap_or(0.0);
        let target = targets.get(workload).copied();
        report.push(format!(
            "{workload}: capacity={capacity} slots={slots} holding={holding} \
             waiting={waiting} answered={answered} derived_replicas={target:?}"
        ));

        if answered <= 0.0 {
            complaints.push(format!(
                "{workload}: a capacity of {capacity} with no replica answering -- the number \
                 has no measurement behind it"
            ));
        }
        if slots <= 0.0 {
            complaints.push(format!(
                "{workload}: a capacity of {capacity} with zero slots summed over the \
                 answering replicas"
            ));
        }
        if *capacity < holding + waiting {
            complaints.push(format!(
                "{workload}: capacity {capacity} does not cover {holding} holding + {waiting} \
                 waiting -- the law left work in the queue it was supposed to place"
            ));
        }
        match target {
            Some(replicas) if replicas > 0.0 => {
                let per_replica = capacity / replicas;
                if per_replica.fract() != 0.0 || per_replica <= 0.0 {
                    complaints.push(format!(
                        "{workload}: capacity {capacity} is not a whole number of slots per \
                         replica at the derived target of {replicas} -- the product and the \
                         scale decision came from different inputs"
                    ));
                }
            }
            Some(_) => complaints.push(format!("{workload}: a derived replica target of zero")),
            None => complaints.push(format!(
                "{workload}: a capacity without a derived replica target -- the capacity was \
                 not published against a target the queue bought"
            )),
        }
    }
    (report, complaints)
}

/// A capacity of one replica's slots times two replicas covers its queue.
const HEALTHY: &str = "\
# TYPE cogneva_workload_replicas gauge
cogneva_workload_replicas{workload=\"sandbox-executor\",source=\"derived\"} 2
cogneva_workload_replica_lane_readings{workload=\"sandbox-executor\"} 2
cogneva_workload_lane_holding{workload=\"sandbox-executor\"} 3
cogneva_workload_lane_waiting{workload=\"sandbox-executor\"} 1
cogneva_workload_lane_slots{workload=\"sandbox-executor\"} 8
cogneva_workload_lane_capacity{workload=\"sandbox-executor\"} 8
";

/// 判词在一份好读数上不开口，在一份坏读数上点出是哪一条不成立。
///
/// 这只探针只在有集群时跑；没有这几条夹具，「它会在哪里红」就没人见过。
#[test]
fn the_judgment_names_each_way_the_capacity_would_lie() {
    let (report, complaints) = judge(HEALTHY);
    assert_eq!(complaints, Vec::<String>::new(), "好读数不该被判词点名");
    assert_eq!(report.len(), 1);
    assert!(report[0].contains("capacity=8"), "{}", report[0]);

    // 需求超过了承载量：八条位子装不下十份在跑与在等的。
    let short = HEALTHY
        .replace(
            "lane_holding{workload=\"sandbox-executor\"} 3",
            "lane_holding{workload=\"sandbox-executor\"} 6",
        )
        .replace(
            "lane_waiting{workload=\"sandbox-executor\"} 1",
            "lane_waiting{workload=\"sandbox-executor\"} 4",
        );
    let (_, complaints) = judge(&short);
    assert!(
        complaints.iter().any(|c| c.contains("does not cover")),
        "{complaints:?}"
    );

    // 这一轮没量到：只有「答上话的副本是 0」，没有容量那一格。
    let nothing = "\
cogneva_workload_replica_lane_readings{workload=\"sandbox-executor\"} 0
";
    let (report, complaints) = judge(nothing);
    assert!(report.is_empty());
    assert!(
        complaints.iter().any(|c| c.contains("measured nothing")),
        "{complaints:?}"
    );

    // 乘积与扩缩用的目标对不上：九条位子不是两副整数条。
    let ragged = HEALTHY.replace(
        "cogneva_workload_lane_capacity{workload=\"sandbox-executor\"} 8",
        "cogneva_workload_lane_capacity{workload=\"sandbox-executor\"} 9",
    );
    let (_, complaints) = judge(&ragged);
    assert!(
        complaints.iter().any(|c| c.contains("not a whole number")),
        "{complaints:?}"
    );

    // 容量在、目标不是队买来的（只有种子）：这一格不该被当成读数。
    let seeded = HEALTHY.replace("source=\"derived\"", "source=\"seed\"");
    let (_, complaints) = judge(&seeded);
    assert!(
        complaints
            .iter()
            .any(|c| c.contains("without a derived replica target")),
        "{complaints:?}"
    );
}
