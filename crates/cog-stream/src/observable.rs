//! The consumers' read health, published as readings.
//!
//! The pair is deliberate: `silent_seconds` says whether the bus is moving,
//! `failures_total` says how much the consumer has had to refuse to get there.
//! A consumer that keeps up by retrying every few seconds reads as healthy in
//! the counter and unremarkable in the silence — and that is the truth about
//! it, while a consumer that has stopped moving shows in the silence no matter
//! how few times it has failed since the last success.
//!
//! `block_seconds` is published with them so the rule's bound is derived from
//! the period this process actually reads at, not from a constant written next
//! to the rule: the two would otherwise drift apart silently, and a bound
//! tighter than the read period fires on a healthy idle consumer.

use std::sync::Arc;

use cog_core::{DimensionSpec, Observable, RawMetric, SFResult, TraceFragment};

use crate::read_health::{now_seconds, ReadHealth, ReadRefusal};

/// Seconds the server has been silent to this consumer (message or empty
/// reply both count as an answer).
pub const READ_SILENT_SECONDS_METRIC: &str = "cogneva_stream_read_silent_seconds";

/// Reads this consumer's loop has had refused since it started, one series per
/// cause (`reason`).
///
/// The label is why this reading can be used at all. `NOGROUP` is answered both
/// by a group that does not exist yet — benign, and the loop's next attempt
/// wins — and by one that is gone for good, which the consumer retries against
/// until the pod dies. A threshold on the pooled number fires on the first and
/// misses the second; read per cause, the pair is decidable against the silence
/// published beside it. A reader that wants the pooled number sums over
/// `reason`, which is what the series held before the label existed.
pub const READ_FAILURES_METRIC: &str = "cogneva_stream_read_failures_total";

/// The label `cogneva_stream_read_failures_total` is broken down by.
pub const READ_FAILURE_REASON_LABEL: &str = "reason";

/// The block period the reads are issued with — the bound below which silence
/// is just an idle stream.
pub const READ_BLOCK_SECONDS_METRIC: &str = "cogneva_stream_read_block_seconds";

/// Publishes every live consumer's read health.
pub struct StreamReadObservable {
    health: Arc<ReadHealth>,
}

impl StreamReadObservable {
    pub fn new(health: Arc<ReadHealth>) -> Self {
        Self { health }
    }
}

#[async_trait::async_trait]
impl Observable for StreamReadObservable {
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let now = now_seconds();
        let mut out = vec![RawMetric::new(
            READ_BLOCK_SECONDS_METRIC,
            self.health.block_seconds(),
        )];
        for ((stream, group), slot) in self.health.snapshot() {
            out.push(
                RawMetric::new(READ_SILENT_SECONDS_METRIC, slot.silent_seconds(now) as f64)
                    .with_label("stream", stream.as_str())
                    .with_label("consumer_group", group.as_str()),
            );
            for refusal in ReadRefusal::ALL {
                out.push(
                    RawMetric::new(READ_FAILURES_METRIC, slot.failures_of(refusal) as f64)
                        .with_label("stream", stream.as_str())
                        .with_label("consumer_group", group.as_str())
                        .with_label(READ_FAILURE_REASON_LABEL, refusal.label()),
                );
            }
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shape the staleness rule reads: one series per consumer, naming the
    /// stream and group it belongs to, and a bound expressed in seconds.
    #[tokio::test]
    async fn every_live_consumer_is_visible_with_the_bound_it_is_read_at() {
        let health = Arc::new(ReadHealth::new(5000));
        let _goals = ReadHealth::track(&health, "goals", "worker");
        health.note_ok("goals", "worker");
        let _inbox = ReadHealth::track(&health, "inbox", "worker");
        let observable = StreamReadObservable::new(Arc::clone(&health));

        let metrics = observable.collect_metrics("").await.unwrap();
        let named = |name: &str, labels: &[(&str, &str)]| {
            metrics.iter().find(|m| {
                m.name == name
                    && labels
                        .iter()
                        .all(|(k, v)| m.labels.get(*k).map(String::as_str) == Some(*v))
            })
        };

        let block = named(READ_BLOCK_SECONDS_METRIC, &[]).expect("the bound is published");
        assert_eq!(block.value, 5.0);
        let goals = named(READ_SILENT_SECONDS_METRIC, &[("stream", "goals")])
            .expect("an answered consumer is published");
        assert!(goals.value <= 1.0, "answered just now: {}", goals.value);
        let inbox = named(READ_SILENT_SECONDS_METRIC, &[("stream", "inbox")])
            .expect("a consumer that never got an answer is published too");
        assert!(
            inbox.value <= 1.0,
            "counted from when it started: {}",
            inbox.value
        );
        assert!(named(READ_FAILURES_METRIC, &[("stream", "goals")]).is_some());
    }

    #[tokio::test]
    async fn a_retired_consumer_leaves_the_scrape() {
        let health = Arc::new(ReadHealth::new(5000));
        let guard = ReadHealth::track(&health, "goals", "worker");
        let observable = StreamReadObservable::new(Arc::clone(&health));
        assert_eq!(
            observable.collect_metrics("").await.unwrap().len(),
            4,
            "the bound, silence, and one failure series per cause for the one \
             live consumer"
        );
        drop(guard);
        let metrics = observable.collect_metrics("").await.unwrap();
        assert_eq!(
            metrics.len(),
            1,
            "only the bound is left once the consumer is gone: {metrics:?}"
        );
    }

    /// The pair `silent_seconds` / `failures_total` is only decidable when the
    /// counter says which of the two `NOGROUP` cases it is showing, and a
    /// consumer that has refused nothing is published at zero for both causes
    /// rather than absent — an absent series and a zero one read the same in a
    /// chart and differently in a rule.
    #[tokio::test]
    async fn every_cause_is_published_and_the_pooled_number_is_their_sum() {
        let health = Arc::new(ReadHealth::new(5000));
        let _goals = ReadHealth::track(&health, "goals", "worker");
        health.note_failure("goals", "worker", ReadRefusal::NoGroup);
        health.note_failure("goals", "worker", ReadRefusal::Other);
        health.note_failure("goals", "worker", ReadRefusal::Other);
        let observable = StreamReadObservable::new(Arc::clone(&health));

        let metrics = observable.collect_metrics("").await.unwrap();
        let failures: Vec<_> = metrics
            .iter()
            .filter(|m| m.name == READ_FAILURES_METRIC)
            .collect();
        assert_eq!(
            failures.len(),
            ReadRefusal::ALL.len(),
            "one series per cause, no more: {failures:?}"
        );
        for refusal in ReadRefusal::ALL {
            let series = failures
                .iter()
                .find(|m| m.labels.get(READ_FAILURE_REASON_LABEL).map(String::as_str)
                    == Some(refusal.label()))
                .unwrap_or_else(|| panic!("no series under {}: {failures:?}", refusal.label()));
            assert!(
                series.labels.get("consumer_group").map(String::as_str) == Some("worker"),
                "the cause is a second dimension, not a replacement for the \
                 consumer: {series:?}"
            );
        }
        let pooled: f64 = failures.iter().map(|m| m.value).sum();
        assert_eq!(
            pooled,
            health.snapshot()[0].1.failures() as f64,
            "summing over the cause label has to give the number this series \
             held before it was broken down"
        );
    }

    /// The rule and the readings are two halves of one contract, and a rename
    /// on either side leaves an alert that can never fire. Asserted against the
    /// shipped configuration, which is what the watcher actually evaluates.
    #[test]
    fn the_shipped_rule_reads_these_metrics() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/helm/cogneva/files/cogneva.json");
        let text = std::fs::read_to_string(&path).expect("read the chart's cogneva.json");
        let parsed: serde_json::Value = serde_json::from_str(&text).expect("chart JSON parses");
        let rules = parsed["observability"]["infra_watch"]["rules"]
            .as_array()
            .expect("rules array");

        let mut matched = Vec::new();
        for rule in rules {
            let promql = rule["promql"].as_str().unwrap_or_default();
            if promql.contains(READ_SILENT_SECONDS_METRIC) {
                matched.push(rule);
            }
        }
        assert_eq!(
            matched.len(),
            1,
            "exactly one rule watches the consumers' silence, found {}",
            matched.len()
        );
        let rule = matched[0];
        let promql = rule["promql"].as_str().unwrap();
        assert!(
            promql.contains(READ_BLOCK_SECONDS_METRIC),
            "the bound must come from the period the consumers read at: {promql}"
        );
        assert!(
            promql.trim_start().starts_with(READ_SILENT_SECONDS_METRIC)
                && !promql.contains("sum(")
                && !promql.contains("avg("),
            "the silence has to stay one series per consumer so the alert can name \
             the one that stalled: {promql}"
        );
        // The left side keeps its labels, so the bound has to reach it as a
        // scalar: an aggregation without a by/without clause is a vector whose
        // label set is empty, and vector-to-vector comparison only pairs up
        // entries whose label sets are equal. Every labelled series on the
        // left then has no partner, the comparison yields nothing, and the
        // rule is one that can never fire -- the shape is valid PromQL, so
        // nothing upstream reports it.
        assert!(
            promql.contains("scalar("),
            "the bound has to be a scalar, or the labelled left side finds no \
             partner to compare against and the rule is silent for good: {promql}"
        );
        let summary = rule["summary"].as_str().unwrap_or_default();
        for placeholder in ["{stream}", "{consumer_group}"] {
            assert!(
                summary.contains(placeholder),
                "an alert about one stalled consumer has to name it: {summary}"
            );
        }
    }
}
