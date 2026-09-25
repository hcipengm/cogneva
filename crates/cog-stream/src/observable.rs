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

use crate::read_health::{now_seconds, ReadHealth};

/// Seconds the server has been silent to this consumer (message or empty
/// reply both count as an answer).
pub const READ_SILENT_SECONDS_METRIC: &str = "cogneva_stream_read_silent_seconds";

/// Reads this consumer's loop has had refused since it started.
pub const READ_FAILURES_METRIC: &str = "cogneva_stream_read_failures_total";

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
            out.push(
                RawMetric::new(READ_FAILURES_METRIC, slot.failures as f64)
                    .with_label("stream", stream.as_str())
                    .with_label("consumer_group", group.as_str()),
            );
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
            3,
            "the bound plus silence and failures for the one live consumer"
        );
        drop(guard);
        let metrics = observable.collect_metrics("").await.unwrap();
        assert_eq!(
            metrics.len(),
            1,
            "only the bound is left once the consumer is gone: {metrics:?}"
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
        let summary = rule["summary"].as_str().unwrap_or_default();
        for placeholder in ["{stream}", "{consumer_group}"] {
            assert!(
                summary.contains(placeholder),
                "an alert about one stalled consumer has to name it: {summary}"
            );
        }
    }
}
