//! Observable implementation for cog-github: merge-loop decision counts.
//!
//! Without these the merge loop's health is invisible — an executor that
//! merges nothing looks exactly like one that has nothing to merge.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

static GLOBAL: OnceLock<Arc<MergeObservable>> = OnceLock::new();

/// Process-wide merge observable.
pub fn global_merge_observable() -> Arc<MergeObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(MergeObservable::new()))
        .clone()
}

/// Counts merge decisions by outcome and reason bucket. Both labels come from
/// closed sets, so the time-series count stays bounded.
#[derive(Default)]
pub struct MergeObservable {
    decisions: Mutex<HashMap<(String, String), u64>>,
}

impl MergeObservable {
    /// Create an empty observable.
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one decision. `outcome` is one of `auto_merged` / `waiting` /
    /// `blocked` / `failed`; `reason` is the coarse bucket, never free text.
    pub fn record(&self, outcome: &str, reason: &str) {
        if let Ok(mut map) = self.decisions.lock() {
            *map.entry((outcome.to_string(), reason.to_string()))
                .or_insert(0) += 1;
        }
    }
}

#[async_trait]
impl Observable for MergeObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D8" {
            if let Ok(map) = self.decisions.lock() {
                for ((outcome, reason), count) in map.iter() {
                    metrics.push(
                        RawMetric::new("merge_decisions_total", *count as f64)
                            .with_label("outcome", outcome)
                            .with_label("reason", reason),
                    );
                }
            }
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D8".into()]
    }
}
