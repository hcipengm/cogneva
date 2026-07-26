//! Observable implementation for cog-collaboration.
//! Exposes D8 (Multi-Agent Collaboration) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<CollaborationObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<CollaborationObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(CollaborationObservable::new()))
        .clone()
}

use tokio::sync::Mutex;

/// Collaboration-layer observable state.
#[derive(Default)]
pub struct CollaborationObservable {
    agent_message_count: AtomicU64,
    agent_turnaround_ms: Arc<Mutex<HashMap<String, Vec<u64>>>>,
    round_count: AtomicU64,
}

impl CollaborationObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_message(&self) {
        self.agent_message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_turnaround(&self, agent_id: impl Into<String>, ms: u64) {
        self.agent_turnaround_ms
            .lock()
            .await
            .entry(agent_id.into())
            .or_default()
            .push(ms);
    }

    pub fn record_round(&self) {
        self.round_count.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl Observable for CollaborationObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        if dimension == "D8" {
            metrics.push(RawMetric::new(
                "collab_message_count",
                self.agent_message_count.load(Ordering::Relaxed) as f64,
            ));
            metrics.push(RawMetric::new(
                "collab_round_count",
                self.round_count.load(Ordering::Relaxed) as f64,
            ));
            let turnarounds = self.agent_turnaround_ms.lock().await;
            for (agent_id, latencies) in turnarounds.iter() {
                if !latencies.is_empty() {
                    let avg = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
                    metrics.push(
                        RawMetric::new("collab_agent_turnaround_ms", avg)
                            .with_label("agent_id", agent_id),
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
