//! Observable implementation for cog-orchestrator.
//! Exposes D1 (Outcome) and D8 (Multi-Agent Collaboration) raw metrics.

use async_trait::async_trait;
use cog_core::observability::{Observable, RawMetric, TraceFragment};
use cog_core::SFResult;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

static GLOBAL: OnceLock<Arc<OrchestratorObservable>> = OnceLock::new();

pub fn global_observable() -> Arc<OrchestratorObservable> {
    GLOBAL
        .get_or_init(|| Arc::new(OrchestratorObservable::new()))
        .clone()
}

use tokio::sync::Mutex;

/// Orchestrator-level observable state.
#[derive(Default)]
pub struct OrchestratorObservable {
    task_count: AtomicU64,
    task_success_count: AtomicU64,
    message_count: AtomicU64,
    crew_rounds: Arc<Mutex<HashMap<String, u64>>>,
}

impl OrchestratorObservable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_task(&self, success: bool) {
        self.task_count.fetch_add(1, Ordering::Relaxed);
        if success {
            self.task_success_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_message(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn record_crew_round(&self, crew_id: impl Into<String>) {
        let mut map = self.crew_rounds.lock().await;
        *map.entry(crew_id.into()).or_insert(0) += 1;
    }
}

#[async_trait]
impl Observable for OrchestratorObservable {
    async fn collect_metrics(&self, dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut metrics = Vec::new();
        match dimension {
            "D1" => {
                let total = self.task_count.load(Ordering::Relaxed);
                let success = self.task_success_count.load(Ordering::Relaxed);
                metrics.push(RawMetric::new("orch_task_count", total as f64));
                metrics.push(RawMetric::new("orch_task_success_count", success as f64));
                if total > 0 {
                    metrics.push(RawMetric::new(
                        "orch_task_success_rate",
                        success as f64 / total as f64,
                    ));
                }
            }
            "D8" => {
                metrics.push(RawMetric::new(
                    "orch_message_count",
                    self.message_count.load(Ordering::Relaxed) as f64,
                ));
                let rounds = self.crew_rounds.lock().await;
                for (crew_id, count) in rounds.iter() {
                    metrics.push(
                        RawMetric::new("orch_crew_rounds", *count as f64)
                            .with_label("crew_id", crew_id),
                    );
                }
            }
            _ => {}
        }
        Ok(metrics)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    fn available_dimensions(&self) -> Vec<String> {
        vec!["D1".into(), "D8".into()]
    }
}
