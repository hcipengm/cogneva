use prometheus::{
    CounterVec, Encoder, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};
use std::sync::Arc;

/// Prometheus metrics for quota tracking.
#[derive(Debug, Clone)]
pub struct QuotaMetrics {
    registry: Arc<Registry>,
    quota_used_total: CounterVec,
    quota_remaining: GaugeVec,
    quota_exceeded_total: CounterVec,
    workspace_quota_used: GaugeVec,
    task_token_cost: HistogramVec,
}

impl QuotaMetrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Arc::new(Registry::new());

        let quota_used_total = CounterVec::new(
            Opts::new("quota_used_total", "Total quota tokens used by user"),
            &["user_id"],
        )?;

        let quota_remaining = GaugeVec::new(
            Opts::new("quota_remaining", "Remaining quota tokens for user"),
            &["user_id"],
        )?;

        let quota_exceeded_total = CounterVec::new(
            Opts::new(
                "quota_exceeded_total",
                "Total quota exceeded events by user",
            ),
            &["user_id"],
        )?;

        let workspace_quota_used = GaugeVec::new(
            Opts::new("workspace_quota_used", "Quota tokens used by workspace"),
            &["workspace_id"],
        )?;

        let task_token_cost = HistogramVec::new(
            HistogramOpts::new("task_token_cost", "Token cost distribution by model")
                .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0]),
            &["model"],
        )?;

        registry.register(Box::new(quota_used_total.clone()))?;
        registry.register(Box::new(quota_remaining.clone()))?;
        registry.register(Box::new(quota_exceeded_total.clone()))?;
        registry.register(Box::new(workspace_quota_used.clone()))?;
        registry.register(Box::new(task_token_cost.clone()))?;

        Ok(Self {
            registry,
            quota_used_total,
            quota_remaining,
            quota_exceeded_total,
            workspace_quota_used,
            task_token_cost,
        })
    }

    /// Register metrics with the default registry.
    pub fn register(&self) -> Result<(), prometheus::Error> {
        // Already registered in new(); this method is for external registry integration.
        Ok(())
    }

    /// Collect metrics as Prometheus text format.
    pub fn collect(&self) -> Result<String, prometheus::Error> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8_lossy(&buffer).to_string())
    }

    /// Record quota usage for a user.
    pub fn record_quota_used(&self, user_id: &str, tokens: u64) {
        self.quota_used_total
            .with_label_values(&[user_id])
            .inc_by(tokens as f64);
    }

    /// Set remaining quota for a user.
    pub fn set_quota_remaining(&self, user_id: &str, remaining: u64) {
        self.quota_remaining
            .with_label_values(&[user_id])
            .set(remaining as f64);
    }

    /// Record a quota exceeded event.
    pub fn record_quota_exceeded(&self, user_id: &str) {
        self.quota_exceeded_total
            .with_label_values(&[user_id])
            .inc();
    }

    /// Set workspace quota usage.
    pub fn set_workspace_quota_used(&self, workspace_id: &str, used: u64) {
        self.workspace_quota_used
            .with_label_values(&[workspace_id])
            .set(used as f64);
    }

    /// Observe task token cost.
    pub fn observe_task_cost(&self, model: &str, cost: f64) {
        self.task_token_cost
            .with_label_values(&[model])
            .observe(cost);
    }
}

impl Default for QuotaMetrics {
    fn default() -> Self {
        Self::new().expect("failed to create QuotaMetrics")
    }
}
