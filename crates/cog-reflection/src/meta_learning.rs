//! Meta-learning engine for predictive decision recommendations.
//! Generalised to track per-category success rates for arbitrary decisions
//! (PGE mode, reset strategy, retry policy, self-review threshold, etc.)
//! and recommend the better-performing option.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::types::{DecisionStatistics, ModeDecisionRecord, ModeStatistics};
use crate::LearningRecorder;
use crate::{DecisionCategory, DecisionOutcome};
use cog_core::SFResult;

use cog_core::{ModeRecommendation, TaskFeatures};

/// Lightweight meta-learning engine that tracks per-category success rates
/// for arbitrary decisions and recommends the better-performing option.
pub struct MetaLearningEngine {
    /// Legacy PgeMode statistics (retained for backward compatibility).
    stats: Arc<RwLock<HashMap<String, ModeStatistics>>>,
    /// Generic decision statistics keyed by (category, feature_key).
    decision_stats: Arc<RwLock<HashMap<(DecisionCategory, String), DecisionStatistics>>>,
    recorder: Arc<dyn LearningRecorder>,
    /// Minimum samples per decision before making a recommendation.
    min_samples: u32,
    /// Success-rate margin required to prefer one decision over another.
    margin: f64,
    /// 产物级进化策略源（§14.3 热替换）：配置后推荐参数以策略产物 active
    /// 版本为准，self.min_samples/self.margin 仅作兜底。
    policy: Option<(crate::PolicyStore, String)>,
}

impl std::fmt::Debug for MetaLearningEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetaLearningEngine")
            .field("min_samples", &self.min_samples)
            .field("margin", &self.margin)
            .finish()
    }
}

impl MetaLearningEngine {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            stats: Arc::new(RwLock::new(HashMap::new())),
            decision_stats: Arc::new(RwLock::new(HashMap::new())),
            recorder,
            min_samples: 3,
            margin: 0.15,
            policy: None,
        }
    }

    /// Configure the minimum samples required per mode before recommending.
    pub fn with_min_samples(mut self, n: u32) -> Self {
        self.min_samples = n;
        self
    }

    /// Configure the success-rate margin (default 0.15 = 15 %).
    pub fn with_margin(mut self, m: f64) -> Self {
        self.margin = m;
        self
    }

    /// 接入产物级进化策略源：推荐参数（min_samples/margin）从策略产物
    /// active 版本读取，`activate` 热替换后下一次推荐即生效。
    pub fn with_policy_store(mut self, store: crate::PolicyStore, policy_name: &str) -> Self {
        self.policy = Some((store, policy_name.to_string()));
        self
    }

    /// 有效调参：策略产物 active 版本优先，缺失字段回退到构造参数。
    async fn tuned_params(&self) -> (u32, f64) {
        if let Some((store, name)) = &self.policy {
            if let Ok(Some(artifact)) = store.load_active(name).await {
                let min_samples = artifact
                    .payload
                    .get("min_samples")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32)
                    .unwrap_or(self.min_samples);
                let margin = artifact
                    .payload
                    .get("margin")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(self.margin);
                return (min_samples, margin);
            }
        }
        (self.min_samples, self.margin)
    }

    /// Build a lookup key from task features.
    fn key(features: &TaskFeatures) -> String {
        // Use task_type + first domain tag as the primary key.
        // This keeps the stat space bounded while still being useful.
        let domain = features.domain_tags.first().cloned().unwrap_or_default();
        format!("{}:{}", features.task_type, domain)
    }

    // ========================================================================
    // Generic API
    // ========================================================================

    /// Record the outcome of a generic decision so the model can learn.
    pub async fn record(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        let key = Self::key(features);
        {
            let mut guard = self.decision_stats.write().await;
            let stats = guard.entry((category, key.clone())).or_default();
            let entry = stats.counts.entry(decision.to_string()).or_insert((0, 0));
            entry.0 += 1;
            if outcome == DecisionOutcome::Success {
                entry.1 += 1;
            }
        }

        let learning = cog_core::Learning::new(
            cog_core::LearningCategory::Insight,
            cog_core::Priority::Medium,
            cog_core::Area::Config,
            format!("Decision {:?} for {}", category, key),
            serde_json::json!({
                "category": category,
                "key": key,
                "decision": decision,
                "outcome": outcome,
            })
            .to_string(),
            "Track decision outcomes",
            cog_core::LearningSource::SelfReview,
        );
        self.recorder.record_learning(learning).await?;

        info!(
            category = ?category,
            key = %key,
            decision = %decision,
            outcome = ?outcome,
            "Recorded generic decision outcome"
        );
        Ok(())
    }

    /// Recommend the best decision for a given category and task features.
    /// Returns `Some(decision)` when historical data strongly favours one
    /// option, or `None` when there is insufficient data or the options are
    /// within the configured margin.
    pub async fn recommend(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
    ) -> Option<String> {
        let key = Self::key(features);
        let guard = self.decision_stats.read().await;
        let stats = guard
            .get(&(category, key.clone()))
            .cloned()
            .unwrap_or_default();
        drop(guard);

        if stats.counts.is_empty() {
            debug!(
                category = ?category,
                key = %key,
                "Meta-learning cold start — no data"
            );
            return None;
        }

        let (min_samples, margin) = self.tuned_params().await;
        let mut best: Option<(String, f64)> = None;
        for (decision, (attempts, successes)) in &stats.counts {
            if *attempts < min_samples {
                continue;
            }
            let rate = *successes as f64 / *attempts as f64;
            match best {
                Some((_, best_rate)) if rate <= best_rate + margin => {
                    // Within margin — no strong preference.
                }
                _ => best = Some((decision.clone(), rate)),
            }
        }

        best.map(|(d, _)| d)
    }

    // ========================================================================
    // Backward-compatible PgeMode API
    // ========================================================================

    /// Recommend a mode based on historical data for this task category.
    pub async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation {
        match self.recommend(DecisionCategory::PgeMode, features).await {
            Some(ref d) if d.eq_ignore_ascii_case("pipeline") => ModeRecommendation::Pipeline,
            Some(ref d) if d.eq_ignore_ascii_case("roundtable") => ModeRecommendation::Roundtable,
            _ => {
                // Fall back to legacy stats table.
                let key = Self::key(features);
                let guard = self.stats.read().await;
                let stats = guard.get(&key).cloned().unwrap_or_default();
                drop(guard);

                let (min_samples, margin) = self.tuned_params().await;
                if stats.pipeline_attempts < min_samples && stats.roundtable_attempts < min_samples
                {
                    debug!(
                        key = %key,
                        pipeline = stats.pipeline_attempts,
                        roundtable = stats.roundtable_attempts,
                        "Meta-learning cold start — falling back to default profile"
                    );
                    return ModeRecommendation::UseDefault;
                }

                let pipeline_rate =
                    stats.pipeline_successes as f64 / stats.pipeline_attempts.max(1) as f64;
                let roundtable_rate =
                    stats.roundtable_successes as f64 / stats.roundtable_attempts.max(1) as f64;

                info!(
                    key = %key,
                    pipeline_rate = %format!("{:.2}", pipeline_rate),
                    roundtable_rate = %format!("{:.2}", roundtable_rate),
                    "Meta-learning mode recommendation (legacy path)"
                );

                if pipeline_rate > roundtable_rate + margin {
                    ModeRecommendation::Pipeline
                } else if roundtable_rate > pipeline_rate + margin {
                    ModeRecommendation::Roundtable
                } else {
                    ModeRecommendation::UseDefault
                }
            }
        }
    }

    /// Record the actual outcome of a mode decision so the model can learn.
    pub async fn record_outcome(
        &self,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> SFResult<()> {
        let outcome = if success {
            DecisionOutcome::Success
        } else {
            DecisionOutcome::Failed
        };
        self.record(DecisionCategory::PgeMode, features, selected_mode, outcome)
            .await?;

        // Also update legacy stats table for backward compatibility.
        let key = Self::key(features);
        {
            let mut guard = self.stats.write().await;
            let stats = guard.entry(key.clone()).or_default();
            match selected_mode.to_lowercase().as_str() {
                "pipeline" => {
                    stats.pipeline_attempts += 1;
                    if success {
                        stats.pipeline_successes += 1;
                    }
                }
                "roundtable" => {
                    stats.roundtable_attempts += 1;
                    if success {
                        stats.roundtable_successes += 1;
                    }
                }
                other => {
                    warn!(mode = %other, "Unknown mode in meta-learning outcome");
                }
            }
        }

        // Persist decision record.
        let record = ModeDecisionRecord {
            task_features: features.clone(),
            selected_mode: selected_mode.to_string(),
            actual_success: success,
            actual_score: score,
            actual_latency_ms: latency_ms,
            timestamp: Utc::now(),
        };

        let learning = cog_core::Learning::new(
            cog_core::LearningCategory::Insight,
            cog_core::Priority::Medium,
            cog_core::Area::Config,
            format!("Mode decision {}", key),
            serde_json::to_string(&record).unwrap_or_default(),
            "Track mode decision outcomes",
            cog_core::LearningSource::SelfReview,
        );
        self.recorder.record_learning(learning).await?;

        info!(
            key = %key,
            mode = %selected_mode,
            success = success,
            "Recorded mode decision outcome"
        );
        Ok(())
    }

    /// Load aggregated stats from a set of persisted `ModeDecisionRecord`s.
    /// Useful for restoring state after restart.
    pub async fn load_from_records(&self, records: Vec<ModeDecisionRecord>) {
        let mut guard = self.stats.write().await;
        for r in records {
            let key = Self::key(&r.task_features);
            let stats = guard.entry(key).or_default();
            match r.selected_mode.to_lowercase().as_str() {
                "pipeline" => {
                    stats.pipeline_attempts += 1;
                    if r.actual_success {
                        stats.pipeline_successes += 1;
                    }
                }
                "roundtable" => {
                    stats.roundtable_attempts += 1;
                    if r.actual_success {
                        stats.roundtable_successes += 1;
                    }
                }
                _ => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl cog_core::MetaLearning for MetaLearningEngine {
    async fn recommend_mode(&self, features: &TaskFeatures) -> ModeRecommendation {
        MetaLearningEngine::recommend_mode(self, features).await
    }

    async fn record_outcome(
        &self,
        features: &TaskFeatures,
        selected_mode: &str,
        success: bool,
        score: f32,
        latency_ms: u64,
    ) -> SFResult<()> {
        MetaLearningEngine::record_outcome(
            self,
            features,
            selected_mode,
            success,
            score,
            latency_ms,
        )
        .await
    }

    async fn recommend(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
    ) -> Option<String> {
        MetaLearningEngine::recommend(self, category, features)
            .await
            .map(|s| s.to_string())
    }

    async fn record(
        &self,
        category: DecisionCategory,
        features: &TaskFeatures,
        decision: &str,
        outcome: DecisionOutcome,
    ) -> SFResult<()> {
        MetaLearningEngine::record(self, category, features, decision, outcome).await
    }
}
