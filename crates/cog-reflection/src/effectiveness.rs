//! Skill effectiveness tracking with A/B comparison and auto-deprecation.
//! Every time an agent uses a skill extracted from reflection, the outcome
//! (success, self-review score, latency, token cost) is fed back into the
//! tracker.  After a minimum sample size the tracker computes a composite
//! effectiveness score and recommends one of four actions:
//! - **Strengthen** – skill is working well, bump its priority.
//! - **Deprecate** – skill is harmful or useless, remove it.
//! - **Refine** – skill is mediocre, trigger LLM-based refinement.
//! - **NoAction** – not enough data yet.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::types::{EffectivenessAction, SkillEffectivenessRecord};
use crate::LearningRecorder;
use cog_core::SFResult;
use cog_core::SkillOutcome;

/// In-memory + recorder-backed tracker for skill effectiveness.
pub struct SkillEffectivenessTracker {
    records: Arc<RwLock<HashMap<(String, String), SkillEffectivenessRecord>>>,
    recorder: Arc<dyn LearningRecorder>,
}

impl std::fmt::Debug for SkillEffectivenessTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkillEffectivenessTracker")
            .field("recorder", &"<dyn LearningRecorder>")
            .finish()
    }
}

impl SkillEffectivenessTracker {
    pub fn new(recorder: Arc<dyn LearningRecorder>) -> Self {
        Self {
            records: Arc::new(RwLock::new(HashMap::new())),
            recorder,
        }
    }

    /// Record a single skill usage outcome.
    pub async fn record_outcome(&self, outcome: SkillOutcome) -> SFResult<()> {
        let key = (outcome.skill_id.clone(), outcome.task_signature.clone());
        {
            let mut guard = self.records.write().await;
            let record = guard.entry(key.clone()).or_insert_with(|| {
                SkillEffectivenessRecord::new(&outcome.skill_id, &outcome.task_signature)
            });
            record.accumulate(&outcome);
            record.recompute_score();
        }

        // Persist to backend as a "custom" schema entry.
        self.persist_record(&key.0, &key.1).await?;

        info!(
            skill_id = %outcome.skill_id,
            task_sig = %outcome.task_signature,
            success = outcome.success,
            "recorded skill outcome"
        );
        Ok(())
    }

    /// Evaluate a skill across all task signatures it has been used for.
    pub async fn evaluate_skill(&self, skill_id: &str) -> Option<SkillEffectivenessRecord> {
        let guard = self.records.read().await;
        // Aggregate all task signatures for this skill into a single record.
        let mut aggregated: Option<SkillEffectivenessRecord> = None;
        for ((sid, _), record) in guard.iter() {
            if sid == skill_id {
                match aggregated.as_mut() {
                    Some(a) => {
                        a.used_count += record.used_count;
                        a.success_count += record.success_count;
                        a.total_score += record.total_score;
                        a.total_latency_ms += record.total_latency_ms;
                        a.total_token_cost += record.total_token_cost;
                    }
                    None => {
                        aggregated = Some(record.clone());
                    }
                }
            }
        }
        if let Some(ref mut a) = aggregated {
            a.recompute_score();
        }
        aggregated
    }

    /// Recommend an action for a skill based on its aggregated effectiveness.
    pub async fn recommend_action(&self, skill_id: &str) -> EffectivenessAction {
        match self.evaluate_skill(skill_id).await {
            Some(record) => record.recommend_action(),
            None => EffectivenessAction::NoAction,
        }
    }

    /// Apply the recommended action to a [`cog_core::SkillRegistry`].
    pub async fn apply_action(
        &self,
        skill_id: &str,
        registry: Arc<RwLock<cog_core::SkillRegistry>>,
        llm: Option<Arc<dyn cog_core::LlmClient>>,
    ) -> SFResult<()> {
        let action = self.recommend_action(skill_id).await;
        match action {
            EffectivenessAction::Strengthen => {
                info!(skill_id, "Strengthening skill — increasing priority");
                let mut reg = registry.write().await;
                let current = reg.get_priority(skill_id);
                reg.set_priority(skill_id, current + 1);
                drop(reg);
                let learning = cog_core::Learning::new(
                    cog_core::LearningCategory::BestPractice,
                    cog_core::Priority::High,
                    cog_core::Area::Config,
                    format!("Skill {} effectiveness high", skill_id),
                    format!(
                        "Effectiveness score warrants strengthening (priority {} -> {})",
                        current,
                        current + 1
                    ),
                    "Increase skill priority in selection",
                    cog_core::LearningSource::SelfReview,
                );
                self.recorder.record_learning(learning).await?;
            }
            EffectivenessAction::Deprecate => {
                info!(skill_id, "Deprecating skill — removing from registry");
                let mut reg = registry.write().await;
                reg.remove_skill_config(skill_id);
            }
            EffectivenessAction::Refine => {
                info!(skill_id, "Refining skill — triggering evolution");
                if let Some(llm) = llm {
                    let evolution =
                        crate::evolution::EvolutionEngine::new(llm, registry.clone(), None);
                    evolution.refine_skill(skill_id).await?;
                } else {
                    warn!(skill_id, "Cannot refine skill: no LLM provider available");
                }
            }
            EffectivenessAction::NoAction => {
                // Nothing to do.
            }
        }
        Ok(())
    }

    /// Compare skill performance against a baseline (tasks executed without
    /// the skill).  Returns the delta in success rate.
    pub async fn compare_to_baseline(
        &self,
        skill_id: &str,
        baseline_registry: &HashMap<String, (u32, u32)>, // task_sig -> (attempts, successes)
    ) -> HashMap<String, f32> {
        let guard = self.records.read().await;
        let mut deltas = HashMap::new();
        for ((sid, task_sig), record) in guard.iter() {
            if sid != skill_id {
                continue;
            }
            let skill_rate = record.success_count as f32 / record.used_count.max(1) as f32;
            let (base_attempts, base_successes) =
                baseline_registry.get(task_sig).copied().unwrap_or((0, 0));
            let base_rate = base_successes as f32 / base_attempts.max(1) as f32;
            deltas.insert(task_sig.clone(), skill_rate - base_rate);
        }
        deltas
    }

    /// Return all unique skill IDs currently tracked.
    pub async fn tracked_skill_ids(&self) -> Vec<String> {
        let guard = self.records.read().await;
        let mut ids = std::collections::HashSet::new();
        for (sid, _) in guard.keys() {
            ids.insert(sid.clone());
        }
        ids.into_iter().collect()
    }

    async fn persist_record(&self, skill_id: &str, task_signature: &str) -> SFResult<()> {
        let guard = self.records.read().await;
        let record = guard
            .get(&(skill_id.to_string(), task_signature.to_string()))
            .cloned();
        drop(guard);

        if let Some(record) = record {
            let learning = cog_core::Learning::new(
                cog_core::LearningCategory::Insight,
                cog_core::Priority::Medium,
                cog_core::Area::Config,
                format!("Effectiveness {}:{}", skill_id, task_signature),
                serde_json::to_string(&record).unwrap_or_default(),
                "Track skill effectiveness",
                cog_core::LearningSource::SelfReview,
            );
            self.recorder.record_learning(learning).await?;
        }
        Ok(())
    }
}
