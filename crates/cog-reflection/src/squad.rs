//! Squad-level reflection — aggregates learnings from multiple agents
//! within a single Squad (Pipeline or Roundtable) into squad-level patterns.
//! A Squad is the unit of PGE (Planner-Generator-Evaluator) execution.
//! After a Squad run completes, `SquadReflection` inspects:
//! - Individual agent learnings and errors
//! - Roundtable iterations (disagreements between planner/generator/evaluator)
//! - Pipeline step failures (signals for PGE mode upgrade)
//! - Retry history (Ralph Loop replacements)

use async_trait::async_trait;
use cog_core::SFResult;
use tracing::{debug, info};

use crate::matcher::LearningMatcher;
use crate::promoter::LearningPromoter;
use crate::recorder::LearningRecorder;
use cog_core::{Learning, LearningCategory, LearningSource, Pattern, Priority};

use std::sync::Arc;

use cog_core::{AgentSquadContribution, SquadReflectionResult};

/// Default implementation combining embedding cosine + keyword heuristics + cross-agent analysis.
pub struct DefaultSquadReflection {
    recorder: Arc<dyn LearningRecorder>,
    matcher: Arc<dyn LearningMatcher>,
    _promoter: Arc<dyn LearningPromoter>,
    embedder: Option<Arc<dyn cog_core::EmbeddingProvider>>,
}

impl DefaultSquadReflection {
    pub fn new(
        recorder: Arc<dyn LearningRecorder>,
        matcher: Arc<dyn LearningMatcher>,
        promoter: Arc<dyn LearningPromoter>,
        embedder: Option<Arc<dyn cog_core::EmbeddingProvider>>,
    ) -> Self {
        Self {
            recorder,
            matcher,
            _promoter: promoter,
            embedder,
        }
    }

    /// Attach an [`EmbeddingProvider`] for BGE-M3 semantic similarity.
    pub fn with_embedder(mut self, embedder: Arc<dyn cog_core::EmbeddingProvider>) -> Self {
        self.embedder = Some(embedder);
        self
    }

    fn extract_text_from_result(result: &serde_json::Value) -> String {
        match result {
            serde_json::Value::Object(map) => {
                let mut texts = Vec::new();
                if let Some(v) = map.get("plan") {
                    texts.push(v.to_string());
                }
                if let Some(v) = map.get("generation") {
                    texts.push(v.to_string());
                }
                if let Some(v) = map.get("evaluation") {
                    texts.push(v.to_string());
                }
                if let Some(v) = map.get("result") {
                    texts.push(v.to_string());
                }
                texts.join(" ").to_lowercase()
            }
            _ => result.to_string().to_lowercase(),
        }
    }
}

#[async_trait]
impl cog_core::SquadReflection for DefaultSquadReflection {
    async fn reflect(
        &self,
        squad_id: &str,
        crew_id: &str,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> SFResult<SquadReflectionResult> {
        let mut all_learnings = Vec::new();
        let mut all_errors = Vec::new();

        // 1. Collect individual agent learnings
        for contrib in contributions {
            for learning in &contrib.learnings {
                let mut l = learning.clone();
                l.related_tasks.push(format!("squad:{}", squad_id));
                self.recorder.record_learning(l.clone()).await?;
                self.matcher.update_recurrence(&mut l).await?;
                all_learnings.push(l);
            }
            for error in &contrib.errors {
                self.recorder.record_error(error.clone()).await?;
                all_errors.push(error.clone());
            }
        }

        // 2. Detect disagreements (Roundtable-specific)
        let disagreements = self.detect_disagreements(contributions).await;
        for mut d in disagreements {
            d.related_tasks.push(format!("squad:{}", squad_id));
            self.recorder.record_learning(d.clone()).await?;
            self.matcher.update_recurrence(&mut d).await?;
            all_learnings.push(d);
        }

        // 3. Detect upgrade signals (Pipeline-specific)
        let upgrades = self
            .detect_upgrade_signals(contributions, retry_count)
            .await;
        let upgrade_recommended = !upgrades.is_empty();
        let upgrade_reason = upgrades.first().map(|u| u.summary.clone());
        for mut u in upgrades {
            u.related_tasks.push(format!("squad:{}", squad_id));
            self.recorder.record_learning(u.clone()).await?;
            self.matcher.update_recurrence(&mut u).await?;
            all_learnings.push(u);
        }

        // 4. Detect squad-wide patterns
        let patterns = self.matcher.detect_patterns().await?;
        let squad_patterns: Vec<Pattern> = patterns
            .into_iter()
            .filter(|p| {
                p.learning_ids
                    .iter()
                    .any(|id| all_learnings.iter().any(|l| l.id == *id))
            })
            .collect();

        info!(
            "Squad reflection for {}: {} learnings, {} patterns, upgrade={}",
            squad_id,
            all_learnings.len(),
            squad_patterns.len(),
            upgrade_recommended
        );

        Ok(SquadReflectionResult {
            squad_id: squad_id.into(),
            task_id: crew_id.into(),
            patterns: squad_patterns,
            learnings: all_learnings,
            upgrade_recommended,
            upgrade_reason,
        })
    }

    async fn detect_disagreements(
        &self,
        contributions: &[AgentSquadContribution],
    ) -> Vec<Learning> {
        let mut learnings = Vec::new();

        // Find planner and evaluator contributions
        let planner_result = contributions
            .iter()
            .find(|c| c.role == "planner")
            .and_then(|c| c.result.as_ref());
        let evaluator_result = contributions
            .iter()
            .find(|c| c.role == "evaluator")
            .and_then(|c| c.result.as_ref());
        let generator_result = contributions
            .iter()
            .find(|c| c.role == "generator")
            .and_then(|c| c.result.as_ref());

        // Detect plan-vs-generation mismatch
        if let (Some(plan), Some(gen)) = (planner_result, generator_result) {
            let plan_text = Self::extract_text_from_result(plan);
            let gen_text = Self::extract_text_from_result(gen);

            // Primary: embedding cosine similarity (BGE-M3).
            // Fallback: token-level Jaccard.
            let sim = if let Some(ref emb) = self.embedder {
                match emb.embed(vec![plan_text.clone(), gen_text.clone()]).await {
                    Ok(vectors) if vectors.len() >= 2 => {
                        cog_core::cosine_similarity(&vectors[0], &vectors[1]) as f32
                    }
                    _ => {
                        // Embedding failed — fall back to Jaccard.
                        let plan_tokens: std::collections::HashSet<String> = plan_text
                            .split_whitespace()
                            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
                            .filter(|s| s.len() > 3)
                            .collect();
                        let gen_tokens: std::collections::HashSet<String> = gen_text
                            .split_whitespace()
                            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
                            .filter(|s| s.len() > 3)
                            .collect();
                        if plan_tokens.is_empty() || gen_tokens.is_empty() {
                            1.0 // Can't compare, assume match.
                        } else {
                            let intersection = plan_tokens.intersection(&gen_tokens).count();
                            intersection as f32 / plan_tokens.union(&gen_tokens).count() as f32
                        }
                    }
                }
            } else {
                let plan_tokens: std::collections::HashSet<String> = plan_text
                    .split_whitespace()
                    .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
                    .filter(|s| s.len() > 3)
                    .collect();
                let gen_tokens: std::collections::HashSet<String> = gen_text
                    .split_whitespace()
                    .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()).to_string())
                    .filter(|s| s.len() > 3)
                    .collect();
                if plan_tokens.is_empty() || gen_tokens.is_empty() {
                    1.0
                } else {
                    let intersection = plan_tokens.intersection(&gen_tokens).count();
                    intersection as f32 / plan_tokens.union(&gen_tokens).count() as f32
                }
            };

            if sim < 0.3 {
                let method = if self.embedder.is_some() {
                    "cosine"
                } else {
                    "Jaccard"
                };
                learnings.push(Learning::new(
                    LearningCategory::Insight,
                    Priority::High,
                    cog_core::Area::Backend,
                    "Roundtable disagreement: plan vs generation mismatch",
                    format!(
                        "Planner and generator produced significantly different outputs. {} similarity: {:.2}",
                        method, sim
                    ),
                    "Review planner instructions or add constraints to align generator with planner intent.",
                    LearningSource::SelfReview,
                ));
            }
        }

        // Detect evaluator rejecting output repeatedly
        if let Some(eval) = evaluator_result {
            let eval_text = Self::extract_text_from_result(eval);
            if eval_text.contains("reject")
                || eval_text.contains("fail")
                || eval_text.contains("low score")
            {
                learnings.push(Learning::new(
                    LearningCategory::Correction,
                    Priority::High,
                    cog_core::Area::Tests,
                    "Evaluator consistently rejects squad output",
                    format!("Evaluator result: {}", eval_text),
                    "Investigate evaluator criteria or improve generator quality.",
                    LearningSource::SelfReview,
                ));
            }
        }

        debug!("detected {} squad disagreements", learnings.len());
        learnings
    }

    async fn detect_upgrade_signals(
        &self,
        contributions: &[AgentSquadContribution],
        retry_count: u32,
    ) -> Vec<Learning> {
        let mut learnings = Vec::new();
        let error_count: usize = contributions.iter().map(|c| c.errors.len()).sum();

        // Signal 1: Too many errors across agents in the squad
        if error_count >= 3 {
            learnings.push(Learning::new(
                LearningCategory::Insight,
                Priority::High,
                cog_core::Area::Backend,
                "Squad experiencing multiple agent failures",
                format!(
                    "{} errors across {} agents in squad. Retry count: {}",
                    error_count,
                    contributions.len(),
                    retry_count
                ),
                "Consider upgrading from Pipeline to Roundtable for better coordination.",
                LearningSource::SelfReview,
            ));
        }

        // Signal 2: Ralph Loop has already retried multiple times
        if retry_count >= 2 {
            learnings.push(Learning::new(
                LearningCategory::Insight,
                Priority::Critical,
                cog_core::Area::Backend,
                "Ralph Loop retries exhausted — squad needs structural change",
                format!(
                    "Squad has been retried {} times via Ralph Loop. Errors: {}",
                    retry_count, error_count
                ),
                "Upgrade PGE mode or reassign agent roles.",
                LearningSource::SelfReview,
            ));
        }

        // Signal 3: All agents failed with similar errors (systemic issue)
        let mut error_signatures: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for contrib in contributions {
            for err in &contrib.errors {
                let sig = format!("{}:{}", err.error_message, err.suggested_fix);
                *error_signatures.entry(sig).or_insert(0) += 1;
            }
        }
        for (sig, count) in error_signatures {
            if count >= 2 {
                learnings.push(Learning::new(
                    LearningCategory::Correction,
                    Priority::High,
                    cog_core::Area::Infra,
                    "Systemic error across multiple squad agents",
                    format!("Error signature '{}' hit {} times: {}", sig, count, sig),
                    "Fix the root cause (tool, config, or environment) before retrying.",
                    LearningSource::SelfReview,
                ));
            }
        }

        debug!("detected {} squad upgrade signals", learnings.len());
        learnings
    }
}
