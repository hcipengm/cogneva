use cog_core::Agent;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::profile::{complexity_score, select_mode, PgeMode, TaskProfile};

/// ModeSelector Actor — semantic wrapper around a `dyn Agent`.
///
/// Decision philosophy:
/// 1. **Keyword heuristics** — zero-cost inference from the goal text.
///    Strong signals ("debate", "compare", "review") → Roundtable immediately.
///    Strong simple signals ("hello", "greet", "simple") → Pipeline immediately.
/// 2. **Static profile rule** — only when no keywords match and no Agent is wired.
/// 3. **LLM semantic judgment** — delegated to the underlying [`Agent`] when
///    keyword signals are ambiguous. The Agent runtime handles prompt formatting
///    and temperature control.
/// 4. **Default to Roundtable** — when everything else is uncertain, prefer
///    quality over speed.
///
/// MetaLearningEngine is treated as advisory context for the LLM, never
/// as a hard override.
#[derive(Clone)]
pub struct ModeSelectorActor {
    agent: Option<Arc<dyn Agent>>,
    meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
    knowledge: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    self_review: Option<cog_core::SelfReviewConfig>,
}

impl ModeSelectorActor {
    pub fn new() -> Self {
        Self {
            agent: None,
            meta_learning: None,
            knowledge: None,
            self_review: None,
        }
    }

    pub fn with_agent(mut self, agent: Arc<dyn Agent>) -> Self {
        self.agent = Some(agent);
        self
    }

    pub fn with_meta_learning(mut self, engine: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.meta_learning = Some(engine);
        self
    }

    pub fn with_knowledge(mut self, knowledge: Arc<dyn cog_core::KnowledgeBackend>) -> Self {
        self.knowledge = Some(knowledge);
        self
    }

    pub fn with_self_review(mut self, config: cog_core::SelfReviewConfig) -> Self {
        self.self_review = Some(config);
        self
    }

    /// Select PGE mode for the given goal and optional profile.
    ///
    /// Priority (fastest → most expensive):
    /// 1. Keyword heuristic (zero-cost)
    /// 2. Static profile rule (cheap math)
    /// 3. Agent semantic judgment (only if Agent is wired)
    /// 4. Default to Roundtable (quality-first when uncertain)
    pub async fn select_mode(
        &self,
        goal: &str,
        profile: Option<&TaskProfile>,
    ) -> (PgeMode, String) {
        let goal_lower = goal.to_lowercase();

        // --- Stage 1: keyword heuristic (zero cost) ---
        if let Some(result) = Self::keyword_heuristic(&goal_lower) {
            return result;
        }

        // --- Stage 2: static profile rule (cheap, no I/O) ---
        if let Some(profile) = profile {
            let mode = select_mode(profile);
            let reason = format!(
                "Static rule: complexity_score={:.2} → {:?}",
                complexity_score(profile),
                mode
            );
            return (mode, reason);
        }

        // --- Stage 3: Knowledge-backed historical context ---
        let mut knowledge_context: Option<String> = None;
        if let Some(ref k) = self.knowledge {
            let synthetic_task = cog_core::Task::new(
                "mode-selector",
                cog_core::TaskType::Custom("mode_selection".into()),
                serde_json::json!({ "goal": goal }),
            );
            match k.retrieve_relevant(&synthetic_task, goal, 3).await {
                Ok(entries) if !entries.is_empty() => {
                    let texts: Vec<String> = entries
                        .iter()
                        .map(|e| format!("- {} ({}): {}", e.title, e.source, e.content))
                        .collect();
                    knowledge_context = Some(texts.join("\n"));
                }
                Err(e) => {
                    tracing::warn!("ModeSelector knowledge query failed: {}", e);
                }
                _ => {}
            }
        }

        // --- Stage 4: Agent semantic judgment (expensive, last resort) ---
        if let Some(ref agent) = self.agent {
            let ml_context = self.meta_learning_context(goal, profile).await;
            match self
                .agent_decide(
                    goal,
                    profile,
                    ml_context.as_ref(),
                    knowledge_context.as_ref(),
                    agent.as_ref(),
                )
                .await
            {
                Some(result) => return result,
                None => warn!("ModeSelector Agent decision failed, falling back to Roundtable"),
            }
        }

        // --- Stage 5: default ---
        (
            PgeMode::Roundtable,
            "Default: Roundtable (quality-first when uncertain)".into(),
        )
    }

    /// Zero-cost keyword heuristic.
    /// Returns `Some((mode, reason))` when the goal contains strong signals.
    fn keyword_heuristic(goal_lower: &str) -> Option<(PgeMode, String)> {
        // Strong Roundtable indicators: tasks that benefit from debate / consensus.
        let roundtable_keywords = [
            "debate",
            "discuss",
            "compare",
            "contrast",
            "review",
            "decide between",
            "choose between",
            "trade-off",
            "tradeoff",
            "prioritize",
            "rank",
            "evaluate options",
            "pros and cons",
            "risk assessment",
            "security audit",
            "architecture review",
        ];
        for kw in &roundtable_keywords {
            if goal_lower.contains(kw) {
                return Some((
                    PgeMode::Roundtable,
                    format!("Keyword heuristic: '{}' suggests Roundtable", kw),
                ));
            }
        }

        // Strong Pipeline indicators: single-step, deterministic, low-ambiguity.
        let pipeline_keywords = [
            "hello",
            "greet",
            "simple",
            "straightforward",
            "basic",
            "convert",
            "translate",
            "summarize",
            "format",
            "parse",
            "extract",
            "count",
            "list",
            "sort",
            "filter",
        ];
        for kw in &pipeline_keywords {
            if goal_lower.contains(kw) {
                return Some((
                    PgeMode::Pipeline,
                    format!("Keyword heuristic: '{}' suggests Pipeline", kw),
                ));
            }
        }

        None
    }

    async fn meta_learning_context(
        &self,
        goal: &str,
        profile: Option<&TaskProfile>,
    ) -> Option<String> {
        let engine = self.meta_learning.as_ref()?;
        let profile = profile?;

        let features = cog_core::TaskFeatures {
            task_type: "squad".into(),
            domain_tags: vec![goal.into()],
            estimated_complexity: complexity_score(profile) as f32,
            has_external_dependencies: profile.dependency_count > 0.0,
            historical_success_rate: profile.historical_success as f32,
            required_skills: vec![],
        };

        let rec = engine.recommend_mode(&features).await;
        let text = match rec {
            cog_core::ModeRecommendation::Pipeline => {
                "Historical data strongly suggests Pipeline (fastest, sufficient for this task category)."
            }
            cog_core::ModeRecommendation::Roundtable => {
                "Historical data strongly suggests Roundtable (higher quality for this task category)."
            }
            cog_core::ModeRecommendation::UseDefault => {
                return None; // cold start — no useful historical context
            }
        };
        Some(text.into())
    }

    async fn agent_decide(
        &self,
        goal: &str,
        profile: Option<&TaskProfile>,
        ml_context: Option<&String>,
        knowledge_context: Option<&String>,
        agent: &dyn Agent,
    ) -> Option<(PgeMode, String)> {
        let input = self.build_input(goal, profile, ml_context, knowledge_context);

        let result = agent.prompt(input).await.ok()?;
        let result_str = serde_json::to_string_pretty(&result).unwrap_or_default();
        crate::actors::maybe_self_review(agent, &self.self_review, &result_str, "mode_selector")
            .await;
        let text = Self::extract_text(&result);

        debug!(raw_response = %text, "ModeSelectorAgent LLM raw response");

        if text.contains("roundtable") {
            info!(mode = "Roundtable", %goal, "LLM selected Roundtable");
            Some((
                PgeMode::Roundtable,
                "LLM semantic decision: Roundtable (iterative debate recommended)".into(),
            ))
        } else if text.contains("pipeline") {
            info!(mode = "Pipeline", %goal, "LLM selected Pipeline");
            Some((
                PgeMode::Pipeline,
                "LLM semantic decision: Pipeline (linear execution sufficient)".into(),
            ))
        } else {
            warn!(response = %text, "LLM returned unparseable mode, will fallback to Roundtable");
            Some((
                PgeMode::Roundtable,
                "LLM returned unparseable mode; defaulting to Roundtable".into(),
            ))
        }
    }

    fn build_input(
        &self,
        goal: &str,
        profile: Option<&TaskProfile>,
        ml_context: Option<&String>,
        knowledge_context: Option<&String>,
    ) -> serde_json::Value {
        let mut input = serde_json::json!({
            "goal": goal,
            "instruction": "Choose the best execution mode. Reply with exactly one word: Pipeline or Roundtable.",
        });

        if let Some(profile) = profile {
            input["profile"] = serde_json::json!({
                "novelty": profile.novelty,
                "risk": profile.risk,
                "ambiguity": profile.ambiguity,
                "dependency_count": profile.dependency_count,
                "historical_success": profile.historical_success,
                "complexity_score": complexity_score(profile),
            });
        }

        if let Some(ctx) = ml_context {
            input["historical_context"] = serde_json::json!(ctx);
        }

        if let Some(ctx) = knowledge_context {
            input["knowledge_context"] = serde_json::json!(ctx);
        }

        input
    }

    fn extract_text(response: &serde_json::Value) -> String {
        let s = match response {
            serde_json::Value::String(s) => s.clone(),
            val => val
                .get("mode")
                .or(val.get("response"))
                .or(val.get("content"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
        };
        s.trim().to_lowercase()
    }
}

impl Default for ModeSelectorActor {
    fn default() -> Self {
        Self::new()
    }
}
