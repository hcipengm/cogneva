use cog_core::Agent;
use std::sync::Arc;
use tracing::{debug, info, warn};

use crate::profile::{
    change_tier, complexity_score, select_mode, ChangeTier, DeclaredScale, PgeMode, RouteDecision,
    RouteStage, TaskProfile,
};

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
        task_id: Option<&str>,
    ) -> RouteDecision {
        let decision = self.decide(goal, profile, task_id).await;
        // Recorded here rather than by the caller: this is where the stage is
        // decided, so a new caller cannot take a route that never reaches the
        // surface. The declared scale is recorded only when a profile was
        // handed in — with none, nothing was measured about the request, and
        // writing that down as "declared nothing" would be a reading invented
        // for the sake of a non-empty series.
        let observable = crate::observable::global_observable();
        observable.record_route_decision(decision.stage.as_str(), decision.mode.as_str());
        if let Some(profile) = profile {
            observable.record_declared_scale(crate::profile::scale_label(profile.declared_scale));
            // Recorded per declaration rather than as one value: which input is
            // missing is the question, and "two of three present" does not
            // answer it.
            for input in profile.declaration_inputs.iter() {
                observable.record_declaration_input(input.as_str());
            }
        }
        decision
    }

    async fn decide(
        &self,
        goal: &str,
        profile: Option<&TaskProfile>,
        task_id: Option<&str>,
    ) -> RouteDecision {
        let goal_lower = goal.to_lowercase();

        // --- Stage 0: declared change scale (deterministic) ---
        // Ahead of the keyword stage on purpose. A word in the goal is weaker
        // evidence than the files the request names, and this is the only stage
        // that can tell a change too large for the lightest topology from one
        // that was merely described in one sentence — the keyword stage reads
        // the sentence and would down-route the former on a word like "simple".
        if let Some(profile) = profile {
            if let Some(result) = Self::declared_scale_verdict(profile.declared_scale) {
                return result;
            }
        }

        // --- Stage 1: keyword heuristic (zero cost) ---
        if let Some(result) = Self::keyword_heuristic(&goal_lower) {
            return result;
        }

        // --- Stage 2: static profile rule (cheap, no I/O) ---
        if let Some(profile) = profile {
            return select_mode(profile);
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
                    task_id,
                )
                .await
            {
                Some(result) => return result,
                None => warn!("ModeSelector Agent decision failed, falling back to Roundtable"),
            }
        }

        // --- Stage 5: default ---
        RouteDecision {
            mode: PgeMode::Roundtable,
            stage: RouteStage::Default,
            reason: "Default: Roundtable (quality-first when uncertain)".into(),
        }
    }

    /// What a measured change scale settles on its own, before any word in the
    /// goal is read. `None` when the request declared nothing readable — the
    /// keyword and score stages then decide exactly as they did before.
    fn declared_scale_verdict(scale: DeclaredScale) -> Option<RouteDecision> {
        let (mode, why) = match change_tier(scale)? {
            ChangeTier::Shortcut => (
                PgeMode::Direct,
                "every file the request names is prose, so its scope is stated",
            ),
            ChangeTier::Deep => (
                PgeMode::Roundtable,
                "the request declares a change too large for the lightest topology",
            ),
        };
        Some(RouteDecision {
            mode,
            stage: RouteStage::DeclaredScale,
            reason: format!("Declared scale {scale:?} → {mode:?}: {why}"),
        })
    }

    /// Zero-cost keyword heuristic.
    /// Returns `Some` when the goal contains strong signals.
    fn keyword_heuristic(goal_lower: &str) -> Option<RouteDecision> {
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
                return Some(RouteDecision {
                    mode: PgeMode::Roundtable,
                    stage: RouteStage::Keyword,
                    reason: format!("Keyword heuristic: '{}' suggests Roundtable", kw),
                });
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
                return Some(RouteDecision {
                    mode: PgeMode::Pipeline,
                    stage: RouteStage::Keyword,
                    reason: format!("Keyword heuristic: '{}' suggests Pipeline", kw),
                });
            }
        }

        None
    }

    async fn meta_learning_context(
        &self,
        _goal: &str,
        profile: Option<&TaskProfile>,
    ) -> Option<String> {
        let engine = self.meta_learning.as_ref()?;
        // Still require a profile: without one the caller has nothing to
        // classify the task by and the recommendation would be noise.
        profile?;

        // The same group the writing side records under — the recommendation
        // has to be looked up in the group the outcomes were recorded in.
        let group = crate::meta_features::squad_decision().group;

        let rec = engine.recommend_mode(&group).await;
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
        task_id: Option<&str>,
    ) -> Option<RouteDecision> {
        let input = self.build_input(goal, profile, ml_context, knowledge_context);

        let result = match task_id {
            Some(tid) => agent.prompt_for_task(tid, input).await.ok()?,
            None => agent.prompt(input).await.ok()?,
        };
        let result_str = serde_json::to_string_pretty(&result).unwrap_or_default();
        crate::actors::maybe_self_review(agent, &self.self_review, &result_str, "mode_selector")
            .await;
        let text = Self::extract_text(&result);

        debug!(raw_response = %text, "ModeSelectorAgent LLM raw response");

        if text.contains("roundtable") {
            info!(mode = "Roundtable", %goal, "LLM selected Roundtable");
            Some(RouteDecision {
                mode: PgeMode::Roundtable,
                stage: RouteStage::Agent,
                reason: "LLM semantic decision: Roundtable (iterative debate recommended)".into(),
            })
        } else if text.contains("pipeline") {
            info!(mode = "Pipeline", %goal, "LLM selected Pipeline");
            Some(RouteDecision {
                mode: PgeMode::Pipeline,
                stage: RouteStage::Agent,
                reason: "LLM semantic decision: Pipeline (linear execution sufficient)".into(),
            })
        } else {
            warn!(response = %text, "LLM returned unparseable mode, will fallback to Roundtable");
            Some(RouteDecision {
                mode: PgeMode::Roundtable,
                stage: RouteStage::Agent,
                reason: "LLM returned unparseable mode; defaulting to Roundtable".into(),
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(declared_scale: DeclaredScale) -> TaskProfile {
        TaskProfile {
            declared_scale,
            ..Default::default()
        }
    }

    /// A word in the goal is weaker evidence than the files the request names.
    /// "simple" used to be enough to send a multi-file rewrite to the lightest
    /// topology, and the sentence that asks for a big change is often a short
    /// one — which is exactly the case the keyword stage cannot see.
    #[tokio::test]
    async fn a_measured_big_change_is_not_down_routed_by_a_keyword() {
        let p = profile(DeclaredScale::Measured {
            files: 6,
            code_files: 6,
            lines: None,
        });
        let decision = ModeSelectorActor::new()
            .select_mode("simple refactor of the parser", Some(&p), None)
            .await;
        assert_eq!(decision.mode, PgeMode::Roundtable);
        assert_eq!(decision.stage, RouteStage::DeclaredScale);
        assert!(decision.reason.contains("Declared scale"), "{decision:?}");
    }

    /// And the same precedence the other way round: a prose-only edit is not
    /// sent to a debate because the request said "review".
    #[tokio::test]
    async fn a_measured_prose_change_wins_over_a_roundtable_keyword() {
        let p = profile(DeclaredScale::Measured {
            files: 1,
            code_files: 0,
            lines: None,
        });
        let decision = ModeSelectorActor::new()
            .select_mode("review the wording in docs/quickstart.md", Some(&p), None)
            .await;
        assert_eq!(decision.mode, PgeMode::Direct);
        assert_eq!(decision.stage, RouteStage::DeclaredScale);
        assert!(decision.reason.contains("Declared scale"), "{decision:?}");
    }

    /// Nothing measured leaves the stages below exactly as they were.
    #[tokio::test]
    async fn an_undeclared_scope_leaves_the_keywords_and_the_score_in_charge() {
        let p = profile(DeclaredScale::Unknown);
        let decision = ModeSelectorActor::new()
            .select_mode("summarize the changelog", Some(&p), None)
            .await;
        assert_eq!(decision.mode, PgeMode::Pipeline);
        assert_eq!(decision.stage, RouteStage::Keyword);
        assert!(
            decision.reason.contains("Keyword heuristic"),
            "{decision:?}"
        );
    }

    /// Every stage a decision can carry is one the observation surface knows
    /// how to count.
    ///
    /// The routing metrics publish a cell per entry of [`RouteStage::ALL`]. A
    /// stage the selector can return but `ALL` does not list would be dropped
    /// from the surface entirely — the decision would be taken and counted
    /// nowhere, and the series would look like a stage that never fired.
    #[tokio::test]
    async fn every_stage_the_selector_returns_is_one_the_surface_counts() {
        let cases: Vec<(DeclaredScale, &str)> = vec![
            (
                DeclaredScale::Measured {
                    files: 1,
                    code_files: 0,
                    lines: None,
                },
                "polish the wording in `docs/quickstart.md`",
            ),
            (DeclaredScale::Unknown, "review the landing policy"),
            (DeclaredScale::Unknown, "summarize the changelog"),
        ];
        for (scale, goal) in cases {
            let p = profile(scale);
            let decision = ModeSelectorActor::new()
                .select_mode(goal, Some(&p), None)
                .await;
            assert!(
                RouteStage::ALL.contains(&decision.stage),
                "{goal:?} produced stage {:?}, which the routing surface does not publish",
                decision.stage
            );
            assert!(
                PgeMode::ALL.contains(&decision.mode),
                "{goal:?} produced mode {:?}, which the routing surface does not publish",
                decision.mode
            );
        }
    }

    /// No two stages share a label, and the set is non-empty.
    ///
    /// The surface publishes one cell per entry, so two entries with the same
    /// label would merge their counts into one series while the reader believes
    /// it is looking at one stage — a stage that decided nothing would be
    /// visible as another stage's traffic.
    #[test]
    fn no_two_stages_share_a_label() {
        let labels: Vec<&str> = RouteStage::ALL.iter().map(|s| s.as_str()).collect();
        let unique: std::collections::HashSet<&&str> = labels.iter().collect();
        assert!(!labels.is_empty());
        assert_eq!(
            unique.len(),
            labels.len(),
            "two stages share a label, so their counts merge: {labels:?}"
        );
    }

    /// A decision taken through the real entry point reaches the metric plane.
    ///
    /// The surface has its own tests, and they call the recorders directly —
    /// which would pass just as well if nothing in the selector ever called
    /// them. Measured as a delta rather than an absolute value because the
    /// observable is process-wide and other tests in this binary record into
    /// it.
    #[tokio::test]
    async fn a_decision_taken_here_reaches_the_metric_plane() {
        use cog_core::Observable;

        async fn cell(stage: &str, mode: &str) -> f64 {
            crate::observable::global_observable()
                .collect_metrics("D8")
                .await
                .unwrap()
                .iter()
                .find(|m| {
                    m.name == "collab_route_decisions_total"
                        && m.labels.get("stage").map(String::as_str) == Some(stage)
                        && m.labels.get("mode").map(String::as_str) == Some(mode)
                })
                .map(|m| m.value)
                .unwrap_or_default()
        }
        async fn tier(label: &str) -> f64 {
            crate::observable::global_observable()
                .collect_metrics("D8")
                .await
                .unwrap()
                .iter()
                .find(|m| {
                    m.name == "collab_declared_scale_total"
                        && m.labels.get("tier").map(String::as_str) == Some(label)
                })
                .map(|m| m.value)
                .unwrap_or_default()
        }

        let p = profile(DeclaredScale::Measured {
            files: 1,
            code_files: 0,
            lines: None,
        });
        let before = (
            cell("declared_scale", "direct").await,
            tier("shortcut").await,
        );
        let decision = ModeSelectorActor::new()
            .select_mode("polish the wording in `docs/quickstart.md`", Some(&p), None)
            .await;
        assert_eq!(decision.mode, PgeMode::Direct);
        let after = (
            cell("declared_scale", "direct").await,
            tier("shortcut").await,
        );

        assert!(
            after.0 > before.0,
            "the decision must land in collab_route_decisions_total{{stage=\"declared_scale\",mode=\"direct\"}}"
        );
        assert!(
            after.1 > before.1,
            "the declared scale must land in collab_declared_scale_total{{tier=\"shortcut\"}}"
        );
    }

    /// A declaration a request actually carried is counted, from the task the
    /// profile was derived from rather than from a set assembled in the test.
    ///
    /// This is the reading that answers "does this declaration have a producer
    /// here": built from a real task, so a provenance set that is populated
    /// nowhere — or populated from the wrong field — leaves the cell at zero.
    #[tokio::test]
    async fn a_declaration_the_request_carried_is_counted_where_it_was_read() {
        use cog_core::Observable;

        async fn input_cell(input: &str) -> f64 {
            crate::observable::global_observable()
                .collect_metrics("D8")
                .await
                .unwrap()
                .iter()
                .find(|m| {
                    m.name == "collab_declaration_inputs_total"
                        && m.labels.get("input").map(String::as_str) == Some(input)
                })
                .map(|m| m.value)
                .unwrap_or_default()
        }

        let task = cog_core::Task::new(
            "t",
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({ "goal": "touch crates/cog-parser/src/lib.rs" }),
        );
        let profile = crate::profile::derive_task_profile(&task);
        assert!(
            profile
                .declaration_inputs
                .contains(crate::profile::DeclarationInput::GoalPaths),
            "the profile has to carry the provenance of its own scale"
        );

        let before = input_cell("goal_paths").await;
        ModeSelectorActor::new()
            .select_mode("touch crates/cog-parser/src/lib.rs", Some(&profile), None)
            .await;
        let after = input_cell("goal_paths").await;

        assert!(
            after > before,
            "the declaration must land in collab_declaration_inputs_total{{input=\"goal_paths\"}}"
        );
    }
}
