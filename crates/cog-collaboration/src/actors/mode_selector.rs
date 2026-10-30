use std::sync::Arc;

use crate::profile::{
    change_tier, select_mode, ChangeTier, DeclaredScale, PgeMode, RouteDecision, RouteStage,
    TaskProfile,
};

/// ModeSelector Actor — the router that picks a PGE mode for one task.
///
/// Decision philosophy, fastest first, every stage deterministic:
/// 1. **Declared change scale** — what the files the request names measured.
///    A request whose files are all prose takes the shortcut, and one that
///    declares a change too large for the lightest topology is never routed
///    there.
/// 2. **Keyword heuristics** — zero-cost inference from the goal text.
///    Strong signals ("debate", "compare", "review") → Roundtable immediately.
///    Strong simple signals ("hello", "greet", "simple") → Pipeline immediately.
/// 3. **Recorded outcomes** — when the group's measured results clear the
///    configured sample and margin, they decide. A measured result outranks
///    the static rule below it.
/// 4. **Static profile rule** — the complexity score, and it answers for every
///    profile.
///
/// The last stage being total is what keeps this actor free of I/O: it holds no
/// agent, no model and no retrieval, so routing a task buys no prompt and leaves
/// no consumer on the bus. Three stages below the static rule used to exist — a
/// knowledge lookup, a model's verdict and a quality-first default — and none of
/// them could run: every caller hands in a profile, and the static rule returns
/// for all of them. What that cost was a `mode-selector` agent created for every
/// atomic task and never prompted, and two stage cells on the routing surface
/// that could only ever read zero.
#[derive(Clone)]
pub struct ModeSelectorActor {
    meta_learning: Option<Arc<dyn cog_core::MetaLearning>>,
}

impl ModeSelectorActor {
    pub fn new() -> Self {
        Self {
            meta_learning: None,
        }
    }

    pub fn with_meta_learning(mut self, engine: Arc<dyn cog_core::MetaLearning>) -> Self {
        self.meta_learning = Some(engine);
        self
    }

    /// Select the PGE mode for `goal`, measured against `profile`.
    ///
    /// The profile is not optional: without one nothing was measured about the
    /// request, and the fallback stages that used to answer that case were
    /// unreachable from every production caller. A route decided for a request
    /// that declared nothing is not a case this actor has evidence for.
    pub async fn select_mode(&self, goal: &str, profile: &TaskProfile) -> RouteDecision {
        let decision = self.decide(goal, profile).await;
        // Recorded here rather than by the caller: this is where the stage is
        // decided, so a new caller cannot take a route that never reaches the
        // surface.
        let observable = crate::observable::global_observable();
        observable.record_route_decision(decision.stage.as_str(), decision.mode.as_str());
        observable.record_declared_scale(crate::profile::scale_label(profile.declared_scale));
        // Recorded per declaration rather than as one value: which input is
        // missing is the question, and "two of three present" does not
        // answer it.
        for input in profile.declaration_inputs.iter() {
            observable.record_declaration_input(input.as_str());
        }
        decision
    }

    async fn decide(&self, goal: &str, profile: &TaskProfile) -> RouteDecision {
        let goal_lower = goal.to_lowercase();

        // --- Stage 1: declared change scale (deterministic) ---
        // Ahead of the keyword stage on purpose. A word in the goal is weaker
        // evidence than the files the request names, and this is the only stage
        // that can tell a change too large for the lightest topology from one
        // that was merely described in one sentence — the keyword stage reads
        // the sentence and would down-route the former on a word like "simple".
        if let Some(result) = Self::declared_scale_verdict(profile.declared_scale) {
            return result;
        }

        // --- Stage 2: keyword heuristic (zero cost) ---
        if let Some(result) = Self::keyword_heuristic(&goal_lower) {
            return result;
        }

        // --- Stage 3: recorded outcomes for this group (measured, in memory) ---
        // Ahead of the static rule on purpose. The static rule is a guess about
        // the request's shape; these are outcomes already measured for this
        // group. When the evidence clears the configured sample and margin the
        // engine returns a recommendation, and a measured result outranks the
        // rule it would otherwise be tested against. Below that threshold it
        // answers `UseDefault` and the static rule decides exactly as it did.
        if let Some(decision) = self.recorded_prior().await {
            return decision;
        }

        // --- Stage 4: static profile rule (cheap, no I/O), total by design ---
        select_mode(profile)
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

    /// What measured outcomes say about this group, as a decision.
    ///
    /// `None` on a cold start or when the evidence does not clear the
    /// configured sample and margin — the static rule then decides exactly as
    /// it did. The lookup is an in-memory table read; it is the only stage here
    /// that reads anything.
    async fn recorded_prior(&self) -> Option<RouteDecision> {
        let engine = self.meta_learning.as_ref()?;

        // The same group the writing side records under — the recommendation
        // has to be looked up in the group the outcomes were recorded in.
        let group = crate::meta_features::squad_decision().group;

        let (mode, why) = match engine.recommend_mode(&group).await {
            cog_core::ModeRecommendation::Pipeline => (
                PgeMode::Pipeline,
                "measured outcomes for this group favor Pipeline (fastest, sufficient here)",
            ),
            cog_core::ModeRecommendation::Roundtable => (
                PgeMode::Roundtable,
                "measured outcomes for this group favor Roundtable (higher quality here)",
            ),
            cog_core::ModeRecommendation::UseDefault => return None,
        };
        Some(RouteDecision {
            mode,
            stage: RouteStage::HistoricalPrior,
            reason: format!("Recorded prior: {why}"),
        })
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
    use std::sync::Arc;

    fn profile(declared_scale: DeclaredScale) -> TaskProfile {
        TaskProfile {
            declared_scale,
            ..Default::default()
        }
    }

    /// A meta-learning engine frozen at one answer, so the stage that reads it
    /// can be measured without a database.
    #[derive(Debug)]
    struct FixedPrior(cog_core::ModeRecommendation);

    #[async_trait::async_trait]
    impl cog_core::MetaLearning for FixedPrior {
        async fn recommend_mode(
            &self,
            _group: &cog_core::DecisionGroupKey,
        ) -> cog_core::ModeRecommendation {
            self.0
        }

        async fn record_outcome(
            &self,
            _group: &cog_core::DecisionGroupKey,
            _features: &cog_core::TaskFeatures,
            _selected_mode: &str,
            _success: bool,
            _score: f32,
            _latency_ms: u64,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn recommend(
            &self,
            _category: cog_core::DecisionCategory,
            _group: &cog_core::DecisionGroupKey,
        ) -> Option<String> {
            None
        }

        async fn record(
            &self,
            _category: cog_core::DecisionCategory,
            _group: &cog_core::DecisionGroupKey,
            _decision: &str,
            _outcome: cog_core::DecisionOutcome,
        ) -> cog_core::SFResult<()> {
            Ok(())
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
            .select_mode("simple refactor of the parser", &p)
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
            .select_mode("review the wording in docs/quickstart.md", &p)
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
            .select_mode("summarize the changelog", &p)
            .await;
        assert_eq!(decision.mode, PgeMode::Pipeline);
        assert_eq!(decision.stage, RouteStage::Keyword);
        assert!(
            decision.reason.contains("Keyword heuristic"),
            "{decision:?}"
        );
    }

    /// A measured outcome outranks the static rule, and the static rule keeps
    /// deciding when the record is too thin to answer.
    ///
    /// Both halves go through the production argument shape — a profile, which
    /// is what the only production caller passes. The previous arrangement had
    /// the recommendation below a stage that returns for every profile, so with
    /// the arguments production actually sends, the reader was unreachable and
    /// the account's `landed` verdict measured a call site rather than a
    /// behaviour.
    #[tokio::test]
    async fn a_recorded_prior_outranks_the_static_profile_rule() {
        let p = profile(DeclaredScale::Unknown);
        let goal = "polish the wording in `docs/quickstart.md`";

        // The rule alone sends this shape to Pipeline: the prior below is the
        // only thing that can move it, so the difference is attributable.
        let static_only = ModeSelectorActor::new().select_mode(goal, &p).await;
        assert_eq!(static_only.stage, RouteStage::Score);
        assert_eq!(static_only.mode, PgeMode::Pipeline);

        let with_prior = ModeSelectorActor::new()
            .with_meta_learning(Arc::new(FixedPrior(
                cog_core::ModeRecommendation::Roundtable,
            )))
            .select_mode(goal, &p)
            .await;
        assert_eq!(with_prior.stage, RouteStage::HistoricalPrior);
        assert_eq!(with_prior.mode, PgeMode::Roundtable);
        assert!(
            with_prior.reason.contains("Recorded prior"),
            "{with_prior:?}"
        );

        // Cold start: nothing measured, so the rule decides as it did before
        // this stage existed.
        let cold = ModeSelectorActor::new()
            .with_meta_learning(Arc::new(FixedPrior(
                cog_core::ModeRecommendation::UseDefault,
            )))
            .select_mode(goal, &p)
            .await;
        assert_eq!(cold.stage, RouteStage::Score);
        assert_eq!(cold.mode, PgeMode::Pipeline);
    }

    /// Every stage the routing surface publishes is one this router can return.
    ///
    /// The surface publishes a cell per entry of [`RouteStage::ALL`], so an
    /// entry no call can produce reads as "that stage decided nothing" forever.
    /// `agent` and `default` sat there exactly that way: the model's verdict and
    /// the quality-first default were listed below a stage that returns for
    /// every profile, so neither could be entered, and the account's zeroes were
    /// a property of the argument shape rather than of the routing.
    ///
    /// This drives every stage with the inputs it needs and fails on the first
    /// listed entry no decision ever carried, so a stage added to the list
    /// without a way to reach it is caught where it is written.
    #[tokio::test]
    async fn every_published_stage_is_one_this_router_can_return() {
        let score_goal = "polish the wording in `docs/quickstart.md`";
        let produced = vec![
            ModeSelectorActor::new()
                .select_mode(
                    "polish the wording in `docs/quickstart.md`",
                    &profile(DeclaredScale::Measured {
                        files: 1,
                        code_files: 0,
                        lines: None,
                    }),
                )
                .await
                .stage,
            ModeSelectorActor::new()
                .select_mode("summarize the changelog", &profile(DeclaredScale::Unknown))
                .await
                .stage,
            ModeSelectorActor::new()
                .select_mode(score_goal, &profile(DeclaredScale::Unknown))
                .await
                .stage,
            ModeSelectorActor::new()
                .with_meta_learning(Arc::new(FixedPrior(cog_core::ModeRecommendation::Pipeline)))
                .select_mode(score_goal, &profile(DeclaredScale::Unknown))
                .await
                .stage,
        ];
        for stage in &produced {
            assert!(
                RouteStage::ALL.contains(stage),
                "the router produced a stage the surface does not publish: {stage:?}"
            );
        }
        for stage in RouteStage::ALL {
            assert!(
                produced.contains(&stage),
                "{stage:?} is published by the routing surface but no decision in \
                 this test could produce it; produced={produced:?}"
            );
        }
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
            let decision = ModeSelectorActor::new().select_mode(goal, &p).await;
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
                        && m.labels.get(crate::LABEL_STAGE).map(String::as_str) == Some(stage)
                        && m.labels.get(crate::LABEL_MODE).map(String::as_str) == Some(mode)
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
                        && m.labels.get(crate::LABEL_TIER).map(String::as_str) == Some(label)
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
            .select_mode("polish the wording in `docs/quickstart.md`", &p)
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
                        && m.labels.get(crate::LABEL_INPUT).map(String::as_str) == Some(input)
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
            .select_mode("touch crates/cog-parser/src/lib.rs", &profile)
            .await;
        let after = input_cell("goal_paths").await;

        assert!(
            after > before,
            "the declaration must land in collab_declaration_inputs_total{{input=\"goal_paths\"}}"
        );
    }
}
