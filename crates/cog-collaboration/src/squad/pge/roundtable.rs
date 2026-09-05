use crate::actors::{
    EvaluatorActor, GeneratorActor, MergeResult, MergerActor, ModeratorActor, PlannerActor,
};
use crate::squad::pge::context_board::ContextBoard;
use crate::squad::pge::types::{
    Artifact, BranchMergeStrategy, Criterion, EvaluationResult, GeneratorOutput, MergeSummary,
    PgeBranchResult, PgeRoundtableIteration, PlannerOutput, Verdict,
};
use std::sync::Arc;

pub struct PgeRoundtableConfig {
    pub max_iterations: u32,
    pub consensus_threshold: f64,
    /// Skill IDs for dynamic agent creation.
    pub skill_ids: Vec<String>,
    /// Optional shared context board (JSON object) that all agents can read
    /// Context Board (Redis Hash) requirement.
    /// This field is used as the **seed** state. For persistent / cross-process
    /// state, use [`board_store`](Self::board_store) instead.
    pub context_board: Option<serde_json::Value>,
    /// Optional shared context-board backing store. When set, the Roundtable
    /// reads the latest snapshot at the start of each iteration and writes
    /// each phase's output back via [`ContextBoard::set`]. Use
    /// [`InMemoryContextBoard`](crate::squad::pge::InMemoryContextBoard) for
    /// single-process tests or [`RedisContextBoard`](crate::squad::pge::RedisContextBoard)
    /// for production multi-agent debates.
    pub board_store: Option<Arc<dyn ContextBoard>>,
    /// Optional moderator agent that reviews the full debate history when
    /// consensus is slow to emerge (iteration >= 3) and decides whether to
    /// continue, change strategy, accept a partial result, or escalate.
    pub moderator: Option<ModeratorActor>,
    /// Optional unified knowledge backend for historical pattern retrieval.
    pub knowledge_backend: Option<Arc<dyn cog_core::KnowledgeBackend>>,
    /// Number of parallel PGE branches to run per iteration. 1 = sequential
    /// backward-compatible behavior.
    pub parallel_branches: u32,
    /// Strategy for merging parallel branch results into a single iteration.
    pub branch_merge_strategy: BranchMergeStrategy,
    /// Optional agent manager used to create fresh agent instances for
    /// parallel branches.
    pub agent_manager: Option<Arc<dyn cog_core::AgentManager>>,
    /// Optional LLM provider passed to [`AgentManager::create_agent`].
    pub llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    /// Optional merger agent used when [`branch_merge_strategy`] is
    /// [`BranchMergeStrategy::Custom`].
    pub merger: Option<MergerActor>,
}

impl std::fmt::Debug for PgeRoundtableConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgeRoundtableConfig")
            .field("max_iterations", &self.max_iterations)
            .field("consensus_threshold", &self.consensus_threshold)
            .field("skill_ids", &self.skill_ids)
            .field("context_board", &self.context_board)
            .field("board_store", &self.board_store.is_some())
            .field("moderator", &self.moderator.is_some())
            .field("parallel_branches", &self.parallel_branches)
            .field("branch_merge_strategy", &self.branch_merge_strategy)
            .field("agent_manager", &self.agent_manager.is_some())
            .field("llm_provider", &self.llm_provider.is_some())
            .field("merger", &self.merger.is_some())
            .finish()
    }
}

impl Default for PgeRoundtableConfig {
    fn default() -> Self {
        Self {
            max_iterations: 5,
            consensus_threshold: 0.8,
            skill_ids: Vec::new(),
            context_board: None,
            board_store: None,
            moderator: None,
            knowledge_backend: None,
            parallel_branches: 1,
            branch_merge_strategy: BranchMergeStrategy::BestScore,
            agent_manager: None,
            llm_provider: None,
            merger: None,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PgeRoundtableResult {
    pub iterations: u32,
    pub consensus_reached: bool,
    pub final_plan: PlannerOutput,
    pub final_generation: GeneratorOutput,
    pub final_evaluation: EvaluationResult,
    pub history: Vec<PgeRoundtableIteration>,
    /// Final state of the shared context board after all debate rounds.
    /// `None` if no context board was configured.
    pub context_board: Option<serde_json::Value>,
}

/// Multi-agent roundtable debate orchestrator.
/// Directly holds [`PlannerActor`], [`GeneratorActor`], and [`EvaluatorActor`]
/// — no internal wrapping of raw [`Agent`]s.
pub struct PgeRoundtable {
    config: PgeRoundtableConfig,
    planner: PlannerActor,
    generator: GeneratorActor,
    evaluator: EvaluatorActor,
}

impl PgeRoundtable {
    pub fn new(
        config: PgeRoundtableConfig,
        planner: PlannerActor,
        generator: GeneratorActor,
        evaluator: EvaluatorActor,
    ) -> Self {
        Self {
            config,
            planner,
            generator,
            evaluator,
        }
    }

    /// Run the roundtable with a structured [`Task`].
    pub async fn debate(
        &self,
        task: &cog_core::Task,
        context: serde_json::Value,
    ) -> PgeRoundtableResult {
        self.debate_task(task, context).await
    }

    /// Initialize the in-memory board JSON from the configured seed and the
    /// optional [`ContextBoard`] backing store. When `board_store` is set, its
    /// snapshot wins over the seed (so re-entering an existing Squad's debate
    /// picks up where the previous run left off).
    async fn init_board(&self) -> serde_json::Value {
        let mut board = self
            .config
            .context_board
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if let Some(ref store) = self.config.board_store {
            if let Ok(serde_json::Value::Object(map)) = store.snapshot().await {
                if let serde_json::Value::Object(ref mut target) = board {
                    for (k, v) in map {
                        target.insert(k, v);
                    }
                } else {
                    board = serde_json::Value::Object(map);
                }
            }
        }
        board
    }

    /// Persist a single board field to the configured [`ContextBoard`] store
    /// if one is set. Errors are logged and swallowed — the in-memory board
    /// is the source of truth for the current iteration.
    async fn persist_field(&self, field: &str, value: &serde_json::Value) {
        if let Some(ref store) = self.config.board_store {
            if let Err(e) = store.set(field, value.clone()).await {
                tracing::warn!(field, "failed to persist context board field: {}", e);
            }
        }
    }

    async fn debate_task(
        &self,
        task: &cog_core::Task,
        _context: serde_json::Value,
    ) -> PgeRoundtableResult {
        let mut history: Vec<PgeRoundtableIteration> = Vec::new();
        let mut consensus_reached = false;

        let mut final_evaluation: Option<EvaluationResult> = None;
        let mut prev_verdict: Option<Verdict> = None;
        let mut board = self.init_board().await;

        for iteration in 1..=self.config.max_iterations {
            crate::observable::global_observable().record_round();
            // Refresh the in-memory board from the backing store so concurrent
            // writers (e.g. a separate Squad sharing the same board key) are
            // visible.
            if self.config.board_store.is_some() {
                board = self.init_board().await;
            }

            let prev_gen_json = history
                .last()
                .map(|h| serde_json::to_value(&h.generation).unwrap_or_default());
            let prev_gen_ref = prev_gen_json.as_ref();
            let prev_eval_ref = final_evaluation
                .as_ref()
                .map(|e| serde_json::to_value(e).unwrap_or_default());
            let prev_eval_ref2 = prev_eval_ref.as_ref();

            let eval_history: Vec<serde_json::Value> = history
                .iter()
                .map(|h| {
                    serde_json::json!({
                        "iteration": h.iteration,
                        "plan": &h.plan,
                        "generation": &h.generation,
                        "evaluation": &h.evaluation,
                    })
                })
                .collect();

            let (plan, generation, evaluation, branches, merge_summary) =
                if self.config.parallel_branches > 1 {
                    self.run_parallel_iteration(
                        task,
                        iteration,
                        prev_eval_ref2,
                        prev_gen_ref,
                        &eval_history,
                        &board,
                    )
                    .await
                } else {
                    let (p, g, e) = self
                        .run_sequential_iteration(
                            task,
                            iteration,
                            prev_eval_ref2,
                            prev_gen_ref,
                            &eval_history,
                            &board,
                        )
                        .await;
                    (p, g, e, Vec::new(), None)
                };

            let plan_json = serde_json::to_value(&plan).unwrap_or_default();
            let generation_json = serde_json::to_value(&generation).unwrap_or_default();
            let eval_json = serde_json::to_value(&evaluation).unwrap_or_default();
            board["latest_plan"] = plan_json.clone();
            self.persist_field("latest_plan", &plan_json).await;
            board["latest_generation"] = generation_json.clone();
            self.persist_field("latest_generation", &generation_json)
                .await;
            board["latest_evaluation"] = eval_json.clone();
            self.persist_field("latest_evaluation", &eval_json).await;
            board["round"] = serde_json::json!(iteration);
            self.persist_field("round", &serde_json::json!(iteration))
                .await;

            history.push(PgeRoundtableIteration {
                iteration,
                plan: plan.clone(),
                generation: generation.clone(),
                evaluation: evaluation.clone(),
                branches,
                merge_summary,
            });

            // Consensus: require Verdict::Pass for at least 2 consecutive iterations.
            let verdict_stable = if let Some(ref prev) = prev_verdict {
                matches!(evaluation.verdict, Verdict::Pass) && matches!(prev, Verdict::Pass)
            } else {
                false
            };

            if matches!(evaluation.verdict, Verdict::Pass) && verdict_stable {
                consensus_reached = true;
                break;
            }

            // --- Moderator intervention ---
            if let Some(ref moderator) = self.config.moderator {
                let mod_output = moderator
                    .moderate(task, &history, &board, self.config.consensus_threshold)
                    .await;
                tracing::info!(
                    iteration,
                    decision = ?mod_output.decision,
                    "Moderator decision"
                );
                match mod_output.decision {
                    crate::actors::moderator::ModeratorDecision::AcceptPartial => {
                        consensus_reached = true;
                        break;
                    }
                    crate::actors::moderator::ModeratorDecision::Escalate => {
                        break;
                    }
                    crate::actors::moderator::ModeratorDecision::ChangeStrategy => {
                        if let Ok(suggestions_json) = serde_json::to_value(&mod_output.suggestions)
                        {
                            board["moderator_suggestions"] = suggestions_json.clone();
                            self.persist_field("moderator_suggestions", &suggestions_json)
                                .await;
                        }
                    }
                    crate::actors::moderator::ModeratorDecision::Continue => {}
                }
            }

            prev_verdict = Some(evaluation.verdict);
            final_evaluation = Some(evaluation);
        }

        let last = history
            .last()
            .cloned()
            .unwrap_or_else(|| PgeRoundtableIteration {
                iteration: 0,
                plan: PlannerOutput {
                    summary: String::new(),
                    plan: serde_json::json!({}),
                    sub_tasks: Vec::new(),
                },
                generation: GeneratorOutput {
                    content: serde_json::Value::Null,
                    artifacts: Vec::new(),
                },
                evaluation: EvaluationResult {
                    verdict: Verdict::Fail,
                    feedback: String::new(),
                    score: None,
                    criteria: Vec::new(),
                    details: None,
                },
                branches: Vec::new(),
                merge_summary: None,
            });

        PgeRoundtableResult {
            iterations: history.len() as u32,
            consensus_reached,
            final_plan: last.plan,
            final_generation: last.generation,
            final_evaluation: last.evaluation,
            history,
            context_board: Some(board),
        }
    }

    /// Run a single sequential PGE iteration using the primary actors.
    async fn run_sequential_iteration(
        &self,
        task: &cog_core::Task,
        iteration: u32,
        prev_eval_ref: Option<&serde_json::Value>,
        prev_gen_ref: Option<&serde_json::Value>,
        eval_history: &[serde_json::Value],
        board: &serde_json::Value,
    ) -> (PlannerOutput, GeneratorOutput, EvaluationResult) {
        let plan = self
            .planner
            .plan(
                task,
                iteration,
                prev_eval_ref
                    .and_then(|e| e.get("feedback"))
                    .and_then(|f| f.as_str()),
                prev_eval_ref
                    .and_then(|e| e.get("score"))
                    .and_then(|s| s.as_u64())
                    .map(|s| s as u32),
                prev_gen_ref,
                Some(board),
            )
            .await;

        let plan_json = serde_json::to_value(&plan).unwrap_or_default();
        let generation = self
            .generator
            .generate(
                task,
                &plan_json,
                iteration,
                crate::actors::PreviousAttempt {
                    evaluation: prev_eval_ref,
                    ..Default::default()
                },
                Some(board),
            )
            .await;

        let generation_json = serde_json::to_value(&generation).unwrap_or_default();
        let evaluation = self
            .evaluator
            .evaluate(
                task,
                &plan_json,
                &generation_json,
                eval_history,
                &[],
                Some(board),
            )
            .await;

        (plan, generation, evaluation)
    }

    /// Run multiple independent PGE branches in parallel and merge the results.
    async fn run_parallel_iteration(
        &self,
        task: &cog_core::Task,
        iteration: u32,
        prev_eval_ref: Option<&serde_json::Value>,
        prev_gen_ref: Option<&serde_json::Value>,
        eval_history: &[serde_json::Value],
        board: &serde_json::Value,
    ) -> (
        PlannerOutput,
        GeneratorOutput,
        EvaluationResult,
        Vec<PgeBranchResult>,
        Option<MergeSummary>,
    ) {
        let branch_count = self.config.parallel_branches.max(1);
        let mut handles = Vec::new();

        for branch_id in 0..branch_count {
            let (planner, generator, evaluator) = if branch_id == 0 {
                (
                    self.planner.clone(),
                    self.generator.clone(),
                    self.evaluator.clone(),
                )
            } else {
                match self.create_branch_actors(branch_id, task).await {
                    Some(actors) => actors,
                    None => continue,
                }
            };

            let task = task.clone();
            let prev_eval = prev_eval_ref.cloned();
            let prev_gen = prev_gen_ref.cloned();
            let eval_history = eval_history.to_vec();
            let board = board.clone();

            let handle = tokio::spawn(async move {
                let plan = planner
                    .plan(
                        &task,
                        iteration,
                        prev_eval
                            .as_ref()
                            .and_then(|e| e.get("feedback"))
                            .and_then(|f| f.as_str()),
                        prev_eval
                            .as_ref()
                            .and_then(|e| e.get("score"))
                            .and_then(|s| s.as_u64())
                            .map(|s| s as u32),
                        prev_gen.as_ref(),
                        Some(&board),
                    )
                    .await;

                let plan_json = serde_json::to_value(&plan).unwrap_or_default();
                let generation = generator
                    .generate(
                        &task,
                        &plan_json,
                        iteration,
                        crate::actors::PreviousAttempt {
                            evaluation: prev_eval.as_ref(),
                            ..Default::default()
                        },
                        Some(&board),
                    )
                    .await;

                let generation_json = serde_json::to_value(&generation).unwrap_or_default();
                let evaluation = evaluator
                    .evaluate(
                        &task,
                        &plan_json,
                        &generation_json,
                        &eval_history,
                        &[],
                        Some(&board),
                    )
                    .await;

                PgeBranchResult {
                    branch_id,
                    plan,
                    generation,
                    evaluation,
                }
            });
            handles.push(handle);
        }

        // Await all branches. If every branch spawn failed, fall back to sequential.
        if handles.is_empty() {
            let (p, g, e) = self
                .run_sequential_iteration(
                    task,
                    iteration,
                    prev_eval_ref,
                    prev_gen_ref,
                    eval_history,
                    board,
                )
                .await;
            return (p, g, e, Vec::new(), None);
        }

        let branches: Vec<PgeBranchResult> = futures::future::join_all(handles)
            .await
            .into_iter()
            .filter_map(|r| match r {
                Ok(branch) => Some(branch),
                Err(e) => {
                    tracing::warn!("Parallel branch task failed: {}", e);
                    None
                }
            })
            .collect();

        let merge_result = self.merge_branches(task, &branches, board).await;
        let merge_summary = Some(MergeSummary {
            selected_branch_id: branches.first().map(|b| b.branch_id).unwrap_or(0),
            strategy: self.config.branch_merge_strategy,
            reasoning: merge_result.reasoning.clone(),
        });

        (
            merge_result.plan,
            merge_result.generation,
            merge_result.evaluation,
            branches,
            merge_summary,
        )
    }

    /// Create a fresh set of actors for a parallel branch.
    async fn create_branch_actors(
        &self,
        branch_id: u32,
        task: &cog_core::Task,
    ) -> Option<(PlannerActor, GeneratorActor, EvaluatorActor)> {
        let manager = self.config.agent_manager.as_ref()?;
        let llm = self.config.llm_provider.as_ref()?;

        let prefix = format!("{}-branch-{}", task.id, branch_id);
        let planner = manager
            .create_agent(&format!("{}-planner", prefix), "planner", llm.clone())
            .await
            .ok()?;
        let generator = manager
            .create_agent(&format!("{}-generator", prefix), "generator", llm.clone())
            .await
            .ok()?;
        let evaluator = manager
            .create_agent(&format!("{}-evaluator", prefix), "evaluator", llm.clone())
            .await
            .ok()?;

        let mut planner_actor = PlannerActor::new(planner);
        let mut generator_actor = GeneratorActor::new(generator);
        let mut evaluator_actor = EvaluatorActor::new(evaluator);

        if let Some(ref kb) = self.config.knowledge_backend {
            planner_actor = planner_actor.with_knowledge(kb.clone());
            generator_actor = generator_actor.with_knowledge(kb.clone());
            evaluator_actor = evaluator_actor.with_knowledge(kb.clone());
        }

        Some((planner_actor, generator_actor, evaluator_actor))
    }

    /// Merge parallel branch results into a single result according to the
    /// configured [`BranchMergeStrategy`].
    async fn merge_branches(
        &self,
        task: &cog_core::Task,
        branches: &[PgeBranchResult],
        board: &serde_json::Value,
    ) -> MergeResult {
        if branches.is_empty() {
            return MergeResult {
                plan: PlannerOutput {
                    summary: String::new(),
                    plan: serde_json::json!({}),
                    sub_tasks: Vec::new(),
                },
                generation: GeneratorOutput {
                    content: serde_json::Value::Null,
                    artifacts: Vec::new(),
                },
                evaluation: EvaluationResult {
                    verdict: Verdict::Fail,
                    feedback: "No branches produced results".into(),
                    score: None,
                    criteria: Vec::new(),
                    details: None,
                },
                reasoning: "No branches".into(),
            };
        }

        match self.config.branch_merge_strategy {
            BranchMergeStrategy::BestScore => self.merge_best_score(branches),
            BranchMergeStrategy::MajorityVote => self.merge_majority_vote(branches),
            BranchMergeStrategy::UnionArtifacts => self.merge_union_artifacts(branches),
            BranchMergeStrategy::Custom => {
                if let Some(ref merger) = self.config.merger {
                    merger.merge(task, branches, board).await
                } else {
                    tracing::warn!("Custom merge strategy requested but no MergerActor configured; falling back to best score");
                    self.merge_best_score(branches)
                }
            }
        }
    }

    fn merge_best_score(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let best = branches
            .iter()
            .max_by_key(|b| b.evaluation.score.unwrap_or(0))
            .cloned()
            .expect("branches is non-empty");

        MergeResult {
            reasoning: format!("Selected branch {} by best score", best.branch_id),
            plan: best.plan,
            generation: best.generation,
            evaluation: best.evaluation,
        }
    }

    fn merge_majority_vote(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let mut counts = std::collections::HashMap::new();
        for b in branches {
            *counts.entry(b.evaluation.verdict).or_insert(0) += 1;
        }
        let majority_verdict = counts
            .into_iter()
            .max_by_key(|(_, c)| *c)
            .map(|(v, _)| v)
            .unwrap_or(Verdict::Fail);

        let best = branches
            .iter()
            .filter(|b| b.evaluation.verdict == majority_verdict)
            .max_by_key(|b| b.evaluation.score.unwrap_or(0))
            .cloned()
            .or_else(|| branches.first().cloned())
            .expect("branches is non-empty");

        MergeResult {
            reasoning: format!(
                "Majority verdict {:?}; selected branch {} by best score",
                majority_verdict, best.branch_id
            ),
            plan: best.plan,
            generation: best.generation,
            evaluation: best.evaluation,
        }
    }

    fn merge_union_artifacts(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let best = branches
            .iter()
            .max_by_key(|b| b.evaluation.score.unwrap_or(0))
            .cloned()
            .expect("branches is non-empty");

        let mut all_artifacts = best.generation.artifacts.clone();
        let mut seen_names = std::collections::HashSet::new();
        for artifact in &all_artifacts {
            seen_names.insert(artifact.name.clone());
        }
        for b in branches {
            for artifact in &b.generation.artifacts {
                if seen_names.insert(artifact.name.clone()) {
                    all_artifacts.push(artifact.clone());
                }
            }
        }

        let mut generation = best.generation;
        generation.artifacts = all_artifacts;

        MergeResult {
            reasoning: format!(
                "Selected branch {} by best score and unioned artifacts",
                best.branch_id
            ),
            plan: best.plan,
            generation,
            evaluation: best.evaluation,
        }
    }
}

pub fn parse_planner_output(value: &serde_json::Value, goal: &str) -> PlannerOutput {
    serde_json::from_value(value.clone()).unwrap_or_else(|_| PlannerOutput {
        summary: format!("Plan for: {}", goal),
        plan: value.clone(),
        sub_tasks: Vec::new(),
    })
}

pub fn parse_generator_output(value: &serde_json::Value) -> GeneratorOutput {
    serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        if let Some(artifact) = try_extract_change_artifact(value) {
            return GeneratorOutput {
                content: serde_json::Value::Null,
                artifacts: vec![artifact],
            };
        }
        GeneratorOutput {
            content: value.clone(),
            artifacts: Vec::new(),
        }
    })
}

/// Best-effort extraction of a change artifact from a raw fallback result.
/// The reasoning-only model sometimes returns XML-wrapped, markdown-fenced,
/// or free-text unified diffs instead of strict JSON; this lets the downstream
/// pipeline still find the change when `build_result` falls back to
/// `{ "result": thought }`.
fn try_extract_change_artifact(value: &serde_json::Value) -> Option<Artifact> {
    let text = value.get("result").and_then(|v| v.as_str())?;

    // Try several diff markers in order of preference.
    let markers = ["diff --git", "```diff", "```change", "--- a/", "--- a\\"];
    let mut start = None;
    for marker in &markers {
        if let Some(pos) = text.find(marker) {
            start = Some(pos);
            break;
        }
    }
    let start = start?;

    // Find the end: prefer the nearest closing markdown fence or XML tag.
    let rest = &text[start..];
    let fence_end = rest.find("\n```").unwrap_or(rest.len());
    let xml_end = rest.find("</artifact>").unwrap_or(rest.len());
    let end = fence_end.min(xml_end);
    let mut content = rest[..end].trim().to_string();

    // Strip leading markdown fence marker if present.
    if content.starts_with("```diff") || content.starts_with("```change") {
        content = content
            .trim_start_matches("```diff")
            .trim_start_matches("```change")
            .trim_start()
            .to_string();
    }

    // Ensure the content starts with a unified-diff marker.
    if !content.starts_with("diff --git")
        && !content.starts_with("--- a/")
        && !content.starts_with("--- a\\")
    {
        return None;
    }

    if content.is_empty() {
        return None;
    }

    Some(Artifact {
        name: "changes.diff".into(),
        content,
        artifact_type: "change".into(),
    })
}

pub fn parse_evaluation_result(value: &serde_json::Value) -> EvaluationResult {
    let passed = value.get("passed").and_then(|v| v.as_bool());
    let score = value
        .get("score")
        .and_then(|v| v.as_u64())
        .map(|s| s as u32);

    let mut result: EvaluationResult = serde_json::from_value(value.clone()).unwrap_or_else(|_| {
        let verdict = match passed {
            Some(true) => Verdict::Pass,
            Some(false) => {
                if score.unwrap_or(0) >= 60 {
                    Verdict::Partial
                } else {
                    Verdict::Fail
                }
            }
            None => match score {
                Some(s) if s >= 80 => Verdict::Pass,
                Some(s) if s >= 60 => Verdict::Partial,
                Some(_) => Verdict::Fail,
                None => Verdict::Fail,
            },
        };
        let criteria: Vec<Criterion> = value
            .get("criteria")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        EvaluationResult {
            verdict,
            feedback: value
                .get("feedback")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            score,
            criteria,
            details: Some(value.clone()),
        }
    });

    // Backward compat: if the input has a legacy `passed` field, re-derive
    // verdict so callers that only supply `passed`+`score` get the correct
    // semantic verdict even though `verdict` has a serde default.
    if passed.is_some() || (score.is_some() && result.verdict == Verdict::Fail) {
        result.verdict = match passed {
            Some(true) => Verdict::Pass,
            Some(false) => {
                if score.unwrap_or(0) >= 60 {
                    Verdict::Partial
                } else {
                    Verdict::Fail
                }
            }
            None => match score {
                Some(s) if s >= 80 => Verdict::Pass,
                Some(s) if s >= 60 => Verdict::Partial,
                Some(_) => Verdict::Fail,
                None => Verdict::Fail,
            },
        };
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockAgent;

    #[async_trait::async_trait]
    impl cog_core::Agent for MockAgent {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
        async fn start(&self) {}
        async fn snapshot(
            &self,
            _task_id: String,
        ) -> cog_core::SFResult<cog_core::AgentCheckpoint> {
            Ok(cog_core::AgentCheckpoint {
                checkpoint_id: String::new(),
                task_id: String::new(),
                agent_state: serde_json::Value::Null,
                context_window: Vec::new(),
                event_offset: 0,
                timestamp: chrono::Utc::now(),
            })
        }
        async fn restore(&self, _snapshot: &cog_core::AgentCheckpoint) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn continue_(
            &self,
            _input: serde_json::Value,
        ) -> cog_core::SFResult<serde_json::Value> {
            Ok(serde_json::Value::Null)
        }
        async fn steer(&self, _instruction: String) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn abort(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn reset(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn state(&self) -> cog_core::SFResult<cog_core::AgentState> {
            Ok(cog_core::AgentState::Idle)
        }
        async fn wait_for_idle(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn restore_from_id(&self, _checkpoint_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, mut producer) = cog_core::AssistantMessageEventStream::with_capacity(1);
            producer.end(cog_core::ChatResponse::default());
            Ok(stream)
        }
        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            self.chat_stream(&[], &cog_core::ChatOptions::default())
                .await
        }
        async fn read_board(
            &self,
            _task_id: &str,
            _field: &str,
        ) -> cog_core::SFResult<Option<String>> {
            Ok(None)
        }
        async fn write_board(
            &self,
            _task_id: &str,
            _field: &str,
            _value: &str,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }
        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    fn empty_planner_output() -> PlannerOutput {
        PlannerOutput {
            summary: String::new(),
            plan: serde_json::json!({}),
            sub_tasks: Vec::new(),
        }
    }

    fn eval_result(verdict: Verdict, score: u32) -> EvaluationResult {
        EvaluationResult {
            verdict,
            feedback: String::new(),
            score: Some(score),
            criteria: Vec::new(),
            details: None,
        }
    }

    fn branch(branch_id: u32, score: u32, artifact_name: &str) -> PgeBranchResult {
        let mut artifacts = Vec::new();
        if !artifact_name.is_empty() {
            artifacts.push(crate::squad::pge::types::Artifact {
                name: artifact_name.into(),
                content: String::new(),
                artifact_type: "code".into(),
            });
        }
        PgeBranchResult {
            branch_id,
            plan: empty_planner_output(),
            generation: GeneratorOutput {
                content: serde_json::Value::Null,
                artifacts,
            },
            evaluation: eval_result(
                if score >= 80 {
                    Verdict::Pass
                } else {
                    Verdict::Fail
                },
                score,
            ),
        }
    }

    fn roundtable_for_merge(strategy: BranchMergeStrategy) -> PgeRoundtable {
        let config = PgeRoundtableConfig {
            branch_merge_strategy: strategy,
            ..Default::default()
        };
        PgeRoundtable::new(
            config,
            PlannerActor::new(std::sync::Arc::new(MockAgent)),
            GeneratorActor::new(std::sync::Arc::new(MockAgent)),
            EvaluatorActor::new(std::sync::Arc::new(MockAgent)),
        )
    }

    #[test]
    fn merge_best_score_selects_highest() {
        let rt = roundtable_for_merge(BranchMergeStrategy::BestScore);
        let branches = vec![branch(0, 40, ""), branch(1, 90, ""), branch(2, 60, "")];
        let merged = rt.merge_best_score(&branches);
        assert_eq!(merged.evaluation.score, Some(90));
    }

    #[test]
    fn merge_majority_vote_selects_majority_verdict() {
        let rt = roundtable_for_merge(BranchMergeStrategy::MajorityVote);
        let branches = vec![branch(0, 40, ""), branch(1, 85, ""), branch(2, 90, "")];
        let merged = rt.merge_majority_vote(&branches);
        assert!(matches!(merged.evaluation.verdict, Verdict::Pass));
        assert_eq!(merged.evaluation.score, Some(90));
    }

    #[test]
    fn merge_union_artifacts_collects_unique_artifacts() {
        let rt = roundtable_for_merge(BranchMergeStrategy::UnionArtifacts);
        let branches = vec![
            branch(0, 40, "a.rs"),
            branch(1, 90, "b.rs"),
            branch(2, 60, "a.rs"),
        ];
        let merged = rt.merge_union_artifacts(&branches);
        assert_eq!(merged.evaluation.score, Some(90));
        let names: Vec<&str> = merged
            .generation
            .artifacts
            .iter()
            .map(|a| a.name.as_str())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"a.rs"));
        assert!(names.contains(&"b.rs"));
    }
}
