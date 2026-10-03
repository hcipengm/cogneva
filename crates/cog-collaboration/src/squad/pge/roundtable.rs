use crate::actors::{
    EvaluatorActor, GeneratorActor, MergeResult, MergerActor, ModeratorActor, PlannerActor,
};
use crate::squad::pge::context_board::ContextBoard;
use crate::squad::pge::stall::{
    degenerate_loop_feedback, ProgressSignals, StallDetector, StallVerdict,
};
use crate::squad::pge::types::{
    best_scored, Artifact, BranchMergeStrategy, ConsensusFloor, Criterion, EvaluationResult,
    GeneratorOutput, MergeSummary, PgeBranchResult, PgeRoundtableIteration, PlannerOutput,
    RoundOutcome, StopCause, StoppedProduct, Verdict,
};
use std::sync::Arc;
use tracing::info;

pub struct PgeRoundtableConfig {
    pub max_iterations: u32,
    /// The score a `Pass` must reach for the debate to confirm, as a fraction
    /// of 100: two consecutive rounds passing with each score at or above it.
    /// Applied to the judgement's own score, so a `Pass` below the bar — and a
    /// `Pass` that reported no score, which cannot be shown to reach it — does
    /// not confirm and the debate keeps going. At the default 0.8 this is the
    /// same bar the evaluator is asked to pass on, so it changes nothing until
    /// it is raised, and raising it means exactly this: harder to end early.
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
    /// Optional moderator agent. When set, it is consulted at the end of every
    /// round that did not end on a consensus, a deterministic stop, or a stall;
    /// it reviews the full debate history and decides whether to continue,
    /// change strategy, accept a partial result, or escalate. There is no
    /// iteration threshold: the rounds that need a moderator are usually the
    /// early ones, and those are exactly the rounds whose history is still
    /// small enough to send whole.
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
    /// Optional LLM provider passed to
    /// [`cog_core::AgentManager::create_agent`].
    pub llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    /// Optional merger agent used when `branch_merge_strategy` is
    /// [`BranchMergeStrategy::Custom`].
    pub merger: Option<MergerActor>,
    /// Consecutive non-progress iterations that declare the debate a
    /// degenerate loop (evaluation score and criteria both flat) and stop it
    /// early. The criterion is whether spend buys progress, never a flat
    /// spend cap. 0 disables stall detection.
    pub stall_threshold: u32,
    /// When true, a consensus Pass is re-judged once in a fresh context
    /// (no debate history) before the roundtable accepts it. On conflict the
    /// reviewer wins; both verdicts are recorded in the final evaluation.
    pub independent_review: bool,
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
            stall_threshold: 2,
            independent_review: true,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PgeRoundtableResult {
    pub iterations: u32,
    pub consensus_reached: bool,
    pub final_plan: PlannerOutput,
    pub final_generation: GeneratorOutput,
    /// What the last round produced: a judgement, or the reason it stopped and
    /// whether there was anything to judge. Read this instead of assuming a
    /// verdict exists — a debate that stopped with no product has none, and a
    /// fabricated one would be indistinguishable from a judgement.
    pub final_outcome: RoundOutcome,
    pub history: Vec<PgeRoundtableIteration>,
    /// Final state of the shared context board after all debate rounds.
    /// `None` if no context board was configured.
    pub context_board: Option<serde_json::Value>,
    /// The composed cause when the debate ended on something no further round
    /// can change — a deterministic environment/protocol failure, or a loop
    /// whose rounds bought no progress. `None` when it ended any other way.
    /// It names the role that actually failed; a reader that re-derives the
    /// cause from `final_generation` alone reports a generator failure for a
    /// debate that stopped because the judge or the reviewer spent its budget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
}

/// Multi-agent roundtable debate orchestrator.
/// Directly holds [`PlannerActor`], [`GeneratorActor`], and [`EvaluatorActor`]
/// — no internal wrapping of raw [`cog_core::Agent`]s.
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

    /// Run the roundtable with a structured [`cog_core::Task`].
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
        let mut stall = StallDetector::new(self.config.stall_threshold);

        let mut last_judgement: Option<EvaluationResult> = None;
        // The previous round's answer to the consensus question, whatever it
        // was: a round that confirmed, or one that did not, resets it. A round
        // that reached no judgement is not a round that agreed with the one
        // before it, so it does not carry a confirmation across the gap.
        let mut prev_confirmed = false;
        let mut board = self.init_board().await;
        let mut terminal_reason: Option<String> = None;

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
            let prev_eval_ref = last_judgement
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
                        "outcome": &h.outcome,
                    })
                })
                .collect();

            // The evaluator is shown the whole run so far through this section;
            // measure it where both the sequential and the parallel path take it.
            let (history_bytes, latest_bytes) =
                crate::observable::eval_doc_byte_ends(&eval_history);
            crate::observable::global_observable()
                .record_eval_doc_bytes(history_bytes, latest_bytes);

            let (plan, generation, outcome, branches, merge_summary) =
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
                    let (p, g, o) = self
                        .run_sequential_iteration(
                            task,
                            iteration,
                            prev_eval_ref2,
                            prev_gen_ref,
                            &eval_history,
                            &board,
                        )
                        .await;
                    (p, g, o, Vec::new(), None)
                };

            let plan_json = serde_json::to_value(&plan).unwrap_or_default();
            let generation_json = serde_json::to_value(&generation).unwrap_or_default();
            let outcome_json = serde_json::to_value(&outcome).unwrap_or_default();
            board["latest_plan"] = plan_json.clone();
            self.persist_field("latest_plan", &plan_json).await;
            board["latest_generation"] = generation_json.clone();
            self.persist_field("latest_generation", &generation_json)
                .await;
            board["latest_outcome"] = outcome_json.clone();
            self.persist_field("latest_outcome", &outcome_json).await;
            board["round"] = serde_json::json!(iteration);
            self.persist_field("round", &serde_json::json!(iteration))
                .await;

            history.push(PgeRoundtableIteration {
                iteration,
                plan: plan.clone(),
                generation: generation.clone(),
                outcome: outcome.clone(),
                branches,
                merge_summary,
            });

            // 这一轮的终止判定由产出它的那条路径做出（顺序路径 / 并行分支），
            // 这里只读结论。三个角色各自的闸放在那里，是因为只有那一层知道
            // 谁被真正调用了：占位的空生成与真的空产出同形，在这里按对象反推
            // 就会把一个从未被调用的角色报成责任人。
            if let Some(reason) = outcome.deterministic_stop() {
                tracing::warn!(
                    iteration,
                    "Roundtable round stopped on a deterministic failure; stopping debate"
                );
                terminal_reason = Some(crate::squad::classify::declare_for(reason));
                break;
            }

            // 没有判定的一轮没有进展可言：读数记成"没有分数、没有判据"，
            // 不借来一个分数。真产物存在但法官失败了的那种（Unjudged）也一样
            // ——评委这一环空转，贡献不了任何进展证据。
            let signals = ProgressSignals::from_outcome(&outcome);

            // Degenerate-loop guard: consecutive iterations the evaluator
            // judged no better mean more rounds only rephrase the same failure.
            // Mark the run and stop before spending more.
            let judged_pass = matches!(outcome.judgement().map(|e| e.verdict), Some(Verdict::Pass));
            if !judged_pass && matches!(stall.observe(signals), StallVerdict::Stalled) {
                tracing::warn!(
                    iteration,
                    "degenerate debate loop detected; stopping roundtable early"
                );
                // 有判词就把它的原文带上：判据说的是"没进展"，而"哪一步
                // 没进展"只有这一轮的判词说得清。
                let detail = match outcome.judgement() {
                    Some(evaluation) => format!("; stopped early: {}", evaluation.feedback),
                    None => " and no round reached a judgement".to_string(),
                };
                terminal_reason = Some(degenerate_loop_feedback(format!(
                    "{} consecutive iterations bought no progress \
                     (evaluation score and criteria both flat); stopped early{detail}",
                    self.config.stall_threshold
                )));
                break;
            }

            // Consensus needs a judgement to be about: a round that reached none
            // carries none, and cannot carry the debate. The moderator below
            // still sees it — a generator that keeps answering with nothing is
            // exactly the moment its ChangeStrategy / Escalate call matters.
            //
            // Consensus is a reading of the round's own judgement, so the
            // configured threshold is applied here rather than left as a line
            // in the moderator's prompt: two consecutive rounds must pass, and
            // each pass must clear the floor those two rounds declare. A pass
            // the evaluator itself scored below the bar cannot confirm the
            // debate, and neither can one that reported no score to compare —
            // in both cases the threshold says the debate is not over yet.
            let confirmed = match outcome.judgement() {
                Some(judgement) => {
                    let floor = judgement.consensus_floor(self.config.consensus_threshold);
                    crate::observable::global_observable()
                        .record_consensus_floor(consensus_floor_cell(floor));
                    floor == ConsensusFloor::Met
                }
                None => {
                    crate::observable::global_observable()
                        .record_consensus_floor(crate::observable::CONSENSUS_FLOOR_NOT_JUDGED);
                    false
                }
            };

            if confirmed && prev_confirmed {
                consensus_reached = true;
                break;
            }
            prev_confirmed = confirmed;

            // --- Moderator intervention ---
            if let Some(ref moderator) = self.config.moderator {
                // The board was written from this round a few lines up, and
                // `history` was appended with the same round, so the moderator
                // is reading it twice. The debate is the carrier that has to
                // arrive whole here — it is what the moderator decides on.
                let moderator_board = Self::board_without_mirrored_round(
                    &board,
                    &serde_json::json!({
                        "plan": &plan_json,
                        "generation": &generation_json,
                        "outcome": &outcome_json,
                    }),
                );
                let mod_output = moderator
                    .moderate(
                        task,
                        &history,
                        &moderator_board,
                        self.config.consensus_threshold,
                    )
                    .await;
                tracing::info!(
                    iteration,
                    decision = ?mod_output.decision,
                    "Moderator decision"
                );
                // Mapped before the effects so every decision has to be named
                // here: a new variant breaks the build rather than silently
                // going uncounted. Only rounds that reach this block write a
                // cell — one that ended earlier (a deterministic stop, a stall,
                // a second consecutive Pass) reached no moderator at all, and
                // writing it as one would file a round's own ending under the
                // moderator's name.
                let outcome = match mod_output.decision {
                    crate::actors::moderator::ModeratorDecision::Continue => {
                        crate::observable::MODERATOR_CONTINUE
                    }
                    crate::actors::moderator::ModeratorDecision::ChangeStrategy => {
                        crate::observable::MODERATOR_CHANGE_STRATEGY
                    }
                    crate::actors::moderator::ModeratorDecision::AcceptPartial => {
                        crate::observable::MODERATOR_ACCEPT_PARTIAL
                    }
                    crate::actors::moderator::ModeratorDecision::Escalate => {
                        crate::observable::MODERATOR_ESCALATE
                    }
                };
                crate::observable::global_observable().record_moderator_consulted(outcome);
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
            } else {
                // The other reason a round ends with no moderator decision: the
                // deployment has none configured. Without this cell the four
                // decisions above would be unreadable — a debate with no
                // moderator and a moderator that always said Continue write the
                // same (no) series.
                crate::observable::global_observable()
                    .record_moderator_consulted(crate::observable::MODERATOR_NOT_ASKED_DISABLED);
            }

            if let Some(judgement) = outcome.judgement() {
                // 一轮没有判定不该抹掉上一轮的判词：下一轮的计划者要看的
                // 是最后一次真实的评价，不是"这一轮什么都没说"。共识那边
                // 相反：确认必须来自相邻两轮，中间的空白轮由上面那段的
                // `prev_confirmed` 归零，不留着旧值跨过去。
                last_judgement = Some(judgement.clone());
            }
        }

        let mut last = history
            .last()
            .cloned()
            .unwrap_or_else(|| PgeRoundtableIteration {
                iteration: 0,
                plan: PlannerOutput {
                    summary: String::new(),
                    plan: serde_json::json!({}),
                    sub_tasks: Vec::new(),
                    acceptance_criteria: Vec::new(),
                    targets: Vec::new(),
                },
                generation: GeneratorOutput::none(),
                outcome: RoundOutcome::Stopped {
                    cause: StopCause::NotAttempted,
                    product: StoppedProduct::None,
                },
                branches: Vec::new(),
                merge_summary: None,
            });

        // Independent review gate: every evaluator pass above saw the debate
        // history, so consensus is self-assessment. Re-judge the final output
        // in a fresh context (no history) before accepting; on conflict the
        // reviewer wins. Both verdicts stay on the record.
        //
        // The review exists to confirm a consensus *Pass*. A round the moderator
        // accepted as a partial is not a Pass, and a round nobody judged has no
        // claim to re-check: in both cases there is nothing to confirm and the
        // fresh-context judge is not paid for.
        if consensus_reached {
            if let RoundOutcome::Judged { evaluation } = &mut last.outcome {
                if matches!(evaluation.verdict, Verdict::Pass) {
                    let observable = crate::observable::global_observable();
                    if !self.config.independent_review {
                        observable.record_independent_review(
                            crate::observable::INDEPENDENT_REVIEW_NOT_ASKED_DISABLED,
                        );
                    } else {
                        let criteria: Vec<&str> = last
                            .plan
                            .acceptance_criteria
                            .iter()
                            .map(|s| s.as_str())
                            .collect();
                        // Fresh context means fresh: the board is withheld, not
                        // just the history. Every round writes its plan, its
                        // generation and its verdict into the board before the
                        // review is reached, and the evaluator's context builder
                        // hands the whole board to whichever evaluator runs — so
                        // a reviewer given the board is handed the consensus it
                        // is supposed to be checking, independently of the
                        // history argument right above.
                        let mut review = self
                            .evaluator
                            .evaluate(
                                task,
                                &serde_json::to_value(&last.plan).unwrap_or_default(),
                                &serde_json::to_value(&last.generation).unwrap_or_default(),
                                &[],
                                &criteria,
                                None,
                            )
                            .await;
                        review.enforce_criteria_evidence(!criteria.is_empty());
                        let review_json = serde_json::to_value(&review).unwrap_or_default();
                        if let Some(reason) = review.terminal_env_failure_reason() {
                            tracing::warn!(
                                "Roundtable independent reviewer reported terminal environment failure"
                            );
                            // Paid for and answered nothing: neither verdict cell
                            // may claim it, and without this one the call would
                            // be invisible except as the run's terminal reason.
                            observable.record_independent_review(
                                crate::observable::INDEPENDENT_REVIEW_NOT_ANSWERED,
                            );
                            consensus_reached = false;
                            evaluation.verdict = Verdict::Fail;
                            evaluation.feedback =
                                crate::squad::classify::declare_for(reason.clone());
                            terminal_reason = Some(reason);
                        } else {
                            let agreed = matches!(review.verdict, Verdict::Pass);
                            observable.record_independent_review(if agreed {
                                crate::observable::INDEPENDENT_REVIEW_AGREED
                            } else {
                                crate::observable::INDEPENDENT_REVIEW_REJECTED
                            });
                            if !agreed {
                                consensus_reached = false;
                                evaluation.verdict = Verdict::Fail;
                                evaluation.feedback = format!(
                                    "independent reviewer rejected the consensus: {}",
                                    review.feedback
                                );
                            }
                        }
                        let mut details =
                            evaluation.details.take().unwrap_or(serde_json::json!({}));
                        details["independent_review"] = review_json;
                        evaluation.details = Some(details);
                    }
                }
            }
        }

        PgeRoundtableResult {
            iterations: history.len() as u32,
            consensus_reached,
            final_plan: last.plan,
            final_generation: last.generation,
            final_outcome: last.outcome,
            history,
            context_board: Some(board),
            terminal_reason,
        }
    }

    /// The stop a round reaches after generating and before evaluating, if any.
    ///
    /// A round whose generator already named a deterministic cause, or answered
    /// with an empty envelope, has nothing for a judge to read. Asking one buys
    /// an inference whose only possible content is "there is nothing here", and
    /// files it — as a verdict — against the attempt. Both the sequential loop
    /// and every parallel branch come through here, so neither can grow the
    /// gate the other is missing.
    fn stop_before_evaluation(generation: &GeneratorOutput) -> Option<RoundOutcome> {
        if let Some(reason) = generation.terminal_env_failure_reason() {
            return Some(RoundOutcome::Stopped {
                cause: StopCause::Deterministic { reason },
                product: StoppedProduct::None,
            });
        }
        if generation.is_empty_envelope() {
            return Some(RoundOutcome::empty_envelope());
        }
        None
    }

    /// The outcome of an evaluation: a judgement, unless the judge itself
    /// failed. A judge that spent its own iteration budget judged nothing —
    /// recording that as a verdict on the attempt blames the product for the
    /// judge's problem. The product is real either way, hence `Unjudged`.
    fn outcome_from_evaluation(evaluation: EvaluationResult) -> RoundOutcome {
        match evaluation.terminal_env_failure_reason() {
            Some(reason) => RoundOutcome::Stopped {
                cause: StopCause::Deterministic { reason },
                product: StoppedProduct::Unjudged,
            },
            None => RoundOutcome::Judged { evaluation },
        }
    }

    /// The board a role is shown, with the round it already reads elsewhere in
    /// the same document taken out.
    ///
    /// Every round is written into the board (`latest_plan` / `latest_generation`
    /// / `latest_outcome`) and is handed to the next roles through a field of
    /// their own as well: the evaluator's `history` ends on it, the planner's
    /// `previous_generation` *is* its generation, the moderator's `history` was
    /// just appended with it. Sending the board whole makes one round arrive
    /// twice, and the board's copy is the one that grows with every round, so it
    /// is the copy no prefix cache can absorb.
    ///
    /// `round` names the round's own carriers by the board's field names — a
    /// `plan` / `generation` / `outcome` key, matching `latest_plan` and the
    /// rest. A field is removed only when the board's copy is byte-equal to the
    /// one the caller named: a board shared with another writer
    /// ([`board_store`](PgeRoundtableConfig::board_store)) can hold a *different*
    /// round, and there the board is that round's only copy, so removing it would
    /// take content away rather than a duplicate. A field the caller did not name
    /// is not compared at all — the planner has no second carrier for the
    /// previous *plan* — and "not compared" must not read as "kept".
    fn board_without_mirrored_round(
        board: &serde_json::Value,
        round: &serde_json::Value,
    ) -> serde_json::Value {
        let (trimmed, dropped, kept) = board_mirror_trim(board, round);
        crate::observable::global_observable().record_board_mirror_bytes(dropped, kept);
        trimmed
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
    ) -> (PlannerOutput, GeneratorOutput, RoundOutcome) {
        // The planner reads the previous generation twice over — as its own
        // argument and as the board's `latest_generation`. Its previous *plan*
        // and full previous outcome have no other carrier, so only the
        // generation is named here and the rest of the board passes through.
        let planner_round = match prev_gen_ref {
            Some(generation) => serde_json::json!({ "generation": generation }),
            None => serde_json::Value::Null,
        };
        let planner_board = Self::board_without_mirrored_round(board, &planner_round);
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
                Some(&planner_board),
            )
            .await;

        // 计划侧已经终止：生成与评估都不该买。占位生成只是让返回类型成立，
        // 真因由 outcome 自己带着，读侧不看那个空对象。
        if let Some(reason) = plan.terminal_env_failure_reason() {
            return (
                plan,
                GeneratorOutput::none(),
                RoundOutcome::Stopped {
                    cause: StopCause::Deterministic { reason },
                    product: StoppedProduct::None,
                },
            );
        }

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

        if let Some(outcome) = Self::stop_before_evaluation(&generation) {
            tracing::warn!(
                iteration,
                "Roundtable round produced nothing to judge; not paying for an evaluation"
            );
            return (plan, generation, outcome);
        }

        let generation_json = serde_json::to_value(&generation).unwrap_or_default();
        let criteria: Vec<&str> = plan
            .acceptance_criteria
            .iter()
            .map(|s| s.as_str())
            .collect();
        // `eval_history` ends on the round the board was last written with, so
        // the board is carrying that same round a second time.
        let evaluation_board = Self::board_without_mirrored_round(
            board,
            eval_history.last().unwrap_or(&serde_json::Value::Null),
        );
        let mut evaluation = self
            .evaluator
            .evaluate(
                task,
                &plan_json,
                &generation_json,
                eval_history,
                &criteria,
                Some(&evaluation_board),
            )
            .await;
        evaluation.enforce_criteria_evidence(!criteria.is_empty());
        evaluation.enforce_change_artifact_integrity(generation.change_artifact_defect(task));

        (plan, generation, Self::outcome_from_evaluation(evaluation))
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
        RoundOutcome,
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
                // The same trims as the sequential path, and for the same
                // reasons. One per branch, because each branch pays for its own
                // document.
                let planner_round = match prev_gen.as_ref() {
                    Some(generation) => serde_json::json!({ "generation": generation }),
                    None => serde_json::Value::Null,
                };
                let planner_board = Self::board_without_mirrored_round(&board, &planner_round);
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
                        Some(&planner_board),
                    )
                    .await;

                // 与顺序路径同一条闸：计划侧已终止的分支不再买生成与评估。
                if let Some(reason) = plan.terminal_env_failure_reason() {
                    return PgeBranchResult {
                        branch_id,
                        plan,
                        generation: GeneratorOutput::none(),
                        outcome: RoundOutcome::Stopped {
                            cause: StopCause::Deterministic { reason },
                            product: StoppedProduct::None,
                        },
                    };
                }

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

                if let Some(outcome) = Self::stop_before_evaluation(&generation) {
                    tracing::warn!(
                        iteration,
                        branch_id,
                        "Roundtable branch produced nothing to judge; not paying for an evaluation"
                    );
                    return PgeBranchResult {
                        branch_id,
                        plan,
                        generation,
                        outcome,
                    };
                }

                let generation_json = serde_json::to_value(&generation).unwrap_or_default();
                let criteria: Vec<&str> = plan
                    .acceptance_criteria
                    .iter()
                    .map(|s| s.as_str())
                    .collect();
                // `eval_history` ends on the round the board was last written
                // with, so the board is carrying that same round a second time.
                let evaluation_board = Self::board_without_mirrored_round(
                    &board,
                    eval_history.last().unwrap_or(&serde_json::Value::Null),
                );
                let mut evaluation = evaluator
                    .evaluate(
                        &task,
                        &plan_json,
                        &generation_json,
                        &eval_history,
                        &criteria,
                        Some(&evaluation_board),
                    )
                    .await;
                evaluation.enforce_criteria_evidence(!criteria.is_empty());
                evaluation
                    .enforce_change_artifact_integrity(generation.change_artifact_defect(&task));

                PgeBranchResult {
                    branch_id,
                    plan,
                    generation,
                    outcome: Self::outcome_from_evaluation(evaluation),
                }
            });
            handles.push(handle);
        }

        // Await all branches. If every branch spawn failed, fall back to sequential.
        if handles.is_empty() {
            let (p, g, o) = self
                .run_sequential_iteration(
                    task,
                    iteration,
                    prev_eval_ref,
                    prev_gen_ref,
                    eval_history,
                    board,
                )
                .await;
            return (p, g, o, Vec::new(), None);
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
            selected_branch_id: merge_result.selected_branch_id,
            strategy: self.config.branch_merge_strategy,
            reasoning: merge_result.reasoning.clone(),
        });

        (
            merge_result.plan,
            merge_result.generation,
            merge_result.outcome,
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

    /// The branches that reached a judgement: the only ones a merge can rank.
    /// A branch with no product has no score and no verdict, so letting it into
    /// "pick the highest score" reads its emptiness as a real bad review and
    /// can hand back a branch nobody ever judged.
    fn judged_branches(branches: &[PgeBranchResult]) -> Vec<&PgeBranchResult> {
        branches
            .iter()
            .filter(|b| b.outcome.judgement().is_some())
            .collect()
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
                    acceptance_criteria: Vec::new(),
                    targets: Vec::new(),
                },
                generation: GeneratorOutput::none(),
                outcome: RoundOutcome::Stopped {
                    cause: StopCause::NotAttempted,
                    product: StoppedProduct::None,
                },
                selected_branch_id: None,
                reasoning: "No branches".into(),
            };
        }

        if Self::judged_branches(branches).is_empty() {
            return Self::merge_without_judgement(branches);
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

    /// The round's result when no branch reached a judgement: the cause is the
    /// branches' own, and the product carried out is the one that actually
    /// exists, so "was there anything to judge" follows the product rather than
    /// being counted separately.
    fn merge_without_judgement(branches: &[PgeBranchResult]) -> MergeResult {
        // A branch that produced something but whose judge failed is the one
        // worth carrying out; failing that, any branch at all — the plan and
        // generation of a stopped round are the round's record of what it got
        // to, not a deliverable.
        let carried = branches
            .iter()
            .find(|b| {
                matches!(
                    &b.outcome,
                    RoundOutcome::Stopped {
                        product: StoppedProduct::Unjudged,
                        ..
                    }
                )
            })
            .or_else(|| branches.first());
        // 确定性原因优先带出去：它自带"重试无用"的语义，比生成器交了个空
        // 信封更该被上层看到。
        //
        // 原因与产物取自不同的分支，这是有意的：这是**整轮**的结论，不是某一支的
        // ——产物是那一支留下的东西，而"这一轮里出现了确定性失败"是另一支也能
        // 贡献的事实，把它藏起来会让整轮看起来还能重试。所以原因要带上它是谁说的，
        // 否则推理串里只写 carried 分支，读的人会以为原因也出自它。
        let cause = branches
            .iter()
            .find_map(|b| match &b.outcome {
                RoundOutcome::Stopped { cause, .. } if cause.is_deterministic() => {
                    Some((b.branch_id, cause.clone()))
                }
                _ => None,
            })
            .or_else(|| {
                branches.iter().find_map(|b| match &b.outcome {
                    RoundOutcome::Stopped { cause, .. } => Some((b.branch_id, cause.clone())),
                    RoundOutcome::Judged { .. } => None,
                })
            });
        let (cause_from, cause) = match cause {
            Some((id, cause)) => (Some(id), cause),
            None => (None, StopCause::NotAttempted),
        };
        let product = match carried.map(|b| &b.outcome) {
            Some(RoundOutcome::Stopped { product, .. }) => *product,
            _ => StoppedProduct::None,
        };
        let selected_branch_id = carried.map(|b| b.branch_id);

        match carried {
            Some(branch) => MergeResult {
                reasoning: match cause_from {
                    Some(id) if id != branch.branch_id => format!(
                        "No branch reached a judgement; carried branch {}, stop cause from branch {}",
                        branch.branch_id, id
                    ),
                    _ => format!(
                        "No branch reached a judgement; carried branch {}",
                        branch.branch_id
                    ),
                },
                plan: branch.plan.clone(),
                generation: branch.generation.clone(),
                outcome: RoundOutcome::Stopped { cause, product },
                selected_branch_id,
            },
            None => MergeResult {
                plan: PlannerOutput {
                    summary: String::new(),
                    plan: serde_json::json!({}),
                    sub_tasks: Vec::new(),
                    acceptance_criteria: Vec::new(),
                    targets: Vec::new(),
                },
                generation: GeneratorOutput::none(),
                outcome: RoundOutcome::Stopped {
                    cause,
                    product: StoppedProduct::None,
                },
                selected_branch_id: None,
                reasoning: "No branches".into(),
            },
        }
    }

    fn merge_best_score(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let best = best_scored(&Self::judged_branches(branches))
            .cloned()
            .expect("the caller checked at least one branch reached a judgement");

        MergeResult {
            reasoning: format!("Selected branch {} by best score", best.branch_id),
            plan: best.plan,
            generation: best.generation,
            outcome: best.outcome,
            selected_branch_id: Some(best.branch_id),
        }
    }

    fn merge_majority_vote(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let judged = Self::judged_branches(branches);
        let mut counts = std::collections::HashMap::new();
        for b in &judged {
            if let Some(verdict) = b.outcome.judgement().map(|e| e.verdict) {
                *counts.entry(verdict).or_insert(0) += 1;
            }
        }
        let majority_verdict = counts
            .into_iter()
            .max_by_key(|(_, c)| *c)
            .map(|(v, _)| v)
            .unwrap_or(Verdict::Fail);

        // 只在与多数判决一致的分支里按分挑。多数判决是从这批分支自己数出来的，所以
        // 至少有一条分支持该判决，这里挑不空。刻意不设"挑空就退回某条分支"的兜底：
        // 那等于把一条判决与合并所用判决相反的分支选上来，而 `reasoning` 里写的仍是
        // 多数判决，读的人无从发现。
        let agreeing: Vec<&PgeBranchResult> = judged
            .iter()
            .copied()
            .filter(|b| b.outcome.judgement().map(|e| e.verdict) == Some(majority_verdict))
            .collect();
        let best = best_scored(&agreeing).cloned().expect(
            "the majority verdict was counted over these same branches, so one of them holds it",
        );

        MergeResult {
            reasoning: format!(
                "Majority verdict {:?}; selected branch {} by best score",
                majority_verdict, best.branch_id
            ),
            plan: best.plan,
            generation: best.generation,
            outcome: best.outcome,
            selected_branch_id: Some(best.branch_id),
        }
    }

    fn merge_union_artifacts(&self, branches: &[PgeBranchResult]) -> MergeResult {
        let best = best_scored(&Self::judged_branches(branches))
            .cloned()
            .expect("the caller checked at least one branch reached a judgement");

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
            outcome: best.outcome,
            selected_branch_id: Some(best.branch_id),
        }
    }
}

pub fn parse_planner_output(value: &serde_json::Value, goal: &str) -> PlannerOutput {
    // Same guard as the generator: an empty plan is a valid plan, so a spent
    // iteration budget that is not named here travels downstream as a planner
    // that looked at the goal and found nothing to do.
    if let Some(reason) = crate::squad::pge::types::iteration_budget_exhausted_reason(value) {
        return PlannerOutput {
            summary: format!("Plan for: {goal}"),
            plan: serde_json::Value::String(reason),
            sub_tasks: Vec::new(),
            acceptance_criteria: Vec::new(),
            targets: Vec::new(),
        };
    }
    serde_json::from_value(value.clone()).unwrap_or_else(|_| PlannerOutput {
        summary: format!("Plan for: {}", goal),
        plan: value.clone(),
        sub_tasks: Vec::new(),
        acceptance_criteria: Vec::new(),
        targets: Vec::new(),
    })
}

pub fn parse_generator_output(value: &serde_json::Value) -> GeneratorOutput {
    repair_change_artifacts(parse_generator_output_as_written(value))
}

/// Read the generator's result exactly as it was written.
fn parse_generator_output_as_written(value: &serde_json::Value) -> GeneratorOutput {
    // A spent iteration budget is checked before the shape parses: the sentinel
    // is a valid JSON object that simply has none of this role's fields, so
    // every later branch would read it as a generator that returned nothing.
    if let Some(reason) = crate::squad::pge::types::iteration_budget_exhausted_reason(value) {
        return GeneratorOutput {
            content: serde_json::Value::String(reason),
            artifacts: Vec::new(),
        };
    }
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

/// Repair the structure of every change artifact from its body.
///
/// The body is the part of a diff that carries the change; the `@@` counts are
/// arithmetic over it and the final newline is punctuation the parser requires.
/// A generator that writes the right body under a wrong count, or closes it
/// without a terminating newline, produces a diff that only the apply gate can
/// reject, and it rejects it with a line number — a verdict the generator
/// cannot act on, so the same mistake recurs on every retry. Repairing here, at
/// the one place model output becomes an artifact, costs no tokens and changes
/// nothing about what the diff says; whether the body applies to the file at the
/// declared start line remains the apply/compile gate's call.
///
/// Only change artifacts are touched: an artifact that merely contains `@@`
/// lines is not a diff and must survive verbatim.
fn repair_change_artifacts(mut output: GeneratorOutput) -> GeneratorOutput {
    for artifact in &mut output.artifacts {
        if !artifact.is_change() {
            continue;
        }
        let defect = cog_core::diff_structural_defect(&artifact.content);
        if let Some(repaired) = cog_core::normalize_diff_hunk_headers(&artifact.content) {
            info!(
                artifact = %artifact.name,
                defect = ?defect,
                "repaired diff structure from the body"
            );
            artifact.content = repaired;
        }
    }
    output
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

/// The cell a round's consensus-floor reading is published under. Exhaustive on
/// purpose: a new reading of the floor has to be named here rather than
/// silently folded into an existing cell.
fn consensus_floor_cell(floor: ConsensusFloor) -> &'static str {
    match floor {
        ConsensusFloor::Met => crate::observable::CONSENSUS_FLOOR_MET,
        ConsensusFloor::NotMet => crate::observable::CONSENSUS_FLOOR_NOT_MET,
        ConsensusFloor::ScoreAbsent => crate::observable::CONSENSUS_FLOOR_SCORE_ABSENT,
        ConsensusFloor::NotAPass => crate::observable::CONSENSUS_FLOOR_NOT_A_PASS,
    }
}

/// What one board field costs the document: the field's own bytes, key
/// included. The punctuation around it is not counted — what the reading is
/// for is "how much did the prompt shrink", and every removed field takes the
/// same two characters with it.
fn board_mirror_bytes(field: &str, value: &serde_json::Value) -> usize {
    field.len() + value.to_string().len()
}

/// The board without its copy of `round`, and what that cost either way: the
/// bytes of the copies removed, and of the copies that were a different round
/// and stayed. Pure, so the byte arithmetic is testable on its own; the caller
/// is what publishes it. See
/// [`PgeRoundtable::board_without_mirrored_round`] for what the two cases mean.
fn board_mirror_trim(
    board: &serde_json::Value,
    round: &serde_json::Value,
) -> (serde_json::Value, usize, usize) {
    const MIRRORS: [(&str, &str); 3] = [
        ("latest_plan", "plan"),
        ("latest_generation", "generation"),
        ("latest_outcome", "outcome"),
    ];
    let mut trimmed = board.clone();
    let mut dropped = 0usize;
    let mut kept = 0usize;
    for (field, key) in MIRRORS {
        let (Some(expected), Some(current)) = (round.get(key), board.get(field)) else {
            continue;
        };
        // What the document gains by keeping it, or loses by removing it.
        let bytes = board_mirror_bytes(field, current);
        if current == expected {
            dropped += bytes;
            if let Some(object) = trimmed.as_object_mut() {
                object.remove(field);
            }
        } else {
            kept += bytes;
        }
    }
    (trimmed, dropped, kept)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockAgent {
        responses: std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>,
        /// How many times this actor was asked for anything. The only way to
        /// observe "this role was never paid for" from outside.
        calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        /// What each call was handed. The actor decides which document a role
        /// reads, so "what did this role actually receive" is observable here
        /// and nowhere else.
        inputs: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    }

    impl MockAgent {
        /// Single response repeated forever.
        fn fixed(value: serde_json::Value) -> Self {
            Self {
                responses: std::sync::Mutex::new(std::collections::VecDeque::from([value])),
                calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                inputs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        /// Responses consumed in order; the last one repeats once exhausted.
        fn sequence(values: Vec<serde_json::Value>) -> Self {
            Self {
                responses: std::sync::Mutex::new(values.into()),
                calls: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                inputs: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn inputs(&self) -> Vec<serde_json::Value> {
            self.inputs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn next_response(&self) -> serde_json::Value {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut responses = self.responses.lock().unwrap();
            match responses.len() {
                0 => serde_json::Value::Null,
                1 => responses
                    .front()
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
                _ => responses.pop_front().unwrap_or(serde_json::Value::Null),
            }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for MockAgent {
        async fn prompt(&self, input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            self.inputs
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(input);
            Ok(self.next_response())
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
            Ok(self.next_response())
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
            acceptance_criteria: Vec::new(),
            targets: Vec::new(),
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
            outcome: RoundOutcome::Judged {
                evaluation: eval_result(
                    if score >= 80 {
                        Verdict::Pass
                    } else {
                        Verdict::Fail
                    },
                    score,
                ),
            },
        }
    }

    /// A branch that was never judged, so it carries no verdict for a merge to
    /// rank. Whether there was anything to judge follows the artifacts it did
    /// produce rather than being asserted separately.
    fn unjudged_branch(branch_id: u32, artifact_name: &str) -> PgeBranchResult {
        let mut artifacts = Vec::new();
        if !artifact_name.is_empty() {
            artifacts.push(crate::squad::pge::types::Artifact {
                name: artifact_name.into(),
                content: String::new(),
                artifact_type: "code".into(),
            });
        }
        let product = if artifacts.is_empty() {
            StoppedProduct::None
        } else {
            StoppedProduct::Unjudged
        };
        PgeBranchResult {
            branch_id,
            plan: empty_planner_output(),
            generation: GeneratorOutput {
                content: serde_json::Value::Null,
                artifacts,
            },
            outcome: RoundOutcome::Stopped {
                cause: StopCause::EmptyEnvelope,
                product,
            },
        }
    }

    fn roundtable_for_merge(strategy: BranchMergeStrategy) -> PgeRoundtable {
        let config = PgeRoundtableConfig {
            branch_merge_strategy: strategy,
            ..Default::default()
        };
        PgeRoundtable::new(
            config,
            PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(
                serde_json::Value::Null,
            ))),
            GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
                serde_json::Value::Null,
            ))),
            EvaluatorActor::new(std::sync::Arc::new(MockAgent::fixed(
                serde_json::Value::Null,
            ))),
        )
    }

    #[test]
    fn merge_best_score_selects_highest() {
        let rt = roundtable_for_merge(BranchMergeStrategy::BestScore);
        let branches = vec![branch(0, 40, ""), branch(1, 90, ""), branch(2, 60, "")];
        let merged = rt.merge_best_score(&branches);
        assert_eq!(merged.outcome.judgement().and_then(|e| e.score), Some(90));
        assert_eq!(merged.selected_branch_id, Some(1));
    }

    #[test]
    fn merge_majority_vote_selects_majority_verdict() {
        let rt = roundtable_for_merge(BranchMergeStrategy::MajorityVote);
        let branches = vec![branch(0, 40, ""), branch(1, 85, ""), branch(2, 90, "")];
        let merged = rt.merge_majority_vote(&branches);
        let judgement = merged.outcome.judgement().expect("a judged branch");
        assert!(matches!(judgement.verdict, Verdict::Pass));
        assert_eq!(judgement.score, Some(90));
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
        assert_eq!(merged.outcome.judgement().and_then(|e| e.score), Some(90));
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

    /// A merge whose branches were never judged has no verdict to hand back.
    /// Reading their emptiness as a bad review picks a "winner" nobody judged
    /// and files that invented review in the debate history.
    #[test]
    fn a_merge_of_unjudged_branches_invents_no_verdict() {
        let branches = vec![
            unjudged_branch(0, ""),
            unjudged_branch(1, "b.rs"),
            unjudged_branch(2, ""),
        ];

        let merged = PgeRoundtable::merge_without_judgement(&branches);

        assert!(
            merged.outcome.judgement().is_none(),
            "no branch was judged, so there is no judgement to carry"
        );
        assert!(merged.outcome.stop_reason().is_some());
        // The branch that has something to carry is the one that travels — its
        // emptiness is the round's record, not a score.
        assert_eq!(merged.selected_branch_id, Some(1));
        assert_eq!(merged.generation.artifacts.len(), 1);
    }

    #[tokio::test]
    async fn independent_review_rejection_breaks_consensus() {
        // Iteration 1 and 2 both pass (consensus), then the fresh-context
        // reviewer rejects. The reviewer verdict must win.
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent::sequence(vec![
            serde_json::json!({"verdict": "pass", "score": 90, "feedback": "ok", "criteria": []}),
            serde_json::json!({"verdict": "pass", "score": 90, "feedback": "ok", "criteria": []}),
            serde_json::json!({"verdict": "fail", "score": 10, "feedback": "reviewer: output ignores the goal", "criteria": []}),
        ])));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: true,
                ..Default::default()
            },
            planner,
            generator,
            evaluator,
        );
        let task = cog_core::Task::new(
            "t-review".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );
        let result = rt.debate(&task, serde_json::json!({})).await;
        assert!(
            !result.consensus_reached,
            "reviewer rejection must break consensus"
        );
        let judgement = result
            .final_outcome
            .judgement()
            .expect("the reviewer's rejection is a judgement");
        assert!(judgement.feedback.contains("independent reviewer rejected"));
        let review = judgement
            .details
            .as_ref()
            .and_then(|d| d.get("independent_review"))
            .expect("reviewer verdict must be recorded");
        assert_eq!(review["verdict"], "fail");
    }

    /// A debate round whose judge spent its iteration budget judged nothing.
    /// Left unnamed it falls through to the degenerate-loop guard, so the run
    /// is filed as a debate that stopped making progress and the local cause —
    /// the judge's own budget — is lost.
    #[tokio::test]
    async fn a_debate_with_an_exhausted_judge_stops_naming_the_budget() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let evaluator =
            EvaluatorActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
                "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                "iterations": 5,
                "pending_tool_calls": 2
            }))));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            planner,
            generator,
            evaluator,
        );
        let task = cog_core::Task::new(
            "t-budget".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let result = rt.debate(&task, serde_json::json!({})).await;

        assert_eq!(result.iterations, 1, "another round spends the same budget");
        assert!(!result.consensus_reached);
        let reason = result
            .terminal_reason
            .as_deref()
            .expect("the debate must report a composed terminal reason");
        assert!(
            reason.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX),
            "the outer loops match on the prefix, got: {reason}"
        );
        assert!(
            !reason.contains("degenerate"),
            "the local cause must not be filed as a stalled debate: {reason}"
        );
        // The generator did write something; the judge is what failed. Those
        // are two different observations and the outcome keeps them apart.
        assert!(
            matches!(
                &result.final_outcome,
                RoundOutcome::Stopped {
                    cause: StopCause::Deterministic { .. },
                    product: StoppedProduct::Unjudged,
                }
            ),
            "a round whose judge spent its budget still produced something: {:?}",
            result.final_outcome
        );
    }

    /// 计划侧耗尽预算的那一轮，生成与评估都不该被买。计划已经明确说了这次
    /// 运行不会有产出，拿一个空计划去生成、再拿空产出评判，两次都是付费的空转；
    /// 而且占位的空生成与真的空产出同形，谁先读它谁就会把责任推给生成器。
    /// 两个观测面各证一件事：生成器的答卷没出现在结果里（生成没买），
    /// 评估器的 pass 没有变成共识（评估没买）。
    #[tokio::test]
    async fn a_debate_with_an_exhausted_planner_stops_before_paying_anyone() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
            "iterations": 10,
            "pending_tool_calls": 1
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "SHOULD_NOT_RUN"}, "artifacts": []}),
        )));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"verdict": "pass", "score": 90, "feedback": "ok", "criteria": []}),
        )));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            planner,
            generator,
            evaluator,
        );
        let task = cog_core::Task::new(
            "t-planner-budget".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let result = rt.debate(&task, serde_json::json!({})).await;

        assert_eq!(result.iterations, 1, "another round spends the same budget");
        assert!(
            !result.consensus_reached,
            "the judge's pass was never asked for, so it cannot carry the debate"
        );
        let reason = result
            .terminal_reason
            .as_deref()
            .expect("the debate must report a composed terminal reason");
        assert!(
            reason.contains(cog_core::contract::outcome::ITERATION_BUDGET_EXHAUSTED_MARKER),
            "the planner's spent budget is the cause, got: {reason}"
        );
        assert!(
            reason.contains("max_iterations=10"),
            "the reason must carry the observed numbers, got: {reason}"
        );
        assert!(
            !reason.contains("no artifacts"),
            "the generator never ran, so it cannot be the blamed role: {reason}"
        );
        assert_eq!(
            result.final_generation.content,
            serde_json::Value::Null,
            "the generator's answer must not appear: it was never asked"
        );
        assert!(
            result.final_generation.artifacts.is_empty(),
            "a generation that never happened produced nothing"
        );
    }

    #[tokio::test]
    async fn independent_review_agreement_keeps_consensus() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"verdict": "pass", "score": 90, "feedback": "ok", "criteria": []}),
        )));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: true,
                ..Default::default()
            },
            planner,
            generator,
            evaluator,
        );
        let task = cog_core::Task::new(
            "t-review-ok".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );
        let result = rt.debate(&task, serde_json::json!({})).await;
        assert!(result.consensus_reached);
        assert!(result
            .final_outcome
            .judgement()
            .and_then(|e| e.details.as_ref())
            .and_then(|d| d.get("independent_review"))
            .is_some());
    }

    /// The threshold is a gate, not a hint: the mock passes every round and the
    /// debate still must not confirm, because the pass it reports is below the
    /// bar the debate declared. The verdict itself is left alone — it is the
    /// judge's reading of the work, and it keeps saying what it said.
    #[tokio::test]
    async fn a_pass_below_the_floor_does_not_confirm_the_debate() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "pass", "score": 70, "feedback": "close enough", "criteria": []
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 3,
                consensus_threshold: 0.8,
                stall_threshold: 0,
                independent_review: true,
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-floor".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let below_before = cell(
            "pge_consensus_floor_total",
            crate::observable::CONSENSUS_FLOOR_NOT_MET,
        )
        .await;
        let result = rt.debate(&task, serde_json::json!({})).await;

        assert!(
            !result.consensus_reached,
            "a pass the judge itself scored below the declared floor cannot confirm the debate"
        );
        assert_eq!(
            result.iterations, 3,
            "no round confirmed, so the debate spends its whole budget"
        );
        assert_eq!(
            evaluator_agent.calls(),
            3,
            "a debate that never confirmed has no consensus for the fresh-context judge to check"
        );
        let judgement = result
            .final_outcome
            .judgement()
            .expect("every round here reached a judgement");
        assert_eq!(
            judgement.verdict,
            Verdict::Pass,
            "the floor decides whether the debate stops; it does not overrule the judge"
        );
        assert_eq!(judgement.score, Some(70));
        assert!(
            cell(
                "pge_consensus_floor_total",
                crate::observable::CONSENSUS_FLOOR_NOT_MET
            )
            .await
                - below_before
                >= 3.0,
            "each round the floor refused has to be readable, or a strict floor and a floor \
             nothing consults look the same"
        );
    }

    /// A pass that reports no score cannot be shown to clear the floor, so it
    /// does not. The cell keeps it apart from a pass that *was* below the bar:
    /// a missing reading and a low one are different facts about the run, and
    /// only one of them is fixed by a better evaluator.
    #[tokio::test]
    async fn a_pass_with_no_score_does_not_confirm_the_debate() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"verdict": "pass", "feedback": "looks fine", "criteria": []}),
        )));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 2,
                consensus_threshold: 0.1,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            planner,
            generator,
            evaluator,
        );
        let task = cog_core::Task::new(
            "t-noscore".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let absent_before = cell(
            "pge_consensus_floor_total",
            crate::observable::CONSENSUS_FLOOR_SCORE_ABSENT,
        )
        .await;
        let result = rt.debate(&task, serde_json::json!({})).await;

        assert!(
            !result.consensus_reached,
            "nothing about a scoreless pass shows it cleared the floor, however low the floor is"
        );
        assert_eq!(result.iterations, 2);
        assert!(
            cell(
                "pge_consensus_floor_total",
                crate::observable::CONSENSUS_FLOOR_SCORE_ABSENT
            )
            .await
                - absent_before
                >= 2.0
        );
    }

    /// Confirmation has to come from two *adjacent* rounds. A round that judged
    /// nothing did not agree with the round before it, so it breaks the pair
    /// instead of being skipped over: carrying the older pass across the gap
    /// would read two passes with a hole between them as a consensus.
    #[tokio::test]
    async fn a_round_that_judged_nothing_breaks_the_pair() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::sequence(vec![
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
            serde_json::json!({"content": null, "artifacts": []}),
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        ])));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "pass", "score": 90, "feedback": "ok", "criteria": []
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 3,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-gap".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let unjudged_before = cell(
            "pge_consensus_floor_total",
            crate::observable::CONSENSUS_FLOOR_NOT_JUDGED,
        )
        .await;
        let result = rt.debate(&task, serde_json::json!({})).await;

        assert_eq!(result.iterations, 3);
        assert!(
            !result.consensus_reached,
            "a round nobody judged cannot be the second half of a pair"
        );
        assert_eq!(
            evaluator_agent.calls(),
            2,
            "the empty round produced nothing to judge and the debate stopped before a third pass"
        );
        assert!(
            cell(
                "pge_consensus_floor_total",
                crate::observable::CONSENSUS_FLOOR_NOT_JUDGED
            )
            .await
                - unjudged_before
                >= 1.0,
            "a round that reached no judgement is a sighting of the gate, not an absence of one"
        );
    }

    /// The fresh-context judge has to be fresh in the document it reads, not
    /// only in the history argument. The board carries `latest_outcome` — the
    /// consensus this review exists to check independently — and it is written
    /// before the review is reached. What the reviewer was actually handed is
    /// only observable from the agent side, so the assertion is on the input
    /// the evaluator's agent received, against the one the round judge got.
    #[tokio::test]
    async fn the_fresh_context_reviewer_is_not_handed_the_board() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "fn main() {}"}, "artifacts": []}),
        )));
        let agent = std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"verdict": "pass", "score": 90, "feedback": "ok", "criteria": []}),
        ));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: true,
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-review-context".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let result = rt.debate(&task, serde_json::json!({})).await;
        assert!(result.consensus_reached);

        let inputs = agent.inputs();
        assert_eq!(
            inputs.len(),
            3,
            "two rounds judged, then one independent review"
        );
        assert!(
            inputs[0]["context"].get("context_board").is_some(),
            "the round's own judge reads the board"
        );
        let review_input = &inputs[2]["context"];
        assert!(
            review_input.get("context_board").is_none(),
            "the reviewer was handed the board, which carries this round's own verdict — \
             the judgement it is supposed to reach independently"
        );
        assert_eq!(
            review_input["history"],
            serde_json::json!([]),
            "and the history it is asked to read stays empty"
        );
    }

    /// The board's copy of a round is removed only when it is the same round the
    /// role already reads elsewhere. A board holding a *different* round is that
    /// round's only carrier — removing it would take content away rather than a
    /// duplicate — and a field the caller never named is not compared at all.
    #[test]
    fn the_board_mirror_goes_out_only_when_it_is_the_same_round() {
        let board = serde_json::json!({
            "seed": "s",
            "round": 3,
            "latest_plan": {"a": 1},
            "latest_generation": {"content": "x"},
            "latest_outcome": {"s": 2},
        });

        // Same round: all three copies go, nothing else moves.
        let same = serde_json::json!({
            "plan": {"a": 1},
            "generation": {"content": "x"},
            "outcome": {"s": 2},
        });
        let trimmed = PgeRoundtable::board_without_mirrored_round(&board, &same);
        assert_eq!(trimmed.get("latest_plan"), None);
        assert_eq!(trimmed.get("latest_generation"), None);
        assert_eq!(trimmed.get("latest_outcome"), None);
        assert_eq!(trimmed["seed"], serde_json::json!("s"));
        assert_eq!(trimmed["round"], serde_json::json!(3));

        // A different round in the same field: kept, because the board is then
        // that round's only copy.
        let other = serde_json::json!({
            "plan": {"a": 9},
            "generation": {"content": "x"},
            "outcome": {"s": 2},
        });
        let trimmed = PgeRoundtable::board_without_mirrored_round(&board, &other);
        assert_eq!(trimmed["latest_plan"], serde_json::json!({"a": 1}));
        assert_eq!(trimmed.get("latest_generation"), None);

        // The planner has no second carrier for the previous *plan*, so it names
        // only the generation and the other two fields pass through untouched.
        let generation_only = serde_json::json!({"generation": {"content": "x"}});
        let trimmed = PgeRoundtable::board_without_mirrored_round(&board, &generation_only);
        assert_eq!(trimmed["latest_plan"], serde_json::json!({"a": 1}));
        assert_eq!(trimmed["latest_outcome"], serde_json::json!({"s": 2}));
        assert_eq!(trimmed.get("latest_generation"), None);

        // The reading is in bytes, not in fields: pinning the unit keeps a
        // later "count the removals" from reading as the same number.
        assert_eq!(
            board_mirror_bytes("latest_generation", &serde_json::json!({"content": "x"})),
            17 + 15
        );
        // Every field's own bytes are summed, and the two ends of one trim are
        // told apart without the process-wide counter (which the tests share).
        let (_, dropped, kept) = board_mirror_trim(&board, &same);
        assert_eq!(
            dropped,
            board_mirror_bytes("latest_plan", &serde_json::json!({"a": 1}))
                + board_mirror_bytes("latest_generation", &serde_json::json!({"content": "x"}))
                + board_mirror_bytes("latest_outcome", &serde_json::json!({"s": 2}))
        );
        assert_eq!(kept, 0, "all three copies were the same round");

        let (_, dropped, kept) = board_mirror_trim(&board, &other);
        assert_eq!(
            dropped,
            board_mirror_bytes("latest_generation", &serde_json::json!({"content": "x"}))
                + board_mirror_bytes("latest_outcome", &serde_json::json!({"s": 2})),
            "the two fields that were the same round"
        );
        assert_eq!(
            kept,
            board_mirror_bytes("latest_plan", &serde_json::json!({"a": 1})),
            "and the one that was not is the guard's own reading"
        );

        let (_, dropped, kept) = board_mirror_trim(&board, &generation_only);
        assert_eq!(
            dropped,
            board_mirror_bytes("latest_generation", &serde_json::json!({"content": "x"}))
        );
        assert_eq!(kept, 0, "a field the caller did not name is not compared");
    }

    /// The board a role reads carries the round once, through the role's own
    /// field: the evaluator's history, the planner's previous generation, the
    /// moderator's debate. Each role is checked in the same run, because the
    /// trim is per-role — the generator has no second carrier and keeps the
    /// whole board.
    #[tokio::test]
    async fn every_role_reads_the_round_once() {
        let planner_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {"approach": "keep going"},
            "sub_tasks": []
        })));
        let generator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "content": {"code": "fn main() {}"},
            "artifacts": []
        })));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "fail", "score": 10, "feedback": "not yet", "criteria": []
        })));
        let moderator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "decision": "continue"
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 1.0,
                // Flat judgements, so the second round stalls the debate and
                // the evaluator is called exactly twice.
                stall_threshold: 2,
                independent_review: false,
                context_board: Some(serde_json::json!({"seed": "s"})),
                moderator: Some(ModeratorActor::new(moderator_agent.clone())),
                ..Default::default()
            },
            PlannerActor::new(planner_agent.clone()),
            GeneratorActor::new(generator_agent.clone()),
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-mirror".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let dropped_before = mirror_bytes(crate::observable::BOARD_MIRROR_DROPPED).await;
        let result = rt.debate(&task, serde_json::json!({})).await;
        assert_eq!(
            result.iterations, 3,
            "the assertions below describe the run that ended here: three flat rounds, \
             then the stall"
        );
        // The saving is on the record too: a trim nobody counted reads exactly
        // like a trim that never ran. Only the removed end is asserted — the
        // tests share this counter, so `kept` cannot be read for one run here;
        // the guard's own reading is pinned in the pure test instead.
        assert!(
            mirror_bytes(crate::observable::BOARD_MIRROR_DROPPED).await > dropped_before,
            "the board's copies of these rounds were removed without a reading"
        );

        // The evaluator: every board after the first was written from the round
        // before, and the history it is handed ends on that same round.
        let judged = evaluator_agent.inputs();
        assert_eq!(judged.len(), 3, "every round was judged before the stall");
        let second = &judged[1]["context"];
        let board = &second["context_board"];
        assert_eq!(
            board.get("latest_generation"),
            None,
            "the board is still carrying the round the history already ends on"
        );
        assert_eq!(
            board["round"],
            serde_json::json!(1),
            "the board is the previous round's, so the round it mirrors is not in question"
        );
        assert_eq!(board["seed"], serde_json::json!("s"));
        assert_eq!(
            second["history"][0]["generation"]["content"]["code"],
            serde_json::json!("fn main() {}"),
            "and the round itself still arrives, through the history that ends on it"
        );

        // The planner: its previous generation is a field of its own, so that
        // one copy goes — while the previous plan, which nothing else carries,
        // stays on the board.
        let planned = planner_agent.inputs();
        assert_eq!(planned.len(), 3);
        let second_plan = &planned[1]["context"];
        assert_eq!(
            second_plan["previous_generation"]["content"]["code"],
            serde_json::json!("fn main() {}")
        );
        assert_eq!(
            second_plan["context_board"].get("latest_generation"),
            None,
            "the plan request was handed the previous generation twice"
        );
        assert_eq!(
            second_plan["context_board"]["latest_plan"]["plan"]["approach"],
            serde_json::json!("keep going"),
            "the previous plan has no second carrier and keeps its place"
        );

        // The generator holds no second copy of anything on the board, so its
        // board is passed through whole — the trim is per-role, not global.
        let generated = generator_agent.inputs();
        assert_eq!(generated.len(), 3);
        assert_eq!(
            generated[1]["context"]["context_board"]["latest_generation"]["content"]["code"],
            serde_json::json!("fn main() {}"),
            "the generator reads the previous generation only from the board"
        );

        // The moderator was consulted in the two rounds that reached it; round 3
        // stalled before it. Each time, the board mirrors the round it was just
        // handed as the debate.
        let moderated = moderator_agent.inputs();
        assert_eq!(
            moderated.len(),
            2,
            "the stalling round reaches no moderator"
        );
        for (round, input) in moderated.iter().enumerate() {
            assert_eq!(
                input["context_board"].get("latest_generation"),
                None,
                "the moderator was handed the round it was just given as the debate"
            );
            assert_eq!(
                input["context_board"]["round"],
                serde_json::json!(round + 1),
                "the board is that round's, so the round it mirrors is not in question"
            );
            assert_eq!(input["context_board"]["seed"], serde_json::json!("s"));
            assert_eq!(
                input["history"][round]["generation"]["content"]["code"],
                serde_json::json!("fn main() {}"),
                "and the debate it decides on still carries that round whole"
            );
        }
    }

    /// The moderator's mapping names every decision and has no catch-all, so a
    /// decision added later cannot be filed as `continue` and leave the four
    /// cells looking complete.
    #[test]
    fn every_moderator_decision_is_mapped_to_its_own_cell() {
        let source = include_str!("roundtable.rs");
        let production = source
            .split("\n#[cfg(test)]\n")
            .next()
            .expect("the file always has a first segment");
        let anchor = "let outcome = match mod_output.decision {";
        let start = production
            .find(anchor)
            .expect("the moderator's decision is no longer mapped to the recorded cells");
        let mapping = &production[start..];
        let record_at = mapping
            .find("record_moderator_consulted(outcome);")
            .expect("the mapping no longer feeds the recorders");
        let mapping = &mapping[..record_at + "record_moderator_consulted(outcome);".len()];
        assert!(
            !mapping.contains("_ =>"),
            "a catch-all arm files every future decision as one of the existing cells"
        );
        for name in [
            "MODERATOR_CONTINUE",
            "MODERATOR_CHANGE_STRATEGY",
            "MODERATOR_ACCEPT_PARTIAL",
            "MODERATOR_ESCALATE",
            "record_moderator_consulted",
        ] {
            assert!(
                mapping.contains(name),
                "the moderator's cells no longer mention `{name}`"
            );
        }

        let block = &production[start..];
        let block = &block[..block
            .find("\n            if let Some(judgement) = outcome.judgement() {")
            .expect("the moderator block no longer ends where the verdict carries over")];
        assert!(
            block.contains("MODERATOR_NOT_ASKED_DISABLED"),
            "a deployment with no moderator writes no decision cell; without the skip's \
             own cell it reads exactly like a moderator that was never consulted"
        );
    }

    /// One end of the board-mirror reading, from the process-wide observable.
    /// Tests share that observable, so a reading is only usable as a delta
    /// against the value taken before the run.
    async fn mirror_bytes(part: &str) -> f64 {
        use cog_core::observability::Observable;
        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        metrics
            .iter()
            .find(|m| {
                m.name == "pge_board_mirror_bytes_total"
                    && m.labels.get("part").map(String::as_str) == Some(part)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// One cell of a global series, read from the process-wide observable.
    /// Tests share that observable, so a reading is only usable as a delta
    /// against the value taken before the run.
    async fn cell(name: &'static str, outcome: &str) -> f64 {
        use cog_core::observability::Observable;
        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        metrics
            .iter()
            .find(|m| {
                m.name == name && m.labels.get("outcome").map(String::as_str) == Some(outcome)
            })
            .map(|m| m.value)
            .unwrap_or(0.0)
    }

    /// A partial the moderator accepted is a consensus, but it is not a Pass,
    /// so there is no consensus claim for the fresh-context judge to confirm.
    /// Running the review anyway would judge a claim nobody made — and a code
    /// path that assumes every consensus is a Pass reads it as one here.
    #[tokio::test]
    async fn an_accepted_partial_is_not_re_reviewed_by_the_fresh_context_judge() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"code": "partial"}, "artifacts": []}),
        )));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "fail", "score": 10, "feedback": "not good enough", "criteria": []
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 5,
                consensus_threshold: 1.0,
                stall_threshold: 0,
                independent_review: true,
                moderator: Some(ModeratorActor::new(std::sync::Arc::new(MockAgent::fixed(
                    serde_json::json!({"decision": "accept_partial"}),
                )))),
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-partial".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let before = cell(
            "pge_moderator_consulted_total",
            crate::observable::MODERATOR_ACCEPT_PARTIAL,
        )
        .await;
        let result = rt.debate(&task, serde_json::json!({})).await;
        let after = cell(
            "pge_moderator_consulted_total",
            crate::observable::MODERATOR_ACCEPT_PARTIAL,
        )
        .await;
        assert!(
            after > before,
            "the round ended on the moderator's decision, and has to be readable as one: \
             {before} -> {after}"
        );

        assert!(
            result.consensus_reached,
            "the moderator accepted the partial"
        );
        assert_eq!(
            evaluator_agent.calls(),
            1,
            "the round's own judge ran once; the fresh-context review had no Pass to confirm"
        );
        let judgement = result
            .final_outcome
            .judgement()
            .expect("the round was judged");
        assert!(matches!(judgement.verdict, Verdict::Fail));
    }

    /// A round that produced nothing has nothing to judge, so the judge is not
    /// paid for. The evaluator here would return a Pass if it were ever asked,
    /// which is exactly the fabrication that used to travel downstream as a
    /// real review of an empty envelope.
    #[tokio::test]
    async fn a_round_that_produces_nothing_pays_no_evaluator() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": null, "artifacts": []}),
        )));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "pass", "score": 90, "feedback": "ok", "criteria": []
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 3,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: false,
                context_board: Some(serde_json::json!({})),
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-empty".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let result = rt.debate(&task, serde_json::json!({})).await;

        assert_eq!(
            evaluator_agent.calls(),
            0,
            "an envelope with no content and no artifacts has nothing to judge"
        );
        assert!(
            !result.consensus_reached,
            "the judge's pass was never asked for, so it cannot carry the debate"
        );
        assert!(
            result.final_outcome.judgement().is_none(),
            "no verdict may be invented for a round nobody judged"
        );
        assert!(result
            .final_outcome
            .stop_reason()
            .is_some_and(|r| r.contains("no content and no artifacts")));

        // Neither the debate history nor the shared board may carry a review of
        // an empty envelope: the next planner and judge read both.
        for iteration in &result.history {
            assert!(
                iteration.outcome.judgement().is_none(),
                "iteration {} wrote a verdict for a round that was never judged",
                iteration.iteration
            );
        }
        let board = result.context_board.expect("the board was configured");
        assert!(
            board.get("latest_outcome").is_some(),
            "the board records what the round reached"
        );
        assert!(
            board.get("latest_evaluation").is_none(),
            "an evaluation-shaped key would describe a review that never happened"
        );
    }

    /// The other half of the same rule: a round that did produce something is
    /// judged as before, so the guard cannot pass by never paying anyone.
    #[tokio::test]
    async fn a_round_that_produces_something_pays_the_evaluator() {
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        }))));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent::fixed(
            serde_json::json!({"content": {"answer": 42}, "artifacts": []}),
        )));
        let evaluator_agent = std::sync::Arc::new(MockAgent::fixed(serde_json::json!({
            "verdict": "pass", "score": 90, "feedback": "ok", "criteria": []
        })));
        let rt = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 3,
                consensus_threshold: 0.5,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            planner,
            generator,
            EvaluatorActor::new(evaluator_agent.clone()),
        );
        let task = cog_core::Task::new(
            "t-product".to_string(),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": "g"}),
        );

        let result = rt.debate(&task, serde_json::json!({})).await;

        assert!(
            evaluator_agent.calls() >= 1,
            "a round with a product is judged"
        );
        let judgement = result
            .final_outcome
            .judgement()
            .expect("the round reached a judgement");
        assert_eq!(judgement.score, Some(90));
        assert!(result.consensus_reached);
    }
}
