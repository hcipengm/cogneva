//! PGE Pipeline mode: Planner → Generator → Evaluator with optional
//! Generator feedback loop.
//! ```text
//! Planner ──▶ Generator ──▶ Evaluator
//!                ▲             │
//!                └─────────────┘
//!              (local repair feedback)
//! ```
//! Rules enforced by this module:
//! - Planner produces the specification; it is not questioned by the Generator.
//! - Generator can receive evaluator feedback and attempt local repairs while
//!   the plan remains fixed.
//! - Evaluator cannot mutate the artifact — it only emits [`EvaluationResult`].
//! - When local repair is exhausted, the pipeline performs a global reset
//!   (re-runs the Planner) up to [`PgePipelineConfig::max_retries`] times.

use crate::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use crate::squad::pge::stall::{
    degenerate_loop_feedback, ProgressSignals, StallDetector, StallVerdict,
};
use crate::squad::pge::types::{
    EvaluationResult, GeneratorOutput, LocalRepairAttempt, PlannerOutput, Verdict,
};

/// Configuration for [`PgePipeline`].
#[derive(Debug)]
pub struct PgePipelineConfig {
    /// Maximum number of full Planner→Generator→Evaluator passes before
    /// returning the last attempt regardless of evaluator outcome.
    pub max_retries: u32,
    /// Soft timeout for a single full pass, in milliseconds.
    /// Currently advisory — the agents themselves drive their own timeouts;
    /// this is exposed so callers and orchestrators can record/respect it.
    pub timeout_ms: u64,
    /// Maximum number of local repair attempts within a single full pass.
    /// When the evaluator fails, the feedback is sent back to the generator
    /// while the plan remains unchanged. 0 disables local repair.
    pub local_repair_max: u32,
    /// Consecutive non-progress attempts that declare the run a degenerate
    /// loop (evaluation score and criteria both flat) and stop it early. The
    /// criterion is whether spend buys progress, never a flat spend cap.
    /// 0 disables stall detection.
    pub stall_threshold: u32,
    /// When true, an evaluator Pass is re-judged once in a fresh context
    /// (no attempt history, no prior feedback) before the pipeline accepts
    /// it. Author and reviewer conclusions are both recorded in the final
    /// evaluation; on conflict the reviewer wins.
    pub independent_review: bool,
}

impl Default for PgePipelineConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            timeout_ms: 30_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: true,
        }
    }
}

/// Single pass through Planner → Generator → Evaluator.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PgePipelineAttempt {
    pub attempt: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
    /// Local repair cycles performed within this attempt.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub local_repairs: Vec<LocalRepairAttempt>,
}

/// Final result of a [`PgePipeline::execute_task`] run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PgePipelineResult {
    /// Total number of attempts executed.
    pub attempts: u32,
    /// Whether the final evaluator pass returned `passed == true`.
    pub passed: bool,
    /// Plan from the final attempt.
    pub final_plan: PlannerOutput,
    /// Generation from the final attempt.
    pub final_generation: GeneratorOutput,
    /// Evaluation from the final attempt.
    pub final_evaluation: EvaluationResult,
    /// Full attempt history, ordered oldest → newest.
    pub history: Vec<PgePipelineAttempt>,
    /// The composed cause when this run ended on a deterministic
    /// environment/protocol failure, `None` when it ended any other way.
    /// It names the role that actually failed, which is not always the
    /// generator: a planner that spent its own iteration budget ends the run
    /// before any generation happens, and a reader that re-derives the cause
    /// from `final_generation` alone blames the generator for a run it never
    /// took part in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_reason: Option<String>,
}

/// Linear Planner → Generator → Evaluator orchestrator with optional
/// Generator local repair feedback.
/// On evaluator failure the pipeline first tries to repair the generation
/// while keeping the plan fixed, up to [`PgePipelineConfig::local_repair_max`]
/// times. Only when local repair is exhausted does it perform a global reset
/// (re-run the Planner) up to [`PgePipelineConfig::max_retries`] times.
#[derive(Debug)]
pub struct PgePipeline {
    config: PgePipelineConfig,
}

impl PgePipeline {
    pub fn new(config: PgePipelineConfig) -> Self {
        Self { config }
    }

    /// Build the failing result for a deterministic environment/protocol
    /// failure: a synthesized zero-score evaluation marks the attempt, and
    /// the feedback prefix lets outer loops (Ralph, squad escalation)
    /// recognize the failure as terminal without re-parsing the generation.
    ///
    /// The cause travels in [`PgePipelineResult::terminal_reason`] and in the
    /// evaluation's feedback — never in `final_generation`. That field may
    /// hold a placeholder written here for a role that never ran, so it is
    /// structurally identical to a real empty output and cannot be told apart
    /// by inspection: a reader that re-derives the cause from it reports the
    /// wrong role.
    fn terminal_result(
        attempt: u32,
        plan: PlannerOutput,
        generation: GeneratorOutput,
        explicit_reason: Option<String>,
        mut history: Vec<PgePipelineAttempt>,
    ) -> PgePipelineResult {
        // 谁先坏谁的原因优先，调用方知道得最清楚，其次才按链路顺序回落。
        // 计划侧优先于生成侧：本轮 planner 都没到上游时，拿生成侧兜底文案会把
        // 责任记在一个从未被调用过的生成器头上。
        let declared = explicit_reason
            .or_else(|| plan.terminal_env_failure_reason())
            .or_else(|| generation.terminal_env_failure_reason())
            .unwrap_or_else(crate::squad::pge::types::unnamed_terminal_reason);
        let evaluation = EvaluationResult {
            verdict: Verdict::Fail,
            // 声明这次运行按终止性环境故障处置；边界会不会把它记成同一分类，
            // 由分类可达性自查比对（声明过却从未被记录 = 分类被丢了）。声明的
            // 分类由这段文本自己给出，不在调用点再写一遍。
            feedback: crate::squad::classify::declare_for(declared.clone()),
            score: Some(0),
            criteria: Vec::new(),
            details: None,
        };
        history.push(PgePipelineAttempt {
            attempt,
            plan: plan.clone(),
            generation: generation.clone(),
            evaluation: evaluation.clone(),
            local_repairs: Vec::new(),
        });
        PgePipelineResult {
            attempts: history.len() as u32,
            passed: false,
            final_plan: plan,
            final_generation: generation,
            final_evaluation: evaluation,
            history,
            terminal_reason: Some(declared),
        }
    }

    /// Run the pipeline with a structured [`cog_core::Task`] instead of a plain `goal` string.
    /// This is the preferred entry point for new code.
    pub async fn execute_task(
        &self,
        task: &cog_core::Task,
        _context: serde_json::Value,
        planner: &PlannerActor,
        generator: &GeneratorActor,
        evaluator: &EvaluatorActor,
    ) -> PgePipelineResult {
        let mut history: Vec<PgePipelineAttempt> = Vec::new();
        let mut last_evaluation: Option<EvaluationResult> = None;
        let mut last_generation: Option<GeneratorOutput> = None;
        let max_attempts = self.config.max_retries.max(1);
        let mut stall = StallDetector::new(self.config.stall_threshold);

        for attempt in 1..=max_attempts {
            // Stage 1: Planner.
            let plan = planner
                .plan(
                    task,
                    attempt,
                    last_evaluation.as_ref().map(|e| e.feedback.as_str()),
                    last_evaluation.as_ref().and_then(|e| e.score),
                    None,
                    None,
                )
                .await;

            // 计划侧的确定性环境失败：planner 的 prompt 没到上游，重试必然同样
            // 失败。在这里收口，既不白花一次生成，也不把一个空计划当成正常计划
            // 一路送进评估。
            if plan.is_terminal_env_failure() {
                tracing::warn!(
                    attempt,
                    "Planner reported terminal environment failure; aborting pipeline without generation"
                );
                // 生成器没被调用，这里是占位而非产出：真因由 plan 侧合成进
                // terminal_reason，读侧不能拿这个空生成反推是谁失败。
                let generation = GeneratorOutput {
                    content: serde_json::Value::Null,
                    artifacts: Vec::new(),
                };
                return Self::terminal_result(attempt, plan, generation, None, history);
            }

            // Stage 2: Generator (initial attempt).
            let prev_eval_json = last_evaluation
                .as_ref()
                .map(|e| serde_json::to_value(e).unwrap_or_default());
            let prev_gen_json = last_generation
                .as_ref()
                .map(|g| serde_json::to_value(g).unwrap_or_default());
            let plan_json = serde_json::to_value(&plan).unwrap_or_default();
            let mut generation = generator
                .generate(
                    task,
                    &plan_json,
                    attempt,
                    crate::actors::PreviousAttempt {
                        evaluation: prev_eval_json.as_ref(),
                        generation: prev_gen_json.as_ref(),
                        ..Default::default()
                    },
                    None,
                )
                .await;

            // Deterministic environment/protocol failure: the generator
            // produced nothing because tools never ran or the upstream cannot
            // honor the protocol. Skip the evaluator and all retries — every
            // further attempt must fail identically and only burns tokens.
            if generation.is_terminal_env_failure() {
                tracing::warn!(
                    attempt,
                    "Generator reported terminal environment failure; aborting pipeline without evaluation"
                );
                return Self::terminal_result(attempt, plan, generation, None, history);
            }

            let eval_history: Vec<serde_json::Value> = history
                .iter()
                .map(|h| {
                    serde_json::json!({
                        "attempt": h.attempt,
                        "plan": &h.plan,
                        "generation": &h.generation,
                        "evaluation": &h.evaluation,
                    })
                })
                .collect();
            // Acceptance criteria: the plan's own verifiable criteria gate the
            // evaluation; caller-supplied context criteria are the fallback.
            let context_criteria: Vec<&str> = _context
                .get("criteria")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
            let criteria: Vec<&str> = if plan.acceptance_criteria.is_empty() {
                context_criteria
            } else {
                plan.acceptance_criteria
                    .iter()
                    .map(|s| s.as_str())
                    .collect()
            };

            // An empty envelope is not a terminal failure, and it is nothing to
            // spend an inference call on either: there is no artifact to judge,
            // so the evaluator's only possible answer is "there is nothing
            // here". Judge it deterministically under its own cause instead, and
            // hand the repair loop a name it can act on.
            let mut evaluation = if generation.is_empty_envelope() {
                tracing::warn!(
                    attempt,
                    "Generator returned an empty envelope; failing the attempt under its own cause instead of evaluating nothing"
                );
                crate::squad::pge::types::empty_envelope_evaluation()
            } else {
                // Stage 3: Evaluator.
                let mut evaluation = evaluator
                    .evaluate(
                        task,
                        &plan_json,
                        &serde_json::to_value(&generation).unwrap_or_default(),
                        &eval_history,
                        &criteria,
                        None,
                    )
                    .await;
                evaluation.enforce_criteria_evidence(!criteria.is_empty());
                evaluation
                    .enforce_change_artifact_integrity(generation.change_artifact_defect(task));
                evaluation
            };

            // Same guard as the two above, on the third role: an evaluator that
            // ran out of iterations judged nothing, so repairing towards its
            // empty feedback and re-judging with the same budget both spend
            // without being able to conclude.
            if let Some(reason) = evaluation.terminal_env_failure_reason() {
                tracing::warn!(
                    attempt,
                    "Evaluator reported terminal environment failure; aborting pipeline without repair or retry"
                );
                return Self::terminal_result(
                    attempt,
                    plan.clone(),
                    generation,
                    Some(reason),
                    history,
                );
            }

            let mut local_repairs: Vec<LocalRepairAttempt> = Vec::new();
            let mut repair_stall = StallDetector::new(self.config.stall_threshold);

            // Local repair loop: feed evaluator feedback back to generator.
            for repair_iteration in 1..=self.config.local_repair_max {
                if matches!(evaluation.verdict, Verdict::Pass) {
                    break;
                }

                let repair_feedback = evaluation.feedback.clone();
                let repair_eval_json = serde_json::to_value(&evaluation).unwrap_or_default();
                let repair_gen_json = serde_json::to_value(&generation).unwrap_or_default();

                generation = generator
                    .generate(
                        task,
                        &plan_json,
                        attempt,
                        crate::actors::PreviousAttempt {
                            evaluation: Some(&repair_eval_json),
                            generation: Some(&repair_gen_json),
                            repair_feedback: Some(&repair_feedback),
                        },
                        None,
                    )
                    .await;

                // Same terminal-failure guard as the initial generation: a
                // repair that produced nothing for environment reasons must
                // not be evaluated or retried.
                if generation.is_terminal_env_failure() {
                    tracing::warn!(
                        attempt,
                        repair_iteration,
                        "Generator repair reported terminal environment failure; aborting pipeline"
                    );
                    return Self::terminal_result(attempt, plan.clone(), generation, None, history);
                }

                // Same rule as the initial generation: a repair that delivered
                // nothing is judged under its own cause, not spent on an
                // inference call that can only answer "there is nothing here".
                evaluation = if generation.is_empty_envelope() {
                    tracing::warn!(
                        attempt,
                        repair_iteration,
                        "Generator repair returned an empty envelope; failing the repair under its own cause"
                    );
                    crate::squad::pge::types::empty_envelope_evaluation()
                } else {
                    let mut evaluation = evaluator
                        .evaluate(
                            task,
                            &plan_json,
                            &serde_json::to_value(&generation).unwrap_or_default(),
                            &eval_history,
                            &criteria,
                            None,
                        )
                        .await;
                    evaluation.enforce_criteria_evidence(!criteria.is_empty());
                    evaluation
                        .enforce_change_artifact_integrity(generation.change_artifact_defect(task));
                    evaluation
                };

                if let Some(reason) = evaluation.terminal_env_failure_reason() {
                    tracing::warn!(
                        attempt,
                        repair_iteration,
                        "Evaluator reported terminal environment failure during repair; aborting pipeline"
                    );
                    return Self::terminal_result(
                        attempt,
                        plan.clone(),
                        generation,
                        Some(reason),
                        history,
                    );
                }

                local_repairs.push(LocalRepairAttempt {
                    repair_iteration,
                    generation: generation.clone(),
                    evaluation: evaluation.clone(),
                    feedback: repair_feedback,
                });

                // Repair-level stall guard: repairs that buy no progress stop
                // early instead of burning the full local_repair_max budget.
                if !matches!(evaluation.verdict, Verdict::Pass)
                    && matches!(
                        repair_stall.observe(ProgressSignals::from_evaluation(&evaluation),),
                        StallVerdict::Stalled
                    )
                {
                    tracing::warn!(
                        attempt,
                        repair_iteration,
                        "degenerate repair loop detected; stopping local repairs early"
                    );
                    break;
                }
            }

            // Independent review gate: the evaluator above saw the attempt's
            // own feedback loop, so its Pass is self-assessment. Re-judge in a
            // fresh context (no history) before accepting; on conflict the
            // reviewer wins. Both verdicts stay on the record.
            if self.config.independent_review && matches!(evaluation.verdict, Verdict::Pass) {
                let mut review = evaluator
                    .evaluate(
                        task,
                        &plan_json,
                        &serde_json::to_value(&generation).unwrap_or_default(),
                        &[],
                        &criteria,
                        None,
                    )
                    .await;
                review.enforce_criteria_evidence(!criteria.is_empty());
                // The reviewer ran out of budget too, so it did not reject the
                // pass — it never looked. Wrapping that as "independent reviewer
                // rejected" would put the second role's failure on a verdict the
                // reviewer never reached.
                if let Some(reason) = review.terminal_env_failure_reason() {
                    tracing::warn!(
                        attempt,
                        "Independent reviewer reported terminal environment failure; aborting pipeline"
                    );
                    return Self::terminal_result(
                        attempt,
                        plan.clone(),
                        generation,
                        Some(reason),
                        history,
                    );
                }
                let review_json = serde_json::to_value(&review).unwrap_or_default();
                if !matches!(review.verdict, Verdict::Pass) {
                    evaluation.verdict = Verdict::Fail;
                    evaluation.feedback = format!(
                        "independent reviewer rejected the pass: {}",
                        review.feedback
                    );
                }
                let details = evaluation.details.take().unwrap_or(serde_json::json!({}));
                let mut details = details;
                details["independent_review"] = review_json;
                evaluation.details = Some(details);
            }

            let passed = matches!(evaluation.verdict, Verdict::Pass);

            // Attempt-level stall guard: when consecutive full attempts buy no
            // progress, mark the run degenerate and stop before spending more.
            // The prefixed feedback lets outer loops classify the failure as
            // non-retryable and route it to reflection as learning material.
            if !passed {
                let signals = ProgressSignals::from_evaluation(&evaluation);
                if matches!(stall.observe(signals), StallVerdict::Stalled) {
                    let mut evaluation = evaluation;
                    evaluation.feedback = degenerate_loop_feedback(format!(
                        "{} consecutive attempts bought no progress \
                         (evaluation score and criteria both flat); stopped early: {}",
                        self.config.stall_threshold, evaluation.feedback
                    ));
                    tracing::warn!(attempt, "degenerate loop detected; stopping pipeline early");
                    history.push(PgePipelineAttempt {
                        attempt,
                        plan,
                        generation,
                        evaluation,
                        local_repairs,
                    });
                    break;
                }
            }

            history.push(PgePipelineAttempt {
                attempt,
                plan,
                generation: generation.clone(),
                evaluation: evaluation.clone(),
                local_repairs,
            });
            last_evaluation = Some(evaluation);
            last_generation = Some(generation);

            if passed {
                break;
            }
        }

        let last = history.last().cloned().expect(
            "PgePipeline::execute always runs at least one attempt because max_retries.max(1)",
        );

        PgePipelineResult {
            attempts: history.len() as u32,
            passed: matches!(last.evaluation.verdict, Verdict::Pass),
            final_plan: last.plan,
            final_generation: last.generation,
            final_evaluation: last.evaluation,
            history,
            terminal_reason: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::contract::outcome::{
        is_deterministic_failure, DEGENERATE_LOOP_PREFIX, EMPTY_GENERATION_PREFIX,
    };

    /// Test-only mock implementing the object-level [`cog_core::Agent`] trait.
    /// All methods except [`prompt`] are no-op stubs.
    struct MockAgent {
        response: serde_json::Value,
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for MockAgent {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Ok(self.response.clone())
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
            Ok(self.response.clone())
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

        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }

        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    fn test_task(goal: &str) -> cog_core::Task {
        cog_core::Task::new(
            format!("test-{}", uuid::Uuid::new_v4()),
            cog_core::TaskType::Custom("test".into()),
            serde_json::json!({"goal": goal}),
        )
    }

    #[tokio::test]
    async fn pipeline_executes_three_stages_in_order() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec", "design": "test design"},
                "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
            }),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "content": {"code": "fn main() { println!(\"hello\"); }", "tests": "", "documentation": ""},
                "artifacts": [],
            }),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
        }));

        let task = test_task("implement a hello world function");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert_eq!(result.attempts, 1);
        assert!(
            !result.final_plan.sub_tasks.is_empty(),
            "planner produced tasks"
        );
        assert!(result.passed, "pipeline should pass with stub agents");
    }

    #[tokio::test]
    async fn pipeline_records_full_history_when_failing() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 2,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "", "criteria": []}),
        }));

        let task = test_task("review code quality");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert_eq!(result.history.len(), result.attempts as usize);
        assert!(result.passed);
        assert_eq!(result.attempts, 1);
    }

    #[tokio::test]
    async fn pipeline_default_config_runs_at_least_once() {
        let pipeline = PgePipeline::new(PgePipelineConfig::default());
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "", "criteria": []}),
        }));

        let task = test_task("any goal");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(result.attempts >= 1);
        assert!(!result.history.is_empty());
    }

    #[tokio::test]
    async fn pipeline_zero_retries_still_runs_once() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 0,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "", "criteria": []}),
        }));

        let task = test_task("any");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert_eq!(result.attempts, 1);
    }

    #[tokio::test]
    async fn pipeline_attempts_are_numbered_sequentially() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 3,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "", "criteria": []}),
        }));

        let task = test_task("x");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        for (i, attempt) in result.history.iter().enumerate() {
            assert_eq!(attempt.attempt as usize, i + 1);
        }
    }

    /// Mock agent that returns responses from a sequence, advancing on each prompt.
    struct SequenceMockAgent {
        responses: std::sync::Mutex<Vec<serde_json::Value>>,
    }

    #[async_trait::async_trait]
    impl cog_core::Agent for SequenceMockAgent {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                Ok(serde_json::Value::Null)
            } else {
                Ok(responses.remove(0))
            }
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
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn pipeline_local_repair_succeeds_without_global_reset() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 2,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec"},
                "sub_tasks": [],
            }),
        }));
        // Generator returns different content on repair (when repair_feedback is present).
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "content": {"code": "fn main() {}"},
                "artifacts": [],
            }),
        }));
        // Evaluator fails once, then passes.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"verdict": "fail", "score": 40, "feedback": "missing print", "criteria": []}),
                serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
            ]),
        }));

        let task = test_task("implement hello world");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(result.passed, "pipeline should pass after local repair");
        assert_eq!(result.attempts, 1, "should not need global reset");
        assert_eq!(
            result.history[0].local_repairs.len(),
            1,
            "one local repair cycle"
        );
        assert_eq!(result.history[0].local_repairs[0].feedback, "missing print");
    }

    #[tokio::test]
    async fn pipeline_local_repair_exhausts_then_global_reset() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 2,
            timeout_ms: 5_000,
            local_repair_max: 1,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        // Evaluator always fails.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "fail", "score": 30, "feedback": "bad", "criteria": []}),
        }));

        let task = test_task("unfixable");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed, "pipeline should fail");
        assert_eq!(
            result.attempts, 2,
            "should exhaust local repair then global reset"
        );
        assert_eq!(
            result.history[0].local_repairs.len(),
            1,
            "first attempt uses local_repair_max"
        );
    }

    #[tokio::test]
    async fn pipeline_stops_degenerate_loop_early() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 5,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        // Evaluator fails identically every attempt: flat score, flat error
        // class, no artifacts — the definition of a degenerate loop.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "fail", "score": 30, "feedback": "bad", "criteria": []}),
        }));

        let task = test_task("spinning");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed);
        assert_eq!(
            result.attempts, 3,
            "baseline + 2 flat attempts then stall stop, not 5 paid attempts"
        );
        assert!(
            result
                .final_evaluation
                .feedback
                .starts_with(DEGENERATE_LOOP_PREFIX),
            "stall stop must be marked for outer loops: {}",
            result.final_evaluation.feedback
        );
    }

    #[tokio::test]
    async fn pipeline_progressing_attempts_are_not_stalled() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 4,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"summary": "fallback", "plan": {}, "sub_tasks": []}),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": {"draft": "attempt output"}, "artifacts": []}),
        }));
        // Score improves every attempt: spend is buying progress, so the
        // pipeline must run to the retry ceiling even though it never passes.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"verdict": "fail", "score": 30, "feedback": "bad", "criteria": []}),
                serde_json::json!({"verdict": "fail", "score": 45, "feedback": "bad", "criteria": []}),
                serde_json::json!({"verdict": "fail", "score": 60, "feedback": "bad", "criteria": []}),
                serde_json::json!({"verdict": "fail", "score": 75, "feedback": "bad", "criteria": []}),
            ]),
        }));

        let task = test_task("slow but progressing");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed);
        assert_eq!(result.attempts, 4, "progressing run must not stall-stop");
        assert!(!result
            .final_evaluation
            .feedback
            .starts_with(DEGENERATE_LOOP_PREFIX));
    }

    /// 空信封是失败，但**不是**终止性失败：模型答了、交付物是空的，没有任何
    /// 观测到的证据排除下一轮成功，把它记成环境故障等于按一个从没看见过的事实
    /// 结账。而且这里连评估器都不该被叫起来——没有东西可评，它唯一可能的答复
    /// 就是"没东西可评"，那一次调用只会把这一轮记在噪声底下而不是事实底下。
    #[tokio::test]
    async fn an_empty_envelope_fails_under_its_own_cause_without_evaluating() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = pass_planner();
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": null, "artifacts": []}),
        }));
        // 评估器一旦被叫到就会给通过：运行若通过，就证明它被叫过。
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
        }));

        let result = pipeline
            .execute_task(
                &test_task("produce something"),
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed, "an empty envelope cannot pass");
        assert_eq!(
            result.final_evaluation.criteria[0].name,
            "generation_is_non_empty"
        );
        assert!(
            result
                .final_evaluation
                .feedback
                .contains(EMPTY_GENERATION_PREFIX),
            "{}",
            result.final_evaluation.feedback
        );
        assert!(
            !is_deterministic_failure(&result.final_evaluation.feedback),
            "an empty deliverable names no observed cause, so retries must stay open"
        );
        assert!(result.terminal_reason.is_none());
    }

    /// 空串与 null 是同一个观测：信封到了、里面是空的。分开读会让一个用空串
    /// 作答的生成器逃掉另一个会得到的病因名。
    #[tokio::test]
    async fn a_blank_string_content_is_the_same_empty_envelope() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": "   \n", "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
        }));

        let result = pipeline
            .execute_task(
                &test_task("produce something"),
                serde_json::json!({}),
                &pass_planner(),
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed);
        assert!(result
            .final_evaluation
            .feedback
            .contains(EMPTY_GENERATION_PREFIX));
    }

    /// 修复轮产出的空信封与首轮同罪：判据的理由（没有东西可评）不区分它来自
    /// 哪一轮，少修一条就等于留了一条花掉评估调用却什么也判不出的路径。
    #[tokio::test]
    async fn an_empty_repair_is_judged_under_its_own_cause_too() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 1,
            stall_threshold: 2,
            independent_review: false,
        });
        // 首轮交了东西（走评估器），修复轮交了空信封。
        let generator = GeneratorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"content": {"draft": "first"}, "artifacts": []}),
                serde_json::json!({"content": null, "artifacts": []}),
            ]),
        }));
        // 第一次评审判失败触发修复；第二次若被叫到会给通过，从而暴露"修复轮被送去评估"。
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"verdict": "fail", "score": 30, "feedback": "bad", "criteria": []}),
                serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
            ]),
        }));

        let result = pipeline
            .execute_task(
                &test_task("produce something"),
                serde_json::json!({}),
                &pass_planner(),
                &generator,
                &evaluator,
            )
            .await;

        assert!(!result.passed);
        assert_eq!(result.history[0].local_repairs.len(), 1);
        assert!(
            result.history[0].local_repairs[0]
                .evaluation
                .feedback
                .contains(EMPTY_GENERATION_PREFIX),
            "{}",
            result.history[0].local_repairs[0].evaluation.feedback
        );
    }

    fn pass_planner() -> PlannerActor {
        PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec"},
                "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
            }),
        }))
    }

    fn pass_generator() -> GeneratorActor {
        GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "content": {"code": "fn main() {}"},
                "artifacts": [],
            }),
        }))
    }

    #[tokio::test]
    async fn independent_review_rejection_fails_the_attempt() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 0,
            independent_review: true,
        });
        // First evaluation (author) passes; second evaluation (independent
        // reviewer) rejects. The reviewer verdict must win.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"verdict": "pass", "score": 92, "feedback": "looks fine", "criteria": []}),
                serde_json::json!({"verdict": "fail", "score": 20, "feedback": "reviewer: output ignores the goal", "criteria": []}),
            ]),
        }));

        let task = test_task("goal with hidden flaw");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &pass_planner(),
                &pass_generator(),
                &evaluator,
            )
            .await;

        assert!(!result.passed, "reviewer rejection must fail the attempt");
        assert!(result
            .final_evaluation
            .feedback
            .contains("independent reviewer rejected"));
        let review = result
            .final_evaluation
            .details
            .as_ref()
            .and_then(|d| d.get("independent_review"))
            .expect("reviewer verdict must be recorded alongside the author verdict");
        assert_eq!(review["verdict"], "fail");
    }

    #[tokio::test]
    async fn independent_review_agreement_passes_and_records_review() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 0,
            independent_review: true,
        });
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(SequenceMockAgent {
            responses: std::sync::Mutex::new(vec![
                serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
                serde_json::json!({"verdict": "pass", "score": 88, "feedback": "reviewer agrees", "criteria": []}),
            ]),
        }));

        let task = test_task("solid goal");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &pass_planner(),
                &pass_generator(),
                &evaluator,
            )
            .await;

        assert!(result.passed);
        assert_eq!(
            result
                .final_evaluation
                .details
                .as_ref()
                .and_then(|d| d.get("independent_review"))
                .map(|r| r["feedback"].as_str().unwrap_or("")),
            Some("reviewer agrees")
        );
    }

    /// Agent whose prompt never reaches an upstream.
    struct FailingAgent;

    #[async_trait::async_trait]
    impl cog_core::Agent for FailingAgent {
        async fn prompt(&self, _input: serde_json::Value) -> cog_core::SFResult<serde_json::Value> {
            Err(cog_core::SFError::LLM(
                "HTTP 503 upstream unavailable".into(),
            ))
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
        fn subscribe(&self) -> tokio::sync::broadcast::Receiver<cog_core::AgentEvent> {
            let (_tx, rx) = tokio::sync::broadcast::channel(1);
            rx
        }
        async fn receive_message(&self, _msg: cog_core::InboxMessage) -> cog_core::SFResult<()> {
            Ok(())
        }
    }

    /// 计划侧的上游故障必须终止本次运行，并且说出真因。若 planner 把失败吞成
    /// 一个空计划，这里会看到一次成功的生成和一次对空计划的评估——一次传输故障
    /// 被静默降级成正常流程，外层也拿不到终止性标记。
    #[tokio::test]
    async fn a_planner_that_never_reached_its_upstream_ends_the_run_and_says_why() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 3,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(FailingAgent));
        // 生成器若被调用会回一个可辨认的内容，用它证明这轮根本没走到生成。
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"content": "SHOULD_NOT_RUN", "artifacts": []}),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "", "criteria": []}),
        }));

        let task = test_task("decompose a goal");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        let feedback = &result.final_evaluation.feedback;
        assert!(
            feedback.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX),
            "外层按前缀识别终止性失败，实到: {feedback}"
        );
        assert!(
            feedback.contains("HTTP 503 upstream unavailable"),
            "反馈必须指向真实原因，实到: {feedback}"
        );
        assert_eq!(
            result.final_generation.content,
            serde_json::Value::Null,
            "计划侧已经失败，不该再为同一个上游买一次生成"
        );
        assert_eq!(result.attempts, 1, "环境类失败不该被重试");
    }

    /// A generator whose ReAct loop spends its whole iteration budget still
    /// exploring comes back as the runtime's `max_iterations_reached` sentinel.
    /// Read as an ordinary empty output it buys an evaluator call, a full round
    /// of local repairs and every configured retry — all of them paid, none of
    /// them able to produce the artifact the budget never left room for.
    #[tokio::test]
    async fn a_generator_that_ran_out_of_iterations_ends_the_run_before_the_evaluator() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 3,
            timeout_ms: 5_000,
            local_repair_max: 2,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "summary": "s",
                "plan": {"steps": ["edit"]},
                "sub_tasks": [],
                "acceptance_criteria": []
            }),
        }));
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                "iterations": 10,
                "pending_tool_calls": 2
            }),
        }));
        // The evaluator would pass anything it is handed; its verdict must never
        // be asked for, because asking is what a spent budget must not pay for.
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "verdict": "pass", "score": 100, "feedback": "", "criteria": []
            }),
        }));

        let task = test_task("edit the workspace");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        let feedback = &result.final_evaluation.feedback;
        assert!(
            feedback.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX),
            "the outer loops match on the prefix, got: {feedback}"
        );
        assert!(
            feedback.contains("iteration budget"),
            "the feedback must name the budget as the real cause, got: {feedback}"
        );
        assert!(
            feedback.contains("max_iterations=10"),
            "the feedback must carry the observed numbers, got: {feedback}"
        );
        assert!(!result.passed, "an exhausted budget is not a pass");
        assert_eq!(result.attempts, 1, "an exhausted budget is not retryable");
    }

    /// The third role's version of the same trap. The evaluator's budget is
    /// smaller, so it is reached differently, but the cost is identical: an
    /// empty `Fail` sends the generator back to repair towards feedback nobody
    /// wrote, and the re-judge spends the same budget to reach the same place.
    #[tokio::test]
    async fn an_evaluator_that_ran_out_of_iterations_ends_the_run_before_repairs() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 3,
            timeout_ms: 5_000,
            local_repair_max: 2,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "summary": "s",
                "plan": {"steps": ["edit"]},
                "sub_tasks": [],
                "acceptance_criteria": []
            }),
        }));
        // A generator that did deliver: the run must stop because the judge
        // spent its budget, not because there was nothing to judge.
        let generator = GeneratorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "content": {"code": "fn main() {}"},
                "artifacts": []
            }),
        }));
        let evaluator = EvaluatorActor::new(std::sync::Arc::new(MockAgent {
            response: serde_json::json!({
                "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                "iterations": 5,
                "pending_tool_calls": 3
            }),
        }));

        let task = test_task("edit the workspace");
        let result = pipeline
            .execute_task(
                &task,
                serde_json::json!({}),
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        let feedback = &result.final_evaluation.feedback;
        assert!(
            feedback.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX),
            "the outer loops match on the prefix, got: {feedback}"
        );
        assert!(
            feedback.contains("evaluator") && feedback.contains("iteration budget"),
            "the feedback must put the spend on the evaluator, got: {feedback}"
        );
        assert!(
            !feedback.contains("rejected"),
            "the judge never reached a verdict, so it rejected nothing: {feedback}"
        );
        assert!(!result.passed, "an exhausted budget is not a pass");
        assert_eq!(result.attempts, 1, "an exhausted budget is not retryable");
        assert!(
            result.history[0].local_repairs.is_empty(),
            "no repair can be aimed at feedback that was never written"
        );
    }
}
