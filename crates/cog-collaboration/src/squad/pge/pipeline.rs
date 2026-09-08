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
    ProgressSignals, StallDetector, StallVerdict, DEGENERATE_LOOP_PREFIX,
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
    /// loop (score/artifacts/error-class all flat) and stop it early. The
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
    fn terminal_result(
        attempt: u32,
        plan: PlannerOutput,
        generation: GeneratorOutput,
        mut history: Vec<PgePipelineAttempt>,
    ) -> PgePipelineResult {
        let evaluation = EvaluationResult {
            verdict: Verdict::Fail,
            feedback: format!(
                "{}: generator produced no artifacts (environment/protocol failure)",
                crate::squad::pge::types::TERMINAL_ENV_FAILURE_PREFIX
            ),
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
                return Self::terminal_result(attempt, plan, generation, history);
            }

            // Stage 3: Evaluator.
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
                    return Self::terminal_result(attempt, plan.clone(), generation, history);
                }

                evaluation = evaluator
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
                        repair_stall
                            .observe(ProgressSignals::from_attempt(&generation, &evaluation,),),
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
                let signals = ProgressSignals::from_attempt(&generation, &evaluation);
                if matches!(stall.observe(signals), StallVerdict::Stalled) {
                    let mut evaluation = evaluation;
                    evaluation.feedback = format!(
                        "{}: {} consecutive attempts bought no progress \
                         (score/artifacts/error-class flat); stopped early: {}",
                        DEGENERATE_LOOP_PREFIX, self.config.stall_threshold, evaluation.feedback
                    );
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
            response: serde_json::json!({"content": "", "artifacts": []}),
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
}
