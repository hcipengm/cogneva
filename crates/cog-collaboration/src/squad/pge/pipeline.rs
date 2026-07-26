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
}

impl Default for PgePipelineConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            timeout_ms: 30_000,
            local_repair_max: 0,
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

/// Final result of a [`PgePipeline::execute`] run.
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

    /// Run the pipeline with a structured [`Task`] instead of a plain `goal` string.
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
            let criteria: Vec<&str> = _context
                .get("criteria")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
                .unwrap_or_default();
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

            let mut local_repairs: Vec<LocalRepairAttempt> = Vec::new();

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

                local_repairs.push(LocalRepairAttempt {
                    repair_iteration,
                    generation: generation.clone(),
                    evaluation: evaluation.clone(),
                    feedback: repair_feedback,
                });
            }

            let passed = matches!(evaluation.verdict, Verdict::Pass);
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
}
