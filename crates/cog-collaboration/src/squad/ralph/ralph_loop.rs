//! Loop 1 — Ralph Loop（Squad 外层质量控制循环）。
//! - 无 max_rounds 硬限制，只有两个停止条件：PGE Pass 或 判定"不可修复"。
//! - 全局重置策略（非局部修补）：Identical / Modified / Escalated。
//! - safety_limit 仅为防止失控的运行时保险，设计语义上视为无限。

use crate::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use crate::squad::pge::pipeline::PgePipeline;
use crate::squad::pge::roundtable::{PgeRoundtable, PgeRoundtableResult};
use crate::squad::pge::types::{EvaluationResult, Verdict};
use cog_core::{Task, TaskType};
use std::sync::Arc;

/// Ralph Loop 的最终判定。
#[derive(Debug, Clone)]
pub enum RalphVerdict {
    /// PGE 通过，任务完成。
    Passed {
        result: serde_json::Value,
        iterations: u32,
        history: Vec<RalphIteration>,
    },
    /// 判定不可修复，需上报人工。
    Unrecoverable {
        reason: String,
        iterations: u32,
        history: Vec<RalphIteration>,
    },
}

/// Ralph Loop 单次迭代记录。
#[derive(Debug, Clone, serde::Serialize)]
pub struct RalphIteration {
    pub iteration: u32,
    pub reset_strategy: ResetStrategy,
    pub pge_passed: bool,
    pub feedback: String,
    pub snapshot: serde_json::Value,
}

/// 全局重置策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum ResetStrategy {
    /// 完全复用相同上下文重新执行。
    Identical,
    /// 调整 Prompt / 参数后复用同 Squad。
    Modified,
    /// 更换 Agent 组合，创建新 Squad 接管。
    Escalated,
}

/// 失败原因分析结果。
#[derive(Debug, Clone)]
pub enum FailureAnalysis {
    /// 可修复，附带建议的重置策略。
    Recoverable(ResetStrategy),
    /// 不可修复，附带原因。
    Unrecoverable(String),
}

/// Semantic failure classification returned by LLM analysis.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SemanticFailureAnalysis {
    /// One of: Contradiction, SkillGap, AmbiguousRequirement, ResourceError, LogicError, Unrecoverable
    pub failure_type: String,
    /// Human-readable root cause.
    pub root_cause: String,
    /// Recommended reset strategy: Identical, Modified, Escalated
    pub recommended_strategy: String,
    /// Concrete modifications to apply (e.g. "add error handling for network timeout").
    pub suggested_modifications: String,
}

/// Ralph Loop 配置。
#[derive(Debug, Clone, Copy)]
pub struct RalphLoopConfig {
    /// 运行时安全上限。设计语义无限制，但生产环境需防止资源耗尽。
    /// 默认 1_000（足够大，视为"无限"）。
    pub safety_limit: u32,
}

impl Default for RalphLoopConfig {
    fn default() -> Self {
        Self {
            safety_limit: 1_000,
        }
    }
}

/// Ralph Loop 外层质量控制循环。
#[derive(Default)]
pub struct RalphLoop {
    config: RalphLoopConfig,
    llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    /// 跨重试累积的迭代历史，支持 Ralph Loop 跨 Squad 重试复用历史。
    history: Vec<RalphIteration>,
}

impl RalphLoop {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: RalphLoopConfig) -> Self {
        Self {
            config,
            ..Default::default()
        }
    }

    pub fn with_llm_provider(mut self, llm: Arc<dyn cog_core::LlmClient>) -> Self {
        self.llm_provider = Some(llm);
        self
    }

    /// 以 Pipeline 模式运行 Ralph Loop。
    pub async fn run_pipeline(
        &mut self,
        goal: &str,
        mut context: serde_json::Value,
        pipeline: &PgePipeline,
        planner: &PlannerActor,
        generator: &GeneratorActor,
        evaluator: &EvaluatorActor,
    ) -> RalphVerdict {
        let start_iteration = self.history.len() as u32 + 1;

        for iteration in start_iteration..=self.config.safety_limit {
            crate::observable::global_observable().record_round();
            if iteration > 1 {
                context["ralph_iteration"] = serde_json::json!(iteration);
                if let Some(prev) = self.history.last() {
                    context["ralph_feedback"] = serde_json::json!(&prev.feedback);
                    context["ralph_strategy"] =
                        serde_json::json!(format!("{:?}", prev.reset_strategy));
                }
            }

            let mut input = context.clone();
            input["goal"] = serde_json::json!(goal);
            let task = Task::new(
                format!("ralph-pipeline-{}", uuid::Uuid::new_v4()),
                TaskType::Custom("ralph_pipeline_goal".into()),
                input,
            );
            let pge_result = pipeline
                .execute_task(&task, context.clone(), planner, generator, evaluator)
                .await;
            let passed = matches!(pge_result.final_evaluation.verdict, Verdict::Pass);
            let feedback = pge_result.final_evaluation.feedback.clone();

            let analysis = if passed {
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            } else {
                self.analyze_failure(&pge_result.final_evaluation, &self.history)
                    .await
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let snapshot = serde_json::json!({
                "plan": pge_result.final_plan,
                "generation": pge_result.final_generation,
                "evaluation": pge_result.final_evaluation,
            });

            self.history.push(RalphIteration {
                iteration,
                reset_strategy,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot,
            });

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: serde_json::json!(pge_result),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            match analysis {
                FailureAnalysis::Recoverable(strategy) => {
                    context["reset_strategy"] = serde_json::json!(format!("{:?}", strategy));
                }
                FailureAnalysis::Unrecoverable(reason) => {
                    let total_iterations = self.history.len() as u32;
                    return RalphVerdict::Unrecoverable {
                        reason,
                        iterations: total_iterations,
                        history: self.history.clone(),
                    };
                }
            }
        }

        let total_iterations = self.history.len() as u32;
        RalphVerdict::Unrecoverable {
            reason: format!(
                "Ralph Loop reached safety limit of {} iterations",
                self.config.safety_limit
            ),
            iterations: total_iterations,
            history: self.history.clone(),
        }
    }

    /// 以 Roundtable 模式运行 Ralph Loop。
    pub async fn run_roundtable(
        &mut self,
        goal: &str,
        mut context: serde_json::Value,
        roundtable: &PgeRoundtable,
    ) -> RalphVerdict {
        let start_iteration = self.history.len() as u32 + 1;

        for iteration in start_iteration..=self.config.safety_limit {
            crate::observable::global_observable().record_round();
            if iteration > 1 {
                context["ralph_iteration"] = serde_json::json!(iteration);
                if let Some(prev) = self.history.last() {
                    context["ralph_feedback"] = serde_json::json!(&prev.feedback);
                }
            }

            let mut input = context.clone();
            input["goal"] = serde_json::json!(goal);
            let task = Task::new(
                format!("ralph-roundtable-{}", uuid::Uuid::new_v4()),
                TaskType::Custom("ralph_roundtable_goal".into()),
                input,
            );
            let rt_result = roundtable.debate(&task, context.clone()).await;
            let passed = rt_result.consensus_reached;
            let feedback = if passed {
                "Consensus reached".to_string()
            } else {
                format!(
                    "No consensus after {} internal iterations",
                    rt_result.iterations
                )
            };

            let analysis = if passed {
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            } else {
                Self::analyze_roundtable_failure(&rt_result, &self.history)
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let snapshot = serde_json::json!({ "roundtable": rt_result });

            self.history.push(RalphIteration {
                iteration,
                reset_strategy,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot: snapshot.clone(),
            });

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: serde_json::json!(snapshot),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            match analysis {
                FailureAnalysis::Recoverable(strategy) => {
                    context["reset_strategy"] = serde_json::json!(format!("{:?}", strategy));
                }
                FailureAnalysis::Unrecoverable(reason) => {
                    let total_iterations = self.history.len() as u32;
                    return RalphVerdict::Unrecoverable {
                        reason,
                        iterations: total_iterations,
                        history: self.history.clone(),
                    };
                }
            }
        }

        let total_iterations = self.history.len() as u32;
        RalphVerdict::Unrecoverable {
            reason: format!(
                "Ralph Loop reached safety limit of {} iterations",
                self.config.safety_limit
            ),
            iterations: total_iterations,
            history: self.history.clone(),
        }
    }

    /// 分析 Pipeline 失败原因。
    /// **Design note**: Determining whether a failure is recoverable is a
    /// semantic judgment. The control-flow rule here defaults to
    /// `Recoverable(Identical)` for all failures, with two exceptions:
    /// 1. Repeated identical feedback → loop detection (pure control flow).
    /// 2. Safety-limit exhaustion → terminal unrecoverable.
    ///
    /// When an LLM provider is available, `analyze_failure_with_llm` performs
    /// semantic classification (contradiction, skill gap, etc.) and returns
    /// a targeted reset strategy.
    async fn analyze_failure(
        &self,
        evaluation: &EvaluationResult,
        history: &[RalphIteration],
    ) -> FailureAnalysis {
        // 检测重复相同失败（循环卡住）—— 纯控制流，无需语义理解
        let recent_same_feedback = history
            .iter()
            .rev()
            .take(2)
            .all(|h| h.feedback == evaluation.feedback);
        if recent_same_feedback && history.len() >= 2 {
            return FailureAnalysis::Unrecoverable(format!(
                "Same failure repeated {} times: {}",
                history.len() + 1,
                evaluation.feedback
            ));
        }

        // If LLM provider is available, perform semantic failure analysis.
        if let Some(ref llm) = self.llm_provider {
            match self
                .analyze_failure_with_llm(evaluation, history, llm)
                .await
            {
                Ok(analysis) => return analysis,
                Err(e) => {
                    tracing::warn!(
                        "LLM failure analysis failed, falling back to default: {}",
                        e
                    );
                }
            }
        }

        // Default: all failures are recoverable with Identical retry.
        FailureAnalysis::Recoverable(ResetStrategy::Identical)
    }

    /// Semantic failure analysis powered by LLM.
    /// Constructs a prompt containing the goal, plan, generation, evaluator
    /// feedback, and history. The LLM returns a structured classification
    /// that maps to a targeted [`ResetStrategy`].
    async fn analyze_failure_with_llm(
        &self,
        evaluation: &EvaluationResult,
        history: &[RalphIteration],
        llm: &Arc<dyn cog_core::LlmClient>,
    ) -> cog_core::SFResult<FailureAnalysis> {
        let history_json = serde_json::to_string_pretty(history).unwrap_or_default();

        let prompt = format!(
            "You are a failure-analysis expert for an AI agent system. \
A squad of agents (Planner → Generator → Evaluator) attempted a task but failed. \
Analyze the failure and classify it into one of the following types:\n\
\n\
- Contradiction: the plan and the generated output contradict each other. → strategy: Modified (adjust prompt/context).\n\
- SkillGap: the generator lacks the skill/tool needed to execute the plan. → strategy: Escalated (swap agent composition).\n\
- AmbiguousRequirement: the goal/requirement is unclear or contradictory. → strategy: Modified (re-analyze goal).\n\
- ResourceError: external tool/API failed (network timeout, rate limit, etc.). → strategy: Identical (simple retry).\n\
- LogicError: the generated code/reasoning contains a logical bug. → strategy: Modified (inject error hint).\n\
- Unrecoverable: the task is fundamentally impossible or requires human judgment. → strategy: Unrecoverable.\n\
\n\
Evaluation result:\n{evaluation:?}\n\
\n\
History of previous attempts:\n{history_json}\n\
\n\
Respond with **only** a JSON object matching this schema:\n\
{{\"failure_type\":\"...\",\"root_cause\":\"...\",\"recommended_strategy\":\"...\",\"suggested_modifications\":\"...\"}}"
        );

        let messages = vec![
            cog_core::Message::System {
                content: "You are a precise failure classifier. Respond only with valid JSON."
                    .into(),
                timestamp: chrono::Utc::now(),
            },
            cog_core::Message::User {
                content: prompt,
                timestamp: chrono::Utc::now(),
            },
        ];

        let options = cog_core::ChatOptions {
            model: None,
            temperature: Some(0.2),
            max_tokens: Some(512),
            tools: None,
            // Use Text mode instead of Json: some OpenAI-compatible providers
            // (e.g. Kimi /coding endpoint) reject response_format=json_object
            // for certain models, and the prompt already constrains output to
            // JSON. Parsing is done manually below.
            response_format: cog_core::ResponseFormat::Text,
            ..Default::default()
        };

        let response = llm.chat(&messages, &options).await?;
        if let Some(ref err) = response.error_message {
            return Err(cog_core::SFError::LLM(format!(
                "Failure-analysis LLM returned API error: {err}"
            )));
        }

        let text = response
            .content
            .iter()
            .filter_map(|block| match block {
                cog_core::ContentBlock::Text { text, .. } => Some(text.as_str()),
                // kimi-k2.6 sometimes returns reasoning-only output with no text
                // content. Treat reasoning blocks as text so we can still parse the
                // structured classification.
                cog_core::ContentBlock::Thinking { thinking, .. } => Some(thinking.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");

        if text.trim().is_empty() {
            tracing::warn!(
                "Failure-analysis response has no usable text; content blocks: {:?}",
                response.content
            );
            return Err(cog_core::SFError::LLM(
                "Failure-analysis LLM returned empty content".into(),
            ));
        }

        // Robust JSON extraction — handle markdown fences.
        let json_str = if text.starts_with("```json") {
            text.trim_start_matches("```json")
                .trim_end_matches("```")
                .trim()
        } else if text.starts_with("```") {
            text.trim_start_matches("```")
                .trim_end_matches("```")
                .trim()
        } else {
            text.as_str()
        };

        let semantic: SemanticFailureAnalysis = serde_json::from_str(json_str).map_err(|e| {
            cog_core::SFError::LLM(format!("Failed to parse semantic analysis JSON: {e}"))
        })?;

        tracing::info!(
            "Semantic failure analysis: type={}, strategy={}",
            semantic.failure_type,
            semantic.recommended_strategy
        );

        let strategy = match semantic.recommended_strategy.to_lowercase().as_str() {
            "identical" => ResetStrategy::Identical,
            "modified" => ResetStrategy::Modified,
            "escalated" => ResetStrategy::Escalated,
            _ => ResetStrategy::Identical,
        };

        if semantic.failure_type.to_lowercase() == "unrecoverable" {
            Ok(FailureAnalysis::Unrecoverable(format!(
                "{}: {}",
                semantic.root_cause, semantic.suggested_modifications
            )))
        } else {
            Ok(FailureAnalysis::Recoverable(strategy))
        }
    }

    /// 分析 Roundtable 失败原因。
    fn analyze_roundtable_failure(
        result: &PgeRoundtableResult,
        history: &[RalphIteration],
    ) -> FailureAnalysis {
        // 若最终 verdict 为 Fail，视为无有效输出（verdict 是核心信号，score 仅作参考）
        if matches!(result.final_evaluation.verdict, Verdict::Fail) {
            return FailureAnalysis::Unrecoverable("Roundtable produced no viable output".into());
        }

        // 检测 Roundtable 是否卡住（连续相同 verdict）
        let current_verdict_str = match result.final_evaluation.verdict {
            Verdict::Pass => "Pass",
            Verdict::Fail => "Fail",
            Verdict::Partial => "Partial",
            Verdict::NeedsReview => "NeedsReview",
            Verdict::Retry => "Retry",
        };
        let recent_same = history.iter().rev().take(2).all(|h| {
            h.snapshot
                .get("roundtable")
                .and_then(|r| r.get("final_evaluation"))
                .and_then(|e| e.get("verdict"))
                .and_then(|s| s.as_str())
                == Some(current_verdict_str)
        });
        if recent_same && history.len() >= 2 {
            return FailureAnalysis::Unrecoverable(
                "Roundtable stuck with identical verdicts".into(),
            );
        }

        FailureAnalysis::Recoverable(ResetStrategy::Modified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::squad::pge::{PgePipeline, PgePipelineConfig, PgeRoundtable, PgeRoundtableConfig};
    use std::sync::Arc;

    /// Test-only mock implementing the object-level [`cog_core::Agent`] trait.
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

    fn pass_planner() -> MockAgent {
        MockAgent {
            response: serde_json::json!({
                "summary": "test analysis",
                "plan": {"specification": "test spec", "design": "test design"},
                "sub_tasks": [{"id": "t1", "name": "Task 1", "task_type": "generate", "input": {}, "blocked_by": []}],
            }),
        }
    }

    fn pass_generator() -> MockAgent {
        MockAgent {
            response: serde_json::json!({
                "content": {"code": "fn main() {}", "tests": "", "documentation": ""},
                "artifacts": [],
            }),
        }
    }

    fn pass_evaluator() -> MockAgent {
        MockAgent {
            response: serde_json::json!({"verdict": "pass", "score": 92, "feedback": "good", "criteria": []}),
        }
    }

    #[tokio::test]
    async fn ralph_pipeline_passes_on_first_attempt() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig { safety_limit: 10 });
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
        });
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(pass_generator()));
        let evaluator = EvaluatorActor::new(Arc::new(pass_evaluator()));

        let verdict = ralph
            .run_pipeline(
                "test goal",
                serde_json::json!({}),
                &pipeline,
                &planner,
                &generator,
                &evaluator,
            )
            .await;

        match verdict {
            RalphVerdict::Passed { iterations, .. } => {
                assert_eq!(iterations, 1);
            }
            other => panic!("Expected Passed, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn ralph_detects_repeated_failure_as_unrecoverable() {
        let fail_eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(30),
            feedback: "Code needs improvement".into(),
            criteria: vec![],
            details: None,
        };

        let history = vec![
            RalphIteration {
                iteration: 1,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: "Code needs improvement".into(),
                snapshot: serde_json::Value::Null,
            },
            RalphIteration {
                iteration: 2,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: "Code needs improvement".into(),
                snapshot: serde_json::Value::Null,
            },
        ];

        let ralph = RalphLoop::new();
        let analysis = ralph.analyze_failure(&fail_eval, &history).await;
        assert!(
            matches!(analysis, FailureAnalysis::Unrecoverable(_)),
            "repeated identical failure should be unrecoverable"
        );
    }

    #[tokio::test]
    async fn ralph_defaults_to_recoverable_for_single_failure() {
        // Without LLM, a single failure defaults to Recoverable(Identical).
        // Semantic classification (contradiction, skill gap, etc.) belongs
        // in an LLM-powered path, not in code heuristics.
        let fail_eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "The requirements contain a contradiction".into(),
            criteria: vec![],
            details: None,
        };

        let ralph = RalphLoop::new();
        let analysis = ralph.analyze_failure(&fail_eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            ),
            "single failure should default to Identical retry"
        );
    }

    #[tokio::test]
    async fn ralph_roundtable_with_low_threshold_passes() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig { safety_limit: 10 });
        let config = PgeRoundtableConfig {
            max_iterations: 3,
            consensus_threshold: 0.3,
            skill_ids: Vec::new(),
            context_board: None,
            board_store: None,
            moderator: None,
            ..Default::default()
        };
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(pass_generator()));
        let evaluator = EvaluatorActor::new(Arc::new(pass_evaluator()));
        let roundtable = PgeRoundtable::new(config, planner, generator, evaluator);

        let verdict = ralph
            .run_roundtable("test", serde_json::json!({}), &roundtable)
            .await;

        match verdict {
            RalphVerdict::Passed { iterations, .. } => {
                // 第一次 internal iteration 因 prev_score=-1 不会 break，
                // 第二次因 score 差值 <5 达成 consensus，Ralph 总迭代应为 1。
                assert_eq!(iterations, 1);
            }
            other => panic!("Expected Passed with low threshold, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn ralph_safety_limit_triggers_unrecoverable() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig { safety_limit: 2 });
        // consensus_threshold=1.0 要求 score=100，mock evaluator 返回 92 分 → 无法 consensus。
        // Ralph 两轮后触发 safety_limit。
        let config = PgeRoundtableConfig {
            max_iterations: 1,
            consensus_threshold: 1.0,
            skill_ids: Vec::new(),
            context_board: None,
            board_store: None,
            moderator: None,
            ..Default::default()
        };
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(pass_generator()));
        let evaluator = EvaluatorActor::new(Arc::new(pass_evaluator()));
        let roundtable = PgeRoundtable::new(config, planner, generator, evaluator);

        let verdict = ralph
            .run_roundtable("test", serde_json::json!({}), &roundtable)
            .await;

        match verdict {
            RalphVerdict::Unrecoverable { iterations, .. } => {
                assert_eq!(iterations, 2);
            }
            other => panic!("Expected Unrecoverable at safety limit, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------
    // Mock LLM provider for semantic failure analysis tests
    // -----------------------------------------------------------------
    struct MockSemanticLlm {
        response_json: String,
    }

    #[async_trait::async_trait]
    impl cog_core::LlmClient for MockSemanticLlm {
        async fn chat_stream(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            let (stream, mut producer) = cog_core::EventStream::with_capacity(4);
            let response = cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::Text {
                    text: self.response_json.clone(),
                    text_signature: None,
                }],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            };
            producer.end(response);
            Ok(stream)
        }

        async fn complete_stream(
            &self,
            _prompt: &str,
            _options: &cog_core::CompleteOptions,
        ) -> cog_core::SFResult<cog_core::AssistantMessageEventStream> {
            unimplemented!()
        }

        async fn chat(
            &self,
            _messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            Ok(cog_core::ChatResponse {
                content: vec![cog_core::ContentBlock::Text {
                    text: self.response_json.clone(),
                    text_signature: None,
                }],
                api: "mock".into(),
                provider: "mock".into(),
                model: "mock".into(),
                response_id: None,
                usage: cog_core::Usage::default(),
                stop_reason: cog_core::StopReason::Stop,
                error_message: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn make_ralph_with_mock(response_json: &str) -> RalphLoop {
        let llm = Arc::new(MockSemanticLlm {
            response_json: response_json.into(),
        });
        RalphLoop::new().with_llm_provider(llm)
    }

    #[tokio::test]
    async fn ralph_semantic_contradiction_maps_to_modified() {
        let ralph = make_ralph_with_mock(
            r#"{"failure_type":"Contradiction","root_cause":"plan vs output mismatch","recommended_strategy":"Modified","suggested_modifications":"align plan"}"#,
        );
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "Contradiction detected".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Modified)
            ),
            "Contradiction should map to Modified, got {:?}",
            analysis
        );
    }

    #[tokio::test]
    async fn ralph_semantic_skill_gap_maps_to_escalated() {
        let ralph = make_ralph_with_mock(
            r#"{"failure_type":"SkillGap","root_cause":"missing tool","recommended_strategy":"Escalated","suggested_modifications":"swap agent"}"#,
        );
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "Missing skill".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Escalated)
            ),
            "SkillGap should map to Escalated, got {:?}",
            analysis
        );
    }

    #[tokio::test]
    async fn ralph_semantic_resource_error_maps_to_identical() {
        let ralph = make_ralph_with_mock(
            r#"{"failure_type":"ResourceError","root_cause":"rate limit","recommended_strategy":"Identical","suggested_modifications":"retry"}"#,
        );
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "API rate limited".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            ),
            "ResourceError should map to Identical, got {:?}",
            analysis
        );
    }

    #[tokio::test]
    async fn ralph_semantic_unrecoverable_maps_to_unrecoverable() {
        let ralph = make_ralph_with_mock(
            r#"{"failure_type":"Unrecoverable","root_cause":"impossible task","recommended_strategy":"Unrecoverable","suggested_modifications":"human review"}"#,
        );
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "Task impossible".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(analysis, FailureAnalysis::Unrecoverable(_)),
            "Unrecoverable should map to Unrecoverable, got {:?}",
            analysis
        );
    }

    #[tokio::test]
    async fn ralph_semantic_unknown_strategy_falls_back_to_identical() {
        let ralph = make_ralph_with_mock(
            r#"{"failure_type":"LogicError","root_cause":"bug","recommended_strategy":"UnknownStrategy","suggested_modifications":"fix"}"#,
        );
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "Logic bug".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            ),
            "Unknown strategy should fallback to Identical, got {:?}",
            analysis
        );
    }
}
