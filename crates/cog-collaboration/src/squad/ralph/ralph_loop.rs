//! Loop 1 — Ralph Loop（Squad 外层质量控制循环）。
//! - 停止条件：PGE Pass、判定"不可修复"、停滞检测、或迭代预算耗尽。
//! - 全局重置策略（非局部修补）：Identical / Modified / Escalated。
//! - 迭代预算（max_iterations）是**一次执行**的有界止损：无人值守场景没有
//!   操作者盯流调 prompt，不收敛的链必须在预算内终止，不能"视为无限"地烧。
//!   预算不跨执行累计——持久化的历史是给下一次执行看的反馈与停滞证据，
//!   不是被消耗掉的额度。

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RalphIteration {
    pub iteration: u32,
    pub reset_strategy: ResetStrategy,
    pub pge_passed: bool,
    pub feedback: String,
    pub snapshot: serde_json::Value,
    /// Hard-progress readings for this iteration. Persisted so the stall
    /// verdict survives a restart: an iteration's outcome is only comparable
    /// against its predecessor, and the predecessor may live in another
    /// process. `None` for entries written before this was recorded, and for
    /// branches whose snapshot shape carries no score/artifacts — such an
    /// entry is undecidable, never counted as progress nor as its absence.
    #[serde(default)]
    pub progress: Option<IterationProgress>,
}

/// What one iteration bought, in the only terms that mean progress: a better
/// evaluation score, or a bigger deliverable.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct IterationProgress {
    pub score: u32,
    pub artifact_count: usize,
    pub artifact_bytes: usize,
}

impl IterationProgress {
    /// Strictly better than `prev` on score or artifact size. Wording changes
    /// are not progress — a loop that rephrases the same failure is spinning.
    fn beats(&self, prev: &Self) -> bool {
        self.score > prev.score
            || self.artifact_count > prev.artifact_count
            || self.artifact_bytes > prev.artifact_bytes
    }
}

fn iteration_progress(
    generation: &crate::squad::pge::types::GeneratorOutput,
    evaluation: &EvaluationResult,
) -> IterationProgress {
    IterationProgress {
        score: evaluation.score.unwrap_or(0),
        artifact_count: generation.artifacts.len(),
        artifact_bytes: generation.artifacts.iter().map(|a| a.content.len()).sum(),
    }
}

/// 全局重置策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
    /// 单次执行的迭代预算硬上限。达到即终止并归档结论——不收敛的链继续
    /// 迭代只是燃烧 token（实证：曾有不收敛链跑到 600+ 迭代零通过）。
    pub max_iterations: u32,
    /// 停滞窗口：最近这么多轮不买进展即判定停滞并终止。两条判据任一命中
    /// 都算停滞——归一化反馈逐字相同（同一失败原样重放），或没有任何硬
    /// 进展（分数未升且产物未增长，即换着说法重复同一个失败）。0 = 关闭
    /// 停滞检测。
    pub stagnation_window: u32,
}

impl Default for RalphLoopConfig {
    fn default() -> Self {
        Self {
            max_iterations: 50,
            stagnation_window: 5,
        }
    }
}

/// Board field under which the Ralph iteration history of a task is stored.
/// Persisting it after every iteration lets a restarted task resume with its
/// accumulated feedback instead of restarting blind.
const RALPH_HISTORY_FIELD: &str = "ralph_history";

/// Ralph Loop 外层质量控制循环。
#[derive(Default)]
pub struct RalphLoop {
    config: RalphLoopConfig,
    llm_provider: Option<Arc<dyn cog_core::LlmClient>>,
    /// 跨重试累积的迭代历史，支持 Ralph Loop 跨 Squad 重试复用历史。
    ///
    /// 它是**上下文**，不是额度：每次执行的迭代都从 1 开始计数，已归档的
    /// 历史只提供两件东西——上一轮的反馈，以及"这个目标还没有买过进展"的
    /// 证据。历史曾经同时充当终身计数器，于是一个逐次成功的任务会把自己
    /// 的成功逐条记成消耗，攒满预算后永久返回"预算耗尽"且不再执行：系统
    /// 一旦失败过就再也无法用改进后的代码重试同一个目标。
    history: Vec<RalphIteration>,
    /// History persistence: when set, the history is loaded from the task's
    /// context board before the first iteration and written back after every
    /// iteration, so a crashed pod's replacement resumes where it stopped.
    history_task_id: Option<String>,
    state_backend: Option<Arc<dyn cog_core::StateBackend>>,
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

    /// Persist the iteration history on the owning task's context board.
    /// `task_id` must be the DAG task id so a retried/transferred execution
    /// of the same task finds the history.
    pub fn with_history_store(
        mut self,
        task_id: String,
        backend: Arc<dyn cog_core::StateBackend>,
    ) -> Self {
        self.history_task_id = Some(task_id);
        self.state_backend = Some(backend);
        self
    }

    /// How many trailing iterations are worth keeping. A later execution only
    /// consumes two things from the archive: the feedback of the most recent
    /// attempt and the evidence behind the stall verdict. Everything older is
    /// dead weight — and it used to be worse than dead weight, because the
    /// archive doubled as a lifetime counter.
    fn history_keep(&self) -> usize {
        (self.config.stagnation_window as usize).max(1)
    }

    /// Load a previously persisted history, replacing the in-memory one.
    /// Only the tail is kept: the archive's job is to carry the last verdict
    /// forward, and reading back every iteration ever run made a re-drive pay
    /// for history it would never look at.
    /// Best-effort: a missing or unreadable board starts from empty.
    async fn load_history(&mut self) {
        let (Some(task_id), Some(backend)) = (&self.history_task_id, &self.state_backend) else {
            return;
        };
        match backend.get_board(task_id).await {
            Ok(Some(board)) => {
                if let Some(raw) = board.fields.get(RALPH_HISTORY_FIELD) {
                    match serde_json::from_str::<Vec<RalphIteration>>(raw) {
                        Ok(mut history) if !history.is_empty() => {
                            let total = history.len();
                            let keep = self.history_keep();
                            if total > keep {
                                history.drain(..total - keep);
                            }
                            tracing::info!(
                                task_id,
                                archived = total,
                                restored = history.len(),
                                "Ralph history restored from state backend"
                            );
                            self.history = history;
                        }
                        Ok(_) => {}
                        Err(e) => {
                            tracing::warn!(task_id, "Ralph history parse failed: {}", e);
                        }
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!(task_id, "Ralph history load failed: {}", e);
            }
        }
    }

    /// Persist the current history's tail. Best-effort: history loss degrades
    /// a restart to a fresh run, it must never fail the loop itself.
    async fn persist_history(&self) {
        let (Some(task_id), Some(backend)) = (&self.history_task_id, &self.state_backend) else {
            return;
        };
        let keep = self.history_keep();
        let start = self.history.len().saturating_sub(keep);
        match serde_json::to_string(&self.history[start..]) {
            Ok(raw) => {
                if let Err(e) = backend
                    .set_board_field(task_id, RALPH_HISTORY_FIELD, &raw)
                    .await
                {
                    tracing::warn!(task_id, "Ralph history persist failed: {}", e);
                }
            }
            Err(e) => tracing::warn!(task_id, "Ralph history serialize failed: {}", e),
        }
    }

    /// 停滞判定：最近 stagnation_window 轮没有买到任何进展——继续迭代只是
    /// 原地烧钱。两条判据任一命中即成立：
    /// 1. 归一化反馈逐字相同：同一个失败原样重放；
    /// 2. 没有任何硬进展：分数未升且产物未增长。模型常把同一个失败换着说法
    ///    重写一遍（"第 18 种失败模式"、"第 6 次仍是诚实的空操作"），逐字判据
    ///    抓不到，但这类改写同样没有买到任何东西。
    ///
    /// 有重置策略变化或任一轮通过则不判停滞：前者说明还在换手段，后者说明
    /// 目标可达。纯确定性判据，不引入额外 LLM 调用。
    fn is_stagnated(&self) -> bool {
        let window = self.config.stagnation_window as usize;
        if window == 0 || self.history.len() < window {
            return false;
        }
        let tail = &self.history[self.history.len() - window..];
        if tail.iter().any(|it| it.pge_passed) {
            return false;
        }
        if tail
            .iter()
            .any(|it| it.reset_strategy != ResetStrategy::Identical)
        {
            return false;
        }
        let normalize = |s: &str| {
            s.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase()
        };
        let first = normalize(&tail[0].feedback);
        let identical_failure = tail.iter().all(|it| normalize(&it.feedback) == first);
        identical_failure || self.tail_bought_no_progress(tail)
    }

    /// 尾部这一窗内在分数或产物上没有抬升过——第一个读数只立基线，之后每个
    /// 读数都必须严格超过此前的最好成绩。窗口内读数缺失（本次改动前归档的
    /// 记录、无分/无产物的分支）按"未观测"处理，既不当作进展也不当作停滞：
    /// 少于两个读数就无从比较，一律返回 false，把判断交回给逐字判据。缺了这道
    /// 下限，一个只是没被观测过的窗口会被 `all` 的空真判成"没进展"。
    fn tail_bought_no_progress(&self, tail: &[RalphIteration]) -> bool {
        let readings: Vec<IterationProgress> = tail.iter().filter_map(|it| it.progress).collect();
        let Some((baseline, rest)) = readings.split_first() else {
            return false;
        };
        if rest.is_empty() {
            return false;
        }
        let mut best = *baseline;
        rest.iter().all(|cur| {
            if cur.beats(&best) {
                best = *cur;
                false
            } else {
                true
            }
        })
    }

    /// 停滞终止的统一形态：Warn 日志 + 指标 + Unrecoverable 判定，
    /// 历史与结论随 verdict 归档，不是无声消失。
    fn stagnated_verdict(&self) -> RalphVerdict {
        let total_iterations = self.history.len() as u32;
        tracing::warn!(
            iterations = total_iterations,
            window = self.config.stagnation_window,
            "Ralph Loop stagnated: no progress across the window; terminating"
        );
        crate::observable::global_observable().record_ralph_termination("stagnated");
        RalphVerdict::Unrecoverable {
            reason: format!(
                "Ralph Loop stagnated: no progress signal across the last {} iterations",
                self.config.stagnation_window
            ),
            iterations: total_iterations,
            history: self.history.clone(),
        }
    }

    /// 预算耗尽的统一形态（与停滞同构，便于下游按 reason 前缀分类）。
    /// 这里耗尽的总是**本次执行**的预算；下一次执行重新起步，不继承消耗。
    fn budget_exhausted_verdict(&self) -> RalphVerdict {
        let total_iterations = self.history.len() as u32;
        tracing::warn!(
            iterations = total_iterations,
            max_iterations = self.config.max_iterations,
            "Ralph Loop exhausted this run's iteration budget; terminating"
        );
        crate::observable::global_observable().record_ralph_termination("budget_exhausted");
        RalphVerdict::Unrecoverable {
            reason: format!(
                "Ralph Loop exhausted iteration budget of {}",
                self.config.max_iterations
            ),
            iterations: total_iterations,
            history: self.history.clone(),
        }
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
        self.load_history().await;

        // 本轮自己的预算：归档历史提供反馈与停滞证据，不从预算里扣。
        // 起算点跟着历史走曾让预算变成目标的终身配额——攒满之后每次重驱
        // 都瞬间"耗尽"且一轮都不跑，连已经能通过的目标也被永久判死。
        for iteration in 1..=self.config.max_iterations {
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
            } else if pge_result.final_generation.is_terminal_env_failure() {
                // Deterministic environment/protocol failure: no reset
                // strategy can fix it, stop before another paid iteration.
                FailureAnalysis::Unrecoverable(format!(
                    "{}: generator produced no artifacts (environment/protocol failure)",
                    crate::squad::pge::types::TERMINAL_ENV_FAILURE_PREFIX
                ))
            } else {
                self.analyze_failure(&pge_result.final_evaluation, &self.history)
                    .await
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let progress = Some(iteration_progress(
                &pge_result.final_generation,
                &pge_result.final_evaluation,
            ));
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
                progress,
            });
            self.persist_history().await;

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: serde_json::json!(pge_result),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            // 确定性失败的结论优先于停滞：环境/协议的失败自带"重试无用"
            // 的语义，比笼统的"没进展"更该被上层看到。
            if let FailureAnalysis::Unrecoverable(reason) = &analysis {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Unrecoverable {
                    reason: reason.clone(),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            if self.is_stagnated() {
                return self.stagnated_verdict();
            }

            if let FailureAnalysis::Recoverable(strategy) = analysis {
                context["reset_strategy"] = serde_json::json!(format!("{:?}", strategy));
            }
        }

        self.budget_exhausted_verdict()
    }

    /// 以 Roundtable 模式运行 Ralph Loop。
    pub async fn run_roundtable(
        &mut self,
        goal: &str,
        mut context: serde_json::Value,
        roundtable: &PgeRoundtable,
    ) -> RalphVerdict {
        self.load_history().await;

        // 与 Pipeline 同构：预算属于本次执行，历史只提供反馈与停滞证据。
        for iteration in 1..=self.config.max_iterations {
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
            } else if rt_result.final_generation.is_terminal_env_failure() {
                FailureAnalysis::Unrecoverable(format!(
                    "{}: generator produced no artifacts (environment/protocol failure)",
                    crate::squad::pge::types::TERMINAL_ENV_FAILURE_PREFIX
                ))
            } else {
                Self::analyze_roundtable_failure(&rt_result, &self.history)
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let progress = Some(iteration_progress(
                &rt_result.final_generation,
                &rt_result.final_evaluation,
            ));
            let snapshot = serde_json::json!({ "roundtable": rt_result });

            self.history.push(RalphIteration {
                iteration,
                reset_strategy,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot: snapshot.clone(),
                progress,
            });
            self.persist_history().await;

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: serde_json::json!(snapshot),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            if let FailureAnalysis::Unrecoverable(reason) = &analysis {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Unrecoverable {
                    reason: reason.clone(),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            if self.is_stagnated() {
                return self.stagnated_verdict();
            }

            if let FailureAnalysis::Recoverable(strategy) = analysis {
                context["reset_strategy"] = serde_json::json!(format!("{:?}", strategy));
            }
        }

        self.budget_exhausted_verdict()
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
        // Stall detection already determined this run is a degenerate loop:
        // classified, non-retryable, no semantic analysis needed.
        if evaluation
            .feedback
            .starts_with(crate::squad::pge::stall::DEGENERATE_LOOP_PREFIX)
        {
            return FailureAnalysis::Unrecoverable(evaluation.feedback.clone());
        }

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
                content: vec![cog_core::ContentBlock::text(prompt)],
                timestamp: chrono::Utc::now(),
            },
        ];

        let options = cog_core::ChatOptions {
            model: None,
            temperature: Some(0.2),
            // Four verbose JSON fields routinely fill ~1.5k chars; 512 tokens
            // truncated the answer mid-string ("EOF while parsing a string")
            // and every failure classification silently fell back to Identical.
            max_tokens: Some(1024),
            tools: None,
            // Use Text mode instead of Json: some OpenAI-compatible providers
            // (e.g. Kimi /coding endpoint) reject response_format=json_object
            // for certain models, and the prompt already constrains output to
            // JSON. Parsing is done manually below.
            response_format: cog_core::ResponseFormat::Text,
            ..Default::default()
        }
        .with_actor("ralph");

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
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 10,
            ..Default::default()
        });
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
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
                progress: None,
            },
            RalphIteration {
                iteration: 2,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: false,
                feedback: "Code needs improvement".into(),
                snapshot: serde_json::Value::Null,
                progress: None,
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
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 10,
            ..Default::default()
        });
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
    async fn ralph_budget_exhaustion_triggers_unrecoverable() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 2,
            ..Default::default()
        });
        // consensus_threshold=1.0 要求 score=100，mock evaluator 返回 92 分 → 无法 consensus。
        // Ralph 两轮后耗尽迭代预算。
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

    fn failed_iteration(iteration: u32, strategy: ResetStrategy, feedback: &str) -> RalphIteration {
        RalphIteration {
            iteration,
            reset_strategy: strategy,
            pge_passed: false,
            feedback: feedback.to_string(),
            snapshot: serde_json::json!({}),
            progress: None,
        }
    }

    #[test]
    fn stagnation_detected_on_identical_repeated_failures() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        for i in 1..=3 {
            ralph.history.push(failed_iteration(
                i,
                ResetStrategy::Identical,
                "Evaluator rejected: missing tests",
            ));
        }
        assert!(ralph.is_stagnated());
    }

    #[test]
    fn stagnation_ignores_whitespace_and_case_drift() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 2,
        });
        ralph.history.push(failed_iteration(
            1,
            ResetStrategy::Identical,
            "Missing  Tests",
        ));
        ralph.history.push(failed_iteration(
            2,
            ResetStrategy::Identical,
            "missing tests",
        ));
        assert!(ralph.is_stagnated());
    }

    #[test]
    fn no_stagnation_when_feedback_varies() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 2,
        });
        ralph.history.push(failed_iteration(
            1,
            ResetStrategy::Identical,
            "missing tests",
        ));
        ralph.history.push(failed_iteration(
            2,
            ResetStrategy::Identical,
            "wrong return type",
        ));
        assert!(!ralph.is_stagnated());
    }

    #[test]
    fn no_stagnation_when_strategy_changes() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 2,
        });
        ralph.history.push(failed_iteration(
            1,
            ResetStrategy::Identical,
            "missing tests",
        ));
        ralph.history.push(failed_iteration(
            2,
            ResetStrategy::Modified,
            "missing tests",
        ));
        assert!(!ralph.is_stagnated());
    }

    #[test]
    fn stagnation_window_zero_disables_detection() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 0,
        });
        for i in 1..=5 {
            ralph
                .history
                .push(failed_iteration(i, ResetStrategy::Identical, "same"));
        }
        assert!(!ralph.is_stagnated());
    }

    #[test]
    fn stagnation_needs_full_window() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 5,
        });
        for i in 1..=4 {
            ralph
                .history
                .push(failed_iteration(i, ResetStrategy::Identical, "same"));
        }
        assert!(!ralph.is_stagnated());
    }

    fn failed_iteration_with_readings(
        iteration: u32,
        feedback: &str,
        progress: IterationProgress,
    ) -> RalphIteration {
        RalphIteration {
            iteration,
            reset_strategy: ResetStrategy::Identical,
            pge_passed: false,
            feedback: feedback.to_string(),
            snapshot: serde_json::json!({}),
            progress: Some(progress),
        }
    }

    fn readings(score: u32, artifact_bytes: usize) -> IterationProgress {
        IterationProgress {
            score,
            artifact_count: 1,
            artifact_bytes,
        }
    }

    /// 换着说法重复同一个失败同样算停滞：逐字判据抓不到改写，但这一窗在分数
    /// 和产物上都没有抬升，继续迭代只是原地烧钱。
    #[test]
    fn stagnation_detected_when_window_buys_no_hard_progress() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        for (i, feedback) in [
            "第 16 种失败模式",
            "第 17 次仍是诚实的空操作",
            "换个说法：依然没有任何产出",
        ]
        .iter()
        .enumerate()
        {
            ralph.history.push(failed_iteration_with_readings(
                i as u32 + 1,
                feedback,
                readings(30, 100),
            ));
        }
        assert!(ralph.is_stagnated());
    }

    /// 只有一个读数无从比较：本次改动前归档的记录没有读数，重驱的第一轮会
    /// 让窗口里只出现一个读数。空真会把"没观测过"判成"没进展"，第一轮就
    /// 掐掉整个目标。
    #[test]
    fn single_reading_is_not_evidence_of_no_progress() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        ralph.history.push(failed_iteration(
            1,
            ResetStrategy::Identical,
            "stale entry without readings",
        ));
        ralph.history.push(failed_iteration(
            2,
            ResetStrategy::Identical,
            "another stale entry",
        ));
        ralph.history.push(failed_iteration_with_readings(
            3,
            "first observed",
            readings(30, 100),
        ));
        assert!(!ralph.is_stagnated());
    }

    /// 有真进展就不判停滞——哪怕措辞一轮比一轮难听。
    #[test]
    fn no_stagnation_when_hard_progress_appears() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        ralph.history.push(failed_iteration_with_readings(
            1,
            "missing tests",
            readings(30, 100),
        ));
        ralph.history.push(failed_iteration_with_readings(
            2,
            "return type wrong",
            readings(30, 100),
        ));
        ralph.history.push(failed_iteration_with_readings(
            3,
            "one test still red",
            readings(60, 400),
        ));
        assert!(!ralph.is_stagnated());
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

    /// In-memory board-only StateBackend for history persistence tests.
    struct BoardMockBackend {
        fields: std::sync::Mutex<std::collections::HashMap<(String, String), String>>,
    }

    impl BoardMockBackend {
        fn new() -> Self {
            Self {
                fields: std::sync::Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl cog_core::StateBackend for BoardMockBackend {
        async fn get_agent_state(
            &self,
            _agent_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::AgentState>> {
            Ok(None)
        }

        async fn set_agent_state(
            &self,
            _agent_id: &str,
            _state: &cog_core::AgentState,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn cas_agent_state(
            &self,
            _agent_id: &str,
            _expected: &cog_core::AgentState,
            _new: &cog_core::AgentState,
        ) -> cog_core::SFResult<bool> {
            Ok(false)
        }

        async fn get_checkpoint(
            &self,
            _task_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::TaskCheckpoint>> {
            Ok(None)
        }

        async fn save_checkpoint(
            &self,
            _checkpoint: &cog_core::TaskCheckpoint,
        ) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn append_event(
            &self,
            _task_id: &str,
            _event: &cog_core::Event,
        ) -> cog_core::SFResult<u64> {
            Ok(0)
        }

        async fn get_events(
            &self,
            _task_id: &str,
            _offset: u64,
            _limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::Event>> {
            Ok(Vec::new())
        }

        async fn get_board(
            &self,
            task_id: &str,
        ) -> cog_core::SFResult<Option<cog_core::ContextBoard>> {
            let fields = self.fields.lock().unwrap();
            let mut board = cog_core::ContextBoard {
                task_id: task_id.to_string(),
                ..Default::default()
            };
            for ((tid, field), value) in fields.iter() {
                if tid == task_id {
                    board.fields.insert(field.clone(), value.clone());
                }
            }
            if board.fields.is_empty() {
                Ok(None)
            } else {
                Ok(Some(board))
            }
        }

        async fn set_board_field(
            &self,
            task_id: &str,
            field: &str,
            value: &str,
        ) -> cog_core::SFResult<()> {
            self.fields
                .lock()
                .unwrap()
                .insert((task_id.to_string(), field.to_string()), value.to_string());
            Ok(())
        }

        async fn delete_checkpoint(&self, _task_id: &str) -> cog_core::SFResult<()> {
            Ok(())
        }

        async fn delete_board(&self, task_id: &str) -> cog_core::SFResult<()> {
            self.fields
                .lock()
                .unwrap()
                .retain(|(tid, _), _| tid != task_id);
            Ok(())
        }

        async fn remove_board_field(&self, task_id: &str, field: &str) -> cog_core::SFResult<()> {
            self.fields
                .lock()
                .unwrap()
                .remove(&(task_id.to_string(), field.to_string()));
            Ok(())
        }
    }

    fn fail_evaluator() -> MockAgent {
        MockAgent {
            response: serde_json::json!({"verdict": "fail", "score": 10, "feedback": "still bad", "criteria": []}),
        }
    }

    #[tokio::test]
    async fn ralph_history_persists_across_loop_instances() {
        let backend = Arc::new(BoardMockBackend::new());
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(pass_generator()));

        // First run: one failing iteration, history persisted on the board.
        let mut first = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 1,
            ..Default::default()
        })
        .with_history_store("task-1".into(), backend.clone());
        let verdict = first
            .run_pipeline(
                "goal",
                serde_json::json!({}),
                &pipeline,
                &planner,
                &generator,
                &EvaluatorActor::new(Arc::new(fail_evaluator())),
            )
            .await;
        let history = match verdict {
            RalphVerdict::Unrecoverable { history, .. } => history,
            other => panic!("expected Unrecoverable, got {:?}", other),
        };
        assert_eq!(history.len(), 1);

        // Restart: a fresh loop on the same task resumes from persisted history.
        let mut second = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 2,
            ..Default::default()
        })
        .with_history_store("task-1".into(), backend.clone());
        let verdict = second
            .run_pipeline(
                "goal",
                serde_json::json!({}),
                &pipeline,
                &planner,
                &generator,
                &EvaluatorActor::new(Arc::new(fail_evaluator())),
            )
            .await;
        let history = match verdict {
            RalphVerdict::Unrecoverable { history, .. } => history,
            other => panic!("expected Unrecoverable, got {:?}", other),
        };
        // 重启的这一轮跑自己的预算（2 轮），此前那 1 条只是作为上下文被恢复，
        // 不从中扣除。iteration 是各轮自己的序号，跨轮重复是正常的。
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].iteration, 1); // restored from the first run
        assert_eq!(history[1].iteration, 1); // the restart's own budget starts at 1
        assert_eq!(history[2].iteration, 2);

        // The board itself holds the full resumed history.
        let raw = backend
            .fields
            .lock()
            .unwrap()
            .get(&("task-1".to_string(), RALPH_HISTORY_FIELD.to_string()))
            .cloned()
            .expect("history field must be persisted");
        let stored: Vec<RalphIteration> = serde_json::from_str(&raw).unwrap();
        assert_eq!(stored.len(), 3);
    }

    /// 回归：历史攒满预算后重驱仍然要跑完本轮预算。曾经的起算点跟着
    /// `history.len()` 走，于是 `max_iterations` 变成目标的终身配额——攒满
    /// 之后每次重驱都在第一轮之前就"预算耗尽"，一轮都不执行，连已经能通过
    /// 的目标也被永久判死。
    #[tokio::test]
    async fn ralph_budget_is_per_run_not_a_lifetime_quota() {
        let backend = Arc::new(BoardMockBackend::new());
        // 旧账法下攒满的目标：历史饱和在 50 条。
        let saturated: Vec<RalphIteration> = (1..=50)
            .map(|i| failed_iteration(i, ResetStrategy::Modified, &format!("attempt {}", i)))
            .collect();
        backend.fields.lock().unwrap().insert(
            ("task-quota".to_string(), RALPH_HISTORY_FIELD.to_string()),
            serde_json::to_string(&saturated).unwrap(),
        );

        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(pass_generator()));

        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 2,
            ..Default::default()
        })
        .with_history_store("task-quota".into(), backend.clone());
        let verdict = ralph
            .run_pipeline(
                "goal",
                serde_json::json!({}),
                &pipeline,
                &planner,
                &generator,
                &EvaluatorActor::new(Arc::new(fail_evaluator())),
            )
            .await;

        let history = match verdict {
            RalphVerdict::Unrecoverable { history, .. } => history,
            other => panic!("expected Unrecoverable, got {:?}", other),
        };
        // 50 条饱和历史只保留最后一窗作为上下文，本轮自己的 2 轮照跑：
        // 起算点若仍跟着历史走，这里会是 5（0 轮执行）而不是 7。
        assert_eq!(ralph.history_keep(), 5);
        assert_eq!(history.len(), 7);
        assert_eq!(history[5].iteration, 1);
        assert_eq!(history[6].iteration, 2);
    }

    #[tokio::test]
    async fn ralph_without_history_store_runs_in_memory_only() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 1,
            ..Default::default()
        });
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 2,
            independent_review: false,
        });
        let verdict = ralph
            .run_pipeline(
                "goal",
                serde_json::json!({}),
                &pipeline,
                &PlannerActor::new(Arc::new(pass_planner())),
                &GeneratorActor::new(Arc::new(pass_generator())),
                &EvaluatorActor::new(Arc::new(fail_evaluator())),
            )
            .await;
        match verdict {
            RalphVerdict::Unrecoverable { history, .. } => assert_eq!(history.len(), 1),
            other => panic!("expected Unrecoverable, got {:?}", other),
        }
    }
}
