//! Loop 1 — Ralph Loop（Squad 外层质量控制循环）。
//! - 停止条件：PGE Pass、判定"不可修复"、停滞检测、或迭代预算耗尽。
//! - 全局重置策略（非局部修补）：Identical / Modified / Escalated。
//! - 迭代预算（max_iterations）是**一次执行**的有界止损：无人值守场景没有
//!   操作者盯流调 prompt，不收敛的链必须在预算内终止，不能"视为无限"地烧。
//!   预算不跨执行累计——持久化的历史是给下一次执行看的反馈与停滞证据，
//!   不是被消耗掉的额度。

use crate::actors::{EvaluatorActor, GeneratorActor, PlannerActor};
use crate::squad::classify::{classify, declared_in};
use crate::squad::pge::pipeline::{PgePipeline, PlanScope};
use crate::squad::pge::roundtable::{PgeRoundtable, PgeRoundtableResult};
use crate::squad::pge::stall::{made_progress, ProgressSignals};
use crate::squad::pge::types::{EvaluationResult, PlannerOutput, RoundOutcome, Verdict};
use cog_core::{Task, TaskType};
use std::sync::Arc;

/// 失败分类的稳定半段：角色、分类学、答案的形状。
///
/// 它与被分类的失败无关，所以它必须逐字节相同地待在整个请求的最前面——
/// 系统消息就是这个位置。此前它和 `Evaluation result:` / `History` 同处一条
/// 用户消息，且**答案 schema 排在两者之后**：请求的第一个字节就已经和上一次
/// 不同，于是这段常量每次失败都重买一遍，运行得越久买得越多。剩下的用户消息
/// 只装这一次的失败本身。
const FAILURE_CLASSIFIER_SYSTEM: &str = "\
You are a precise failure classifier. Respond only with valid JSON.\n\
You are a failure-analysis expert for an AI agent system. \
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
Respond with **only** a JSON object matching this schema:\n\
{\"failure_type\":\"...\",\"root_cause\":\"...\",\"recommended_strategy\":\"...\",\"suggested_modifications\":\"...\"}";

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
    /// Progress readings for this iteration. Persisted so the stall verdict
    /// survives a restart: an iteration's outcome is only comparable against
    /// its predecessor, and the predecessor may live in another process.
    /// `None` only on entries archived before the field existed — those are
    /// skipped as unobserved, never read as zeroes. A live iteration always
    /// records a reading: an evaluation that moved nowhere is not an
    /// undecidable case, it is the plain statement that the iteration bought
    /// nothing, which is exactly what the stall verdict is looking for.
    #[serde(default)]
    pub progress: Option<ProgressSignals>,
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
    /// 都算停滞——归一化反馈逐字相同（同一失败原样重放），或评估结论没有
    /// 抬升（分数与标准项都平，即换着说法重复同一个失败）。0 = 关闭
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

/// Event type under which a finished iteration is appended to the cold
/// archive. The board holds the tail because that is all a restart consumes;
/// this channel is append-only and keeps the whole sequence, which is what
/// post-hoc root-cause analysis of a non-converging run needs.
pub const RALPH_ITERATION_EVENT: &str = "ralph_iteration";

/// Read back a task's archived iterations, oldest first. `limit` bounds one
/// call because the event store has no streaming read, and a re-driven task
/// keeps appending to the same sequence rather than replacing it — so a caller
/// that expects more pages by raising the limit.
pub async fn archived_iterations(
    backend: &dyn cog_core::StateBackend,
    task_id: &str,
    limit: usize,
) -> cog_core::SFResult<Vec<RalphIteration>> {
    backend
        .get_events(task_id, 0, limit)
        .await?
        .into_iter()
        .filter(|event| event.event_type == RALPH_ITERATION_EVENT)
        .map(|event| {
            serde_json::from_value::<RalphIteration>(event.payload)
                .map_err(cog_core::SFError::Serialization)
        })
        .collect()
}

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

    /// The span of `history` a failure analysis is shown, and the span it is
    /// not. Same bound as [`Self::history_keep`], for the same stated reason:
    /// the earlier iterations are dead weight in a prompt exactly as they are
    /// on the board. Returned as two slices rather than a length so the caller
    /// measures what it puts in the prompt against what it left out, instead of
    /// asserting the difference.
    fn failure_prompt_history<'a>(
        &self,
        history: &'a [RalphIteration],
    ) -> (&'a [RalphIteration], &'a [RalphIteration]) {
        let keep = self.history_keep();
        if history.len() > keep {
            let (older, tail) = history.split_at(history.len() - keep);
            (tail, older)
        } else {
            (history, &history[..0])
        }
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

    /// Append the iteration to the cold archive. The board keeps only the tail,
    /// so without this the earlier iterations of a non-converging run exist
    /// nowhere and the whole-sequence analysis that root-causes the ratchet has
    /// nothing to read. Best-effort: a cold archive that is down degrades
    /// forensics, it must never fail the loop.
    async fn archive_iteration(&self, iteration: &RalphIteration) {
        let (Some(task_id), Some(backend)) = (&self.history_task_id, &self.state_backend) else {
            return;
        };
        let payload = match serde_json::to_value(iteration) {
            Ok(payload) => payload,
            Err(e) => {
                tracing::warn!(task_id, "Ralph iteration archive serialize failed: {}", e);
                return;
            }
        };
        let event = cog_core::Event {
            // Assigned by the backend when it appends.
            offset: 0,
            task_id: task_id.clone(),
            event_type: RALPH_ITERATION_EVENT.to_string(),
            payload,
            timestamp: chrono::Utc::now(),
        };
        if let Err(e) = backend.append_event(task_id, &event).await {
            tracing::warn!(task_id, "Ralph iteration archive append failed: {}", e);
        }
    }

    /// Record one finished iteration: keep it in memory, append it to the cold
    /// archive in full, and write the board tail. The three are one step
    /// because an iteration that reaches only some of them is exactly the loss
    /// this separation is meant to prevent.
    async fn record_iteration(&mut self, iteration: RalphIteration) {
        self.history.push(iteration);
        if let Some(latest) = self.history.last() {
            self.archive_iteration(latest).await;
        }
        self.persist_history().await;
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

    /// 尾部这一窗内评估结论没有抬升过——第一个读数只立基线，之后每个读数都
    /// 必须严格超过此前的最好成绩。判据与 pipeline/roundtable 的停滞检测共用
    /// 同一个 [`made_progress`]，全链路对"什么算进展"只有一套口径。窗口内缺失
    /// 的读数（本次改动前归档的记录没有这个字段）按"未观测"处理，跳过而不是
    /// 当成零分：少于两个读数就无从比较，一律返回 false，把判断交回给逐字判据。
    /// 缺了这道下限，一个只是没被观测过的窗口会被 `all` 的空真判成"没进展"。
    fn tail_bought_no_progress(&self, tail: &[RalphIteration]) -> bool {
        let readings: Vec<ProgressSignals> =
            tail.iter().filter_map(|it| it.progress.clone()).collect();
        let Some((baseline, rest)) = readings.split_first() else {
            return false;
        };
        if rest.is_empty() {
            return false;
        }
        let mut best = baseline.clone();
        rest.iter().all(|cur| {
            if made_progress(&best, cur) {
                best = cur.clone();
                false
            } else {
                true
            }
        })
    }

    /// 不可恢复终止的统一形态（与停滞、预算同构）：Warn 日志 + 指标 +
    /// Unrecoverable 判定。分类取自 reason 前缀而不是另立判据——前缀本来就是
    /// 全链路共用的 wire 标记（squad executor 也按它决定不再升级策略），
    /// 另立一套只会在两处之间分叉。原因落进指标面之前，"分解为什么停"只存在
    /// 于日志里，告警面看不见。
    fn unrecoverable_verdict(&self, reason: String) -> RalphVerdict {
        let class = classify(&reason);
        tracing::warn!(
            reason = %reason,
            class,
            "Ralph Loop terminated without a recoverable strategy"
        );
        crate::observable::global_observable().record_ralph_termination(class);
        RalphVerdict::Unrecoverable {
            reason,
            iterations: self.history.len() as u32,
            history: self.history.clone(),
        }
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

    /// 以分解模式运行 Ralph Loop：只跑 Planner，交付物是原子任务列表。
    ///
    /// 与另外两条路径共用同一套预算、停滞判据与历史复用，区别只在"这一轮的
    /// 产物怎么判"：判据是纯结构的（[`crate::squad::plan::judge_plan`]），所以
    /// 这里不调评估器——没有产出可判，评估器能给的唯一答案就是"这里什么都没有"。
    /// 也不调失败语义分析：分解只有两种失败（没拿到任务列表，或计划侧的终止性
    /// 环境失败），两者的重试输入都是那段原因本身，一次 LLM 判读买不到新信息。
    pub async fn run_plan(
        &mut self,
        goal: &str,
        mut context: serde_json::Value,
        planner: &PlannerActor,
    ) -> RalphVerdict {
        self.load_history().await;

        // 与 Pipeline/Roundtable 同构：预算属于本次执行，历史只提供反馈与停滞证据。
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
                format!("ralph-plan-{}", uuid::Uuid::new_v4()),
                TaskType::Custom("ralph_plan_goal".into()),
                input,
            );

            // 上一轮的失败原因直接喂回 Planner：重试的全部价值就在这段反馈里，
            // 丢掉它等于把同一轮原样重放一次。
            let previous_feedback = self.history.last().map(|h| h.feedback.clone());
            let plan = planner
                .plan(
                    &task,
                    iteration,
                    previous_feedback.as_deref(),
                    None,
                    None,
                    None,
                )
                .await;

            // 计划侧的终止性失败与另外两条路径同一条闸：prompt 没到上游，重试
            // 必然同样失败。
            if let Some(reason) = plan.terminal_env_failure_reason() {
                tracing::warn!(
                    iteration,
                    "Ralph plan loop: planner reported terminal environment failure; stopping"
                );
                self.record_iteration(RalphIteration {
                    iteration,
                    reset_strategy: ResetStrategy::Identical,
                    pge_passed: false,
                    feedback: reason.clone(),
                    snapshot: serde_json::json!({ "plan": &plan, "reason": &reason }),
                    progress: None,
                })
                .await;
                return self.unrecoverable_verdict(reason);
            }

            let evaluation = crate::squad::plan::judge_plan(&plan);
            let passed = matches!(evaluation.verdict, Verdict::Pass);
            let feedback = evaluation.feedback.clone();
            let progress = Some(ProgressSignals::from_evaluation(&evaluation));

            let run = crate::squad::plan::PlanRunResult { plan, evaluation };
            let snapshot = match serde_json::to_value(&run) {
                Ok(snapshot) => snapshot,
                Err(e) => {
                    // 交付物在手上却序列化不出来，是这一环的内部缺陷：当成
                    // "没有产出"会把它读成一次普通的空交付物，然后照着重试。
                    return self.unrecoverable_verdict(format!(
                        "Ralph plan loop could not serialize its own deliverable: {e}"
                    ));
                }
            };

            self.record_iteration(RalphIteration {
                iteration,
                reset_strategy: ResetStrategy::Identical,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot: snapshot.clone(),
                progress,
            })
            .await;

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: snapshot,
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            if self.is_stagnated() {
                return self.stagnated_verdict();
            }
        }

        self.budget_exhausted_verdict()
    }

    /// 以 Pipeline 模式运行 Ralph Loop。
    pub async fn run_pipeline(
        &mut self,
        goal: &str,
        context: serde_json::Value,
        pipeline: &PgePipeline,
        planner: &PlannerActor,
        generator: &GeneratorActor,
        evaluator: &EvaluatorActor,
    ) -> RalphVerdict {
        self.run_pipeline_scoped(
            goal,
            context,
            pipeline,
            PlanScope::Planned(planner),
            generator,
            evaluator,
        )
        .await
    }

    /// 以 Pipeline 模式运行 Ralph Loop，计划由请求自己声明的范围充当——没有
    /// planner 阶段。
    ///
    /// 外层循环与 [`Self::run_pipeline`] 完全同构：预算、停滞判据、失败分析与
    /// 全局重置都照旧。省掉的只是一个阶段，不是这套循环本身——声明式范围不需要
    /// 谁来"重新计划"，所以重置只是再生成一次。
    pub async fn run_declared_scope(
        &mut self,
        goal: &str,
        context: serde_json::Value,
        pipeline: &PgePipeline,
        declared_plan: &PlannerOutput,
        generator: &GeneratorActor,
        evaluator: &EvaluatorActor,
    ) -> RalphVerdict {
        self.run_pipeline_scoped(
            goal,
            context,
            pipeline,
            PlanScope::Declared(declared_plan),
            generator,
            evaluator,
        )
        .await
    }

    async fn run_pipeline_scoped(
        &mut self,
        goal: &str,
        mut context: serde_json::Value,
        pipeline: &PgePipeline,
        scope: PlanScope<'_>,
        generator: &GeneratorActor,
        evaluator: &EvaluatorActor,
    ) -> RalphVerdict {
        self.load_history().await;

        // 请求就是目标加上它被提出时的上下文，整个外层循环只有这一份。本轮
        // 自己的字段（轮次号、上一轮的反馈、重置策略）写在 `context` 里，随
        // `context` 走——那才是"本次尝试"该待的地方。它们曾经也被折进任务
        // 的 input，因为 input 是从**已经写过的** context 克隆出来的：于是同
        // 一个请求的两轮在请求内部就不一样了，上游缓存连那截从不变的部分也
        // 存不住。请求与尝试分开，两半才各自成立。
        let request = context.clone();

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

            let mut input = request.clone();
            input["goal"] = serde_json::json!(goal);
            let task = Task::new(
                format!("ralph-pipeline-{}", uuid::Uuid::new_v4()),
                TaskType::Custom("ralph_pipeline_goal".into()),
                input,
            );
            let pge_result = match scope {
                PlanScope::Planned(planner) => {
                    pipeline
                        .execute_task(&task, context.clone(), planner, generator, evaluator)
                        .await
                }
                PlanScope::Declared(declared) => {
                    pipeline
                        .execute_declared_scope(
                            &task,
                            context.clone(),
                            declared,
                            generator,
                            evaluator,
                        )
                        .await
                }
            };
            let passed = matches!(pge_result.final_evaluation.verdict, Verdict::Pass);
            let feedback = pge_result.final_evaluation.feedback.clone();

            let analysis = if passed {
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            } else if let Some(reason) = pge_result.terminal_reason.clone() {
                // Deterministic environment/protocol failure: no reset
                // strategy can fix it, stop before another paid iteration.
                // The cause comes from the pipeline, which is the side that
                // made the decision and knows which role failed; re-deriving
                // it here from the generation alone puts a planner's spent
                // budget on a generator that never ran.
                FailureAnalysis::Unrecoverable(reason)
            } else {
                self.analyze_failure(&pge_result.final_evaluation, &self.history)
                    .await
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let progress = Some(ProgressSignals::from_evaluation(
                &pge_result.final_evaluation,
            ));
            let snapshot = serde_json::json!({
                "plan": pge_result.final_plan,
                "generation": pge_result.final_generation,
                "evaluation": pge_result.final_evaluation,
            });

            self.record_iteration(RalphIteration {
                iteration,
                reset_strategy,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot,
                progress,
            })
            .await;

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
                return self.unrecoverable_verdict(reason.clone());
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

        // 与 Pipeline 同构：请求整轮只有一份，轮次自己的字段只落在 context 里。
        let request = context.clone();

        // 与 Pipeline 同构：预算属于本次执行，历史只提供反馈与停滞证据。
        for iteration in 1..=self.config.max_iterations {
            crate::observable::global_observable().record_round();
            if iteration > 1 {
                context["ralph_iteration"] = serde_json::json!(iteration);
                if let Some(prev) = self.history.last() {
                    context["ralph_feedback"] = serde_json::json!(&prev.feedback);
                }
            }

            let mut input = request.clone();
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
            } else if let Some(reason) = rt_result.terminal_reason.clone() {
                // Same as the pipeline branch: the debate already composed the
                // cause and named the role that failed.
                FailureAnalysis::Unrecoverable(reason)
            } else {
                Self::analyze_roundtable_failure(&rt_result, &self.history)
            };

            let reset_strategy = match &analysis {
                FailureAnalysis::Recoverable(s) => *s,
                FailureAnalysis::Unrecoverable(_) => ResetStrategy::Identical,
            };

            let progress = Some(ProgressSignals::from_outcome(&rt_result.final_outcome));
            let snapshot = serde_json::json!({ "roundtable": rt_result });

            self.record_iteration(RalphIteration {
                iteration,
                reset_strategy,
                pge_passed: passed,
                feedback: feedback.clone(),
                snapshot: snapshot.clone(),
                progress,
            })
            .await;

            if passed {
                let total_iterations = self.history.len() as u32;
                return RalphVerdict::Passed {
                    result: serde_json::json!(snapshot),
                    iterations: total_iterations,
                    history: self.history.clone(),
                };
            }

            if let FailureAnalysis::Unrecoverable(reason) = &analysis {
                return self.unrecoverable_verdict(reason.clone());
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
    /// 1. A failure whose cause is already declared → terminal.
    /// 2. Repeated identical feedback → loop detection (pure control flow).
    ///
    /// When an LLM provider is available, `analyze_failure_with_llm` performs
    /// semantic classification (contradiction, skill gap, etc.) and returns
    /// a targeted reset strategy.
    async fn analyze_failure(
        &self,
        evaluation: &EvaluationResult,
        history: &[RalphIteration],
    ) -> FailureAnalysis {
        let observable = crate::observable::global_observable();

        // A declared cause needs no reading. Both classes say retrying cannot
        // clear this, and the text that carries them is produced next to the
        // code that knows it, so the answer is already in hand before any model
        // is asked. The class table is the single reader of that text: a bare
        // prefix match misses the markers when something wraps them — the
        // defect prepended ahead of the original feedback is one such wrapper —
        // and a miss here is not a warning, it is a paid classification call
        // plus an extra iteration for a run that had already been told to stop.
        if let Some(class) = declared_in(&evaluation.feedback) {
            observable.record_failure_route(class);
            return FailureAnalysis::Unrecoverable(evaluation.feedback.clone());
        }

        // 检测重复相同失败（循环卡住）—— 纯控制流，无需语义理解
        let recent_same_feedback = history
            .iter()
            .rev()
            .take(2)
            .all(|h| h.feedback == evaluation.feedback);
        if recent_same_feedback && history.len() >= 2 {
            observable.record_failure_route(crate::observable::FAILURE_ROUTE_REPEATED);
            return FailureAnalysis::Unrecoverable(format!(
                "Same failure repeated {} times: {}",
                history.len() + 1,
                evaluation.feedback
            ));
        }

        // If LLM provider is available, perform semantic failure analysis.
        // The two ways of not paying here are counted apart: nothing wired to
        // ask is a deployment's own choice, while a classifier that was wired
        // and errored is an upstream to look at, and one shared cell would add
        // a configuration to a fault.
        let Some(llm) = self.llm_provider.as_ref() else {
            observable.record_failure_route(crate::observable::FAILURE_ROUTE_LLM_ABSENT);
            return FailureAnalysis::Recoverable(ResetStrategy::Identical);
        };
        match self
            .analyze_failure_with_llm(evaluation, history, llm)
            .await
        {
            Ok(analysis) => {
                observable.record_failure_route(crate::observable::FAILURE_ROUTE_LLM_CLASSIFIED);
                analysis
            }
            Err(e) => {
                tracing::warn!(
                    "LLM failure analysis failed, falling back to default: {}",
                    e
                );
                observable.record_failure_route(crate::observable::FAILURE_ROUTE_LLM_FAILED);
                FailureAnalysis::Recoverable(ResetStrategy::Identical)
            }
        }
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
        // The classifier is shown the same tail the other two carriers of this
        // history are held to. Three places read this run's history — the state
        // board restore, the archive write and this prompt — and the first two
        // both trim to `history_keep()`, whose own reason is that everything
        // older buys nothing. Only the prompt grew with the run, so a long
        // non-converging run paid for the same dead weight on every failure,
        // and the later the run got the more it paid. The bound is applied to
        // what goes into the prompt, never to the slice the caller passed:
        // `analyze_failure` counts the whole history for its "same failure
        // repeated N times" verdict, and a trimmed slice would undercount it.
        let (recent, dropped) = self.failure_prompt_history(history);
        let history_json = serde_json::to_string_pretty(recent).unwrap_or_default();
        let dropped_bytes = serde_json::to_string_pretty(dropped)
            .map(|json| json.len())
            .unwrap_or(0);
        crate::observable::global_observable()
            .record_failure_history_bytes(history_json.len(), dropped_bytes);

        // The varying half: this failure, and the tail of what the run did before
        // it. The taxonomy and the answer schema are not here — they sit in
        // `FAILURE_CLASSIFIER_SYSTEM`, ahead of everything that changes.
        let prompt = format!(
            "Evaluation result:\n{evaluation:?}\n\
\n\
History of previous attempts:\n{history_json}"
        );

        let messages = vec![
            cog_core::Message::System {
                content: FAILURE_CLASSIFIER_SYSTEM.into(),
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

        // 答复拿到了、只是解不出来：内容类失败，不是上游故障。
        let semantic: SemanticFailureAnalysis =
            serde_json::from_str(json_str).map_err(cog_core::SFError::Serialization)?;

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
        // 没有终判的一轮只有它自己说出的停止原因，没有 verdict 可读。从产物
        // 反推一个 verdict 会把这个原因丢掉，还会把账算到没跑过的角色头上。
        let evaluation = match &result.final_outcome {
            RoundOutcome::Judged { evaluation } => evaluation,
            RoundOutcome::Stopped { cause, .. } => {
                return FailureAnalysis::Unrecoverable(cause.reason());
            }
        };
        let feedback = &evaluation.feedback;

        // 若最终 verdict 为 Fail，视为无有效输出（verdict 是核心信号，score 仅作参考）。
        // reason 取真实 feedback，落盘与指标才带得动定位信息；feedback 为空时才回退常量。
        if matches!(evaluation.verdict, Verdict::Fail) {
            return FailureAnalysis::Unrecoverable(if feedback.trim().is_empty() {
                "Roundtable produced no viable output".into()
            } else {
                feedback.clone()
            });
        }

        // 检测 Roundtable 是否卡住（连续相同 verdict）
        let current_verdict_str = match evaluation.verdict {
            Verdict::Pass => "Pass",
            Verdict::Fail => "Fail",
            Verdict::Partial => "Partial",
            Verdict::NeedsReview => "NeedsReview",
            Verdict::Retry => "Retry",
        };
        let recent_same = history.iter().rev().take(2).all(|h| {
            h.snapshot
                .get("roundtable")
                .and_then(|r| r.get("final_outcome"))
                .and_then(|o| o.get("evaluation"))
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
    use crate::squad::pge::types::{StopCause, StoppedProduct};
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

    /// 分解拿到任务列表就是通过：这条路径上没有生成器也没有评估器，判据是结构性的。
    #[tokio::test]
    async fn ralph_plan_passes_when_the_planner_returns_tasks() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 10,
            ..Default::default()
        });
        let planner = PlannerActor::new(Arc::new(pass_planner()));

        let verdict = ralph
            .run_plan("decompose the goal", serde_json::json!({}), &planner)
            .await;

        match verdict {
            RalphVerdict::Passed {
                iterations, result, ..
            } => {
                assert_eq!(iterations, 1);
                assert_eq!(
                    result["plan"]["sub_tasks"].as_array().map(Vec::len),
                    Some(1),
                    "the deliverable must be the task list itself: {result}"
                );
            }
            other => panic!("Expected Passed, got {:?}", other),
        }
    }

    /// 空分解不是终止性失败：没有任何观测到的证据排除下一轮成功，第一轮就收摊
    /// 等于按一个从没看见过的事实结账。这里量的是"它到底有没有接着试"。
    #[tokio::test]
    async fn an_empty_decomposition_retries_instead_of_terminating() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 3,
            // 关掉停滞检测，这条用例只量"空交付物会不会被当成终止性失败"。
            stagnation_window: 0,
        });
        let planner = PlannerActor::new(Arc::new(MockAgent {
            response: serde_json::json!({"summary": "n/a", "plan": {}, "sub_tasks": []}),
        }));

        let verdict = ralph
            .run_plan("decompose the goal", serde_json::json!({}), &planner)
            .await;

        match verdict {
            RalphVerdict::Unrecoverable {
                reason,
                iterations,
                history,
            } => {
                assert_eq!(
                    iterations, 3,
                    "an empty deliverable must be retried, not terminated on the first attempt"
                );
                assert!(
                    !cog_core::contract::outcome::is_deterministic_failure(&reason),
                    "{reason}"
                );
                assert!(
                    history.iter().all(|it| it
                        .feedback
                        .contains(crate::squad::plan::EMPTY_DECOMPOSITION_PREFIX)),
                    "every retry must carry the cause it is retrying on: {history:?}"
                );
            }
            other => panic!("Expected Unrecoverable budget exhaustion, got {:?}", other),
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
                // roundtable 第 2 个 internal iteration 确认共识：连续两轮 Pass 且每轮
                // score 92 都高过 0.3 的地板（30），Ralph 外层因此一轮就结束。
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
        // roundtable 只给一轮，凑不出「连续两轮」这个前提，共识无从谈起——地板
        // （1.0 要 100 分，mock 返回 92）只是又拦一道。Ralph 两轮后耗尽迭代预算。
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

    // -----------------------------------------------------------------

    /// 失败分析的提示词只带归档同一条界内的尾部，不带整段历史。
    ///
    /// 这条界决定了分类器的提示词会不会随一次已经注定不收敛的运行一直长；
    /// 没有它，每一次失败都要重新为「循环自己已经认定是死重」的那些迭代
    /// 付费，而且越到后面付得越多。两端都断言：只裁到空的界会用零证据
    /// 回答同一个问题。
    #[test]
    fn the_failure_prompt_carries_only_the_tail_the_archive_keeps() {
        let ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        let history: Vec<RalphIteration> = (1..=10)
            .map(|i| failed_iteration(i, ResetStrategy::Identical, &format!("failure {i}")))
            .collect();

        let (shown, left_out) = ralph.failure_prompt_history(&history);

        assert_eq!(shown.len(), 3, "一个窗口，不是十轮");
        assert_eq!(shown[0].iteration, 8, "留下的是尾部");
        assert_eq!(shown[2].iteration, 10);
        assert_eq!(left_out.len(), 7);
        assert_eq!(left_out[0].iteration, 1);
    }

    /// 比窗口短的历史一条不裁：这条界是上限，不是配额。
    #[test]
    fn a_run_shorter_than_the_window_is_shown_in_full() {
        let ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 5,
        });
        let history: Vec<RalphIteration> = (1..=2)
            .map(|i| failed_iteration(i, ResetStrategy::Identical, &format!("failure {i}")))
            .collect();

        let (shown, left_out) = ralph.failure_prompt_history(&history);

        assert_eq!(shown.len(), 2);
        assert!(left_out.is_empty());
    }

    /// 裁下来的量落成读数：`dropped` 是这条界真的省掉的字节，`fed` 是真正
    /// 进了提示词的那部分。只发布 `fed` 说不出省了多少，而一个恒为 0 的
    /// `dropped` 说明这条界从没生效过——那不是坏了，是没被用到。
    #[tokio::test]
    async fn the_failure_prompt_records_what_the_bound_left_out() {
        let llm = Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"LogicError","root_cause":"x","recommended_strategy":"Modified","suggested_modifications":"y"}"#,
        ));
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 2,
        })
        .with_llm_provider(llm);
        for i in 1..=5 {
            // 反馈各不相同：逐字相同的反馈会被纯控制流提前判成不可恢复，
            // 那条路上根本不建提示词，也就没有这条读数可言。
            ralph.history.push(failed_iteration(
                i,
                ResetStrategy::Identical,
                &format!("failure {i}"),
            ));
        }
        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "the newest failure".into(),
            criteria: vec![],
            details: None,
        };

        let before = prompt_bytes().await;
        let history = ralph.history.clone();
        let _ = ralph.analyze_failure(&eval, &history).await;
        let after = prompt_bytes().await;

        assert!(
            after.1 > before.1,
            "被裁掉的字节要记成读数：before={:?} after={:?}",
            before,
            after
        );
        assert!(after.0 > before.0, "喂进去的那段也要记");
    }

    /// 分类学的稳定半段逐字节待在请求最前面，且不许有第二个持有者。
    ///
    /// 判据是「每轮都要重发的内容必须逐字节相同地待在请求最前面」。这段文本
    /// （角色、六个分类、答案 schema）与这一次的失败无关，此前它和
    /// `Evaluation result:` / `History` 同处一条用户消息、schema 还排在两者
    /// **之后**，于是一次运行里每失败一次就重买一遍，历史越长它越靠后。
    ///
    /// 两次失败读**同一份**系统消息，才分得开「常量在最前」与「常量这次恰好
    /// 写对了」；只看一次调用的话，一条把本次失败也塞进系统消息的实现在这条
    /// 断言下同样绿。
    #[tokio::test]
    async fn the_classifier_keeps_its_stable_half_ahead_of_what_changes() {
        let double = Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"LogicError","root_cause":"x","recommended_strategy":"Modified","suggested_modifications":"y"}"#,
        ));
        let llm: Arc<dyn cog_core::LlmClient> = double.clone();
        let ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });

        for failure in ["the first failure", "the second failure"] {
            let eval = EvaluationResult {
                verdict: Verdict::Fail,
                score: Some(0),
                feedback: failure.into(),
                criteria: vec![],
                details: None,
            };
            // 历史也各不相同：两份请求的易变半段必须真的不同，否则下面
            // 「稳定半段相同」这条比较证明不了任何事。
            let history = vec![failed_iteration(1, ResetStrategy::Identical, failure)];
            ralph
                .analyze_failure_with_llm(&eval, &history, &llm)
                .await
                .expect("mock 的答复是一个合法的分类");
        }

        let calls = double.calls();
        assert_eq!(calls.len(), 2, "两次失败要读两次");

        for (i, msgs) in calls.iter().enumerate() {
            assert_eq!(
                msgs.len(),
                2,
                "分类请求只该有系统消息与用户消息两条（第 {i} 次）"
            );
            assert_eq!(
                msgs[0].role(),
                "system",
                "稳定半段必须待在整个请求的最前面（第 {i} 次）"
            );
            assert_eq!(msgs[1].role(), "user", "易变半段在用户消息里（第 {i} 次）");
        }

        let stable = calls[0][0].content();
        assert_eq!(
            stable,
            calls[1][0].content(),
            "两次失败的系统消息不同，说明它带了本次失败，缓存不了"
        );
        for word in [
            "Contradiction",
            "SkillGap",
            "AmbiguousRequirement",
            "ResourceError",
            "LogicError",
            "Unrecoverable",
            "failure_type",
            "recommended_strategy",
        ] {
            assert!(
                stable.contains(word),
                "系统消息漏了 {word:?}，模型只能猜；系统消息是：{stable}"
            );
        }

        for (i, msgs) in calls.iter().enumerate() {
            let varying = msgs[1].content();
            for word in [
                "Contradiction",
                "SkillGap",
                "AmbiguousRequirement",
                "ResourceError",
                "LogicError",
                "Unrecoverable",
                "matching this schema",
            ] {
                assert!(
                    !varying.contains(word),
                    "稳定半段在用户消息里还有第二个持有者（{word:?}，第 {i} 次）：{varying}"
                );
            }
            assert!(
                varying.contains("Evaluation result:"),
                "用户消息要把这次的失败本身带进去（第 {i} 次）"
            );
        }

        assert!(calls[0][1].content().contains("the first failure"));
        assert!(calls[1][1].content().contains("the second failure"));
        assert_ne!(
            calls[0][1].content(),
            calls[1][1].content(),
            "两次的易变半段一样，这条测试什么也没证明"
        );
    }

    /// 失败分析提示词的两端读数（fed, dropped），取自全局观测面。
    async fn prompt_bytes() -> (f64, f64) {
        use cog_core::observability::Observable;
        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        let cell = |part: &str| {
            metrics
                .iter()
                .find(|m| {
                    m.name == "ralph_failure_prompt_bytes_total"
                        && m.labels.get("part").map(String::as_str) == Some(part)
                })
                .map(|m| m.value)
                .unwrap_or(0.0)
        };
        (
            cell(crate::observable::RALPH_HISTORY_FED),
            cell(crate::observable::RALPH_HISTORY_DROPPED),
        )
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
        progress: ProgressSignals,
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

    /// 读数只有评估器给出的分数与标准项，产物大小不在其中——这正是本次要
    /// 消灭的代理量：把产物堆大并不能换来窗口内的"买到进展"。
    fn readings(score: u32) -> ProgressSignals {
        ProgressSignals {
            score: Some(score),
            criteria: Vec::new(),
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
                readings(30),
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
            readings(30),
        ));
        assert!(!ralph.is_stagnated());
    }

    /// 既无分又无标准项不是"不可判定"：读数全零就是确定的"这一轮什么都没
    /// 买到"，同样要判停滞。若把它当作未观测而跳过，这类链条（不产出也不被
    /// 评分）会绕过停滞判据、每轮烧满预算——正是本次修复要消灭的形态。
    #[test]
    fn zero_readings_are_no_progress_not_undecidable() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 2,
        });
        for i in 1..=2 {
            ralph.history.push(failed_iteration_with_readings(
                i,
                "nothing produced, nothing scored",
                ProgressSignals {
                    score: Some(0),
                    criteria: Vec::new(),
                },
            ));
        }
        assert!(ralph.is_stagnated());
    }

    /// 窗口里评估分数真的抬过就不算停滞：读数语义化不等于把判据变严，
    /// 只是把"进展"的锚点从代理量换成评估器自己的判断。
    #[test]
    fn a_higher_evaluation_score_in_the_window_is_progress() {
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 50,
            stagnation_window: 3,
        });
        for (i, (score, feedback)) in [
            (10u32, "missing tests"),
            (10, "return type wrong"),
            (85, "one case still red"),
        ]
        .iter()
        .enumerate()
        {
            ralph.history.push(failed_iteration_with_readings(
                i as u32 + 1,
                feedback,
                readings(*score),
            ));
        }
        assert!(!ralph.is_stagnated());
    }

    fn roundtable_result(verdict: Verdict, feedback: &str) -> PgeRoundtableResult {
        use crate::squad::pge::types::{EvaluationResult, GeneratorOutput, PlannerOutput};

        PgeRoundtableResult {
            iterations: 1,
            consensus_reached: false,
            final_plan: PlannerOutput {
                summary: String::new(),
                plan: serde_json::json!({}),
                sub_tasks: Vec::new(),
                acceptance_criteria: Vec::new(),
                targets: Vec::new(),
            },
            final_generation: GeneratorOutput {
                content: serde_json::json!({}),
                artifacts: Vec::new(),
            },
            final_outcome: RoundOutcome::Judged {
                evaluation: EvaluationResult {
                    verdict,
                    feedback: feedback.to_string(),
                    score: None,
                    criteria: Vec::new(),
                    details: None,
                },
            },
            history: Vec::new(),
            context_board: None,
            terminal_reason: None,
        }
    }

    /// A debate that ended without anyone judging anything, carrying the cause
    /// the round itself composed.
    fn stopped_roundtable_result(reason: &str) -> PgeRoundtableResult {
        use crate::squad::pge::types::{GeneratorOutput, PlannerOutput};

        let mut result = roundtable_result(Verdict::Fail, "");
        result.final_plan = PlannerOutput {
            summary: String::new(),
            plan: serde_json::json!({}),
            sub_tasks: Vec::new(),
            acceptance_criteria: Vec::new(),
            targets: Vec::new(),
        };
        result.final_generation = GeneratorOutput::none();
        result.final_outcome = RoundOutcome::Stopped {
            cause: StopCause::Deterministic {
                reason: reason.to_string(),
            },
            product: StoppedProduct::None,
        };
        result.terminal_reason = Some(reason.to_string());
        result
    }

    #[test]
    fn a_degenerate_roundtable_keeps_its_class_instead_of_falling_back() {
        use crate::squad::pge::stall::degenerate_loop_feedback;
        use cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX;

        let degenerate =
            degenerate_loop_feedback("3 consecutive iterations bought no progress".into());
        let result = stopped_roundtable_result(&degenerate);
        match RalphLoop::analyze_roundtable_failure(&result, &[]) {
            FailureAnalysis::Unrecoverable(reason) => {
                assert!(reason.starts_with(DEGENERATE_LOOP_PREFIX));
                assert_eq!(classify(&reason), "degenerate_loop");
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    /// A round nobody judged reports the cause the round composed, not the
    /// verdict of a judge that never ran.
    #[test]
    fn a_round_nobody_judged_reports_its_own_cause() {
        let mut empty_envelope = roundtable_result(Verdict::Pass, "looks good");
        empty_envelope.final_outcome = RoundOutcome::Stopped {
            cause: StopCause::EmptyEnvelope,
            product: StoppedProduct::None,
        };
        match RalphLoop::analyze_roundtable_failure(&empty_envelope, &[]) {
            FailureAnalysis::Unrecoverable(reason) => assert!(
                reason.contains("no content and no artifacts"),
                "the cause must be the round's own, not the verdict that was never given: \
                 {reason}"
            ),
            other => panic!("expected Unrecoverable, got {other:?}"),
        }

        // A round that produced something whose judge is what failed names the
        // judge's own cause, and the reason survives the boundary intact.
        let mut unjudged_product = roundtable_result(Verdict::Pass, "looks good");
        unjudged_product.final_outcome = RoundOutcome::Stopped {
            cause: StopCause::Deterministic {
                reason: format!(
                    "{}: evaluator prompt failed: HTTP 503",
                    cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX
                ),
            },
            product: StoppedProduct::Unjudged,
        };
        match RalphLoop::analyze_roundtable_failure(&unjudged_product, &[]) {
            FailureAnalysis::Unrecoverable(reason) => {
                assert!(reason.contains("evaluator prompt failed: HTTP 503"));
                assert_eq!(classify(&reason), "terminal_env_failure");
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    #[test]
    fn a_roundtable_failure_carries_its_real_reason_when_it_has_one() {
        let result = roundtable_result(
            Verdict::Fail,
            "change artifact 'changes.diff' is not an appliable unified diff: jwt.rs:55: \
             unexpected line outside any hunk",
        );
        match RalphLoop::analyze_roundtable_failure(&result, &[]) {
            FailureAnalysis::Unrecoverable(reason) => {
                assert!(
                    reason.contains("unexpected line outside any hunk"),
                    "the reason must survive the ralph boundary, got: {reason}"
                );
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    #[test]
    fn a_silent_roundtable_failure_falls_back_to_the_constant_reason() {
        let result = roundtable_result(Verdict::Fail, "   ");
        match RalphLoop::analyze_roundtable_failure(&result, &[]) {
            FailureAnalysis::Unrecoverable(reason) => {
                assert_eq!(reason, "Roundtable produced no viable output");
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unrecoverable_stop_is_counted_on_the_metric_plane() {
        use cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX;
        use cog_core::Observable;

        let ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 5,
            stagnation_window: 2,
        });
        let verdict = ralph.unrecoverable_verdict(format!(
            "{TERMINAL_ENV_FAILURE_PREFIX}: upstream unavailable"
        ));
        match verdict {
            RalphVerdict::Unrecoverable { reason, .. } => {
                assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        assert!(
            metrics.iter().any(|m| m.name == "ralph_terminations_total"
                && m.labels.get("reason").map(String::as_str) == Some("terminal_env_failure")),
            "terminal env failures must be visible as ralph_terminations_total{{reason=...}}"
        );
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
            readings(30),
        ));
        ralph.history.push(failed_iteration_with_readings(
            2,
            "return type wrong",
            readings(30),
        ));
        ralph.history.push(failed_iteration_with_readings(
            3,
            "one test still red",
            readings(60),
        ));
        assert!(!ralph.is_stagnated());
    }

    // -----------------------------------------------------------------
    // Mock LLM provider for semantic failure analysis tests
    // -----------------------------------------------------------------
    struct MockSemanticLlm {
        response_json: String,
        /// Every message list this double was asked to answer, so a test can read
        /// what the classifier actually sent instead of what it was meant to send.
        seen: std::sync::Mutex<Vec<Vec<cog_core::Message>>>,
    }

    impl MockSemanticLlm {
        fn new(response_json: impl Into<String>) -> Self {
            Self {
                response_json: response_json.into(),
                seen: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<Vec<cog_core::Message>> {
            self.seen.lock().unwrap_or_else(|e| e.into_inner()).clone()
        }
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
                upstream_failure: None,
                retry_after_secs: None,
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
            messages: &[cog_core::Message],
            _options: &cog_core::ChatOptions,
        ) -> cog_core::SFResult<cog_core::ChatResponse> {
            self.seen
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(messages.to_vec());
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
                upstream_failure: None,
                retry_after_secs: None,
                timestamp: chrono::Utc::now(),
            })
        }

        async fn health_check(&self) -> bool {
            true
        }
    }

    fn make_ralph_with_mock(response_json: &str) -> RalphLoop {
        let llm = Arc::new(MockSemanticLlm::new(response_json));
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

    /// A failure whose text already declares its class is answered from that
    /// text, and the classifier is never asked.
    ///
    /// The call-count assertion is the point: the classifier is the retry
    /// path's only paid call, and the reason strings below are shapes the wire
    /// actually produces — an error type wrapping the marker, and a defect
    /// prepended ahead of the original feedback. Read as "nothing declared",
    /// each of them costs a classification call plus the extra iteration the
    /// answer buys, for a run that had already been told retrying cannot help.
    #[tokio::test]
    async fn a_declared_class_is_answered_without_asking_the_classifier() {
        use cog_core::contract::outcome::DEGENERATE_LOOP_PREFIX;
        use cog_core::Observable;

        let llm = Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"LogicError","root_cause":"bug","recommended_strategy":"Modified","suggested_modifications":"fix"}"#,
        ));
        let ralph = RalphLoop::new().with_llm_provider(llm.clone());

        for feedback in [
            format!(
                "Agent execution error: {DEGENERATE_LOOP_PREFIX}: 3 iterations bought no progress"
            ),
            format!("missing deliverable; original feedback: {DEGENERATE_LOOP_PREFIX}: flat"),
        ] {
            let eval = EvaluationResult {
                verdict: Verdict::Fail,
                score: Some(0),
                feedback: feedback.clone(),
                criteria: vec![],
                details: None,
            };
            let analysis = ralph.analyze_failure(&eval, &[]).await;
            assert!(
                matches!(analysis, FailureAnalysis::Unrecoverable(_)),
                "a declared class is terminal however the text was wrapped: {feedback:?} → {analysis:?}"
            );
        }
        assert!(
            llm.calls().is_empty(),
            "a declared class must not cost a classification call, got {} call(s)",
            llm.calls().len()
        );

        let metrics = crate::observable::global_observable()
            .collect_metrics("D8")
            .await
            .unwrap();
        assert!(
            metrics.iter().any(|m| m.name == "ralph_failure_route_total"
                && m.labels.get("route").map(String::as_str) == Some("degenerate_loop")
                && m.value > 0.0),
            "the route must be readable as ralph_failure_route_total{{route=\"degenerate_loop\"}}"
        );
    }

    /// The same door for the other declared class: a terminal environment or
    /// protocol failure is terminal because the text says so, not because a
    /// model agreed.
    #[tokio::test]
    async fn a_terminal_marker_is_answered_without_asking_the_classifier() {
        use cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX;

        let llm = Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"ResourceError","root_cause":"rate limit","recommended_strategy":"Identical","suggested_modifications":"retry"}"#,
        ));
        let ralph = RalphLoop::new().with_llm_provider(llm.clone());

        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: format!("Agent execution error: {TERMINAL_ENV_FAILURE_PREFIX}: upstream 503"),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(analysis, FailureAnalysis::Unrecoverable(_)),
            "terminal env failures are terminal without a classifier: {analysis:?}"
        );
        assert!(
            llm.calls().is_empty(),
            "a terminal marker must not cost a classification call, got {} call(s)",
            llm.calls().len()
        );
    }

    /// A failure that declares nothing still goes to the classifier and still
    /// gets its answer, so the guard above cannot quietly become a run that
    /// never classifies anything.
    #[tokio::test]
    async fn an_undeclared_failure_still_reaches_the_classifier() {
        let llm = Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"Contradiction","root_cause":"plan vs output mismatch","recommended_strategy":"Modified","suggested_modifications":"align plan"}"#,
        ));
        let ralph = RalphLoop::new().with_llm_provider(llm.clone());

        let eval = EvaluationResult {
            verdict: Verdict::Fail,
            score: Some(0),
            feedback: "the plan contradicts the deliverable".into(),
            criteria: vec![],
            details: None,
        };
        let analysis = ralph.analyze_failure(&eval, &[]).await;
        assert!(
            matches!(
                analysis,
                FailureAnalysis::Recoverable(ResetStrategy::Modified)
            ),
            "an undeclared failure must still get the classified answer: {analysis:?}"
        );
        assert_eq!(
            llm.calls().len(),
            1,
            "the undeclared route must still be paid for exactly once"
        );
    }

    /// In-memory board + append-only event store for history persistence tests.
    struct BoardMockBackend {
        fields: std::sync::Mutex<std::collections::HashMap<(String, String), String>>,
        events: std::sync::Mutex<std::collections::HashMap<String, Vec<cog_core::Event>>>,
        /// When set, every append fails, so a test can hold the loop to its
        /// promise that a dead cold archive never fails the run.
        reject_appends: bool,
    }

    impl BoardMockBackend {
        fn new() -> Self {
            Self {
                fields: std::sync::Mutex::new(std::collections::HashMap::new()),
                events: std::sync::Mutex::new(std::collections::HashMap::new()),
                reject_appends: false,
            }
        }

        fn rejecting_appends() -> Self {
            Self {
                reject_appends: true,
                ..Self::new()
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
            task_id: &str,
            event: &cog_core::Event,
        ) -> cog_core::SFResult<u64> {
            if self.reject_appends {
                return Err(cog_core::SFError::Agent("event store unavailable".into()));
            }
            let mut events = self.events.lock().unwrap();
            let list = events.entry(task_id.to_string()).or_default();
            list.push(event.clone());
            Ok(list.len() as u64)
        }

        async fn get_events(
            &self,
            task_id: &str,
            offset: u64,
            limit: usize,
        ) -> cog_core::SFResult<Vec<cog_core::Event>> {
            let events = self.events.lock().unwrap();
            let Some(list) = events.get(task_id) else {
                return Ok(Vec::new());
            };
            let start = (offset as usize).min(list.len());
            let end = (start + limit).min(list.len());
            Ok(list[start..end].to_vec())
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

    /// 冷归档要留住 board 不再留的那部分。一轮不收敛的执行在 board 上只剩
    /// 尾窗——重启只消费这些——但根因分析要的正是被删掉的那些轮次。
    #[tokio::test]
    async fn every_iteration_is_archived_even_when_the_board_keeps_only_the_tail() {
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

        // 每轮都换重置策略：判据是"还在换手段就不算停滞"，循环于是不会在
        // 第 1 轮就被停滞判据截停；它最终停在第 3 轮——评估器每次报同一个
        // 失败，控制流判据在那里认定"同一失败重复"。
        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 5,
            stagnation_window: 1,
        })
        .with_llm_provider(Arc::new(MockSemanticLlm::new(
            r#"{"failure_type":"Contradiction","root_cause":"plan vs output","recommended_strategy":"Modified","suggested_modifications":"align"}"#,
        )))
        .with_history_store("task-archive".into(), backend.clone());

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
        assert_eq!(history.len(), 3, "the run stops at the repeated failure");

        let archived = archived_iterations(backend.as_ref(), "task-archive", 64)
            .await
            .unwrap();
        assert_eq!(
            archived.len(),
            3,
            "every iteration reaches the cold archive"
        );
        assert_eq!(
            archived.iter().map(|it| it.iteration).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the archive keeps the order the loop produced"
        );

        let raw = backend
            .fields
            .lock()
            .unwrap()
            .get(&("task-archive".to_string(), RALPH_HISTORY_FIELD.to_string()))
            .cloned()
            .expect("history field must be persisted");
        let stored: Vec<RalphIteration> = serde_json::from_str(&raw).unwrap();
        assert_eq!(
            stored.len(),
            1,
            "the board still holds only the stagnation window"
        );
    }

    /// 冷归档是尽力而为：它挂了只能让取证少一份，绝不能把循环本身带下去。
    #[tokio::test]
    async fn an_archive_that_cannot_be_written_does_not_fail_the_loop() {
        let backend = Arc::new(BoardMockBackend::rejecting_appends());
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
            stagnation_window: 2,
        })
        .with_history_store("task-archive-down".into(), backend.clone());

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
        assert!(
            matches!(verdict, RalphVerdict::Unrecoverable { .. }),
            "a dead cold archive must not change what the loop decides"
        );

        // The board is a separate write and still lands.
        let raw = backend
            .fields
            .lock()
            .unwrap()
            .get(&(
                "task-archive-down".to_string(),
                RALPH_HISTORY_FIELD.to_string(),
            ))
            .cloned()
            .expect("board persistence is independent of the archive");
        let stored: Vec<RalphIteration> = serde_json::from_str(&raw).unwrap();
        assert_eq!(stored.len(), 2);
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
            // 窗口显式给值：这条用例断言的是"保留多少条上下文、本轮跑几轮"，
            // 用默认值会让窗口默认值一改就红，那是锁实现细节而不是锁行为。
            stagnation_window: 3,
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
        // 起算点若仍跟着历史走，这里会是 3（0 轮执行）而不是 5。
        assert_eq!(ralph.history_keep(), 3);
        assert_eq!(history.len(), 5);
        assert_eq!(history[3].iteration, 1);
        assert_eq!(history[4].iteration, 2);
    }

    /// 生成器报出确定性环境/协议失败时，终止原因是那条失败本身，而不是
    /// "停滞"。两者都是 Unrecoverable，但语义不同：环境失败自带"重试无用"，
    /// 会被上层按前缀跳过策略升级；停滞只是"这一轮没买到东西"。窗口取 1 让
    /// 停滞判据在同一次迭代后必然成立，于是"谁优先"是可判的——把终止顺序
    /// 换回去，reason 就会变成 "Ralph Loop stagnated: ..."。
    #[tokio::test]
    async fn a_terminal_environment_failure_outranks_stagnation() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 0,
            independent_review: false,
        });
        let planner = PlannerActor::new(Arc::new(pass_planner()));
        let generator = GeneratorActor::new(Arc::new(MockAgent {
            response: serde_json::json!({
                "content": "environment_error: HTTP 503 upstream unavailable",
                "artifacts": [],
            }),
        }));

        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 3,
            stagnation_window: 1,
        });
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

        match verdict {
            RalphVerdict::Unrecoverable { reason, .. } => {
                assert!(
                    reason.starts_with(cog_core::contract::outcome::TERMINAL_ENV_FAILURE_PREFIX),
                    "the environment failure must be reported, not the stall: {reason}"
                );
                assert!(
                    reason.contains("HTTP 503 upstream unavailable"),
                    "the reason must carry the generator's own cause: {reason}"
                );
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    /// 计划侧耗尽迭代预算时，终止因必须指向计划器的预算，而不是某个生成器缺陷。
    /// 计划侧一坏 pipeline 就收口，生成器根本没被调用——它留下的那个空生成是
    /// 合成出来的证据，不是证据。外层若改从 final_generation 重新推导，就会把
    /// 责任记在一个从未运行的角色头上：实证里 github-issue-62 报的正是
    /// "generator produced no artifacts"，而日志里一次生成器调用都没有。
    #[tokio::test]
    async fn a_planner_that_spent_its_budget_is_not_reported_as_a_generator_defect() {
        let pipeline = PgePipeline::new(PgePipelineConfig {
            max_retries: 1,
            timeout_ms: 5_000,
            local_repair_max: 0,
            stall_threshold: 0,
            independent_review: false,
        });
        let planner = PlannerActor::new(Arc::new(MockAgent {
            response: serde_json::json!({
                "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                "iterations": 10,
                "pending_tool_calls": 1,
            }),
        }));
        // 若生成器真被调用会回一个可辨认的内容，用它证明这一轮没走到生成。
        let generator = GeneratorActor::new(Arc::new(MockAgent {
            response: serde_json::json!({"content": "SHOULD_NOT_RUN", "artifacts": []}),
        }));

        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 3,
            stagnation_window: 1,
        });
        let verdict = ralph
            .run_pipeline(
                "goal",
                serde_json::json!({}),
                &pipeline,
                &planner,
                &generator,
                &EvaluatorActor::new(Arc::new(pass_evaluator())),
            )
            .await;

        match verdict {
            RalphVerdict::Unrecoverable { reason, .. } => {
                assert_eq!(
                    classify(&reason),
                    crate::squad::classify::TERMINAL_ENV_FAILURE_CLASS,
                    "外层按分类前缀跳过策略升级，实到: {reason}"
                );
                assert!(
                    reason.contains(cog_core::contract::outcome::ITERATION_BUDGET_EXHAUSTED_MARKER),
                    "真因是计划器的预算耗尽，实到: {reason}"
                );
                assert!(
                    reason.contains("max_iterations=10"),
                    "原因必须带上实测数字，实到: {reason}"
                );
                assert!(
                    !reason.contains("generator produced no artifacts"),
                    "生成器从未运行，不能被报成生成器缺陷: {reason}"
                );
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
    }

    /// 同一件事在 Roundtable 上的镜像：评审器耗尽预算的那一轮，真因是评审器，
    /// 而它的生成是正常交付的。从 final_generation 推导在这里会得出相反的结论。
    #[tokio::test]
    async fn a_roundtable_judge_that_spent_its_budget_keeps_its_own_reason() {
        let roundtable = PgeRoundtable::new(
            PgeRoundtableConfig {
                max_iterations: 3,
                consensus_threshold: 0.8,
                stall_threshold: 0,
                independent_review: false,
                ..Default::default()
            },
            PlannerActor::new(Arc::new(pass_planner())),
            GeneratorActor::new(Arc::new(pass_generator())),
            EvaluatorActor::new(Arc::new(MockAgent {
                response: serde_json::json!({
                    "status": cog_core::contract::outcome::MAX_ITERATIONS_STATUS,
                    "iterations": 5,
                    "pending_tool_calls": 3,
                }),
            })),
        );

        let mut ralph = RalphLoop::with_config(RalphLoopConfig {
            max_iterations: 2,
            stagnation_window: 1,
        });
        let verdict = ralph
            .run_roundtable("goal", serde_json::json!({}), &roundtable)
            .await;

        match verdict {
            RalphVerdict::Unrecoverable { reason, .. } => {
                assert_eq!(
                    classify(&reason),
                    crate::squad::classify::TERMINAL_ENV_FAILURE_CLASS,
                    "实到: {reason}"
                );
                assert!(
                    reason.contains("evaluator") && reason.contains("iteration budget"),
                    "真因是评审器的预算耗尽，实到: {reason}"
                );
            }
            other => panic!("expected Unrecoverable, got {other:?}"),
        }
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
