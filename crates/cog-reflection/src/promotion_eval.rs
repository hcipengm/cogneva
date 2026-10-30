//! 晋升侧 eval A/B 门的**当次重跑**驱动，以及两处 eval 门共用的读数词汇。
//!
//! 晋升门原先读 `EvolutionResult.eval_summary`——一个**存量判词**。生产上没有任何
//! 东西给它写 `Some`（变更从盘上重建成 `None`），于是那道门从来没触发过，而它与
//! 「门跑了、判词不可用」在读数上同形。这里把它换成**当次重跑**：晋级前让沙盒
//! agent 在提交所在的 git 目录里对 `(rev, rev^)` 各建一棵工作树、各跑一遍固定评估
//! 集，控制器只对报回来的两组结果跑确定性 z 检验。
//!
//! 同一份评估也在基线移植侧跑（[`crate::baseline_port`]）。两处共用这一格的词汇
//! （[`EvalGateOutcome`]）与写点（[`report_eval_gate_outcome`]），用
//! [`EvalGateSite`] 分开——「门跑没跑」在两个站点上不是同一条读数：一个站点的门
//! 停摆，不代表另一个站点也在动。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cog_core::{
    ActionPlannerMeta, ActionPlannerSource, OrchestratorControl, SFError, SFResult, Task,
    TaskStatus, TaskType,
};
use tracing::{debug, warn};

use crate::auto_promoter::PromotionSource;
use crate::eval_harness::{compare as eval_compare, EvalComparison, EvalOutcome, EvalVerdict};

/// 这道 eval A/B 门在哪个站点跑，闭集。
///
/// 同一条门（解析两组结果 + z 检验）有两个入口：基线移植侧与晋升侧。两个入口的
/// **拦截策略不同**（移植侧只拦统计显著回归，晋升侧拦 `blocks_promotion()` 全集），
/// 所以把它们的结局混进一条无标签的序列，会让「哪个站点的门在动、按什么策略在动」
/// 读不出来。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvalGateSite {
    /// 基线移植侧（[`crate::baseline_port::BaselinePorter`] 的质量门）。
    Port,
    /// 晋升侧（晋级前的当次重跑）。
    Promotion,
}

impl EvalGateSite {
    /// 读数标签的取值。闭集。
    fn as_cell(self) -> &'static str {
        match self {
            EvalGateSite::Port => "port",
            EvalGateSite::Promotion => "promotion",
        }
    }
}

/// 这次 eval A/B 门判成了哪一种结局。
///
/// 判据来自被判方——沙盒 agent 自己跑两遍评估集、自己报 `success` 布尔，控制器
/// 只对报回来的两组数跑 z 检验。所以「门测过并放行」与「门没测」是两件不同的事实，
/// 这几格就是那道区别。
///
/// `Inconclusive` 单列一格：z 检验判「无统计显著差异」既不是通过也不是回归。
/// 把它记成 `passed`，会让一个「没测出差异」的结论冒充「通过」——那正是这棵树最
/// 忌的假绿：读数读成了它不是的东西。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EvalGateOutcome {
    /// 跑了 z 检验，显著更好（`Adopt`）。
    Passed,
    /// 跑了 z 检验，显著更差（`Reject`）：回归。
    Rejected,
    /// 跑了 z 检验，无统计显著差异（`Inconclusive`）。
    Inconclusive,
    /// 被判方自报没有评估套件可跑。门没有测量。
    NotApplicable,
    /// 被判方返回的东西读不成两组结果集。门没有测量。
    Unreadable,
    /// 根本没走到判词：无编排器、或评估任务没跑成。
    Unavailable,
}

impl EvalGateOutcome {
    /// 读数标签的取值。闭集，读的人按这一格把「测过且通过」与几种「没测」分开。
    pub(crate) fn as_cell(self) -> &'static str {
        match self {
            EvalGateOutcome::Passed => "passed",
            EvalGateOutcome::Rejected => "rejected",
            EvalGateOutcome::Inconclusive => "inconclusive",
            EvalGateOutcome::NotApplicable => "not_applicable",
            EvalGateOutcome::Unreadable => "unreadable",
            EvalGateOutcome::Unavailable => "unavailable",
        }
    }

    /// 闭集全集，供开机播种用。
    pub(crate) const ALL: [EvalGateOutcome; 6] = [
        EvalGateOutcome::Passed,
        EvalGateOutcome::Rejected,
        EvalGateOutcome::Inconclusive,
        EvalGateOutcome::NotApplicable,
        EvalGateOutcome::Unreadable,
        EvalGateOutcome::Unavailable,
    ];
}

/// 发布一次门的结局。站点由 `site` 分开。
///
/// 与各站点的 tick 结局读数同形：门有多处提前返回，写点逐一排在每个返回点之前，
/// 让「每轮都写」按构造成立。
pub(crate) async fn report_eval_gate_outcome(
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
    outcome: EvalGateOutcome,
    site: EvalGateSite,
) {
    let Some(metrics) = metrics else {
        return;
    };
    let mut labels = HashMap::new();
    labels.insert(
        crate::LABEL_OUTCOME.to_string(),
        outcome.as_cell().to_string(),
    );
    labels.insert(crate::LABEL_SITE.to_string(), site.as_cell().to_string());
    if let Err(e) = metrics
        .record_counter(
            cog_core::metric_names::EVAL_GATE_OUTCOMES_TOTAL,
            1.0,
            labels,
        )
        .await
    {
        warn!(error = %e, "eval gate outcome emit failed");
    }
}

/// 开机把**一个站点**的六格按 0 摆出来。
///
/// 这格只在 eval A/B 门被走到时才写，而门走的是「有变更可移植 / 有变更待晋级」那条
/// 路径：没有变更在途时整条路一次都不进，于是**门没跑过**与**读数没接线**在
/// `/metrics` 上是同一个样子——一条序列都不出现。播种之后「本进程起来后门一次都没跑」
/// 是一排可读的 0，而不是一片缺席；缺席只留给「这个二进制根本没有这条读数」。
///
/// 按**站点**分，不一次摆两站：两站的门由不同的接线驱动（移植门在移植循环里、晋升门
/// 在晋级器里），一个站点的开关关掉不该让另一个站点的「0 而不缺席」跟着消失——那正是
/// `site` 这一维存在的理由。每个站点的写者播自己那六格。
pub(crate) async fn seed_eval_gate_site(
    metrics: &Option<Arc<dyn cog_core::MetricsBackend>>,
    site: EvalGateSite,
) {
    let Some(metrics) = metrics else {
        return;
    };
    for outcome in EvalGateOutcome::ALL {
        let mut labels = HashMap::new();
        labels.insert(
            crate::LABEL_OUTCOME.to_string(),
            outcome.as_cell().to_string(),
        );
        labels.insert(crate::LABEL_SITE.to_string(), site.as_cell().to_string());
        if let Err(e) = metrics
            .record_counter(
                cog_core::metric_names::EVAL_GATE_OUTCOMES_TOTAL,
                0.0,
                labels,
            )
            .await
        {
            warn!(error = %e, "eval gate outcome seed failed");
        }
    }
}

/// 一次评估任务结果的解析结论。两处站点共用这一份解析，只把**策略**留给调用方。
pub(crate) enum CompareOutcome {
    /// 被判方自报没有套件（`applicable=false`）。门没有测量。
    NotApplicable,
    /// 返回的东西读不成两组结果集。门没有测量，`String` 是原因。
    Unreadable(String),
    /// 测出来了：完整对比报告（判词、z、两组摘要）＋原始 after 条目
    /// （调用方用它点名失败的任务写进判词）。
    Measured(EvalComparison, Vec<EvalOutcome>),
}

/// 对一次评估任务的结果跑解析 + z 检验。**只有这一处**把 agent 报回来的 JSON
/// 变成判词；两处站点的拦截策略不同，但读的是同一份判词。
pub(crate) fn compare_result(result: &serde_json::Value) -> CompareOutcome {
    let applicable = result
        .get("applicable")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    if !applicable {
        return CompareOutcome::NotApplicable;
    }
    let parse = |key: &str| -> Result<Vec<EvalOutcome>, String> {
        let raw = result
            .get(key)
            .ok_or_else(|| format!("eval result missing `{key}` outcomes"))?;
        serde_json::from_value(raw.clone())
            .map_err(|e| format!("eval result `{key}` outcomes unparseable: {e}"))
    };
    let before = match parse("before") {
        Ok(v) => v,
        Err(e) => return CompareOutcome::Unreadable(e),
    };
    let after = match parse("after") {
        Ok(v) => v,
        Err(e) => return CompareOutcome::Unreadable(e),
    };
    if before.is_empty() || after.is_empty() {
        return CompareOutcome::Unreadable("eval result returned empty outcome sets".into());
    }
    let comparison = eval_compare(&before, &after);
    CompareOutcome::Measured(comparison, after)
}

/// 把一次 A/B 结论压成一句可落台账的摘要：判词、z、两组成功率、失败的任务名。
/// 台账里只留这一句，所以失败任务要**点名**——只写「回归了」事后说不出是哪几条挂了。
pub(crate) fn comparison_summary(cmp: &EvalComparison, after: &[EvalOutcome]) -> String {
    let failing: Vec<&str> = after
        .iter()
        .filter(|o| !o.success)
        .map(|o| o.task_id.as_str())
        .collect();
    format!(
        "{:?} z={:.2}, success {}/{} -> {}/{}; failing tasks: {}",
        cmp.verdict,
        cmp.z_score,
        cmp.before.succeeded,
        cmp.before.total,
        cmp.after.succeeded,
        cmp.after.total,
        if failing.is_empty() {
            "none".to_string()
        } else {
            failing.join(", ")
        }
    )
}

/// 晋升侧的 A/B 求值器：晋级前对一条待发布提交的 `(rev, rev^)` 各跑一遍固定评估集。
///
/// 它只对**提交所在的 git 目录 + 两个 rev** 说话（从 [`PromotionSource`] 取），
/// 不假定调用方站在哪个状态——这与移植侧那道门相反：移植侧的门假定调用方已经在
/// after 态、只让 agent 建前态那棵，所以只有「解析 + z 检验」那半边可复用。
///
/// 评估执行本身是智能步骤（沙盒双构建），打包成主流程任务；本驱动只提交、等待、
/// 对返回的两组数跑确定性 z 检验。
pub(crate) struct PromotionEval {
    orchestrator: Arc<dyn OrchestratorControl>,
    /// A/B 任务的等待上限（秒，含沙盒双构建）。
    timeout_secs: u64,
    /// 轮询任务结果的间隔（秒）。
    poll_interval_secs: u64,
    /// 结局读数的产出面。
    metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
}

/// 晋升侧 A/B 门的求值面。
///
/// 抽成 trait 是为了让晋级器只依赖「跑一次 A/B 得到结局」这个契约，而不是那台
/// 具体的沙盒双构建机器——测试据此注入一个返回固定结局的求值器，考的是**门本身**
/// （拦不拦、记哪一格），不必连真编排器一起搭起来。
#[async_trait::async_trait]
pub(crate) trait PromotionEvaluator: Send + Sync {
    /// 跑一次当次 A/B，返回结局。
    async fn evaluate(
        &self,
        change_id: &str,
        source: Option<&PromotionSource>,
    ) -> PromotionEvalOutcome;
}

#[async_trait::async_trait]
impl PromotionEvaluator for PromotionEval {
    async fn evaluate(
        &self,
        change_id: &str,
        source: Option<&PromotionSource>,
    ) -> PromotionEvalOutcome {
        PromotionEval::evaluate(self, change_id, source).await
    }
}

/// 一次晋升侧 A/B 的结论：调用方据此决定拦不拦。
pub(crate) enum PromotionEvalOutcome {
    /// 测出来了：判词 ＋ 一句可落台账的摘要（z、两组成功率、失败任务名）。
    Measured(EvalVerdict, String),
    /// 被判方自报没有套件可跑。
    NotApplicable,
    /// 读不成两组结果。
    Unreadable(String),
    /// 根本没测：无来源、无编排器、或任务没跑成。
    Unavailable,
}

impl PromotionEvalOutcome {
    /// 这一刻度落在哪一格。
    fn cell(&self) -> EvalGateOutcome {
        match self {
            PromotionEvalOutcome::Measured(EvalVerdict::Adopt, _) => EvalGateOutcome::Passed,
            PromotionEvalOutcome::Measured(EvalVerdict::Reject, _) => EvalGateOutcome::Rejected,
            PromotionEvalOutcome::Measured(EvalVerdict::Inconclusive, _) => {
                EvalGateOutcome::Inconclusive
            }
            PromotionEvalOutcome::NotApplicable => EvalGateOutcome::NotApplicable,
            PromotionEvalOutcome::Unreadable(_) => EvalGateOutcome::Unreadable,
            PromotionEvalOutcome::Unavailable => EvalGateOutcome::Unavailable,
        }
    }

    /// 这次结论是否应当拦下晋级。
    ///
    /// 只由 `blocks_promotion()` 在契约层定义一处：`Reject` 与 `Inconclusive` 拦，
    /// `Adopt` 放行。没测到（不适用 / 读不出 / 不可用）不是拦——否则全仓没有套件
    /// 时整条晋升线会停住，而那不是「回归」。
    pub(crate) fn blocks_promotion(&self) -> bool {
        match self {
            PromotionEvalOutcome::Measured(v, _) => v.blocks_promotion(),
            _ => false,
        }
    }

    /// 拦下时要落进台账的理由。只有 `blocks_promotion()` 为真时才有意义。
    pub(crate) fn block_reason(&self) -> String {
        match self {
            PromotionEvalOutcome::Measured(_, summary) => {
                format!("eval A/B gate blocked promotion: {summary}")
            }
            PromotionEvalOutcome::NotApplicable => "eval A/B gate: no suite applicable".into(),
            PromotionEvalOutcome::Unreadable(reason) => {
                format!("eval A/B gate unreadable: {reason}")
            }
            PromotionEvalOutcome::Unavailable => "eval A/B gate unavailable".into(),
        }
    }
}

/// A/B 任务的默认等待上限（秒，含沙盒双构建）。
///
/// 与基线移植侧 eval 门同一量级（`baseline_port::BaselinePorter::eval_timeout_secs`
/// 默认 3600）：同样是「把一棵工作树建起来 + 跑固定评估集」这件最贵的事。两个数
/// 目前各自写在构造点、没有共同的配置位——值一致是因为它们量的是同一件事，不是因为
/// 有一处领它们；真要分开调时这两处就是覆盖点。
// cap: bound — 两处构造点值一致是因为它们量的是同一件事，没有共同配置位；真要分开调时这两处就是覆盖点
pub(crate) const DEFAULT_EVAL_TIMEOUT_SECS: u64 = 3600;

/// 轮询任务结果的默认间隔（秒），与基线移植侧一致（默认 5）。
// cap: bound — 与基线移植侧的轮询间隔一致
pub(crate) const DEFAULT_EVAL_POLL_INTERVAL_SECS: u64 = 5;

impl PromotionEval {
    pub(crate) fn new(
        orchestrator: Arc<dyn OrchestratorControl>,
        timeout_secs: u64,
        poll_interval_secs: u64,
        metrics: Option<Arc<dyn cog_core::MetricsBackend>>,
    ) -> Self {
        Self {
            orchestrator,
            timeout_secs,
            poll_interval_secs,
            metrics,
        }
    }

    /// 对一条待晋升变更跑当次 A/B，并把结局写进读数面。
    ///
    /// 没有来源（不知道提交在哪个 git 目录、是哪个 rev）就没有可重跑的对象，
    /// 记 `Unavailable` 而不拦——这一格是「门没测」，不是「门判了回归」。
    pub(crate) async fn evaluate(
        &self,
        change_id: &str,
        source: Option<&PromotionSource>,
    ) -> PromotionEvalOutcome {
        let Some(source) = source else {
            let outcome = PromotionEvalOutcome::Unavailable;
            self.report(&outcome).await;
            return outcome;
        };
        let task_id = format!("promotion-eval-{}", sanitize_ref(change_id));
        let goal = format!(
            "Run the fixed evaluation suite twice to check whether change `{change_id}` \
             (commit {rev}) improves or regresses behavior against its parent commit.\n\n\
             Working git repository (contains both commits): {repo}\n\n\
             Procedure:\n\
             1. Create a temporary git worktree at the PARENT commit {rev}^, build there, \
             and run the project's standard fixed eval task set; record outcomes as `before`. \
             Remove the temporary worktree afterwards.\n\
             2. Create a temporary git worktree at {rev}, build there, and run the SAME eval \
             task set; record outcomes as `after`. Remove it afterwards.\n\
             Do NOT commit anything in either worktree.\n\
             3. If no eval suite exists for this component, report applicable=false.\n\n\
             Return ONLY a JSON object:\n\
             {{\"applicable\": true, \"before\": [{{\"task_id\": \"...\", \"success\": \
             true, \"latency_ms\": 0, \"cost_tokens\": 0}}], \"after\": [...], \
             \"note\": \"\"}}",
            repo = source.repo.display(),
            rev = source.rev,
        );
        let mut task = Task::new(
            task_id.clone(),
            TaskType::Custom("promotion_eval".into()),
            serde_json::json!({
                (cog_core::INPUT_EVOLUTION_MODE): "promotion",
                (cog_core::INPUT_TASK_KIND): "promotion_eval",
                "change_id": change_id,
                "rev": source.rev,
                "repo": source.repo.display().to_string(),
            }),
        );
        task.timeout_seconds = self.timeout_secs;
        task.action_planner_meta = Some(verified_meta(
            "Promotion eval A/B; route to the sandbox evolution agent",
        ));

        let completed = match submit_and_wait(
            &self.orchestrator,
            self.poll_interval_secs,
            task,
            goal,
            self.timeout_secs,
        )
        .await
        {
            Ok(Ok(t)) => t,
            Ok(Err(reason)) => {
                debug!(change_id = %change_id, reason = %reason, "promotion eval task did not complete");
                let outcome = PromotionEvalOutcome::Unavailable;
                self.report(&outcome).await;
                return outcome;
            }
            Err(e) => {
                warn!(change_id = %change_id, error = %e, "promotion eval submit failed");
                let outcome = PromotionEvalOutcome::Unavailable;
                self.report(&outcome).await;
                return outcome;
            }
        };

        let result = completed.result.unwrap_or_else(|| serde_json::json!({}));
        let outcome = match compare_result(&result) {
            CompareOutcome::NotApplicable => PromotionEvalOutcome::NotApplicable,
            CompareOutcome::Unreadable(r) => PromotionEvalOutcome::Unreadable(r),
            CompareOutcome::Measured(cmp, after) => {
                PromotionEvalOutcome::Measured(cmp.verdict, comparison_summary(&cmp, &after))
            }
        };
        self.report(&outcome).await;
        outcome
    }

    async fn report(&self, outcome: &PromotionEvalOutcome) {
        report_eval_gate_outcome(&self.metrics, outcome.cell(), EvalGateSite::Promotion).await;
    }
}

/// 提交任务给主流程并轮询至终态。`Completed` 返回任务本身（带 result），
/// `Failed`/`Cancelled`/超时返回原因文本。移植侧与晋升侧共用这一份提交等待。
pub(crate) async fn submit_and_wait(
    orch: &Arc<dyn OrchestratorControl>,
    poll_interval_secs: u64,
    task: Task,
    goal: String,
    timeout_secs: u64,
) -> SFResult<Result<Task, String>> {
    let task_id = task.id.clone();
    let ids = orch
        .submit_goal_auto(&goal, vec![task])
        .await
        .map_err(|e| SFError::Internal(format!("submit task {task_id} failed: {e}")))?;
    let id = ids.into_iter().next().unwrap_or(task_id);

    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if let Some(t) = orch.get_task(&id).await {
            match t.status {
                TaskStatus::Completed => return Ok(Ok(t)),
                TaskStatus::Failed => {
                    return Ok(Err(format!("failed: {}", t.error.unwrap_or_default())))
                }
                TaskStatus::Cancelled => return Ok(Err("cancelled".into())),
                _ => {}
            }
        }
        if Instant::now() >= deadline {
            return Ok(Err(crate::cmd_timeout::timed_out_after(timeout_secs)));
        }
        tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;
    }
}

fn verified_meta(note: &str) -> ActionPlannerMeta {
    ActionPlannerMeta {
        verified: true,
        version: Some("1.0.0".into()),
        note: Some(note.into()),
        source: Some(ActionPlannerSource::UserProvided),
        confidence: None,
        timestamp: Some(chrono::Utc::now()),
    }
}

/// 任务 id / 文件名安全化：只留 [A-Za-z0-9-_]。
fn sanitize_ref(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .collect()
}
