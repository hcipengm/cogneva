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
use std::path::Path;
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
        let mut task = promotion_eval_task(change_id, &source.rev, &source.repo, self.timeout_secs);
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

    /// 把这道门交上去、如今已经没人等的行收回来，答收起几条。
    ///
    /// 等待超时的那条路自己会收回行；收不回的只有一种：等它的进程在等待结束前就
    /// 没了，于是那行留在 `Pending` 上，没有第二处会再来读它。这道门是那个 id 前缀
    /// 唯一的写者，所以清点这个命名空间是它自己的事，不该留给人在库里找。
    ///
    /// 在启动时走一趟就够了：能留下这种行的只有进程消失，而进程消失之后的第一件事
    /// 就是下一次启动。
    pub(crate) async fn withdraw_abandoned(&self) -> SFResult<usize> {
        let now = chrono::Utc::now();
        let mut withdrawn = 0usize;
        for task in self.orchestrator.get_all_tasks().await {
            if !is_abandoned_promotion_eval(&task, now, self.timeout_secs) {
                continue;
            }
            match self.orchestrator.cancel_task(&task.id).await {
                Ok(_) => {
                    withdrawn += 1;
                    debug!(
                        task_id = %task.id,
                        since = %task.updated_at,
                        "promotion eval: withdrew an A/B row whose wait is over; nothing was \
                         reading it"
                    );
                }
                Err(e) => warn!(
                    task_id = %task.id,
                    error = %e,
                    "promotion eval: an abandoned A/B row could not be withdrawn"
                ),
            }
        }
        if withdrawn > 0 {
            debug!(withdrawn, "promotion eval: abandoned A/B rows withdrawn");
        }
        Ok(withdrawn)
    }

    async fn report(&self, outcome: &PromotionEvalOutcome) {
        report_eval_gate_outcome(&self.metrics, outcome.cell(), EvalGateSite::Promotion).await;
    }
}

/// 提交任务给主流程并轮询至终态。`Completed` 返回任务本身（带 result），
/// `Failed`/`Cancelled`/超时返回原因文本。移植侧与晋升侧共用这一份提交等待。
/// 这道门给自己那行起的 id 前缀。
///
/// 这个命名空间只有这道门往里写，所以也只有它能把里面的行收回来：一行还挂在
/// 图上、而等它的那次等待已经不在了，图上没有任何别的读者分得出来。
pub const PROMOTION_EVAL_ID_PREFIX: &str = "promotion-eval-";

/// The row whose run is this change's A/B, as filed.
///
/// Public so a contract test can put it in front of the dispatcher's own
/// question. The row has to state what would close it: a work package that does
/// not is never started (`ready_queue::is_dispatchable_work`), which keeps it
/// safely pending and visible on the board — and also means this gate never
/// runs while its outcome keeps reading `Unavailable`, the same word written
/// when the gate was never asked at all. Two very different facts, one cell.
pub fn promotion_eval_task(change_id: &str, rev: &str, repo: &Path, timeout_secs: u64) -> Task {
    let mut task = Task::new(
        format!("{PROMOTION_EVAL_ID_PREFIX}{}", sanitize_ref(change_id)),
        TaskType::Custom("promotion_eval".into()),
        serde_json::json!({
            (cog_core::INPUT_EVOLUTION_MODE): "promotion",
            (cog_core::INPUT_TASK_KIND): "promotion_eval",
            "change_id": change_id,
            "rev": rev,
            "repo": repo.display().to_string(),
        }),
    );
    task.timeout_seconds = timeout_secs;
    task.acceptance
        .push(promotion_eval_acceptance(change_id, rev));
    task
}

/// 这一行是不是这道门交上去、而等它的那次等待已经不在了的行。
///
/// 判据只用这道门自己的时钟：等待上限就是「还在等」的界，过了它还没有终态的行，
/// 要么等的人已经把它收回去（那它就不是这个状态），要么等的人已经不在了——进程
/// 重启、任务被取消，都算。留一个等待上限的宽限，是为了不在收尾那一刻和正在收尾
/// 的那次等待抢同一行。
pub fn is_abandoned_promotion_eval(
    task: &Task,
    now: chrono::DateTime<chrono::Utc>,
    timeout_secs: u64,
) -> bool {
    if !task.id.starts_with(PROMOTION_EVAL_ID_PREFIX) {
        return false;
    }
    if !matches!(task.status, TaskStatus::Pending | TaskStatus::Scheduled) {
        return false;
    }
    let grace = chrono::Duration::from_std(Duration::from_secs(timeout_secs.saturating_mul(2)))
        .unwrap_or_else(|_| chrono::Duration::seconds(i64::MAX / 1000));
    task.updated_at + grace < now
}

/// What closes one promotion-eval row.
///
/// The requirement belongs to the change, not to this run: it is met when the
/// answer is on the record in a shape the gate can read back — both sides of the
/// A/B, or a stated `applicable: false`. A run that ends having said neither
/// leaves the gate with nothing to judge, and a row read by status cannot tell
/// that apart from a change that was never measured.
pub fn promotion_eval_acceptance(change_id: &str, rev: &str) -> String {
    format!(
        "变更 {change_id} 在 {rev} 上的 A/B 结局落成一条可取的判决：交回来的 JSON 里 \
         applicable=false 时写清这个组件没有可跑的固定评测集，否则 before 与 after 各是一组\
         带 task_id 与 success 的用例记录——而不是这次运行自己报完成"
    )
}

pub(crate) async fn submit_and_wait(
    orch: &Arc<dyn OrchestratorControl>,
    poll_interval_secs: u64,
    task: Task,
    goal: String,
    timeout_secs: u64,
) -> SFResult<Result<Task, String>> {
    let task_id = task.id.clone();
    // 图里同 id 的旧行会吃掉这一次提交：`add_tasks_batch` 撞上已有的 id 一律跳过，
    // 不读新交上去的那一份。上一次尝试留下的终态行（超时被收回、或失败）说的正是
    // 「那次没有答案」；让它挡在这里，同一个变更就再也跑不成 A/B，而且读起来是
    // 「门没跑」而不是「门被一行旧记录挡住了」。
    if let Some(held) = orch.get_task(&task_id).await {
        if matches!(held.status, TaskStatus::Cancelled | TaskStatus::Failed) {
            if let Err(e) = orch.delete_task(&task_id).await {
                warn!(
                    task_id = %task_id,
                    error = %e,
                    "promotion eval: an attempt that already ended is holding this id; \
                     the new run will be skipped as a redelivery"
                );
            }
        }
    }
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
            // 这次等待结束了，而那一行是它交上去的。就这么走开的话，行会一直挂在
            // 图上：没有第二处会再来读它（派发器只在行有判据时才开工，清扫器只收
            // `Scheduled`/`Running`），而按状态读这一行的人分不出「门还在想」与
            // 「门走了、行留下了」。所以等的人把它收回去。
            if let Err(e) = orch.cancel_task(&id).await {
                warn!(
                    task_id = %id,
                    error = %e,
                    "promotion eval: the wait is over but the row it filed could not be \
                     withdrawn; it stays on the graph with nothing left to read it"
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// 图里只放这道门自己交上去的行；答得出「那行现在什么样」，并记下每一次收回。
    #[derive(Default)]
    struct GateGraph {
        tasks: Mutex<HashMap<String, Task>>,
        cancelled: Mutex<Vec<String>>,
        deleted: Mutex<Vec<String>>,
    }

    fn pending_row(id: &str, since: chrono::DateTime<chrono::Utc>) -> Task {
        let mut task = Task::new(
            id.to_string(),
            TaskType::Custom("promotion_eval".into()),
            serde_json::json!({}),
        );
        task.status = TaskStatus::Pending;
        task.created_at = since;
        task.updated_at = since;
        task
    }

    #[async_trait::async_trait]
    impl OrchestratorControl for GateGraph {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            Ok(())
        }
        async fn submit_goal_auto(&self, _goal: &str, tasks: Vec<Task>) -> SFResult<Vec<String>> {
            let mut ids = Vec::new();
            for task in tasks {
                // 与真编排器同一条规矩：已有的 id 一律当重投跳过，新交的那份不读。
                let mut held = self.tasks.lock().unwrap();
                if held.contains_key(&task.id) {
                    continue;
                }
                ids.push(task.id.clone());
                held.insert(task.id.clone(), task);
            }
            Ok(ids)
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn add_task(&self, _task: Task) -> SFResult<()> {
            unimplemented!()
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            false
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            0
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            vec![]
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            self.tasks.lock().unwrap().values().cloned().collect()
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
            unimplemented!()
        }
        async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            unimplemented!()
        }
        async fn start_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> SFResult<Vec<String>> {
            unimplemented!()
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::UpstreamFailure>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<cog_core::UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            unimplemented!()
        }
        async fn cancel_task(&self, task_id: &str) -> SFResult<Vec<String>> {
            self.cancelled.lock().unwrap().push(task_id.to_string());
            if let Some(task) = self.tasks.lock().unwrap().get_mut(task_id) {
                task.status = TaskStatus::Cancelled;
            }
            Ok(vec![])
        }
        async fn get_task(&self, task_id: &str) -> Option<Task> {
            self.tasks.lock().unwrap().get(task_id).cloned()
        }
        async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!()
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            vec![]
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
            None
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            (self.get_all_tasks().await, vec![])
        }
        async fn delete_task(&self, task_id: &str) -> SFResult<()> {
            self.deleted.lock().unwrap().push(task_id.to_string());
            self.tasks.lock().unwrap().remove(task_id);
            Ok(())
        }
        async fn all_completed(&self) -> bool {
            false
        }
        async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
            unimplemented!()
        }
    }

    /// 等的人走了，行不能留在图上：按状态读它的人分不出「门还在想」与「门走了」。
    #[tokio::test]
    async fn a_wait_that_ran_out_takes_its_row_back() {
        let graph = Arc::new(GateGraph::default());
        let row = promotion_eval_task(
            "change-ledger-abc",
            "deadbeef",
            Path::new("/workspace/cogneva"),
            900,
        );

        let orch: Arc<dyn OrchestratorControl> = graph.clone();
        let outcome = submit_and_wait(&orch, 0, row.clone(), "goal".into(), 0).await;

        assert!(
            matches!(outcome, Ok(Err(_))),
            "超时要以理由回来，不是以一条任务回来"
        );
        assert_eq!(
            *graph.cancelled.lock().unwrap(),
            vec![row.id.clone()],
            "等待结束了，而它交上去的那一行还挂在图上"
        );
    }

    /// 上一次尝试已经终态的行会占住这个 id：提交撞上已有 id 一律跳过，于是这一次
    /// 提交什么都没交上去，等的还是那条旧行。它得先让开。
    #[tokio::test]
    async fn an_attempt_that_already_ended_does_not_hold_the_id_against_the_next_one() {
        let graph = Arc::new(GateGraph::default());
        let row = promotion_eval_task(
            "change-ledger-abc",
            "deadbeef",
            Path::new("/workspace/cogneva"),
            900,
        );
        let mut ended = row.clone();
        ended.status = TaskStatus::Cancelled;
        graph.tasks.lock().unwrap().insert(ended.id.clone(), ended);

        let orch: Arc<dyn OrchestratorControl> = graph.clone();
        let _ = submit_and_wait(&orch, 0, row.clone(), "goal".into(), 0).await;

        assert_eq!(
            *graph.deleted.lock().unwrap(),
            vec![row.id.clone()],
            "旧行没让开，这一次提交会被当成重投跳过"
        );
        // 让开之后这一次真的交上去了，才会又被这一次自己的等待收回。
        assert_eq!(
            *graph.cancelled.lock().unwrap(),
            vec![row.id.clone()],
            "这一次的提交没有落到图上"
        );
    }

    /// 生产上那条真的行：2026-11-01 交上去，等它的进程在等待结束前就没了，
    /// 于是它一直停在 `Pending`。
    #[test]
    fn a_row_left_by_a_wait_that_is_gone_reads_as_abandoned() {
        let now = chrono::Utc::now();
        let row = pending_row(
            "promotion-eval-change-ledger-ec0fc6a8c301bbf3-6862a991a1ff",
            now - chrono::Duration::hours(20),
        );
        assert!(
            is_abandoned_promotion_eval(&row, now, DEFAULT_EVAL_TIMEOUT_SECS),
            "等待上限 3600 秒，而这行已经在图上停了 20 小时"
        );
    }

    /// 三条反例：还在等的、已经终态的、别人的行，都不许被收走。
    #[test]
    fn only_rows_this_gate_filed_and_no_longer_waits_on_are_abandoned() {
        let now = chrono::Utc::now();
        let fresh = pending_row("promotion-eval-change-ledger-new", now);
        assert!(
            !is_abandoned_promotion_eval(&fresh, now, DEFAULT_EVAL_TIMEOUT_SECS),
            "刚落地的行还在等待上限之内"
        );

        let mut done = pending_row(
            "promotion-eval-change-ledger-old",
            now - chrono::Duration::hours(20),
        );
        done.status = TaskStatus::Completed;
        assert!(
            !is_abandoned_promotion_eval(&done, now, DEFAULT_EVAL_TIMEOUT_SECS),
            "终态的行不是没人等，是已经等到了"
        );

        let foreign = pending_row("some-other-work", now - chrono::Duration::hours(20));
        assert!(
            !is_abandoned_promotion_eval(&foreign, now, DEFAULT_EVAL_TIMEOUT_SECS),
            "不是这道门交上去的行，轮不到这道门来收"
        );
    }

    /// 启动时那一趟只收没人等的，别的一行都不碰。
    #[tokio::test]
    async fn the_startup_sweep_withdraws_only_the_rows_no_one_is_waiting_on() {
        let graph = Arc::new(GateGraph::default());
        let now = chrono::Utc::now();
        let stale = pending_row(
            "promotion-eval-change-ledger-ec0fc6a8c301bbf3-6862a991a1ff",
            now - chrono::Duration::hours(20),
        );
        let fresh = pending_row("promotion-eval-change-ledger-new", now);
        let foreign = pending_row("some-other-work", now - chrono::Duration::hours(20));
        for row in [&stale, &fresh, &foreign] {
            graph
                .tasks
                .lock()
                .unwrap()
                .insert(row.id.clone(), row.clone());
        }

        let orch: Arc<dyn OrchestratorControl> = graph.clone();
        let eval = PromotionEval::new(orch, DEFAULT_EVAL_TIMEOUT_SECS, 1, None);
        let withdrawn = eval.withdraw_abandoned().await.unwrap();

        assert_eq!(withdrawn, 1, "收走的条数不对");
        assert_eq!(*graph.cancelled.lock().unwrap(), vec![stale.id.clone()]);
    }
}
