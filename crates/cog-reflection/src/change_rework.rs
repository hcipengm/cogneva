//! 被门禁拒绝的变更回流主流程这条支路的属主门与读数。
//!
//! 一条变更被门禁拒绝，是系统自己拿到的一条需求：判词点名的文件和成因就是
//! 下一次生成的要求。这条需求过去由反射引擎自己的第二套生成器应答——没有任务
//! 行、没有 intent、没有按成因的预算，迭代信号是自己的结构校验而不是真门禁的
//! 判定。现在它回流成一条主流程任务（`task_kind = "change_rework"`），提交权
//! 和读数都落在本模块。
//!
//! 两件事必须一起做，缺一处就是「路删了、读数也没了」：
//!
//! - **属主门**：只有承担执行器职责的进程才提交。插件表在两个进程里整表加载，
//!   两个都会看见同一条学习；都提交就是同一份工作提交两遍。门是
//!   `self_evolution.executor_enabled`，与变更队列的属主同一把。
//! - **读数**：提交不出去必须看得见。编排器只在 `start()` 才可消费，而触发点
//!   在 `init()` 阶段就已经在跑，所以槽由一个延迟句柄跨过这个时间窗——init 建、
//!   start 填、触发点只读。槽空时提交会被丢掉，那是这条支路最可能的静默形态，
//!   因此它和提交失败各占一格。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use cog_core::{
    DimensionSpec, Observable, OrchestratorControl, RawMetric, SFResult, TraceFragment,
};

/// 一次触发想要回流时，工作最后到了哪里。
///
/// 闭集：标签就是它，一个没人命名的归宿会被计进一个说谎的标签里；而这三格
/// 各自的修法不同——提交成功不用管，提交失败是编排器在但拒了，空槽是这条
/// 支路根本没能把话说出去。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeReworkOutcome {
    /// 主流程收下了这条再生成任务。
    Submitted,
    /// 编排器在手上，提交被拒。
    SubmitFailed,
    /// 该提交的时刻槽是空的：编排器没到，这份需求没有出口。
    NoExecutor,
}

/// 三格就是全部。生产者与读它的规则都从这张表走，改一处不会漏掉另一处。
pub const REWORK_OUTCOMES: [ChangeReworkOutcome; 3] = [
    ChangeReworkOutcome::Submitted,
    ChangeReworkOutcome::SubmitFailed,
    ChangeReworkOutcome::NoExecutor,
];

impl ChangeReworkOutcome {
    /// 标签值。枚举是全部取值域，所以这些字符串也是。
    pub fn as_str(self) -> &'static str {
        match self {
            ChangeReworkOutcome::Submitted => "submitted",
            ChangeReworkOutcome::SubmitFailed => "submit_failed",
            ChangeReworkOutcome::NoExecutor => "no_executor",
        }
    }

    fn index(self) -> usize {
        match self {
            ChangeReworkOutcome::Submitted => 0,
            ChangeReworkOutcome::SubmitFailed => 1,
            ChangeReworkOutcome::NoExecutor => 2,
        }
    }
}

/// 家族名。生产者和读它的规则共用一个常量：各持一份副本就是改名之后规则还在
/// 选一条不存在的序列，而这正是这些规则要防的静默。
pub const CHANGE_REWORK_METRIC: &str = "self_evolution_change_rework_total";

/// 归宿所在的那一维。
pub const OUTCOME_LABEL: &str = "outcome";

/// 提交权与编排器句柄。
///
/// init 建（挂进引擎、发布读数）、init 里按属主门 `arm`、start 填编排器。
/// 与 `plugin.rs` 里的 `PoolGate` 同一个形状，理由也相同：跨插件消费
/// `OrchestratorControl` 只能在 `start()`，而消费它的触发点在 `init()` 之后就已
/// 经可能在跑。
#[derive(Default)]
pub struct ChangeReworkGate {
    /// 本进程是否承担提交职责。由 `self_evolution.executor_enabled` 决定。
    owns_submission: AtomicBool,
    orchestrator: Mutex<Option<Arc<dyn OrchestratorControl>>>,
    counts: [AtomicU64; REWORK_OUTCOMES.len()],
}

/// 一句判词，连同判词要点的那只手：这一刻能不能提交，能的话交给谁。
///
/// 句柄和判词从同一次持锁里取出来，所以「判词说可以、句柄却是空的」这件事
/// 在构造上就不存在——分两次问就会有那样一个窗口，而落在窗口里的那次提交会被
/// 静默丢掉。
pub enum Submission {
    /// 本进程不承担提交职责，这份需求由承担它的那个进程去提交。
    NotOwner,
    /// 该提交而槽是空的。这是要报出来的那一态。
    NoExecutor,
    /// 可以提交，编排器在这里。
    Ready(Arc<dyn OrchestratorControl>),
}

impl ChangeReworkGate {
    pub fn new() -> Self {
        Self::default()
    }

    /// 记为承担提交职责的进程。在 `init` 里按配置调用一次。
    pub fn arm(&self) {
        self.owns_submission.store(true, Ordering::Relaxed);
    }

    pub fn owns_submission(&self) -> bool {
        self.owns_submission.load(Ordering::Relaxed)
    }

    /// 填编排器。在 `start()` 里调用。填不进去就什么都不填——空槽本身是读数，
    /// 拿一个假的编排器把它盖过去正是这个模块要避免的。
    pub fn set_orchestrator(&self, orchestrator: Arc<dyn OrchestratorControl>) {
        if let Ok(mut slot) = self.orchestrator.lock() {
            *slot = Some(orchestrator);
        }
    }

    /// 此刻能不能提交，以及不能的话是哪一种不能。
    ///
    /// 两种「不能」的归宿不同：不承担职责是设计内的一态，不算丢；空槽是这条
    /// 支路没能把话说出去，要计数、要告警。
    pub fn submission(&self) -> Submission {
        if !self.owns_submission() {
            return Submission::NotOwner;
        }
        match self.orchestrator.lock() {
            Ok(slot) => match slot.as_ref() {
                Some(orch) => Submission::Ready(orch.clone()),
                None => Submission::NoExecutor,
            },
            // 锁中毒意味着另一个线程在持锁时 panic 了；把它当成空槽报出去，
            // 不让一次提交在这个分支上静默通过。
            Err(_) => Submission::NoExecutor,
        }
    }

    pub fn record(&self, outcome: ChangeReworkOutcome) {
        self.counts[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    pub fn count(&self, outcome: ChangeReworkOutcome) -> u64 {
        self.counts[outcome.index()].load(Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl Observable for ChangeReworkGate {
    /// 不承担提交的进程什么都不发。一个零会说「这里没丢过东西」，而真相是这个
    /// 进程根本没法丢——与「零是读数、缺席是没跑过」同一条规矩，方向相反：
    /// 这里的缺席是「这一格不归它管」，而承担职责的进程把那三格按零补全发布,
    /// 那样一格停下来才读得出来，而不是靠它消失。
    async fn collect_metrics(&self, _dimension: &str) -> SFResult<Vec<RawMetric>> {
        let mut out = Vec::new();
        if !self.owns_submission() {
            return Ok(out);
        }
        for outcome in REWORK_OUTCOMES {
            out.push(
                RawMetric::new(CHANGE_REWORK_METRIC, self.count(outcome) as f64)
                    .with_label(OUTCOME_LABEL, outcome.as_str()),
            );
        }
        Ok(out)
    }

    async fn collect_trace(&self, _task_id: &str) -> SFResult<Vec<TraceFragment>> {
        Ok(Vec::new())
    }

    /// 这条支路每进程一把门、回答一个问题，所以不声明维度：采集侧只抓一次。
    fn available_dimensions(&self) -> Vec<DimensionSpec> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cog_core::{SFResult, Task, UpstreamFailure};

    /// 只为把槽填上：这条支路对编排器做的唯一一件事是持有它，所以每个方法
    /// 都 `unimplemented!()` —— 一个会在测试里悄悄做事的替身，是让测试断言
    /// 到别的东西上去的第一步。
    struct SlotFiller;

    #[async_trait::async_trait]
    impl OrchestratorControl for SlotFiller {
        async fn submit_goal(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn submit_goal_auto(&self, _goal: &str, _tasks: Vec<Task>) -> SFResult<Vec<String>> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn assign_task(&self, _task_id: &str, _agent_id: &str) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn add_task(&self, _task: Task) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn crew_can_retry(&self, _task_ids: &[String]) -> bool {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn crew_retry_all(&self, _task_ids: &[String]) -> usize {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_ready_tasks(&self) -> Vec<Task> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_all_tasks(&self) -> Vec<Task> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn push_to_dlq(&self, _task_id: &str, _error: String) -> SFResult<bool> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn retry_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn dlq_len(&self) -> SFResult<usize> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn start_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn complete_task(
            &self,
            _task_id: &str,
            _result: serde_json::Value,
        ) -> SFResult<Vec<String>> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn fail_task(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<UpstreamFailure>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn fail_task_after(
            &self,
            _task_id: &str,
            _error: String,
            _cause: Option<UpstreamFailure>,
            _retry_after_secs: Option<u64>,
        ) -> SFResult<(bool, Vec<String>, bool)> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn cancel_task(&self, _task_id: &str) -> SFResult<Vec<String>> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_task(&self, _task_id: &str) -> Option<Task> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn schedule_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn check_timeouts(&self) -> Vec<(String, bool, Vec<String>, bool)> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_dependents(&self, _task_id: &str) -> Option<Vec<Task>> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_dependencies(&self, _task_id: &str) -> Option<Vec<Task>> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn get_graph(&self) -> (Vec<Task>, Vec<(String, String)>) {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn delete_task(&self, _task_id: &str) -> SFResult<()> {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn all_completed(&self) -> bool {
            unimplemented!("the gate only holds the orchestrator")
        }
        async fn replay_dlq(&self, _task_id: &str) -> SFResult<bool> {
            unimplemented!("the gate only holds the orchestrator")
        }
    }

    /// 一格一值，键就是标签值——读断言时不必再猜哪一条是哪一格。
    async fn cells(gate: &ChangeReworkGate) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = gate
            .collect_metrics("")
            .await
            .unwrap()
            .into_iter()
            .map(|m| {
                (
                    m.labels.get(OUTCOME_LABEL).cloned().unwrap_or_default(),
                    m.value,
                )
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// 不承担提交的进程不发这个家族：它的缺席说的是「不归它管」，不是零。
    #[tokio::test]
    async fn a_process_that_does_not_submit_publishes_nothing() {
        let gate = ChangeReworkGate::new();
        assert!(matches!(gate.submission(), Submission::NotOwner));
        assert!(gate.collect_metrics("").await.unwrap().is_empty());
    }

    /// 承担职责但槽还空着：判词是空槽，且三格按零补全发布——一格停下来要
    /// 读得出来，而不是靠它从抓取里消失。
    #[tokio::test]
    async fn an_armed_gate_without_an_orchestrator_says_so_and_still_publishes_the_cells() {
        let gate = ChangeReworkGate::new();
        gate.arm();
        assert!(matches!(gate.submission(), Submission::NoExecutor));

        let cells = cells(&gate).await;
        assert_eq!(cells.len(), REWORK_OUTCOMES.len());
        for (outcome, value) in &cells {
            assert_eq!(
                *value, 0.0,
                "{outcome} has no trigger yet and must read zero"
            );
        }
    }

    #[tokio::test]
    async fn filling_the_slot_turns_the_verdict_into_ready() {
        let gate = ChangeReworkGate::new();
        gate.arm();
        gate.set_orchestrator(Arc::new(SlotFiller));
        assert!(matches!(gate.submission(), Submission::Ready(_)));
    }

    /// 三格各计各的：一次提交、两次空槽，读数必须出现且是三个互不相同的数。
    #[tokio::test]
    async fn each_outcome_is_counted_under_its_own_cell() {
        let gate = ChangeReworkGate::new();
        gate.arm();
        gate.record(ChangeReworkOutcome::Submitted);
        gate.record(ChangeReworkOutcome::NoExecutor);
        gate.record(ChangeReworkOutcome::NoExecutor);

        assert_eq!(gate.count(ChangeReworkOutcome::Submitted), 1);
        assert_eq!(gate.count(ChangeReworkOutcome::SubmitFailed), 0);
        assert_eq!(gate.count(ChangeReworkOutcome::NoExecutor), 2);

        let published: Vec<(String, f64)> = cells(&gate)
            .await
            .into_iter()
            .filter(|(_, value)| *value > 0.0)
            .collect();
        assert_eq!(
            published,
            vec![
                ("no_executor".to_string(), 2.0),
                ("submitted".to_string(), 1.0)
            ]
        );
    }

    /// 标签值就是枚举的拼写，闭集和发布走同一张表。
    #[test]
    fn the_label_values_are_the_closed_set_and_stay_distinct() {
        let mut seen: Vec<&str> = REWORK_OUTCOMES.iter().map(|o| o.as_str()).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), REWORK_OUTCOMES.len());
        for outcome in REWORK_OUTCOMES {
            assert!(!outcome.as_str().is_empty());
        }
    }
}
