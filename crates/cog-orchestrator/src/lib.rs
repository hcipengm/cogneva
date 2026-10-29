pub mod action_planner;
pub mod backup_runs;
pub mod change_ledger;
pub mod change_measure;
pub mod dag_executor;
pub mod face_drift;
pub mod face_lifecycle;
pub mod face_staffing;
pub mod governance_acts;
pub mod observable;
pub mod orchestrator_control_impl;
pub mod plugin;
pub mod principle_ledger;
pub mod principle_measure;
pub mod ready_queue;
pub mod replan;
pub mod resource_ledger;
pub mod sop;
pub mod subscribe_backoff;
pub mod task_executor_router;
pub mod wbs;

/// The label that names which reading a sample is.
///
/// The ledgers in this crate publish several readings under one metric name and
/// tell them apart only by this label; a reader selects one reading out of the
/// shared store by it. `cog-observability` and `cog-reflection` write the same
/// label, so it is declared once per crate under the same identifier and a gate
/// compares the spellings -- one edited copy would publish a family that every
/// existing selector reads as empty, which is indistinguishable from that family
/// never having run.
pub const LABEL_READOUT: &str = "readout";

/// The label naming how a reading ended.
///
/// Several families here carry it, and the readers that count one ending
/// select by it. The name is declared once per crate that writes it, under the
/// same identifier, and a gate compares the spellings: one edited copy
/// publishes a family under a name every existing selector reads as empty,
/// which is what "this family never ran" looks like.
pub const LABEL_OUTCOME: &str = "outcome";

/// 读数按「哪一类」归属时用的标签键。
pub const LABEL_KIND: &str = "kind";

/// 读数按「哪一行读数」归属时用的标签键。
pub const LABEL_READING: &str = "reading";

/// 读数带上「为什么是这个值」时用的标签键。
pub const LABEL_REASON: &str = "reason";

/// 读数按「这一格处在哪个状态」归属时用的标签键。
pub const LABEL_STATE: &str = "state";

/// 同一个问题被分两批问时（未结清的行、已落地的行），按「哪一批」归属的标签键。
///
/// 两批用同一个键而不是两个序列名：问的是同一件事、失败的理由是同一组，分开放的
/// 只是一次 pass 里两段不同的扫描。合成一个键之后，读者既能按批选行，也能一次
/// 看到全部。
pub const LABEL_HALF: &str = "half";

/// 读数按「这条原则说它该怎么被查」归属时用的标签键。
///
/// 与 [`LABEL_STATE`] 是同一总体的两个分区，各用各的键而不是挤在一个键的不同取值里：
/// 两个分区相加会把语料算两遍，而读者按一个键选行时，两族共键就分不开谁是谁。
pub const LABEL_CHECK_KIND: &str = "check_kind";

/// 读数按「哪条流」归属时用的标签键。
pub const LABEL_STREAM: &str = "stream";

/// 读数按「哪种结局」归属时用的标签键。
pub const LABEL_STATUS: &str = "status";

/// 读数按「哪个面」归属时用的标签键。
pub const LABEL_FACE: &str = "face";

/// 读数按「最初那一路输入」归属时用的标签键。
pub const LABEL_ORIGINAL_INPUT: &str = "original_input";

/// 读数按「哪个 agent」归属时用的标签键。
pub const LABEL_AGENT: &str = "agent";

/// 读数带上「为什么是这个结局」时用的标签键。
pub const LABEL_CAUSE: &str = "cause";

/// 读数按「哪个决定」归属时用的标签键。
pub const LABEL_DECISION: &str = "decision";

/// 读数按「哪一行池子读数」归属时用的标签键。
pub const LABEL_POOL_READING: &str = "pool_reading";

/// 读数按「哪一块」归属时用的标签键。
pub const LABEL_BLOCK: &str = "block";

/// 读数按「哪个项目」归属时用的标签键。
pub const LABEL_PROJECT: &str = "project";

/// 读数按「哪一列」归属时用的标签键。
pub const LABEL_COLUMN: &str = "column";

/// 读数按「这一格的结果」归属时用的标签键。
pub const LABEL_RESULT: &str = "result";

pub use action_planner::ActionPlanOrchestrator;
pub use dag_executor::task_phase::{ExitCriteria, PhaseTransitionRules, PhasedTask, TaskPhase};
pub use dag_executor::{
    checkpoint_task_with_timeout, BackoffStrategy, CheckpointAgents, CheckpointRound,
    CircuitBreakerConfig, CircuitBreakerRegistry, DagExecutor, DagExecutorConfig,
    DagExecutorRuntime, LiveCheckpointAgents, RetryConfig, RetryMatrix, CHECKPOINT_STEP_TIMEOUT,
};
pub use observable::OrchestratorObservable;
pub use orchestrator_control_impl::OrchestratorControlImpl;
pub use task_executor_router::TaskExecutorRouter;

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;

/// 单测共用的样例：把几行「路径 ＋ 文本」解析成一份声明账。
#[cfg(test)]
pub(crate) mod test_support;
