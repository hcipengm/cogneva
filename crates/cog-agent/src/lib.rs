pub mod agent;
pub mod agent_kernel;
pub mod archive;
pub mod background;
pub mod config;
pub mod consumer;
pub mod context;
pub mod event_bus_sink;
pub mod hooks;
pub mod identity;
pub mod lifecycle;
pub mod observable;
pub mod permission;
pub mod pool_sizing;
pub mod principles;
pub mod runtime;
pub mod tools;
pub mod wal;
pub mod web;

/// The label naming how a reading ended.
///
/// Every family here with more than one ending carries this label, and the
/// readers that count one ending select by it. It is declared once per crate
/// that writes it, under the same identifier, and a gate compares the
/// spellings: one edited copy publishes a family under a name every existing
/// selector reads as empty, which is what "this family never ran" looks like.
pub const LABEL_OUTCOME: &str = "outcome";

/// 读数按「哪一类」归属时用的标签键。
pub const LABEL_KIND: &str = "kind";

/// 读数带上「为什么是这个值」时用的标签键。
pub const LABEL_REASON: &str = "reason";

/// 读数按「这一格处在哪个状态」归属时用的标签键。
pub const LABEL_STATE: &str = "state";

/// 读数按「哪位角色」归属时用的标签键。
pub const LABEL_ROLE: &str = "role";

/// 读数按「这份预算活多久」归属时用的标签键。
pub const LABEL_LIFETIME: &str = "lifetime";

/// 读数带上「为什么是这个结局」时用的标签键。
pub const LABEL_CAUSE: &str = "cause";

/// 读数按「哪个工具」归属时用的标签键。
pub const LABEL_TOOL: &str = "tool";

/// 读数按「这件活的声明来自哪一类」归属时用的标签键。
///
/// 与 [`LABEL_ROLE`] 是两个问题：角色是名册上的一行，声明是活自己的话。同一个
/// 人接不同活时，前者不变、后者该变——这正是「待遇跟活走，不跟名字走」在读面上
/// 的样子。
pub const LABEL_DECLARATION: &str = "declaration";

/// 读数按「这个动作有没有外部效果」归属时用的标签键。
///
/// 与 [`LABEL_CAUSE`] 是两个问题：成因说这次为什么发生，这一格说这次重放要付
/// 什么代价。续跑对三类处置不同，所以计数必须按它分开。
pub const LABEL_EFFECT: &str = "effect";

/// 只读：重放它除了再花一次调用，不改变任何东西。
pub const LABEL_EFFECT_READ: &str = "read";

/// 写文件：重放是幂等的，前提是先判「它是不是已经落过了」。
pub const LABEL_EFFECT_WRITE: &str = "write";

/// 有外部效果：绝不盲目重放，交给模型去核对。
pub const LABEL_EFFECT_EXTERNAL: &str = "external";

/// 一次尝试读到了可续的日志：它从被中断的那一段接着跑。
pub const ATTEMPT_RESUMED: &str = "resumed";

/// 一次尝试在日志里没有可续的一段：它从第一轮开始。
pub const ATTEMPT_FRESH: &str = "fresh";

/// 读数按「这个数是谁给的」归属时用的标签键。
///
/// 目前只有轮次上限用它：钉死的数、实测给出、与两边都没有（无上限）是三种
/// 读数上必须分得开的情形。
pub const LABEL_CEILING_SOURCE: &str = "source";

pub use archive::{HttpToolOutputArchive, ToolOutputArchive};
pub use config::{AgentLoopConfig, AgentManagerConfig};

pub use agent::Agent;
pub use consumer::AgentConsumer;
pub use event_bus_sink::EventBusSink;

// Re-export the canonical AgentState from cog-core so consumers see a single type.
pub use agent_kernel::{AgentHooks, AgentRuntime, ReActLoop, ReActStep, RuntimeState, RuntimeStep};

/// 把进程的提示词本子交给运行时：每次运行开头那段通用指示从它这里取。
pub use agent_kernel::base_instruction::install as install_prompt_provider;

/// 把进程的规则账交给运行时：每一轮开头生效的那几条规则从它这里取。
pub use principles::install as install_principle_source;

/// Open agent-role identifier (not an enum) so new roles can be added
/// without recompiling `cog-core`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct AgentRole(pub String);

impl AgentRole {
    // 名字声明在 `cog_core::roles`；这里只是给本 crate 的老调用点留的别名，
    // 多了 MERGER 一个，因为角色表是开放的、常被按名字取用。
    pub const PLANNER: &'static str = cog_core::roles::PLANNER;
    pub const GENERATOR: &'static str = cog_core::roles::GENERATOR;
    pub const EVALUATOR: &'static str = cog_core::roles::EVALUATOR;
    pub const MODERATOR: &'static str = cog_core::roles::MODERATOR;
    pub const MERGER: &'static str = cog_core::roles::MERGER;
}

impl std::fmt::Display for AgentRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for AgentRole {
    type Err = std::convert::Infallible;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl From<String> for AgentRole {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for AgentRole {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}
pub use context::ContextWindow;
pub use hooks::{
    apply_runtime_overrides, load_and_apply, DefaultHookPublisher, HookConfig, HookEngine,
    HookEngineConfig, HookHandler, HookPublisher, HookType, LifecycleHookEngine,
    LifecycleHookEvent, TieredHookPublisher, DEFAULT_LIFECYCLE_CHANNEL_BUFFER,
};
pub use lifecycle::{LifecycleManager, StateTransitionHook};
pub use observable::AgentObservable;
pub use runtime::{GlobalAgentManager, WorkerHandle};
pub use tools::builtins;
pub use tools::ToolRegistry;
pub use wal::AgentWal;

pub mod drain;
pub mod plugin;
pub mod self_review;
pub use self_review::SelfReviewLoop;

/// 这棵树的根：一份算法，全仓共用。
///
/// `tests/` 之外，`src/` 里的单测也要问「这棵树在哪」。声明在这里而不是每个单测
/// 模块各写一条 `#[path]`：`#[path]` 的基准目录会随所在内联模块的嵌套层数变化，
/// 挪一次模块就得改一次路径；crate 根这一处的基准固定是 `src/`。
#[cfg(test)]
#[path = "../../cogneva/tests/common/root.rs"]
pub(crate) mod checkout;

/// 测试用的记账后端：把每一次写入按「序列名、标签、数值」记下来。
///
/// 放在这里而不是各测试模块各写一份：本 crate 有三处要判「写的是哪条序列、
/// 带什么标签」，三份拷贝会让一个改了标签的写入在另外两处仍然绿着。
#[cfg(test)]
pub mod recording_metrics {
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// 一次写入。
    pub type Recorded = (String, HashMap<String, String>, f64);

    /// 记下每一次写入的后端。
    #[derive(Default)]
    pub struct RecordingMetrics(pub Mutex<Vec<Recorded>>);

    impl RecordingMetrics {
        /// 写给某条序列的全部行。
        pub fn rows_for(&self, name: cog_core::MetricName) -> Vec<Recorded> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|(n, _, _)| n == name.as_str())
                .cloned()
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl cog_core::MetricsBackend for RecordingMetrics {
        async fn record_gauge(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.0
                .lock()
                .unwrap()
                .push((name.to_string(), labels, value));
            Ok(())
        }
        async fn record_counter(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.0
                .lock()
                .unwrap()
                .push((name.to_string(), labels, value));
            Ok(())
        }
        async fn record_histogram(
            &self,
            name: cog_core::MetricName,
            value: f64,
            labels: HashMap<String, String>,
        ) -> cog_core::SFResult<()> {
            self.0
                .lock()
                .unwrap()
                .push((name.to_string(), labels, value));
            Ok(())
        }
        async fn query_gauge_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_gauge_latest(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_counter_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn query_histogram_totals(
            &self,
            _name: &str,
        ) -> cog_core::SFResult<Vec<cog_core::HistogramTotals>> {
            Ok(Vec::new())
        }
        async fn query_histogram_range(
            &self,
            _name: &str,
            _start: chrono::DateTime<chrono::Utc>,
            _end: chrono::DateTime<chrono::Utc>,
        ) -> cog_core::SFResult<Vec<cog_core::MetricSample>> {
            Ok(Vec::new())
        }
        async fn list_metric_names(
            &self,
            _metric_type: cog_core::MetricType,
        ) -> cog_core::SFResult<Vec<String>> {
            Ok(Vec::new())
        }
        async fn health_check(&self) -> cog_core::SFResult<()> {
            Ok(())
        }
    }
}
