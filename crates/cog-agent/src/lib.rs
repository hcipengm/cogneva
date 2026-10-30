pub mod agent;
pub mod agent_kernel;
pub mod archive;
pub mod config;
pub mod consumer;
pub mod context;
pub mod event_bus_sink;
pub mod hooks;
pub mod identity;
pub mod lifecycle;
pub mod observable;
pub mod pool_sizing;
pub mod runtime;
pub mod tools;
pub mod wal;
pub mod web;
pub mod worktree;

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

pub use archive::{HttpToolOutputArchive, ToolOutputArchive};
pub use config::{AgentLoopConfig, AgentManagerConfig};

pub use agent::Agent;
pub use consumer::AgentConsumer;
pub use event_bus_sink::EventBusSink;

// Re-export the canonical AgentState from cog-core so consumers see a single type.
pub use agent_kernel::{AgentHooks, AgentRuntime, ReActLoop, ReActStep, RuntimeState, RuntimeStep};

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
pub use lifecycle::{HeartbeatConfig, LifecycleManager, StateTransitionHook};
pub use observable::AgentObservable;
pub use runtime::{GlobalAgentManager, WorkerHandle};
pub use tools::builtins;
pub use tools::ToolRegistry;
pub use wal::AgentWal;

pub mod plugin;
pub mod self_review;
pub use self_review::SelfReviewLoop;
