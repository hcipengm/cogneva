pub mod agent;
pub mod agent_kernel;
pub mod consumer;
pub mod context;
pub mod hooks;
pub mod lifecycle;
pub mod observable;
pub mod runtime;
pub mod tools;
pub mod wal;
pub mod working_memory;
pub mod worktree;

pub use agent::Agent;
pub use consumer::{AgentInboxConsumer, InboxMessage};

// Re-export the canonical AgentState from cog-core so consumers see a single type.
pub use agent_kernel::{AgentHooks, AgentRuntime, ReActLoop, ReActStep, RuntimeState, RuntimeStep};

/// Open agent-role identifier (not an enum) so new roles can be added
/// without recompiling `cog-core`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct AgentRole(pub String);

impl AgentRole {
    pub const PLANNER: &'static str = "planner";
    pub const GENERATOR: &'static str = "generator";
    pub const EVALUATOR: &'static str = "evaluator";
    pub const MODERATOR: &'static str = "moderator";
    pub const MODE_SELECTOR: &'static str = "mode_selector";
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
