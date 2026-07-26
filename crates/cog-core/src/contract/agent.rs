//!Pure-trait control-plane contracts for Agent <-> Supervisor communication.
//!Zero `tonic`/`prost` references — concrete gRPC implementations live in
//!`cog-protocol` so that `cog-supervisor` and `cog-agent` depend only on
//!`cog-core`.

use crate::types::AgentEvent;
use crate::{InboxMessage, SFResult};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};

/// Trait for an agent runtime that can execute a single run.
/// This abstraction lives in `cog-core` so that `cog-eval` and other
/// downstream crates can depend on the interface rather than the concrete
/// `cog-agent` crate.
#[async_trait]
pub trait AgentRuntime: Send + Sync {
    async fn run(
        &mut self,
        input: serde_json::Value,
        llm: &dyn crate::llm::LlmClient,
    ) -> crate::SFResult<serde_json::Value>;

    /// Return the agent identifier assigned to this runtime.
    fn agent_id(&self) -> &str;

    /// Return the role assigned to this runtime.
    fn role(&self) -> &str;
}

/// Agent loop configuration.
#[derive(Clone)]
pub struct RuntimeConfig {
    pub agent_id: String,
    /// Agent role as an open string (e.g. "planner", "generator").
    /// Constants are defined in `cog-agent::AgentRole` so new roles can be
    /// added without recompiling `cog-core`.
    pub role: String,
    pub max_iterations: u32,
    pub context_window_size: usize,
    /// TTL for the available_skills cache in seconds.
    pub skill_cache_ttl_secs: u64,
    /// Optional dynamic skill configuration that overrides role defaults.
    pub skill_config: Option<crate::SkillConfig>,
    /// Optional Crew identifier — attached to lifecycle events so the hook
    /// engine can route them to Crew scope.
    pub crew_id: Option<String>,
    /// Optional Squad identifier — attached to lifecycle events so the hook
    /// engine can route them to Squad scope.
    pub squad_id: Option<String>,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            agent_id: "agent".into(),
            role: "planner".into(),
            max_iterations: 10,
            context_window_size: 4000,
            skill_cache_ttl_secs: 30,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        }
    }
}

impl From<crate::config::AgentLoopConfig> for RuntimeConfig {
    fn from(c: crate::config::AgentLoopConfig) -> Self {
        Self {
            agent_id: c.agent_id,
            role: c.role,
            max_iterations: c.max_iterations,
            context_window_size: c.context_window_size,
            skill_cache_ttl_secs: c.skill_cache_ttl_secs,
            skill_config: None,
            crew_id: None,
            squad_id: None,
        }
    }
}

/// High-level agent trait — abstracts the concrete [`cog_agent::Agent`] so
/// that downstream crates (e.g. `cog-collaboration`) can create and drive
/// agents without depending on `cog-agent`.
#[async_trait]
pub trait Agent: Send + Sync {
    async fn prompt(&self, input: serde_json::Value) -> crate::SFResult<serde_json::Value>;
    async fn start(&self);
    async fn snapshot(&self, task_id: String) -> crate::SFResult<crate::snapshot::AgentCheckpoint>;
    async fn restore(&self, snapshot: &crate::snapshot::AgentCheckpoint) -> crate::SFResult<()>;

    /// Continue the conversation with additional input. Context is preserved
    /// across calls.
    async fn continue_(&self, input: serde_json::Value) -> crate::SFResult<serde_json::Value>;

    /// Send a steering instruction (injected as a system message).
    async fn steer(&self, instruction: String) -> crate::SFResult<()>;

    /// Abort the current run and reset to idle.
    async fn abort(&self) -> crate::SFResult<()>;

    /// Reset the agent, clearing all context and state.
    async fn reset(&self) -> crate::SFResult<()>;

    /// Get the current agent state.
    async fn state(&self) -> crate::SFResult<crate::AgentState>;

    /// Wait until the agent is no longer active or completing.
    async fn wait_for_idle(&self) -> crate::SFResult<()>;

    /// Restore agent state from a persisted checkpoint by id.
    async fn restore_from_id(&self, checkpoint_id: &str) -> crate::SFResult<()>;

    /// Subscribe to agent lifecycle events.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::AgentEvent>;

    /// Direct streaming access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    async fn chat_stream(
        &self,
        messages: &[crate::Message],
        options: &crate::ChatOptions,
    ) -> crate::SFResult<crate::AssistantMessageEventStream>;

    /// Direct streaming completion access to the underlying LLM provider.
    /// Bypasses AgentRuntime, tool execution, and state management.
    async fn complete_stream(
        &self,
        prompt: &str,
        options: &crate::CompleteOptions,
    ) -> crate::SFResult<crate::AssistantMessageEventStream>;

    /// Read a field from the shared ContextBoard for the given task.
    async fn read_board(&self, task_id: &str, field: &str) -> crate::SFResult<Option<String>>;

    /// Write a field to the shared ContextBoard for the given task.
    async fn write_board(&self, task_id: &str, field: &str, value: &str) -> crate::SFResult<()>;

    /// Receive an external inbox message directed at this agent.
    /// The agent delivers the message to its configured message backend so that
    /// the inbox consumer can pick it up.
    async fn receive_message(&self, msg: InboxMessage) -> crate::SFResult<()>;

    /// Review an output string via the agent's self-review capability.
    /// Returns a [`SelfReviewResult`] indicating pass or need-revision.
    /// Default: always pass.  Concrete implementations in `cog-agent` override
    /// this with the full SelfReviewLoop logic.
    async fn review_output(
        &self,
        _output: &str,
        _config: &crate::SelfReviewConfig,
    ) -> crate::SFResult<crate::SelfReviewResult> {
        Ok(crate::SelfReviewResult::Pass {
            score: 1.0,
            summary: "default pass".into(),
        })
    }

    /// Review an output and return the (possibly revised) text together with
    /// the review result. Default: delegates to [`Self::review_output`] and
    /// returns the original output unchanged. Concrete implementations in
    /// `cog-agent` override this to surface the SelfReviewLoop revision.
    async fn review_and_revise(
        &self,
        output: &str,
        config: &crate::SelfReviewConfig,
    ) -> crate::SFResult<(String, crate::SelfReviewResult)> {
        let result = self.review_output(output, config).await?;
        Ok((output.to_string(), result))
    }
}

/// Commands that the Supervisor may send to an Agent.
#[derive(Debug, Clone)]
pub enum AgentCommand {
    Kill { reason: String },
    Restart { preserve_context: bool },
    Checkpoint { task_id: String },
    ConfigUpdate { config_json: Vec<u8> },
}

/// Client-side trait — implemented by `cog-protocol` and consumed by `cog-agent`.
#[async_trait]
pub trait AgentLifecycleClient: Send + Sync {
    /// Send a unary heartbeat to the Supervisor.
    async fn heartbeat(&self, agent_id: &str, state: &str) -> SFResult<()>;

    /// Open a Server-Streaming RPC that yields commands from the Supervisor.
    async fn subscribe_commands(
        &self,
        agent_id: &str,
    ) -> SFResult<BoxStream<'static, AgentCommand>>;

    /// Report a single low-frequency critical event (AgentError, ResourceAlert,
    /// TaskStatusChange, StateChange, etc.) via Unary RPC.
    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()>;

    /// Upload a batch of high-frequency events (MessageUpdate, ReAct steps,
    /// tool execution progress, etc.) via Client Streaming RPC.
    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32>;
}

/// Server-side trait — implemented by `cog-protocol` and consumed by `cog-supervisor`.
#[async_trait]
pub trait AgentLifecycleServer: Send + Sync {
    /// Push a command to the specified agent.
    async fn push_command(&self, agent_id: &str, command: AgentCommand) -> SFResult<()>;

    /// Convenience: ask an agent to kill itself.
    async fn kill(&self, agent_id: &str, reason: &str) -> SFResult<bool> {
        self.push_command(
            agent_id,
            AgentCommand::Kill {
                reason: reason.into(),
            },
        )
        .await?;
        Ok(true)
    }

    /// Convenience: ask an agent to restart.
    async fn restart(&self, agent_id: &str, preserve_context: bool) -> SFResult<bool> {
        self.push_command(agent_id, AgentCommand::Restart { preserve_context })
            .await?;
        Ok(true)
    }

    /// Convenience: ask an agent to checkpoint.
    async fn checkpoint(&self, agent_id: &str, task_id: &str) -> SFResult<String> {
        self.push_command(
            agent_id,
            AgentCommand::Checkpoint {
                task_id: task_id.into(),
            },
        )
        .await?;
        Ok(task_id.into())
    }

    /// List currently connected agent IDs.
    async fn connected_agents(&self) -> SFResult<Vec<String>>;

    /// Receive a single critical event reported by an agent via Unary RPC.
    async fn report_event(&self, agent_id: &str, event: &AgentEvent) -> SFResult<()>;

    /// Receive a batch of high-frequency events from an agent via Client Streaming RPC.
    /// Returns the number of events accepted.
    async fn upload_events(&self, agent_id: &str, events: Vec<AgentEvent>) -> SFResult<u32>;
}

// Agent registration & heartbeat protocol.
// deterministic agent_id (blake3 of hostname + pod_ip + role + uuid_v7),
// Redis TTL registration with heartbeat renewal, and a stable
// [`AgentRegistry`] trait that can be backed by Redis or memory.

/// Resource hints attached to a registration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ResourceInfo {
    pub cpu_cores: u32,
    pub memory_gb: u32,
}

/// Registration payload submitted by an Agent on startup and persisted in Redis.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AgentRegistration {
    pub agent_id: String,
    pub role: String,
    pub workspace_id: String,
    pub capabilities: Vec<String>,
    pub resources: ResourceInfo,
    pub hostname: String,
    pub pod_ip: String,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

impl AgentRegistration {
    /// Build a registration with an externally-supplied `agent_id`.
    /// Callers (e.g. `cog-storage` or the binary crate) are responsible for
    /// generating the id so that `cog-core` stays free of hashing logic.
    pub fn new(
        agent_id: impl Into<String>,
        hostname: impl Into<String>,
        pod_ip: impl Into<String>,
        role: impl Into<String>,
        workspace_id: impl Into<String>,
        capabilities: Vec<String>,
        resources: ResourceInfo,
    ) -> Self {
        let now = Utc::now();
        Self {
            agent_id: agent_id.into(),
            role: role.into(),
            workspace_id: workspace_id.into(),
            capabilities,
            resources,
            hostname: hostname.into(),
            pod_ip: pod_ip.into(),
            registered_at: now,
            last_heartbeat: now,
        }
    }
}

/// Registry abstraction so callers can plug Redis (production) or in-memory (tests).
#[async_trait]
pub trait AgentRegistry: Send + Sync {
    async fn register(&self, registration: &AgentRegistration) -> SFResult<()>;
    async fn heartbeat(&self, agent_id: &str) -> SFResult<()>;
    async fn deregister(&self, agent_id: &str) -> SFResult<()>;
    async fn get(&self, agent_id: &str) -> SFResult<Option<AgentRegistration>>;
    async fn list(&self) -> SFResult<Vec<AgentRegistration>>;
    /// List agents filtered by role.
    async fn list_by_role(&self, role: &str) -> SFResult<Vec<AgentRegistration>>;
    /// List agents that have all the given capabilities.
    async fn list_by_capability(&self, capability: &str) -> SFResult<Vec<AgentRegistration>>;
}

// ─── Agent Manager ─────────────────────────────────────────────────────────

/// Lightweight snapshot of a worker agent managed by the agent manager.
/// Does not contain the concrete [`Agent`] handle so that `cog-core`
/// remains decoupled from `cog-agent` internals.
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub agent_id: String,
    pub role: String,
    pub capabilities: Vec<String>,
}

/// Agent manager for creating, discovering, and communicating with agent instances.
/// Serves as the service-level gateway to object-level [`Agent`] capabilities.
#[async_trait]
pub trait AgentManager: Send + Sync {
    /// Create and start a new agent worker with the given role and LLM provider.
    /// Returns a shareable handle to the object-level [`Agent`] trait.
    async fn create_agent(
        &self,
        agent_id: &str,
        role: &str,
        llm: std::sync::Arc<dyn crate::llm::LlmClient>,
    ) -> crate::SFResult<std::sync::Arc<dyn crate::Agent>>;

    /// Dispatch a message using round-robin selection among managed agents.
    async fn dispatch(&self, msg: InboxMessage) -> crate::SFResult<()>;

    /// Return a snapshot of currently live managed agents.
    async fn list_workers(&self) -> crate::SFResult<Vec<WorkerInfo>>;

    /// Gracefully shutdown all managed agents: abort and deregister.
    async fn shutdown(&self) -> crate::SFResult<()>;

    /// Get a managed agent instance by its `agent_id`.
    /// Returns `Arc<dyn Agent>` so downstream can invoke the full object-level
    /// [`Agent`] trait (prompt, steer, snapshot, etc.).
    async fn get_agent(
        &self,
        agent_id: &str,
    ) -> crate::SFResult<Option<std::sync::Arc<dyn crate::Agent>>>;
}

/// Generate a deterministic agent_id from the four input dimensions.
pub fn generate_agent_id(hostname: &str, pod_ip: &str, role: &str, uuid: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(hostname.as_bytes());
    hasher.update(b"|");
    hasher.update(pod_ip.as_bytes());
    hasher.update(b"|");
    hasher.update(role.as_bytes());
    hasher.update(b"|");
    hasher.update(uuid.as_bytes());
    let hash = hasher.finalize();
    hash.to_hex().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_registration_construction() {
        let reg = AgentRegistration::new(
            "id-1",
            "host-1",
            "10.0.0.1",
            "planner",
            "ws-1",
            vec!["llm".into()],
            ResourceInfo::default(),
        );
        assert_eq!(reg.agent_id, "id-1");
        assert_eq!(reg.hostname, "host-1");
        assert_eq!(reg.role, "planner");
    }
}
