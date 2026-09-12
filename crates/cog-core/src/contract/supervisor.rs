//!Core supervisor types shared across crates.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Agent's instantaneous self-assessment in a heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatStatus {
    /// Agent is processing tasks normally.
    #[default]
    Healthy,
    /// Agent is alive but reporting elevated load or partial errors.
    Degraded,
    /// Agent is alive but unable to make progress.
    Unhealthy,
}

/// One Agent's most recent heartbeat snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeartbeatRecord {
    pub agent_id: String,
    pub timestamp: DateTime<Utc>,
    pub status: HeartbeatStatus,
    /// Self-reported load, normalised to `0.0` (idle) – `1.0` (saturated).
    pub load_score: f32,
    /// Number of tasks currently owned by the Agent.
    pub task_count: u32,
}

/// Snapshot of a crew (squad-level execution group) tracked by the supervisor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrewSummary {
    pub crew_id: String,
    pub agent_ids: Vec<String>,
    pub task_ids: Vec<String>,
    /// Crew-level retries already attempted (capped by the supervisor).
    pub crew_retry_count: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Registry that tracks heartbeat history for agents.
pub trait HeartbeatRegistry: Send + Sync {
    /// Get the full heartbeat history for an agent.
    fn get_heartbeat_history(&self, agent_id: &str) -> Vec<HeartbeatRecord>;

    /// List every crew currently tracked by the supervisor.
    /// Default empty so test doubles only stub what they use.
    fn list_crews(&self) -> Vec<CrewSummary> {
        Vec::new()
    }
}

/// Health report returned by a supervisor health pass.
#[derive(Debug, Clone, Default)]
pub struct HealthReport {
    pub healthy: Vec<String>,
    pub dead: Vec<String>,
    pub suspect: Vec<String>,
    pub stuck: Vec<String>,
}

/// Top-level supervisor trait — abstracts the cog-supervisor implementation.
#[async_trait::async_trait]
pub trait Supervisor: Send + Sync {
    /// Run a health pass and return the report.
    async fn run_health_pass(&self) -> crate::SFResult<HealthReport>;

    /// Return the scheduler gate.
    fn gate(&self) -> Arc<dyn crate::SchedulerGate>;

    /// Number of pending autonomous handoffs.
    async fn autonomous_pending_count(&self) -> usize;

    /// Number of autonomous retries.
    async fn autonomous_retry_count(&self) -> usize;

    /// Length of the dead-letter queue.
    async fn orchestrator_dlq_len(&self) -> crate::SFResult<usize>;

    /// Optional control-plane URL.
    fn control_plane_url(&self) -> Option<String>;

    /// Subscribe to the Supervisor's event broadcast channel.
    fn subscribe(&self) -> tokio::sync::broadcast::Receiver<crate::SupervisorEvent>;

    /// Fetch a clone of the broadcast sender.
    fn event_sender(&self) -> tokio::sync::broadcast::Sender<crate::SupervisorEvent>;

    /// Send a kill command to the target agent.
    /// Returns true if the agent was found and the command was dispatched.
    async fn kill_agent(&self, agent_id: &str, reason: &str) -> crate::SFResult<bool>;

    /// Send a restart command to the target agent.
    /// Returns true if the agent was found and the command was dispatched.
    async fn restart_agent(&self, agent_id: &str, preserve_context: bool) -> crate::SFResult<bool>;

    /// Request a checkpoint for the target agent and task.
    /// Returns the checkpoint ID on success.
    async fn checkpoint_agent(&self, agent_id: &str, task_id: &str) -> crate::SFResult<String>;
}

// ─── Binary Switcher ───────────────────────────────────────────────────────

/// Strategy for atomically swapping the running `cogneva` binary during
/// self-evolution deployment. Implemented by `cog-supervisor` and consumed
/// by `cog-reflection` through the core contract.
#[async_trait::async_trait]
pub trait BinarySwitcher: Send + Sync {
    /// Copy the freshly-built binary into the staging area used by this switcher.
    async fn stage_new_binary(&self, new_binary_path: &std::path::Path) -> crate::SFResult<()>;

    /// Activate the staged binary and restart the service/process.
    async fn switch_and_restart(&self) -> crate::SFResult<()>;

    /// Restore the previous binary and restart.
    async fn rollback(&self) -> crate::SFResult<()>;
}

// ─── Scheduler Gate ────────────────────────────────────────────────────────

/// Cooperative task classification for pause gating.
///
/// An unavailable LLM upstream pool stops only the work that genuinely needs
/// an LLM; mechanical work (builds, image publishing, CI/mainline deployment,
/// metric collection, health checks, baseline rebase/merge) must keep running.
/// Pausing everything would stall deployment and collection that can make
/// progress without an LLM.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskClass {
    /// Requires a reachable LLM upstream: agent planner/generator/evaluator,
    /// goal decomposition, intent assessment, reflection generation, chat.
    LlmDependent,
    /// Makes progress without any LLM call.
    Mechanical,
}

/// Pause signal for the autonomous scheduler.
///
/// The global [`pause`](SchedulerGate::pause) / [`resume`](SchedulerGate::resume)
/// pair is the coarse operator switch; the per-class
/// [`pause_kind`](SchedulerGate::pause_kind) family lets one task class pause
/// without stopping the others (used when the LLM upstream pool goes down).
pub trait SchedulerGate: Send + Sync {
    /// Returns `true` while the scheduler is paused.
    fn is_paused(&self) -> bool;

    /// Pause the scheduler. Returns the previous state.
    fn pause(&self) -> bool;

    /// Resume the scheduler. Returns the previous state.
    fn resume(&self) -> bool;

    /// Returns `true` while the given task class is paused.
    fn is_paused_kind(&self, class: TaskClass) -> bool;

    /// Pause one task class. Returns the previous state.
    fn pause_kind(&self, class: TaskClass) -> bool;

    /// Resume one task class. Returns the previous state.
    fn resume_kind(&self, class: TaskClass) -> bool;
}

/// Redis key carrying the LLM upstream pool status between the security
/// gateway (which owns upstream health) and the scheduler side (which decides
/// whether LLM-dependent work may run). The gateway writes it with a TTL
/// bounded by the earliest known recovery, so a crashed gateway cannot wedge
/// the scheduler open or closed forever.
pub const LLM_POOL_STATUS_KEY: &str = "llm:pool:status";

/// Cross-process snapshot of LLM upstream pool health.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LlmPoolStatus {
    /// `true` when no configured upstream can currently serve a request.
    pub unavailable: bool,
    /// Earliest known upstream recovery as unix seconds; `0` when unknown.
    pub earliest_recovery_unix: i64,
    /// Identity (`base_url|model`) of the unusable upstreams.
    pub unavailable_upstreams: Vec<String>,
}

/// Health issue identified for an Agent / Crew / Squad.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum HealthIssue {
    /// Heartbeat missed `missed_beats` consecutive intervals.
    Suspect { missed_beats: u32 },
    /// Heartbeat absent for too long; Agent is presumed dead.
    Dead { last_seen: DateTime<Utc> },
    /// Agent has not transitioned out of Active in `stuck_seconds`.
    Stuck { stuck_seconds: u64 },
    /// Agent moved to Dead via the StateBackend.
    StateBackendDead,
}

/// Loop severity detected by the behavior monitor.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LoopSeverity {
    None,
    /// Continuous 3 observations with no new info: send reminder.
    Mild,
    /// Continuous 5 observations: trigger escalation.
    Escalate,
    /// Continuous 10 observations with no mutations: force termination.
    Critical,
}

/// Events emitted by the Supervisor on its broadcast channel.
/// Other parts of the platform (Web UI, alerting, audit log) subscribe
/// to these to react to cluster health changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum SupervisorEvent {
    /// Periodic supervisor tick — emitted every health-check cycle so
    /// downstream systems can probe liveness.
    Tick {
        timestamp: DateTime<Utc>,
        cycle: u64,
    },

    /// An Agent was identified as unhealthy.
    AgentUnhealthy {
        agent_id: String,
        issue: HealthIssue,
        timestamp: DateTime<Utc>,
    },

    /// An Agent recovered to a healthy state.
    AgentRecovered {
        agent_id: String,
        timestamp: DateTime<Utc>,
    },

    /// An Agent was killed via gRPC command.
    AgentKilled {
        agent_id: String,
        reason: String,
        timestamp: DateTime<Utc>,
    },

    /// An Agent was restarted via gRPC command.
    AgentRestarted {
        agent_id: String,
        preserve_context: bool,
        timestamp: DateTime<Utc>,
    },

    /// A checkpoint was requested for an Agent via gRPC command.
    CheckpointRequested {
        agent_id: String,
        task_id: String,
        checkpoint_id: String,
        timestamp: DateTime<Utc>,
    },

    /// Crew retry was triggered for a set of failed task ids.
    CrewRetried {
        crew_id: String,
        task_ids: Vec<String>,
        retried: usize,
        timestamp: DateTime<Utc>,
    },

    /// Crew exhausted its retry budget; Squad respawn requested.
    SquadRespawnRequested {
        crew_id: String,
        squad_id: Option<String>,
        reason: String,
        timestamp: DateTime<Utc>,
    },

    /// Squad respawn was directly executed by the Respawner.
    SquadRespawnExecuted {
        crew_id: String,
        reason: String,
        timestamp: DateTime<Utc>,
    },

    /// Quota enforcement decision.
    QuotaThresholdBreached {
        workspace_id: String,
        remaining: u64,
        threshold: u64,
        scheduler_paused: bool,
        timestamp: DateTime<Utc>,
    },

    /// Quota recovered above threshold; scheduler resumed.
    QuotaRecovered {
        workspace_id: String,
        remaining: u64,
        timestamp: DateTime<Utc>,
    },

    /// Every configured LLM upstream is unavailable (quota exhausted, auth
    /// rejected, unreachable). LLM-dependent work is paused until recovery;
    /// mechanical work keeps running.
    LlmUpstreamPoolDown {
        /// Earliest known upstream recovery time as unix seconds; `0` when no
        /// upstream reported a reset time, so the alert can say "unknown".
        earliest_recovery_unix: i64,
        /// Identity of the unusable upstreams (`base_url|model`).
        unavailable: Vec<String>,
        timestamp: DateTime<Utc>,
    },

    /// At least one LLM upstream is usable again; LLM-dependent work resumes.
    LlmUpstreamPoolRecovered { timestamp: DateTime<Utc> },

    /// Imbalanced workload detected and a rebalance plan was emitted.
    Rebalance {
        ready_tasks: usize,
        active_agents: usize,
        plan_size: usize,
        /// Number of tasks with checkpoint recovery in this plan.
        checkpoint_recoveries: usize,
        timestamp: DateTime<Utc>,
    },

    /// Agent behavior loop detected by BehaviorMonitor.
    AgentRuntimeDetected {
        agent_id: String,
        severity: LoopSeverity,
        timestamp: DateTime<Utc>,
    },

    /// Automatic task hand-off triggered when a predecessor completes.
    TaskHandOff {
        predecessor_task_id: String,
        successor_task_id: String,
        agent_id: Option<String>,
        crew_id: Option<String>,
        timestamp: DateTime<Utc>,
    },

    /// Task retry triggered by the autonomous collaborator.
    TaskRetry {
        task_id: String,
        agent_id: Option<String>,
        crew_id: Option<String>,
        retry_count: u32,
        timestamp: DateTime<Utc>,
    },

    /// Agent proactively reported a resource threshold breach.
    AgentResourceAlert {
        agent_id: String,
        metric: String,
        threshold: f64,
        current: f64,
        timestamp: DateTime<Utc>,
    },

    /// Task sent to dead-letter queue after max retries exceeded.
    TaskDeadLetter {
        task_id: String,
        agent_id: Option<String>,
        crew_id: Option<String>,
        retry_count: u32,
        timestamp: DateTime<Utc>,
    },

    /// Aggregated observability event (forwarded from the runtime
    /// broadcast channel).
    EventAggregated {
        count: u64,
        window_seconds: u64,
        timestamp: DateTime<Utc>,
    },

    /// Autonomous scale-out recommendation based on load ratio.
    ScaleRecommendation {
        reason: String,
        recommended_agents: u32,
        timestamp: DateTime<Utc>,
    },

    /// Agent reported an event via the gRPC control plane (Unary or Client Streaming).
    AgentEventReported {
        agent_id: String,
        event: crate::types::AgentEvent,
        timestamp: DateTime<Utc>,
    },
}

impl SupervisorEvent {
    /// Return the supervisor event type name for filtering.
    pub fn name(&self) -> &'static str {
        match self {
            SupervisorEvent::Tick { .. } => "tick",
            SupervisorEvent::AgentUnhealthy { .. } => "agent_unhealthy",
            SupervisorEvent::AgentRecovered { .. } => "agent_recovered",
            SupervisorEvent::AgentKilled { .. } => "agent_killed",
            SupervisorEvent::AgentRestarted { .. } => "agent_restarted",
            SupervisorEvent::CheckpointRequested { .. } => "checkpoint_requested",
            SupervisorEvent::CrewRetried { .. } => "crew_retried",
            SupervisorEvent::SquadRespawnRequested { .. } => "squad_respawn_requested",
            SupervisorEvent::SquadRespawnExecuted { .. } => "squad_respawn_executed",
            SupervisorEvent::QuotaThresholdBreached { .. } => "quota_threshold_breached",
            SupervisorEvent::QuotaRecovered { .. } => "quota_recovered",
            SupervisorEvent::LlmUpstreamPoolDown { .. } => "llm_upstream_pool_down",
            SupervisorEvent::LlmUpstreamPoolRecovered { .. } => "llm_upstream_pool_recovered",
            SupervisorEvent::Rebalance { .. } => "rebalance",
            SupervisorEvent::AgentRuntimeDetected { .. } => "agent_loop_detected",
            SupervisorEvent::TaskHandOff { .. } => "task_hand_off",
            SupervisorEvent::TaskRetry { .. } => "task_retry",
            SupervisorEvent::AgentResourceAlert { .. } => "agent_resource_alert",
            SupervisorEvent::TaskDeadLetter { .. } => "task_dead_letter",
            SupervisorEvent::EventAggregated { .. } => "event_aggregated",
            SupervisorEvent::ScaleRecommendation { .. } => "scale_recommendation",
            SupervisorEvent::AgentEventReported { .. } => "agent_event_reported",
        }
    }
}
