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
/// bounded by the next attempt, so a crashed gateway cannot wedge the
/// scheduler open or closed forever.
pub const LLM_POOL_STATUS_KEY: &str = "llm:pool:status";

/// Cross-process snapshot of LLM upstream pool health.
///
/// Two bounds travel side by side instead of being collapsed into one number,
/// because they rest on different evidence. An upstream that reported a quota
/// reset time is stating a fact about itself; a suspect window that will merely
/// expire is our own retry cadence and says nothing about whether that upstream
/// will serve again. Publishing the cadence as "recovery" asserts knowledge of
/// an external system that nothing here has, and it understates a real outage:
/// one upstream resets in six hours while the rest sit behind a sixty-second
/// backoff, and the single number reports one minute.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LlmPoolStatus {
    /// `true` when no configured upstream can currently serve a request.
    pub unavailable: bool,
    /// Earliest quota-reset time reported by any suspect upstream, unix
    /// seconds; `0` when none reported one. Evidence about the upstreams.
    #[serde(default)]
    pub evidenced_recovery_unix: i64,
    /// Earliest time a suspect upstream will be probed again, unix seconds;
    /// `0` when nothing is suspect. Our own retry cadence, not evidence.
    ///
    /// Carries an alias for the pre-split field, whose value was the same
    /// combined bound under a name that claimed more: a payload written by a
    /// process still running the old build parses with identical meaning
    /// instead of failing and dropping the pause entirely for the length of a
    /// rollout. Both bounds default, so a missing field reads as "no such
    /// evidence" rather than an error.
    #[serde(default, alias = "earliest_recovery_unix")]
    pub next_attempt_unix: i64,
    /// Identity (`base_url|model`) of the unusable upstreams.
    pub unavailable_upstreams: Vec<String>,
}

impl LlmPoolStatus {
    /// Seconds from `now_unix` until a caller should resume the work it is
    /// holding back, given how long that caller needs to observe one trial run.
    ///
    /// The two bounds are statements of different strength, so the observation
    /// window applies to only one of them. `evidenced_recovery_unix` is an
    /// upstream saying when it will serve again: nothing is added to a stated
    /// instant, because waiting past it would be waiting past the answer.
    /// `next_attempt_unix` is our own probe cadence, and a probe has to happen
    /// and be observable before a caller learns anything from it — resuming
    /// exactly at the probe means resuming before its outcome exists, which is
    /// how a retry loop reads as progress while nothing has changed.
    ///
    /// `observation_secs == 0` resumes at the bound itself.
    ///
    /// `None` means no bound lies in the future — the recovery point is unknown
    /// rather than imminent, so callers fall back to their own recheck cadence
    /// instead of polling once a second. A bound already in the past is no
    /// future opening either: the upstream said its quota resets at a time that
    /// came and went without a call succeeding. An absent cadence bound stays
    /// absent: adding a window to `0` would invent a bound out of "nothing is
    /// suspect", which is the opposite of what it says.
    pub fn resume_wait_secs(&self, now_unix: i64, observation_secs: u64) -> Option<u64> {
        let observation = i64::try_from(observation_secs).unwrap_or(i64::MAX);
        let retry = if self.next_attempt_unix > 0 {
            self.next_attempt_unix.saturating_add(observation)
        } else {
            0
        };
        [self.evidenced_recovery_unix, retry]
            .into_iter()
            .filter(|t| *t > now_unix)
            .map(|t| (t - now_unix) as u64)
            .min()
    }
}

/// Reads the cross-process pool snapshot published by the security gateway.
///
/// The gateway is the only process holding upstream credentials, so it owns the
/// verdict on whether the pool can serve a request; everything that must hold
/// work back while it cannot shares this one source rather than forming its own
/// opinion from local failures. `None` means the snapshot is absent or unusable
/// — no evidence of a healthy pool, and no evidence of a sick one — which is
/// why consumers keep a local fallback instead of treating `None` as "fine".
#[async_trait::async_trait]
pub trait LlmPoolStatusSource: Send + Sync {
    /// Current snapshot, or `None` when the pool is healthy / unknown.
    async fn status(&self) -> Option<LlmPoolStatus>;
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
        /// Earliest quota-reset time reported by any suspect upstream, unix
        /// seconds; `0` when none reported one, so the alert says the recovery
        /// time is unreported rather than inventing one.
        evidenced_recovery_unix: i64,
        /// Earliest time a suspect upstream will be probed again, unix seconds.
        /// Retry cadence, not an upstream fact, and worded as such downstream.
        next_attempt_unix: i64,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_wait_takes_the_nearer_future_bound() {
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 1_000,
            next_attempt_unix: 400,
            unavailable_upstreams: vec![],
        };
        assert_eq!(
            status.resume_wait_secs(100, 0),
            Some(300),
            "no observation window: the nearer bound decides"
        );
        assert_eq!(
            status.resume_wait_secs(100, 300),
            Some(600),
            "the probe bound carries the window, so it decides"
        );
    }

    /// The window belongs to the cadence bound, not to a stated instant: an
    /// upstream that named a time is not made to wait longer than it said.
    #[test]
    fn a_stated_instant_is_not_pushed_back_by_the_observation_window() {
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 400,
            next_attempt_unix: 1_000,
            unavailable_upstreams: vec![],
        };
        assert_eq!(status.resume_wait_secs(100, 300), Some(300));
    }

    /// `next_attempt_unix == 0` means nothing is suspect, not "a probe is due
    /// now". Adding a window to it would turn that reading into a bound a few
    /// minutes out and resume against a pool that said nothing at all.
    #[test]
    fn an_absent_cadence_bound_does_not_become_one() {
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 0,
            next_attempt_unix: 0,
            unavailable_upstreams: vec![],
        };
        assert_eq!(status.resume_wait_secs(100, 300), None);
    }

    #[test]
    fn resume_wait_is_none_when_no_bound_lies_ahead() {
        let status = LlmPoolStatus {
            unavailable: true,
            evidenced_recovery_unix: 50,
            next_attempt_unix: 0,
            unavailable_upstreams: vec![],
        };
        // A reset that has already elapsed is not a future opening: the upstream
        // named a time that came and went without a call succeeding.
        assert_eq!(status.resume_wait_secs(100, 300), None);
        assert_eq!(
            LlmPoolStatus {
                unavailable: true,
                ..Default::default()
            }
            .resume_wait_secs(100, 300),
            None
        );
    }

    /// The payload changed shape here: what used to travel as one collapsed
    /// `earliest_recovery_unix` now travels as two bounds. A process still
    /// running the old build publishes the old field, and during a rollout the
    /// new build reads it — dropping that payload would silently unpause
    /// LLM-dependent work against a dead pool for the length of the rollout.
    /// The old value was the same combined bound under a name that claimed
    /// more, so it loads as the retry bound with identical meaning.
    #[test]
    fn pre_split_payload_still_loads_as_the_retry_bound() {
        let legacy = r#"{"unavailable":true,"earliest_recovery_unix":1789744595,
                         "unavailable_upstreams":["https://a|m"]}"#;
        let status: LlmPoolStatus = serde_json::from_str(legacy).expect("old payload parses");
        assert!(status.unavailable);
        assert_eq!(status.next_attempt_unix, 1_789_744_595);
        assert_eq!(
            status.evidenced_recovery_unix, 0,
            "the old field carried no evidence about any upstream"
        );
        assert_eq!(status.resume_wait_secs(1_789_744_595 - 10, 0), Some(10));
        assert_eq!(
            status.resume_wait_secs(1_789_744_595 - 10, 300),
            Some(310),
            "the loaded bound is a retry bound, so it carries the window"
        );
    }
}
