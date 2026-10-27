//! Cogneva Supervisor Process
//! External daemon that observes the runtime cluster but never blocks
//! Agent execution.  Implements the architecture described in
//! Components:
//! - [`HealthChecker`] — polls Agent / Crew / Squad heartbeats, marks
//!   stuck workers as Suspect / Dead.
//! - [`Respawner`] — on health failure, drives Crew retry via
//!   [`cog_core::OrchestratorControl`] or marks tasks for respawn.
//! - [`QuotaEnforcer`] — monitors per-workspace token quotas; pauses
//!   the autonomous scheduler when limits are exceeded.
//! - [`TaskRebalancer`] — detects imbalanced ready-task / Agent
//!   distribution and surfaces rebalance recommendations.
//! - [`EventAggregator`] — fans out the broadcast channel into the
//!   shared [`cog_core::ObservabilityGateway`].

pub mod alert_store;
pub mod autonomous;
pub mod behavior_monitor;
pub mod binary_switcher;
pub mod consumer_lag;
pub mod control_plane;
pub mod error;
pub mod event_aggregator;
pub mod events;
pub mod health_checker;
pub mod heartbeat_driver;
pub mod lifecycle_coordinator;
pub mod llm_pool_guard;
pub mod multi_backend_consumer;
pub mod plugin;
pub mod quota_enforcer;
pub mod registry;
pub mod respawner;
pub mod scheduler_gate;
pub mod supervisor;
pub mod task_rebalancer;
pub mod udp_heartbeat;
pub mod unix_heartbeat;

/// The label naming how a reading ended.
///
/// Every family here with more than one ending carries this label, and the
/// readers that count one ending select by it. It is declared once per crate
/// that writes it, under the same identifier, and a gate compares the
/// spellings: one edited copy publishes a family under a name every existing
/// selector reads as empty, which is what "this family never ran" looks like.
pub const LABEL_OUTCOME: &str = "outcome";

/// 读数按「第几遍」归属时用的标签键。
pub const LABEL_PASS: &str = "pass";

/// 读数按「哪个消费方」归属时用的标签键。
pub const LABEL_CONSUMER: &str = "consumer";

/// 读数按「交接走到哪一段」归属时用的标签键。
pub const LABEL_STAGE: &str = "stage";

pub use alert_store::{Alert, AlertStore};
pub use autonomous::{AutonomousCollaborator, AutonomousConfig};
pub use behavior_monitor::{ActionOutcome, ActionType, BehaviorMonitor};
pub use binary_switcher::{
    build_switcher, BinarySwitcherConfig, SelfExecSwitcher, SidecarSwitcher, SystemdSwitcher,
};
pub use control_plane::{
    ControlPlaneClient, HttpControlPlaneClient, NoopControlPlaneClient, SupervisorStatus,
};
pub use error::{SupervisorError, SupervisorResult};
pub use event_aggregator::{EventAggregator, EventAggregatorStats};
pub use health_checker::{HealthChecker, HealthCheckerConfig, HealthReport};
pub use heartbeat_driver::HeartbeatDriver;
pub use lifecycle_coordinator::{LifecycleCoordinator, LifecycleReport, RecoveredCheckpoint};
pub use llm_pool_guard::{KeyValuePoolStatusSource, LlmPoolGuard, PoolTransition};
pub use multi_backend_consumer::MultiBackendEventConsumer;
pub use quota_enforcer::{QuotaEnforcer, QuotaSnapshot};
pub use registry::{AgentInfo, AgentRegistry, CrewInfo};
pub use respawner::{RespawnReport, Respawner};
pub use scheduler_gate::SchedulerGate;
pub use supervisor::{Supervisor, SupervisorConfig};
pub use task_rebalancer::{RebalancePlan, TaskRebalancer, TaskRebalancerConfig};
