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
