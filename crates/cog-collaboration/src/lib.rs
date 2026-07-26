//! Multi-Agent collaboration layer — actors, squads, and orchestration.
//! Built on top of `sf-agent` (Agent kernel).  Consumers that only need a
//! single Agent loop should depend on `sf-agent` directly.

pub mod actors;
pub mod collaboration_executor;
pub mod hierarchy;
pub mod ipc;
pub mod observable;
pub mod profile;
pub mod squad;

pub use actors::{ModeratorDecision, ModeratorOutput};
pub use collaboration_executor::CollaborationExecutor;
pub use hierarchy::{
    cross_squad_notify, AgentIdent, BroadcastRouter, HierarchicalCommunication,
    HierarchicalMessage, InterSquadMessage, RoutingStrategy, SquadId, TopicName,
};
pub use ipc::{FileSystemIpc, IpcChannel, IpcMessage};
pub use observable::CollaborationObservable;
pub use profile::{complexity_score, select_mode, PgeMode, TaskProfile, PIPELINE_SCORE_THRESHOLD};
pub use squad::pge::{
    parse_evaluation_result, parse_generator_output, parse_planner_output, Artifact, ContextBoard,
    Criterion, EvaluationResult, GeneratorOutput, InMemoryContextBoard, PgePipeline,
    PgePipelineAttempt, PgePipelineConfig, PgePipelineResult, PgeRoundtable, PgeRoundtableConfig,
    PgeRoundtableIteration, PgeRoundtableResult, PlannerOutput, RedisContextBoard, TaskSpec,
    Verdict,
};
pub use squad::ralph::{
    FailureAnalysis, RalphIteration, RalphLoop, RalphLoopConfig, RalphVerdict, ResetStrategy,
};
pub use squad::{Squad, SquadConfig, SquadExecutor, SquadResult};

pub mod plugin;
