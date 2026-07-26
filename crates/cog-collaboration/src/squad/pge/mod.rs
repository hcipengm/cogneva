//! Loop 2 — PGE (Pipeline / Roundtable): Planner → Generator → Evaluator.

pub mod context_board;
pub mod pipeline;
pub mod roundtable;
pub mod types;

pub use context_board::{ContextBoard, InMemoryContextBoard, RedisContextBoard};
pub use pipeline::{PgePipeline, PgePipelineAttempt, PgePipelineConfig, PgePipelineResult};
pub use roundtable::{
    parse_evaluation_result, parse_generator_output, parse_planner_output, PgeRoundtable,
    PgeRoundtableConfig, PgeRoundtableResult,
};
pub use types::{
    Artifact, Criterion, EvaluationResult, GeneratorOutput, PgeRoundtableIteration, PlannerOutput,
    TaskSpec, Verdict,
};
