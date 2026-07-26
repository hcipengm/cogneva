//! Prompt context construction contract for PGE actors.
//!
//! Design: `docs/20250607_context_engineering_action_plan.md` P0-2.
//! Before this contract existed, every PGE actor hand-assembled its prompt
//! context with inline `serde_json::json!` blocks, so the per-role context
//! schema was implicit and inconsistent. [`TaskContextBuilder`] centralizes
//! the construction: given the actor [`PgeRole`] and a [`TaskContextInput`],
//! it produces the JSON context passed to the LLM. Implementations live in
//! the consuming crates (e.g. `cog-collaboration` ships a
//! `StandardTaskContextBuilder`); `cog-core` owns only the contract.

use crate::Task;

/// The PGE role requesting a prompt context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgeRole {
    Planner,
    Generator,
    Evaluator,
    Moderator,
    Merger,
}

impl std::fmt::Display for PgeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgeRole::Planner => write!(f, "planner"),
            PgeRole::Generator => write!(f, "generator"),
            PgeRole::Evaluator => write!(f, "evaluator"),
            PgeRole::Moderator => write!(f, "moderator"),
            PgeRole::Merger => write!(f, "merger"),
        }
    }
}

/// Everything a [`TaskContextBuilder`] may need to assemble a prompt
/// context. Optional fields are filled only by the roles that consume them;
/// a builder must ignore fields irrelevant to the requested [`PgeRole`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TaskContextInput<'a> {
    /// The task being processed (goal, type, hierarchy, planner meta).
    pub task: Option<&'a Task>,
    /// Current PGE attempt counter (1-based on first try).
    pub attempt: u32,
    /// The current plan (Generator/Evaluator).
    pub plan: Option<&'a serde_json::Value>,
    /// The current generation (Evaluator) or previous generation
    /// (Planner/Generator repair).
    pub generation: Option<&'a serde_json::Value>,
    /// Evaluation history from earlier attempts (Evaluator).
    pub history: &'a [serde_json::Value],
    /// Evaluation criteria (Evaluator).
    pub criteria: &'a [&'a str],
    /// Evaluator feedback from the previous attempt (Planner/Generator).
    pub previous_feedback: Option<&'a str>,
    /// Evaluator score from the previous attempt (Planner).
    pub previous_score: Option<u32>,
    /// Full evaluation record from the previous attempt (Generator).
    pub previous_evaluation: Option<&'a serde_json::Value>,
    /// Repair-targeted feedback distilled by the Evaluator (Generator).
    pub repair_feedback: Option<&'a str>,
    /// Shared squad context board snapshot.
    pub context_board: Option<&'a serde_json::Value>,
}

/// Builds the JSON prompt context for a PGE role.
///
/// The returned value is passed as the user-message context of the role's
/// LLM call. Implementations must be deterministic for the same input so
/// prompt caching stays effective.
pub trait TaskContextBuilder: Send + Sync {
    /// Assemble the prompt context for `role` from `input`.
    fn build(&self, role: PgeRole, input: &TaskContextInput<'_>) -> serde_json::Value;
}
