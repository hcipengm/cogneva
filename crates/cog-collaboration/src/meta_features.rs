//! What the meta-learning engine is told about a PGE-mode decision.
//!
//! One decision, two moments: mode selection reads a recommendation before the
//! squad exists, squad completion writes the outcome after it finished. The
//! group the engine learns from is a task *kind* — a squad id or a goal string
//! would make every group a single observation, and the engine's sample floor
//! would then be unreachable no matter how many squads run. Two sides that
//! group an observation differently never see each other's trials either.
//!
//! Both sides take the group and the recorded context from here, in one value,
//! so neither half of the loop can drift from the other.

use cog_core::{DecisionGroupKey, TaskFeatures};

/// The group and recorded context of a squad's PGE-mode decision.
pub struct SquadDecision {
    pub group: DecisionGroupKey,
    /// Recorded context for the decision log. Carries no grouping: the engine
    /// groups by [`DecisionGroupKey`] alone.
    pub features: TaskFeatures,
}

/// The one definition of which group a squad's PGE-mode decision belongs to.
pub fn squad_decision() -> SquadDecision {
    SquadDecision {
        group: DecisionGroupKey::by_task_type("squad"),
        features: TaskFeatures {
            task_type: "squad".into(),
            domain_tags: Vec::new(),
            estimated_complexity: 0.5,
            has_external_dependencies: false,
            historical_success_rate: 0.5,
            required_skills: Vec::new(),
        },
    }
}
