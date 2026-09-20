//! Task features for the meta-learning decision about the PGE mode.
//!
//! The engine groups observations by task type plus the first domain tag, and
//! the two halves of the loop run at different moments: mode selection reads a
//! recommendation before the squad exists, squad completion writes the outcome
//! after it finished. Only the task kind is knowable on both sides, so it is
//! the whole discriminator — a squad id or a goal string would make every group
//! a single observation, and the engine needs `min_samples` observations in a
//! group before it can recommend anything at all. Two sides that build the key
//! from different fields never share a group either.
//!
//! Both sides take their features from here so the read key and the write key
//! cannot drift apart.

use cog_core::TaskFeatures;

/// Features for the PGE-mode decision. Only `task_type` and `domain_tags`
/// participate in the grouping key; the remaining fields are placeholders for
/// a richer model that is not wired yet.
pub fn squad_decision_features() -> TaskFeatures {
    TaskFeatures {
        task_type: "squad".into(),
        domain_tags: Vec::new(),
        estimated_complexity: 0.5,
        has_external_dependencies: false,
        historical_success_rate: 0.5,
        required_skills: Vec::new(),
    }
}
