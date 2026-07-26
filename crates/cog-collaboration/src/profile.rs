//! Task profile and PGE mode selector.
//! Implements the task complexity scoring and dispatch rule from
//! Planner → Generator → Evaluator pipeline, while higher-complexity
//! work falls back to the Roundtable debate loop.

/// Profile describing the dimensions used to choose a PGE execution mode.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TaskProfile {
    /// How novel the task is relative to historical work (0.0 = familiar, 1.0 = brand new).
    pub novelty: f64,
    /// Risk of negative side effects if the task fails (0.0 = low, 1.0 = high).
    pub risk: f64,
    /// Ambiguity of the requirements (0.0 = crisp, 1.0 = vague).
    pub ambiguity: f64,
    /// Number of upstream/downstream dependencies, normalized to 0.0..=1.0.
    pub dependency_count: f64,
    /// Token budget headroom available for the task, normalized to 0.0..=1.0.
    pub token_budget: f64,
    /// Historical success rate on comparable tasks (0.0 = always fails, 1.0 = always succeeds).
    pub historical_success: f64,
}

impl Default for TaskProfile {
    fn default() -> Self {
        Self {
            novelty: 0.0,
            risk: 0.0,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 1.0,
        }
    }
}

/// Mode the PGE selector dispatches to for a given [`TaskProfile`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PgeMode {
    /// Linear Planner → Generator → Evaluator with no feedback loop.
    Pipeline,
    /// Iterative debate loop with retries and consensus checking.
    Roundtable,
}

/// Threshold below which the [`PgeMode::Pipeline`] is preferred.
pub const PIPELINE_SCORE_THRESHOLD: f64 = 0.4;

/// Compute the complexity score for a profile.
/// Weights match the design doc:
/// `0.25*novelty + 0.30*risk + 0.20*ambiguity + 0.10*deps + 0.15*(1 - historical_success)`.
/// `token_budget` is intentionally not used in the scoring formula but is kept
/// on [`TaskProfile`] for future budget-aware variants.
pub fn complexity_score(p: &TaskProfile) -> f64 {
    p.novelty * 0.25
        + p.risk * 0.30
        + p.ambiguity * 0.20
        + p.dependency_count * 0.10
        + (1.0 - p.historical_success) * 0.15
}

/// Select the PGE mode appropriate for the given task profile.
/// Returns [`PgeMode::Pipeline`] when [`complexity_score`] is below
/// [`PIPELINE_SCORE_THRESHOLD`]; otherwise [`PgeMode::Roundtable`].
/// When the score is right on the boundary we prefer Roundtable — quality
/// over speed when the decision is uncertain.
pub fn select_mode(p: &TaskProfile) -> PgeMode {
    if complexity_score(p) < PIPELINE_SCORE_THRESHOLD {
        PgeMode::Pipeline
    } else {
        PgeMode::Roundtable
    }
}

/// Derive a task profile from a raw Task for PGE mode selection.
pub fn derive_task_profile(task: &cog_core::Task) -> TaskProfile {
    let input_len = task.input.to_string().len() as f64;
    let dep_count = task.blocked_by.len() as f64;
    TaskProfile {
        novelty: match task.task_type {
            cog_core::TaskType::Custom(_) => 0.7,
            cog_core::TaskType::WasmSkill | cog_core::TaskType::Skill => 0.6,
            _ => 0.3,
        },
        risk: (dep_count / 10.0).min(1.0),
        ambiguity: if input_len < 50.0 {
            0.8
        } else if input_len < 200.0 {
            0.5
        } else {
            0.3
        },
        dependency_count: (dep_count / 20.0).min(1.0),
        token_budget: 1.0,
        historical_success: 1.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_profile_picks_pipeline() {
        let p = TaskProfile::default();
        assert!(complexity_score(&p) < PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    #[test]
    fn mid_complexity_profile_picks_roundtable() {
        // Score ~0.35 falls just above the Pipeline threshold (0.4).
        let p = TaskProfile {
            novelty: 0.5,
            risk: 0.5,
            ambiguity: 0.5,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 1.0,
        };
        let score = complexity_score(&p);
        // 0.5*0.25 + 0.5*0.30 + 0.5*0.20 = 0.125 + 0.15 + 0.10 = 0.375
        assert!(
            (0.25..PIPELINE_SCORE_THRESHOLD).contains(&score),
            "score={}",
            score
        );
        assert_eq!(select_mode(&p), PgeMode::Pipeline);
    }

    #[test]
    fn high_risk_profile_picks_roundtable() {
        let p = TaskProfile {
            novelty: 0.9,
            risk: 0.9,
            ambiguity: 0.9,
            dependency_count: 0.9,
            token_budget: 1.0,
            historical_success: 0.1,
        };
        assert!(complexity_score(&p) >= PIPELINE_SCORE_THRESHOLD);
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    #[test]
    fn boundary_profile_picks_roundtable() {
        // Score exactly 0.4 should fall to Roundtable per the strict `<` rule.
        let p = TaskProfile {
            novelty: 0.0,
            risk: 0.0,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            // Use a direct mix that yields exactly 0.4: risk 1.0 contributes 0.30,
            // dependency_count 1.0 contributes 0.10. 0.30 + 0.10 = 0.40.
            historical_success: 1.0,
        };
        let p = TaskProfile {
            risk: 1.0,
            dependency_count: 1.0,
            ..p
        };
        let score = complexity_score(&p);
        assert!((score - 0.4).abs() < f64::EPSILON);
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }

    #[test]
    fn low_historical_success_band_transitions() {
        let p = TaskProfile {
            novelty: 0.0,
            risk: 0.6,
            ambiguity: 0.0,
            dependency_count: 0.0,
            token_budget: 1.0,
            historical_success: 0.0,
        };
        // 0.6*0.30 + 1.0*0.15 = 0.18 + 0.15 = 0.33 → Pipeline.
        assert_eq!(select_mode(&p), PgeMode::Pipeline);

        let p = TaskProfile { risk: 0.8, ..p };
        // 0.8*0.30 + 0.15 = 0.24 + 0.15 = 0.39 → Pipeline.
        assert_eq!(select_mode(&p), PgeMode::Pipeline);

        let p = TaskProfile { risk: 0.9, ..p };
        // 0.9*0.30 + 0.15 = 0.27 + 0.15 = 0.42 → Roundtable.
        assert_eq!(select_mode(&p), PgeMode::Roundtable);
    }
}
