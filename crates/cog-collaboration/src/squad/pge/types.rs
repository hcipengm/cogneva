use serde::{Deserialize, Serialize};

/// Specification of a single atomic task produced by the Planner.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct TaskSpec {
    pub id: String,
    pub name: String,
    pub task_type: String,
    pub input: serde_json::Value,
    #[serde(alias = "blockedBy")]
    pub blocked_by: Vec<String>,
}

/// Output produced by a Planner agent.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct PlannerOutput {
    /// Human-readable plan summary.
    pub summary: String,
    /// Machine-consumable structured plan.
    pub plan: serde_json::Value,
    /// Atom tasks decomposed from the meta-task.
    pub sub_tasks: Vec<TaskSpec>,
    /// Verifiable acceptance criteria the Evaluation stage must check one by one.
    /// Empty means the plan imposes no explicit gate and the evaluator falls back
    /// to its generic scoring rubric.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub acceptance_criteria: Vec<String>,
}

/// A named artifact produced by the Generator.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct Artifact {
    pub name: String,
    pub content: String,
    pub artifact_type: String,
}

/// Output produced by a Generator agent.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct GeneratorOutput {
    /// Primary output content.
    pub content: serde_json::Value,
    /// Named deliverables.
    pub artifacts: Vec<Artifact>,
}

/// Prefix marking a run whose failure cause is deterministic (environment or
/// upstream protocol), so retry/upgrade loops can stop instead of re-paying
/// for attempts that must fail again.
pub const TERMINAL_ENV_FAILURE_PREFIX: &str = "terminal_env_failure";

/// Failure reason for a run that produced nothing and carried no cause of its
/// own. One definition, so every producer of this feedback — pipeline, Ralph,
/// roundtable escalation — reports the identical string.
pub const NO_ARTIFACTS_REASON: &str =
    "terminal_env_failure: generator produced no artifacts (environment/protocol failure)";

impl GeneratorOutput {
    /// Structural defect of this run's change artifact, if any.
    ///
    /// A self-evolution run's deliverable is a unified diff that a later apply
    /// gate consumes verbatim. The evaluator is an LLM: it can judge whether
    /// the change looks right, but it cannot see that a hunk header's declared
    /// line counts disagree with the hunk body, so it passes artifacts that
    /// `git apply` will reject. Naming the defect here lets the repair loop
    /// hand the exact arithmetic back to the generator instead of the artifact
    /// travelling downstream to die as an opaque "corrupt patch".
    pub fn change_artifact_defect(&self, task: &cog_core::Task) -> Option<String> {
        if !task.is_self_evolution() {
            return None;
        }
        self.artifacts.iter().find_map(|artifact| {
            let is_change = artifact.artifact_type == "change"
                || artifact.name.to_lowercase().ends_with(".diff");
            if !is_change {
                return None;
            }
            cog_core::diff_structural_defect(&artifact.content).map(|defect| {
                format!(
                    "change artifact '{}' is not an appliable unified diff: {}",
                    artifact.name, defect
                )
            })
        })
    }

    /// True when the generator produced nothing for a deterministic reason:
    /// it reported an environment/protocol failure (tools never executed,
    /// explicit `environment_error`), or it returned no artifacts and no
    /// content at all. Retrying with the same environment cannot succeed, so
    /// callers must treat the run as terminal rather than paying for more
    /// attempts.
    pub fn is_terminal_env_failure(&self) -> bool {
        if !self.artifacts.is_empty() {
            return false;
        }
        let text = match &self.content {
            serde_json::Value::String(s) => s.as_str(),
            serde_json::Value::Null => return true,
            other => return other.to_string().is_empty(),
        };
        let t = text.to_ascii_lowercase();
        t.contains("environment_error") || t.contains("tool_pipeline_broken")
    }

    /// Failure reason in the wire format outer loops match on, carrying the
    /// generator's own error when it has one. A prompt that never reached the
    /// upstream reports that failure; only a run that produced nothing without
    /// a cause of its own falls back to [`NO_ARTIFACTS_REASON`]. Reporting the
    /// generic label for an upstream outage blames a generator defect that does
    /// not exist and sends the learning chain after it. `None` when this is not
    /// a terminal environment failure.
    pub fn terminal_env_failure_reason(&self) -> Option<String> {
        if !self.is_terminal_env_failure() {
            return None;
        }
        let detail = match &self.content {
            serde_json::Value::String(s) if !s.trim().is_empty() => s.trim(),
            _ => return Some(NO_ARTIFACTS_REASON.to_string()),
        };
        Some(format!("{TERMINAL_ENV_FAILURE_PREFIX}: {detail}"))
    }
}

/// A single evaluation criterion.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct Criterion {
    pub name: String,
    pub score: u32,
    pub comment: String,
}

/// Verdict of an evaluation.
#[derive(Debug, Clone, Copy, Serialize, schemars::JsonSchema, PartialEq, Eq, Hash, Default)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    #[default]
    Fail,
    Pass,
    Partial,
    NeedsReview,
    Retry,
}

impl<'de> serde::Deserialize<'de> for Verdict {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct VerdictVisitor;
        impl<'de> serde::de::Visitor<'de> for VerdictVisitor {
            type Value = Verdict;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a verdict string or boolean")
            }
            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value.to_lowercase().as_str() {
                    "pass" | "passed" => Ok(Verdict::Pass),
                    "fail" | "failed" => Ok(Verdict::Fail),
                    "partial" => Ok(Verdict::Partial),
                    "needs_review" | "needsreview" => Ok(Verdict::NeedsReview),
                    "retry" => Ok(Verdict::Retry),
                    _ => Err(serde::de::Error::unknown_variant(
                        value,
                        &["pass", "fail", "partial", "needs_review", "retry"],
                    )),
                }
            }
            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value {
                    Ok(Verdict::Pass)
                } else {
                    Ok(Verdict::Fail)
                }
            }
        }
        deserializer.deserialize_any(VerdictVisitor)
    }
}

/// Result of a single evaluation.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct EvaluationResult {
    #[serde(default)]
    pub verdict: Verdict,
    pub feedback: String,
    pub score: Option<u32>,
    pub criteria: Vec<Criterion>,
    pub details: Option<serde_json::Value>,
}

impl EvaluationResult {
    /// Deterministic gate: when the plan declared acceptance criteria, a Pass
    /// verdict is only credible if the evaluator actually reported a
    /// per-criterion judgement. A Pass without any criterion evidence means
    /// the declared gate was never checked — downgrade to Fail.
    pub fn enforce_criteria_evidence(&mut self, criteria_were_supplied: bool) {
        if criteria_were_supplied && self.verdict == Verdict::Pass && self.criteria.is_empty() {
            self.verdict = Verdict::Fail;
            self.feedback = format!(
                "acceptance criteria were declared but the evaluator passed without judging them; original feedback: {}",
                self.feedback
            );
        }
    }

    /// Deterministic gate on the artifact itself: a change that `git apply`
    /// cannot consume is not a deliverable, however sound the surrounding
    /// prose. The evaluator only judges intent, so a structurally broken diff
    /// would otherwise pass and be discovered — unattributably — at the apply
    /// gate much later. Downgrade a Pass to Fail and carry the precise defect
    /// into the feedback so the repair loop can fix it; when the verdict
    /// already failed, still append the defect so the repair sees both reasons.
    pub fn enforce_change_artifact_integrity(&mut self, defect: Option<String>) {
        let Some(defect) = defect else { return };
        if self.verdict == Verdict::Pass {
            self.verdict = Verdict::Fail;
            self.feedback = format!("{defect}; original feedback: {}", self.feedback);
        } else if !self.feedback.contains(&defect) {
            self.feedback = format!("{}; {defect}", self.feedback);
        }
    }
}

/// A single local repair cycle inside a pipeline attempt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LocalRepairAttempt {
    pub repair_iteration: u32,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
    pub feedback: String,
}

/// Result of a single parallel PGE branch in a roundtable iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PgeBranchResult {
    pub branch_id: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
}

/// Strategy for merging parallel branch results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum BranchMergeStrategy {
    /// Pick the branch with the highest evaluation score.
    #[default]
    BestScore,
    /// Majority vote across branch verdicts.
    MajorityVote,
    /// Union artifacts and pick the best generation by score.
    UnionArtifacts,
    /// Delegate to a MergerActor LLM.
    Custom,
}

/// Summary of how parallel branches were merged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MergeSummary {
    pub selected_branch_id: u32,
    pub strategy: BranchMergeStrategy,
    pub reasoning: String,
}

/// A single roundtable iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PgeRoundtableIteration {
    pub iteration: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub evaluation: EvaluationResult,
    /// Parallel branch results that produced this iteration, if any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<PgeBranchResult>,
    /// How the branches were merged, if parallel branches were used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_summary: Option<MergeSummary>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_output_without_criteria_deserializes_to_empty() {
        let value = serde_json::json!({
            "summary": "s",
            "plan": {},
            "sub_tasks": []
        });
        let output: PlannerOutput = serde_json::from_value(value).unwrap();
        assert!(output.acceptance_criteria.is_empty());
    }

    #[test]
    fn planner_output_roundtrip_preserves_criteria() {
        let output = PlannerOutput {
            summary: "s".into(),
            plan: serde_json::json!({}),
            sub_tasks: Vec::new(),
            acceptance_criteria: vec!["answer states the exact version".into()],
        };
        let value = serde_json::to_value(&output).unwrap();
        let back: PlannerOutput = serde_json::from_value(value).unwrap();
        assert_eq!(back.acceptance_criteria, output.acceptance_criteria);
    }

    fn self_evolution_task() -> cog_core::Task {
        cog_core::Task::new(
            "t-1",
            cog_core::TaskType::Custom("self_evolution".into()),
            serde_json::json!({ "evolution_mode": "generate_change" }),
        )
    }

    fn plain_task() -> cog_core::Task {
        cog_core::Task::new(
            "t-2",
            cog_core::TaskType::Custom("summarize".into()),
            serde_json::json!({ "goal": "summarize" }),
        )
    }

    fn change_output(diff: &str) -> GeneratorOutput {
        GeneratorOutput {
            content: serde_json::json!({}),
            artifacts: vec![crate::squad::pge::types::Artifact {
                name: "changes.diff".into(),
                content: diff.into(),
                artifact_type: "change".into(),
            }],
        }
    }

    #[test]
    fn an_unappliable_change_artifact_is_named_as_a_defect() {
        let output = change_output(
            "--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1,15 +1,49 @@\n fn a() {}\n+fn b() {}\n",
        );
        let defect = output
            .change_artifact_defect(&self_evolution_task())
            .expect("a malformed diff must be reported");
        assert!(defect.contains("changes.diff"), "{defect}");
    }

    #[test]
    fn a_sound_change_artifact_has_no_defect() {
        let output = change_output("--- a/src/a.rs\n+++ b/src/a.rs\n@@ -1 +1 @@\n-old\n+new\n");
        assert_eq!(output.change_artifact_defect(&self_evolution_task()), None);
    }

    #[test]
    fn a_plain_task_is_never_checked_for_change_integrity() {
        let output = change_output("not a diff at all");
        assert_eq!(output.change_artifact_defect(&plain_task()), None);
    }

    #[test]
    fn a_pass_over_an_unappliable_change_is_downgraded() {
        let mut evaluation = EvaluationResult {
            verdict: Verdict::Pass,
            feedback: "looks correct".into(),
            score: Some(95),
            criteria: Vec::new(),
            details: None,
        };
        evaluation.enforce_change_artifact_integrity(Some("hunk header miscounts".into()));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(evaluation.feedback.contains("hunk header miscounts"));
        assert!(evaluation.feedback.contains("looks correct"));
    }

    #[test]
    fn an_already_failed_verdict_still_learns_the_defect() {
        let mut evaluation = EvaluationResult {
            verdict: Verdict::Fail,
            feedback: "criteria not met".into(),
            score: Some(20),
            criteria: Vec::new(),
            details: None,
        };
        evaluation.enforce_change_artifact_integrity(Some("hunk header miscounts".into()));
        assert_eq!(evaluation.verdict, Verdict::Fail);
        assert!(evaluation.feedback.contains("criteria not met"));
        assert!(evaluation.feedback.contains("hunk header miscounts"));
    }

    #[test]
    fn a_sound_artifact_leaves_a_pass_untouched() {
        let mut evaluation = EvaluationResult {
            verdict: Verdict::Pass,
            feedback: "looks correct".into(),
            score: Some(95),
            criteria: Vec::new(),
            details: None,
        };
        evaluation.enforce_change_artifact_integrity(None);
        assert_eq!(evaluation.verdict, Verdict::Pass);
        assert_eq!(evaluation.feedback, "looks correct");
    }

    fn pass_result(criteria: Vec<Criterion>) -> EvaluationResult {
        EvaluationResult {
            verdict: Verdict::Pass,
            feedback: "looks good".into(),
            score: Some(90),
            criteria,
            details: None,
        }
    }

    #[test]
    fn pass_without_criterion_evidence_is_downgraded_when_criteria_supplied() {
        let mut result = pass_result(Vec::new());
        result.enforce_criteria_evidence(true);
        assert_eq!(result.verdict, Verdict::Fail);
    }

    #[test]
    fn pass_with_criterion_evidence_stays_pass() {
        let mut result = pass_result(vec![Criterion {
            name: "c1".into(),
            score: 100,
            comment: "met".into(),
        }]);
        result.enforce_criteria_evidence(true);
        assert_eq!(result.verdict, Verdict::Pass);
    }

    #[test]
    fn no_declared_criteria_leaves_unevidenced_pass_alone() {
        let mut result = pass_result(Vec::new());
        result.enforce_criteria_evidence(false);
        assert_eq!(result.verdict, Verdict::Pass);
    }

    #[test]
    fn carried_upstream_error_is_the_reported_reason() {
        // What the generator now emits when its prompt never reached an upstream.
        let output = GeneratorOutput {
            content: serde_json::Value::String(
                "environment_error: Agent execution error: API error: upstream unavailable".into(),
            ),
            artifacts: Vec::new(),
        };
        let reason = output.terminal_env_failure_reason().unwrap();
        assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
        assert!(
            reason.contains("upstream unavailable"),
            "the real cause must survive into the reason, got: {reason}"
        );
        assert_ne!(reason, NO_ARTIFACTS_REASON);
    }

    #[test]
    fn bare_empty_output_reports_the_generic_reason() {
        let output = GeneratorOutput {
            content: serde_json::Value::Null,
            artifacts: Vec::new(),
        };
        assert_eq!(
            output.terminal_env_failure_reason().as_deref(),
            Some(NO_ARTIFACTS_REASON)
        );
    }

    #[test]
    fn artifacts_present_is_not_a_terminal_failure() {
        let output = GeneratorOutput {
            content: serde_json::Value::String("environment_error".into()),
            artifacts: vec![Artifact {
                name: "changes.diff".into(),
                content: "diff --git a/x b/x".into(),
                artifact_type: "change".into(),
            }],
        };
        assert!(output.terminal_env_failure_reason().is_none());
    }
}
