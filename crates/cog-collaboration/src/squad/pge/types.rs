use cog_core::contract::outcome::{
    EMPTY_GENERATION_PREFIX, ITERATION_BUDGET_EXHAUSTED_MARKER, MAX_ITERATIONS_STATUS,
    TERMINAL_ENV_FAILURE_PREFIX,
};
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

/// Whether an in-band cause names a deterministic failure: the transport never
/// reached the upstream, or the loop spent its budget before producing anything.
/// One predicate for every role output so the planner and the generator cannot
/// drift apart on what counts as terminal.
fn names_a_deterministic_cause(text: &str) -> bool {
    let t = text.to_ascii_lowercase();
    t.contains("environment_error")
        || t.contains("tool_pipeline_broken")
        || t.contains(ITERATION_BUDGET_EXHAUSTED_MARKER)
}

/// Whether a role output's `content` holds nothing at all: absent, or a string
/// of whitespace. Both are one observation — the envelope arrived and was empty
/// — and both must read the same way, or a generator that answers with a blank
/// string escapes the naming its null twin gets.
fn content_is_blank(content: &serde_json::Value) -> bool {
    match content {
        serde_json::Value::Null => true,
        serde_json::Value::String(s) => s.trim().is_empty(),
        _ => false,
    }
}

/// The agent runtime's sentinel for a ReAct loop that spent its whole iteration
/// budget while tool calls were still pending, rendered as the in-band cause the
/// rest of the chain reads. `None` when the value is a normal role result.
///
/// The sentinel has no `content`, no `artifacts` and no `plan`, so a parser that
/// does not name it hands downstream an empty output that reads exactly like a
/// role which finished and had nothing to say. The real cause is local — the
/// budget ran out mid-exploration — and both the operator and the discovery
/// loop's backoff decision are reading that text, so it has to say so.
pub fn iteration_budget_exhausted_reason(value: &serde_json::Value) -> Option<String> {
    if value.get("status").and_then(|v| v.as_str()) != Some(MAX_ITERATIONS_STATUS) {
        return None;
    }
    let iterations = value
        .get("iterations")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let pending = value
        .get("pending_tool_calls")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    // Whether the model was asked straight out for an answer, and what came of
    // it. "Not asked" and "asked and still delivered nothing" are different
    // states: the first is a budget we chose, the second is the model having
    // nothing to hand over even with the tools taken away.
    let asked = match value.get("final_draft").and_then(|v| v.as_str()) {
        Some("empty") => "asked for a final draft without tools and produced nothing",
        Some("malformed") => {
            "asked for a final draft without tools and produced text with no deliverable in it"
        }
        Some("unavailable") => "the final-draft ask could not be made: the request itself failed",
        _ => "not asked for a final draft",
    };
    Some(format!(
        "{ITERATION_BUDGET_EXHAUSTED_MARKER}: the agent loop used its whole iteration budget \
         while tool calls were still pending (max_iterations={iterations}, \
         pending_tool_calls={pending}); {asked}; it stopped mid-exploration and wrote no deliverable"
    ))
}

impl PlannerOutput {
    /// True when the planner's prompt never reached its upstream, so the empty
    /// plan it reports is the transport failing rather than the planner
    /// deciding there is nothing to do. Both look identical to every downstream
    /// consumer — an empty plan is a valid plan — so the cause has to travel
    /// in-band.
    pub fn is_terminal_env_failure(&self) -> bool {
        match &self.plan {
            serde_json::Value::String(s) => names_a_deterministic_cause(s),
            _ => false,
        }
    }

    /// Failure reason in the wire format outer loops match on, carrying the
    /// planner's own error. `None` when the plan was actually produced.
    pub fn terminal_env_failure_reason(&self) -> Option<String> {
        if !self.is_terminal_env_failure() {
            return None;
        }
        match &self.plan {
            serde_json::Value::String(s) if !s.trim().is_empty() => {
                Some(format!("{TERMINAL_ENV_FAILURE_PREFIX}: {}", s.trim()))
            }
            _ => Some(format!(
                "{TERMINAL_ENV_FAILURE_PREFIX}: planner produced no plan (environment/protocol failure)"
            )),
        }
    }
}

/// A named artifact produced by the Generator.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct Artifact {
    pub name: String,
    pub content: String,
    pub artifact_type: String,
}

/// True when an artifact is the self-evolution deliverable: a unified diff that
/// a later apply gate consumes verbatim.
///
/// Both the declared type and the file name are honoured because generators
/// emit both shapes. Every consumer of a change artifact asks through this one
/// function: an inline copy at each call site drifts, and a site that drifts
/// silently stops recognising artifacts (or starts recognising non-diffs) while
/// still compiling.
pub fn is_change_artifact(artifact_type: &str, name: &str) -> bool {
    artifact_type == "change" || name.to_lowercase().ends_with(".diff")
}

impl Artifact {
    pub fn is_change(&self) -> bool {
        is_change_artifact(&self.artifact_type, &self.name)
    }
}

/// Output produced by a Generator agent.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema, PartialEq)]
pub struct GeneratorOutput {
    /// Primary output content.
    pub content: serde_json::Value,
    /// Named deliverables.
    pub artifacts: Vec<Artifact>,
}

impl GeneratorOutput {
    /// The output of a round that never reached the generator, or reached it and
    /// got nothing. Whoever returns this must also say why in its outcome: an
    /// empty output on its own is indistinguishable from a generator that ran
    /// and wrote nothing, which is the reading this constructor exists to not
    /// invite.
    pub fn none() -> Self {
        Self {
            content: serde_json::Value::Null,
            artifacts: Vec::new(),
        }
    }
}

/// Cause of a generator that answered with an envelope carrying neither content
/// nor artifacts, and named no cause of its own. One definition, so every
/// producer of this feedback — pipeline attempt, local repair, roundtable round
/// — reports the identical string.
///
/// It is a generation defect, not a transport one: the prompt reached the
/// upstream and the model replied with a well-formed empty envelope. Naming it
/// under the terminal prefix instead sent the learning chain after the
/// environment while the defect sat in what the generator wrote.
pub fn empty_envelope_reason() -> String {
    format!(
        "{EMPTY_GENERATION_PREFIX}: the generator returned an envelope with no content and no artifacts"
    )
}

/// Deterministic judgement of an empty envelope: one failed criterion that
/// states the observed fact, and a zero score so the stall detector reads the
/// flat reading it actually is.
///
/// There is nothing here for an evaluator to judge, so asking one buys an
/// inference call whose only possible content is "there is nothing to assess",
/// and files the attempt under that noise instead of under the fact.
pub fn empty_envelope_evaluation() -> EvaluationResult {
    let reason = empty_envelope_reason();
    EvaluationResult {
        verdict: Verdict::Fail,
        score: Some(0),
        criteria: vec![Criterion {
            name: "generation_is_non_empty".into(),
            score: 0,
            comment: reason.clone(),
        }],
        feedback: reason,
        details: None,
    }
}

/// Last-resort label for a run that ended through a terminal guard without any
/// role naming the cause. It does not point at an observed defect, and it must
/// not be spelled as one: naming a role here blames a role that may never have
/// been called for a cause that was simply lost in transit.
pub fn unnamed_terminal_reason() -> String {
    format!(
        "{TERMINAL_ENV_FAILURE_PREFIX}: the run ended on a deterministic failure but no role named the cause"
    )
}

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
            if !artifact.is_change() {
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

    /// True when the generator returned an envelope with nothing in it: no
    /// content and no artifacts. The prompt was answered, so this is the
    /// generator failing to produce, never the transport failing to deliver.
    pub fn is_empty_envelope(&self) -> bool {
        self.artifacts.is_empty() && content_is_blank(&self.content)
    }

    /// True when the run failed for a deterministic environment/protocol
    /// reason, which only an in-band cause of its own can declare — tools that
    /// never executed, an explicit `environment_error`, a spent iteration
    /// budget. An empty envelope is not one of these: it names no cause, and
    /// nothing about it rules out the next attempt.
    pub fn is_terminal_env_failure(&self) -> bool {
        self.terminal_env_failure_reason().is_some()
    }

    /// Failure reason in the wire format outer loops match on, carrying the
    /// generator's own error when it has one. A prompt that never reached the
    /// upstream reports that failure. `None` for every other outcome — a run
    /// that produced something is not a failure, and a run that produced
    /// nothing without a cause of its own is named by
    /// [`empty_envelope_reason`], which is the repair loop's business and not a
    /// reason to stop.
    pub fn terminal_env_failure_reason(&self) -> Option<String> {
        if !self.artifacts.is_empty() {
            return None;
        }
        let serde_json::Value::String(detail) = &self.content else {
            return None;
        };
        if !names_a_deterministic_cause(detail) {
            return None;
        }
        Some(format!("{TERMINAL_ENV_FAILURE_PREFIX}: {}", detail.trim()))
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
    /// Whether the evaluator never judged anything because its own ReAct loop
    /// spent its whole iteration budget. The raw payload is read structurally
    /// rather than by scanning the feedback: that feedback is prose about
    /// somebody else's work, so a deterministic-sounding failure named inside
    /// it is a judgement the evaluator made, not a cause the evaluator suffered.
    ///
    /// Without this the exhausted run comes back as a bare `Fail` with empty
    /// feedback — indistinguishable from an evaluator that read the attempt and
    /// found it wanting — and buys a full round of local repairs plus every
    /// configured retry, none of which can judge anything either.
    pub fn terminal_env_failure_reason(&self) -> Option<String> {
        let cause = iteration_budget_exhausted_reason(self.details.as_ref()?)?;
        Some(format!("{TERMINAL_ENV_FAILURE_PREFIX}: evaluator {cause}"))
    }

    pub fn is_terminal_env_failure(&self) -> bool {
        self.terminal_env_failure_reason().is_some()
    }

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

/// Why a round stopped without anyone judging anything.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StopCause {
    /// A role named a cause of its own: the prompt never reached its upstream,
    /// a tool pipeline broke, or a loop spent its budget before producing
    /// anything. Another round must fail identically, so the run stops instead
    /// of paying for it.
    Deterministic { reason: String },
    /// The generator answered the prompt with an envelope carrying neither
    /// content nor artifacts. It names no cause and rules out no retry: the
    /// round stops, the debate need not.
    EmptyEnvelope,
    /// Nothing ran at all: the debate was configured for zero iterations, or a
    /// round was configured for parallel branches and none of them started.
    /// Nothing failed and nothing was observed — a cause invented here would
    /// send a reader after a role that was never called.
    NotAttempted,
}

impl StopCause {
    /// The composed cause in the wire format the outer loops match on.
    pub fn reason(&self) -> String {
        match self {
            Self::Deterministic { reason } => reason.clone(),
            Self::EmptyEnvelope => empty_envelope_reason(),
            Self::NotAttempted => {
                "nothing was attempted, so nothing was produced and no role failed".to_string()
            }
        }
    }

    /// Whether this cause rules out the next attempt. An empty envelope does
    /// not: it is a generation defect a repair can act on.
    pub fn is_deterministic(&self) -> bool {
        matches!(self, Self::Deterministic { .. } | Self::NotAttempted)
    }
}

/// Whether a round that stopped without a judgement had anything to judge.
///
/// Kept apart from [`StopCause`] rather than folded into it, because "why it
/// stopped" and "was there a product" are two observations and either one
/// re-derived from the other misreads a caller that acts on it: a round whose
/// judge spent its budget *did* produce something, and a round that stopped
/// before the generator wrote a word did not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoppedProduct {
    /// Nothing to judge: the generator never ran, or what it wrote was the
    /// failure itself.
    None,
    /// A product exists and no judge ever saw it, because the judge is what
    /// failed.
    Unjudged,
}

/// What one round or one branch produced: either a judgement of a product, or
/// the reason it stopped and whether a product existed.
///
/// A round with no product has no judgement to carry, and inventing one is not
/// a bookkeeping detail. That verdict enters the debate history the next planner
/// and judge read, the merge that picks a winning branch, and the contributions
/// the reflection chain learns from — teaching all three a conclusion nobody
/// reached.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RoundOutcome {
    /// A judge read a product and ruled on it.
    Judged { evaluation: EvaluationResult },
    /// The round ended without a judgement.
    Stopped {
        cause: StopCause,
        product: StoppedProduct,
    },
}

impl RoundOutcome {
    /// The judgement, when this round reached one.
    pub fn judgement(&self) -> Option<&EvaluationResult> {
        match self {
            Self::Judged { evaluation } => Some(evaluation),
            Self::Stopped { .. } => None,
        }
    }

    /// The composed cause when the round stopped on something no further round
    /// can change. `None` for a judged round and for an empty envelope, which
    /// rules out no retry. Reads through [`StopCause::is_deterministic`] so the
    /// two can never disagree about which causes those are.
    pub fn deterministic_stop(&self) -> Option<String> {
        match self {
            Self::Stopped { cause, .. } if cause.is_deterministic() => Some(cause.reason()),
            _ => None,
        }
    }

    /// The composed cause of any stop, deterministic or not. `None` when the
    /// round was judged.
    pub fn stop_reason(&self) -> Option<String> {
        match self {
            Self::Stopped { cause, .. } => Some(cause.reason()),
            Self::Judged { .. } => None,
        }
    }

    /// A round that stopped because the generator answered with an empty
    /// envelope. The cause is the round's own business; the debate decides for
    /// itself whether that is worth another round.
    pub fn empty_envelope() -> Self {
        Self::Stopped {
            cause: StopCause::EmptyEnvelope,
            product: StoppedProduct::None,
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
    pub outcome: RoundOutcome,
}

/// The highest-scoring branch among the ones a judge actually ruled on.
///
/// Only a judgement carries a score, so only judged branches can be ranked:
/// ranking the rest would read "nobody judged this" as a score of zero and let
/// an unjudged branch win a contest it never entered. Callers pass the judged
/// subset, which is also the set they had to find non-empty before ranking
/// anything at all.
pub fn best_scored<'a>(judged: &[&'a PgeBranchResult]) -> Option<&'a PgeBranchResult> {
    judged
        .iter()
        .copied()
        .max_by_key(|b| b.outcome.judgement().and_then(|e| e.score).unwrap_or(0))
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
    /// The branch that was carried out. `None` when no branch reached a
    /// judgement and the round stopped: there was no selection to make, and a
    /// sentinel id would read as one.
    #[serde(default)]
    pub selected_branch_id: Option<u32>,
    pub strategy: BranchMergeStrategy,
    pub reasoning: String,
}

/// A single roundtable iteration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PgeRoundtableIteration {
    pub iteration: u32,
    pub plan: PlannerOutput,
    pub generation: GeneratorOutput,
    pub outcome: RoundOutcome,
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

    /// 一个真计划（哪怕内容为空）不是环境失败：空计划是合法计划，把它判成
    /// 环境故障会让正常的"无事可做"也终止整条链。
    #[test]
    fn an_empty_but_real_plan_is_not_an_environment_failure() {
        let output = PlannerOutput {
            summary: "nothing to decompose".into(),
            plan: serde_json::json!({}),
            sub_tasks: Vec::new(),
            acceptance_criteria: Vec::new(),
        };
        assert!(!output.is_terminal_env_failure());
        assert!(output.terminal_env_failure_reason().is_none());
    }

    /// 传输失败要能被识别成终止性环境失败，并且把真因带出去。
    #[test]
    fn a_plan_that_never_reached_its_upstream_carries_the_real_cause() {
        let output = PlannerOutput {
            summary: "Planner prompt did not reach its upstream: HTTP 503".into(),
            plan: serde_json::Value::String(
                "environment_error: HTTP 503 upstream unavailable".into(),
            ),
            sub_tasks: Vec::new(),
            acceptance_criteria: Vec::new(),
        };
        assert!(output.is_terminal_env_failure());
        let reason = output.terminal_env_failure_reason().unwrap();
        assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
        assert!(reason.contains("HTTP 503 upstream unavailable"));
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

    /// The same spent budget on the third role. Read as an ordinary payload it
    /// is a `Fail` with empty feedback — indistinguishable from an evaluator
    /// that read the attempt and rejected it — so the run repairs towards
    /// nothing and re-judges with the same budget.
    #[test]
    fn an_exhausted_evaluator_is_not_read_as_a_bare_fail() {
        let parsed = crate::squad::pge::roundtable::parse_evaluation_result(&serde_json::json!({
            "status": MAX_ITERATIONS_STATUS,
            "iterations": 5,
            "pending_tool_calls": 1
        }));
        // The shape-level fallback is what makes this reachable at all: the
        // sentinel has no `feedback`, so it lands in `details` intact.
        assert_eq!(
            parsed.details,
            Some(serde_json::json!({
                "status": MAX_ITERATIONS_STATUS,
                "iterations": 5,
                "pending_tool_calls": 1
            }))
        );
        let reason = parsed
            .terminal_env_failure_reason()
            .expect("an evaluator that judged nothing did not reject anything");
        assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
        assert!(
            reason.contains("evaluator") && reason.contains("iteration budget"),
            "the reason must say which role spent what, got: {reason}"
        );
    }

    /// What came of asking the model straight out for an answer is part of the
    /// cause. "Never asked", "asked and silent", "asked and answered with words
    /// that hold no deliverable" and "the ask itself failed" are four states,
    /// and one string for all four cannot tell a budget we chose to end from a
    /// model that had nothing to hand over with the tools taken away.
    #[test]
    fn the_ways_a_spent_budget_can_end_each_read_differently() {
        let of = |final_draft: Option<&str>| {
            let mut value = serde_json::json!({
                "status": MAX_ITERATIONS_STATUS,
                "iterations": 1,
                "pending_tool_calls": 2
            });
            if let Some(v) = final_draft {
                value["final_draft"] = serde_json::Value::String(v.into());
            }
            value
        };
        let all: Vec<String> = [None, Some("empty"), Some("malformed"), Some("unavailable")]
            .into_iter()
            .map(|state| iteration_budget_exhausted_reason(&of(state)).unwrap())
            .collect();

        for (i, a) in all.iter().enumerate() {
            for b in all.iter().skip(i + 1) {
                assert_ne!(a, b, "two different end states read the same: {a}");
            }
            assert!(
                names_a_deterministic_cause(a),
                "a spent budget is terminal however the ask went: {a}"
            );
        }
    }

    /// The evaluator's feedback is prose about somebody else's work. An
    /// evaluator complaining that a change mishandles an environment error is
    /// making a judgement — reading that as the evaluator's own cause would
    /// turn a substantive review into a non-retryable environment failure.
    #[test]
    fn an_evaluator_judging_an_environment_error_is_still_judging() {
        let parsed = crate::squad::pge::roundtable::parse_evaluation_result(&serde_json::json!({
            "verdict": "fail",
            "feedback": "the change mishandles environment_error and tool_pipeline_broken cases",
            "score": 10
        }));
        assert!(
            parsed.terminal_env_failure_reason().is_none(),
            "a judgement about the environment is not the evaluator failing on the environment"
        );
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
        assert_ne!(reason, empty_envelope_reason());
    }

    /// An envelope with nothing in it and no cause of its own is the
    /// generator's own defect, not the transport's: the prompt was answered.
    /// It names no observed cause, so it must not be filed as a terminal
    /// environment failure — that reading ends the retries on the strength of
    /// a fact nobody saw, and points the reader at the upstream while the
    /// defect is in what the generator wrote.
    #[test]
    fn a_bare_empty_output_is_a_generation_defect_not_a_transport_one() {
        let output = GeneratorOutput {
            content: serde_json::Value::Null,
            artifacts: Vec::new(),
        };
        assert!(output.is_empty_envelope());
        assert!(!output.is_terminal_env_failure());
        assert_eq!(output.terminal_env_failure_reason(), None);
    }

    /// The same empty envelope with whitespace instead of `null` — the other
    /// shape a model writes when it means "nothing".
    #[test]
    fn a_whitespace_only_output_is_the_same_empty_envelope() {
        let output = GeneratorOutput {
            content: serde_json::Value::String("   \n".into()),
            artifacts: Vec::new(),
        };
        assert!(output.is_empty_envelope());
        assert!(!output.is_terminal_env_failure());
    }

    /// The ReAct runtime returns this sentinel when the loop runs out of
    /// iterations with tool calls still pending — the model was still reading
    /// and testing when the budget ended, so no artifact ever got a chance to
    /// be written. The cause is the budget, not the environment, and the
    /// reason has to say so: the generic wording sends the operator to look
    /// at the upstream while the run was failing locally.
    #[test]
    fn an_exhausted_iteration_budget_names_the_budget_not_the_environment() {
        let output = crate::squad::pge::roundtable::parse_generator_output(&serde_json::json!({
            "status": "max_iterations_reached",
            "iterations": 10,
            "pending_tool_calls": 2
        }));
        let reason = output
            .terminal_env_failure_reason()
            .expect("retrying with the same budget exhausts it the same way: not retryable");
        assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
        assert!(
            reason.contains("iteration budget"),
            "the reason must name the budget, got: {reason}"
        );
        assert!(
            reason.contains("max_iterations_reached") || reason.contains("10"),
            "the reason must carry the observed numbers, got: {reason}"
        );
        assert_ne!(
            reason,
            empty_envelope_reason(),
            "a budget exhaustion is not a generator that chose to produce nothing"
        );
    }

    /// An empty plan is a valid plan, so a planner that ran out of iterations
    /// is the same trap on the plan side: unnamed, it reads as a planner that
    /// considered the goal and had nothing to propose, and the generator is
    /// then asked to implement that nothing.
    #[test]
    fn a_planner_that_ran_out_of_iterations_is_not_read_as_an_empty_plan() {
        let output = crate::squad::pge::roundtable::parse_planner_output(
            &serde_json::json!({
                "status": MAX_ITERATIONS_STATUS,
                "iterations": 10,
                "pending_tool_calls": 4
            }),
            "edit the workspace",
        );
        let reason = output
            .terminal_env_failure_reason()
            .expect("the same budget fails the same way on a retry: not retryable");
        assert!(reason.starts_with(TERMINAL_ENV_FAILURE_PREFIX));
        assert!(
            reason.contains("iteration budget") && reason.contains("max_iterations=10"),
            "the reason must name the budget and the observed numbers, got: {reason}"
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
